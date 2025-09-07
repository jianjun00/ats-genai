from datetime import datetime
from typing import Dict, List, Any
import torch
import torch.nn as nn

from state.forecast_interval import ForecastInterval


class MultiInstrumentTransformer(nn.Module):
    """
    Minimal copy of the training-time model to support inference.
    Input:  [batch, steps, instruments, features]
    Output: [batch, steps, instruments, 1]
    """
    def __init__(self, num_instruments: int, num_features: int, d_model: int = 64, nhead: int = 4, num_layers: int = 2):
        super().__init__()
        self.input_proj = nn.Linear(num_features, d_model)
        self.inst_embed = nn.Embedding(num_instruments, d_model)
        encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, batch_first=True)
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.output_proj = nn.Linear(d_model, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        b, s, n, f = x.shape
        x = self.input_proj(x)
        inst_idx = torch.arange(n, device=x.device).unsqueeze(0).unsqueeze(0).expand(b, s, n)
        inst_emb = self.inst_embed(inst_idx)
        x = x + inst_emb
        x = x.view(b, s * n, -1)
        out = self.transformer(x)
        out = self.output_proj(out)
        out = out.view(b, s, n, 1)
        return out


class ForecastCallback:
    """
    Loads a transformer checkpoint and augments UniverseStateInterval with forecasts per instrument.

    Attempts to infer architecture hyperparameters from the state_dict shapes. If some are not
    inferable (e.g., nhead), picks a sensible default compatible with d_model.
    """

    def __init__(self, checkpoint_path: str, device: str | None = None, lead_steps: int = 1):
        self.checkpoint_path = checkpoint_path
        self.device = torch.device(device) if device else (torch.device('cuda') if torch.cuda.is_available() else (torch.device('mps') if torch.backends.mps.is_available() else torch.device('cpu')))
        self.lead_steps = lead_steps
        self.model: nn.Module | None = None
        self.num_instruments = None
        self.num_features = None
        self.d_model = None
        self.nhead = None
        self.num_layers = None
        self._load_model()

    def _infer_hparams_from_state_dict(self, sd: Dict[str, torch.Tensor]):
        # num_features, d_model from input_proj
        w = sd.get('input_proj.weight')
        if w is None:
            raise ValueError('Checkpoint missing input_proj.weight')
        d_model, num_features = w.shape
        # num_instruments from inst_embed
        inst_weight = sd.get('inst_embed.weight')
        if inst_weight is None:
            raise ValueError('Checkpoint missing inst_embed.weight')
        num_instruments = inst_weight.shape[0]
        # num_layers from transformer layers keys
        layer_indices = set()
        for k in sd.keys():
            if k.startswith('transformer.layers.'):
                parts = k.split('.')
                try:
                    idx = int(parts[2])
                    layer_indices.add(idx)
                except Exception:
                    pass
        num_layers = (max(layer_indices) + 1) if layer_indices else 2
        # nhead isn't stored; pick a divisor of d_model
        for cand in [8, 4, 2, 1]:
            if d_model % cand == 0:
                nhead = cand
                break
        else:
            nhead = 1
        return num_instruments, num_features, d_model, nhead, num_layers

    def _load_model(self):
        obj = torch.load(self.checkpoint_path, map_location=self.device)
        if isinstance(obj, dict) and all(isinstance(v, torch.Tensor) for v in obj.values()):
            state_dict = obj
            num_instruments, num_features, d_model, nhead, num_layers = self._infer_hparams_from_state_dict(state_dict)
            self.model = MultiInstrumentTransformer(num_instruments, num_features, d_model=d_model, nhead=nhead, num_layers=num_layers).to(self.device)
            self.model.load_state_dict(state_dict)
        elif isinstance(obj, dict) and 'state_dict' in obj and 'hparams' in obj:
            h = obj['hparams']
            self.model = MultiInstrumentTransformer(h['num_instruments'], h['num_features'], d_model=h['d_model'], nhead=h['nhead'], num_layers=h['num_layers']).to(self.device)
            self.model.load_state_dict(obj['state_dict'])
        else:
            raise ValueError('Unsupported checkpoint format')
        self.model.eval()
        # cache
        self.num_instruments = self.model.inst_embed.num_embeddings
        self.d_model = self.model.output_proj.in_features
        # infer features from input layer
        self.num_features = self.model.input_proj.in_features

    def _build_feature_tensor(self, instrument_ids: List[int], instrument_history: Dict[int, List[Any]]) -> torch.Tensor:
        # Determine sequence length from history (use min history length across instruments)
        lengths = [len(instrument_history.get(i, [])) for i in instrument_ids]
        seq_len = min([l for l in lengths if l > 0]) if any(l > 0 for l in lengths) else 1
        F = self.num_features

        def interval_to_feat(ii) -> List[float]:
            # Map features based on F
            o = getattr(ii, 'open', 0.0) or 0.0
            h = getattr(ii, 'high', 0.0) or 0.0
            l = getattr(ii, 'low', 0.0) or 0.0
            c = getattr(ii, 'close', 0.0) or 0.0
            v = getattr(ii, 'traded_volume', 0.0) or 0.0
            base = [c] if F == 1 else ([o, h, l, c] if F == 4 else [o, h, l, c, v])
            # pad or trim
            if len(base) < F:
                base = base + [0.0] * (F - len(base))
            elif len(base) > F:
                base = base[:F]
            return base

        X = torch.zeros(1, seq_len, len(instrument_ids), F, dtype=torch.float32)
        for n, inst_id in enumerate(instrument_ids):
            hist = instrument_history.get(inst_id, [])
            recent = hist[-seq_len:] if hist else []
            for t, ii in enumerate(recent):
                X[0, t, n, :] = torch.tensor(interval_to_feat(ii), dtype=torch.float32)
        return X.to(self.device)

    def augment_universe_state(self, universe_state, instrument_ids: List[int], instrument_history: Dict[int, List[Any]], current_time: datetime):
        if self.model is None:
            return
        if not hasattr(universe_state, 'instrument_forecast_intervals'):
            universe_state.instrument_forecast_intervals = {}
        X = self._build_feature_tensor(instrument_ids, instrument_history)
        with torch.no_grad():
            yhat = self.model(X)  # [1, S, N, 1]
        # Use last step prediction as next-interval forecast
        last = yhat[0, -1, :, 0].tolist()
        for idx, inst_id in enumerate(instrument_ids):
            universe_state.instrument_forecast_intervals[inst_id] = ForecastInterval(
                instrument_id=inst_id,
                start_date_time=current_time,
                end_date_time=universe_state.end_date_time,
                forecasts=[last[idx]]
            )
