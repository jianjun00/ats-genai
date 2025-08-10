"""
PyTorch Multi-Instrument Forecasting: Training/Validation Pipeline
- Uses DataLoader and model from pytorch_multi_instrument_forecast.py
- Includes masking for missing data, validation split, metrics, and checkpointing
- Assumes real-data batching via your runner framework (see TODO)
"""
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader, random_split
import numpy as np
import os
import argparse

# Define the model here to avoid importing modules that execute demo code at import-time
class MultiInstrumentTransformer(nn.Module):
    def __init__(self, num_instruments, num_features, d_model=64, nhead=4, num_layers=2):
        super().__init__()
        self.num_instruments = num_instruments
        self.input_proj = nn.Linear(num_features, d_model)
        self.inst_embed = nn.Embedding(num_instruments, d_model)
        self.transformer = nn.Transformer(
            d_model=d_model,
            nhead=nhead,
            num_encoder_layers=num_layers,
            num_decoder_layers=num_layers,
            batch_first=True
        )
        self.output_proj = nn.Linear(d_model, 1)
    def forward(self, x):
        # x: [batch, step, instrument, features]
        b, s, n, f = x.shape
        x = self.input_proj(x)  # [b, s, n, d_model]
        # Add instrument embedding
        inst_idx = torch.arange(n, device=x.device).unsqueeze(0).unsqueeze(0).expand(b, s, n)
        inst_emb = self.inst_embed(inst_idx)  # [b, s, n, d_model]
        x = x + inst_emb
        # Merge instrument into step: [b, s*n, d_model]
        x = x.view(b, s * n, -1)
        # For simplicity, use x as both src and tgt
        out = self.transformer(x, x)  # [b, s*n, d_model]
        out = self.output_proj(out)   # [b, s*n, 1]
        out = out.view(b, s, n, 1)    # [b, s, n, 1]
        return out

# --- Config ---
BATCH_SIZE = 32
EPOCHS = 20
VAL_RATIO = 0.2
CHECKPOINT_PATH = 'model_checkpoint.pt'

# --- Data loader from saved .pt ---
def load_train_data(path: str):
    """
    Load tensors saved by the generator into a dict containing X, y, and optional mask.
    Expected shapes:
      - X: [batch, lag_steps, num_instruments, num_features]
      - y: [batch, lead_steps, num_instruments, 1]
      - mask (optional): same shape as y, with 1 for valid targets and 0 for missing
    """
    data = torch.load(path, map_location="cpu")
    if isinstance(data, dict):
        X = data.get('X')
        y = data.get('y')
        mask = data.get('mask')
    else:
        # Backward compat: some examples may have saved a tuple
        X, y = data
        mask = None
    if mask is None:
        mask = ~torch.isnan(y)
        mask = mask.to(y.dtype)  # use float mask for multiplication
    else:
        # Ensure mask is float for loss math
        mask = mask.to(y.dtype)
    return X, y, mask

# --- Dataset and DataLoader ---
class MaskedMultiInstrumentDataset(Dataset):
    def __init__(self, X, y, mask=None):
        self.X = X
        self.y = y
        if mask is None:
            mask = ~torch.isnan(y)
        # store mask as float tensor for arithmetic
        self.mask = mask.to(y.dtype)
    def __len__(self):
        return self.X.shape[0]
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx], self.mask[idx]

def masked_mse_loss(pred, target, mask):
    diff = (pred - target) * mask
    denom = mask.sum().clamp_min(1.0)
    return (diff ** 2).sum() / denom

# --- Main Training Pipeline ---
def main():
    parser = argparse.ArgumentParser(description="Train Multi-Instrument Transformer from train_data.pt")
    parser.add_argument("--data-path", default="train_data.pt", help="Path to train_data.pt produced by generator")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Training batch size")
    parser.add_argument("--epochs", type=int, default=EPOCHS, help="Number of training epochs")
    parser.add_argument("--val-ratio", type=float, default=VAL_RATIO, help="Validation split ratio")
    parser.add_argument("--checkpoint", default=CHECKPOINT_PATH, help="Path to save best model checkpoint")
    args = parser.parse_args()

    data_path = args.data_path
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")

    X, y, mask = load_train_data(data_path)

    dataset = MaskedMultiInstrumentDataset(X, y, mask)
    val_len = int(args.val_ratio * len(dataset))
    train_len = len(dataset) - val_len
    train_set, val_set = random_split(dataset, [train_len, val_len])
    train_loader = DataLoader(train_set, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_set, batch_size=args.batch_size)

    # Infer model dimensions from X
    _, lag_steps, num_instruments, num_features = X.shape
    model = MultiInstrumentTransformer(num_instruments, num_features)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    best_val_loss = float('inf')
    # Device setup
    device = (
        torch.device("cuda") if torch.cuda.is_available() else
        torch.device("mps") if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available() else
        torch.device("cpu")
    )
    model.to(device)

    for epoch in range(args.epochs):
        model.train()
        for xb, yb, mb in train_loader:
            xb = xb.to(device)
            yb = yb.to(device)
            mb = mb.to(device)
            pred = model(xb)
            # Align sequence lengths if model output steps != target lead steps
            if pred.size(1) != yb.size(1):
                align = min(pred.size(1), yb.size(1))
                pred = pred[:, -align:, ...]
                yb = yb[:, -align:, ...]
                mb = mb[:, -align:, ...]
            loss = masked_mse_loss(pred, yb, mb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        model.eval()
        val_loss = 0
        n = 0
        with torch.no_grad():
            for xb, yb, mb in val_loader:
                xb = xb.to(device)
                yb = yb.to(device)
                mb = mb.to(device)
                pred = model(xb)
                if pred.size(1) != yb.size(1):
                    align = min(pred.size(1), yb.size(1))
                    pred = pred[:, -align:, ...]
                    yb = yb[:, -align:, ...]
                    mb = mb[:, -align:, ...]
                loss = masked_mse_loss(pred, yb, mb)
                val_loss += loss.item() * xb.size(0)
                n += xb.size(0)
        val_loss /= n
        print(f"Epoch {epoch+1}, Val Loss: {val_loss:.4f}")
        # Checkpoint
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save(model.state_dict(), args.checkpoint)
            print(f"Checkpoint saved at epoch {epoch+1} -> {args.checkpoint}")
    print("Training complete. Best val loss:", best_val_loss)

if __name__ == "__main__":
    main()
