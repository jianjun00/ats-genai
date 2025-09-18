#!/usr/bin/env python3
"""
Enhanced Temporal Fusion Transformer with Event Integration

Extends the existing TFT implementation with event integration capabilities,
multi-scale processing, and cross-scale attention mechanisms following
2024-2025 research on multi-modal financial time series forecasting.

Key Features:
- Integration with EventIntegrationLayer
- Multi-scale temporal processing
- Enhanced attention mechanisms
- Interpretable event impact analysis
- Memory-efficient implementation
"""

import torch
import torch.nn as nn
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import logging

# Import existing TFT components
from .temporal_fusion_transformer import (
    TFTConfig,
    TemporalFusionTransformer,
    TFTTrainer
)

# Import event integration components
from ..events.event_integration import (
    EventIntegrationLayer,
    EventSequence
)

# Import multi-scale components
from ..storage.multi_scale_sequence import (
    MultiScaleSequence,
    TimeScale
)

logger = logging.getLogger(__name__)


@dataclass
class EnhancedTFTConfig(TFTConfig):
    """Enhanced configuration with event integration and multi-scale support."""

    # Event integration settings
    enable_event_integration: bool = True
    max_events_per_step: int = 5
    event_attention_heads: int = 4
    event_dropout: float = 0.1

    # Multi-scale settings
    enable_multi_scale: bool = True
    scales_to_use: List[TimeScale] = field(default_factory=lambda: [
        TimeScale.MINUTE, TimeScale.HOURLY, TimeScale.DAILY
    ])
    scale_weights: Dict[str, float] = field(default_factory=lambda: {
        'minute': 0.6,
        'hourly': 0.3,
        'daily': 0.1
    })

    # Cross-scale attention settings
    enable_cross_scale_attention: bool = True
    cross_scale_attention_heads: int = 4

    # Enhanced features
    enable_volatility_clustering: bool = True
    enable_momentum_features: bool = True

    def __post_init__(self):
        super().__post_init__()

        # Ensure event integration is compatible with multi-scale
        if self.enable_event_integration and not self.enable_multi_scale:
            logger.warning("Event integration works best with multi-scale processing")


class MultiScaleFeatureProcessor(nn.Module):
    """Processes features from multiple temporal scales."""

    def __init__(
        self,
        scales: List[TimeScale],
        scale_weights: Dict[str, float],
        d_model: int = 64,
        dropout: float = 0.1
    ):
        super().__init__()
        self.scales = scales
        self.scale_weights = scale_weights
        self.d_model = d_model

        # Scale-specific projection layers
        self.scale_projections = nn.ModuleDict()
        for scale in scales:
            self.scale_projections[scale.value] = nn.Sequential(
                nn.Linear(d_model, d_model),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(d_model, d_model)
            )

        # Scale fusion network
        self.scale_fusion = nn.Sequential(
            nn.Linear(d_model * len(scales), d_model * 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * 2, d_model)
        )

        # Scale attention for adaptive weighting
        self.scale_attention = nn.MultiheadAttention(
            d_model, num_heads=4, batch_first=True
        )

        self.layer_norm = nn.LayerNorm(d_model)

        self._init_weights()

    def _init_weights(self):
        """Initialize weights."""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                torch.nn.init.xavier_uniform_(module.weight)
                torch.nn.init.zeros_(module.bias)

    def forward(
        self,
        scale_features: Dict[str, torch.Tensor]
    ) -> Tuple[torch.Tensor, Dict[str, torch.Tensor]]:
        """
        Process multi-scale features.

        Args:
            scale_features: Dict of features for each scale

        Returns:
            fused_features: Combined multi-scale features
            attention_weights: Attention weights for each scale
        """
        processed_scales = []
        scale_names = []

        # Process each scale
        for scale in self.scales:
            scale_name = scale.value
            if scale_name in scale_features:
                features = scale_features[scale_name]
                processed = self.scale_projections[scale_name](features)
                processed_scales.append(processed)
                scale_names.append(scale_name)

        if not processed_scales:
            # No scale features available
            batch_size, seq_len = 1, 1
            return torch.zeros(batch_size, seq_len, self.d_model), {}

        # Stack processed scales
        stacked_scales = torch.stack(processed_scales, dim=-2)  # (..., n_scales, d_model)

        # Apply scale attention for adaptive weighting
        batch_shape = stacked_scales.shape[:-2]
        flat_scales = stacked_scales.view(-1, len(processed_scales), self.d_model)

        attended_scales, attention_weights = self.scale_attention(
            flat_scales, flat_scales, flat_scales
        )

        # Reshape back
        attended_scales = attended_scales.view(*batch_shape, len(processed_scales), self.d_model)

        # Weighted combination using learned attention
        fused_features = attended_scales.mean(dim=-2)  # Average across scales

        # Apply layer normalization
        fused_features = self.layer_norm(fused_features)

        # Create attention weights dict
        attention_dict = {}
        for i, scale_name in enumerate(scale_names):
            attention_dict[scale_name] = attention_weights[..., i].mean(dim=1)  # Average across heads

        return fused_features, attention_dict


class CrossScaleAttention(nn.Module):
    """Cross-scale attention mechanism for multi-temporal processing."""

    def __init__(
        self,
        d_model: int = 64,
        num_heads: int = 4,
        dropout: float = 0.1
    ):
        super().__init__()
        self.d_model = d_model
        self.num_heads = num_heads

        # Cross-scale attention
        self.cross_attention = nn.MultiheadAttention(
            d_model, num_heads, dropout=dropout, batch_first=True
        )

        # Feed-forward network
        self.feed_forward = nn.Sequential(
            nn.Linear(d_model, d_model * 4),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * 4, d_model)
        )

        # Layer normalization
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)

        self.dropout = nn.Dropout(dropout)

        self._init_weights()

    def _init_weights(self):
        """Initialize weights."""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                torch.nn.init.xavier_uniform_(module.weight)
                torch.nn.init.zeros_(module.bias)

    def forward(
        self,
        minute_features: torch.Tensor,
        context_features: torch.Tensor,
        context_mask: Optional[torch.Tensor] = None
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Apply cross-scale attention.

        Args:
            minute_features: Primary minute-scale features (batch, seq_len, d_model)
            context_features: Context from other scales (batch, context_len, d_model)
            context_mask: Optional mask for context features

        Returns:
            enhanced_features: Enhanced minute features
            attention_weights: Cross-scale attention weights
        """
        # Self-attention with cross-scale context
        attended_features, attention_weights = self.cross_attention(
            minute_features,  # Query
            context_features,  # Key
            context_features,  # Value
            key_padding_mask=context_mask
        )

        # Residual connection and layer norm
        attended_features = self.norm1(minute_features + self.dropout(attended_features))

        # Feed-forward network
        ff_output = self.feed_forward(attended_features)
        enhanced_features = self.norm2(attended_features + self.dropout(ff_output))

        return enhanced_features, attention_weights


class EnhancedTemporalFusionTransformer(nn.Module):
    """
    Enhanced TFT with event integration and multi-scale processing.

    Combines the original TFT architecture with:
    - Event integration layer
    - Multi-scale feature processing
    - Cross-scale attention mechanisms
    - Enhanced interpretability features
    """

    def __init__(self, config: EnhancedTFTConfig):
        super().__init__()
        self.config = config

        # Base TFT model (without event integration initially)
        tft_config = TFTConfig(
            hidden_size=config.hidden_size,
            lstm_layers=config.lstm_layers,
            attention_head_size=config.attention_head_size,
            dropout=config.dropout,
            max_encoder_length=config.max_encoder_length,
            max_prediction_length=config.max_prediction_length,
            temporal_features=config.temporal_features,
            target_features=config.target_features,
            use_sentiment_features=False  # We'll handle this separately
        )
        self.base_tft = TemporalFusionTransformer(tft_config)

        # Event integration layer
        if config.enable_event_integration:
            self.event_integration = EventIntegrationLayer(
                d_model=config.hidden_size,
                num_attention_heads=config.event_attention_heads,
                dropout=config.event_dropout,
                max_events_per_step=config.max_events_per_step
            )

        # Multi-scale processing
        if config.enable_multi_scale:
            self.multi_scale_processor = MultiScaleFeatureProcessor(
                scales=config.scales_to_use,
                scale_weights=config.scale_weights,
                d_model=config.hidden_size,
                dropout=config.dropout
            )

        # Cross-scale attention
        if config.enable_cross_scale_attention:
            self.cross_scale_attention = CrossScaleAttention(
                d_model=config.hidden_size,
                num_heads=config.cross_scale_attention_heads,
                dropout=config.dropout
            )

        # Enhanced feature extractors
        if config.enable_volatility_clustering:
            self.volatility_extractor = self._create_volatility_extractor()

        if config.enable_momentum_features:
            self.momentum_extractor = self._create_momentum_extractor()

        # Interpretability layers
        self.feature_importance_scorer = nn.Sequential(
            nn.Linear(config.hidden_size, config.hidden_size // 2),
            nn.ReLU(),
            nn.Linear(config.hidden_size // 2, len(config.temporal_features)),
            nn.Sigmoid()
        )

        self._init_weights()

    def _create_volatility_extractor(self) -> nn.Module:
        """Create volatility clustering feature extractor."""
        return nn.Sequential(
            nn.Linear(self.config.hidden_size, self.config.hidden_size),
            nn.ReLU(),
            nn.Linear(self.config.hidden_size, self.config.hidden_size // 2),
            nn.Tanh()
        )

    def _create_momentum_extractor(self) -> nn.Module:
        """Create momentum feature extractor."""
        return nn.Sequential(
            nn.Linear(self.config.hidden_size, self.config.hidden_size),
            nn.ReLU(),
            nn.Linear(self.config.hidden_size, self.config.hidden_size // 2),
            nn.Tanh()
        )

    def _init_weights(self):
        """Initialize weights for new components."""
        for module in [self.feature_importance_scorer]:
            for layer in module:
                if isinstance(layer, nn.Linear):
                    torch.nn.init.xavier_uniform_(layer.weight)
                    torch.nn.init.zeros_(layer.bias)

    def forward(
        self,
        encoder_input: torch.Tensor,
        decoder_input: torch.Tensor,
        encoder_lengths: torch.Tensor,
        multi_scale_features: Optional[Dict[str, torch.Tensor]] = None,
        event_features: Optional[torch.Tensor] = None,
        event_mask: Optional[torch.Tensor] = None,
        timestamps: Optional[torch.Tensor] = None
    ) -> Dict[str, torch.Tensor]:
        """
        Enhanced forward pass with event integration and multi-scale processing.

        Args:
            encoder_input: Historical data (batch, encoder_length, features)
            decoder_input: Future known features (batch, decoder_length, features)
            encoder_lengths: Actual encoder sequence lengths
            multi_scale_features: Optional multi-scale features
            event_features: Optional event features (batch, total_length, max_events, event_dim)
            event_mask: Optional event mask (batch, total_length, max_events)
            timestamps: Optional timestamps for temporal encoding

        Returns:
            Dictionary containing predictions and interpretability information
        """
        encoder_input.size(0)
        encoder_length = encoder_input.size(1)
        decoder_input.size(1)

        # Process multi-scale features if available
        if self.config.enable_multi_scale and multi_scale_features is not None:
            multi_scale_processed, scale_attention = self.multi_scale_processor(multi_scale_features)

            # Integrate multi-scale features with encoder input
            if multi_scale_processed.shape[1] == encoder_length:
                encoder_input = encoder_input + multi_scale_processed[:, :encoder_length]

        # Get base TFT output without sentiment features
        tft_output = self.base_tft(
            encoder_input, decoder_input, encoder_lengths,
            sentiment_features=None  # We handle events separately
        )

        # Extract features for event integration
        encoder_output = tft_output['encoder_output']
        decoder_output = tft_output['decoder_output']

        # Combine encoder and decoder features for event processing
        total_features = torch.cat([encoder_output, decoder_output], dim=1)

        # Apply event integration if enabled
        event_results = {}
        if (self.config.enable_event_integration and
            event_features is not None and
            event_mask is not None):

            event_integration_output = self.event_integration(
                total_features, event_features, event_mask, timestamps
            )

            # Split enhanced features back into encoder/decoder
            enhanced_total = event_integration_output['enhanced_features']
            enhanced_encoder = enhanced_total[:, :encoder_length]
            enhanced_decoder = enhanced_total[:, encoder_length:]

            # Store event integration results
            event_results = {
                'event_attention_weights': event_integration_output['attention_weights'],
                'event_impact_scores': event_integration_output['event_impact_scores'],
                'event_importance': self.event_integration.get_event_importance(
                    event_integration_output['attention_weights'], event_mask
                )
            }

            # Use enhanced features for final prediction
            final_decoder_features = enhanced_decoder
        else:
            final_decoder_features = decoder_output

        # Apply cross-scale attention if enabled
        if self.config.enable_cross_scale_attention and multi_scale_features is not None:
            # Use longer-scale features as context
            context_features = []
            for scale in [TimeScale.HOURLY, TimeScale.DAILY]:
                if scale.value in multi_scale_features:
                    context_features.append(multi_scale_features[scale.value])

            if context_features:
                combined_context = torch.cat(context_features, dim=1)
                final_decoder_features, cross_scale_attention = self.cross_scale_attention(
                    final_decoder_features, combined_context
                )
                event_results['cross_scale_attention'] = cross_scale_attention

        # Generate final predictions
        predictions = self.base_tft.output_projection(final_decoder_features)

        # Extract enhanced features
        enhanced_features = {}
        if self.config.enable_volatility_clustering:
            enhanced_features['volatility'] = self.volatility_extractor(final_decoder_features)

        if self.config.enable_momentum_features:
            enhanced_features['momentum'] = self.momentum_extractor(final_decoder_features)

        # Feature importance scoring
        feature_importance = self.feature_importance_scorer(final_decoder_features.mean(dim=1))

        # Combine all results
        results = {
            'predictions': predictions,
            'attention_weights': tft_output['attention_weights'],
            'encoder_variable_weights': tft_output['encoder_variable_weights'],
            'decoder_variable_weights': tft_output['decoder_variable_weights'],
            'feature_importance': feature_importance,
            'enhanced_features': enhanced_features,
            'encoder_output': tft_output['encoder_output'],
            'decoder_output': final_decoder_features
        }

        # Add event integration results
        results.update(event_results)

        # Add multi-scale results
        if self.config.enable_multi_scale and multi_scale_features is not None:
            results['scale_attention'] = scale_attention

        return results

    def predict_with_events(
        self,
        multi_scale_sequence: MultiScaleSequence,
        event_sequence: Optional[EventSequence] = None,
        prediction_horizon: int = None
    ) -> Dict[str, torch.Tensor]:
        """
        Convenient prediction method using multi-scale sequence and events.

        Args:
            multi_scale_sequence: Multi-scale temporal data
            event_sequence: Optional event sequence
            prediction_horizon: Prediction horizon (defaults to config)

        Returns:
            Prediction results with interpretability information
        """
        self.eval()

        prediction_horizon = prediction_horizon or self.config.max_prediction_length

        # Extract features from multi-scale sequence
        minute_features = multi_scale_sequence.get_features(TimeScale.MINUTE, 'all')
        if minute_features is None:
            raise ValueError("Minute-level features required for prediction")

        # Prepare encoder input (historical data)
        encoder_length = min(self.config.max_encoder_length, minute_features.shape[0] - prediction_horizon)
        encoder_input = torch.tensor(
            minute_features[:encoder_length], dtype=torch.float32
        ).unsqueeze(0)  # Add batch dimension

        # Prepare decoder input (future known features - zeros for prediction)
        decoder_input = torch.zeros(
            1, prediction_horizon, minute_features.shape[1]
        )

        # Encoder lengths
        encoder_lengths = torch.tensor([encoder_length])

        # Prepare multi-scale features
        multi_scale_features = {}
        for scale in self.config.scales_to_use:
            features = multi_scale_sequence.get_features(scale, 'all')
            if features is not None:
                multi_scale_features[scale.value] = torch.tensor(
                    features, dtype=torch.float32
                ).unsqueeze(0)

        # Prepare event features if available
        event_features = None
        event_mask = None
        if event_sequence is not None:
            timestamps = multi_scale_sequence.get_timestamps(TimeScale.MINUTE)
            if timestamps is not None:
                timestamps_tensor = torch.tensor(
                    [ts.timestamp() for ts in timestamps], dtype=torch.float32
                ).unsqueeze(0)

                event_features, event_mask = event_sequence.to_tensor_sequence(
                    timestamps_tensor, self.config.max_events_per_step
                )

        # Generate prediction
        with torch.no_grad():
            results = self.forward(
                encoder_input=encoder_input,
                decoder_input=decoder_input,
                encoder_lengths=encoder_lengths,
                multi_scale_features=multi_scale_features,
                event_features=event_features,
                event_mask=event_mask
            )

        return results


class EnhancedTFTTrainer(TFTTrainer):
    """Enhanced trainer for the Enhanced TFT model."""

    def __init__(self, model: EnhancedTemporalFusionTransformer, config: EnhancedTFTConfig):
        # Initialize base trainer with model and config
        super().__init__(model, config)
        self.enhanced_config = config

        # Enhanced loss functions
        if config.enable_event_integration:
            self.event_loss_weight = 0.1

        if config.enable_multi_scale:
            self.scale_consistency_loss_weight = 0.05

    def compute_enhanced_loss(
        self,
        outputs: Dict[str, torch.Tensor],
        targets: torch.Tensor,
        event_mask: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """Compute enhanced loss with additional regularization terms."""

        # Base prediction loss
        base_loss = self.criterion(outputs['predictions'], targets)

        total_loss = base_loss

        # Event integration regularization
        if (self.enhanced_config.enable_event_integration and
            'event_impact_scores' in outputs and
            event_mask is not None):

            # Encourage sparse event usage (L1 regularization)
            event_sparsity_loss = torch.mean(outputs['event_impact_scores'])
            total_loss += self.event_loss_weight * event_sparsity_loss

        # Scale consistency loss
        if (self.enhanced_config.enable_multi_scale and
            'scale_attention' in outputs):

            # Encourage balanced scale usage
            scale_attention = outputs['scale_attention']
            scale_entropy = sum(-torch.mean(attn * torch.log(attn + 1e-8))
                              for attn in scale_attention.values())
            total_loss += self.scale_consistency_loss_weight * scale_entropy

        return total_loss

    def train_epoch_enhanced(self, train_loader):
        """Enhanced training epoch with additional loss terms."""
        self.model.train()
        total_loss = 0
        num_batches = 0

        for batch in train_loader:
            self.optimizer.zero_grad()

            # Extract batch data
            encoder_input = batch['encoder_input'].to(self.device)
            decoder_input = batch['decoder_input'].to(self.device)
            encoder_lengths = batch['encoder_lengths'].to(self.device)
            targets = batch['targets'].to(self.device)

            # Optional enhanced features
            multi_scale_features = batch.get('multi_scale_features')
            event_features = batch.get('event_features')
            event_mask = batch.get('event_mask')
            timestamps = batch.get('timestamps')

            if multi_scale_features is not None:
                multi_scale_features = {
                    k: v.to(self.device) for k, v in multi_scale_features.items()
                }

            if event_features is not None:
                event_features = event_features.to(self.device)
                event_mask = event_mask.to(self.device)

            if timestamps is not None:
                timestamps = timestamps.to(self.device)

            # Forward pass
            outputs = self.model(
                encoder_input, decoder_input, encoder_lengths,
                multi_scale_features, event_features, event_mask, timestamps
            )

            # Compute enhanced loss
            loss = self.compute_enhanced_loss(outputs, targets, event_mask)

            # Backward pass
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
            self.optimizer.step()

            total_loss += loss.item()
            num_batches += 1

        return total_loss / num_batches


def create_enhanced_tft(
    temporal_features: List[str],
    target_features: List[str],
    hidden_size: int = 64,
    max_encoder_length: int = 120,
    max_prediction_length: int = 30,
    enable_events: bool = True,
    enable_multi_scale: bool = True,
    **kwargs
) -> EnhancedTemporalFusionTransformer:
    """
    Create enhanced TFT model with event integration and multi-scale processing.

    Args:
        temporal_features: List of temporal feature names
        target_features: List of target feature names
        hidden_size: Hidden size of the model
        max_encoder_length: Maximum encoder sequence length
        max_prediction_length: Maximum prediction horizon
        enable_events: Whether to enable event integration
        enable_multi_scale: Whether to enable multi-scale processing
        **kwargs: Additional configuration parameters

    Returns:
        Configured Enhanced TFT model
    """
    config = EnhancedTFTConfig(
        hidden_size=hidden_size,
        max_encoder_length=max_encoder_length,
        max_prediction_length=max_prediction_length,
        temporal_features=temporal_features,
        target_features=target_features,
        enable_event_integration=enable_events,
        enable_multi_scale=enable_multi_scale,
        **kwargs
    )

    return EnhancedTemporalFusionTransformer(config)