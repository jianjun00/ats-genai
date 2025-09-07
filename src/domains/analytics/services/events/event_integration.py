#!/usr/bin/env python3
"""
Event Integration Layer for Financial Time Series Models

Implements time-aware event integration following 2024-2025 research on
LLM-based event analysis and reflection mechanisms for financial forecasting.

Key Features:
- Time-aware event attention mechanism
- Multi-type event support (news, earnings, upgrades, economic)
- Event embedding and encoding
- Gated integration with sequence models
- Interpretable event impact scoring
- Memory-efficient event caching
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import logging
from enum import Enum
import math

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of market events."""
    NEWS = "news"
    EARNINGS = "earnings"
    UPGRADE = "upgrade"
    DOWNGRADE = "downgrade"
    ECONOMIC = "economic"
    CORPORATE_ACTION = "corporate_action"
    INSIDER_TRADING = "insider_trading"


@dataclass
class MarketEvent:
    """Market event data structure."""
    event_id: str
    symbol: str
    timestamp: datetime
    event_type: EventType
    content: str
    sentiment_score: float  # -1 to 1
    importance_score: float  # 0 to 1
    confidence_score: float = 1.0  # 0 to 1
    embedding: Optional[torch.Tensor] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_tensor(self, device: torch.device = None) -> torch.Tensor:
        """Convert event to tensor representation."""
        # Basic features: sentiment, importance, confidence, event type
        event_type_idx = list(EventType).index(self.event_type)
        features = torch.tensor([
            self.sentiment_score,
            self.importance_score,
            self.confidence_score,
            event_type_idx / len(EventType)  # Normalize event type
        ], dtype=torch.float32)

        if device is not None:
            features = features.to(device)

        return features


@dataclass
class EventSequence:
    """Sequence of market events with temporal indexing."""
    events: List[MarketEvent]
    time_range: Tuple[datetime, datetime]
    symbol: str

    def get_events_in_window(
        self,
        center_time: datetime,
        window_hours: float = 2.0
    ) -> List[MarketEvent]:
        """Get events within time window around center time."""
        window = timedelta(hours=window_hours)
        start_time = center_time - window
        end_time = center_time + window

        return [
            event for event in self.events
            if start_time <= event.timestamp <= end_time
        ]

    def to_tensor_sequence(
        self,
        timestamps: torch.Tensor,
        max_events_per_step: int = 5,
        device: torch.device = None
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Convert to tensor sequence aligned with timestamps.

        Returns:
            event_features: (batch_size, seq_len, max_events, event_dim)
            event_mask: (batch_size, seq_len, max_events) - 1 for valid events
        """
        batch_size, seq_len = timestamps.shape
        event_dim = 4  # Basic event features

        event_features = torch.zeros(
            batch_size, seq_len, max_events_per_step, event_dim,
            device=device
        )
        event_mask = torch.zeros(
            batch_size, seq_len, max_events_per_step,
            device=device
        )

        for b in range(batch_size):
            for t in range(seq_len):
                # Convert timestamp to datetime (assuming timestamps are Unix timestamps)
                if isinstance(timestamps[b, t].item(), (int, float)):
                    current_time = datetime.fromtimestamp(timestamps[b, t].item())
                else:
                    continue

                # Get events near this timestamp
                nearby_events = self.get_events_in_window(current_time, window_hours=0.5)

                # Fill event features
                for i, event in enumerate(nearby_events[:max_events_per_step]):
                    event_features[b, t, i] = event.to_tensor(device)
                    event_mask[b, t, i] = 1.0

        return event_features, event_mask


class EventEncoder(nn.Module):
    """Encodes market events into fixed-size representations."""

    def __init__(
        self,
        event_dim: int = 4,
        hidden_dim: int = 64,
        output_dim: int = 32,
        num_event_types: int = 7
    ):
        super().__init__()
        self.event_dim = event_dim
        self.hidden_dim = hidden_dim
        self.output_dim = output_dim

        # Event type embedding
        self.event_type_embedding = nn.Embedding(num_event_types, hidden_dim // 4)

        # Feature projection
        self.feature_projection = nn.Linear(event_dim - 1, hidden_dim - hidden_dim // 4)

        # Encoding layers
        self.encoder = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, output_dim),
            nn.Tanh()
        )

        self._init_weights()

    def _init_weights(self):
        """Initialize weights."""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                torch.nn.init.xavier_uniform_(module.weight)
                torch.nn.init.zeros_(module.bias)
            elif isinstance(module, nn.Embedding):
                torch.nn.init.xavier_uniform_(module.weight)

    def forward(self, event_features: torch.Tensor) -> torch.Tensor:
        """
        Encode event features.

        Args:
            event_features: (..., event_dim) - [sentiment, importance, confidence, event_type_normalized]

        Returns:
            Encoded event representations (..., output_dim)
        """
        # Separate event type from other features
        other_features = event_features[..., :3]  # sentiment, importance, confidence
        event_type_norm = event_features[..., 3]  # normalized event type

        # Convert normalized event type back to indices
        event_type_idx = (event_type_norm * (len(EventType) - 1)).long()

        # Get embeddings
        type_embedding = self.event_type_embedding(event_type_idx)
        feature_projection = self.feature_projection(other_features)

        # Combine features
        combined_features = torch.cat([feature_projection, type_embedding], dim=-1)

        # Encode
        encoded = self.encoder(combined_features)

        return encoded


class TemporalEventAttention(nn.Module):
    """Time-aware attention between sequence features and events."""

    def __init__(
        self,
        d_model: int = 64,
        event_dim: int = 32,
        num_heads: int = 4,
        dropout: float = 0.1
    ):
        super().__init__()
        self.d_model = d_model
        self.event_dim = event_dim
        self.num_heads = num_heads
        self.head_dim = d_model // num_heads

        assert d_model % num_heads == 0, "d_model must be divisible by num_heads"

        # Project events to model dimension
        self.event_projection = nn.Linear(event_dim, d_model)

        # Multi-head attention components
        self.query_projection = nn.Linear(d_model, d_model)
        self.key_projection = nn.Linear(d_model, d_model)
        self.value_projection = nn.Linear(d_model, d_model)

        # Output projection
        self.output_projection = nn.Linear(d_model, d_model)

        # Temporal position encoding
        self.temporal_encoder = TemporalPositionEncoder(d_model)

        self.dropout = nn.Dropout(dropout)
        self.layer_norm = nn.LayerNorm(d_model)

        self._init_weights()

    def _init_weights(self):
        """Initialize weights."""
        for module in [self.event_projection, self.query_projection,
                      self.key_projection, self.value_projection, self.output_projection]:
            torch.nn.init.xavier_uniform_(module.weight)
            torch.nn.init.zeros_(module.bias)

    def forward(
        self,
        sequence_features: torch.Tensor,  # (batch, seq_len, d_model)
        event_features: torch.Tensor,    # (batch, seq_len, max_events, event_dim)
        event_mask: torch.Tensor,        # (batch, seq_len, max_events)
        timestamps: Optional[torch.Tensor] = None
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Apply temporal event attention.

        Returns:
            enhanced_features: (batch, seq_len, d_model)
            attention_weights: (batch, seq_len, max_events)
        """
        batch_size, seq_len, d_model = sequence_features.shape
        event_features.shape[2]

        # Project events to model dimension
        event_projected = self.event_projection(event_features)  # (batch, seq_len, max_events, d_model)

        # Add temporal position encoding if timestamps provided
        if timestamps is not None:
            sequence_features = self.temporal_encoder(sequence_features, timestamps)

        # Reshape for multi-head attention
        queries = self.query_projection(sequence_features)  # (batch, seq_len, d_model)

        # For each timestep, attend to its events
        attended_features = []
        all_attention_weights = []

        for t in range(seq_len):
            # Get queries for this timestep
            step_queries = queries[:, t:t+1, :]  # (batch, 1, d_model)

            # Get events for this timestep
            step_events = event_projected[:, t, :, :]  # (batch, max_events, d_model)
            step_mask = event_mask[:, t, :]  # (batch, max_events)

            # Keys and values from events
            keys = self.key_projection(step_events)  # (batch, max_events, d_model)
            values = self.value_projection(step_events)  # (batch, max_events, d_model)

            # Multi-head attention
            attended, attention_weights = self._multi_head_attention(
                step_queries, keys, values, step_mask
            )

            attended_features.append(attended)
            all_attention_weights.append(attention_weights)

        # Combine attended features
        attended_sequence = torch.cat(attended_features, dim=1)  # (batch, seq_len, d_model)
        attention_weights = torch.stack(all_attention_weights, dim=1)  # (batch, seq_len, max_events)

        # Apply output projection and residual connection
        output = self.output_projection(attended_sequence)
        output = self.layer_norm(output + sequence_features)

        return output, attention_weights

    def _multi_head_attention(
        self,
        queries: torch.Tensor,  # (batch, 1, d_model)
        keys: torch.Tensor,     # (batch, max_events, d_model)
        values: torch.Tensor,   # (batch, max_events, d_model)
        mask: torch.Tensor      # (batch, max_events)
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Multi-head attention computation."""
        batch_size = queries.shape[0]

        # Reshape for multi-head attention
        Q = queries.view(batch_size, 1, self.num_heads, self.head_dim).transpose(1, 2)
        K = keys.view(batch_size, -1, self.num_heads, self.head_dim).transpose(1, 2)
        V = values.view(batch_size, -1, self.num_heads, self.head_dim).transpose(1, 2)

        # Scaled dot-product attention
        scores = torch.matmul(Q, K.transpose(-2, -1)) / math.sqrt(self.head_dim)

        # Apply mask (set masked positions to large negative value)
        if mask is not None:
            mask_expanded = mask.unsqueeze(1).unsqueeze(1)  # (batch, 1, 1, max_events)
            scores = scores.masked_fill(mask_expanded == 0, -1e9)

        attention_weights = F.softmax(scores, dim=-1)
        attention_weights = self.dropout(attention_weights)

        # Apply attention to values
        attended = torch.matmul(attention_weights, V)

        # Concatenate heads
        attended = attended.transpose(1, 2).contiguous().view(
            batch_size, 1, self.d_model
        )

        # Average attention weights across heads for interpretability
        avg_attention = attention_weights.mean(dim=1).squeeze(1)  # (batch, max_events)

        return attended, avg_attention


class TemporalPositionEncoder(nn.Module):
    """Encode temporal positions for time-aware processing."""

    def __init__(self, d_model: int, max_len: int = 10000):
        super().__init__()
        self.d_model = d_model

        # Create sinusoidal position encoding
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)

        div_term = torch.exp(torch.arange(0, d_model, 2).float() *
                            (-math.log(10000.0) / d_model))

        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)

        self.register_buffer('pe', pe)

        # Learnable temporal scaling
        self.temporal_scale = nn.Parameter(torch.ones(1))

    def forward(
        self,
        x: torch.Tensor,
        timestamps: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        Add temporal position encoding.

        Args:
            x: Input tensor (batch, seq_len, d_model)
            timestamps: Optional timestamps for absolute positioning

        Returns:
            Tensor with position encoding added
        """
        if timestamps is not None:
            # Use absolute timestamps for positioning
            # Normalize timestamps to reasonable range
            normalized_timestamps = (timestamps - timestamps.min()) / 3600  # Hours
            positions = normalized_timestamps.long().clamp(0, self.pe.size(0) - 1)

            pe_batch = self.pe[positions]  # (batch, seq_len, d_model)
        else:
            # Use relative positions
            seq_len = x.size(1)
            pe_batch = self.pe[:seq_len].unsqueeze(0).expand(x.size(0), -1, -1)

        return x + self.temporal_scale * pe_batch


class EventGatingMechanism(nn.Module):
    """Gated integration of event context with sequence features."""

    def __init__(self, d_model: int, dropout: float = 0.1):
        super().__init__()
        self.d_model = d_model

        # Gating network
        self.gate_network = nn.Sequential(
            nn.Linear(d_model * 2, d_model),
            nn.Sigmoid()
        )

        # Event context transformation
        self.context_transform = nn.Sequential(
            nn.Linear(d_model, d_model),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(d_model, d_model)
        )

        # Output layer norm
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
        sequence_features: torch.Tensor,  # (batch, seq_len, d_model)
        event_context: torch.Tensor       # (batch, seq_len, d_model)
    ) -> torch.Tensor:
        """
        Apply gated integration of event context.

        Returns:
            Enhanced sequence features (batch, seq_len, d_model)
        """
        # Transform event context
        transformed_context = self.context_transform(event_context)

        # Compute gates based on both sequence and event features
        combined_features = torch.cat([sequence_features, transformed_context], dim=-1)
        gates = self.gate_network(combined_features)

        # Apply gated combination
        enhanced_features = sequence_features + gates * transformed_context

        # Apply layer normalization
        output = self.layer_norm(enhanced_features)

        return output


class EventIntegrationLayer(nn.Module):
    """
    Complete event integration layer for financial time series models.

    Combines event encoding, temporal attention, and gated integration
    to incorporate market events into sequence models.
    """

    def __init__(
        self,
        d_model: int = 64,
        event_dim: int = 4,
        encoded_event_dim: int = 32,
        num_attention_heads: int = 4,
        dropout: float = 0.1,
        max_events_per_step: int = 5
    ):
        super().__init__()
        self.d_model = d_model
        self.event_dim = event_dim
        self.max_events_per_step = max_events_per_step

        # Event encoder
        self.event_encoder = EventEncoder(
            event_dim=event_dim,
            output_dim=encoded_event_dim
        )

        # Temporal event attention
        self.temporal_attention = TemporalEventAttention(
            d_model=d_model,
            event_dim=encoded_event_dim,
            num_heads=num_attention_heads,
            dropout=dropout
        )

        # Event gating mechanism
        self.event_gate = EventGatingMechanism(d_model, dropout)

        # Event impact scoring (for interpretability)
        self.impact_scorer = nn.Sequential(
            nn.Linear(d_model, d_model // 2),
            nn.ReLU(),
            nn.Linear(d_model // 2, 1),
            nn.Sigmoid()
        )

        self._init_weights()

    def _init_weights(self):
        """Initialize weights."""
        for module in self.impact_scorer:
            if isinstance(module, nn.Linear):
                torch.nn.init.xavier_uniform_(module.weight)
                torch.nn.init.zeros_(module.bias)

    def forward(
        self,
        sequence_features: torch.Tensor,  # (batch, seq_len, d_model)
        event_features: torch.Tensor,    # (batch, seq_len, max_events, event_dim)
        event_mask: torch.Tensor,        # (batch, seq_len, max_events)
        timestamps: Optional[torch.Tensor] = None
    ) -> Dict[str, torch.Tensor]:
        """
        Integrate events with sequence features.

        Returns:
            Dictionary containing:
            - enhanced_features: Enhanced sequence features
            - attention_weights: Event attention weights
            - event_impact_scores: Per-timestep event impact scores
        """
        # Encode events
        encoded_events = self.event_encoder(event_features)

        # Apply temporal event attention
        attended_features, attention_weights = self.temporal_attention(
            sequence_features, encoded_events, event_mask, timestamps
        )

        # Apply gated integration
        enhanced_features = self.event_gate(sequence_features, attended_features)

        # Compute event impact scores for interpretability
        event_impact_scores = self.impact_scorer(enhanced_features).squeeze(-1)

        return {
            'enhanced_features': enhanced_features,
            'attention_weights': attention_weights,
            'event_impact_scores': event_impact_scores,
            'encoded_events': encoded_events
        }

    def get_event_importance(
        self,
        attention_weights: torch.Tensor,  # (batch, seq_len, max_events)
        event_mask: torch.Tensor         # (batch, seq_len, max_events)
    ) -> torch.Tensor:
        """
        Compute overall event importance scores.

        Returns:
            Event importance scores (batch, seq_len)
        """
        # Weighted sum of attention weights
        masked_attention = attention_weights * event_mask
        importance_scores = masked_attention.sum(dim=-1)

        return importance_scores


def create_event_integration_layer(
    d_model: int = 64,
    max_events_per_step: int = 5,
    **kwargs
) -> EventIntegrationLayer:
    """
    Convenience function to create event integration layer.

    Args:
        d_model: Model hidden dimension
        max_events_per_step: Maximum events per timestep
        **kwargs: Additional arguments for EventIntegrationLayer

    Returns:
        Configured EventIntegrationLayer
    """
    return EventIntegrationLayer(
        d_model=d_model,
        max_events_per_step=max_events_per_step,
        **kwargs
    )


# Example usage
if __name__ == "__main__":
    # Example usage of event integration layer
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    # Create sample data
    batch_size = 2
    seq_len = 60  # 1 hour of minute data
    d_model = 64
    max_events = 3

    # Sample sequence features
    sequence_features = torch.randn(batch_size, seq_len, d_model).to(device)

    # Sample event features
    event_features = torch.randn(batch_size, seq_len, max_events, 4).to(device)
    event_mask = torch.randint(0, 2, (batch_size, seq_len, max_events)).float().to(device)

    # Create event integration layer
    event_layer = create_event_integration_layer(d_model=d_model).to(device)

    # Apply event integration
    results = event_layer(sequence_features, event_features, event_mask)

    print(f"Enhanced features shape: {results['enhanced_features'].shape}")
    print(f"Attention weights shape: {results['attention_weights'].shape}")
    print(f"Event impact scores shape: {results['event_impact_scores'].shape}")

    # Get event importance
    importance = event_layer.get_event_importance(
        results['attention_weights'], event_mask
    )
    print(f"Event importance shape: {importance.shape}")