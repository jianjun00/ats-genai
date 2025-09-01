#!/usr/bin/env python3
"""
Cross-Scale Attention Mechanism for Multi-Temporal Financial Modeling

Implements hierarchical attention across multiple temporal scales (minute, hourly, daily)
following 2024-2025 research on multi-scale transformers and temporal fusion.

Key Features:
- Multi-scale temporal attention
- Hierarchical positional encoding
- Scale-specific feature processing
- Attention visualization capabilities
- Memory-efficient implementation
- Interpretable cross-scale patterns
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import pandas as pd
import math
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
import logging
from enum import Enum

from ...storage.multi_scale_sequence import TimeScale

logger = logging.getLogger(__name__)


@dataclass
class AttentionConfig:
    """Configuration for cross-scale attention."""
    d_model: int = 64
    n_heads: int = 4
    n_scales: int = 3
    dropout: float = 0.1
    temperature: float = 1.0
    use_relative_position: bool = True
    max_relative_position: int = 512
    attention_window: Optional[int] = None  # Local attention window


class HierarchicalPositionalEncoding(nn.Module):
    """
    Positional encoding that handles multiple time scales.
    
    Creates scale-specific positional encodings that capture the
    hierarchical nature of temporal relationships.
    """
    
    def __init__(
        self,
        d_model: int,
        max_len: int = 10000,
        scales: List[TimeScale] = None
    ):
        super().__init__()
        self.d_model = d_model
        self.max_len = max_len
        self.scales = scales or [TimeScale.MINUTE, TimeScale.HOURLY, TimeScale.DAILY]
        
        # Create scale-specific positional encodings
        self.scale_encodings = nn.ModuleDict()
        
        for scale in self.scales:
            encoding = self._create_positional_encoding(max_len, d_model, scale)
            self.scale_encodings[scale.value] = nn.Parameter(encoding, requires_grad=False)
        
        # Learnable scale mixing weights
        self.scale_mixing = nn.Parameter(torch.ones(len(self.scales)) / len(self.scales))
        
    def _create_positional_encoding(
        self, 
        max_len: int, 
        d_model: int, 
        scale: TimeScale
    ) -> torch.Tensor:
        """Create positional encoding for specific time scale."""
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        
        # Scale-specific frequency adjustment
        scale_factor = self._get_scale_factor(scale)
        
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * 
                            (-math.log(10000.0 * scale_factor) / d_model))
        
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        
        return pe
    
    def _get_scale_factor(self, scale: TimeScale) -> float:
        """Get frequency scaling factor for different time scales."""
        scale_factors = {
            TimeScale.MINUTE: 1.0,
            TimeScale.HOURLY: 60.0,
            TimeScale.DAILY: 1440.0,
            TimeScale.WEEKLY: 10080.0
        }
        return scale_factors.get(scale, 1.0)
    
    def forward(
        self, 
        x: torch.Tensor, 
        scale: TimeScale,
        positions: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        """
        Add hierarchical positional encoding.
        
        Args:
            x: Input tensor (batch, seq_len, d_model)
            scale: Primary time scale
            positions: Optional explicit positions
        
        Returns:
            Tensor with positional encoding added
        """
        batch_size, seq_len, _ = x.shape
        
        if positions is not None:
            # Use explicit positions
            positions = positions.clamp(0, self.max_len - 1)
            pe = self.scale_encodings[scale.value][positions]
        else:
            # Use sequence positions
            pe = self.scale_encodings[scale.value][:seq_len].unsqueeze(0).expand(batch_size, -1, -1)
        
        return x + pe


class RelativePositionBias(nn.Module):
    """Relative position bias for local attention patterns."""
    
    def __init__(
        self,
        num_heads: int,
        max_relative_position: int = 512
    ):
        super().__init__()
        self.num_heads = num_heads
        self.max_relative_position = max_relative_position
        
        # Relative position embeddings
        self.relative_position_bias_table = nn.Parameter(
            torch.zeros(2 * max_relative_position - 1, num_heads)
        )
        
        # Initialize bias table
        nn.init.trunc_normal_(self.relative_position_bias_table, std=0.02)
    
    def forward(self, seq_len: int) -> torch.Tensor:
        """
        Compute relative position bias.
        
        Args:
            seq_len: Sequence length
        
        Returns:
            Relative position bias (num_heads, seq_len, seq_len)
        """
        # Create relative position indices
        coords = torch.arange(seq_len)
        relative_coords = coords[:, None] - coords[None, :]  # (seq_len, seq_len)
        
        # Clamp to valid range
        relative_coords = relative_coords.clamp(
            -self.max_relative_position + 1,
            self.max_relative_position - 1
        )
        
        # Shift to make indices positive
        relative_coords += self.max_relative_position - 1
        
        # Get bias values
        relative_position_bias = self.relative_position_bias_table[relative_coords]  # (seq_len, seq_len, num_heads)
        relative_position_bias = relative_position_bias.permute(2, 0, 1)  # (num_heads, seq_len, seq_len)
        
        return relative_position_bias


class ScaleSpecificAttention(nn.Module):
    """Attention mechanism for a specific time scale."""
    
    def __init__(
        self,
        d_model: int,
        num_heads: int,
        dropout: float = 0.1,
        attention_window: Optional[int] = None,
        use_relative_position: bool = True
    ):
        super().__init__()
        self.d_model = d_model
        self.num_heads = num_heads
        self.head_dim = d_model // num_heads
        self.attention_window = attention_window
        self.scale = 1.0 / math.sqrt(self.head_dim)
        
        assert d_model % num_heads == 0, "d_model must be divisible by num_heads"
        
        # Linear projections
        self.q_proj = nn.Linear(d_model, d_model, bias=False)
        self.k_proj = nn.Linear(d_model, d_model, bias=False)
        self.v_proj = nn.Linear(d_model, d_model, bias=False)
        self.out_proj = nn.Linear(d_model, d_model)
        
        # Relative position bias
        if use_relative_position:
            self.relative_position_bias = RelativePositionBias(num_heads)
        else:
            self.relative_position_bias = None
        
        self.dropout = nn.Dropout(dropout)
        self.layer_norm = nn.LayerNorm(d_model)
        
        self._init_weights()
    
    def _init_weights(self):
        """Initialize weights."""
        for module in [self.q_proj, self.k_proj, self.v_proj, self.out_proj]:
            nn.init.xavier_uniform_(module.weight)
            if module.bias is not None:
                nn.init.zeros_(module.bias)
    
    def forward(
        self,
        query: torch.Tensor,
        key: torch.Tensor,
        value: torch.Tensor,
        attention_mask: Optional[torch.Tensor] = None,
        return_attention: bool = False
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]:
        """
        Apply scale-specific attention.
        
        Args:
            query: Query tensor (batch, seq_len, d_model)
            key: Key tensor (batch, key_len, d_model)
            value: Value tensor (batch, key_len, d_model)
            attention_mask: Optional attention mask
            return_attention: Whether to return attention weights
        
        Returns:
            Attention output and optionally attention weights
        """
        batch_size, seq_len, _ = query.shape
        key_len = key.shape[1]
        
        # Linear projections and reshape for multi-head attention
        Q = self.q_proj(query).view(batch_size, seq_len, self.num_heads, self.head_dim).transpose(1, 2)
        K = self.k_proj(key).view(batch_size, key_len, self.num_heads, self.head_dim).transpose(1, 2)
        V = self.v_proj(value).view(batch_size, key_len, self.num_heads, self.head_dim).transpose(1, 2)
        
        # Compute attention scores
        attention_scores = torch.matmul(Q, K.transpose(-2, -1)) * self.scale
        
        # Add relative position bias
        if self.relative_position_bias is not None and seq_len == key_len:
            relative_bias = self.relative_position_bias(seq_len)
            attention_scores += relative_bias.unsqueeze(0)  # Add batch dimension
        
        # Apply attention window (local attention)
        if self.attention_window is not None and seq_len == key_len:
            attention_scores = self._apply_local_attention(attention_scores, self.attention_window)
        
        # Apply attention mask
        if attention_mask is not None:
            attention_scores = attention_scores.masked_fill(attention_mask == 0, -1e9)
        
        # Compute attention weights
        attention_weights = F.softmax(attention_scores, dim=-1)
        attention_weights = self.dropout(attention_weights)
        
        # Apply attention to values
        attention_output = torch.matmul(attention_weights, V)
        
        # Concatenate heads
        attention_output = attention_output.transpose(1, 2).contiguous().view(
            batch_size, seq_len, self.d_model
        )
        
        # Output projection
        output = self.out_proj(attention_output)
        
        # Residual connection and layer norm
        output = self.layer_norm(output + query)
        
        if return_attention:
            # Average attention weights across heads
            avg_attention = attention_weights.mean(dim=1)
            return output, avg_attention
        
        return output
    
    def _apply_local_attention(
        self, 
        attention_scores: torch.Tensor, 
        window_size: int
    ) -> torch.Tensor:
        """Apply local attention window."""
        seq_len = attention_scores.shape[-1]
        
        # Create local attention mask
        mask = torch.zeros_like(attention_scores)
        
        for i in range(seq_len):
            start = max(0, i - window_size // 2)
            end = min(seq_len, i + window_size // 2 + 1)
            mask[:, :, i, start:end] = 1
        
        attention_scores = attention_scores.masked_fill(mask == 0, -1e9)
        
        return attention_scores


class CrossScaleFusion(nn.Module):
    """Fusion mechanism for combining cross-scale attention outputs."""
    
    def __init__(
        self,
        d_model: int,
        num_scales: int,
        fusion_method: str = 'learned_weighted'
    ):
        super().__init__()
        self.d_model = d_model
        self.num_scales = num_scales
        self.fusion_method = fusion_method
        
        if fusion_method == 'learned_weighted':
            # Learnable fusion weights
            self.fusion_weights = nn.Parameter(torch.ones(num_scales) / num_scales)
            self.fusion_norm = nn.LayerNorm(d_model)
            
        elif fusion_method == 'attention_based':
            # Attention-based fusion
            self.fusion_attention = nn.MultiheadAttention(d_model, 4, batch_first=True)
            self.fusion_norm = nn.LayerNorm(d_model)
            
        elif fusion_method == 'gated':
            # Gated fusion
            self.gate_networks = nn.ModuleList([
                nn.Sequential(
                    nn.Linear(d_model, d_model // 2),
                    nn.ReLU(),
                    nn.Linear(d_model // 2, 1),
                    nn.Sigmoid()
                )
                for _ in range(num_scales)
            ])
            self.fusion_norm = nn.LayerNorm(d_model)
        
        self._init_weights()
    
    def _init_weights(self):
        """Initialize weights."""
        if hasattr(self, 'gate_networks'):
            for gate in self.gate_networks:
                for layer in gate:
                    if isinstance(layer, nn.Linear):
                        nn.init.xavier_uniform_(layer.weight)
                        nn.init.zeros_(layer.bias)
    
    def forward(self, scale_outputs: List[torch.Tensor]) -> torch.Tensor:
        """
        Fuse outputs from different scales.
        
        Args:
            scale_outputs: List of tensors from different scales
        
        Returns:
            Fused output tensor
        """
        if not scale_outputs:
            raise ValueError("No scale outputs provided")
        
        if len(scale_outputs) == 1:
            return self.fusion_norm(scale_outputs[0])
        
        if self.fusion_method == 'learned_weighted':
            # Learned weighted combination
            weights = F.softmax(self.fusion_weights[:len(scale_outputs)], dim=0)
            fused = sum(w * output for w, output in zip(weights, scale_outputs))
            
        elif self.fusion_method == 'attention_based':
            # Stack outputs for attention-based fusion
            stacked = torch.stack(scale_outputs, dim=-2)  # (..., num_scales, d_model)
            batch_shape = stacked.shape[:-2]
            
            # Flatten for attention
            flat_stacked = stacked.view(-1, len(scale_outputs), self.d_model)
            
            # Self-attention across scales
            attended, _ = self.fusion_attention(flat_stacked, flat_stacked, flat_stacked)
            
            # Average across scales
            fused = attended.mean(dim=-2)  # Average across scale dimension
            
            # Reshape back
            fused = fused.view(*batch_shape, self.d_model)
            
        elif self.fusion_method == 'gated':
            # Gated fusion
            gated_outputs = []
            
            for i, output in enumerate(scale_outputs):
                gate = self.gate_networks[i](output)
                gated_outputs.append(gate * output)
            
            fused = sum(gated_outputs)
        
        else:
            # Simple averaging fallback
            fused = torch.stack(scale_outputs, dim=0).mean(dim=0)
        
        return self.fusion_norm(fused)


class CrossScaleAttention(nn.Module):
    """
    Complete cross-scale attention mechanism.
    
    Enables models to attend to patterns across different temporal scales
    simultaneously, following hierarchical attention principles.
    """
    
    def __init__(self, config: AttentionConfig):
        super().__init__()
        self.config = config
        self.d_model = config.d_model
        self.num_heads = config.n_heads
        self.num_scales = config.n_scales
        
        # Hierarchical positional encoding
        self.positional_encoding = HierarchicalPositionalEncoding(
            d_model=config.d_model,
            scales=[TimeScale.MINUTE, TimeScale.HOURLY, TimeScale.DAILY]
        )
        
        # Scale-specific attention layers
        self.scale_attentions = nn.ModuleDict()
        for scale in ['minute', 'hourly', 'daily']:
            self.scale_attentions[scale] = ScaleSpecificAttention(
                d_model=config.d_model,
                num_heads=config.n_heads,
                dropout=config.dropout,
                attention_window=config.attention_window,
                use_relative_position=config.use_relative_position
            )
        
        # Cross-scale fusion
        self.fusion_layer = CrossScaleFusion(
            d_model=config.d_model,
            num_scales=config.n_scales,
            fusion_method='learned_weighted'
        )
        
        # Output processing
        self.output_projection = nn.Sequential(
            nn.Linear(config.d_model, config.d_model * 2),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.d_model * 2, config.d_model)
        )
        
        self.final_norm = nn.LayerNorm(config.d_model)
        
        self._init_weights()
    
    def _init_weights(self):
        """Initialize weights."""
        for module in self.output_projection:
            if isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                nn.init.zeros_(module.bias)
    
    def forward(
        self,
        scale_features: Dict[str, torch.Tensor],
        primary_scale: str = 'minute',
        attention_masks: Optional[Dict[str, torch.Tensor]] = None,
        return_attention_weights: bool = False
    ) -> Union[torch.Tensor, Tuple[torch.Tensor, Dict[str, torch.Tensor]]]:
        """
        Apply cross-scale attention.
        
        Args:
            scale_features: Features for each scale
            primary_scale: Primary scale for output alignment
            attention_masks: Optional attention masks for each scale
            return_attention_weights: Whether to return attention weights
        
        Returns:
            Cross-scale attended features and optionally attention weights
        """
        if primary_scale not in scale_features:
            raise ValueError(f"Primary scale '{primary_scale}' not found in scale_features")
        
        # Add positional encodings
        encoded_features = {}
        for scale_name, features in scale_features.items():
            if scale_name in ['minute', 'hourly', 'daily']:
                time_scale = getattr(TimeScale, scale_name.upper())
                encoded_features[scale_name] = self.positional_encoding(features, time_scale)
            else:
                encoded_features[scale_name] = features
        
        # Apply scale-specific attention
        attended_features = {}
        attention_weights = {}
        
        for scale_name, features in encoded_features.items():
            if scale_name in self.scale_attentions:
                mask = attention_masks.get(scale_name) if attention_masks else None
                
                if return_attention_weights:
                    attended, weights = self.scale_attentions[scale_name](
                        features, features, features, mask, return_attention=True
                    )
                    attention_weights[scale_name] = weights
                else:
                    attended = self.scale_attentions[scale_name](
                        features, features, features, mask
                    )
                
                attended_features[scale_name] = attended
        
        # Align all scales to primary scale length
        primary_length = scale_features[primary_scale].shape[1]
        aligned_features = []
        
        for scale_name in ['minute', 'hourly', 'daily']:
            if scale_name in attended_features:
                features = attended_features[scale_name]
                
                # Align to primary scale length
                if features.shape[1] != primary_length:
                    features = self._align_to_length(features, primary_length)
                
                aligned_features.append(features)
        
        # Cross-scale fusion
        if aligned_features:
            fused_features = self.fusion_layer(aligned_features)
        else:
            # Fallback to primary scale features
            fused_features = encoded_features[primary_scale]
        
        # Output projection
        output = self.output_projection(fused_features)
        output = self.final_norm(output + scale_features[primary_scale])
        
        if return_attention_weights:
            return output, attention_weights
        
        return output
    
    def _align_to_length(self, features: torch.Tensor, target_length: int) -> torch.Tensor:
        """Align features to target sequence length."""
        current_length = features.shape[1]
        
        if current_length == target_length:
            return features
        
        elif current_length < target_length:
            # Interpolate to longer length
            features_transposed = features.transpose(1, 2)  # (batch, d_model, seq_len)
            interpolated = F.interpolate(
                features_transposed, 
                size=target_length, 
                mode='linear', 
                align_corners=False
            )
            return interpolated.transpose(1, 2)  # (batch, seq_len, d_model)
        
        else:
            # Downsample to shorter length
            step = current_length // target_length
            return features[:, ::step, :][:, :target_length, :]
    
    def get_attention_patterns(
        self,
        scale_features: Dict[str, torch.Tensor],
        attention_masks: Optional[Dict[str, torch.Tensor]] = None
    ) -> Dict[str, torch.Tensor]:
        """Get attention patterns for visualization."""
        _, attention_weights = self.forward(
            scale_features, 
            attention_masks=attention_masks,
            return_attention_weights=True
        )
        return attention_weights


def create_cross_scale_attention(
    d_model: int = 64,
    num_heads: int = 4,
    num_scales: int = 3,
    **kwargs
) -> CrossScaleAttention:
    """
    Create cross-scale attention mechanism.
    
    Args:
        d_model: Model dimension
        num_heads: Number of attention heads
        num_scales: Number of temporal scales
        **kwargs: Additional configuration parameters
    
    Returns:
        Configured CrossScaleAttention module
    """
    config = AttentionConfig(
        d_model=d_model,
        n_heads=num_heads,
        n_scales=num_scales,
        **kwargs
    )
    
    return CrossScaleAttention(config)


# Example usage and testing
if __name__ == "__main__":
    # Example usage of cross-scale attention
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    # Create sample multi-scale features
    batch_size = 2
    d_model = 64
    
    scale_features = {
        'minute': torch.randn(batch_size, 120, d_model).to(device),  # 2 hours of minute data
        'hourly': torch.randn(batch_size, 24, d_model).to(device),   # 1 day of hourly data
        'daily': torch.randn(batch_size, 7, d_model).to(device)      # 1 week of daily data
    }
    
    # Create cross-scale attention
    cross_scale_attn = create_cross_scale_attention(d_model=d_model).to(device)
    
    # Apply cross-scale attention
    output = cross_scale_attn(scale_features, primary_scale='minute')
    
    print(f"Output shape: {output.shape}")
    print(f"Expected shape: ({batch_size}, 120, {d_model})")
    
    # Get attention patterns
    attention_patterns = cross_scale_attn.get_attention_patterns(scale_features)
    
    for scale, patterns in attention_patterns.items():
        print(f"{scale} attention shape: {patterns.shape}")