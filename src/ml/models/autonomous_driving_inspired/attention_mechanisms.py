"""
Autonomous Driving Inspired Attention Mechanisms for Financial Transformers

This module implements multi-scale attention mechanisms inspired by cutting-edge
autonomous driving models like DriveTransformer, BEVFormer, and others.

Key components:
- Sensor Cross-Attention (timeframe fusion)
- Temporal Cross-Attention (historical context)
- Task Self-Attention (multi-task interaction)
- Multi-Scale Attention Layer (unified processing)
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, Tuple, Optional, Any
import math
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AttentionConfig:
    """Configuration for attention mechanisms."""
    d_model: int = 256
    num_heads: int = 8
    dropout: float = 0.1
    temperature: float = 1.0
    max_seq_length: int = 1000


class SensorCrossAttention(nn.Module):
    """
    Sensor Cross-Attention inspired by DriveTransformer.

    In autonomous driving, this allows task queries to attend to different sensor inputs.
    In finance, this allows prediction tasks to attend to different timeframes.

    Each task query (price, volatility, volume, etc.) can attend to all timeframe
    "sensors" (5m, 15m, 1h, 1d, 1w) to extract relevant multi-scale information.
    """

    def __init__(self, config: AttentionConfig):
        super().__init__()
        self.d_model = config.d_model
        self.num_heads = config.num_heads
        self.head_dim = config.d_model // config.num_heads
        self.temperature = config.temperature
        self.dropout = config.dropout

        assert config.d_model % config.num_heads == 0, "d_model must be divisible by num_heads"

        # Query projection for task queries
        self.query_proj = nn.Linear(config.d_model, config.d_model)

        # Key and value projections for sensor inputs
        self.key_proj = nn.Linear(config.d_model, config.d_model)
        self.value_proj = nn.Linear(config.d_model, config.d_model)

        # Output projection
        self.output_proj = nn.Linear(config.d_model, config.d_model)

        # Dropout
        self.dropout_layer = nn.Dropout(config.dropout)

        # Layer normalization
        self.layer_norm = nn.LayerNorm(config.d_model)

    def forward(self, task_queries: torch.Tensor,
                sensor_features: Dict[str, torch.Tensor],
                sensor_masks: Optional[Dict[str, torch.Tensor]] = None) -> Tuple[torch.Tensor, Dict[str, torch.Tensor]]:
        """
        Args:
            task_queries: [batch, num_tasks, d_model] - Task query vectors
            sensor_features: Dict mapping timeframe -> [batch, seq_len, d_model]
            sensor_masks: Optional attention masks for each sensor

        Returns:
            attended_features: [batch, num_tasks, d_model] - Task features after sensor attention
            attention_weights: Dict mapping timeframe -> [batch, num_heads, num_tasks, seq_len]
        """
        batch_size, num_tasks, _ = task_queries.shape

        # Project task queries
        queries = self.query_proj(task_queries)  # [batch, num_tasks, d_model]
        queries = queries.view(batch_size, num_tasks, self.num_heads, self.head_dim)
        queries = queries.transpose(1, 2)  # [batch, num_heads, num_tasks, head_dim]

        attended_features_per_sensor = []
        attention_weights = {}

        # Attend to each sensor (timeframe)
        for sensor_name, sensor_data in sensor_features.items():
            seq_len = sensor_data.shape[1]

            # Project keys and values
            keys = self.key_proj(sensor_data)  # [batch, seq_len, d_model]
            values = self.value_proj(sensor_data)  # [batch, seq_len, d_model]

            # Reshape for multi-head attention
            keys = keys.view(batch_size, seq_len, self.num_heads, self.head_dim)
            keys = keys.transpose(1, 2)  # [batch, num_heads, seq_len, head_dim]

            values = values.view(batch_size, seq_len, self.num_heads, self.head_dim)
            values = values.transpose(1, 2)  # [batch, num_heads, seq_len, head_dim]

            # Compute attention scores
            scores = torch.matmul(queries, keys.transpose(-2, -1)) / (math.sqrt(self.head_dim) * self.temperature)
            # scores: [batch, num_heads, num_tasks, seq_len]

            # Apply mask if provided
            if sensor_masks and sensor_name in sensor_masks:
                mask = sensor_masks[sensor_name].unsqueeze(1).unsqueeze(1)  # [batch, 1, 1, seq_len]
                scores = scores.masked_fill(~mask, float('-inf'))

            # Softmax attention weights
            attn_weights = F.softmax(scores, dim=-1)
            attn_weights = self.dropout_layer(attn_weights)

            attention_weights[sensor_name] = attn_weights

            # Apply attention to values
            attended = torch.matmul(attn_weights, values)  # [batch, num_heads, num_tasks, head_dim]
            attended = attended.transpose(1, 2)  # [batch, num_tasks, num_heads, head_dim]
            attended = attended.reshape(batch_size, num_tasks, self.d_model)

            attended_features_per_sensor.append(attended)

        # Combine attended features from all sensors
        if attended_features_per_sensor:
            # Simple averaging - could use more sophisticated fusion
            combined_attended = torch.stack(attended_features_per_sensor, dim=0).mean(dim=0)
        else:
            combined_attended = torch.zeros_like(task_queries)

        # Output projection and residual connection
        output = self.output_proj(combined_attended)
        output = self.layer_norm(output + task_queries)

        return output, attention_weights


class TemporalCrossAttention(nn.Module):
    """
    Temporal Cross-Attention with FIFO memory bank, inspired by DriveTransformer.

    Maintains a First-In-First-Out queue of historical market states and allows
    current task queries to attend to this temporal context for better predictions.
    """

    def __init__(self, config: AttentionConfig, memory_size: int = 100):
        super().__init__()
        self.config = config
        self.memory_size = memory_size
        self.d_model = config.d_model
        self.num_heads = config.num_heads
        self.head_dim = config.d_model // config.num_heads

        # Memory bank (FIFO queue) for historical states
        self.memory_bank = None  # Will be initialized on first use

        # Attention projections
        self.query_proj = nn.Linear(config.d_model, config.d_model)
        self.key_proj = nn.Linear(config.d_model, config.d_model)
        self.value_proj = nn.Linear(config.d_model, config.d_model)
        self.output_proj = nn.Linear(config.d_model, config.d_model)

        # Relative time embedding
        self.time_embeddings = nn.Embedding(memory_size, config.d_model)

        # Layer norm and dropout
        self.layer_norm = nn.LayerNorm(config.d_model)
        self.dropout = nn.Dropout(config.dropout)

    def update_memory(self, current_state: torch.Tensor):
        """
        Update FIFO memory bank with current market state.

        Args:
            current_state: [batch, d_model] - Current market state representation
        """
        batch_size = current_state.shape[0]

        if self.memory_bank is None:
            # Initialize memory bank
            self.memory_bank = torch.zeros(batch_size, self.memory_size, self.d_model,
                                         device=current_state.device, dtype=current_state.dtype)
            self.memory_timestamps = torch.zeros(batch_size, self.memory_size, dtype=torch.long,
                                               device=current_state.device)
            self.current_time = 0

        # Shift memory (FIFO)
        self.memory_bank[:, :-1, :] = self.memory_bank[:, 1:, :].clone()
        self.memory_bank[:, -1, :] = current_state

        # Update timestamps
        self.memory_timestamps[:, :-1] = self.memory_timestamps[:, 1:].clone()
        self.memory_timestamps[:, -1] = self.current_time
        self.current_time += 1

    def forward(self, task_queries: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Args:
            task_queries: [batch, num_tasks, d_model] - Current task queries

        Returns:
            temporal_attended: [batch, num_tasks, d_model] - Temporally attended features
            temporal_attention_weights: [batch, num_heads, num_tasks, memory_size]
        """
        if self.memory_bank is None or self.memory_bank.sum() == 0:
            # No temporal context available yet
            return task_queries, None

        batch_size, num_tasks, _ = task_queries.shape

        # Project queries
        queries = self.query_proj(task_queries)
        queries = queries.view(batch_size, num_tasks, self.num_heads, self.head_dim)
        queries = queries.transpose(1, 2)  # [batch, num_heads, num_tasks, head_dim]

        # Add relative time embeddings to memory
        time_embeds = self.time_embeddings(torch.arange(self.memory_size, device=task_queries.device))
        memory_with_time = self.memory_bank + time_embeds.unsqueeze(0)

        # Project memory keys and values
        keys = self.key_proj(memory_with_time)
        values = self.value_proj(memory_with_time)

        keys = keys.view(batch_size, self.memory_size, self.num_heads, self.head_dim)
        keys = keys.transpose(1, 2)  # [batch, num_heads, memory_size, head_dim]

        values = values.view(batch_size, self.memory_size, self.num_heads, self.head_dim)
        values = values.transpose(1, 2)  # [batch, num_heads, memory_size, head_dim]

        # Compute temporal attention
        scores = torch.matmul(queries, keys.transpose(-2, -1)) / math.sqrt(self.head_dim)
        temporal_attention_weights = F.softmax(scores, dim=-1)
        temporal_attention_weights = self.dropout(temporal_attention_weights)

        # Apply attention to values
        temporal_attended = torch.matmul(temporal_attention_weights, values)
        temporal_attended = temporal_attended.transpose(1, 2)  # [batch, num_tasks, num_heads, head_dim]
        temporal_attended = temporal_attended.reshape(batch_size, num_tasks, self.d_model)

        # Output projection and residual
        output = self.output_proj(temporal_attended)
        output = self.layer_norm(output + task_queries)

        return output, temporal_attention_weights


class TaskSelfAttention(nn.Module):
    """
    Task Self-Attention inspired by DriveTransformer.

    Allows different financial prediction tasks (price, volatility, volume, regime, risk)
    to interact with each other through self-attention. This enables tasks to share
    information and make more informed predictions.
    """

    def __init__(self, config: AttentionConfig):
        super().__init__()
        self.d_model = config.d_model
        self.num_heads = config.num_heads
        self.head_dim = config.d_model // config.num_heads

        # Standard self-attention projections
        self.query_proj = nn.Linear(config.d_model, config.d_model)
        self.key_proj = nn.Linear(config.d_model, config.d_model)
        self.value_proj = nn.Linear(config.d_model, config.d_model)
        self.output_proj = nn.Linear(config.d_model, config.d_model)

        # Layer norm and dropout
        self.layer_norm = nn.LayerNorm(config.d_model)
        self.dropout = nn.Dropout(config.dropout)

    def forward(self, task_queries: torch.Tensor,
                task_mask: Optional[torch.Tensor] = None) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Args:
            task_queries: [batch, num_tasks, d_model] - Task query vectors
            task_mask: [batch, num_tasks] - Optional mask for tasks

        Returns:
            attended_tasks: [batch, num_tasks, d_model] - Self-attended task features
            attention_weights: [batch, num_heads, num_tasks, num_tasks]
        """
        batch_size, num_tasks, _ = task_queries.shape

        # Project queries, keys, and values
        queries = self.query_proj(task_queries)
        keys = self.key_proj(task_queries)
        values = self.value_proj(task_queries)

        # Reshape for multi-head attention
        queries = queries.view(batch_size, num_tasks, self.num_heads, self.head_dim)
        queries = queries.transpose(1, 2)  # [batch, num_heads, num_tasks, head_dim]

        keys = keys.view(batch_size, num_tasks, self.num_heads, self.head_dim)
        keys = keys.transpose(1, 2)  # [batch, num_heads, num_tasks, head_dim]

        values = values.view(batch_size, num_tasks, self.num_heads, self.head_dim)
        values = values.transpose(1, 2)  # [batch, num_heads, num_tasks, head_dim]

        # Compute attention scores
        scores = torch.matmul(queries, keys.transpose(-2, -1)) / math.sqrt(self.head_dim)

        # Apply mask if provided
        if task_mask is not None:
            mask = task_mask.unsqueeze(1).unsqueeze(1)  # [batch, 1, 1, num_tasks]
            scores = scores.masked_fill(~mask, float('-inf'))

        # Softmax attention weights
        attention_weights = F.softmax(scores, dim=-1)
        attention_weights = self.dropout(attention_weights)

        # Apply attention to values
        attended = torch.matmul(attention_weights, values)  # [batch, num_heads, num_tasks, head_dim]
        attended = attended.transpose(1, 2)  # [batch, num_tasks, num_heads, head_dim]
        attended = attended.reshape(batch_size, num_tasks, self.d_model)

        # Output projection and residual connection
        output = self.output_proj(attended)
        output = self.layer_norm(output + task_queries)

        return output, attention_weights


class MultiScaleAttentionLayer(nn.Module):
    """
    Unified Multi-Scale Attention Layer combining all three attention mechanisms.

    This is the core building block of our autonomous driving inspired transformer,
    combining:
    1. Task Self-Attention (tasks interact with each other)
    2. Sensor Cross-Attention (tasks attend to timeframes)
    3. Temporal Cross-Attention (tasks attend to historical context)
    """

    def __init__(self, config: AttentionConfig, memory_size: int = 100):
        super().__init__()
        self.config = config

        # Three attention mechanisms
        self.task_self_attention = TaskSelfAttention(config)
        self.sensor_cross_attention = SensorCrossAttention(config)
        self.temporal_cross_attention = TemporalCrossAttention(config, memory_size)

        # Feed-forward network
        self.ffn = nn.Sequential(
            nn.Linear(config.d_model, config.d_model * 4),
            nn.GELU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.d_model * 4, config.d_model),
            nn.Dropout(config.dropout)
        )

        # Layer normalization
        self.ffn_layer_norm = nn.LayerNorm(config.d_model)

    def forward(self, task_queries: torch.Tensor,
                sensor_features: Dict[str, torch.Tensor],
                sensor_masks: Optional[Dict[str, torch.Tensor]] = None,
                task_mask: Optional[torch.Tensor] = None,
                update_memory: bool = True) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """
        Args:
            task_queries: [batch, num_tasks, d_model] - Task query vectors
            sensor_features: Dict mapping timeframe -> [batch, seq_len, d_model]
            sensor_masks: Optional attention masks for sensors
            task_mask: Optional mask for tasks
            update_memory: Whether to update temporal memory bank

        Returns:
            output_queries: [batch, num_tasks, d_model] - Updated task queries
            attention_info: Dict containing attention weights and metadata
        """
        attention_info = {}

        # 1. Task Self-Attention - tasks interact with each other
        task_attended, task_attention_weights = self.task_self_attention(task_queries, task_mask)
        attention_info['task_self_attention'] = task_attention_weights

        # 2. Sensor Cross-Attention - tasks attend to timeframes
        sensor_attended, sensor_attention_weights = self.sensor_cross_attention(
            task_attended, sensor_features, sensor_masks
        )
        attention_info['sensor_cross_attention'] = sensor_attention_weights

        # 3. Temporal Cross-Attention - tasks attend to historical context
        temporal_attended, temporal_attention_weights = self.temporal_cross_attention(sensor_attended)
        attention_info['temporal_cross_attention'] = temporal_attention_weights

        # Update temporal memory bank with current state
        if update_memory and sensor_features:
            # Use mean of all sensor features as current market state
            # Handle different sequence lengths by averaging each timeframe first, then combining
            timeframe_states = []
            for tf_features in sensor_features.values():
                tf_state = tf_features.mean(dim=1)  # [batch, d_model] - average over sequence length
                timeframe_states.append(tf_state)

            current_state = torch.stack(timeframe_states).mean(dim=0)  # [batch, d_model]
            self.temporal_cross_attention.update_memory(current_state)

        # 4. Feed-forward network
        ffn_output = self.ffn(temporal_attended)
        output_queries = self.ffn_layer_norm(ffn_output + temporal_attended)

        return output_queries, attention_info


if __name__ == "__main__":
    # Test attention mechanisms
    logging.basicConfig(level=logging.INFO)

    # Create test data
    batch_size, num_tasks, d_model = 4, 5, 256
    config = AttentionConfig(d_model=d_model, num_heads=8)

    # Test task queries
    task_queries = torch.randn(batch_size, num_tasks, d_model)

    # Test sensor features (multi-timeframe)
    sensor_features = {
        '5m': torch.randn(batch_size, 52, d_model),
        '15m': torch.randn(batch_size, 52, d_model),
        '1h': torch.randn(batch_size, 24, d_model),
        '1d': torch.randn(batch_size, 20, d_model),
        '1w': torch.randn(batch_size, 12, d_model)
    }

    print("Testing Multi-Scale Attention Mechanisms...")

    # Test unified attention layer
    attention_layer = MultiScaleAttentionLayer(config)
    output_queries, attention_info = attention_layer(task_queries, sensor_features)

    print(f"Input task queries shape: {task_queries.shape}")
    print(f"Output task queries shape: {output_queries.shape}")
    print(f"Attention info keys: {list(attention_info.keys())}")

    # Test attention weights shapes
    if attention_info['task_self_attention'] is not None:
        print(f"Task self-attention weights shape: {attention_info['task_self_attention'].shape}")

    if attention_info['sensor_cross_attention']:
        for sensor_name, weights in attention_info['sensor_cross_attention'].items():
            print(f"Sensor cross-attention ({sensor_name}) weights shape: {weights.shape}")

    if attention_info['temporal_cross_attention'] is not None:
        print(f"Temporal cross-attention weights shape: {attention_info['temporal_cross_attention'].shape}")

    print("\n✅ Attention mechanisms test completed successfully!")