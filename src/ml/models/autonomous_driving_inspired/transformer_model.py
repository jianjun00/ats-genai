"""
Autonomous Driving Inspired Financial Transformer Model

Main transformer architecture combining all components:
- Multi-timeframe encoders (sensor-like processing)
- Task query system (multi-task prediction)
- Temporal memory bank (FIFO historical context)
- Multi-scale attention mechanisms
- Multi-horizon prediction heads

Inspired by DriveTransformer, BEVFormer, and Temporal Fusion Transformer architectures.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Any, Union
import math
import logging
from dataclasses import dataclass
from collections import OrderedDict

from .attention_mechanisms import (
    MultiScaleAttentionLayer, AttentionConfig,
    SensorCrossAttention, TemporalCrossAttention, TaskSelfAttention
)
from .data_preprocessing import (
    MarketPositionEncoder, TimeframeVariableSelector, TimeframeConfig
)

logger = logging.getLogger(__name__)


@dataclass
class TransformerConfig:
    """Configuration for the autonomous driving inspired transformer."""
    # Model dimensions
    d_model: int = 256
    num_heads: int = 8
    num_layers: int = 6
    dropout: float = 0.1

    # Multi-scale attention
    attention_temperature: float = 1.0
    temporal_memory_size: int = 100

    # Task configuration
    num_tasks: int = 5  # price, volatility, volume, regime, risk
    prediction_horizon: int = 10  # 10 hours ahead

    # Timeframe configuration
    timeframe_configs: List[TimeframeConfig] = None

    def __post_init__(self):
        if self.timeframe_configs is None:
            self.timeframe_configs = [
                TimeframeConfig('5m', 52, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 1.0),
                TimeframeConfig('15m', 52, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.8),
                TimeframeConfig('1h', 24, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.6),
                TimeframeConfig('1d', 20, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.4),
                TimeframeConfig('1w', 12, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.2)
            ]


class MultiTimeframeEncoder(nn.Module):
    """
    Multi-timeframe encoder that processes each timeframe like a different sensor modality.

    Similar to autonomous driving systems processing camera, LiDAR, radar inputs,
    this processes 5m, 15m, 1h, 1d, 1w timeframes independently before fusion.
    """

    def __init__(self, config: TransformerConfig):
        super().__init__()
        self.config = config
        self.d_model = config.d_model

        # Individual encoders for each timeframe
        self.timeframe_encoders = nn.ModuleDict()
        self.variable_selectors = nn.ModuleDict()

        for tf_config in config.timeframe_configs:
            tf_name = tf_config.name
            input_features = len(tf_config.features)

            # Feature embedding for this timeframe
            self.timeframe_encoders[tf_name] = nn.Sequential(
                nn.Linear(input_features, config.d_model // 2),
                nn.LayerNorm(config.d_model // 2),
                nn.ReLU(),
                nn.Dropout(config.dropout),
                nn.Linear(config.d_model // 2, config.d_model),
                nn.LayerNorm(config.d_model)
            )

            # Variable selector for intelligent feature selection
            self.variable_selectors[tf_name] = TimeframeVariableSelector(
                input_features, config.d_model // 4
            )

        # Position encoder for autonomous driving style position encoding
        self.position_encoder = MarketPositionEncoder(config.d_model)

        # Timeframe importance weights
        importance_weights = [tf_config.importance_weight for tf_config in config.timeframe_configs]
        self.register_buffer('importance_weights', torch.tensor(importance_weights))

    def forward(self, timeframe_sequences: Dict[str, torch.Tensor],
                position_data: Dict[str, Dict[str, torch.Tensor]]) -> Dict[str, torch.Tensor]:
        """
        Args:
            timeframe_sequences: Dict mapping timeframe -> [batch, seq_len, features]
            position_data: Dict mapping timeframe -> position encoding components

        Returns:
            encoded_timeframes: Dict mapping timeframe -> [batch, seq_len, d_model]
        """
        encoded_timeframes = {}

        for tf_name, sequences in timeframe_sequences.items():
            if tf_name not in self.timeframe_encoders:
                logger.warning(f"No encoder found for timeframe {tf_name}")
                continue

            batch_size, seq_len, _ = sequences.shape

            # Apply variable selection
            selected_features, importance_weights = self.variable_selectors[tf_name](sequences)

            # Encode features
            encoded = self.timeframe_encoders[tf_name](selected_features)

            # Add position encoding if available
            if tf_name in position_data:
                pos_data = position_data[tf_name]
                position_encoding = self.position_encoder(
                    pos_data['timeframe_ids'],
                    pos_data['bar_indices'],
                    pos_data['temporal_offsets'],
                    pos_data['market_regimes']
                )
                encoded = encoded + position_encoding

            encoded_timeframes[tf_name] = encoded

            logger.debug(f"Encoded {tf_name}: {sequences.shape} -> {encoded.shape}")

        return encoded_timeframes


class TaskQuerySystem(nn.Module):
    """
    Task Query System inspired by DriveTransformer.

    Maintains learnable query vectors for different financial prediction tasks:
    - Price movement prediction
    - Volatility forecasting
    - Volume pattern analysis
    - Market regime detection
    - Risk assessment
    """

    def __init__(self, config: TransformerConfig):
        super().__init__()
        self.config = config
        self.d_model = config.d_model
        self.num_tasks = config.num_tasks

        # Learnable task query embeddings
        self.task_queries = nn.Parameter(torch.randn(config.num_tasks, config.d_model))

        # Task type embeddings
        self.task_type_embeddings = nn.Embedding(config.num_tasks, config.d_model)

        # Task names for interpretability
        self.task_names = ['price_movement', 'volatility', 'volume_profile', 'regime_change', 'risk_assessment']

        # Initialize task queries with Xavier initialization
        nn.init.xavier_uniform_(self.task_queries)

    def forward(self, batch_size: int) -> torch.Tensor:
        """
        Args:
            batch_size: Batch size for expanding queries

        Returns:
            task_queries: [batch, num_tasks, d_model] - Task query vectors
        """
        # Expand task queries for batch
        task_queries = self.task_queries.unsqueeze(0).expand(batch_size, -1, -1)

        # Add task type embeddings
        task_type_ids = torch.arange(self.num_tasks, device=task_queries.device)
        task_type_embeds = self.task_type_embeddings(task_type_ids).unsqueeze(0)

        task_queries = task_queries + task_type_embeds

        return task_queries


class TemporalMemoryBank(nn.Module):
    """
    FIFO Temporal Memory Bank for maintaining historical market states.

    Similar to autonomous driving systems that maintain temporal context,
    this keeps track of past market states for temporal reasoning.
    """

    def __init__(self, config: TransformerConfig):
        super().__init__()
        self.memory_size = config.temporal_memory_size
        self.d_model = config.d_model

        # Memory will be initialized on first use
        self.register_buffer('memory_initialized', torch.tensor(False))

    def forward(self, current_state: torch.Tensor) -> torch.Tensor:
        """
        Update memory with current state and return historical context.

        Args:
            current_state: [batch, d_model] - Current market state

        Returns:
            memory_context: [batch, memory_size, d_model] - Historical context
        """
        batch_size = current_state.shape[0]

        if not self.memory_initialized:
            # Initialize memory bank
            self.register_buffer('memory_bank',
                               torch.zeros(batch_size, self.memory_size, self.d_model))
            self.register_buffer('memory_initialized', torch.tensor(True))

        # Expand memory bank if batch size changed
        if self.memory_bank.shape[0] != batch_size:
            new_memory = torch.zeros(batch_size, self.memory_size, self.d_model,
                                   device=self.memory_bank.device, dtype=self.memory_bank.dtype)
            # Copy existing memory for common batch elements
            min_batch = min(batch_size, self.memory_bank.shape[0])
            new_memory[:min_batch] = self.memory_bank[:min_batch]
            self.memory_bank = new_memory

        # FIFO update: shift left and add new state
        self.memory_bank[:, :-1, :] = self.memory_bank[:, 1:, :].clone()
        self.memory_bank[:, -1, :] = current_state

        return self.memory_bank.clone()


class MultiHorizonPredictor(nn.Module):
    """
    Multi-horizon prediction heads for forecasting next N hours.

    Enhanced with multi-task heads, uncertainty estimation, and temporal consistency tracking
    for optimal loss function integration.
    """

    def __init__(self, config: TransformerConfig):
        super().__init__()
        self.config = config
        self.num_tasks = config.num_tasks
        self.prediction_horizon = config.prediction_horizon
        self.d_model = config.d_model

        # Enhanced prediction heads for each task
        self.prediction_heads = nn.ModuleDict({
            'price_movement': self._create_regression_head(),
            'volatility': self._create_volatility_head(),
            'volume_profile': self._create_volume_head(),
            'regime_change': self._create_classification_head(4),  # Bull, Bear, Sideways, Transition
            'risk_assessment': self._create_risk_head()
        })

        # NEW: Uncertainty estimation heads for optimal loss function
        self.uncertainty_heads = nn.ModuleDict({
            task: nn.Sequential(
                nn.Linear(self.d_model, self.d_model // 4),
                nn.ReLU(),
                nn.Linear(self.d_model // 4, 1),
                nn.Softplus()  # Ensure positive uncertainty
            ) for task in ['price_movement', 'volatility', 'volume_profile', 'regime_change', 'risk_assessment']
        })

        # Store previous predictions for temporal consistency (unified loss requirement)
        self.register_buffer('previous_predictions', torch.zeros(1, 5, config.prediction_horizon))
        self.register_buffer('predictions_initialized', torch.tensor(False))

    def _create_regression_head(self) -> nn.Module:
        """Create regression head for price movement predictions."""
        return nn.Sequential(
            nn.Linear(self.d_model, self.d_model // 2),
            nn.GELU(),
            nn.Dropout(self.config.dropout),
            nn.Linear(self.d_model // 2, self.prediction_horizon),
            nn.Tanh()  # Bound returns between -1 and 1
        )

    def _create_volatility_head(self) -> nn.Module:
        """Create volatility prediction head with positive output."""
        return nn.Sequential(
            nn.Linear(self.d_model, self.d_model // 2),
            nn.GELU(),
            nn.Dropout(self.config.dropout),
            nn.Linear(self.d_model // 2, self.prediction_horizon),
            nn.Sigmoid()  # Volatility is always positive
        )

    def _create_volume_head(self) -> nn.Module:
        """Create volume profile prediction head."""
        return nn.Sequential(
            nn.Linear(self.d_model, self.d_model // 2),
            nn.GELU(),
            nn.Dropout(self.config.dropout),
            nn.Linear(self.d_model // 2, self.prediction_horizon),
            nn.Softplus()  # Volume is always positive
        )

    def _create_classification_head(self, num_classes: int) -> nn.Module:
        """Create classification head for discrete predictions."""
        return nn.Sequential(
            nn.Linear(self.d_model, self.d_model // 2),
            nn.GELU(),
            nn.Dropout(self.config.dropout),
            nn.Linear(self.d_model // 2, num_classes)  # Single timestep regime classification
        )

    def _create_risk_head(self) -> nn.Module:
        """Create risk assessment head with bounded output."""
        return nn.Sequential(
            nn.Linear(self.d_model, self.d_model // 2),
            nn.GELU(),
            nn.Dropout(self.config.dropout),
            nn.Linear(self.d_model // 2, 1),  # Single risk score
            nn.Sigmoid()  # Risk score between 0 and 1
        )

    def forward(self, task_features: torch.Tensor) -> Dict[str, torch.Tensor]:
        """
        Args:
            task_features: [batch, num_tasks, d_model] - Task representations

        Returns:
            predictions: Dict containing predictions and uncertainties for unified loss
        """
        predictions = {}
        uncertainties = {}
        batch_size = task_features.shape[0]

        task_names = ['price_movement', 'volatility', 'volume_profile', 'regime_change', 'risk_assessment']

        # Initialize previous predictions buffer if needed
        if not self.predictions_initialized:
            self.previous_predictions = torch.zeros(
                batch_size, 5, self.prediction_horizon,
                device=task_features.device, dtype=task_features.dtype
            )
            self.predictions_initialized = True
        elif self.previous_predictions.shape[0] != batch_size:
            # Resize buffer for different batch size
            self.previous_predictions = torch.zeros(
                batch_size, 5, self.prediction_horizon,
                device=task_features.device, dtype=task_features.dtype
            )

        for task_idx, task_name in enumerate(task_names):
            if task_idx >= task_features.shape[1]:
                continue

            task_feature = task_features[:, task_idx, :]  # [batch, d_model]

            if task_name == 'regime_change':
                # Classification output - single timestep
                logits = self.prediction_heads[task_name](task_feature)  # [batch, 4]
                predictions[task_name] = logits  # Raw logits for cross entropy loss
            elif task_name == 'risk_assessment':
                # Single risk score
                pred = self.prediction_heads[task_name](task_feature)  # [batch, 1]
                predictions[task_name] = pred
            else:
                # Multi-horizon regression output
                pred = self.prediction_heads[task_name](task_feature)  # [batch, horizon]
                predictions[task_name] = pred.unsqueeze(-1)  # [batch, horizon, 1]

            # Compute uncertainty estimate for this task
            uncertainty = self.uncertainty_heads[task_name](task_feature)  # [batch, 1]
            uncertainties[f'{task_name}_uncertainty'] = uncertainty

        # Update previous predictions for temporal consistency
        if self.training:
            current_preds = torch.stack([
                predictions.get('price_movement', torch.zeros(batch_size, self.prediction_horizon, 1, device=task_features.device)).squeeze(-1),
                predictions.get('volatility', torch.zeros(batch_size, self.prediction_horizon, 1, device=task_features.device)).squeeze(-1),
                predictions.get('volume_profile', torch.zeros(batch_size, self.prediction_horizon, 1, device=task_features.device)).squeeze(-1),
                predictions.get('regime_change', torch.zeros(batch_size, self.prediction_horizon, device=task_features.device)),
                predictions.get('risk_assessment', torch.zeros(batch_size, self.prediction_horizon, device=task_features.device)).squeeze(-1) if predictions.get('risk_assessment') is not None else torch.zeros(batch_size, self.prediction_horizon, device=task_features.device)
            ], dim=1)  # [batch, 5, horizon]

            # Update previous predictions buffer
            self.previous_predictions = current_preds.detach()

        # Combine predictions and uncertainties
        predictions.update(uncertainties)

        # Add previous predictions for temporal consistency loss
        if hasattr(self, 'previous_predictions') and self.previous_predictions.numel() > 0:
            predictions['previous_predictions'] = {
                'price_movement': self.previous_predictions[:, 0],
                'volatility': self.previous_predictions[:, 1],
                'volume_profile': self.previous_predictions[:, 2],
                'regime_change': self.previous_predictions[:, 3],
                'risk_assessment': self.previous_predictions[:, 4]
            }

        return predictions


class AutonomousFinanceTransformer(nn.Module):
    """
    Main autonomous driving inspired financial transformer model.

    Combines all components into a unified architecture:
    1. Multi-timeframe encoding (sensor processing)
    2. Task query initialization (multi-task setup)
    3. Multi-scale attention layers (unified attention)
    4. Temporal memory management (historical context)
    5. Multi-horizon prediction (forecasting outputs)
    """

    def __init__(self, config: TransformerConfig):
        super().__init__()
        self.config = config

        # Core components
        self.timeframe_encoder = MultiTimeframeEncoder(config)
        self.task_query_system = TaskQuerySystem(config)
        self.temporal_memory_bank = TemporalMemoryBank(config)
        self.multi_horizon_predictor = MultiHorizonPredictor(config)

        # Multi-scale attention layers
        attention_config = AttentionConfig(
            d_model=config.d_model,
            num_heads=config.num_heads,
            dropout=config.dropout,
            temperature=config.attention_temperature
        )

        self.attention_layers = nn.ModuleList([
            MultiScaleAttentionLayer(attention_config, config.temporal_memory_size)
            for _ in range(config.num_layers)
        ])

        # Output layer normalization
        self.output_layer_norm = nn.LayerNorm(config.d_model)

        logger.info(f"Initialized AutonomousFinanceTransformer with {config.num_layers} layers, "
                   f"{config.d_model} dimensions, {config.num_heads} attention heads")

    def forward(self, timeframe_sequences: Dict[str, torch.Tensor],
                position_data: Dict[str, Dict[str, torch.Tensor]],
                return_attention_weights: bool = False) -> Dict[str, Any]:
        """
        Forward pass of the autonomous finance transformer.

        Args:
            timeframe_sequences: Dict mapping timeframe -> [batch, seq_len, features]
            position_data: Dict mapping timeframe -> position encoding components
            return_attention_weights: Whether to return attention weights for visualization

        Returns:
            output: Dict containing:
                - predictions: Multi-task predictions
                - attention_weights: Optional attention weights
                - intermediate_states: Optional intermediate representations
        """
        if not timeframe_sequences:
            raise ValueError("No timeframe sequences provided")

        batch_size = next(iter(timeframe_sequences.values())).shape[0]

        # 1. Encode timeframes (sensor processing)
        encoded_timeframes = self.timeframe_encoder(timeframe_sequences, position_data)

        # 2. Initialize task queries
        task_queries = self.task_query_system(batch_size)

        # 3. Create current market state for memory update
        if encoded_timeframes:
            # Average all timeframe representations as current market state
            # Handle different sequence lengths properly
            market_states = []
            for tf_name, tf_encoded in encoded_timeframes.items():
                tf_state = tf_encoded.mean(dim=1)  # [batch, d_model] - average over sequence length
                market_states.append(tf_state)
            current_market_state = torch.stack(market_states).mean(dim=0)  # [batch, d_model]
        else:
            current_market_state = torch.zeros(batch_size, self.config.d_model,
                                             device=task_queries.device)

        # 4. Update temporal memory
        self.temporal_memory_bank(current_market_state)

        # 5. Apply multi-scale attention layers
        all_attention_weights = [] if return_attention_weights else None

        for layer_idx, attention_layer in enumerate(self.attention_layers):
            task_queries, attention_info = attention_layer(
                task_queries, encoded_timeframes,
                update_memory=(layer_idx == len(self.attention_layers) - 1)  # Update memory on last layer
            )

            if return_attention_weights:
                all_attention_weights.append(attention_info)

        # 6. Final layer normalization
        task_queries = self.output_layer_norm(task_queries)

        # 7. Multi-horizon predictions
        predictions = self.multi_horizon_predictor(task_queries)

        # Prepare output
        output = {
            'predictions': predictions,
            'task_representations': task_queries
        }

        if return_attention_weights:
            output['attention_weights'] = all_attention_weights

        return output

    def get_model_info(self) -> Dict[str, Any]:
        """Get detailed model information."""
        total_params = sum(p.numel() for p in self.parameters())
        trainable_params = sum(p.numel() for p in self.parameters() if p.requires_grad)

        return {
            'model_name': 'AutonomousFinanceTransformer',
            'total_parameters': total_params,
            'trainable_parameters': trainable_params,
            'model_size_mb': total_params * 4 / (1024 * 1024),  # Approximate size in MB
            'config': self.config,
            'components': {
                'timeframe_encoder': type(self.timeframe_encoder).__name__,
                'task_query_system': type(self.task_query_system).__name__,
                'temporal_memory_bank': type(self.temporal_memory_bank).__name__,
                'attention_layers': f"{len(self.attention_layers)}x {type(self.attention_layers[0]).__name__}",
                'prediction_heads': type(self.multi_horizon_predictor).__name__
            }
        }


if __name__ == "__main__":
    # Test the complete transformer model
    logging.basicConfig(level=logging.INFO)

    # Create model configuration
    config = TransformerConfig(
        d_model=256,
        num_heads=8,
        num_layers=4,
        dropout=0.1,
        num_tasks=5,
        prediction_horizon=10
    )

    # Create model
    model = AutonomousFinanceTransformer(config)

    # Create test data
    batch_size = 4
    timeframe_sequences = {
        '5m': torch.randn(batch_size, 52, 6),    # 52 bars, 6 features (OHLCV + VWAP)
        '15m': torch.randn(batch_size, 52, 6),
        '1h': torch.randn(batch_size, 24, 6),
        '1d': torch.randn(batch_size, 20, 6),
        '1w': torch.randn(batch_size, 12, 6)
    }

    # Create mock position data
    position_data = {}
    for tf_name, sequences in timeframe_sequences.items():
        seq_len = sequences.shape[1]
        position_data[tf_name] = {
            'timeframe_ids': torch.zeros(batch_size, seq_len, dtype=torch.long),
            'bar_indices': torch.arange(seq_len).unsqueeze(0).repeat(batch_size, 1),
            'temporal_offsets': torch.arange(seq_len, 0, -1).unsqueeze(0).repeat(batch_size, 1),
            'market_regimes': torch.zeros(batch_size, seq_len, dtype=torch.long)
        }

    print("Testing AutonomousFinanceTransformer...")

    # Forward pass
    with torch.no_grad():
        output = model(timeframe_sequences, position_data, return_attention_weights=True)

    print(f"\nModel Info:")
    model_info = model.get_model_info()
    for key, value in model_info.items():
        if key != 'config':
            print(f"  {key}: {value}")

    print(f"\nOutput Structure:")
    print(f"  Predictions: {list(output['predictions'].keys())}")
    for task_name, pred in output['predictions'].items():
        print(f"    {task_name}: {pred.shape}")

    print(f"  Task Representations: {output['task_representations'].shape}")
    print(f"  Attention Layers: {len(output['attention_weights'])}")

    print("\n✅ AutonomousFinanceTransformer test completed successfully!")