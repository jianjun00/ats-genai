"""
Autonomous Driving Inspired Financial Transformer Models

This package contains transformer models inspired by cutting-edge autonomous driving
architectures, adapted for multi-timeframe financial time series prediction.

Key components:
- Multi-timeframe data preprocessing (sensor-like data fusion)
- Temporal attention mechanisms with FIFO memory banks
- Task self-attention for multi-task financial prediction
- Real-time streaming inference capabilities
"""

from .data_preprocessing import (
    AutonomousFinanceDataLoader,
    MultiTimeframeProcessor,
    MarketPositionEncoder,
    TimeframeVariableSelector
)

from .transformer_model import (
    AutonomousFinanceTransformer,
    MultiTimeframeEncoder,
    TemporalMemoryBank,
    TaskQuerySystem,
    MultiHorizonPredictor
)

from .attention_mechanisms import (
    SensorCrossAttention,
    TemporalCrossAttention, 
    TaskSelfAttention,
    MultiScaleAttentionLayer
)

from .training import (
    AutonomousFinanceTrainer,
    MultiTaskLoss,
    CurriculumScheduler
)

__all__ = [
    # Data preprocessing
    'AutonomousFinanceDataLoader',
    'MultiTimeframeProcessor', 
    'MarketPositionEncoder',
    'TimeframeVariableSelector',
    
    # Model components
    'AutonomousFinanceTransformer',
    'MultiTimeframeEncoder',
    'TemporalMemoryBank',
    'TaskQuerySystem', 
    'MultiHorizonPredictor',
    
    # Attention mechanisms
    'SensorCrossAttention',
    'TemporalCrossAttention',
    'TaskSelfAttention', 
    'MultiScaleAttentionLayer',
    
    # Training
    'AutonomousFinanceTrainer',
    'MultiTaskLoss',
    'CurriculumScheduler'
]