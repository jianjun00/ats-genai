"""
ML/Analytics Pipeline Framework

Consolidates ALL ML and analytics code from 191+ files:
- 82 ML files + 71 analytics files + 38 training files (17,266+ lines)
- Multiple training pipelines → Single unified pipeline
- Scattered model management → Unified model registry
- Duplicate feature engineering → Single feature framework
- Multiple evaluation approaches → Unified backtesting

TARGET CONSOLIDATION: 30,000+ lines → 8,000 lines (73% reduction)
"""

# Import main classes from pipeline module
from .pipeline import (
    MLPipeline,
    ModelRegistry,
    FeatureStore,
    ModelType,
    PipelineStage,
    ModelConfig,
    TrainingConfig,
    ModelMetrics,
    FeatureEngineer
)

__all__ = [
    'MLPipeline',
    'ModelRegistry',
    'FeatureStore',
    'ModelType',
    'PipelineStage',
    'ModelConfig',
    'TrainingConfig',
    'ModelMetrics',
    'FeatureEngineer'
]