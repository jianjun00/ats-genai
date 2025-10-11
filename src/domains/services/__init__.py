"""
Training Infrastructure for Advanced ML Models

This module provides training pipelines and utilities for advanced ML models
including the Temporal Fusion Transformer and other research-grade models.
"""

# Make TFT imports optional to avoid heavy dependency chain when only accessing training_data modules
try:
    from .tft_training_pipeline import (
        TFTTrainingPipeline,
        ExperimentConfig,
        TrainingMetrics,
        ExperimentTracker,
        create_experiment_config,
        run_tft_experiment
    )
    
    __all__ = [
        "TFTTrainingPipeline",
        "ExperimentConfig",
        "TrainingMetrics",
        "ExperimentTracker",
        "create_experiment_config",
        "run_tft_experiment"
    ]
except ImportError as e:
    # TFT pipeline dependencies not available, skip TFT exports
    __all__ = []