#!/usr/bin/env python3
"""
Time Series Sequence Training Data Generator

This module provides configuration and data structures for training data generation.
"""

import gin
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime


@gin.configurable
def get_technical_indicators(indicators: List[str] = None) -> List[str]:
    """Get the list of technical indicators from gin configuration."""
    return indicators or ["etop", "ebot", "pldot"]


@gin.configurable
@dataclass 
class TrainingDataConfig:
    """Configuration for training data generation."""
    
    # Base timing configuration
    base_interval_minutes: int = 1
    training_interval_minutes: int = 60
    
    # Multi-timeframe sequence configuration
    sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
        '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
        '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
        '1h': 24,   # Past 24 x 1-hour intervals (1 day)
        '1d': 20,   # Past 20 x daily intervals (4 weeks)
    })
    
    prediction_horizons: Dict[str, int] = field(default_factory=lambda: {
        '1h': 6,    # Next 6 hours
        '1d': 5,    # Next 5 days
    })
    
    # Feature configuration
    timeframes: List[str] = field(default_factory=lambda: [
        '1m', '5m', '15m', '1h', '1d', '1w', '1M'
    ])
    
    feature_types: List[str] = field(default_factory=lambda: [
        'ohlcv',
        'returns', 
        'volatility',
        'volume_profile',
        'technical',
        'market_structure'
    ])


@dataclass
class SequenceTrainingExample:
    """Single training example with multi-timeframe sequences and labels."""
    
    symbol: str
    timestamp: datetime
    features: Dict[str, Any]  # Multi-timeframe feature arrays
    labels: Dict[str, Any]    # Multi-horizon prediction labels
    metadata: Dict[str, Any] = field(default_factory=dict)


class TimeSeriesSequenceTrainingGenerator:
    """Generator for time series sequence training data."""
    
    @gin.configurable
    def __init__(self, config: Optional[TrainingDataConfig] = None):
        """Initialize with configuration."""
        self.config = config or TrainingDataConfig()
    
    def generate_sequences(self, data, symbols: List[str]) -> List[SequenceTrainingExample]:
        """Generate training sequences from data."""
        # Implementation would go here
        # For now, return empty list as placeholder
        return []


@gin.configurable
class MultiTimeframeFeatureExtractor:
    """Extract features from multiple timeframes."""
    
    def __init__(self, config: Optional[TrainingDataConfig] = None):
        self.config = config or TrainingDataConfig()


@gin.configurable 
class SequenceWindowBuilder:
    """Build sequence windows for training."""
    
    def __init__(self, config: Optional[TrainingDataConfig] = None):
        self.config = config or TrainingDataConfig()