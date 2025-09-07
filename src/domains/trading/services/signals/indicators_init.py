"""
Signals package for technical indicators and signal generation.

This package provides a comprehensive suite of technical indicators
organized by functionality for better maintainability.
"""

# Import base classes
from .base_indicator import UniverseState, Indicator

# Import all indicator categories
from .price_indicators import (
    PL, OneOneDot, OneOneHigh, OneOneLow, 
    EnvelopeBot, EnvelopeTop
)

from .volume_indicators import (
    CumulativeVolume, CumulativeDollars, VolumeProfile
)

from .trend_indicators import (
    L11, H11, Z1B, Z2B, Z5T, Z6T, BXTrenderBasic
)

from .signal_indicators import (
    FiveNineSell, FiveNineBuy, FiveOneBuy, FiveOneSell,
    FiveTwoBuy, FiveTwoSell
)

from .advanced_indicators import (
    BXTrenderDirectional, BXTrenderVolumeWeighted
)

# Export all indicators for backward compatibility
__all__ = [
    # Base classes
    'UniverseState', 'Indicator',
    
    # Price indicators
    'PL', 'OneOneDot', 'OneOneHigh', 'OneOneLow', 
    'EnvelopeBot', 'EnvelopeTop',
    
    # Volume indicators  
    'CumulativeVolume', 'CumulativeDollars', 'VolumeProfile',
    
    # Trend indicators
    'L11', 'H11', 'Z1B', 'Z2B', 'Z5T', 'Z6T', 'BXTrenderBasic',
    
    # Signal indicators
    'FiveNineSell', 'FiveNineBuy', 'FiveOneBuy', 'FiveOneSell',
    'FiveTwoBuy', 'FiveTwoSell',
    
    # Advanced indicators
    'BXTrenderDirectional', 'BXTrenderVolumeWeighted'
]