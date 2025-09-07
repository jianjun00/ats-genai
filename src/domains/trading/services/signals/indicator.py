"""
Indicator Module - Compatibility Layer.

This module now serves as a compatibility layer, importing all indicators
from their specialized modules while maintaining backward compatibility.

The indicators have been reorganized into logical groups:
- base_indicator.py: Base classes (UniverseState, Indicator)
- price_indicators.py: Price/level indicators (PL, OneOne*, Envelope*)
- volume_indicators.py: Volume-based indicators (Cumulative*, VolumeProfile)
- trend_indicators.py: Trend/zone indicators (L11, H11, Z*, BXTrenderBasic)
- signal_indicators.py: Signal generators (Five* series)
- advanced_indicators.py: Advanced trend analysis (BXTrender variants)
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

# Maintain backward compatibility - all classes available at module level
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