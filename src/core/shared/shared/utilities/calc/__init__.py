"""
Calculation utilities for ATS-GenAI platform.

Provides common mathematical and financial calculation functions.
"""

from .financial import (
    calculate_returns,
    calculate_volatility,
    calculate_technical_levels,
    safe_divide
)
from .indicators import (
    detect_splits_and_dividends,
    adjust_prices_for_splits
)

__all__ = [
    'calculate_returns',
    'calculate_volatility',
    'calculate_technical_levels',
    'safe_divide',
    'detect_splits_and_dividends',
    'adjust_prices_for_splits'
]