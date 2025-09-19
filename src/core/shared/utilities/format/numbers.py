"""
Number formatting utilities.
"""

from typing import Union


def format_percentage(value: float, decimal_places: int = 2) -> str:
    """Format number as percentage."""
    return f"{value * 100:.{decimal_places}f}%"


def format_large_number(value: Union[int, float]) -> str:
    """Format large numbers with appropriate suffixes."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    elif abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    else:
        return str(int(value))