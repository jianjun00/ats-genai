"""
Indicator classes for technical analysis.
"""

import gin
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional


class Indicator(ABC):
    """Base class for all technical indicators."""
    
    def __init__(self):
        self.value = None
        self.status = "ok"
        self.update_at = None
    
    def update(self, intervals: List) -> None:
        """Update the indicator with new interval data."""
        if intervals:
            # Use the most recent interval
            latest_interval = intervals[-1]
            self.value = self.calculate(latest_interval)
            self.update_at = getattr(latest_interval, 'end_date_time', None)
    
    def get_value(self) -> Optional[float]:
        """Get the current indicator value."""
        return self.value
    
    @abstractmethod
    def calculate(self, interval) -> float:
        """Calculate the indicator value from interval data."""
        pass


class PL(Indicator):
    """Price Level indicator."""
    
    def calculate(self, interval) -> float:
        return getattr(interval, 'close', 0.0)


class OneOneHigh(Indicator):
    """One-period high indicator."""
    
    def calculate(self, interval) -> float:
        return getattr(interval, 'high', 0.0)


class OneOneLow(Indicator):
    """One-period low indicator."""
    
    def calculate(self, interval) -> float:
        return getattr(interval, 'low', 0.0)


class OneOneDot(Indicator):
    """One-period dot (close) indicator."""
    
    def calculate(self, interval) -> float:
        return getattr(interval, 'close', 0.0)


class EnvelopeBot(Indicator):
    """Envelope bottom indicator."""
    
    def calculate(self, interval) -> float:
        close = getattr(interval, 'close', 0.0)
        return close * 0.98  # 2% below close


class EnvelopeTop(Indicator):
    """Envelope top indicator."""
    
    def calculate(self, interval) -> float:
        close = getattr(interval, 'close', 0.0)
        return close * 1.02  # 2% above close


@gin.configurable
def indicator_config():
    """Configurable indicator function."""
    pass