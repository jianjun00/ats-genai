"""
Base Indicator classes and utilities.

Extracted from indicator.py to provide foundation classes and shared utilities
for all specific indicator implementations.
"""

import gin
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from state.factor_interval import FactorInterval
from state.instrument_interval import InstrumentInterval
from state.indicator_interval import IndicatorInterval


@dataclass
class UniverseState:
    intervals: List[FactorInterval] = field(default_factory=list)  # List of FactorInterval, e.g., one per time step
    instrument_intervals: Dict[int, InstrumentInterval] = field(default_factory=dict)
    indicator_intervals: Dict[int, IndicatorInterval] = field(default_factory=dict)  # Map instrument_id to computed indicators
    instrument_history: Dict[int, List[InstrumentInterval]] = field(default_factory=dict)  # Historical intervals per instrument for indicator computation

    def __post_init__(self):
        # If instrument_intervals not provided, populate from last interval
        if not self.instrument_intervals and self.intervals:
            self.instrument_intervals = self.intervals[-1].instrument_intervals.copy()
    
    def add_interval(self, interval: FactorInterval):
        """Add a new FactorInterval and update instrument history."""
        self.intervals.append(interval)
        self._update_instrument_history(interval)
    
    def _update_instrument_history(self, interval: FactorInterval):
        """Update instrument history with intervals from the new FactorInterval."""
        for instrument_id, instrument_interval in interval.instrument_intervals.items():
            if instrument_id not in self.instrument_history:
                self.instrument_history[instrument_id] = []
            self.instrument_history[instrument_id].append(instrument_interval)
    
    def reset(self):
        """Clear all intervals and history."""
        self.intervals.clear()
        self.instrument_intervals.clear()
        self.indicator_intervals.clear()
        self.instrument_history.clear()


class Indicator:
    def __init__(self):
        self.status: Optional[str] = None
        self.update_at: Optional[datetime] = None

    def update(self, intervals: List[InstrumentInterval]):
        """
        Update the indicator using the provided intervals.
        Subclasses should implement this method.
        """
        raise NotImplementedError("Subclasses must implement the update method")