from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime
from .universe_interval import UniverseInterval
from .instrument_interval import InstrumentInterval
from .indicator_interval import IndicatorInterval

@dataclass
class UniverseState:
    """
    Represents the complete state of the universe for a given interval.
    Contains:
      - universe_interval: The overall interval for the universe (time window & membership)
      - instrument_intervals: Dict mapping instrument_id to InstrumentInterval
      - indicator_intervals: Dict mapping indicator_type (str) to dict of instrument_id to IndicatorInterval
    """
    universe_interval: UniverseInterval
    instrument_intervals: Dict[int, InstrumentInterval] = field(default_factory=dict)
    indicator_intervals: Dict[str, Dict[int, IndicatorInterval]] = field(default_factory=dict)
