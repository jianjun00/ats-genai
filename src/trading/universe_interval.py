from dataclasses import dataclass
from typing import Dict
from trading.instrument_interval import InstrumentInterval

@dataclass
class UniverseInterval:
    instrument_intervals: Dict[int, InstrumentInterval]  # instrument_id -> InstrumentInterval
