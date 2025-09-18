from dataclasses import dataclass, field
from typing import Dict
from src.domains.trading.services.state.instrument_interval import InstrumentInterval

from datetime import datetime

@dataclass
class FactorInterval:
    start_date_time: datetime
    end_date_time: datetime
    instrument_intervals: Dict[int, InstrumentInterval] = field(default_factory=dict)
