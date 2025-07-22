from dataclasses import dataclass
from typing import Dict
from trading.instrument_interval import InstrumentInterval

from datetime import datetime

@dataclass
class UniverseInterval:
    start_date_time: datetime
    end_date_time: datetime
