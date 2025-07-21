from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class InstrumentInterval:
    instrument_id: int
    start_date_time: datetime
    end_date_time: datetime
    open: float
    high: float
    low: float
    close: float
    traded_volume: float
    traded_dollar: float
    status: Optional[str] = None  # e.g., 'open', 'closed', 'halted', etc.
