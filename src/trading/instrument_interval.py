from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class InstrumentInterval:
    """
    Represents a single instrument's OHLCV interval.
    status: str or None. Indicates if the price can be trusted. Examples:
      - 'trusted': price is valid
      - 'suspended', 'halted', 'unreliable', etc.: price should not be used for trading/analytics
      - None: unknown
    """
    instrument_id: int
    start_date_time: datetime
    end_date_time: datetime
    open: float
    high: float
    low: float
    close: float
    traded_volume: float
    traded_dollar: float
    status: Optional[str] = None  # 'ok', 'halted', 'unreliable', etc.
