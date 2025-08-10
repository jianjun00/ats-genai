from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass
class ForecastInterval:
    """
    Forecasts for a specific instrument and time window.
    forecasts: list of horizon values length = lead_steps.
    """
    instrument_id: int
    start_date_time: datetime
    end_date_time: datetime
    forecasts: List[float] = field(default_factory=list)
