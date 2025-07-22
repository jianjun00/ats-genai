from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
from trading.universe_interval import UniverseInterval
from trading.instrument_interval import InstrumentInterval
from trading.indicator_interval import IndicatorInterval

@dataclass
class UniverseState:
    intervals: List[UniverseInterval]  # List of UniverseInterval, e.g., one per time step
    instrument_intervals: Dict[int, InstrumentInterval] = field(default_factory=dict)
    indicator_intervals: Dict[int, IndicatorInterval] = field(default_factory=dict)  # Map instrument_id to computed indicators

    def __post_init__(self):
        # If instrument_intervals not provided, populate from last interval
        if not self.instrument_intervals and self.intervals:
            self.instrument_intervals = self.intervals[-1].instrument_intervals.copy()

from typing import List, Optional
from trading.instrument_interval import InstrumentInterval
from datetime import datetime

class Indicator:
    def __init__(self):
        self.status: Optional[str] = None
        self.update_at: Optional[datetime] = None

    def update(self, intervals: List[InstrumentInterval]):
        """
        Update the indicator based on the provided list of InstrumentInterval (rolling window for a single instrument).
        This method should be implemented by subclasses.
        """
        self.update_at = datetime.now()
        if any(i.status != 'ok' for i in intervals):
            self.status = 'invalid'
        else:
            self.status = 'ok'
        # Subclass should override and implement logic here

class PL(Indicator):
    """
    PLDot indicator: for each interval, compute the average of (high, low, close) for the past three intervals, then average these three values.
    """
    def __init__(self):
        super().__init__()
        self.latest_pl: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        self.update_at = datetime.now()
        if len(intervals) < 3:
            self.status = 'invalid'
            self.latest_pl = None
            return
        last_three = intervals[-3:]
        if any(i.status != 'ok' for i in last_three):
            self.status = 'invalid'
            self.latest_pl = None
            return
        vals = [(i.high + i.low + i.close) / 3.0 for i in last_three]
        self.latest_pl = sum(vals) / 3.0
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_pl

class OneOneHigh(Indicator):
    """
    Indicator that computes OneOneHigh = 2*OneOneDot - last low.
    Status is 'ok' if current interval is valid, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_high: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        self.update_at = datetime.now()
        if len(intervals) < 1:
            self.status = 'invalid'
            self.latest_high = None
            return
        current = intervals[-1]
        if current.status != 'ok':
            self.status = 'invalid'
            self.latest_high = None
            return
        oneonedot = (current.high + current.low + current.close) / 3.0
        self.latest_high = 2 * oneonedot - current.low
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_high

class OneOneLow(Indicator):
    """
    Indicator that computes OneOneLow = 2*OneOneDot - last high.
    Status is 'ok' if current interval is valid, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_low: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        self.update_at = datetime.now()
        if len(intervals) < 1:
            self.status = 'invalid'
            self.latest_low = None
            return
        current = intervals[-1]
        if current.status != 'ok':
            self.status = 'invalid'
            self.latest_low = None
            return
        oneonedot = (current.high + current.low + current.close) / 3.0
        self.latest_low = 2 * oneonedot - current.high
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_low

class OneOneDot(Indicator):
    """
    Indicator that computes the average of the most recent interval's high, low, and close.
    Status is 'ok' if the interval is valid, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_dot: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        self.update_at = datetime.now()
        if len(intervals) < 1:
            self.status = 'invalid'
            self.latest_dot = None
            return
        last = intervals[-1]
        if last.status != 'ok':
            self.status = 'invalid'
            self.latest_dot = None
            return
        self.latest_dot = (last.high + last.low + last.close) / 3.0
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_dot


class EBot(Indicator):
    """
    Indicator that computes the average of OneOneLow values for the past three intervals.
    Status is 'ok' if all three intervals are valid and OneOneLow is valid for each, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_ebot: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        self.update_at = datetime.now()
        if len(intervals) < 3:
            self.status = 'invalid'
            self.latest_ebot = None
            return
        last_three = intervals[-3:]
        oneonelows = []
        for i in range(3):
            current = last_three[i]
            if i == 0:
                prior_index = -4 if len(intervals) >= 4 else None
            else:
                prior_index = -(3 - i + 1)
            if current.status != 'ok' or prior_index is None or abs(prior_index) > len(intervals):
                self.status = 'invalid'
                self.latest_ebot = None
                return
            prior = intervals[prior_index]
            if prior.status != 'ok':
                self.status = 'invalid'
                self.latest_ebot = None
                return
            oneonedot = (current.high + current.low + current.close) / 3.0
            oneonelow = 2 * oneonedot - current.high
            oneonelows.append(oneonelow)
        self.latest_ebot = sum(oneonelows) / 3.0
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_ebot


class ETop(Indicator):
    """
    Indicator that computes the average of OneOneHigh values for the past three intervals.
    Status is 'ok' if all three intervals are valid and OneOneHigh is valid for each, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_etop: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        self.update_at = datetime.now()
        if len(intervals) < 3:
            self.status = 'invalid'
            self.latest_etop = None
            return
        last_three = intervals[-3:]
        oneonehighs = []
        for i in range(3):
            # For each of the last 3 intervals, compute OneOneHigh as per current OneOneHigh logic
            current = last_three[i]
            if i == 0:
                prior_index = -4 if len(intervals) >= 4 else None
            else:
                prior_index = -(3 - i + 1)
            if current.status != 'ok' or prior_index is None or abs(prior_index) > len(intervals):
                self.status = 'invalid'
                self.latest_etop = None
                return
            prior = intervals[prior_index]
            if prior.status != 'ok':
                self.status = 'invalid'
                self.latest_etop = None
                return
            oneonedot = (current.high + current.low + current.close) / 3.0
            oneonehigh = 2 * oneonedot - current.low
            oneonehighs.append(oneonehigh)
        self.latest_etop = sum(oneonehighs) / 3.0
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_etop
