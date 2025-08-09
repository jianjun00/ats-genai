import gin
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
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



import logging
import math

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

@gin.configurable
class PL(Indicator):
    """
    PLDot indicator: for each interval, compute the average of (high, low, close) for the past three intervals, then average these three values.
    """
    def __init__(self):
        super().__init__()
        self.latest_pl: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        self.update_at = datetime.now()
        logging.debug('[PL] update called: intervals=%d, instrument_id=%s', len(intervals), getattr(intervals[-1], 'instrument_id', None) if intervals else None)
        if len(intervals) < 3:
            logging.debug('[PL] Not enough intervals: %s', intervals)
            self.status = 'invalid'
            self.latest_pl = None
            return
        last_three = intervals[-3:]
        for i in last_three:
            ohlc = {x: getattr(i, x, None) for x in ['high', 'low', 'close']}
            logging.debug('[PL] interval: instrument_id=%s, date=%s, OHLC=%s, status=%s', getattr(i, 'instrument_id', None), getattr(i, 'start_date_time', None), ohlc, i.status)
            if i.status != 'ok':
                logging.debug('[PL] Invalid status: %s', i.status)
                self.status = 'invalid'
                self.latest_pl = None
                return
            for k, v in ohlc.items():
                if v is None or (isinstance(v, float) and math.isnan(v)):
                    logging.debug('[PL] NaN or None detected: %s=%s for instrument_id=%s at %s', k, v, getattr(i, 'instrument_id', None), getattr(i, 'start_date_time', None))
                    self.status = 'invalid'
                    self.latest_pl = None
                    return
        vals = []
        for i in last_three:
            if i.high is None or i.low is None or i.close is None:
                logging.debug('[PL] Skipping interval with None OHLC: instrument_id=%s, date=%s, high=%s, low=%s, close=%s', getattr(i, 'instrument_id', None), getattr(i, 'start_date_time', None), i.high, i.low, i.close)
                continue
            vals.append((i.high + i.low + i.close) / 3.0)
        if len(vals) < 3:
            logging.debug('[PL] Not enough valid intervals for PL calculation. vals=%s', vals)
            self.status = 'invalid'
            self.latest_pl = None
            return
        self.latest_pl = sum(vals) / 3.0
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_pl

@gin.configurable
class OneOneHigh(Indicator):
    """
    Indicator that computes OneOneHigh = 2*OneOneDot - last low.
    Status is 'ok' if current interval is valid, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_high: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        self.update_at = datetime.now()
        logging.debug('[OneOneHigh] update called: intervals=%d, instrument_id=%s', len(intervals), getattr(intervals[-1], 'instrument_id', None) if intervals else None)
        if len(intervals) < 1:
            logging.debug('[OneOneHigh] Not enough intervals: %s', intervals)
            self.status = 'invalid'
            self.latest_high = None
            return
        current = intervals[-1]
        ohlc = {x: getattr(current, x, None) for x in ['high', 'low', 'close']}
        logging.debug('[OneOneHigh] interval: instrument_id=%s, date=%s, OHLC=%s, status=%s', getattr(current, 'instrument_id', None), getattr(current, 'start_date_time', None), ohlc, current.status)
        if current.status != 'ok':
            logging.debug('[OneOneHigh] Invalid status: %s', current.status)
            self.status = 'invalid'
            self.latest_high = None
            return
        for k, v in ohlc.items():
            if v is None or (isinstance(v, float) and math.isnan(v)):
                logging.debug('[OneOneHigh] NaN or None detected: %s=%s for instrument_id=%s at %s', k, v, getattr(current, 'instrument_id', None), getattr(current, 'start_date_time', None))
                self.status = 'invalid'
                self.latest_high = None
                return
        if any(getattr(current, x, None) is None or (isinstance(getattr(current, x, None), float) and math.isnan(getattr(current, x, None))) for x in ['high', 'low', 'close']):
            logging.debug('[OneOneHigh] Current interval has None/NaN OHLC: %s', current)
            self.status = 'invalid'
            self.latest_high = None
            return
        oneonedot = (current.high + current.low + current.close) / 3.0
        self.latest_high = 2 * oneonedot - current.low
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_high

@gin.configurable
class OneOneLow(Indicator):
    """
    Indicator that computes OneOneLow = 2*OneOneDot - last high.
    Status is 'ok' if current interval is valid, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_low: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        self.update_at = datetime.now()
        if len(intervals) < 1:
            logging.debug('[OneOneLow] Not enough intervals: %s', intervals)
            self.status = 'invalid'
            self.latest_low = None
            return
        current = intervals[-1]
        if current.status != 'ok' or any(getattr(current, x, None) is None or (isinstance(getattr(current, x, None), float) and math.isnan(getattr(current, x, None))) for x in ['high', 'low', 'close']):
            logging.debug('[OneOneLow] Invalid or missing OHLC: %s', current)
            self.status = 'invalid'
            self.latest_low = None
            return
        oneonedot = (current.high + current.low + current.close) / 3.0
        self.latest_low = 2 * oneonedot - current.high
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_low

@gin.configurable
class OneOneDot(Indicator):
    """
    Indicator that computes the average of the most recent interval's high, low, and close.
    Status is 'ok' if the interval is valid, otherwise 'invalid'.
    """
    def __init__(self):
        super().__init__()
        self.latest_dot: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        self.update_at = datetime.now()
        if len(intervals) < 1:
            logging.debug('[OneOneDot] Not enough intervals: %s', intervals)
            self.status = 'invalid'
            self.latest_dot = None
            return
        # Check all intervals for status and OHLC presence
        for i in intervals:
            if i.status != 'ok' or any(getattr(i, x, None) is None or (isinstance(getattr(i, x, None), float) and math.isnan(getattr(i, x, None))) for x in ['high', 'low', 'close']):
                logging.debug('[OneOneDot] Invalid or missing OHLC: %s', i)
                self.status = 'invalid'
                self.latest_dot = None
                return
        last = intervals[-1]
        if any(getattr(last, x, None) is None or (isinstance(getattr(last, x, None), float) and math.isnan(getattr(last, x, None))) for x in ['high', 'low', 'close']):
            logging.debug('[OneOneDot] Last interval has None/NaN OHLC: %s', last)
            self.status = 'invalid'
            self.latest_dot = None
            return
        self.latest_dot = (last.high + last.low + last.close) / 3.0
        self.status = 'ok'

    def get_value(self) -> Optional[float]:
        return self.latest_dot


@gin.configurable
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
            if current.status != 'ok':
                self.status = 'invalid'
                self.latest_ebot = None
                return
            if any(getattr(current, x, None) is None or (isinstance(getattr(current, x, None), float) and math.isnan(getattr(current, x, None))) for x in ['high', 'low', 'close']):
                logging.debug('[EBot] Current interval has None/NaN OHLC: %s', current)
                self.status = 'invalid'
                self.latest_ebot = None
                return
            prior_index = -(3 - i + 1)
            if prior_index is None or abs(prior_index) > len(intervals):
                self.status = 'invalid'
                self.latest_ebot = None
                return
            prior = intervals[prior_index]
            if prior.status != 'ok':
                self.status = 'invalid'
                self.latest_ebot = None
                return
            if any(getattr(prior, x, None) is None or (isinstance(getattr(prior, x, None), float) and math.isnan(getattr(prior, x, None))) for x in ['high', 'low', 'close']):
                logging.debug('[EBot] Prior interval has None/NaN OHLC: %s', prior)
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


@gin.configurable
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
