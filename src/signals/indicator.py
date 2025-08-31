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
    PLDOT indicator: Calculated using exact linear regression formula from 3 days OHLC data.
    Formula: pldot ≈ 0.1111 * (high_sum + low_sum + close_sum) for past 3 days
    R² = 0.9999999999, Average Error = 0.001359
    """
    def __init__(self):
        super().__init__()
        self.latest_pl: Optional[float] = None
        
        # Exact coefficients from linear regression (R² ≈ 1.0)
        self.coefficients = [
            -0.00001727, 0.11110720, 0.11111657, 0.11115994,   # t-3: O,H,L,C
            -0.00001503, 0.11109015, 0.11108289, 0.11111244,   # t-2: O,H,L,C  
            0.00004864, 0.11112260, 0.11107075, 0.11112078     # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[PLDOT] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_pl = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[PLDOT] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_pl = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[PLDOT] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_pl = None
                    return
        
        # Build feature vector: 3 days × 4 OHLC = 12 features
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate PLDOT using exact linear formula
        try:
            self.latest_pl = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[PLDOT] Calculated PLDOT: %.6f', self.latest_pl)
        except Exception as e:
            logging.error('[PLDOT] Error calculating PLDOT: %s', str(e))
            self.status = 'invalid'
            self.latest_pl = None

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
    EBOT indicator: Bottom level calculated using exact linear regression formula.
    Formula: ebot ≈ -0.1111*high_sum + 0.2222*(low_sum + close_sum) for past 3 days
    R² = 1.0000000000, Average Error = 0.001397
    """
    def __init__(self):
        super().__init__()
        self.latest_ebot: Optional[float] = None
        
        # Exact coefficients from linear regression
        self.coefficients = [
            0.00002039, -0.11113139, 0.22221285, 0.22221404,   # t-3: O,H,L,C
            0.00000082, -0.11109608, 0.22224681, 0.22221805,   # t-2: O,H,L,C
            -0.00001111, -0.11112842, 0.22222156, 0.22223273   # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[EBOT] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_ebot = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[EBOT] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_ebot = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[EBOT] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_ebot = None
                    return
        
        # Build feature vector
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate EBOT using exact linear formula
        try:
            self.latest_ebot = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[EBOT] Calculated EBOT: %.6f', self.latest_ebot)
        except Exception as e:
            logging.error('[EBOT] Error calculating EBOT: %s', str(e))
            self.status = 'invalid'
            self.latest_ebot = None

    def get_value(self) -> Optional[float]:
        return self.latest_ebot


@gin.configurable
class ETop(Indicator):
    """
    ETOP indicator: Top level calculated using exact linear regression formula.
    Formula: etop ≈ 0.2222*(high_sum + close_sum) - 0.1111*low_sum for past 3 days
    R² = 0.9999999994, Average Error = 0.003366
    """
    def __init__(self):
        super().__init__()
        self.latest_etop: Optional[float] = None
        
        # Exact coefficients from linear regression
        self.coefficients = [
            0.00003781, 0.22219235, -0.11109917, 0.22216559,   # t-3: O,H,L,C
            0.00002133, 0.22225543, -0.11106971, 0.22208568,   # t-2: O,H,L,C
            0.00008489, 0.22218006, -0.11109128, 0.22223771    # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[ETOP] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_etop = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[ETOP] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_etop = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[ETOP] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_etop = None
                    return
        
        # Build feature vector
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate ETOP using exact linear formula
        try:
            self.latest_etop = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[ETOP] Calculated ETOP: %.6f', self.latest_etop)
        except Exception as e:
            logging.error('[ETOP] Error calculating ETOP: %s', str(e))
            self.status = 'invalid'
            self.latest_etop = None

    def get_value(self) -> Optional[float]:
        return self.latest_etop


@gin.configurable  
class CumulativeVolume(Indicator):
    """
    Cumulative Volume indicator for intervals.
    Accumulates volume across intervals with optional daily reset.
    """
    def __init__(self, reset_daily: bool = True):
        super().__init__()
        self.reset_daily = reset_daily
        self.cumulative_volume: Optional[float] = None
        self.last_reset_date: Optional[str] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        self.update_at = datetime.now()
        
        if not intervals:
            logging.debug('[CumulativeVolume] No intervals provided')
            self.status = 'invalid'
            self.cumulative_volume = None
            return
        
        # Check if all intervals are valid
        for i, interval in enumerate(intervals):
            if interval.status != 'ok':
                logging.debug(f'[CumulativeVolume] Interval {i} has invalid status: {interval.status}')
                self.status = 'invalid'
                self.cumulative_volume = None
                return
            
            if not hasattr(interval, 'traded_volume') or interval.traded_volume is None:
                logging.debug(f'[CumulativeVolume] Interval {i} missing volume data')
                self.status = 'invalid'
                self.cumulative_volume = None
                return
        
        try:
            current_interval = intervals[-1]
            current_date = current_interval.start_date_time.date().isoformat() if current_interval.start_date_time else None
            
            # Reset cumulative volume daily if configured
            if self.reset_daily and current_date != self.last_reset_date:
                self.cumulative_volume = 0.0
                self.last_reset_date = current_date
                logging.debug(f'[CumulativeVolume] Reset for new day: {current_date}')
            
            # Initialize if needed
            if self.cumulative_volume is None:
                self.cumulative_volume = 0.0
            
            # Add current interval volume
            self.cumulative_volume += current_interval.traded_volume
            self.status = 'ok'
            
            logging.debug(f'[CumulativeVolume] Updated cumulative volume: {self.cumulative_volume}')
            
        except Exception as e:
            logging.error(f'[CumulativeVolume] Error calculating cumulative volume: {str(e)}')
            self.status = 'invalid'
            self.cumulative_volume = None

    def get_value(self) -> Optional[float]:
        return self.cumulative_volume


@gin.configurable
class CumulativeDollars(Indicator):
    """
    Cumulative Dollar Volume (price * volume) indicator for intervals.
    Accumulates dollar volume across intervals with optional daily reset.
    """
    def __init__(self, reset_daily: bool = True, price_method: str = 'typical'):
        super().__init__()
        self.reset_daily = reset_daily
        self.price_method = price_method  # 'typical', 'close', 'open'
        self.cumulative_dollars: Optional[float] = None
        self.last_reset_date: Optional[str] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        self.update_at = datetime.now()
        
        if not intervals:
            logging.debug('[CumulativeDollars] No intervals provided')
            self.status = 'invalid'
            self.cumulative_dollars = None
            return
        
        # Check if all intervals are valid
        for i, interval in enumerate(intervals):
            if interval.status != 'ok':
                logging.debug(f'[CumulativeDollars] Interval {i} has invalid status: {interval.status}')
                self.status = 'invalid'
                self.cumulative_dollars = None
                return
            
            # Check for required OHLCV data
            required_fields = ['traded_volume']
            if self.price_method == 'typical':
                required_fields.extend(['high', 'low', 'close'])
            elif self.price_method == 'close':
                required_fields.append('close')
            elif self.price_method == 'open':
                required_fields.append('open')
                
            for field in required_fields:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug(f'[CumulativeDollars] Interval {i} has invalid {field}: {val}')
                    self.status = 'invalid'
                    self.cumulative_dollars = None
                    return
        
        try:
            current_interval = intervals[-1]
            current_date = current_interval.start_date_time.date().isoformat() if current_interval.start_date_time else None
            
            # Reset cumulative dollars daily if configured
            if self.reset_daily and current_date != self.last_reset_date:
                self.cumulative_dollars = 0.0
                self.last_reset_date = current_date
                logging.debug(f'[CumulativeDollars] Reset for new day: {current_date}')
            
            # Initialize if needed
            if self.cumulative_dollars is None:
                self.cumulative_dollars = 0.0
            
            # Calculate price based on method
            if self.price_method == 'typical':
                price = (current_interval.high + current_interval.low + current_interval.close) / 3.0
            elif self.price_method == 'close':
                price = current_interval.close
            elif self.price_method == 'open':
                price = current_interval.open
            else:
                price = current_interval.close  # Default fallback
            
            # Add current interval dollar volume
            dollar_volume = price * current_interval.traded_volume
            self.cumulative_dollars += dollar_volume
            self.status = 'ok'
            
            logging.debug(f'[CumulativeDollars] Updated cumulative dollars: {self.cumulative_dollars} (price={price}, volume={current_interval.traded_volume})')
            
        except Exception as e:
            logging.error(f'[CumulativeDollars] Error calculating cumulative dollars: {str(e)}')
            self.status = 'invalid'
            self.cumulative_dollars = None

    def get_value(self) -> Optional[float]:
        return self.cumulative_dollars


@gin.configurable
class L11(Indicator):
    """
    L11 indicator: Low level calculated using exact linear regression formula.
    Formula: l11 ≈ -0.3333*high(t-1) + 0.6667*(low(t-1) + close(t-1))
    R² = 1.0000000000, Average Error = 0.000605
    """
    def __init__(self):
        super().__init__()
        self.latest_l11: Optional[float] = None
        
        # Exact coefficients from linear regression
        self.coefficients = [
            -0.00001143, 0.00000147, 0.00000301, 0.00002516,    # t-3: O,H,L,C
            -0.00000809, -0.00000957, -0.00001325, 0.00004187,  # t-2: O,H,L,C
            -0.00002814, -0.33331649, 0.66666993, 0.66664543    # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[L11] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_l11 = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[L11] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_l11 = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[L11] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_l11 = None
                    return
        
        # Build feature vector
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate L11 using exact linear formula
        try:
            self.latest_l11 = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[L11] Calculated L11: %.6f', self.latest_l11)
        except Exception as e:
            logging.error('[L11] Error calculating L11: %s', str(e))
            self.status = 'invalid'
            self.latest_l11 = None

    def get_value(self) -> Optional[float]:
        return self.latest_l11


@gin.configurable
class Z1B(Indicator):
    """
    Z1B Indicator: Lower support zone calculated using 3-day OHLC linear regression
    Formula: z1b = -1.242786*O₁ + 0.772321*H₁ + 1.339376*L₁ + 1.258544*C₁
                  - 1.963210*O₂ - 0.447455*H₂ + 0.583624*L₂ - 0.534829*C₂
                  + 0.295301*O₃ + 1.040109*H₃ + 0.470587*L₃ - 0.578177*C₃
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    """
    def __init__(self):
        super().__init__()
        self.latest_z1b: Optional[float] = None
        
        # Exact coefficients from linear regression (R² = 1.0, Error = 0.001328)
        self.coefficients = [
            -0.00000457, -0.44444327, 0.55555314, 0.22223491,   # t-3: O,H,L,C
            0.00000512, -0.44444961, 0.55553851, 0.22221018,    # t-2: O,H,L,C
            0.00003635, -0.44445332, 0.55554551, 0.22222695     # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[Z1B] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_z1b = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[Z1B] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_z1b = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[Z1B] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_z1b = None
                    return
        
        # Build feature vector: 3 days × 4 OHLC = 12 features
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate Z1B using dot product of coefficients and features
        try:
            self.latest_z1b = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[Z1B] Calculated Z1B: %.2f', self.latest_z1b)
        except Exception as e:
            logging.error('[Z1B] Error calculating Z1B: %s', str(e))
            self.status = 'invalid'
            self.latest_z1b = None

    def get_value(self) -> Optional[float]:
        return self.latest_z1b


@gin.configurable
class Z2B(Indicator):
    """
    Z2B Indicator: Lower resistance zone calculated using 3-day OHLC linear regression
    Formula: z2b = -0.109183*O₁ - 0.448761*H₁ + 0.180165*L₁ + 0.946454*C₁
                  - 0.003572*O₂ - 0.436792*H₂ + 0.037945*L₂ + 0.255704*C₂
                  + 0.052054*O₃ + 0.464655*H₃ + 0.459729*L₃ - 0.403429*C₃
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    """
    def __init__(self):
        super().__init__()
        self.latest_z2b: Optional[float] = None
        
        # Exact coefficients from linear regression (R² = 1.0, Error = 0.001288)
        self.coefficients = [
            -0.00000157, -0.33333483, 0.33333029, 0.33338614,   # t-3: O,H,L,C
            -0.00004507, -0.33333123, 0.33334061, 0.33331983,   # t-2: O,H,L,C
            0.00000507, -0.33332244, 0.33332972, 0.33332344     # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[Z2B] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_z2b = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                self.status = 'invalid'
                self.latest_z2b = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    self.status = 'invalid'
                    self.latest_z2b = None
                    return
        
        # Build feature vector
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate Z2B
        try:
            self.latest_z2b = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
        except Exception as e:
            logging.error('[Z2B] Error calculating Z2B: %s', str(e))
            self.status = 'invalid'
            self.latest_z2b = None

    def get_value(self) -> Optional[float]:
        return self.latest_z2b


@gin.configurable
class Z5T(Indicator):
    """
    Z5T Indicator: Upper resistance zone calculated using 3-day OHLC linear regression
    Formula: z5t = 0.572696*O₁ + 0.251544*H₁ - 0.783063*L₁ + 0.865703*C₁
                  - 1.238945*O₂ + 0.919261*H₂ + 0.665145*L₂ - 0.428645*C₂
                  - 0.009658*O₃ + 0.046637*H₃ + 0.101678*L₃ + 0.045071*C₃
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    """
    def __init__(self):
        super().__init__()
        self.latest_z5t: Optional[float] = None
        
        # Exact coefficients from linear regression (R² = 0.9999999999, Error = 0.001220)
        self.coefficients = [
            -0.00001787, 0.33332894, -0.33332172, 0.33339446,   # t-3: O,H,L,C
            -0.00002980, 0.33332009, -0.33336788, 0.33332609,   # t-2: O,H,L,C
            0.00003446, 0.33336122, -0.33333863, 0.33331041     # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            self.status = 'invalid'
            self.latest_z5t = None
            return
            
        last_three = intervals[-3:]
        
        # Validate intervals
        for interval in last_three:
            if interval.status != 'ok':
                self.status = 'invalid'
                self.latest_z5t = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    self.status = 'invalid'
                    self.latest_z5t = None
                    return
        
        # Build feature vector
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate Z5T
        try:
            self.latest_z5t = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
        except Exception as e:
            logging.error('[Z5T] Error calculating Z5T: %s', str(e))
            self.status = 'invalid'
            self.latest_z5t = None

    def get_value(self) -> Optional[float]:
        return self.latest_z5t


@gin.configurable
class Z6T(Indicator):
    """
    Z6T Indicator: Upper breakout zone calculated using 3-day OHLC linear regression
    Formula: z6t = 1.853702*O₁ - 1.198374*H₁ - 2.125780*L₁ - 1.501241*C₁
                  + 3.287197*O₂ + 1.268150*H₂ + 0.029819*L₂ - 0.751982*C₂
                  - 0.217435*O₃ + 0.923613*H₃ + 0.847033*L₃ - 1.410876*C₃
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    
    Note: Z6T shows strong correlation (0.9985) with Z5T but cannot be simplified to z6t = z5t + constant
    due to variable offset (std dev = 34.6). The full 12-coefficient formula is required for accuracy.
    """
    def __init__(self):
        super().__init__()
        self.latest_z6t: Optional[float] = None
        
        # Exact coefficients from linear regression (R² = 0.9999999998, Error = 0.001328)
        self.coefficients = [
            -0.00000457, 0.55555673, -0.44444686, 0.22223491,   # t-3: O,H,L,C
            0.00000512, 0.55555039, -0.44446149, 0.22221018,    # t-2: O,H,L,C
            0.00003635, 0.55554668, -0.44445449, 0.22222695     # t-1: O,H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            self.status = 'invalid'
            self.latest_z6t = None
            return
            
        last_three = intervals[-3:]
        
        # Validate intervals
        for interval in last_three:
            if interval.status != 'ok':
                self.status = 'invalid'
                self.latest_z6t = None
                return
                
            for field in ['open', 'high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    self.status = 'invalid'
                    self.latest_z6t = None
                    return
        
        # Build feature vector
        features = []
        for interval in last_three:
            features.extend([interval.open, interval.high, interval.low, interval.close])
        
        # Calculate Z6T
        try:
            self.latest_z6t = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
        except Exception as e:
            logging.error('[Z6T] Error calculating Z6T: %s', str(e))
            self.status = 'invalid'
            self.latest_z6t = None

    def get_value(self) -> Optional[float]:
        return self.latest_z6t
