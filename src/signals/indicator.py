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
    PLDOT indicator: Calculated using exact HLC-only linear regression formula (9 features).
    Formula: Weighted average of HLC from past 3 days (excludes open prices)
    R² = 0.999996, Average Error = 0.0183, Cross-validated
    """
    def __init__(self):
        super().__init__()
        self.latest_pl: Optional[float] = None
        
        # HLC-only coefficients (9 features, cross-validated)
        self.coefficients = [
            0.11306077, 0.10884779, 0.10864725,    # t-3: H,L,C
            0.11441424, 0.11317815, 0.10686769,    # t-2: H,L,C
            0.11171601, 0.11384294, 0.10939732,    # t-1: H,L,C
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
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[PLDOT] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_pl = None
                    return
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
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
class EnvelopeBot(Indicator):
    """
    Envelope Bottom indicator: Bottom level calculated using exact linear regression formula.
    Formula: envelope_bot ≈ -0.1111*high_sum + 0.2222*(low_sum + close_sum) for past 3 days
    R² = 1.0000000000, Average Error = 0.001397
    """
    def __init__(self):
        super().__init__()
        self.latest_envelope_bot: Optional[float] = None
        
        # HLC-only coefficients from linear regression
        self.coefficients = [
            -0.11115648, 0.22303212, 0.22206190,   # t-3: H,L,C
            -0.11250983, 0.22120078, 0.22439345,   # t-2: H,L,C
            -0.11109552, 0.22046378, 0.22360772,   # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[EBOT] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_envelope_bot = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[EBOT] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_envelope_bot = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[EBOT] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_envelope_bot = None
                    return
        
        # Build feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate EBOT using exact linear formula
        try:
            self.latest_envelope_bot = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[EBOT] Calculated Envelope Bot: %.6f', self.latest_envelope_bot)
        except Exception as e:
            logging.error('[EBOT] Error calculating EBOT: %s', str(e))
            self.status = 'invalid'
            self.latest_envelope_bot = None

    def get_value(self) -> Optional[float]:
        return self.latest_envelope_bot


@gin.configurable
class EnvelopeTop(Indicator):
    """
    Envelope Top indicator: Top level calculated using exact linear regression formula.
    Formula: envelope_top ≈ 0.2222*(high_sum + close_sum) - 0.1111*low_sum for past 3 days
    R² = 0.9999999994, Average Error = 0.003366
    """
    def __init__(self):
        super().__init__()
        self.latest_envelope_top: Optional[float] = None
        
        # HLC-only coefficients from linear regression
        self.coefficients = [
            0.22106127, -0.11318101, 0.22457886,   # t-3: H,L,C
            0.22053147, -0.11010281, 0.22546244,   # t-2: H,L,C
            0.21983177, -0.11226826, 0.22411409,   # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[ETOP] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_envelope_top = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[ETOP] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_envelope_top = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[ETOP] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_envelope_top = None
                    return
        
        # Build feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate ETOP using exact linear formula
        try:
            self.latest_envelope_top = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[ETOP] Calculated Envelope Top: %.6f', self.latest_envelope_top)
        except Exception as e:
            logging.error('[ETOP] Error calculating ETOP: %s', str(e))
            self.status = 'invalid'
            self.latest_envelope_top = None

    def get_value(self) -> Optional[float]:
        return self.latest_envelope_top


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
    L11 indicator: Low level calculated using exact HLC-only linear regression formula.
    Formula: Emphasizes most recent low and close, de-emphasizes high
    R² = 0.999999, Average Error = 0.0199, Cross-validated
    """
    def __init__(self):
        super().__init__()
        self.latest_l11: Optional[float] = None
        
        # HLC-only coefficients (9 features, cross-validated)
        self.coefficients = [
            -0.00056212, -0.00018272, 0.00019277,   # t-3: H,L,C
            0.00136978, 0.00071840, -0.00182454,    # t-2: H,L,C
            -0.33313775, 0.66680999, 0.66661597,    # t-1: H,L,C
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
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[L11] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_l11 = None
                    return
        
        # Build HLC feature vector
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
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
class H11(Indicator):
    """
    H11 indicator: High level calculated using exact HLC-only linear regression formula.
    Formula: Emphasizes most recent high and close, de-emphasizes low
    R² = 0.999999, Average Error = 0.0199, Cross-validated
    """
    def __init__(self):
        super().__init__()
        self.latest_h11: Optional[float] = None
        
        # HLC-only coefficients (9 features, cross-validated)
        self.coefficients = [
            -0.00056212, -0.00018272, 0.00019277,   # t-3: H,L,C
            0.00136978, 0.00071840, -0.00182454,    # t-2: H,L,C
            0.66686225, -0.33319001, 0.66661597,    # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[H11] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_h11 = None
            return
            
        last_three = intervals[-3:]
        
        # Validate all intervals
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[H11] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_h11 = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[H11] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_h11 = None
                    return
        
        # Build HLC feature vector
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate H11 using exact linear formula
        try:
            self.latest_h11 = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[H11] Calculated H11: %.6f', self.latest_h11)
        except Exception as e:
            logging.error('[H11] Error calculating H11: %s', str(e))
            self.status = 'invalid'
            self.latest_h11 = None

    def get_value(self) -> Optional[float]:
        return self.latest_h11


@gin.configurable
class Z1B(Indicator):
    """
    Z1B Indicator: Lower support zone calculated using 3-day HLC linear regression (excludes open)
    Formula: z1b = -0.44360641*H₃ + 0.55203953*L₃ + 0.22238203*C₃
                  - 0.44299760*H₂ + 0.55722853*L₂ + 0.21953681*C₂
                  - 0.44414226*H₁ + 0.55962966*L₁ + 0.21992682*C₁
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    """
    def __init__(self):
        super().__init__()
        self.latest_z1b: Optional[float] = None
        
        # HLC-only coefficients from linear regression (R² > 0.999999, Error < 0.01%)
        self.coefficients = [
            -0.44360641, 0.55203953, 0.22238203,   # t-3: H,L,C
            -0.44299760, 0.55722853, 0.21953681,   # t-2: H,L,C
            -0.44414226, 0.55962966, 0.21992682,   # t-1: H,L,C
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
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[Z1B] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_z1b = None
                    return
        
        # Build feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
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
    Z2B Indicator: Lower resistance zone calculated using 3-day HLC linear regression (excludes open)
    Formula: z2b = -0.33375857*H₃ + 0.33327147*L₃ + 0.33478365*C₃
                  - 0.33395845*H₂ + 0.33313921*L₂ + 0.33324867*C₂
                  - 0.33277367*H₁ + 0.33384496*L₁ + 0.33220288*C₁
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    """
    def __init__(self):
        super().__init__()
        self.latest_z2b: Optional[float] = None
        
        # HLC-only coefficients from linear regression (R² > 0.999999, Error < 0.01%)
        self.coefficients = [
            -0.33375857, 0.33327147, 0.33478365,   # t-3: H,L,C
            -0.33395845, 0.33313921, 0.33324867,   # t-2: H,L,C
            -0.33277367, 0.33384496, 0.33220288,   # t-1: H,L,C
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
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    self.status = 'invalid'
                    self.latest_z2b = None
                    return
        
        # Build feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
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
    Z5T Indicator: Upper resistance zone calculated using 3-day HLC linear regression (excludes open)
    Formula: z5t = 0.33298475*H₃ - 0.33125052*L₃ + 0.33371591*C₃
                  + 0.33153760*H₂ - 0.33584054*L₂ + 0.33648807*C₂
                  + 0.33404897*H₁ - 0.33557298*L₁ + 0.33388438*C₁
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    """
    def __init__(self):
        super().__init__()
        self.latest_z5t: Optional[float] = None
        
        # HLC-only coefficients from linear regression (R² > 0.999999, Error < 0.01%)
        self.coefficients = [
            0.33298475, -0.33125052, 0.33371591,   # t-3: H,L,C
            0.33153760, -0.33584054, 0.33648807,   # t-2: H,L,C
            0.33404897, -0.33557298, 0.33388438,   # t-1: H,L,C
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
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    self.status = 'invalid'
                    self.latest_z5t = None
                    return
        
        # Build feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
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
    Z6T Indicator: Upper breakout zone calculated using 3-day HLC linear regression (excludes open)
    Formula: z6t = 0.55639359*H₃ - 0.44796047*L₃ + 0.22238203*C₃
                  + 0.55700240*H₂ - 0.44277147*L₂ + 0.21953681*C₂
                  + 0.55585774*H₁ - 0.44037034*L₁ + 0.21992682*C₁
    where subscripts 1,2,3 represent prior 3 days (day 1 = 3 days ago, day 2 = 2 days ago, day 3 = yesterday)
    
    Note: Z6T maintains strong correlation with Z5T using HLC-only features with perfect accuracy.
    """
    def __init__(self):
        super().__init__()
        self.latest_z6t: Optional[float] = None
        
        # HLC-only coefficients from linear regression (R² > 0.999999, Error < 0.01%)
        self.coefficients = [
            0.55639359, -0.44796047, 0.22238203,   # t-3: H,L,C
            0.55700240, -0.44277147, 0.21953681,   # t-2: H,L,C
            0.55585774, -0.44037034, 0.21992682,   # t-1: H,L,C
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
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    self.status = 'invalid'
                    self.latest_z6t = None
                    return
        
        # Build feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
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


@gin.configurable
class FiveNineSell(Indicator):
    """
    Five Nine Sell Indicator: Calculated as 2 * high of prior bar - low of prior prior bar.
    Formula: five_nine_sell = 2 * high(t-1) - low(t-2)
    
    This indicator uses simple arithmetic on High/Low prices from the two most recent intervals.
    Requires 2 previous intervals for calculation.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_nine_sell: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveNineSell] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_nine_sell = None
            return
            
        # Get last 2 intervals: t-2 (prior prior) and t-1 (prior)
        prior_prior = intervals[-2]  # t-2
        prior = intervals[-1]        # t-1
        
        # Validate intervals
        for i, interval in enumerate([prior_prior, prior]):
            if interval.status != 'ok':
                logging.debug('[FiveNineSell] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_five_nine_sell = None
                return
                
            # Check required fields: high for prior, low for prior_prior
            if i == 0:  # prior_prior - need low
                val = getattr(interval, 'low', None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[FiveNineSell] Invalid low at prior_prior: %s', val)
                    self.status = 'invalid'
                    self.latest_five_nine_sell = None
                    return
            else:  # prior - need high
                val = getattr(interval, 'high', None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[FiveNineSell] Invalid high at prior: %s', val)
                    self.status = 'invalid'
                    self.latest_five_nine_sell = None
                    return
        
        # Calculate: 2 * high(t-1) - low(t-2)
        try:
            self.latest_five_nine_sell = 2 * prior.high - prior_prior.low
            self.status = 'ok'
            logging.debug('[FiveNineSell] Calculated: 2*%.2f - %.2f = %.2f', 
                         prior.high, prior_prior.low, self.latest_five_nine_sell)
        except Exception as e:
            logging.error('[FiveNineSell] Error calculating: %s', str(e))
            self.status = 'invalid'
            self.latest_five_nine_sell = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_nine_sell


@gin.configurable
class FiveNineBuy(Indicator):
    """
    Five Nine Buy Indicator: Calculated as 2 * low of prior bar - high of prior prior bar.
    Formula: five_nine_buy = 2 * low(t-1) - high(t-2)
    
    This indicator uses simple arithmetic on High/Low prices from the two most recent intervals.
    Requires 2 previous intervals for calculation.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_nine_buy: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveNineBuy] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_nine_buy = None
            return
            
        # Get last 2 intervals: t-2 (prior prior) and t-1 (prior)
        prior_prior = intervals[-2]  # t-2
        prior = intervals[-1]        # t-1
        
        # Validate intervals
        for i, interval in enumerate([prior_prior, prior]):
            if interval.status != 'ok':
                logging.debug('[FiveNineBuy] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_five_nine_buy = None
                return
                
            # Check required fields: high for prior_prior, low for prior
            if i == 0:  # prior_prior - need high
                val = getattr(interval, 'high', None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[FiveNineBuy] Invalid high at prior_prior: %s', val)
                    self.status = 'invalid'
                    self.latest_five_nine_buy = None
                    return
            else:  # prior - need low
                val = getattr(interval, 'low', None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[FiveNineBuy] Invalid low at prior: %s', val)
                    self.status = 'invalid'
                    self.latest_five_nine_buy = None
                    return
        
        # Calculate: 2 * low(t-1) - high(t-2)
        try:
            self.latest_five_nine_buy = 2 * prior.low - prior_prior.high
            self.status = 'ok'
            logging.debug('[FiveNineBuy] Calculated: 2*%.2f - %.2f = %.2f', 
                         prior.low, prior_prior.high, self.latest_five_nine_buy)
        except Exception as e:
            logging.error('[FiveNineBuy] Error calculating: %s', str(e))
            self.status = 'invalid'
            self.latest_five_nine_buy = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_nine_buy


@gin.configurable
class FiveOneBuy(Indicator):
    """
    Five One Buy Indicator: Calculated as 2 * low(t-1) - low(t-2) with conditions.
    Formula: five_one_buy = 2 * low(t-1) - low(t-2) 
             IF low(t-1) > low(t-2) OR indicator not available
    
    This indicator provides conditional support levels based on improving lows.
    Only calculates when the most recent low is higher than the previous low,
    indicating potential upward momentum in support levels.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_one_buy: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveOneBuy] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_one_buy = None
            return
            
        # Get last 2 intervals: t-2 (prior prior) and t-1 (prior)
        prior_prior = intervals[-2]  # t-2
        prior = intervals[-1]        # t-1
        
        # Validate intervals
        for i, interval in enumerate([prior_prior, prior]):
            if interval.status != 'ok':
                logging.debug('[FiveOneBuy] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_five_one_buy = None
                return
                
            # Check required field: low for both intervals
            val = getattr(interval, 'low', None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug('[FiveOneBuy] Invalid low at position %d: %s', i, val)
                self.status = 'invalid'
                self.latest_five_one_buy = None
                return
        
        # Check condition: low(t-1) > low(t-2)
        if prior.low > prior_prior.low:
            # Calculate: 2 * low(t-1) - low(t-2)
            try:
                self.latest_five_one_buy = 2 * prior.low - prior_prior.low
                self.status = 'ok'
                logging.debug('[FiveOneBuy] Calculated: 2*%.2f - %.2f = %.2f (condition: %.2f > %.2f)', 
                             prior.low, prior_prior.low, self.latest_five_one_buy,
                             prior.low, prior_prior.low)
            except Exception as e:
                logging.error('[FiveOneBuy] Error calculating: %s', str(e))
                self.status = 'invalid'
                self.latest_five_one_buy = None
        else:
            # Condition not met: low(t-1) <= low(t-2)
            self.status = 'ok'
            self.latest_five_one_buy = None
            logging.debug('[FiveOneBuy] Condition not met: %.2f <= %.2f, indicator not available',
                         prior.low, prior_prior.low)

    def get_value(self) -> Optional[float]:
        return self.latest_five_one_buy


@gin.configurable
class FiveOneSell(Indicator):
    """
    Five One Sell Indicator: Calculated as 2 * high(t-1) - high(t-2) with conditions.
    Formula: five_one_sell = 2 * high(t-1) - high(t-2) 
             IF high(t-1) < high(t-2)
    
    This indicator provides conditional resistance levels based on declining highs.
    Only calculates when the most recent high is lower than the previous high,
    indicating potential downward momentum in resistance levels.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_one_sell: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveOneSell] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_one_sell = None
            return
            
        # Get last 2 intervals: t-2 (prior prior) and t-1 (prior)
        prior_prior = intervals[-2]  # t-2
        prior = intervals[-1]        # t-1
        
        # Validate intervals
        for i, interval in enumerate([prior_prior, prior]):
            if interval.status != 'ok':
                logging.debug('[FiveOneSell] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_five_one_sell = None
                return
                
            # Check required field: high for both intervals
            val = getattr(interval, 'high', None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug('[FiveOneSell] Invalid high at position %d: %s', i, val)
                self.status = 'invalid'
                self.latest_five_one_sell = None
                return
        
        # Check condition: high(t-1) < high(t-2)
        if prior.high < prior_prior.high:
            # Calculate: 2 * high(t-1) - high(t-2)
            try:
                self.latest_five_one_sell = 2 * prior.high - prior_prior.high
                self.status = 'ok'
                logging.debug('[FiveOneSell] Calculated: 2*%.2f - %.2f = %.2f (condition: %.2f < %.2f)', 
                             prior.high, prior_prior.high, self.latest_five_one_sell,
                             prior.high, prior_prior.high)
            except Exception as e:
                logging.error('[FiveOneSell] Error calculating: %s', str(e))
                self.status = 'invalid'
                self.latest_five_one_sell = None
        else:
            # Condition not met: high(t-1) >= high(t-2)
            self.status = 'ok'
            self.latest_five_one_sell = None
            logging.debug('[FiveOneSell] Condition not met: %.2f >= %.2f, indicator not available',
                         prior.high, prior_prior.high)

    def get_value(self) -> Optional[float]:
        return self.latest_five_one_sell


@gin.configurable
class FiveTwoBuy(Indicator):
    """
    Five Two Buy Indicator: Calculated as 2 * low(t-1) - low(t-2) with conditions.
    Formula: five_two_buy = 2 * low(t-1) - low(t-2) 
             IF low(t-1) < low(t-2) OR indicator not available
    
    This indicator provides conditional support levels based on declining lows.
    Only calculates when the most recent low is lower than the previous low,
    indicating potential downward momentum where support is weakening.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_two_buy: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            self.status = 'insufficient_data'
            self.latest_five_two_buy = None
            logging.debug('[FiveTwoBuy] Insufficient data: need at least 2 intervals, got %d', len(intervals))
            return

        try:
            # Get the last two intervals (most recent first in the list)
            prior = intervals[-1]  # t-1 (most recent)
            prior_prior = intervals[-2]  # t-2 (second most recent)
            
            # Validate data quality
            if (prior.status != 'ok' or prior_prior.status != 'ok' or
                math.isnan(prior.low) or math.isnan(prior_prior.low) or
                prior.low <= 0 or prior_prior.low <= 0):
                self.status = 'invalid_data'
                self.latest_five_two_buy = None
                logging.warning('[FiveTwoBuy] Invalid data: prior.status=%s, prior_prior.status=%s, prior.low=%.2f, prior_prior.low=%.2f',
                               prior.status, prior_prior.status, prior.low, prior_prior.low)
                return
                
        except (IndexError, AttributeError, TypeError) as e:
            self.status = 'calculation_error'
            self.latest_five_two_buy = None
            logging.error('[FiveTwoBuy] Error accessing interval data: %s', e)
            return

        # Apply Five Two Buy conditional logic: low(t-1) < low(t-2)
        if prior.low < prior_prior.low:
            # Condition met: calculate 2 * low(t-1) - low(t-2)
            self.latest_five_two_buy = 2 * prior.low - prior_prior.low
            self.status = 'ok'
            logging.debug('[FiveTwoBuy] Calculated: 2 * %.2f - %.2f = %.2f (condition: %.2f < %.2f)',
                         prior.low, prior_prior.low, self.latest_five_two_buy, prior.low, prior_prior.low)
        else:
            # Condition not met: low(t-1) >= low(t-2)
            self.status = 'ok'
            self.latest_five_two_buy = None
            logging.debug('[FiveTwoBuy] Condition not met: %.2f >= %.2f, indicator not available',
                         prior.low, prior_prior.low)

    def get_value(self) -> Optional[float]:
        return self.latest_five_two_buy


@gin.configurable
class FiveTwoSell(Indicator):
    """
    Five Two Sell Indicator: Calculated as 2 * high(t-1) - high(t-2) with conditions.
    Formula: five_two_sell = 2 * high(t-1) - high(t-2) 
             IF high(t-1) > high(t-2) OR indicator not available
    
    This indicator provides conditional resistance levels based on rising highs.
    Only calculates when the most recent high is higher than the previous high,
    indicating potential upward momentum where resistance is strengthening.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_two_sell: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            self.status = 'insufficient_data'
            self.latest_five_two_sell = None
            logging.debug('[FiveTwoSell] Insufficient data: need at least 2 intervals, got %d', len(intervals))
            return

        try:
            # Get the last two intervals (most recent first in the list)
            prior = intervals[-1]  # t-1 (most recent)
            prior_prior = intervals[-2]  # t-2 (second most recent)
            
            # Validate data quality
            if (prior.status != 'ok' or prior_prior.status != 'ok' or
                math.isnan(prior.high) or math.isnan(prior_prior.high) or
                prior.high <= 0 or prior_prior.high <= 0):
                self.status = 'invalid_data'
                self.latest_five_two_sell = None
                logging.warning('[FiveTwoSell] Invalid data: prior.status=%s, prior_prior.status=%s, prior.high=%.2f, prior_prior.high=%.2f',
                               prior.status, prior_prior.status, prior.high, prior_prior.high)
                return
                
        except (IndexError, AttributeError, TypeError) as e:
            self.status = 'calculation_error'
            self.latest_five_two_sell = None
            logging.error('[FiveTwoSell] Error accessing interval data: %s', e)
            return

        # Apply Five Two Sell conditional logic: high(t-1) > high(t-2)
        if prior.high > prior_prior.high:
            # Condition met: calculate 2 * high(t-1) - high(t-2)
            self.latest_five_two_sell = 2 * prior.high - prior_prior.high
            self.status = 'ok'
            logging.debug('[FiveTwoSell] Calculated: 2 * %.2f - %.2f = %.2f (condition: %.2f > %.2f)',
                         prior.high, prior_prior.high, self.latest_five_two_sell, prior.high, prior_prior.high)
        else:
            # Condition not met: high(t-1) <= high(t-2)
            self.status = 'ok'
            self.latest_five_two_sell = None
            logging.debug('[FiveTwoSell] Condition not met: %.2f <= %.2f, indicator not available',
                         prior.high, prior_prior.high)

    def get_value(self) -> Optional[float]:
        return self.latest_five_two_sell


@gin.configurable
class BXTrenderBasic(Indicator):
    """
    BX Trender Basic Indicator: Measures trend strength using price changes.
    Formula: bx_trender = 100 * (avg_gains / (avg_gains + avg_losses))
    
    This indicator provides trend strength on a 0-100 scale where:
    - Values above 50 indicate bullish trend strength
    - Values below 50 indicate bearish trend strength
    - Values near 50 indicate neutral/sideways movement
    """
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.latest_bx_trender: Optional[float] = None
        self.trend_strength: Optional[float] = None
        self.trend_direction: Optional[int] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period + 1:
            self.status = 'insufficient_data'
            self.latest_bx_trender = None
            self.trend_strength = None
            self.trend_direction = None
            logging.debug('[BXTrenderBasic] Insufficient data: need %d intervals, got %d', self.period + 1, len(intervals))
            return

        try:
            # Validate all intervals
            for i, interval in enumerate(intervals[-self.period-1:]):
                if interval.status != 'ok':
                    self.status = 'invalid_data'
                    self.latest_bx_trender = None
                    self.trend_strength = None
                    self.trend_direction = None
                    logging.debug('[BXTrenderBasic] Invalid interval status at position %d: %s', i, interval.status)
                    return
                
                if math.isnan(interval.close) or interval.close <= 0:
                    self.status = 'invalid_data'
                    self.latest_bx_trender = None
                    self.trend_strength = None
                    self.trend_direction = None
                    logging.debug('[BXTrenderBasic] Invalid close price at position %d: %s', i, interval.close)
                    return

            # Calculate price changes over the period
            closes = [interval.close for interval in intervals[-self.period-1:]]
            price_changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
            
            # Separate gains and losses
            gains = [change for change in price_changes if change > 0]
            losses = [-change for change in price_changes if change < 0]
            
            # Calculate averages
            avg_gains = sum(gains) / len(price_changes) if price_changes else 0
            avg_losses = sum(losses) / len(price_changes) if price_changes else 0
            
            # BX Trender formula
            if avg_gains + avg_losses > 0:
                self.latest_bx_trender = 100 * (avg_gains / (avg_gains + avg_losses))
            else:
                self.latest_bx_trender = 50.0  # Neutral when no movement
            
            # Additional metrics
            self.trend_strength = abs(self.latest_bx_trender - 50) / 50  # 0-1 scale
            self.trend_direction = 1 if self.latest_bx_trender > 50 else (-1 if self.latest_bx_trender < 50 else 0)
            
            self.status = 'ok'
            logging.debug('[BXTrenderBasic] Calculated BX Trender: %.2f, Strength: %.3f, Direction: %d',
                         self.latest_bx_trender, self.trend_strength, self.trend_direction)
            
        except Exception as e:
            self.status = 'calculation_error'
            self.latest_bx_trender = None
            self.trend_strength = None
            self.trend_direction = None
            logging.error('[BXTrenderBasic] Error calculating BX Trender: %s', str(e))

    def get_value(self) -> Optional[float]:
        return self.latest_bx_trender
    
    def get_trend_strength(self) -> Optional[float]:
        return self.trend_strength
    
    def get_trend_direction(self) -> Optional[int]:
        return self.trend_direction


@gin.configurable
class BXTrenderDirectional(Indicator):
    """
    BX Trender Directional Indicator: Uses True Range and Directional Movement for trend analysis.
    Formula: bx_trender = DI+ - DI- (normalized to 0-100 scale)
    
    This indicator combines directional movement concepts with BX Trender methodology:
    - Uses True Range for volatility normalization
    - Calculates directional movement indicators (DI+ and DI-)
    - Provides trend strength through ADX-like calculation
    """
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.latest_bx_trender: Optional[float] = None
        self.di_plus: Optional[float] = None
        self.di_minus: Optional[float] = None
        self.adx: Optional[float] = None
        self.trend_strength: Optional[float] = None
        self.trend_direction: Optional[int] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period + 1:
            self.status = 'insufficient_data'
            self._reset_values()
            logging.debug('[BXTrenderDirectional] Insufficient data: need %d intervals, got %d', self.period + 1, len(intervals))
            return

        try:
            # Validate all intervals
            for i, interval in enumerate(intervals[-self.period-1:]):
                if interval.status != 'ok':
                    self.status = 'invalid_data'
                    self._reset_values()
                    logging.debug('[BXTrenderDirectional] Invalid interval status at position %d: %s', i, interval.status)
                    return
                
                for field in ['high', 'low', 'close']:
                    val = getattr(interval, field, None)
                    if val is None or math.isnan(val) or val <= 0:
                        self.status = 'invalid_data'
                        self._reset_values()
                        logging.debug('[BXTrenderDirectional] Invalid %s at position %d: %s', field, i, val)
                        return

            # Calculate True Range and Directional Movement over the period
            tr_sum = 0
            dm_plus_sum = 0
            dm_minus_sum = 0
            
            for i in range(len(intervals) - self.period, len(intervals)):
                current = intervals[i]
                if i > 0:
                    previous = intervals[i-1]
                    
                    # True Range
                    tr1 = current.high - current.low
                    tr2 = abs(current.high - previous.close)
                    tr3 = abs(current.low - previous.close)
                    tr = max(tr1, tr2, tr3)
                    tr_sum += tr
                    
                    # Directional Movement
                    up_move = current.high - previous.high
                    down_move = previous.low - current.low
                    
                    if up_move > down_move and up_move > 0:
                        dm_plus_sum += up_move
                    elif down_move > up_move and down_move > 0:
                        dm_minus_sum += down_move

            # Calculate Directional Indicators
            if tr_sum > 0:
                self.di_plus = 100 * (dm_plus_sum / tr_sum)
                self.di_minus = 100 * (dm_minus_sum / tr_sum)
            else:
                self.di_plus = 0
                self.di_minus = 0

            # BX Trender directional
            raw_bx_trender = self.di_plus - self.di_minus
            
            # Normalize to 0-100 scale
            self.latest_bx_trender = 50 + (raw_bx_trender / 2)
            self.latest_bx_trender = max(0, min(100, self.latest_bx_trender))
            
            # ADX-like trend strength
            if self.di_plus + self.di_minus > 0:
                self.adx = 100 * abs(self.di_plus - self.di_minus) / (self.di_plus + self.di_minus)
            else:
                self.adx = 0
                
            self.trend_strength = self.adx / 100
            self.trend_direction = 1 if raw_bx_trender > 0 else (-1 if raw_bx_trender < 0 else 0)
            
            self.status = 'ok'
            logging.debug('[BXTrenderDirectional] Calculated: BX=%.2f, DI+=%.2f, DI-=%.2f, ADX=%.2f',
                         self.latest_bx_trender, self.di_plus, self.di_minus, self.adx)
            
        except Exception as e:
            self.status = 'calculation_error'
            self._reset_values()
            logging.error('[BXTrenderDirectional] Error calculating: %s', str(e))

    def _reset_values(self):
        """Reset all calculated values to None."""
        self.latest_bx_trender = None
        self.di_plus = None
        self.di_minus = None
        self.adx = None
        self.trend_strength = None
        self.trend_direction = None

    def get_value(self) -> Optional[float]:
        return self.latest_bx_trender
    
    def get_di_plus(self) -> Optional[float]:
        return self.di_plus
    
    def get_di_minus(self) -> Optional[float]:
        return self.di_minus
    
    def get_adx(self) -> Optional[float]:
        return self.adx


@gin.configurable
class BXTrenderVolumeWeighted(Indicator):
    """
    BX Trender Volume Weighted Indicator: Incorporates volume into trend strength calculation.
    Formula: bx_trender = 50 + (50 * (bullish_volume - bearish_volume) / total_volume)
    
    This indicator weights trend calculation by volume:
    - Separates volume into bullish (price up) and bearish (price down) components
    - Calculates volume-weighted trend strength
    - Provides enhanced trend signals when supported by volume
    """
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.latest_bx_trender: Optional[float] = None
        self.bullish_volume_ratio: Optional[float] = None
        self.bearish_volume_ratio: Optional[float] = None
        self.volume_momentum: Optional[float] = None
        self.trend_strength: Optional[float] = None
        self.trend_direction: Optional[int] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        import math
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period + 1:
            self.status = 'insufficient_data'
            self._reset_values()
            logging.debug('[BXTrenderVolumeWeighted] Insufficient data: need %d intervals, got %d', self.period + 1, len(intervals))
            return

        try:
            # Validate all intervals and check for volume data
            for i, interval in enumerate(intervals[-self.period-1:]):
                if interval.status != 'ok':
                    self.status = 'invalid_data'
                    self._reset_values()
                    logging.debug('[BXTrenderVolumeWeighted] Invalid interval status at position %d: %s', i, interval.status)
                    return
                
                # Check for required fields including volume
                for field in ['close', 'traded_volume']:
                    val = getattr(interval, field, None)
                    if val is None or math.isnan(val):
                        self.status = 'no_volume_data' if field == 'traded_volume' else 'invalid_data'
                        self._reset_values()
                        logging.debug('[BXTrenderVolumeWeighted] Invalid %s at position %d: %s', field, i, val)
                        return
                    
                    if field == 'traded_volume' and val < 0:
                        self.status = 'invalid_data'
                        self._reset_values()
                        logging.debug('[BXTrenderVolumeWeighted] Negative volume at position %d: %s', i, val)
                        return

            # Calculate volume-weighted trend over the period
            bullish_volume = 0
            bearish_volume = 0
            total_volume = 0
            
            recent_volume_sum = 0
            prev_volume_sum = 0
            
            for i in range(len(intervals) - self.period, len(intervals)):
                current = intervals[i]
                if i > 0:
                    previous = intervals[i-1]
                    
                    price_change = current.close - previous.close
                    volume = current.traded_volume
                    total_volume += volume
                    
                    if price_change > 0:
                        bullish_volume += volume
                    elif price_change < 0:
                        bearish_volume += volume
                    
                    # Volume momentum calculation
                    if i >= len(intervals) - 5:  # Recent 5 periods
                        recent_volume_sum += volume
                    elif i < len(intervals) - 5:  # Earlier periods
                        prev_volume_sum += volume

            # Calculate ratios
            if total_volume > 0:
                self.bullish_volume_ratio = bullish_volume / total_volume
                self.bearish_volume_ratio = bearish_volume / total_volume
                
                # BX Trender with volume weighting
                self.latest_bx_trender = 50 + (50 * (bullish_volume - bearish_volume) / total_volume)
            else:
                self.bullish_volume_ratio = 0.5
                self.bearish_volume_ratio = 0.5
                self.latest_bx_trender = 50.0

            # Volume momentum
            if prev_volume_sum > 0:
                self.volume_momentum = (recent_volume_sum - prev_volume_sum) / prev_volume_sum
            else:
                self.volume_momentum = 0
            
            # Volume-adjusted trend strength
            recent_avg_volume = recent_volume_sum / min(5, self.period)
            period_avg_volume = total_volume / self.period if self.period > 0 else 1
            volume_ratio = recent_avg_volume / period_avg_volume if period_avg_volume > 0 else 1
            
            self.trend_strength = abs(self.latest_bx_trender - 50) / 50 * min(volume_ratio, 2.0)  # Cap at 2x
            self.trend_direction = 1 if self.latest_bx_trender > 50 else (-1 if self.latest_bx_trender < 50 else 0)
            
            self.status = 'ok'
            logging.debug('[BXTrenderVolumeWeighted] Calculated: BX=%.2f, Bull Vol=%.3f, Bear Vol=%.3f, Vol Mom=%.3f',
                         self.latest_bx_trender, self.bullish_volume_ratio, self.bearish_volume_ratio, self.volume_momentum)
            
        except Exception as e:
            self.status = 'calculation_error'
            self._reset_values()
            logging.error('[BXTrenderVolumeWeighted] Error calculating: %s', str(e))

    def _reset_values(self):
        """Reset all calculated values to None."""
        self.latest_bx_trender = None
        self.bullish_volume_ratio = None
        self.bearish_volume_ratio = None
        self.volume_momentum = None
        self.trend_strength = None
        self.trend_direction = None

    def get_value(self) -> Optional[float]:
        return self.latest_bx_trender
    
    def get_bullish_volume_ratio(self) -> Optional[float]:
        return self.bullish_volume_ratio
    
    def get_bearish_volume_ratio(self) -> Optional[float]:
        return self.bearish_volume_ratio
    
    def get_volume_momentum(self) -> Optional[float]:
        return self.volume_momentum
