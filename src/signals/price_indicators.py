"""
Price and Level Indicators.

Contains indicators that calculate price levels, dots, envelopes, and related
price-based technical analysis metrics.

Extracted from indicator.py to separate price-related indicators.
"""

import gin
import math
from datetime import datetime
from typing import List, Optional
from .base_indicator import Indicator
from state.instrument_interval import InstrumentInterval


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
        
        if len(intervals) == 0:
            self.status = 'invalid'
            self.latest_dot = None
            return
            
        last_interval = intervals[-1]
        
        if last_interval.status != 'ok':
            self.status = 'invalid'
            self.latest_dot = None
            return
        
        # Check for valid HLC data
        for field in ['high', 'low', 'close']:
            val = getattr(last_interval, field, None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug('[OneOneDot] Invalid %s: %s', field, val)
                self.status = 'invalid'
                self.latest_dot = None
                return
        
        # Calculate OneOneDot = (high + low + close) / 3
        self.latest_dot = (last_interval.high + last_interval.low + last_interval.close) / 3
        self.status = 'ok'
    
    def get_value(self) -> Optional[float]:
        return self.latest_dot


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
        
        self.update_at = datetime.now()
        
        if len(intervals) == 0:
            self.status = 'invalid'
            self.latest_high = None
            return
            
        last_interval = intervals[-1]
        
        if last_interval.status != 'ok':
            self.status = 'invalid'
            self.latest_high = None
            return
        
        # Check for valid HLC data
        for field in ['high', 'low', 'close']:
            val = getattr(last_interval, field, None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug('[OneOneHigh] Invalid %s: %s', field, val)
                self.status = 'invalid'
                self.latest_high = None
                return
        
        # Calculate OneOneDot = (high + low + close) / 3
        dot = (last_interval.high + last_interval.low + last_interval.close) / 3
        
        # Calculate OneOneHigh = 2*OneOneDot - low
        self.latest_high = 2 * dot - last_interval.low
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
        
        if len(intervals) == 0:
            self.status = 'invalid'
            self.latest_low = None
            return
            
        last_interval = intervals[-1]
        
        if last_interval.status != 'ok':
            self.status = 'invalid'
            self.latest_low = None
            return
        
        # Check for valid HLC data
        for field in ['high', 'low', 'close']:
            val = getattr(last_interval, field, None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug('[OneOneLow] Invalid %s: %s', field, val)
                self.status = 'invalid'
                self.latest_low = None
                return
        
        # Calculate OneOneDot = (high + low + close) / 3
        dot = (last_interval.high + last_interval.low + last_interval.close) / 3
        
        # Calculate OneOneLow = 2*OneOneDot - high
        self.latest_low = 2 * dot - last_interval.high
        self.status = 'ok'
    
    def get_value(self) -> Optional[float]:
        return self.latest_low


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
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[EnvelopeBot] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_envelope_bot = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[EnvelopeBot] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_envelope_bot = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[EnvelopeBot] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_envelope_bot = None
                    return
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate envelope_bot using exact linear formula
        try:
            self.latest_envelope_bot = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[EnvelopeBot] Calculated envelope_bot: %.6f', self.latest_envelope_bot)
        except Exception as e:
            logging.error('[EnvelopeBot] Error calculating envelope_bot: %s', str(e))
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
            0.22206190, -0.11115648, 0.22303212,   # t-3: H,L,C
            0.22439345, -0.11250983, 0.22120078,   # t-2: H,L,C
            0.22360772, -0.11109552, 0.22046378,   # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[EnvelopeTop] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_envelope_top = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[EnvelopeTop] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_envelope_top = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[EnvelopeTop] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_envelope_top = None
                    return
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate envelope_top using exact linear formula
        try:
            self.latest_envelope_top = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[EnvelopeTop] Calculated envelope_top: %.6f', self.latest_envelope_top)
        except Exception as e:
            logging.error('[EnvelopeTop] Error calculating envelope_top: %s', str(e))
            self.status = 'invalid'
            self.latest_envelope_top = None

    def get_value(self) -> Optional[float]:
        return self.latest_envelope_top