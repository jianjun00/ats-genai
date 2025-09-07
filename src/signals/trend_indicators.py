"""
Trend and Zone Indicators.

Contains indicators that identify trend strength, support/resistance zones,
and directional market movements including L11/H11 levels and Z-series zones.

Extracted from indicator.py to separate trend analysis indicators.
"""

import gin
import math
from datetime import datetime
from typing import List, Optional
from .base_indicator import Indicator
from state.instrument_interval import InstrumentInterval


@gin.configurable
class L11(Indicator):
    """
    L11 indicator: Low level calculated using exact HLC-only linear regression formula.
    Formula: Emphasizes most recent low and close, de-emphasizes high
    R² = 0.999996, Average Error = 0.0183
    """
    def __init__(self):
        super().__init__()
        self.latest_l11: Optional[float] = None
        
        # HLC-only coefficients optimized for low level prediction
        self.coefficients = [
            0.11306077, -0.10884779, 0.10864725,   # t-3: H,L,C
            -0.11441424, 0.31317815, 0.10686769,   # t-2: H,L,C
            -0.11171601, 0.41384294, 0.30939732,   # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[L11] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_l11 = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
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
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
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
    R² = 0.999996, Average Error = 0.0183
    """
    def __init__(self):
        super().__init__()
        self.latest_h11: Optional[float] = None
        
        # HLC-only coefficients optimized for high level prediction
        self.coefficients = [
            0.31306077, 0.10884779, 0.10864725,    # t-3: H,L,C
            0.41441424, -0.11317815, 0.10686769,   # t-2: H,L,C
            0.41171601, -0.11384294, 0.30939732,   # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[H11] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_h11 = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
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
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
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
    Formula: z1b = -0.44360641*H₃ + 0.55203953*L₃ + 0.22238203*C₃ + 0.33333333*(H₂+L₂+C₂) + 0.33333333*(H₁+L₁+C₁)
    """
    def __init__(self):
        super().__init__()
        self.latest_z1b: Optional[float] = None
        
        # Z1B coefficients for lower support zone
        self.coefficients = [
            -0.44360641, 0.55203953, 0.22238203,   # t-3: H,L,C
            0.11111111, 0.11111111, 0.11111111,    # t-2: H,L,C  
            0.11111111, 0.11111111, 0.11111111,    # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
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
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate Z1B using exact linear formula
        try:
            self.latest_z1b = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[Z1B] Calculated Z1B: %.6f', self.latest_z1b)
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
    Formula: z2b = -0.33375857*H₃ + 0.33327147*L₃ + 0.33478365*C₃ + coefficients for H₂L₂C₂ + H₁L₁C₁
    """
    def __init__(self):
        super().__init__()
        self.latest_z2b: Optional[float] = None
        
        # Z2B coefficients for lower resistance zone
        self.coefficients = [
            -0.33375857, 0.33327147, 0.33478365,   # t-3: H,L,C
            0.11111111, 0.11111111, 0.11111111,    # t-2: H,L,C  
            0.11111111, 0.11111111, 0.11111111,    # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[Z2B] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_z2b = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[Z2B] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_z2b = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[Z2B] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_z2b = None
                    return
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate Z2B using exact linear formula
        try:
            self.latest_z2b = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[Z2B] Calculated Z2B: %.6f', self.latest_z2b)
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
    Formula: z5t = 0.33298475*H₃ - 0.33125052*L₃ + 0.33371591*C₃ + coefficients for H₂L₂C₂ + H₁L₁C₁
    """
    def __init__(self):
        super().__init__()
        self.latest_z5t: Optional[float] = None
        
        # Z5T coefficients for upper resistance zone
        self.coefficients = [
            0.33298475, -0.33125052, 0.33371591,   # t-3: H,L,C
            0.11111111, 0.11111111, 0.11111111,    # t-2: H,L,C  
            0.11111111, 0.11111111, 0.11111111,    # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[Z5T] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_z5t = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[Z5T] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_z5t = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[Z5T] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_z5t = None
                    return
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate Z5T using exact linear formula
        try:
            self.latest_z5t = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[Z5T] Calculated Z5T: %.6f', self.latest_z5t)
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
    Formula: z6t = 0.55639359*H₃ - 0.44796047*L₃ + 0.22238203*C₃ + coefficients for H₂L₂C₂ + H₁L₁C₁
    """
    def __init__(self):
        super().__init__()
        self.latest_z6t: Optional[float] = None
        
        # Z6T coefficients for upper breakout zone
        self.coefficients = [
            0.55639359, -0.44796047, 0.22238203,   # t-3: H,L,C
            0.11111111, 0.11111111, 0.11111111,    # t-2: H,L,C  
            0.11111111, 0.11111111, 0.11111111,    # t-1: H,L,C
        ]

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 3:
            logging.debug('[Z6T] Not enough intervals: need 3, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_z6t = None
            return
            
        # Get last 3 intervals
        last_three = intervals[-3:]
        
        # Validate all intervals have valid OHLC data
        for i, interval in enumerate(last_three):
            if interval.status != 'ok':
                logging.debug('[Z6T] Invalid interval status at position %d: %s', i, interval.status)
                self.status = 'invalid'
                self.latest_z6t = None
                return
                
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[Z6T] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_z6t = None
                    return
        
        # Build HLC feature vector: 3 days × 3 HLC = 9 features
        features = []
        for interval in last_three:
            features.extend([interval.high, interval.low, interval.close])
        
        # Calculate Z6T using exact linear formula
        try:
            self.latest_z6t = sum(coef * feat for coef, feat in zip(self.coefficients, features))
            self.status = 'ok'
            logging.debug('[Z6T] Calculated Z6T: %.6f', self.latest_z6t)
        except Exception as e:
            logging.error('[Z6T] Error calculating Z6T: %s', str(e))
            self.status = 'invalid'
            self.latest_z6t = None

    def get_value(self) -> Optional[float]:
        return self.latest_z6t


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
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period + 1:
            self.status = 'insufficient_data'
            self.latest_bx_trender = None
            self.trend_strength = None
            self.trend_direction = None
            return
        
        # Get required intervals
        required_intervals = intervals[-(self.period + 1):]
        
        # Validate intervals
        for i, interval in enumerate(required_intervals):
            if interval.status != 'ok':
                logging.debug(f'[BXTrenderBasic] Invalid interval status at position {i}: {interval.status}')
                self.status = 'invalid'
                self.latest_bx_trender = None
                self.trend_strength = None
                self.trend_direction = None
                return
            
            if not hasattr(interval, 'close') or interval.close is None:
                logging.debug(f'[BXTrenderBasic] Missing close price at position {i}')
                self.status = 'invalid'
                self.latest_bx_trender = None
                self.trend_strength = None
                self.trend_direction = None
                return
        
        try:
            # Calculate price changes
            gains = []
            losses = []
            
            for i in range(1, len(required_intervals)):
                change = required_intervals[i].close - required_intervals[i-1].close
                if change > 0:
                    gains.append(change)
                elif change < 0:
                    losses.append(abs(change))
            
            # Calculate averages
            avg_gains = sum(gains) / len(gains) if gains else 0.0
            avg_losses = sum(losses) / len(losses) if losses else 0.0
            
            # Calculate BX Trender
            if avg_gains + avg_losses == 0:
                self.latest_bx_trender = 50.0  # Neutral
            else:
                self.latest_bx_trender = 100 * (avg_gains / (avg_gains + avg_losses))
            
            # Calculate trend strength and direction
            self.trend_strength = abs(self.latest_bx_trender - 50) / 50  # 0-1 scale
            self.trend_direction = 1 if self.latest_bx_trender > 50 else -1 if self.latest_bx_trender < 50 else 0
            
            self.status = 'ok'
            logging.debug(f'[BXTrenderBasic] BX Trender: {self.latest_bx_trender:.2f}, Strength: {self.trend_strength:.3f}, Direction: {self.trend_direction}')
            
        except Exception as e:
            logging.error(f'[BXTrenderBasic] Error calculating BX Trender: {str(e)}')
            self.status = 'invalid'
            self.latest_bx_trender = None
            self.trend_strength = None
            self.trend_direction = None

    def get_value(self) -> Optional[float]:
        return self.latest_bx_trender

    def get_trend_strength(self) -> Optional[float]:
        return self.trend_strength

    def get_trend_direction(self) -> Optional[int]:
        return self.trend_direction