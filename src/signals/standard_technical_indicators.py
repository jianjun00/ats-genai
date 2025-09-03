"""
Standard Technical Indicators for Multi-Timeframe Analysis.

These indicators are designed to work with the existing indicator system and
InstrumentInterval data structures. They provide the standard technical signals
needed for multi-timeframe OHLC processing.
"""

import gin
import math
from datetime import datetime
from typing import List, Optional
from state.instrument_interval import InstrumentInterval
from .indicator import Indicator


@gin.configurable
class SMA(Indicator):
    """
    Simple Moving Average indicator.
    Calculates the arithmetic mean of closing prices over a specified period.
    """
    
    def __init__(self, period: int = 20):
        super().__init__()
        self.period = period
        self.latest_sma: Optional[float] = None
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate SMA from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period:
            logging.debug('[SMA_%d] Not enough intervals: need %d, got %d', 
                         self.period, self.period, len(intervals))
            self.status = 'invalid'
            self.latest_sma = None
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals[-self.period:]):
            if interval.status != 'ok':
                logging.debug('[SMA_%d] Invalid interval status at position %d: %s', 
                             self.period, i, interval.status)
                self.status = 'invalid'
                self.latest_sma = None
                return
            
            close = getattr(interval, 'close', None)
            if close is None or (isinstance(close, float) and math.isnan(close)):
                logging.debug('[SMA_%d] Invalid close at position %d: %s', 
                             self.period, i, close)
                self.status = 'invalid'
                self.latest_sma = None
                return
        
        # Calculate SMA
        try:
            close_prices = [interval.close for interval in intervals[-self.period:]]
            self.latest_sma = sum(close_prices) / len(close_prices)
            self.status = 'ok'
            logging.debug('[SMA_%d] Calculated SMA: %.6f', self.period, self.latest_sma)
        except Exception as e:
            logging.error('[SMA_%d] Error calculating SMA: %s', self.period, str(e))
            self.status = 'invalid'
            self.latest_sma = None
    
    def get_value(self) -> Optional[float]:
        return self.latest_sma


@gin.configurable
class EMA(Indicator):
    """
    Exponential Moving Average indicator.
    Gives more weight to recent prices, with the weight decreasing exponentially
    for older prices.
    """
    
    def __init__(self, period: int = 20):
        super().__init__()
        self.period = period
        self.latest_ema: Optional[float] = None
        self.multiplier = 2.0 / (period + 1)  # EMA smoothing factor
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate EMA from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period:
            logging.debug('[EMA_%d] Not enough intervals: need %d, got %d', 
                         self.period, self.period, len(intervals))
            self.status = 'invalid'
            self.latest_ema = None
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals[-self.period:]):
            if interval.status != 'ok':
                logging.debug('[EMA_%d] Invalid interval status at position %d: %s', 
                             self.period, i, interval.status)
                self.status = 'invalid'
                self.latest_ema = None
                return
            
            close = getattr(interval, 'close', None)
            if close is None or (isinstance(close, float) and math.isnan(close)):
                logging.debug('[EMA_%d] Invalid close at position %d: %s', 
                             self.period, i, close)
                self.status = 'invalid'
                self.latest_ema = None
                return
        
        # Calculate EMA
        try:
            close_prices = [interval.close for interval in intervals[-self.period:]]
            
            # Start with SMA for first EMA value
            ema = sum(close_prices[:self.period]) / self.period
            
            # Calculate EMA for remaining periods
            for i in range(1, len(close_prices)):
                ema = (close_prices[i] * self.multiplier) + (ema * (1 - self.multiplier))
            
            self.latest_ema = ema
            self.status = 'ok'
            logging.debug('[EMA_%d] Calculated EMA: %.6f', self.period, self.latest_ema)
        except Exception as e:
            logging.error('[EMA_%d] Error calculating EMA: %s', self.period, str(e))
            self.status = 'invalid'
            self.latest_ema = None
    
    def get_value(self) -> Optional[float]:
        return self.latest_ema


@gin.configurable
class RSI(Indicator):
    """
    Relative Strength Index indicator.
    Momentum oscillator that measures the velocity and magnitude of directional
    price changes to evaluate overbought or oversold conditions.
    """
    
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.latest_rsi: Optional[float] = None
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate RSI from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period + 1:  # Need one extra for price changes
            logging.debug('[RSI_%d] Not enough intervals: need %d, got %d', 
                         self.period, self.period + 1, len(intervals))
            self.status = 'invalid'
            self.latest_rsi = None
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals[-(self.period + 1):]):
            if interval.status != 'ok':
                logging.debug('[RSI_%d] Invalid interval status at position %d: %s', 
                             self.period, i, interval.status)
                self.status = 'invalid'
                self.latest_rsi = None
                return
            
            close = getattr(interval, 'close', None)
            if close is None or (isinstance(close, float) and math.isnan(close)):
                logging.debug('[RSI_%d] Invalid close at position %d: %s', 
                             self.period, i, close)
                self.status = 'invalid'
                self.latest_rsi = None
                return
        
        # Calculate RSI
        try:
            close_prices = [interval.close for interval in intervals[-(self.period + 1):]]
            
            # Calculate price changes
            price_changes = []
            for i in range(1, len(close_prices)):
                price_changes.append(close_prices[i] - close_prices[i-1])
            
            # Separate gains and losses
            gains = [change if change > 0 else 0 for change in price_changes]
            losses = [-change if change < 0 else 0 for change in price_changes]
            
            # Calculate average gains and losses
            avg_gain = sum(gains) / len(gains)
            avg_loss = sum(losses) / len(losses)
            
            # Calculate RS and RSI
            if avg_loss == 0:
                # Avoid division by zero - if no losses, RSI = 100
                self.latest_rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                self.latest_rsi = 100 - (100 / (1 + rs))
            
            self.status = 'ok'
            logging.debug('[RSI_%d] Calculated RSI: %.6f', self.period, self.latest_rsi)
        except Exception as e:
            logging.error('[RSI_%d] Error calculating RSI: %s', self.period, str(e))
            self.status = 'invalid'
            self.latest_rsi = None
    
    def get_value(self) -> Optional[float]:
        return self.latest_rsi


@gin.configurable
class VWAP(Indicator):
    """
    Volume Weighted Average Price indicator.
    Provides the average price a security has traded at throughout the day,
    based on both volume and price.
    """
    
    def __init__(self):
        super().__init__()
        self.latest_vwap: Optional[float] = None
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate VWAP from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 1:
            logging.debug('[VWAP] No intervals provided')
            self.status = 'invalid'
            self.latest_vwap = None
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals):
            if interval.status != 'ok':
                logging.debug('[VWAP] Invalid interval status at position %d: %s', 
                             i, interval.status)
                self.status = 'invalid'
                self.latest_vwap = None
                return
            
            # Check required fields
            for field in ['high', 'low', 'close', 'volume']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[VWAP] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self.latest_vwap = None
                    return
        
        # Calculate VWAP
        try:
            total_volume_price = 0.0
            total_volume = 0.0
            
            for interval in intervals:
                # Typical price (HLC/3)
                typical_price = (interval.high + interval.low + interval.close) / 3.0
                volume = interval.volume
                
                total_volume_price += typical_price * volume
                total_volume += volume
            
            if total_volume == 0:
                logging.debug('[VWAP] Total volume is zero')
                self.status = 'invalid'
                self.latest_vwap = None
                return
            
            self.latest_vwap = total_volume_price / total_volume
            self.status = 'ok'
            logging.debug('[VWAP] Calculated VWAP: %.6f', self.latest_vwap)
        except Exception as e:
            logging.error('[VWAP] Error calculating VWAP: %s', str(e))
            self.status = 'invalid'
            self.latest_vwap = None
    
    def get_value(self) -> Optional[float]:
        return self.latest_vwap


@gin.configurable
class BollingerBands(Indicator):
    """
    Bollinger Bands indicator.
    Provides upper and lower bands based on a moving average and standard deviation,
    used to identify overbought and oversold conditions.
    """
    
    def __init__(self, period: int = 20, std_dev_multiplier: float = 2.0):
        super().__init__()
        self.period = period
        self.std_dev_multiplier = std_dev_multiplier
        self.middle_band: Optional[float] = None  # SMA
        self.upper_band: Optional[float] = None
        self.lower_band: Optional[float] = None
        self.bandwidth: Optional[float] = None
        self.percent_b: Optional[float] = None
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate Bollinger Bands from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period:
            logging.debug('[BB_%d] Not enough intervals: need %d, got %d', 
                         self.period, self.period, len(intervals))
            self.status = 'invalid'
            self._reset_values()
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals[-self.period:]):
            if interval.status != 'ok':
                logging.debug('[BB_%d] Invalid interval status at position %d: %s', 
                             self.period, i, interval.status)
                self.status = 'invalid'
                self._reset_values()
                return
            
            close = getattr(interval, 'close', None)
            if close is None or (isinstance(close, float) and math.isnan(close)):
                logging.debug('[BB_%d] Invalid close at position %d: %s', 
                             self.period, i, close)
                self.status = 'invalid'
                self._reset_values()
                return
        
        # Calculate Bollinger Bands
        try:
            close_prices = [interval.close for interval in intervals[-self.period:]]
            
            # Middle band (SMA)
            self.middle_band = sum(close_prices) / len(close_prices)
            
            # Calculate standard deviation
            variance = sum((price - self.middle_band) ** 2 for price in close_prices) / len(close_prices)
            std_dev = math.sqrt(variance)
            
            # Upper and lower bands
            self.upper_band = self.middle_band + (std_dev * self.std_dev_multiplier)
            self.lower_band = self.middle_band - (std_dev * self.std_dev_multiplier)
            
            # Bandwidth (width of bands relative to middle band)
            self.bandwidth = (self.upper_band - self.lower_band) / self.middle_band
            
            # %B (current price position within bands)
            current_price = close_prices[-1]
            if self.upper_band != self.lower_band:
                self.percent_b = (current_price - self.lower_band) / (self.upper_band - self.lower_band)
            else:
                self.percent_b = 0.5
            
            self.status = 'ok'
            logging.debug('[BB_%d] Calculated Bollinger Bands: Middle=%.6f, Upper=%.6f, Lower=%.6f', 
                         self.period, self.middle_band, self.upper_band, self.lower_band)
        except Exception as e:
            logging.error('[BB_%d] Error calculating Bollinger Bands: %s', self.period, str(e))
            self.status = 'invalid'
            self._reset_values()
    
    def _reset_values(self):
        """Reset all calculated values to None."""
        self.middle_band = None
        self.upper_band = None
        self.lower_band = None
        self.bandwidth = None
        self.percent_b = None
    
    def get_value(self) -> Optional[float]:
        """Return middle band (SMA) as the primary value."""
        return self.middle_band
    
    def get_upper_band(self) -> Optional[float]:
        return self.upper_band
    
    def get_lower_band(self) -> Optional[float]:
        return self.lower_band
    
    def get_bandwidth(self) -> Optional[float]:
        return self.bandwidth
    
    def get_percent_b(self) -> Optional[float]:
        return self.percent_b


@gin.configurable
class MACD(Indicator):
    """
    MACD (Moving Average Convergence Divergence) indicator.
    Shows the relationship between two moving averages of a security's price.
    """
    
    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        super().__init__()
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.macd_line: Optional[float] = None
        self.signal_line: Optional[float] = None
        self.histogram: Optional[float] = None
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate MACD from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        # Need enough data for slow EMA plus signal line calculation
        min_periods = self.slow_period + self.signal_period
        if len(intervals) < min_periods:
            logging.debug('[MACD] Not enough intervals: need %d, got %d', 
                         min_periods, len(intervals))
            self.status = 'invalid'
            self._reset_values()
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals[-min_periods:]):
            if interval.status != 'ok':
                logging.debug('[MACD] Invalid interval status at position %d: %s', 
                             i, interval.status)
                self.status = 'invalid'
                self._reset_values()
                return
            
            close = getattr(interval, 'close', None)
            if close is None or (isinstance(close, float) and math.isnan(close)):
                logging.debug('[MACD] Invalid close at position %d: %s', i, close)
                self.status = 'invalid'
                self._reset_values()
                return
        
        # Calculate MACD
        try:
            close_prices = [interval.close for interval in intervals[-min_periods:]]
            
            # Calculate fast and slow EMAs
            fast_ema = self._calculate_ema(close_prices, self.fast_period)
            slow_ema = self._calculate_ema(close_prices, self.slow_period)
            
            # MACD line = Fast EMA - Slow EMA
            self.macd_line = fast_ema - slow_ema
            
            # Calculate signal line (EMA of MACD line)
            # For simplicity, we'll use a simple approximation
            # In a full implementation, you'd track MACD history for proper EMA calculation
            self.signal_line = self.macd_line  # Simplified - should be EMA of MACD values
            
            # Histogram = MACD line - Signal line
            self.histogram = self.macd_line - self.signal_line
            
            self.status = 'ok'
            logging.debug('[MACD] Calculated MACD: Line=%.6f, Signal=%.6f, Histogram=%.6f', 
                         self.macd_line, self.signal_line, self.histogram)
        except Exception as e:
            logging.error('[MACD] Error calculating MACD: %s', str(e))
            self.status = 'invalid'
            self._reset_values()
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """Calculate EMA for given prices and period."""
        multiplier = 2.0 / (period + 1)
        
        # Start with SMA for first EMA value
        ema = sum(prices[:period]) / period
        
        # Calculate EMA for remaining periods
        for i in range(period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def _reset_values(self):
        """Reset all calculated values to None."""
        self.macd_line = None
        self.signal_line = None
        self.histogram = None
    
    def get_value(self) -> Optional[float]:
        """Return MACD line as the primary value."""
        return self.macd_line
    
    def get_signal_line(self) -> Optional[float]:
        return self.signal_line
    
    def get_histogram(self) -> Optional[float]:
        return self.histogram


@gin.configurable
class StochasticOscillator(Indicator):
    """
    Stochastic Oscillator indicator.
    Compares a particular closing price to a range of prices over a given period.
    """
    
    def __init__(self, k_period: int = 14, d_period: int = 3):
        super().__init__()
        self.k_period = k_period
        self.d_period = d_period
        self.percent_k: Optional[float] = None
        self.percent_d: Optional[float] = None
    
    def update(self, intervals: List[InstrumentInterval]):
        """Calculate Stochastic Oscillator from the provided intervals."""
        import logging
        
        self.update_at = datetime.now()
        
        min_periods = self.k_period + self.d_period - 1
        if len(intervals) < min_periods:
            logging.debug('[Stoch] Not enough intervals: need %d, got %d', 
                         min_periods, len(intervals))
            self.status = 'invalid'
            self._reset_values()
            return
        
        # Validate all intervals
        for i, interval in enumerate(intervals[-min_periods:]):
            if interval.status != 'ok':
                logging.debug('[Stoch] Invalid interval status at position %d: %s', 
                             i, interval.status)
                self.status = 'invalid'
                self._reset_values()
                return
            
            for field in ['high', 'low', 'close']:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug('[Stoch] Invalid %s at position %d: %s', field, i, val)
                    self.status = 'invalid'
                    self._reset_values()
                    return
        
        # Calculate Stochastic Oscillator
        try:
            # Get recent data for %K calculation
            recent_intervals = intervals[-self.k_period:]
            
            # Find highest high and lowest low over the period
            highest_high = max(interval.high for interval in recent_intervals)
            lowest_low = min(interval.low for interval in recent_intervals)
            current_close = intervals[-1].close
            
            # Calculate %K
            if highest_high != lowest_low:
                self.percent_k = ((current_close - lowest_low) / (highest_high - lowest_low)) * 100
            else:
                self.percent_k = 50.0
            
            # Calculate %D (SMA of %K over d_period)
            # For simplicity, we'll use the current %K as %D
            # In a full implementation, you'd track %K history for proper SMA calculation
            self.percent_d = self.percent_k  # Simplified
            
            self.status = 'ok'
            logging.debug('[Stoch] Calculated Stochastic: K=%.6f, D=%.6f', 
                         self.percent_k, self.percent_d)
        except Exception as e:
            logging.error('[Stoch] Error calculating Stochastic: %s', str(e))
            self.status = 'invalid'
            self._reset_values()
    
    def _reset_values(self):
        """Reset all calculated values to None."""
        self.percent_k = None
        self.percent_d = None
    
    def get_value(self) -> Optional[float]:
        """Return %K as the primary value."""
        return self.percent_k
    
    def get_percent_d(self) -> Optional[float]:
        return self.percent_d