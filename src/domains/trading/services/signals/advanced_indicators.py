"""
Advanced Trend and Market Analysis Indicators.

Contains sophisticated indicators that combine multiple concepts like
directional movement, volume weighting, and advanced trend analysis.

Extracted from indicator.py to separate advanced analytical indicators.
"""

import gin
import math
from datetime import datetime
from typing import List, Optional
from .base_indicator import Indicator
from state.instrument_interval import InstrumentInterval


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
                self.di_plus = 0.0
                self.di_minus = 0.0
            
            # Calculate ADX (trend strength)
            if self.di_plus + self.di_minus > 0:
                dx = 100 * abs(self.di_plus - self.di_minus) / (self.di_plus + self.di_minus)
                self.adx = dx
                self.trend_strength = dx / 100.0  # Normalize to 0-1
            else:
                self.adx = 0.0
                self.trend_strength = 0.0
            
            # Calculate BX Trender value (0-100 scale)
            self.latest_bx_trender = 50 + (self.di_plus - self.di_minus) / 2
            
            # Determine trend direction
            if self.di_plus > self.di_minus:
                self.trend_direction = 1  # Bullish
            elif self.di_minus > self.di_plus:
                self.trend_direction = -1  # Bearish
            else:
                self.trend_direction = 0  # Neutral
            
            self.status = 'ok'
            logging.debug('[BXTrenderDirectional] BX Trender: %.2f, DI+: %.2f, DI-: %.2f, ADX: %.2f', 
                         self.latest_bx_trender, self.di_plus, self.di_minus, self.adx)
            
        except Exception as e:
            logging.error('[BXTrenderDirectional] Error calculating indicator: %s', str(e))
            self.status = 'invalid'
            self._reset_values()

    def _reset_values(self):
        """Reset all calculated values."""
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

    def get_trend_strength(self) -> Optional[float]:
        return self.trend_strength

    def get_trend_direction(self) -> Optional[int]:
        return self.trend_direction


@gin.configurable
class BXTrenderVolumeWeighted(Indicator):
    """
    BX Trender Volume Weighted Indicator: Incorporates volume into trend strength calculation.
    Formula: bx_trender = 50 + (50 * (bullish_volume - bearish_volume) / total_volume)
    
    This advanced indicator:
    - Weighs price movements by their corresponding volume
    - Identifies volume-confirmed trends vs. price-only trends
    - Provides more reliable trend signals in liquid markets
    """
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.latest_bx_trender: Optional[float] = None
        self.bullish_volume_ratio: Optional[float] = None
        self.bearish_volume_ratio: Optional[float] = None
        self.volume_trend_strength: Optional[float] = None
        self.price_volume_divergence: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < self.period + 1:
            self.status = 'insufficient_data'
            self._reset_values()
            logging.debug('[BXTrenderVolumeWeighted] Insufficient data: need %d intervals, got %d', self.period + 1, len(intervals))
            return

        try:
            # Validate intervals for required data
            for i, interval in enumerate(intervals[-self.period-1:]):
                if interval.status != 'ok':
                    self.status = 'invalid_data'
                    self._reset_values()
                    logging.debug('[BXTrenderVolumeWeighted] Invalid interval status at position %d: %s', i, interval.status)
                    return
                
                for field in ['high', 'low', 'close', 'traded_volume']:
                    val = getattr(interval, field, None)
                    if val is None or math.isnan(val):
                        self.status = 'invalid_data'
                        self._reset_values()
                        logging.debug('[BXTrenderVolumeWeighted] Invalid %s at position %d: %s', field, i, val)
                        return
                
                if interval.traded_volume < 0:
                    self.status = 'invalid_data'
                    self._reset_values()
                    logging.debug('[BXTrenderVolumeWeighted] Negative volume at position %d: %s', i, interval.traded_volume)
                    return

            # Calculate volume-weighted trend components
            total_volume = 0
            bullish_volume = 0
            bearish_volume = 0
            neutral_volume = 0
            
            price_changes = []
            volumes = []
            
            for i in range(len(intervals) - self.period, len(intervals)):
                current = intervals[i]
                if i > 0:
                    previous = intervals[i-1]
                    
                    # Calculate price movement type
                    price_change = current.close - previous.close
                    typical_price_change = ((current.high + current.low + current.close) / 3) - ((previous.high + previous.low + previous.close) / 3)
                    
                    volume = current.traded_volume
                    total_volume += volume
                    
                    # Classify volume based on price movement
                    if typical_price_change > 0:
                        bullish_volume += volume
                    elif typical_price_change < 0:
                        bearish_volume += volume
                    else:
                        neutral_volume += volume
                    
                    price_changes.append(price_change)
                    volumes.append(volume)

            # Calculate volume ratios
            if total_volume > 0:
                self.bullish_volume_ratio = bullish_volume / total_volume
                self.bearish_volume_ratio = bearish_volume / total_volume
            else:
                self.bullish_volume_ratio = 0.0
                self.bearish_volume_ratio = 0.0
            
            # Calculate volume-weighted BX Trender
            if total_volume > 0:
                net_volume_bias = (bullish_volume - bearish_volume) / total_volume
                self.latest_bx_trender = 50 + (50 * net_volume_bias)
            else:
                self.latest_bx_trender = 50.0
            
            # Calculate volume trend strength (how concentrated the volume is in one direction)
            self.volume_trend_strength = abs(self.bullish_volume_ratio - self.bearish_volume_ratio)
            
            # Calculate price-volume divergence (difference between price trend and volume trend)
            if price_changes and volumes:
                # Simple price trend (average price change)
                avg_price_change = sum(price_changes) / len(price_changes)
                price_trend_direction = 1 if avg_price_change > 0 else -1 if avg_price_change < 0 else 0
                
                # Volume trend direction
                volume_trend_direction = 1 if self.bullish_volume_ratio > self.bearish_volume_ratio else -1 if self.bearish_volume_ratio > self.bullish_volume_ratio else 0
                
                # Divergence: 0 = aligned, 1 = opposite, 0.5 = one neutral
                if price_trend_direction == volume_trend_direction:
                    self.price_volume_divergence = 0.0  # Aligned
                elif price_trend_direction == 0 or volume_trend_direction == 0:
                    self.price_volume_divergence = 0.5  # One neutral
                else:
                    self.price_volume_divergence = 1.0  # Opposite
            else:
                self.price_volume_divergence = 0.0
            
            self.status = 'ok'
            logging.debug('[BXTrenderVolumeWeighted] BX Trender: %.2f, Bullish Vol: %.1f%%, Bearish Vol: %.1f%%, Strength: %.3f, Divergence: %.3f', 
                         self.latest_bx_trender, self.bullish_volume_ratio * 100, self.bearish_volume_ratio * 100, 
                         self.volume_trend_strength, self.price_volume_divergence)
            
        except Exception as e:
            logging.error('[BXTrenderVolumeWeighted] Error calculating indicator: %s', str(e))
            self.status = 'invalid'
            self._reset_values()

    def _reset_values(self):
        """Reset all calculated values."""
        self.latest_bx_trender = None
        self.bullish_volume_ratio = None
        self.bearish_volume_ratio = None
        self.volume_trend_strength = None
        self.price_volume_divergence = None

    def get_value(self) -> Optional[float]:
        return self.latest_bx_trender

    def get_bullish_volume_ratio(self) -> Optional[float]:
        return self.bullish_volume_ratio

    def get_bearish_volume_ratio(self) -> Optional[float]:
        return self.bearish_volume_ratio

    def get_volume_trend_strength(self) -> Optional[float]:
        return self.volume_trend_strength

    def get_price_volume_divergence(self) -> Optional[float]:
        return self.price_volume_divergence

    def is_volume_confirmed_trend(self, threshold: float = 0.6) -> Optional[bool]:
        """
        Check if the current trend is confirmed by volume.
        
        Args:
            threshold: Minimum volume ratio required for confirmation (default 0.6 = 60%)
        
        Returns:
            True if trend is volume-confirmed, False otherwise, None if insufficient data
        """
        if self.bullish_volume_ratio is None or self.bearish_volume_ratio is None:
            return None
        
        return max(self.bullish_volume_ratio, self.bearish_volume_ratio) >= threshold