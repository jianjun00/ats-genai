"""
Volume-based Indicators.

Contains indicators that analyze volume, dollar volume, and volume profile
for market structure analysis and liquidity assessment.

Extracted from indicator.py to separate volume-related indicators.
"""

import gin
import math
from datetime import datetime
from typing import List, Optional, Dict
from .base_indicator import Indicator
from state.instrument_interval import InstrumentInterval


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
                if not hasattr(interval, field) or getattr(interval, field) is None:
                    logging.debug(f'[CumulativeDollars] Interval {i} missing {field} data')
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
                price = (current_interval.high + current_interval.low + current_interval.close) / 3
            elif self.price_method == 'close':
                price = current_interval.close
            elif self.price_method == 'open':
                price = current_interval.open
            else:
                raise ValueError(f'Unknown price method: {self.price_method}')
            
            # Add current interval dollar volume
            dollar_volume = price * current_interval.traded_volume
            self.cumulative_dollars += dollar_volume
            self.status = 'ok'
            
            logging.debug(f'[CumulativeDollars] Updated cumulative dollars: {self.cumulative_dollars}')
            
        except Exception as e:
            logging.error(f'[CumulativeDollars] Error calculating cumulative dollars: {str(e)}')
            self.status = 'invalid'
            self.cumulative_dollars = None

    def get_value(self) -> Optional[float]:
        return self.cumulative_dollars


@gin.configurable
class VolumeProfile(Indicator):
    """
    Volume Profile Indicator for Market Structure Analysis.
    
    Analyzes volume distribution across price levels to identify:
    - Point of Control (POC): Price level with highest volume
    - Value Area High (VAH): Top of 70% volume area
    - Value Area Low (VAL): Bottom of 70% volume area
    
    This is critical for understanding market structure and liquidity.
    """
    def __init__(self, price_levels: int = 50, value_area_percent: float = 0.70, 
                 reset_daily: bool = True):
        super().__init__()
        self.price_levels = price_levels
        self.value_area_percent = value_area_percent
        self.reset_daily = reset_daily
        
        # Volume profile data
        self.volume_profile: Dict[float, float] = {}
        self.total_volume: float = 0.0
        self.last_reset_date: Optional[str] = None
        
        # Market structure levels
        self.point_of_control: Optional[float] = None
        self.value_area_high: Optional[float] = None
        self.value_area_low: Optional[float] = None
        self.market_structure_strength: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        self.update_at = datetime.now()
        
        if not intervals:
            logging.debug('[VolumeProfile] No intervals provided')
            self.status = 'invalid'
            self._reset_values()
            return
        
        # Validate intervals
        for i, interval in enumerate(intervals):
            if interval.status != 'ok':
                logging.debug(f'[VolumeProfile] Interval {i} has invalid status: {interval.status}')
                self.status = 'invalid'
                self._reset_values()
                return
            
            required_fields = ['high', 'low', 'close', 'traded_volume']
            for field in required_fields:
                val = getattr(interval, field, None)
                if val is None or (isinstance(val, float) and math.isnan(val)):
                    logging.debug(f'[VolumeProfile] Interval {i} missing {field} data')
                    self.status = 'invalid'
                    self._reset_values()
                    return
        
        try:
            current_interval = intervals[-1]
            current_date = current_interval.start_date_time.date().isoformat() if current_interval.start_date_time else None
            
            # Reset profile daily if configured
            if self.reset_daily and current_date != self.last_reset_date:
                self._reset_profile()
                self.last_reset_date = current_date
                logging.debug(f'[VolumeProfile] Reset for new day: {current_date}')
            
            # Process current interval
            self._process_interval(current_interval)
            
            # Calculate market structure levels
            self._calculate_market_structure()
            
            self.status = 'ok'
            logging.debug(f'[VolumeProfile] Updated - POC: {self.point_of_control}, VAH: {self.value_area_high}, VAL: {self.value_area_low}')
            
        except Exception as e:
            logging.error(f'[VolumeProfile] Error calculating volume profile: {str(e)}')
            self.status = 'invalid'
            self._reset_values()

    def _reset_profile(self):
        """Reset the volume profile data."""
        self.volume_profile.clear()
        self.total_volume = 0.0

    def _reset_values(self):
        """Reset all calculated values."""
        self.point_of_control = None
        self.value_area_high = None
        self.value_area_low = None
        self.market_structure_strength = None

    def _process_interval(self, interval: InstrumentInterval):
        """Process a single interval and add to volume profile."""
        # Calculate price range and step
        price_range = interval.high - interval.low
        if price_range <= 0:
            # If high == low, assign all volume to that price
            price_step = 0
            levels = [interval.high]
        else:
            price_step = price_range / self.price_levels
            levels = [interval.low + i * price_step for i in range(self.price_levels + 1)]
        
        # Distribute volume across price levels
        # Simplified assumption: volume is distributed based on typical price
        typical_price = (interval.high + interval.low + interval.close) / 3
        
        # Add volume to the closest price level
        closest_level = min(levels, key=lambda x: abs(x - typical_price))
        
        if closest_level not in self.volume_profile:
            self.volume_profile[closest_level] = 0.0
        
        self.volume_profile[closest_level] += interval.traded_volume
        self.total_volume += interval.traded_volume

    def _calculate_market_structure(self):
        """Calculate Point of Control and Value Area levels."""
        if not self.volume_profile or self.total_volume == 0:
            self._reset_values()
            return
        
        # Find Point of Control (highest volume price level)
        self.point_of_control = max(self.volume_profile.keys(), 
                                   key=lambda x: self.volume_profile[x])
        
        # Calculate Value Area (70% of total volume around POC)
        target_volume = self.total_volume * self.value_area_percent
        
        # Sort price levels by volume (descending)
        sorted_levels = sorted(self.volume_profile.keys(), 
                             key=lambda x: self.volume_profile[x], reverse=True)
        
        # Accumulate volume starting from POC
        accumulated_volume = 0.0
        value_area_levels = []
        
        for level in sorted_levels:
            accumulated_volume += self.volume_profile[level]
            value_area_levels.append(level)
            if accumulated_volume >= target_volume:
                break
        
        # Value Area High and Low
        if value_area_levels:
            self.value_area_high = max(value_area_levels)
            self.value_area_low = min(value_area_levels)
        else:
            self.value_area_high = self.point_of_control
            self.value_area_low = self.point_of_control
        
        # Calculate market structure strength (concentration of volume)
        max_volume = max(self.volume_profile.values())
        self.market_structure_strength = max_volume / self.total_volume if self.total_volume > 0 else 0

    def get_point_of_control(self) -> Optional[float]:
        """Get the Point of Control (price level with highest volume)."""
        return self.point_of_control

    def get_value_area_high(self) -> Optional[float]:
        """Get the Value Area High."""
        return self.value_area_high

    def get_value_area_low(self) -> Optional[float]:
        """Get the Value Area Low."""
        return self.value_area_low

    def get_market_structure_strength(self) -> Optional[float]:
        """Get market structure strength (0-1, higher = more concentrated)."""
        return self.market_structure_strength

    def get_value(self) -> Optional[float]:
        """Returns Point of Control as the primary value."""
        return self.point_of_control