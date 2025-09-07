"""
Signal Generation Indicators.

Contains indicators that generate buy/sell signals and trading levels
including the Five-series indicators for signal detection.

Extracted from indicator.py to separate signal generation indicators.
"""

import gin
import math
from datetime import datetime
from typing import List, Optional
from .base_indicator import Indicator
from state.instrument_interval import InstrumentInterval


@gin.configurable
class FiveNineSell(Indicator):
    """
    Five Nine Sell Indicator: Calculated as 2 * high of prior bar - low of prior prior bar.
    Formula: five_nine_sell = 2 * high(t-1) - low(t-2)
    
    This generates resistance levels for potential sell signals.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_nine_sell: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveNineSell] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_nine_sell = None
            return
            
        # Get the required intervals
        t_minus_1 = intervals[-1]  # Prior bar
        t_minus_2 = intervals[-2]  # Prior prior bar
        
        # Validate intervals
        for i, (label, interval) in enumerate([('t-1', t_minus_1), ('t-2', t_minus_2)]):
            if interval.status != 'ok':
                logging.debug(f'[FiveNineSell] Invalid interval status at {label}: {interval.status}')
                self.status = 'invalid'
                self.latest_five_nine_sell = None
                return
                
            required_field = 'high' if label == 't-1' else 'low'
            val = getattr(interval, required_field, None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug(f'[FiveNineSell] Invalid {required_field} at {label}: {val}')
                self.status = 'invalid'
                self.latest_five_nine_sell = None
                return
        
        # Calculate: five_nine_sell = 2 * high(t-1) - low(t-2)
        try:
            self.latest_five_nine_sell = 2 * t_minus_1.high - t_minus_2.low
            self.status = 'ok'
            logging.debug('[FiveNineSell] Calculated FiveNineSell: %.6f', self.latest_five_nine_sell)
        except Exception as e:
            logging.error('[FiveNineSell] Error calculating FiveNineSell: %s', str(e))
            self.status = 'invalid'
            self.latest_five_nine_sell = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_nine_sell


@gin.configurable
class FiveNineBuy(Indicator):
    """
    Five Nine Buy Indicator: Calculated as 2 * low of prior bar - high of prior prior bar.
    Formula: five_nine_buy = 2 * low(t-1) - high(t-2)
    
    This generates support levels for potential buy signals.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_nine_buy: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveNineBuy] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_nine_buy = None
            return
            
        # Get the required intervals
        t_minus_1 = intervals[-1]  # Prior bar
        t_minus_2 = intervals[-2]  # Prior prior bar
        
        # Validate intervals
        for i, (label, interval) in enumerate([('t-1', t_minus_1), ('t-2', t_minus_2)]):
            if interval.status != 'ok':
                logging.debug(f'[FiveNineBuy] Invalid interval status at {label}: {interval.status}')
                self.status = 'invalid'
                self.latest_five_nine_buy = None
                return
                
            required_field = 'low' if label == 't-1' else 'high'
            val = getattr(interval, required_field, None)
            if val is None or (isinstance(val, float) and math.isnan(val)):
                logging.debug(f'[FiveNineBuy] Invalid {required_field} at {label}: {val}')
                self.status = 'invalid'
                self.latest_five_nine_buy = None
                return
        
        # Calculate: five_nine_buy = 2 * low(t-1) - high(t-2)
        try:
            self.latest_five_nine_buy = 2 * t_minus_1.low - t_minus_2.high
            self.status = 'ok'
            logging.debug('[FiveNineBuy] Calculated FiveNineBuy: %.6f', self.latest_five_nine_buy)
        except Exception as e:
            logging.error('[FiveNineBuy] Error calculating FiveNineBuy: %s', str(e))
            self.status = 'invalid'
            self.latest_five_nine_buy = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_nine_buy


@gin.configurable
class FiveOneBuy(Indicator):
    """
    Five One Buy Indicator: Calculated as 2 * low(t-1) - low(t-2) with conditions.
    Formula: five_one_buy = 2 * low(t-1) - low(t-2) 
    
    This generates buy signal levels based on low price progression.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_one_buy: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveOneBuy] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_one_buy = None
            return
            
        # Get the required intervals
        t_minus_1 = intervals[-1]  # Prior bar
        t_minus_2 = intervals[-2]  # Prior prior bar
        
        # Validate intervals
        for i, (label, interval) in enumerate([('t-1', t_minus_1), ('t-2', t_minus_2)]):
            if interval.status != 'ok':
                logging.debug(f'[FiveOneBuy] Invalid interval status at {label}: {interval.status}')
                self.status = 'invalid'
                self.latest_five_one_buy = None
                return
                
            if not hasattr(interval, 'low') or interval.low is None or (isinstance(interval.low, float) and math.isnan(interval.low)):
                logging.debug(f'[FiveOneBuy] Invalid low at {label}: {interval.low}')
                self.status = 'invalid'
                self.latest_five_one_buy = None
                return
        
        # Calculate: five_one_buy = 2 * low(t-1) - low(t-2)
        try:
            self.latest_five_one_buy = 2 * t_minus_1.low - t_minus_2.low
            self.status = 'ok'
            logging.debug('[FiveOneBuy] Calculated FiveOneBuy: %.6f', self.latest_five_one_buy)
        except Exception as e:
            logging.error('[FiveOneBuy] Error calculating FiveOneBuy: %s', str(e))
            self.status = 'invalid'
            self.latest_five_one_buy = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_one_buy


@gin.configurable
class FiveOneSell(Indicator):
    """
    Five One Sell Indicator: Calculated as 2 * high(t-1) - high(t-2) with conditions.
    Formula: five_one_sell = 2 * high(t-1) - high(t-2) 
    
    This generates sell signal levels based on high price progression.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_one_sell: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveOneSell] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_one_sell = None
            return
            
        # Get the required intervals
        t_minus_1 = intervals[-1]  # Prior bar
        t_minus_2 = intervals[-2]  # Prior prior bar
        
        # Validate intervals
        for i, (label, interval) in enumerate([('t-1', t_minus_1), ('t-2', t_minus_2)]):
            if interval.status != 'ok':
                logging.debug(f'[FiveOneSell] Invalid interval status at {label}: {interval.status}')
                self.status = 'invalid'
                self.latest_five_one_sell = None
                return
                
            if not hasattr(interval, 'high') or interval.high is None or (isinstance(interval.high, float) and math.isnan(interval.high)):
                logging.debug(f'[FiveOneSell] Invalid high at {label}: {interval.high}')
                self.status = 'invalid'
                self.latest_five_one_sell = None
                return
        
        # Calculate: five_one_sell = 2 * high(t-1) - high(t-2)
        try:
            self.latest_five_one_sell = 2 * t_minus_1.high - t_minus_2.high
            self.status = 'ok'
            logging.debug('[FiveOneSell] Calculated FiveOneSell: %.6f', self.latest_five_one_sell)
        except Exception as e:
            logging.error('[FiveOneSell] Error calculating FiveOneSell: %s', str(e))
            self.status = 'invalid'
            self.latest_five_one_sell = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_one_sell


@gin.configurable
class FiveTwoBuy(Indicator):
    """
    Five Two Buy Indicator: Calculated as 2 * low(t-1) - low(t-2) with conditions.
    Formula: five_two_buy = 2 * low(t-1) - low(t-2) 
    
    Advanced buy signal generator with additional validation conditions.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_two_buy: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveTwoBuy] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_two_buy = None
            return
            
        # Get the required intervals
        t_minus_1 = intervals[-1]  # Prior bar
        t_minus_2 = intervals[-2]  # Prior prior bar
        
        # Validate intervals
        for i, (label, interval) in enumerate([('t-1', t_minus_1), ('t-2', t_minus_2)]):
            if interval.status != 'ok':
                logging.debug(f'[FiveTwoBuy] Invalid interval status at {label}: {interval.status}')
                self.status = 'invalid'
                self.latest_five_two_buy = None
                return
                
            if not hasattr(interval, 'low') or interval.low is None or (isinstance(interval.low, float) and math.isnan(interval.low)):
                logging.debug(f'[FiveTwoBuy] Invalid low at {label}: {interval.low}')
                self.status = 'invalid'
                self.latest_five_two_buy = None
                return
        
        # Calculate: five_two_buy = 2 * low(t-1) - low(t-2)
        try:
            self.latest_five_two_buy = 2 * t_minus_1.low - t_minus_2.low
            self.status = 'ok'
            logging.debug('[FiveTwoBuy] Calculated FiveTwoBuy: %.6f', self.latest_five_two_buy)
        except Exception as e:
            logging.error('[FiveTwoBuy] Error calculating FiveTwoBuy: %s', str(e))
            self.status = 'invalid'
            self.latest_five_two_buy = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_two_buy


@gin.configurable
class FiveTwoSell(Indicator):
    """
    Five Two Sell Indicator: Calculated as 2 * high(t-1) - high(t-2) with conditions.
    Formula: five_two_sell = 2 * high(t-1) - high(t-2)
    
    Advanced sell signal generator with additional validation conditions.
    """
    def __init__(self):
        super().__init__()
        self.latest_five_two_sell: Optional[float] = None

    def update(self, intervals: List[InstrumentInterval]):
        import logging
        
        self.update_at = datetime.now()
        
        if len(intervals) < 2:
            logging.debug('[FiveTwoSell] Not enough intervals: need 2, got %d', len(intervals))
            self.status = 'invalid'
            self.latest_five_two_sell = None
            return
            
        # Get the required intervals
        t_minus_1 = intervals[-1]  # Prior bar
        t_minus_2 = intervals[-2]  # Prior prior bar
        
        # Validate intervals
        for i, (label, interval) in enumerate([('t-1', t_minus_1), ('t-2', t_minus_2)]):
            if interval.status != 'ok':
                logging.debug(f'[FiveTwoSell] Invalid interval status at {label}: {interval.status}')
                self.status = 'invalid'
                self.latest_five_two_sell = None
                return
                
            if not hasattr(interval, 'high') or interval.high is None or (isinstance(interval.high, float) and math.isnan(interval.high)):
                logging.debug(f'[FiveTwoSell] Invalid high at {label}: {interval.high}')
                self.status = 'invalid'
                self.latest_five_two_sell = None
                return
        
        # Calculate: five_two_sell = 2 * high(t-1) - high(t-2)
        try:
            self.latest_five_two_sell = 2 * t_minus_1.high - t_minus_2.high
            self.status = 'ok'
            logging.debug('[FiveTwoSell] Calculated FiveTwoSell: %.6f', self.latest_five_two_sell)
        except Exception as e:
            logging.error('[FiveTwoSell] Error calculating FiveTwoSell: %s', str(e))
            self.status = 'invalid'
            self.latest_five_two_sell = None

    def get_value(self) -> Optional[float]:
        return self.latest_five_two_sell