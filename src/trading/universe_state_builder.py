from typing import List, Dict, Optional
from datetime import datetime, date
from copy import deepcopy
from trading.universe_interval import UniverseInterval
from trading.indicator import UniverseState
from trading.market_data_manager import MarketDataManager
from trading.instrument_interval import InstrumentInterval
from trading.indicator_interval import IndicatorInterval
from trading.universe import Universe
from trading.indicator_config import IndicatorConfig

class UniverseStateBuilder:
    """
    Manages the construction of UniverseState from a sequence of UniverseInterval objects.
    Uses MarketDataManager to fetch market data and build intervals.
    Contains a Universe instance that manages the list of instrument_ids for the current date.
    Computes indicators for each instrument based on the provided configuration.
    """
    def __init__(self, universe: Universe, 
                 indicator_config: Optional[IndicatorConfig] = None,
                 market_data_manager: Optional[MarketDataManager] = None,
                 universe_state: Optional[UniverseState] = None):
        self.universe = universe
        self.indicator_config = indicator_config or IndicatorConfig.empty_config()
        self.market_data_manager = market_data_manager or MarketDataManager()
        self.universe_state = universe_state or UniverseState()

    def add_interval(self, interval: UniverseInterval):
        """Add interval to the universe state."""
        self.universe_state.add_interval(interval)
    
    def _compute_indicator_intervals(self, start_time: datetime, end_time: datetime) -> Dict[int, IndicatorInterval]:
        """
        Compute indicator intervals for all instruments in the current universe.
        
        Args:
            start_time: Start time for the indicator interval
            end_time: End time for the indicator interval
            
        Returns:
            Dictionary mapping instrument_id to IndicatorInterval with computed indicators
        """
        indicator_intervals = {}
        
        for instrument_id in self.universe.instrument_ids:
            # Get historical intervals for this instrument
            history = self.universe_state.instrument_history.get(instrument_id, [])
            
            if not history:
                # No history available, skip this instrument
                continue
            
            # Create indicator interval for this instrument
            indicator_interval = IndicatorInterval(
                instrument_id=instrument_id,
                start_date_time=start_time,
                end_date_time=end_time
            )
            
            # Compute each configured indicator
            for indicator_name, indicator_class in self.indicator_config.indicators.items():
                indicator_instance = indicator_class()
                indicator_instance.update(history)
                
                # Add the computed indicator to the interval
                indicator_interval.add_indicator(
                    name=indicator_name,
                    value=indicator_instance.get_value(),
                    status=indicator_instance.status,
                    update_at=indicator_instance.update_at
                )
            
            indicator_intervals[instrument_id] = indicator_interval
        
        return indicator_intervals


    def build_next_interval(self, start_time: datetime, end_time: datetime) -> UniverseInterval:
        """
        Build the next interval by fetching market data for the current universe's instruments.
        
        Args:
            start_time: Start time for the interval
            end_time: End time for the interval
            
        Returns:
            UniverseInterval with populated instrument data
        """
        # Use the universe's current instrument_ids
        instrument_ids = self.universe.instrument_ids
        
        # Fetch OHLC data for all instruments in the universe
        ohlc_data = self.market_data_manager.get_ohlc_batch(instrument_ids, start_time, end_time)
        
        # Create InstrumentInterval objects for each instrument
        instrument_intervals = {}
        for instrument_id in instrument_ids:
            ohlc = ohlc_data.get(instrument_id)
            if ohlc is not None:
                # Create InstrumentInterval with fetched data
                instrument_interval = InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=start_time,
                    end_date_time=end_time,
                    open=ohlc['open'],
                    high=ohlc['high'],
                    low=ohlc['low'],
                    close=ohlc['close'],
                    traded_volume=ohlc.get('volume', 0.0),  # Default to 0 if not provided
                    traded_dollar=ohlc.get('dollar_volume', 0.0),  # Default to 0 if not provided
                    status='ok'  # Assume data is valid, can be customized based on data quality
                )
                instrument_intervals[instrument_id] = instrument_interval
        
        # Create and return the UniverseInterval
        universe_interval = UniverseInterval(
            start_date_time=start_time,
            end_date_time=end_time,
            instrument_intervals=instrument_intervals
        )
        
        return universe_interval

    def add_next_interval(self, start_time: datetime, end_time: datetime, 
                         new_date: Optional[date] = None, 
                         new_instrument_ids: Optional[List[int]] = None):
        """
        Build and add the next interval to the builder, optionally advancing the universe first.
        
        Args:
            start_time: Start time for the interval
            end_time: End time for the interval
            new_date: Optional new date to advance the universe to
            new_instrument_ids: Optional new list of instrument IDs
        """
        # Advance universe if new date or instrument_ids provided
        if new_date is not None:
            self.universe.advanceTo(new_date, new_instrument_ids)
        elif new_instrument_ids is not None:
            self.universe.instrument_ids = new_instrument_ids
        
        # Build and add the interval
        interval = self.build_next_interval(start_time, end_time)
        self.add_interval(interval)

    def build(self) -> UniverseState:
        """Build UniverseState with computed indicator intervals for the latest interval."""
        # Compute indicator intervals for the latest time period if we have intervals
        if self.universe_state.intervals:
            latest_interval = self.universe_state.intervals[-1]
            indicator_intervals = self._compute_indicator_intervals(
                latest_interval.start_date_time,
                latest_interval.end_date_time
            )
            self.universe_state.indicator_intervals = indicator_intervals
        
        return self.universe_state

    def reset(self):
        """Reset the universe state by clearing all intervals and history."""
        self.universe_state.reset()
