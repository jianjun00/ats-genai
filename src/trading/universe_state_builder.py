from typing import List, Dict, Optional
from datetime import datetime, date
from copy import deepcopy
from trading.universe_interval import UniverseInterval
from trading.indicator import UniverseState
from trading.market_data_manager import MarketDataManager
from trading.instrument_interval import InstrumentInterval
from trading.universe import Universe

class UniverseStateBuilder:
    """
    Manages the construction of UniverseState from a sequence of UniverseInterval objects.
    Uses MarketDataManager to fetch market data and build intervals.
    Contains a Universe instance that manages the list of instrument_ids for the current date.
    """
    def __init__(self, universe: Universe, market_data_manager: Optional[MarketDataManager] = None):
        self.intervals: List[UniverseInterval] = []
        self.universe = universe
        self.market_data_manager = market_data_manager or MarketDataManager()

    def add_interval(self, interval: UniverseInterval):
        self.intervals.append(interval)


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
        return UniverseState(intervals=deepcopy(self.intervals))

    def reset(self):
        self.intervals.clear()
