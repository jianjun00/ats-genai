from typing import List, Dict, Optional
from datetime import datetime, date
from copy import deepcopy
from state.universe_interval import UniverseInterval
from trading.indicator import UniverseState
from trading.market_data_manager import MarketDataManager
from state.instrument_interval import InstrumentInterval
from state.indicator_interval import IndicatorInterval
from trading.universe import Universe
from trading.indicator_config import IndicatorConfig
from trading.time_duration import TimeDuration

class UniverseStateBuilder:
    """
    Manages the construction of UniverseState from a sequence of UniverseInterval objects.
    Uses MarketDataManager to fetch market data and build intervals.
    Contains a Universe instance that manages the list of instrument_ids for the current date.
    Computes indicators for each instrument based on the provided configuration.
    Supports building intervals for multiple time durations with a base duration approach.
    """
    def __init__(self, universe: Universe, 
                 indicator_config: Optional[IndicatorConfig] = None,
                 market_data_manager: Optional[MarketDataManager] = None,
                 universe_state: Optional[UniverseState] = None,
                 base_duration: Optional[TimeDuration] = None,
                 target_durations: Optional[List[TimeDuration]] = None):
        self.universe = universe
        self.indicator_config = indicator_config or IndicatorConfig.empty_config()
        self.market_data_manager = market_data_manager or MarketDataManager()
        self.universe_state = universe_state or UniverseState()
        
        # Multi-duration support
        self.base_duration = base_duration or TimeDuration.create_5_minutes()
        self.target_durations = target_durations or [self.base_duration]
        
        # Validate that base duration is compatible with target durations
        self._validate_duration_compatibility()

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

    def _validate_duration_compatibility(self):
        """
        Validate that target durations are compatible with the base duration.
        Target durations should be multiples of the base duration for proper aggregation.
        """
        base_minutes = self.base_duration.get_duration_minutes()
        
        # If base duration is not minute-based, we can't validate multiples easily
        if base_minutes is None:
            return
        
        for target_duration in self.target_durations:
            target_minutes = target_duration.get_duration_minutes()
            
            # Skip validation for date-based durations (daily, weekly, etc.)
            if target_minutes is None:
                continue
                
            # Check if target duration is a multiple of base duration
            if target_minutes % base_minutes != 0:
                raise ValueError(
                    f"Target duration {target_duration.get_duration_string()} "
                    f"({target_minutes} minutes) is not a multiple of base duration "
                    f"{self.base_duration.get_duration_string()} ({base_minutes} minutes)"
                )
    
    def build_multi_duration_intervals(self, start_time: datetime) -> Dict[str, UniverseInterval]:
        """
        Build intervals for multiple durations starting from the given start time.
        
        Args:
            start_time: The start time for interval building
            
        Returns:
            Dictionary mapping duration strings to their corresponding UniverseInterval
        """
        intervals = {}
        
        for duration in self.target_durations:
            end_time = duration.get_end_time(start_time)
            duration_key = duration.get_duration_string()
            
            # Build interval for this specific duration
            interval = self.build_next_interval(start_time, end_time)
            intervals[duration_key] = interval
            
        return intervals
    
    def add_multi_duration_intervals(self, start_time: datetime, 
                                   new_date: Optional[date] = None, 
                                   new_instrument_ids: Optional[List[int]] = None):
        """
        Build and add intervals for all target durations to the builder.
        
        Args:
            start_time: Start time for interval building
            new_date: Optional new date to advance the universe to
            new_instrument_ids: Optional new list of instrument IDs
        """
        # Advance universe if new date or instrument_ids provided
        if new_date is not None:
            self.universe.advanceTo(new_date, new_instrument_ids)
        elif new_instrument_ids is not None:
            self.universe.instrument_ids = new_instrument_ids
        
        # Build intervals for all target durations
        multi_duration_intervals = self.build_multi_duration_intervals(start_time)
        
        # Add each interval to the universe state
        for duration_key, interval in multi_duration_intervals.items():
            self.add_interval(interval)
    
    def build_aggregated_interval(self, base_intervals: List[UniverseInterval], 
                                target_duration: TimeDuration) -> UniverseInterval:
        """
        Build an aggregated interval from multiple base intervals for a target duration.
        This is useful when you want to aggregate multiple base duration intervals
        into a longer duration interval (e.g., aggregate 3 x 5-minute intervals into 1 x 15-minute interval).
        
        Args:
            base_intervals: List of base duration intervals to aggregate
            target_duration: Target duration for the aggregated interval
            
        Returns:
            Aggregated UniverseInterval
        """
        if not base_intervals:
            raise ValueError("Cannot aggregate empty list of intervals")
        
        # Sort intervals by start time to ensure proper ordering
        sorted_intervals = sorted(base_intervals, key=lambda x: x.start_date_time)
        
        # Determine aggregated interval time bounds
        start_time = sorted_intervals[0].start_date_time
        end_time = sorted_intervals[-1].end_date_time
        
        # Validate that the end time matches the expected target duration end time
        expected_end_time = target_duration.get_end_time(start_time)
        if end_time != expected_end_time:
            # Adjust end time to match target duration
            end_time = expected_end_time
        
        # Aggregate instrument data across all intervals
        aggregated_instrument_intervals = {}
        
        # Get all unique instrument IDs across all intervals
        all_instrument_ids = set()
        for interval in sorted_intervals:
            all_instrument_ids.update(interval.instrument_intervals.keys())
        
        # Aggregate data for each instrument
        for instrument_id in all_instrument_ids:
            # Collect all instrument intervals for this instrument
            instrument_intervals = []
            for interval in sorted_intervals:
                if instrument_id in interval.instrument_intervals:
                    instrument_intervals.append(interval.instrument_intervals[instrument_id])
            
            if instrument_intervals:
                # Aggregate OHLC data: Open from first, Close from last, High/Low from all
                first_interval = instrument_intervals[0]
                last_interval = instrument_intervals[-1]
                
                # Calculate aggregated OHLC
                open_price = first_interval.open
                close_price = last_interval.close
                high_price = max(interval.high for interval in instrument_intervals)
                low_price = min(interval.low for interval in instrument_intervals)
                
                # Sum volumes and dollar volumes
                total_volume = sum(interval.traded_volume for interval in instrument_intervals)
                total_dollar = sum(interval.traded_dollar for interval in instrument_intervals)
                
                # Determine status (ok if all are ok, otherwise invalid)
                status = 'ok' if all(interval.status == 'ok' for interval in instrument_intervals) else 'invalid'
                
                # Create aggregated instrument interval
                aggregated_interval = InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=start_time,
                    end_date_time=end_time,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    traded_volume=total_volume,
                    traded_dollar=total_dollar,
                    status=status
                )
                
                aggregated_instrument_intervals[instrument_id] = aggregated_interval
        
        # Create and return aggregated universe interval
        return UniverseInterval(
            start_date_time=start_time,
            end_date_time=end_time,
            instrument_intervals=aggregated_instrument_intervals
        )
    
    def get_base_duration(self) -> TimeDuration:
        """Get the base duration used for interval building."""
        return self.base_duration
    
    def get_target_durations(self) -> List[TimeDuration]:
        """Get the list of target durations for interval building."""
        return self.target_durations.copy()
    
    def set_target_durations(self, durations: List[TimeDuration]):
        """
        Set new target durations and validate compatibility with base duration.
        
        Args:
            durations: List of TimeDuration objects to set as targets
        """
        self.target_durations = durations
        self._validate_duration_compatibility()

    def reset(self):
        """Reset the universe state by clearing all intervals and history."""
        self.universe_state.reset()
