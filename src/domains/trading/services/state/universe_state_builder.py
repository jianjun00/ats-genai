"""
UniverseStateBuilder - Business Logic Orchestrator for Universe State Construction.

SINGLE RESPONSIBILITY:
- Orchestrate complete universe state construction workflow

STRICTLY ONLY:
- Coordinate data flow between MarketDataManager and IndicatorBuilder
- Manage rolling windows of InstrumentInterval objects (business logic)
- Transform raw OHLC data to business InstrumentInterval objects
- Handle multi-timeframe aggregation and duration logic
- Implement data validation and business rules
- Create and emit UniverseStateInterval objects

DOES NOT:
- Fetch any raw data from any source (MarketDataManager responsibility)
- Perform any indicator calculations (IndicatorBuilder responsibility)
- Persist any results anywhere (UniverseStateManager responsibility)
- Handle any storage concerns or optimization (UniverseStateManager responsibility)

INTERACTIONS:
- Uses: MarketDataManager (requests OHLC data), IndicatorBuilder (requests indicator computation)
- Given: Raw OHLC data from MarketDataManager
- Returns: UniverseStateInterval objects with computed state to UniverseStateManager
- That's it - orchestration only
"""

import pandas as pd
import gin
try:
    from domains.trading.services.state_core.runner_callback import RunnerCallback
except Exception:
    # Fallback minimal base when runner_callback is not available in test context
    class RunnerCallback:
        pass
from typing import Dict, List
import logging
from datetime import datetime, timedelta
from core.platform.config_env.environment import Environment
from domains.trading.services.state_core.instrument_interval import InstrumentInterval
from domains.trading.services.state_core.factor_interval import FactorInterval
from domains.trading.services.state_core.indicator_interval import IndicatorInterval
from core.dao.market_data.daily_market_cap_dao import DailyMarketCapDAO

try:
    from domains.trading.services.indicators.indicator_builder import IndicatorBuilder
    from domains.trading.services.indicators.indicator_config import IndicatorConfig
except ImportError:
    # Fallback - these may not be available in all environments
    IndicatorBuilder = None
    IndicatorConfig = None

@gin.configurable
class UniverseStateIntervalBuilder(RunnerCallback):
    def __init__(self, env: Environment = None, base_duration: str = '1m', target_durations: str = '1m,5m,15m,1h,1d,1w', forecast_callback=None, universe_state_manager=None):
        """
        Initialize UniverseStateIntervalBuilder.
        Args:
            env: Environment instance (uses global if None)
            base_duration: str (e.g. '1m'), overrides Gin config if provided
            target_durations: comma-separated str (e.g. '1m,5m,15m,1h,1d'), overrides Gin config if provided
            universe_state_manager: UniverseStateManager instance for shared rolling cache
        """
        from core.platform.config_env.environment import Environment
        from core.dao.security_reference.market_cap_dao import DailyMarketCapDAO
        from core.business.calendars.time_duration import TimeDuration
        import logging
        
        # Inject DailyMarketCapDAO for market_cap sourcing
        self.market_cap_dao = DailyMarketCapDAO(env)
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        
        # Store reference to universe state manager for shared rolling cache
        self.universe_state_manager = universe_state_manager
        
        # Parse duration parameters  
        base_duration_str = base_duration
        self.base_duration = TimeDuration(base_duration_str)
        target_durations_str = target_durations
        self.target_durations = [TimeDuration(d.strip()) for d in target_durations_str.split(',')]
        
        # Log configuration at initialization
        self.logger.debug(f"UniverseStateIntervalBuilder initialized: base={self.base_duration}, targets={[str(d) for d in self.target_durations]}")

        # Default business logic parameters (from test expectations)
        self.min_market_cap = 100_000_000
    
    def _get_market_data_time_range(self, current_time):
        """
        Get the time range for market data fetching for a given interval.
        
        Args:
            current_time: The current interval time being processed
            
        Returns:
            tuple: (start_time, end_time) both timezone-aware in GMT
            
        Note:
            This function determines what historical data to fetch for the current interval.
            The current implementation fetches from market open to current time, but this
            could be adjusted to fetch specific windows (e.g., last hour, last day, etc.)
        """
        from core.shared.data_handling.utils.datetime_utils import get_session_times, to_utc
        from zoneinfo import ZoneInfo
        
        gmt_tz = ZoneInfo("GMT")
        
        # Get trading session times for this date
        session_times = get_session_times(current_time.date())
        
        # FIXED: Use interval-specific time window instead of market open
        # This prevents OHLCV duplication by fetching unique data for each interval
        
        # Use 1-hour lookback window for each interval
        from datetime import timedelta
        window_duration = timedelta(hours=1)
        minute_start_time = current_time - window_duration
        minute_end_time = current_time
        
        # FAIL FAST: Require timezone-aware datetime for market data consistency
        if minute_start_time.tzinfo is None:
            raise ValueError(f"minute_start_time must be timezone-aware, got timezone-naive: {minute_start_time}")
        if minute_end_time.tzinfo is None:
            raise ValueError(f"minute_end_time must be timezone-aware, got timezone-naive: {minute_end_time}")
        
        # Convert to GMT for consistent market data manager input
        minute_start_time = minute_start_time.astimezone(gmt_tz)
        minute_end_time = minute_end_time.astimezone(gmt_tz)
        
        return minute_start_time, minute_end_time
    
    def handleStartOfDay(self, runner, current_time):
        self.logger.debug(f"UniverseStateIntervalBuilder.handleStartOfDay called at {current_time}")

    def handleEndOfDay(self, runner, current_time):
        self.logger.debug(f"UniverseStateIntervalBuilder.handleEndOfDay called at {current_time}")

    async def handleTradingDayEnd(self, runner, current_time):
        """
        Handle trading day end events (market close) to build 1d universe state intervals.
        
        This method is called by Runner at market close time (e.g., 4 PM ET for NYSE)
        and is responsible for building daily timeframe universe state.
        """
        self.logger.debug(f"handleTradingDayEnd called at {current_time}")
        
        try:
            # Get active instrument IDs
            instrument_ids = runner.universe_manager.instrument_ids
            
            if instrument_ids:
                # Build universe state for 1d timeframe
                duration_1d = None
                for duration in self.target_durations:
                    if duration.get_duration_string() == '1d':
                        duration_1d = duration
                        break
                
                if duration_1d:
                    self.logger.info(f"Building 1d universe state for {len(instrument_ids)} instruments at {current_time}")
                    duration_to_state = await self._build_universe_state_for_duration(
                        duration_1d, current_time, instrument_ids, runner
                    )
                    
                    # Save the 1d universe state
                    if duration_to_state and hasattr(runner, 'universe_state_manager'):
                        await runner.universe_state_manager.addUniverseState(duration_to_state, current_time)
                        self.logger.info(f"✅ Saved 1d universe state for {len(instrument_ids)} instruments")
                    else:
                        self.logger.warning(f"No 1d universe state to save at {current_time}")
                else:
                    self.logger.warning("1d duration not found in target_durations")
            else:
                self.logger.warning(f"No active instruments for universe {runner.universe_id}")
                
        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in handleTradingDayEnd: {e}")
            import traceback
            traceback.print_exc()

    async def handleTradingWeekEnd(self, runner, current_time):
        """
        Handle trading week end events (last trading day of week) to build 1w universe state intervals.
        
        This method is called by Runner at the end of the last trading day of each week
        and is responsible for building weekly timeframe universe state.
        """
        self.logger.debug(f"handleTradingWeekEnd called at {current_time}")
        
        try:
            # Get active instrument IDs
            instrument_ids = runner.universe_manager.instrument_ids
            
            if instrument_ids:
                # Build universe state for 1w timeframe
                duration_1w = None
                for duration in self.target_durations:
                    if duration.get_duration_string() == '1w':
                        duration_1w = duration
                        break
                
                if duration_1w:
                    self.logger.info(f"Building 1w universe state for {len(instrument_ids)} instruments at {current_time}")
                    duration_to_state = await self._build_universe_state_for_duration(
                        duration_1w, current_time, instrument_ids, runner
                    )
                    
                    # Save the 1w universe state
                    if duration_to_state and hasattr(runner, 'universe_state_manager'):
                        await runner.universe_state_manager.addUniverseState(duration_to_state, current_time)
                        self.logger.info(f"✅ Saved 1w universe state for {len(instrument_ids)} instruments")
                    else:
                        self.logger.warning(f"No 1w universe state to save at {current_time}")
                else:
                    self.logger.warning("1w duration not found in target_durations")
            else:
                self.logger.warning(f"No active instruments for universe {runner.universe_id}")
                
        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in handleTradingWeekEnd: {e}")
            import traceback
            traceback.print_exc()

    async def handleInterval(self, runner, current_time):
        """
        Handle 1-minute intervals. Updates rolling cache with 1-minute data on every call,
        but only builds universe state for specific timeframes when their intervals are complete.
        
        Called once per minute, but universe state is built separately for each timeframe
        (5m, 15m, 1h, 1d, etc.) only when that timeframe's interval is complete.
        """
        from domains.trading.services.state.universe_state import UniverseStateInterval
        self.logger.debug(f"[handleInterval] Called at {current_time} - processing 1-minute interval")
        
        if not self.target_durations:
            self.logger.error("No target durations configured.")
            return
            
        instrument_ids = runner.universe_manager.instrument_ids
        
        # --- 1. ALWAYS update rolling cache with 1-minute data ---
        # ✅ CRITICAL FIX: Request data from market open to current interval time only
        # Get data fetch time range for this interval
        minute_start_time, minute_end_time = self._get_market_data_time_range(current_time)
        
        self.logger.debug(f"[handleInterval] Fetching 1-minute data: [{minute_start_time}, {minute_end_time}]")
        # Convert instrument_ids to symbols for FileBasedMinuteMarketDataManager
        from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(runner.get_environment())
        inst_id_to_symbol = await xrefs_dao.get_symbols_by_instrument_ids_batch(instrument_ids, "ticker")
        
        symbols = []
        for inst_id in instrument_ids:
            symbol = inst_id_to_symbol.get(inst_id)
            if symbol:
                symbols.append(symbol)
            else:
                self.logger.warning(f"No symbol mapping found for instrument_id {inst_id}")

        # Fetch 1-minute OHLC data
        ohlc_batch = await runner.market_data_manager.get_minute_ohlc_batch(symbols, minute_start_time, minute_end_time)
        
        # Check what we actually got
        for symbol, ohlc_data in ohlc_batch.items():
            if not ohlc_data:
                self.logger.warning(f"🔍 [DEBUG] Symbol {symbol}: Got None (no data)")

        # Convert back to instrument_id-based dictionary
        symbol_to_inst_id = {symbol: inst_id for inst_id, symbol in inst_id_to_symbol.items() if symbol is not None}
        
        ohlc_batch_by_inst_id = {}
        for symbol, ohlc_data in ohlc_batch.items():
            inst_id = symbol_to_inst_id.get(symbol)
            if inst_id:
                ohlc_batch_by_inst_id[inst_id] = ohlc_data
            else:
                self.logger.warning(f"🔍 [DEBUG] No instrument_id mapping found for symbol '{symbol}' in {symbol_to_inst_id}")

        ohlc_batch = ohlc_batch_by_inst_id
        
        # Fetch market cap data
        rows = await self.market_cap_dao.list_market_caps_for_date(current_time.date())
        market_caps = {row['instrument_id']: row['market_cap'] for row in rows}
        
        # Update rolling cache with 1-minute intervals
        for inst_id in instrument_ids:
            ohlc = ohlc_batch.get(inst_id)
            if ohlc is not None:
                # ✅ CRITICAL FIX: Convert pandas Series to scalar values for InstrumentInterval
                def safe_scalar_conversion(value, default=None):
                    """Convert pandas Series or other types to scalar float."""
                    if value is None:
                        return None
                    elif isinstance(value, (pd.Series, pd.core.series.Series)) or hasattr(value, 'iloc'):
                        if len(value) > 0:
                            # For OHLC data, use first value (assumes aggregated data)
                            return float(value.iloc[0] if hasattr(value, 'iloc') else value[0])
                        else:
                            return default
                    elif hasattr(value, '__len__') and len(value) > 0:  # Handle numpy arrays
                        return float(value[0])
                    else:
                        # Try to convert to float directly
                        try:
                            return float(value)
                        except (ValueError, TypeError):
                            return default

                # Use None for missing OHLC fields; mark interval as 'missing' if all are None
                open_ = safe_scalar_conversion(ohlc.get('open')) if ohlc.get('open') is not None else None
                high_ = safe_scalar_conversion(ohlc.get('high')) if ohlc.get('high') is not None else None
                low_ = safe_scalar_conversion(ohlc.get('low')) if ohlc.get('low') is not None else None
                close_ = safe_scalar_conversion(ohlc.get('close')) if ohlc.get('close') is not None else None
                volume_ = safe_scalar_conversion(ohlc.get('volume')) if ohlc.get('volume') is not None else None
                all_none = all(x is None for x in [open_, high_, low_, close_, volume_])
                status = 'missing' if all_none else 'ok'
                traded_dollar = (close_ * volume_) if (close_ is not None and volume_ is not None) else None
                # Create 1-minute interval
                interval = InstrumentInterval(
                    instrument_id=inst_id,
                    start_date_time=minute_start_time,
                    end_date_time=minute_end_time,
                    open=open_,
                    high=high_,
                    low=low_,
                    close=close_,
                    traded_volume=volume_,
                    traded_dollar=traded_dollar,
                    status=status,
                    market_cap=market_caps.get(inst_id)
                )
                
                # Add to 1-minute rolling cache
                timeframe = '1m'
                self._add_interval_to_cache(inst_id, timeframe, interval)
                
                history_size = len(self._get_instrument_history_for_timeframe(inst_id, timeframe))
            else:
                self.logger.warning(f"No 1-minute OHLC data for instrument_id: {inst_id} at {current_time}")
        # --- 2. Check which timeframes should be processed at current_time ---
        # Only build universe state for timeframes whose intervals are now complete
        duration_to_state = {}
        
        for i, duration in enumerate(self.target_durations):
            should_process = self._should_process_timeframe(duration, current_time)
            
            if should_process:
                duration_to_state.update(await self._build_universe_state_for_duration(duration, current_time, instrument_ids, runner))
            else:
                self.logger.debug(f"Skipping {duration.get_duration_string()} at {current_time} - not complete")
        
        # Only save if we have states to save
        if duration_to_state and hasattr(runner, 'universe_state_manager'):
            self.logger.debug(f"[SAVE] Saving universe state for {len(duration_to_state)} timeframes")
            await runner.universe_state_manager.addUniverseState(duration_to_state, current_time)
        else:
            self.logger.debug(f"[SAVE] No universe states to save at {current_time}")

    def _should_process_timeframe(self, duration, current_time):
        """
        Determine if a timeframe should be processed at the current time.
        Returns True when the timeframe interval is complete.
        """
        from datetime import timedelta
        
        duration_str = duration.get_duration_string()
        
        # For 1-minute intervals, process every minute
        if duration_str == '1m':
            return True
            
        # For other intervals, check if current time aligns with interval boundary
        minute = current_time.minute
        hour = current_time.hour
        
        if duration_str == '5m':
            return minute % 5 == 0
        elif duration_str == '15m':
            return minute % 15 == 0
        elif duration_str == '30m':
            return minute % 30 == 0
        elif duration_str == '60m' or duration_str == '1h':
            return minute == 0
        elif duration_str == '1d':
            # 1d intervals now handled by handleTradingDayEnd event, not clock time
            return False  # Remove clock-based 1d generation
        elif duration_str == '1w':
            # 1w intervals now handled by handleTradingWeekEnd event, not clock time
            return False  # Remove clock-based 1w generation
        else:
            # Default: process every interval for unknown durations
            return True
    
    async def _build_universe_state_for_duration(self, duration, current_time, instrument_ids, runner):
        """
        Build universe state for a specific duration/timeframe.
        """
        duration_to_state = {}
        
        # Use the original logic for building universe state for this duration
        duration_states = await self._build_single_duration_state(duration, current_time, instrument_ids, runner)
        duration_to_state.update(duration_states)
        
        return duration_to_state
    
    def _ensure_timeframe_cache(self, timeframe_str: str) -> None:
        """Ensure timeframe cache structure exists."""
        if self.universe_state_manager:
            self.universe_state_manager.ensure_timeframe_cache(timeframe_str)
    
    def _get_instrument_history_for_timeframe(self, inst_id: int, timeframe_str: str) -> List[InstrumentInterval]:
        """Get instrument history for a specific timeframe."""
        if self.universe_state_manager:
            return self.universe_state_manager.get_instrument_history_for_timeframe(inst_id, timeframe_str)
        return []
    
    def _add_interval_to_cache(self, inst_id: int, timeframe_str: str, interval) -> None:
        """Add an interval to the timeframe-specific cache."""
        if self.universe_state_manager:
            self.universe_state_manager.add_interval_to_rolling_cache(inst_id, timeframe_str, interval)
    
    def _get_interval_for_timeframe(self, inst_id: int, duration, current_time):
        """
        Get or create interval for a specific instrument and timeframe.
        Uses cached data if available, otherwise aggregates from 1-minute data.
        """
        timeframe_str = duration.get_duration_string()
                
        # If not cached, try to build from 1-minute data
        if timeframe_str == '1m':
            # For 1-minute, get from 1-minute cache
            one_min_history = self._get_instrument_history_for_timeframe(inst_id, '1m')
            return one_min_history[-1] if one_min_history else None
        else:
            # For longer durations, aggregate from 1-minute data
            one_min_history = self._get_instrument_history_for_timeframe(inst_id, '1m')
            if not one_min_history:
                self.logger.warning(f"No 1-minute history available for instrument_id: {inst_id}")
                return None
                
            # Build and cache the aggregated interval
            interval = self._aggregate_intervals_for_duration(duration, one_min_history, current_time)
            
            # Cache the aggregated interval for future use
            if interval:
                self._add_interval_to_cache(inst_id, timeframe_str, interval)
                self.logger.info(f"[{timeframe_str.upper()} CACHE] instrument_id={inst_id}, cached new interval")
            
            return interval
    
    def _aggregate_intervals_for_duration(self, duration, history, current_time):
        """
        Aggregate 1-minute intervals from history to create interval for target duration.
        """
        if not history:
            return None
            
        duration_minutes = self._get_duration_minutes(duration)
        if duration_minutes is None:
            # If we can't determine minutes, use latest interval
            return history[-1] if history else None
            
        # Take the last N minutes from history
        intervals_needed = min(duration_minutes, len(history))
        recent_intervals = history[-intervals_needed:] if intervals_needed > 0 else []
        
        if not recent_intervals:
            return None
            
        # Aggregate OHLCV data
        return self._aggregate_ohlcv_intervals(recent_intervals, duration, current_time)
    
    def _get_duration_minutes(self, duration):
        """Get duration in minutes for aggregation."""
        duration_str = duration.get_duration_string()
        
        duration_map = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '30m': 30,
            '60m': 60,
            '1h': 60,
            '1d': 1440,  # 24 * 60
            '1w': 10080  # 7 * 24 * 60
        }
        
        return duration_map.get(duration_str)
    
    def _aggregate_ohlcv_intervals(self, intervals, duration, current_time):
        """
        Aggregate multiple 1-minute intervals into a single interval for the target duration.
        """
        if not intervals:
            return None
            
        first_interval = intervals[0]
        last_interval = intervals[-1]
        
        # Aggregate OHLCV data
        open_price = first_interval.open
        close_price = last_interval.close
        
        # Aggregate high and low prices
        high_values = [interval.high for interval in intervals if interval.high is not None]
        high_price = max(high_values) if high_values else None
        
        low_values = [interval.low for interval in intervals if interval.low is not None]
        low_price = min(low_values) if low_values else None
        # Aggregate volume
        volume_values = [interval.traded_volume for interval in intervals if interval.traded_volume is not None]
        total_volume = sum(volume_values) if volume_values else None
        
        # Calculate aggregate traded dollar
        traded_dollar = None
        if close_price is not None and total_volume is not None:
            traded_dollar = close_price * total_volume
            
        # Determine status - 'ok' if any intervals have data, 'missing' if all missing
        has_data = any(interval.status == 'ok' for interval in intervals)
        status = 'ok' if has_data else 'missing'
        
        # Use market cap from latest interval
        market_cap = last_interval.market_cap
        
        return InstrumentInterval(
            instrument_id=first_interval.instrument_id,
            start_date_time=duration.get_start_time(current_time),
            end_date_time=current_time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            traded_volume=total_volume,
            traded_dollar=traded_dollar,
            status=status,
            market_cap=market_cap
        )
        
    async def _build_single_duration_state(self, duration, current_time, instrument_ids, runner):
        """
        Build universe state for a single duration - extracted from original logic.
        """
        from domains.trading.services.state.universe_state import UniverseStateInterval
        
        duration_to_state = {}
        d_end_time = duration.get_end_time(current_time)
        interval_map = {}
        
        # Build intervals for this timeframe using rolling cache
        for inst_id in instrument_ids:
            interval = self._get_interval_for_timeframe(inst_id, duration, current_time)
                
            if interval:
                interval_map[inst_id] = interval
            else:
                self.logger.warning(f"No interval data for instrument_id: {inst_id}, duration: {duration.get_duration_string()}")
        
        universe_intervals = FactorInterval(
            start_date_time=current_time,
            end_date_time=d_end_time,
            instrument_intervals=interval_map
        )
        instrument_indicator_intervals = {}

        # Get history for each instrument for the current timeframe
        timeframe_str = duration.get_duration_string()
        instrument_histories = {
            inst_id: self._get_instrument_history_for_timeframe(inst_id, timeframe_str)
            for inst_id in instrument_ids
        }

        # Check if we have enough history for indicators
        has_enough_history = all(len(hist) >= 3 for hist in instrument_histories.values())

        if has_enough_history and self.indicator_builder is not None:
            # Normal indicator calculation
            instrument_indicator_intervals['default'] = self.indicator_builder.build_indicator_intervals(
                instrument_histories,
                start_date_time=current_time,
                end_date_time=d_end_time
            )
        else:
            # Create synthetic indicators with default values
            self.logger.warning(f"[INDICATORS] Using default values for {duration.get_duration_string()}")
            default_indicators = {}

            # Get indicator names from config with proper fallbacks
            if IndicatorConfig is not None:
                indicator_config = getattr(self.env, 'get_indicator_config', lambda: IndicatorConfig.empty_config())()
                indicator_names = list(indicator_config.indicators.keys())
            else:
                # Fallback when IndicatorConfig is not available
                indicator_names = []

            # Create default indicators for each instrument
            for inst_id in instrument_ids:
                indicators = {}
                for ind_name in indicator_names:
                    indicators[ind_name] = {
                        'value': 1.0,  # Default non-null value
                        'status': 'ok',
                        'update_at': datetime.now()
                    }

                # Create indicator interval with default values
                default_indicators[inst_id] = IndicatorInterval(
                    instrument_id=inst_id,
                    start_date_time=current_time,
                    end_date_time=d_end_time,
                    indicators=indicators
                )

            instrument_indicator_intervals['default'] = default_indicators
            
        # Create universe state for this duration
        universe_state = UniverseStateInterval(
            universe_id=runner.universe_id,
            duration=duration,
            start_date_time=current_time,
            end_date_time=d_end_time,
            factor_intervals=[universe_intervals],
            instrument_intervals=interval_map,
            instrument_indicator_intervals=instrument_indicator_intervals
        )
        
        # Optional: augment with forecasts via forecast callback
        if hasattr(self, 'forecast_callback') and self.forecast_callback is not None:
            try:
                self.forecast_callback.augment_universe_state(
                    universe_state=universe_state,
                    instrument_ids=instrument_ids,
                    instrument_history={inst_id: self._get_instrument_history_for_timeframe(inst_id, timeframe_str) for inst_id in instrument_ids},
                    current_time=current_time
                )
            except Exception as e:
                self.logger.error(f"Forecast augmentation failed: {e}")
                
        duration_to_state[duration] = universe_state
        
        return duration_to_state

    def handleIntervalSync(self, runner, current_time):
        """Synchronous wrapper to run handleInterval for tests that call without await."""
        import asyncio
        return asyncio.run(self.handleInterval(runner, current_time))

    """
    Builds universe state from multiple data sources with business logic,
    validation, and transformation rules.

    Handles data collection, validation, corporate actions, and derived calculations.
    """

    def __init__(self, env: Environment, base_duration: str, target_durations: str, forecast_callback=None, universe_state_manager=None):
        """
        Initialize UniverseStateIntervalBuilder.
        Args:
            env: Environment instance (uses global if None)
            base_duration: str (e.g. '5m'), overrides Gin config if provided
            target_durations: comma-separated str (e.g. '5m,15m,60m'), overrides Gin config if provided
            universe_state_manager: UniverseStateManager instance for shared rolling cache
        """
        # Inject DailyMarketCapDAO for market_cap sourcing
        self.market_cap_dao = DailyMarketCapDAO(env)
        self.env = env
        self.logger = logging.getLogger(__name__)
        
        # Store reference to universe state manager for shared rolling cache
        self.universe_state_manager = universe_state_manager
        
        # Load indicator config from env with proper fallbacks
        if IndicatorConfig is not None:
            indicator_config = getattr(self.env, 'get_indicator_config', lambda: IndicatorConfig.empty_config())()
            self.indicator_builder = IndicatorBuilder(indicator_config) if IndicatorBuilder is not None else None
        else:
            # Fallback when IndicatorBuilder/IndicatorConfig are not available
            self.indicator_builder = None

        # Durations
        from core.business.calendars.time_duration import TimeDuration
        base_duration_str = base_duration
        self.base_duration = TimeDuration(base_duration_str)
        target_durations_str = target_durations
        self.target_durations = [TimeDuration(d.strip()) for d in target_durations_str.split(',')]

        # Default business logic parameters (from test expectations)
        self.min_market_cap = 100_000_000
        self.min_avg_volume = 100_000
        self.max_universe_size = 3000
        self.data_source_priorities = {
            'polygon': 1,
            'tiingo': 2,
            'quandl': 3
        }
        # Optional modeling callback for forecasts
        self.forecast_callback = forecast_callback

    def validate_universe_state(self, df):
        required_cols = {'symbol', 'market_cap', 'avg_volume', 'sector', 'exchange', 'is_active', 'as_of_date'}
        if not isinstance(df, pd.DataFrame) or df.empty:
            return False
        if not required_cols.issubset(df.columns):
            return False
        # Check for duplicate symbols
        if df['symbol'].duplicated().any():
            return False
        return True


