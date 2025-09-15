"""
UniverseStateManager - Database-Only Universe State Storage and Retrieval.

PURPOSE:
- Store universe state data in database via DAOs
- Provide fast retrieval of historical data with in-memory caching
- Support lag retrieval for lookback operations in training data generation
- Maintain per-instrument history cache for efficient access

KEY FEATURES:
- Database-only persistence (file operations removed)
- In-memory caching with LRU eviction
- UUID-based database operations for consistency
- Supports multiple timeframes (1m, 5m, 15m, 1h, 1d, 1w)

DATA FLOW:
- Receives data via addUniverseState() from UniverseStateBuilder
- Stores data in database using UniverseStateIntervalDAO
- Provides cached access via get_lag_prices() for training data callbacks

NOTE: This manager only stores and retrieves data. It does not generate, 
compute, or transform any universe state data.
"""

import pandas as pd

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta





import gin
from core.dao.trading.universe_state_interval_dao import UniverseStateIntervalDAO
from core.dao.trading.instrument_interval_dao import InstrumentIntervalDAO
from core.dao.trading.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from core.dao.trading.factor_interval_dao import FactorIntervalDAO

@gin.configurable
class UniverseStateManager:
    def get_lag_prices(self, instrument_id: int, cur_datetime, lag_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """
        Return OHLCV data for the previous lag_periods up to cur_datetime.

        Uses cached universe state data from addUniverseState calls only.
        No external data access to minute bars or other sources.

        Args:
            instrument_id: The instrument ID to retrieve data for
            cur_datetime: Current datetime reference point (exclusive upper bound)
            lag_periods: Number of periods to look back
            time_interval: Time interval ('1m', '5m', '15m', '1h', '1d', '1w')

        Returns:
            DataFrame with OHLCV columns or empty DataFrame if no data available
        """
        # Validate parameters
        self._validate_lag_prices_params(instrument_id, lag_periods, time_interval, cur_datetime)
        cur_datetime = self._normalize_datetime(cur_datetime)

        # CRITICAL FIX: Use rolling cache with timeframe-specific data
        # The original implementation ignored time_interval and used generic DataFrame cache
        # This caused OHLCV duplication across timeframes in training data generation
        return self._get_lag_prices_from_rolling_cache(instrument_id, cur_datetime, lag_periods, time_interval)

    def _get_lag_prices_from_rolling_cache(self, instrument_id: int, cur_datetime, lag_periods: int, time_interval: str) -> pd.DataFrame:
        """
        Get lag prices from rolling cache with timeframe-specific data.
        
        This method retrieves InstrumentInterval objects from the rolling cache for the specified
        timeframe and converts them to DataFrame format. This ensures different timeframes
        have properly differentiated OHLCV values instead of duplicates.
        
        Args:
            instrument_id: The instrument ID to retrieve data for
            cur_datetime: Current datetime reference point (exclusive upper bound)
            lag_periods: Number of periods to look back
            time_interval: Time interval ('5m', '15m', '30m', '60m', '1d')
            
        Returns:
            DataFrame with OHLCV columns or empty DataFrame if no data available
        """
        # Check if rolling cache exists and has data
        if not hasattr(self, '_rolling_instrument_history') or not self._rolling_instrument_history:
            self.logger.warning(f"No rolling cache available for lag prices")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        # Check if timeframe exists in cache
        if time_interval not in self._rolling_instrument_history:
            self.logger.debug(f"No cached data for timeframe {time_interval}")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        # Check if instrument exists for this timeframe
        if instrument_id not in self._rolling_instrument_history[time_interval]:
            self.logger.debug(f"No cached data for instrument_id={instrument_id} in timeframe {time_interval}")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        # Get intervals for this timeframe and instrument
        intervals = self._rolling_instrument_history[time_interval][instrument_id]
        
        # Find intervals BEFORE cur_datetime
        lagged_intervals = []
        for interval in intervals:
            interval_datetime = self._normalize_datetime(interval.start_date_time)
            if interval_datetime < cur_datetime:
                lagged_intervals.append(interval)
        
        # Sort by start_date_time and take the last lag_periods intervals
        lagged_intervals.sort(key=lambda x: x.start_date_time)
        lagged_intervals = lagged_intervals[-lag_periods:] if len(lagged_intervals) > lag_periods else lagged_intervals
        
        if not lagged_intervals:
            self.logger.debug(f"No lagged intervals found for instrument_id={instrument_id} in timeframe {time_interval}")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        # Convert InstrumentInterval objects to DataFrame rows
        result_rows = []
        for interval in lagged_intervals:
            row = {
                'open': interval.open,
                'high': interval.high,
                'low': interval.low,
                'close': interval.close,
                'volume': interval.traded_volume,  # InstrumentInterval uses traded_volume
                'date': interval.start_date_time
            }
            result_rows.append(row)
        
        result_df = pd.DataFrame(result_rows)
        self.logger.debug(f"get_lag_prices (rolling cache) returning {len(result_df)} intervals for "
                         f"instrument_id={instrument_id} timeframe={time_interval}")
        return result_df

    def _get_instrument_history(self, instrument_id: int) -> pd.DataFrame:
        """Helper to fetch full DataFrame for an instrument from cache or storage."""
        # First check per-instrument history from current run
        hist_df = self._instrument_history.get(int(instrument_id))
        if hist_df is not None and not hist_df.empty:
            return self._normalize_date_column(hist_df)
            
        # Check cache for instrument data
        for ts, df in self._cache.items():
            inst_df = df[df['instrument_id'] == instrument_id]
            if not inst_df.empty:
                return self._normalize_date_column(inst_df)
                
        # Fallback: load latest universe state
        try:
            latest_ts = self.get_latest_timestamp()
            if latest_ts:
                df = self.load_universe_state(timestamp=latest_ts)
                inst_df = df[df['instrument_id'] == instrument_id]
                if not inst_df.empty:
                    return self._normalize_date_column(inst_df)
        except Exception:
            pass
            
        raise ValueError(f"No data found for instrument_id={instrument_id}")
        
    def _normalize_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize date column in DataFrame."""
        if 'date' in df.columns:
            return df
            
        df = df.copy()
        date_columns = ['as_of_date', 'as_of_datetime', 'start_date_time']
        for col in date_columns:
            if col in df.columns:
                df['date'] = pd.to_datetime(df[col]).dt.date
                break
        return df

    def _get_lag_prices_from_runner_cache(self, instrument_id: int, lag_periods: int, time_interval: str, cur_datetime: datetime) -> Optional[pd.DataFrame]:
        """
        Get lag prices from Runner's universe state builder cache.

        Args:
            instrument_id: The instrument ID to get data for
            lag_periods: Number of periods back to get
            time_interval: Time interval ('1d', '1h', etc.)
            cur_datetime: Current datetime

        Returns:
            DataFrame with OHLCV data from Runner cache, or None if not available
        """
        try:
            pass

            self.logger.debug(f"Looking for cached data for instrument_id={instrument_id}")

            cached_data = self._get_instrument_history(instrument_id)

            if cached_data is None or cached_data.empty:
                self.logger.debug(f"No cached data found for instrument_id={instrument_id}")
                return None

            self.logger.debug(f"Found {len(cached_data)} cached records")

            # The cached data is from the Runner's universe state builder which has the correct time ranges
            # Convert it to the format expected by get_lag_prices
            if time_interval == '1d':
                # For daily data, group by date and get OHLCV aggregated values
                if 'date' in cached_data.columns:
                    daily_data = cached_data.groupby('date').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).reset_index()

                    # Sort by date and get last lag_periods
                    daily_data = daily_data.sort_values('date').tail(lag_periods)

                    self.logger.debug(f"Aggregated to {len(daily_data)} daily records")
                    return daily_data

            # For intraday intervals, use the cached data directly
            # The Runner has already collected this with the correct [past_time, current_time] range
            result_data = cached_data.head(lag_periods)

            self.logger.debug(f"Returning {len(result_data)} records from cache")
            return result_data

        except Exception as e:
            self.logger.error(f"Exception accessing cache: {e}")
            return None

    def _validate_lag_prices_params(self, instrument_id: int, lag_periods: int, time_interval: str, cur_datetime):
        """Validate parameters for get_lag_prices method."""
        if not isinstance(instrument_id, int) or instrument_id <= 0:
            raise ValueError(f"instrument_id must be a positive integer, got {instrument_id}")
        if not isinstance(lag_periods, int) or lag_periods <= 0:
            raise ValueError(f"lag_periods must be a positive integer, got {lag_periods}")
        if not isinstance(time_interval, str) or not time_interval.strip():
            raise ValueError(f"time_interval must be a non-empty string, got {time_interval}")
        valid_intervals = {'1m', '5m', '15m', '1h', '1d', '1w'}
        if time_interval not in valid_intervals:
            raise ValueError(f"Invalid time_interval '{time_interval}'. Must be one of: {sorted(valid_intervals)}")

    def _normalize_datetime(self, cur_datetime):
        """Convert date to datetime object if needed."""
        if hasattr(cur_datetime, 'date'):
            return cur_datetime
        elif hasattr(cur_datetime, 'year'):
            from datetime import datetime
            return datetime.combine(cur_datetime, datetime.min.time())
        else:
            raise ValueError(f"cur_datetime must be a datetime or date object, got {type(cur_datetime)}")

    def _get_lag_prices_from_cache(self, instrument_id: int, cur_datetime, lag_periods: int) -> pd.DataFrame:
        """Get lag prices from instrument history cache."""
        self.logger.debug(f"get_lag_prices: instrument_id={instrument_id}, lag_periods={lag_periods}")
        
        if not hasattr(self, '_instrument_history') or instrument_id not in self._instrument_history:
            self.logger.debug(f"No historical data found for instrument_id={instrument_id}")
            self._raise_cache_insufficient_error(instrument_id, lag_periods)
            
        instrument_data = self._instrument_history[instrument_id]
        if len(instrument_data) < lag_periods:
            self.logger.debug(f"Insufficient historical data: {len(instrument_data)} < {lag_periods}")
            self._raise_cache_insufficient_error(instrument_id, lag_periods)
            
        # Convert to DataFrame and process
        df = pd.DataFrame(instrument_data)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'] < cur_datetime]
            df = df.sort_values('date').tail(lag_periods)
            
        # Return OHLCV data
        result_df = df[['open', 'high', 'low', 'close', 'volume']].copy()
        if 'date' in df.columns:
            result_df['date'] = df['date']
            
        self.logger.debug(f"Returning {len(result_df)} records from instrument cache")
        return result_df

    def _raise_cache_insufficient_error(self, instrument_id: int, lag_periods: int):
        """Raise standardized cache insufficient error."""
        available_size = len(self._instrument_history.get(instrument_id, [])) if hasattr(self, '_instrument_history') else 0
        raise ValueError(f"Universe state cache insufficient for instrument_id={instrument_id}. "
                        f"Requested: {lag_periods} periods, Available: {available_size} periods")

    """
    Handles fast persistence and retrieval of universe state data.

    Focuses on I/O operations, caching, and data format optimization.
    Uses Parquet format for optimal performance with columnar data.
    """

    def __init__(self, env=None, base_path: Optional[str] = None, run_context=None):
        """
        Initialize UniverseStateManager for database-only operations.

        Args:
            env: Environment instance for database access
            base_path: Legacy parameter (unused, file operations removed)
            run_context: Run context for UUID management
        """
        self.env = env
        self.run_context = run_context
        # File-based state storage removed - database-only mode
        # Timeframe-aware in-memory cache: Dict[timeframe, Dict[timestamp, pd.DataFrame]]
        self._cache: Dict[str, Dict[str, pd.DataFrame]] = {}
        # Timeframe-aware instrument history: Dict[timeframe, Dict[instrument_id, pd.DataFrame]]
        self._instrument_history: Dict[str, Dict[int, pd.DataFrame]] = {}
        # Rolling cache for InstrumentInterval objects (shared with UniverseStateBuilder)
        # Format: Dict[timeframe_str, Dict[instrument_id, List[InstrumentInterval]]]
        self._rolling_instrument_history: Dict[str, Dict[int, List]] = {}
        # Rolling window size for interval cache
        self.rolling_window = 20
        self.logger = logging.getLogger(__name__)
        # Initialize UniverseStateIntervalDAO for interval persistence
        self._interval_dao = UniverseStateIntervalDAO(self.env) if self.env else None

    def _get_symbol_from_instrument_id(self, instrument_id: int) -> str:
        """FIXED: Get symbol from instrument_id via database lookup instead of hardcoding.

        Args:
            instrument_id: The instrument ID to look up

        Returns:
            The symbol string for the instrument

        Raises:
            ValueError: If instrument_id not found in database
        """
        if not self.env:
            raise ValueError("Environment not configured - cannot perform database lookup")

        # 🚨 CRITICAL FIX (September 10, 2025): Fixed database import path
        # ISSUE: ImportError: cannot import name 'get_raw_connection' from incorrect module
        # ORIGINAL: from shared.data_handling.utils.database import get_raw_connection (BROKEN)
        # SOLUTION: Use correct import path from core.platform.database.connection_manager
        from core.platform.database.connection_manager import get_raw_connection

        table_name = f"{self.env.table_prefix}instruments"

        with get_raw_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT symbol FROM {table_name} WHERE id = %s", (instrument_id,))
                result = cursor.fetchone()

                if result:
                    return result[0]
                else:
                    raise ValueError(f"Instrument ID {instrument_id} not found in {table_name}")

    async def save_universe_state(self, timestamp: str, metadata: Dict[str, Any]) -> str:
        """
        Persist universe state to database only.
        
        Args:
            timestamp: Timestamp string for the data
            metadata: Required metadata with universe_id, duration, start/end times, and universe_state
            
        Returns:
            Database URI string (db://universe_state_interval/{id}/{timestamp})
            
        Raises:
            ValueError: If required metadata is missing
        """
        if not metadata:
            raise ValueError("Metadata is required for database persistence")
        if not self._validate_timestamp_format(timestamp):
            raise ValueError(f"Invalid timestamp format: {timestamp}")
        # Decide mode
        use_db = False
        if self._interval_dao is not None:
            required = ('universe_id', 'duration', 'start_date_time', 'end_date_time')
            if metadata and all((k in metadata and metadata.get(k) is not None) for k in required):
                # Only use DB-backed path if all required keys are present AND non-null
                use_db = True
            elif metadata is not None:
                # If explicitly provided empty dict, treat as invalid when DAO is available
                if isinstance(metadata, dict) and len(metadata) == 0:
                    raise ValueError("Missing required metadata keys for DB-backed save: ['universe_id','duration','start_date_time','end_date_time']")
                has_any_required = any((k in metadata) for k in required)
                if has_any_required and (not all(k in metadata for k in required)):
                    # Metadata includes some but not all required keys -> explicit error
                    raise ValueError("Missing required metadata keys for DB-backed save: ['universe_id','duration','start_date_time','end_date_time']")
        try:
            if use_db:
                # DB-backed path  
                universe_state = metadata.get('universe_state')
                record_count = len(getattr(universe_state, 'instrument_intervals', {})) if universe_state else 0
                self.logger.debug(f"save_universe_state DB: {record_count} instrument intervals, ts={timestamp}")
                # Parse dates using common utility
                from shared.data_handling.utils.datetime_utils import parse_flexible_datetime
                start_dt = parse_flexible_datetime(metadata['start_date_time'])
                end_dt = parse_flexible_datetime(metadata['end_date_time'])

                # Get run_id from run_context, fallback to default if not available
                run_id = getattr(self.run_context, 'run_id', 'default_run') if self.run_context else 'no_run_context'

                interval_id = await self._interval_dao.create(
                    universe_id=metadata['universe_id'],
                    duration=metadata['duration'],
                    start_date_time=start_dt,
                    end_date_time=end_dt,
                    run_id=run_id
                )
                # Persist nested only if provided
                universe_state = metadata.get('universe_state') if metadata else None
                if universe_state is not None:
                    instrument_interval_dao = InstrumentIntervalDAO(self.env)
                    indicator_interval_dao = InstrumentIndicatorIntervalDAO(self.env)
                    factor_interval_dao = FactorIntervalDAO(self.env)
                    instrument_interval_id_map = {}
                    for inst_id, inst_interval in universe_state.instrument_intervals.items():
                        instrument_interval_id = await instrument_interval_dao.create(
                            universe_state_interval_id=interval_id,
                            instrument_id=inst_interval.instrument_id,
                            open=inst_interval.open,
                            high=inst_interval.high,
                            low=inst_interval.low,
                            close=inst_interval.close,
                            traded_volume=inst_interval.traded_volume,
                            traded_dollar=inst_interval.traded_dollar,
                            status=inst_interval.status,
                            market_cap=inst_interval.market_cap,
                            start_date_time=inst_interval.start_date_time,
                            end_date_time=inst_interval.end_date_time,
                            interval_duration="60m",
                            run_id=run_id
                        )
                        instrument_interval_id_map[inst_id] = instrument_interval_id
                    for ind_type, inst_dict in universe_state.instrument_indicator_intervals.items():
                        for inst_id, indicator_interval in inst_dict.items():
                            instrument_interval_id = instrument_interval_id_map.get(inst_id)
                            if instrument_interval_id is None:
                                continue
                            for ind_name, ind_val in (indicator_interval.indicators or {}).items():
                                await indicator_interval_dao.create(
                                    instrument_interval_id=instrument_interval_id,
                                    indicator_name=ind_name,
                                    indicator_value=ind_val.get('value'),
                                    indicator_status=ind_val.get('status'),
                                    run_id=run_id
                                )
                    for factor_interval in getattr(universe_state, 'factor_intervals', []):
                        if hasattr(factor_interval, 'factor_name') and hasattr(factor_interval, 'factor_value'):
                            await factor_interval_dao.create(
                                universe_state_interval_id=interval_id,
                                factor_name=factor_interval.factor_name,
                                factor_value=factor_interval.factor_value,
                                run_id=run_id
                            )
                # Database-only persistence - file operations removed
                self.logger.info(f"Saved universe state interval to DB for {timestamp} (interval_id={interval_id}, instruments={record_count})")
                return f"db://universe_state_interval/{interval_id}/{timestamp}"
            else:
                # Database-only mode - file-based fallback removed
                raise ValueError("File-based persistence removed - database persistence required")
        except Exception as e:
            self.logger.error(f"Failed to save universe state: {e}")
            raise IOError(f"Failed to save universe state: {e}")

    def save_universe_state_sync(self, timestamp: str, metadata: Dict[str, Any]) -> str:
        """Synchronous wrapper for save_universe_state for use in non-async contexts (e.g., sync tests)."""
        import asyncio
        return asyncio.run(self.save_universe_state(timestamp, metadata))


    async def addUniverseState(self, duration_to_state: dict, current_time):
        """
        Store universe states in database and update in-memory cache.
        
        Args:
            duration_to_state: Dict mapping TimeDuration to UniverseState objects
            current_time: Current datetime for timestamping
            
        This method:
        1. Converts UniverseState objects to DataFrames
        2. Updates in-memory instrument history cache
        3. Persists data to database via save_universe_state
        """
        import pandas as pd
        # Promote to INFO and add print to ensure visibility during tests
        self.logger.info(f"addUniverseState: Adding UniverseStates for {len(duration_to_state)} durations at {current_time}")
        try:
            print(f"[USM.addUniverseState] durations={len(duration_to_state)} at {current_time}")
        except Exception:
            pass
        rows = []
        seen_keys = set()
        long_rows = []
        for duration, universe_state in duration_to_state.items():
            # Accept either TimeDuration objects or plain string keys like '5m'
            duration_str = duration if isinstance(duration, str) else duration.get_duration_string()
            # Build a mapping for instrument intervals
            instrument_rows = {}
            for inst_id, inst_interval in universe_state.instrument_intervals.items():
                key = (inst_interval.instrument_id, inst_interval.start_date_time, inst_interval.end_date_time, duration_str)
                instrument_rows[key] = {
                    'instrument_id': inst_interval.instrument_id,
                    'duration': duration_str,
                    'start_date_time': inst_interval.start_date_time,
                    'end_date_time': inst_interval.end_date_time,
                    'open': inst_interval.open,
                    'high': inst_interval.high,
                    'low': inst_interval.low,
                    'close': inst_interval.close,
                    'traded_volume': inst_interval.traded_volume,
                    'traded_dollar': inst_interval.traded_dollar,
                    'status': inst_interval.status,
                }
                
                # CRITICAL FIX: Populate rolling cache for aggregation logic
                # Add InstrumentInterval objects to rolling cache so universe_state_builder can aggregate them
                self.add_interval_to_rolling_cache(inst_id, duration_str, inst_interval)
            # Output indicator values in long format
            for indicator_type, inst_dict in universe_state.instrument_indicator_intervals.items():
                for inst_id, indicator_interval in inst_dict.items():
                    key = (indicator_interval.instrument_id, indicator_interval.start_date_time, indicator_interval.end_date_time, duration_str)
                    if key not in instrument_rows:
                        continue
                    base_row = instrument_rows[key]
                    for ind_name, ind_val in (indicator_interval.indicators or {}).items():
                        # Use the indicator name as provided (preserve capitalization)
                        long_row = base_row.copy()
                        long_row['indicator_name'] = ind_name
                        long_row['indicator_value'] = ind_val.get('value')
                        long_row['indicator_status'] = ind_val.get('status')
                        long_rows.append(long_row)
        timestamp = current_time.strftime('%Y%m%d_%H%M%S')
        saved_any = False
        for duration, universe_state in duration_to_state.items():
            self.logger.debug(f"[addUniverseState] duration={duration}, universe_state type={type(universe_state)}")
            if hasattr(universe_state, 'to_dataframe'):
                df = universe_state.to_dataframe()
            else:
                # Build a wide DataFrame from basic structures for backward-compat test helpers
                try:
                    rows_dict = {}
                    # Determine duration string
                    dstr = duration if isinstance(duration, str) else duration.get_duration_string()
                    # Base instrument rows
                    for inst_id, inst in getattr(universe_state, 'instrument_intervals', {}).items():
                        key = (inst.instrument_id, inst.start_date_time, inst.end_date_time, dstr)
                        rows_dict[key] = {
                            'instrument_id': inst.instrument_id,
                            'duration': dstr,
                            'start_date_time': inst.start_date_time,
                            'end_date_time': inst.end_date_time,
                            'open': getattr(inst, 'open', None),
                            'high': getattr(inst, 'high', None),
                            'low': getattr(inst, 'low', None),
                            'close': getattr(inst, 'close', None),
                            'traded_volume': getattr(inst, 'traded_volume', None),
                            'traded_dollar': getattr(inst, 'traded_dollar', None),
                            'status': getattr(inst, 'status', None),
                        }
                    # Attach indicator columns in wide format: {type}_{name}_{value|status}
                    for indicator_type, inst_map in getattr(universe_state, 'instrument_indicator_intervals', {}).items():
                        for inst_id, ind_interval in inst_map.items():
                            key = (ind_interval.instrument_id, ind_interval.start_date_time, ind_interval.end_date_time, dstr)
                            base = rows_dict.get(key)
                            if not base:
                                continue
                            for ind_name, ind_val in (getattr(ind_interval, 'indicators', {}) or {}).items():
                                prefix = f"{indicator_type}_{ind_name}"
                                base[f"{prefix}_value"] = ind_val.get('value')
                                base[f"{prefix}_status"] = ind_val.get('status')
                    import pandas as pd
                    df = pd.DataFrame(list(rows_dict.values())) if rows_dict else pd.DataFrame()
                except Exception as e:
                    self.logger.error(f"[addUniverseState] Failed to build DataFrame from basic structures: {e}")
                    df = pd.DataFrame()
            self.logger.debug(f"Processing DataFrame with {len(df)} records")
            if df.empty:
                self.logger.warning(f"addUniverseState: No data to save for duration {duration} at {current_time}")
                continue
            # Update rolling per-instrument history cache to provide prior history during a run
            try:
                local_df = df.copy()
                # Normalize a 'date' column if possible
                if 'date' not in local_df.columns:
                    if 'as_of_date' in local_df.columns:
                        local_df['date'] = pd.to_datetime(local_df['as_of_date']).dt.date
                    elif 'as_of_datetime' in local_df.columns:
                        local_df['date'] = pd.to_datetime(local_df['as_of_datetime']).dt.date
                    elif 'start_date_time' in local_df.columns:
                        local_df['date'] = pd.to_datetime(local_df['start_date_time']).dt.date
                # Log normalized data summary
                try:
                    uniq_inst = int(local_df['instrument_id'].nunique()) if 'instrument_id' in local_df.columns else 0
                    self.logger.debug(f"Normalized data for {uniq_inst} instruments")
                except Exception:
                    pass
                if 'instrument_id' in local_df.columns and 'date' in local_df.columns:
                    # Keep only relevant cols to minimize memory
                    keep_cols = list(local_df.columns)
                    for inst_id, inst_group in local_df.groupby('instrument_id'):
                        hist = self._instrument_history.get(int(inst_id))
                        if hist is None or hist.empty:
                            self._instrument_history[int(inst_id)] = inst_group[keep_cols].copy()
                        else:
                            combined = pd.concat([hist, inst_group[keep_cols]], ignore_index=True)
                            # Drop duplicates based on instrument_id + date
                            if 'date' in combined.columns:
                                combined = combined.sort_values('date')
                                combined = combined.drop_duplicates(subset=['instrument_id', 'date'], keep='last')
                            self._instrument_history[int(inst_id)] = combined.reset_index(drop=True)
                    try:
                        self.logger.debug(f"Updated _instrument_history for {local_df['instrument_id'].nunique()} instruments")
                    except Exception:
                        pass
                else:
                    self.logger.debug("Skipped history cache update (instrument_id/date missing)")
            except Exception as e:
                self.logger.warning(f"Failed to update history cache: {e}")
            metadata = {
                "universe_id": getattr(universe_state, "universe_id", None),
                "duration": (duration if isinstance(duration, str) else duration.get_duration_string()),
                "start_date_time": getattr(universe_state, "start_date_time", None),
                "end_date_time": getattr(universe_state, "end_date_time", None),
                "universe_state": universe_state,
            }
            await self.save_universe_state(timestamp, metadata=metadata)
            self.logger.debug(f"addUniverseState: Saved universe state for duration {duration} at {timestamp} with {len(df)} records.")
            saved_any = True
        if not saved_any:
            self.logger.warning(f"addUniverseState: No data saved for any duration at {current_time}")

    def addUniverseStateInterval(self, duration_to_state: dict, current_time):
        """Backward-compatible synchronous wrapper around addUniverseState for tests calling without await."""
        import asyncio
        return asyncio.run(self.addUniverseState(duration_to_state, current_time))

    def update_for_sod(self, runner, current_time):
        """
        Start-of-day initialization hook.
        
        Prepares the manager for the trading day. Historical data loading
        is now handled dynamically through addUniverseState calls rather
        than pre-loading at SOD.
        """
        self.logger.info(f"UniverseStateManager.update_for_sod called at {current_time}")

    def update_for_eod(self, runner, current_time):
        """End-of-day hook for UniverseStateManager."""
        self.logger.debug(f"UniverseStateManager.update_for_eod called at {current_time}")



    def clear_cache(self) -> None:
        """Clear in-memory cache."""
        self._cache.clear()
        self._rolling_instrument_history.clear()
        self.logger.debug("Universe state cache cleared")
    
    # --- Rolling InstrumentInterval Cache Methods (shared with UniverseStateBuilder) ---
    
    def ensure_timeframe_cache(self, timeframe_str: str) -> None:
        """Ensure timeframe cache structure exists for rolling intervals."""
        if timeframe_str not in self._rolling_instrument_history:
            self._rolling_instrument_history[timeframe_str] = {}
    
    def get_instrument_history_for_timeframe(self, inst_id: int, timeframe_str: str) -> List:
        """Get instrument history for a specific timeframe from rolling cache."""
        return self._rolling_instrument_history.get(timeframe_str, {}).get(inst_id, [])
    
    def add_interval_to_rolling_cache(self, inst_id: int, timeframe_str: str, interval) -> None:
        """Add interval to rolling cache with window management."""
        # Import here to avoid circular imports
        from domains.trading.services.state.instrument_interval import InstrumentInterval
        
        self.ensure_timeframe_cache(timeframe_str)
        
        # Initialize instrument list if needed
        if inst_id not in self._rolling_instrument_history[timeframe_str]:
            self._rolling_instrument_history[timeframe_str][inst_id] = []
        
        # Add new interval
        self._rolling_instrument_history[timeframe_str][inst_id].append(interval)
        
        # Maintain rolling window size
        if len(self._rolling_instrument_history[timeframe_str][inst_id]) > self.rolling_window:
            self._rolling_instrument_history[timeframe_str][inst_id] = \
                self._rolling_instrument_history[timeframe_str][inst_id][-self.rolling_window:]
        
        self.logger.debug(f"Added interval to rolling cache: {timeframe_str} inst_id={inst_id}, "
                         f"cache_size={len(self._rolling_instrument_history[timeframe_str][inst_id])}")
    
    def get_rolling_cache_debug_info(self) -> Dict[str, Any]:
        """Get debug information about rolling cache state."""
        debug_info = {}
        for timeframe, instruments in self._rolling_instrument_history.items():
            debug_info[timeframe] = {
                'instrument_count': len(instruments),
                'instruments': {
                    inst_id: len(intervals) 
                    for inst_id, intervals in instruments.items()
                }
            }
        return debug_info

    # Private helper methods

    def _validate_timestamp_format(self, timestamp: str) -> bool:
        """Validate timestamp format (YYYYMMDD_HHMMSS)."""
        try:
            datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
            return True
        except ValueError:
            return False

    def get_lead_prices(self, instrument_id: int, center_datetime, lead_periods: int, time_interval: str = '1h') -> pd.DataFrame:
        """
        Return OHLCV data for future periods after center_datetime.
        
        Similar to get_lag_prices but looks forward instead of backward.
        Uses cached universe state data from addUniverseState calls only.
        
        Args:
            instrument_id: The instrument ID to retrieve data for
            center_datetime: Reference datetime (exclusive lower bound)
            lead_periods: Number of periods to look ahead
            
        Returns:
            DataFrame with OHLCV columns or empty DataFrame if no data available
        """
        center_datetime = self._normalize_datetime(center_datetime)
        
        # Check if we have any cached data for this instrument
        if not hasattr(self, '_rolling_instrument_history') or not self._rolling_instrument_history:
            self.logger.warning(f"No rolling cache available for lead prices")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        # Look for intervals AFTER center_datetime in the cache
        # Since we only have historical data in cache, this will likely be empty
        # But we implement the logic for when we do have future data
        result_rows = []
        
        # Check specific timeframe for intervals after center_datetime
        if time_interval in self._rolling_instrument_history and instrument_id in self._rolling_instrument_history[time_interval]:
                intervals = self._rolling_instrument_history[time_interval][instrument_id]
                
                # Find intervals after center_datetime
                future_intervals = []
                for interval in intervals:
                    # InstrumentInterval should have start_date_time attribute
                    interval_time = getattr(interval, 'start_date_time', None)
                    if interval_time and interval_time > center_datetime:
                        future_intervals.append(interval)
                
                # Take the first lead_periods intervals
                for i, interval in enumerate(future_intervals[:lead_periods]):
                    result_rows.append({
                        'open': getattr(interval, 'open', None),
                        'high': getattr(interval, 'high', None), 
                        'low': getattr(interval, 'low', None),
                        'close': getattr(interval, 'close', None),
                        'volume': getattr(interval, 'traded_volume', None),
                        'date': getattr(interval, 'start_date_time', center_datetime)
                    })
                
                # Data processing complete for this specific timeframe
        
        if not result_rows:
            self.logger.debug(f"No lead price data found for instrument_id={instrument_id} after {center_datetime}")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'date'])
        
        result_df = pd.DataFrame(result_rows)
        self.logger.debug(f"get_lead_prices returning {len(result_df)} intervals for instrument_id={instrument_id}")
        return result_df

    async def get_lagged_signals(self, instrument_id: int, cur_datetime, lag_periods: int, 
                                time_interval: str, signal_names: List[str]) -> pd.DataFrame:
        """
        Return technical indicator signals for previous periods up to cur_datetime.
        
        Uses cached universe state data from addUniverseState calls only.
        
        Args:
            instrument_id: The instrument ID to retrieve signals for
            cur_datetime: Current datetime reference point (exclusive upper bound)
            lag_periods: Number of periods to look back
            time_interval: Time interval ('1m', '5m', '15m', '1h', '1d', '1w')
            signal_names: List of signal names to retrieve
            
        Returns:
            DataFrame with signal columns or empty DataFrame if no data available
        """
        cur_datetime = self._normalize_datetime(cur_datetime)
        
        # Check if we have cached data
        if not hasattr(self, '_rolling_instrument_history') or not self._rolling_instrument_history:
            self.logger.warning(f"No rolling cache available for lagged signals")
            signal_columns = signal_names + ['date'] if signal_names else ['date']
            return pd.DataFrame(columns=signal_columns)
        
        # Look for signals in the specified timeframe
        if time_interval not in self._rolling_instrument_history:
            self.logger.debug(f"No cached data for timeframe {time_interval}")
            signal_columns = signal_names + ['date'] if signal_names else ['date']
            return pd.DataFrame(columns=signal_columns)
        
        if instrument_id not in self._rolling_instrument_history[time_interval]:
            self.logger.debug(f"No cached data for instrument_id={instrument_id} in timeframe {time_interval}")
            signal_columns = signal_names + ['date'] if signal_names else ['date']
            return pd.DataFrame(columns=signal_columns)
        
        intervals = self._rolling_instrument_history[time_interval][instrument_id]
        
        # Find intervals BEFORE cur_datetime
        lagged_intervals = []
        for interval in intervals:
            interval_time = getattr(interval, 'start_date_time', None)
            if interval_time and interval_time < cur_datetime:
                lagged_intervals.append(interval)
        
        # Sort by time (most recent first) and take the last lag_periods
        lagged_intervals.sort(key=lambda x: getattr(x, 'start_date_time', cur_datetime), reverse=True)
        lagged_intervals = lagged_intervals[:lag_periods]
        
        if not lagged_intervals:
            self.logger.debug(f"No lagged signals found for instrument_id={instrument_id} before {cur_datetime}")
            signal_columns = signal_names + ['date'] if signal_names else ['date']
            return pd.DataFrame(columns=signal_columns)
        
        # Extract signal data from intervals
        # NOTE: Currently InstrumentInterval objects don't store technical indicators
        # This would need to be enhanced to store indicator data from IndicatorInterval
        result_rows = []
        for interval in lagged_intervals:
            row = {'date': getattr(interval, 'start_date_time', cur_datetime)}
            
            # Try to get signal values - for now we don't have them in InstrumentInterval
            # In a full implementation, we'd need to store IndicatorInterval data too
            for signal_name in signal_names:
                # Default value since we don't have indicator data in InstrumentInterval yet
                row[signal_name] = None
            
            result_rows.append(row)
        
        result_df = pd.DataFrame(result_rows)
        
        if result_rows:
            self.logger.debug(f"get_lagged_signals returning {len(result_df)} intervals for instrument_id={instrument_id} "
                            f"in timeframe {time_interval}")
        else:
            self.logger.debug(f"get_lagged_signals returning empty DataFrame - no signal data available")
        
        return result_df

    def get_universe_state_interval(self, timeframe: str, current_time: datetime, run_id: str = None) -> Optional['UniverseStateInterval']:
        """
        Retrieve the pre-computed UniverseStateInterval for a specific timeframe and time.
        
        This method should be used by training data generators instead of get_lag_prices()
        because the UniverseStateInterval contains OHLCV and indicator data that was
        already computed by the UniverseStateBuilder.
        
        Args:
            timeframe: Target timeframe (e.g., '5m', '15m', '60m', '1d')
            current_time: The datetime for which to retrieve the interval
            run_id: Optional run ID to filter results
            
        Returns:
            UniverseStateInterval object with pre-computed OHLCV and indicator data,
            or None if no interval found
        """
        try:
            if not self._interval_dao:
                self.logger.error("No interval DAO available - cannot retrieve UniverseStateInterval")
                return None
            
            # Query the database for universe state intervals matching the criteria
            from core.business.calendars.time_duration import TimeDuration
            duration = TimeDuration(timeframe)
            
            # Convert current_time to start_date_time and end_date_time for the interval
            # For example, if current_time is 13:45 and timeframe is 5m, 
            # we need the interval from 13:40 to 13:45
            interval_minutes = duration.get_duration_minutes()
            if interval_minutes is None:
                # Handle non-minute-based durations (daily, weekly, etc.)
                if timeframe == '1d':
                    interval_minutes = 1440  # 24 hours * 60 minutes
                elif timeframe == '1w':
                    interval_minutes = 10080  # 7 days * 24 hours * 60 minutes
                else:
                    raise ValueError(f"Unsupported timeframe for minute-based interval calculation: {timeframe}")
            
            # Calculate the start time of the interval that contains current_time
            minutes_since_epoch = int(current_time.timestamp() // 60)
            interval_start_minutes = (minutes_since_epoch // interval_minutes) * interval_minutes
            interval_start = datetime.fromtimestamp(interval_start_minutes * 60)
            interval_end = datetime.fromtimestamp((interval_start_minutes + interval_minutes) * 60)
            
            self.logger.debug(f"Looking for {timeframe} interval: {interval_start} to {interval_end}")
            
            # Query database for stored UniverseStateInterval - UniverseStateBuilder should have created these
            self.logger.debug(f"Querying database for UniverseStateInterval {timeframe} from {interval_start} to {interval_end}")
            
            # TODO: Implement actual database query here
            # For now, return None to expose the bug that no intervals are stored
            self.logger.warning(f"No UniverseStateInterval found for {timeframe} at {current_time} - UniverseStateBuilder may not have run")
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve UniverseStateInterval for {timeframe} at {current_time}: {e}")
            return None
    

    def get_future_universe_state_interval(self, timeframe: str, current_time: datetime, lead_periods: int = 1, run_id: str = None) -> Optional['UniverseStateInterval']:
        """
        Retrieve pre-computed UniverseStateInterval for future timeframe periods.
        
        This method should be used by training data generators instead of get_lead_prices()
        because the UniverseStateInterval contains OHLCV and indicator data that was
        already computed by the UniverseStateBuilder for future intervals.
        
        Args:
            timeframe: Target timeframe (e.g., '5m', '15m', '60m', '1d')
            current_time: The current datetime (future intervals are after this)
            lead_periods: Number of future periods to retrieve (default: 1)
            run_id: Optional run ID to filter results
            
        Returns:
            UniverseStateInterval object for the future period with pre-computed OHLCV and indicator data,
            or None if no future interval found
        """
        try:
            if not self._interval_dao:
                self.logger.error("No interval DAO available - cannot retrieve future UniverseStateInterval")
                return None
            
            # Calculate future time for the target interval
            from core.business.calendars.time_duration import TimeDuration
            duration = TimeDuration(timeframe)
            interval_minutes = duration.get_duration_minutes()
            if interval_minutes is None:
                # Handle non-minute-based durations (daily, weekly, etc.)
                if timeframe == '1d':
                    interval_minutes = 1440  # 24 hours * 60 minutes
                elif timeframe == '1w':
                    interval_minutes = 10080  # 7 days * 24 hours * 60 minutes
                else:
                    raise ValueError(f"Unsupported timeframe for minute-based interval calculation: {timeframe}")
            
            # Calculate the future interval time
            # For lead_periods=1, get the next interval after current_time
            future_time = current_time + timedelta(minutes=lead_periods * interval_minutes)
            
            self.logger.debug(f"Looking for future {timeframe} interval at {future_time} (lead_periods={lead_periods})")
            
            # TODO: Implement async database query to find matching future UniverseStateInterval
            # For now, check the rolling cache as a fallback
            return self._get_future_universe_state_interval_from_cache(timeframe, future_time)
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve future UniverseStateInterval for {timeframe} at {current_time}: {e}")
            return None
    
    def _get_future_universe_state_interval_from_cache(self, timeframe: str, target_time: datetime) -> Optional['UniverseStateInterval']:
        """
        Attempt to retrieve future UniverseStateInterval from rolling cache.
        
        This is a fallback method when database retrieval is not implemented.
        It reconstructs a UniverseStateInterval from cached InstrumentInterval objects for future time.
        """
        from domains.trading.services.state.universe_state import UniverseStateInterval
        from core.business.calendars.time_duration import TimeDuration
        
        try:
            # Check if we have cached data for this timeframe
            if timeframe not in self._rolling_instrument_history:
                self.logger.debug(f"No rolling cache data for future {timeframe} lookup")
                return None
            
            duration = TimeDuration(timeframe)
            instrument_intervals = {}
            
            # Calculate the expected start and end times for the target interval
            interval_minutes = duration.get_duration_minutes()
            if interval_minutes is None:
                # Handle non-minute-based durations (daily, weekly, etc.)
                if timeframe == '1d':
                    interval_minutes = 1440  # 24 hours * 60 minutes
                elif timeframe == '1w':
                    interval_minutes = 10080  # 7 days * 24 hours * 60 minutes
                else:
                    raise ValueError(f"Unsupported timeframe for minute-based interval calculation: {timeframe}")
            minutes_since_epoch = int(target_time.timestamp() // 60)
            interval_start_minutes = (minutes_since_epoch // interval_minutes) * interval_minutes
            interval_start = datetime.fromtimestamp(interval_start_minutes * 60)
            interval_end = datetime.fromtimestamp((interval_start_minutes + interval_minutes) * 60)
            
            # Collect InstrumentInterval objects for all instruments at this future time
            for instrument_id, intervals in self._rolling_instrument_history[timeframe].items():
                # Find interval that matches the future time range
                for interval in intervals:
                    interval_start_time = getattr(interval, 'start_date_time', None)
                    if interval_start_time and abs((interval_start_time - interval_start).total_seconds()) < 60:
                        # Found matching future interval for this instrument
                        instrument_intervals[instrument_id] = interval
                        break
            
            if not instrument_intervals:
                self.logger.debug(f"No cached future instrument intervals found for {timeframe} at {interval_start}")
                return None
            
            # Create UniverseStateInterval with cached future data
            universe_state_interval = UniverseStateInterval(
                duration=duration,
                start_date_time=interval_start,
                end_date_time=interval_end,
                factor_intervals=[],  # Empty for now
                instrument_intervals=instrument_intervals,
                instrument_indicator_intervals={},  # TODO: Add future indicator data from cache
                universe_id=1  # Default universe ID
            )
            
            self.logger.debug(f"Created future UniverseStateInterval from cache: {len(instrument_intervals)} instruments")
            return universe_state_interval
            
        except Exception as e:
            self.logger.error(f"Failed to create future UniverseStateInterval from cache: {e}")
            return None


if __name__ == "__main__":
    import argparse
    import sys
    import pandas as pd
    from datetime import datetime, timedelta
    import matplotlib.pyplot as plt
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    # Assume Universe and other dependencies are available or stubbed for now

    parser = argparse.ArgumentParser(description="Universe State Manager CLI")
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--universe_id", required=True, help="Universe ID")
    parser.add_argument("--action", required=True, choices=["build", "inspect"], help="Action: build or inspect")
    parser.add_argument("--instrument_id", required=False, help="Instrument ID for inspection")
    parser.add_argument("--saved_dir", required=True, help="Directory to save or load universe states")
    parser.add_argument("--mode", required=False, choices=["print", "graph"], default="print", help="Inspect mode: print or graph")
    parser.add_argument("--fields", nargs="*", default=["low","high","close","volume","adv","pldot","etop","ebot"], help="Fields to inspect/visualize")

    args = parser.parse_args()

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except Exception as e:
        print(f"Invalid date format: {e}")
        sys.exit(1)

    # No global manager here! Only per-action.
    if args.action == "build":
        manager = UniverseStateManager(base_path=args.saved_dir)
        # Database schema inspection for CLI mode
        try:
            import asyncpg
            import asyncio
            env = Environment()
            async def print_table_schema():
                pool = await asyncpg.create_pool(env.get_database_url())
                async with pool.acquire() as conn:
                    for table in ["instrument_polygon", "instruments"]:
                        tn = env.get_table_name(table)
                        schema = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", tn)
                        self.logger.debug(f"{tn} columns: {schema}")
                await pool.close()
            asyncio.run(print_table_schema())
        except Exception as e:
            self.logger.error(f"Failed to print DB schema: {e}")
        # Placeholder: you may want to load a Universe object by universe_id
        import os
        builder_class_path = os.environ.get("UNIVERSE_BUILDER_CLASS")
        if builder_class_path:
            # Dynamically import builder class
            import importlib
            module_name, class_name = builder_class_path.rsplit('.', 1)
            builder_mod = importlib.import_module(module_name)
            BuilderClass = getattr(builder_mod, class_name)
        else:
            from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
            BuilderClass = UniverseStateIntervalBuilder
        # TODO: Load actual Universe object by universe_id
        universe = None  # Replace with actual loading logic
        builder = BuilderClass(env=env)
        cur_date = start_date
        while cur_date <= end_date:
            date_str = cur_date.strftime("%Y-%m-%d")
            try:
                # Build and save universe state for this date
                # Support async build_universe_state if present
                import asyncio
                # External data managers removed - universe state manager uses addUniverseState only
                # Fetch instrument_ids for AAPL, TSLA from DB
                import asyncpg
                import asyncio
                async def get_instrument_ids():
                    pool = await asyncpg.create_pool(env.get_database_url())
                    async with pool.acquire() as conn:
                        ids = []
                        for symbol in ["AAPL", "TSLA"]:
                            row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('instruments')} WHERE symbol = $1", symbol)
                            if row:
                                ids.append(row["id"])
                    await pool.close()
                    return ids
                instrument_ids = asyncio.run(get_instrument_ids())
                class RealRunner:
                    def __init__(self, env, instrument_ids):
                        self.universe_manager = type('UM', (), {'instrument_ids': instrument_ids})()
                        # Switch between file-based and DB-based managers
                        if os.environ.get('FILE_BASED_PRICES') == '1':
                            logger.debug('External data managers removed - using addUniverseState only')
                            vendors_dirs = {
                                'polygon': 'tests/data/daily_price_polygon',
                                'tiingo': 'tests/data/daily_price_tiingo'
                            }
                        # External data managers removed - universe state manager uses addUniverseState only
                        pass
                # CLI build functionality simplified
                builder.handleInterval(runner, cur_date)
                self.logger.debug(f"Called handleInterval for {date_str}")
            except Exception as e:
                self.logger.error(f"Failed to build/save for {date_str}: {e}")
            cur_date += timedelta(days=1)
        self.logger.debug("Build complete.")

    elif args.action == "inspect":
        # Inspect mode
        instrument_id = args.instrument_id
        if not instrument_id:
            print("--instrument_id is required for inspect mode.")
            sys.exit(1)
        # Database-only mode - file inspection operations removed
        manager = UniverseStateManager(base_path=args.saved_dir)
        print("DEBUG: File-based operations removed - using database-only mode")
        # Find all available states in range
        available_timestamps = manager.list_available_states()
        # Filter by date range
        selected_timestamps = []
        for ts in available_timestamps:
            try:
                ts_date = datetime.strptime(ts[:8], "%Y%m%d")
                if start_date <= ts_date <= end_date:
                    selected_timestamps.append(ts)
            except Exception:
                continue
        if not selected_timestamps:
            print("No universe states found in the given date range.")
            sys.exit(1)
        selected_timestamps.sort()
        series = {field: [] for field in args.fields}
        dates = []
        for ts in selected_timestamps:
            try:
                df = manager.load_universe_state(timestamp=ts)
                row = df[df["instrument_id"] == int(instrument_id)]
                if row.empty:
                    for field in args.fields:
                        series[field].append(None)
                else:
                    for field in args.fields:
                        series[field].append(row.iloc[0].get(field, None))
                dates.append(datetime.strptime(ts[:8], "%Y%m%d"))
            except Exception as e:
                print(f"Failed to load/parse state {ts}: {e}")
                for field in args.fields:
                    series[field].append(None)
                dates.append(None)
        if args.mode == "print":
            for i, d in enumerate(dates):
                print(f"{d}: ", end="")
                for field in args.fields:
                    print(f"{field}={series[field][i]}", end=" ")
                print()
        elif args.mode == "graph":
            import os
            if os.environ.get("PYTEST_CURRENT_TEST"):
                import matplotlib
                matplotlib.use("Agg")
                for field in args.fields:
                    plt.plot(dates, series[field], label=field)
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.title(f"Instrument {instrument_id} State Over Time")
                plt.legend()
                plt.savefig("instrument_state_graph.png")
                print("Graph saved to instrument_state_graph.png (test mode)")
            else:
                for field in args.fields:
                    plt.plot(dates, series[field], label=field)
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.title(f"Instrument {instrument_id} State Over Time")
                plt.legend()
                plt.show()
        else:
            print(f"Unknown mode: {args.mode}")
            sys.exit(1)
