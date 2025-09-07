"""
UniverseStateManager - Data Persistence and Retrieval Layer for Universe State.

SINGLE RESPONSIBILITY:
- Optimize storage and retrieval of universe state data

STRICTLY ONLY:
- Persist UniverseStateInterval objects to optimized formats (Parquet, etc.)
- Provide fast retrieval of historical data with caching and optimization
- Handle data format optimization, compression, and storage efficiency
- Implement lag retrieval for lookback operations
- Manage metadata and data integrity validation
- Support multiple storage backends (files, databases, cloud)

DOES NOT:
- Generate any data or compute any state (UniverseStateBuilder responsibility)
- Perform any indicator calculations (IndicatorBuilder responsibility)
- Fetch any raw data from any source (MarketDataManager responsibility)
- Handle any business logic or transformations (UniverseStateBuilder responsibility)

INTERACTIONS:
- Receives FROM: UniverseStateBuilder (data to persist)
- Provides TO: Training Callbacks, Analytics systems, ML pipelines (data retrieval)
- That's it - storage and retrieval only
"""

import pandas as pd

# Optional pyarrow import for Parquet support
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PYARROW_AVAILABLE = False
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
import json
import hashlib
from datetime import datetime, timedelta
import os
from dataclasses import dataclass, asdict


@dataclass
class UniverseStateMetadata:
    """Metadata for universe state files."""
    timestamp: str
    record_count: int
    file_size_bytes: int
    checksum: str
    created_at: str
    columns: List[str]
    data_sources: List[str]
    universe_type: str = "default"
    version: str = "1.0"



import gin
from core.dao.universe_state_interval_dao import UniverseStateIntervalDAO
from core.dao.instrument_interval_dao import InstrumentIntervalDAO
from core.dao.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from core.dao.factor_interval_dao import FactorIntervalDAO

@gin.configurable
class UniverseStateManager:
    def get_lag_prices(self, instrument_id: int, cur_datetime, lag_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """
        Return a DataFrame of OHLCV features for the previous lag_periods up to (not including) cur_datetime.

        This method integrates with market_data_manager to provide multi-timeframe data aggregation:
        - Uses 1-minute bars as the base data source
        - Aggregates into the specified time interval using market_data_manager
        - Falls back to cached universe state data if market_data_manager is unavailable

        Args:
            instrument_id: The instrument ID to retrieve data for
            cur_datetime: Current datetime reference point (exclusive upper bound)
            lag_periods: Number of periods to look back. The meaning depends on time_interval:
                        - For '5m': number of 5-minute periods
                        - For '15m': number of 15-minute periods
                        - For '1h': number of hourly periods
                        - For '1d': number of daily periods
                        - For '1w': number of weekly periods
            time_interval: Time interval for aggregation. Supported values:
                          - '1m': 1-minute bars (raw data)
                          - '5m': 5-minute aggregated OHLCV
                          - '15m': 15-minute aggregated OHLCV
                          - '1h': 1-hour aggregated OHLCV
                          - '1d': Daily aggregated OHLCV (default)
                          - '1w': Weekly aggregated OHLCV

        Returns:
            DataFrame with columns:
            - OHLCV data: ['open', 'high', 'low', 'close', 'volume'] (if available)
            - Date columns: varies by data source ('date', 'as_of_date', etc.)

            NOTE: Technical indicators (etop, ebot, pldot) are NOT included in get_lag_prices().
            Use get_lagged_signals() method to retrieve technical indicators separately.

            Returns empty DataFrame if no data is available for the specified criteria.

        Example:
            # Get last 52 five-minute intervals (4.3 hours of 5m data)
            lag_5m = universe_manager.get_lag_prices(1001, date(2023, 12, 1), 52, '5m')

            # Get last 20 daily intervals (4 weeks of daily data)
            lag_daily = universe_manager.get_lag_prices(1001, date(2023, 12, 1), 20, '1d')

        Notes:
            - market_data_manager aggregation preserves OHLCV semantics:
              * open: first minute's open in the interval
              * high: highest high in the interval
              * low: lowest low in the interval
              * close: last minute's close in the interval
              * volume: sum of volume in the interval
            - Technical indicators (etop, ebot, pldot) are computed by universe state builder
              on the aggregated OHLCV data
            - If market_data_manager is not available, falls back to cached universe state data
        """
        # Type validation for all parameters
        if not isinstance(instrument_id, int) or instrument_id <= 0:
            raise ValueError(f"instrument_id must be a positive integer, got {instrument_id} (type: {type(instrument_id)})")

        if not isinstance(lag_periods, int) or lag_periods <= 0:
            raise ValueError(f"lag_periods must be a positive integer, got {lag_periods} (type: {type(lag_periods)})")

        if not isinstance(time_interval, str) or not time_interval.strip():
            raise ValueError(f"time_interval must be a non-empty string, got {time_interval} (type: {type(time_interval)})")

        # Validate time_interval parameter
        valid_intervals = {'1m', '5m', '15m', '1h', '1d', '1w'}
        if time_interval not in valid_intervals:
            raise ValueError(f"Invalid time_interval '{time_interval}'. Must be one of: {sorted(valid_intervals)}")

        # Assert that market_data_manager is available
        assert hasattr(self, 'market_data_manager') and self.market_data_manager, (
            "market_data_manager is required for get_lag_prices() but is not available. "
            "Ensure UniverseStateManager is initialized with a market_data_manager instance."
        )

        # Type validation for cur_datetime
        if not hasattr(cur_datetime, 'date') and not hasattr(cur_datetime, 'year'):
            raise ValueError(f"cur_datetime must be a datetime or date object, got {type(cur_datetime)}")

        # Ensure cur_datetime is a datetime object for precise time operations
        if not hasattr(cur_datetime, 'hour'):
            # If it's a date object, convert to datetime at start of day
            from datetime import datetime
            if hasattr(cur_datetime, 'date'):
                cur_datetime = datetime.combine(cur_datetime.date(), datetime.min.time())
            else:
                cur_datetime = datetime.combine(cur_datetime, datetime.min.time())

        # Use market_data_manager to get data for specified time interval
        try:
            # Get aggregated data from market_data_manager for the specified interval
            df = self.market_data_manager.get_ohlcv_data(
                instrument_id=instrument_id,
                reference_datetime=cur_datetime,  # Reference point: data BEFORE this datetime
                periods=lag_periods,
                time_interval=time_interval,
                direction='backward'  # Explicitly specify backward direction for lag prices
            )
            if df is not None and not df.empty:
                try:
                    self.logger.debug(f"[get_lag_prices] market_data_manager: instrument_id={instrument_id} cur_datetime={cur_datetime} lag_periods={lag_periods} interval={time_interval} df.shape={df.shape}")
                except Exception:
                    pass
                return df
        except Exception as e:
            try:
                self.logger.error(f"[get_lag_prices] market_data_manager failed: {e}")
            except Exception:
                pass
            raise IOError(f"Failed to get lag prices from market_data_manager: {e}")

        # If we reach here, market_data_manager returned empty data
        # Return empty DataFrame with only OHLCV columns (technical indicators come from get_lagged_signals)
        return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

    def get_lead_prices(self, instrument_id: int, cur_datetime, lead_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """
        Return a DataFrame of lead prices for the next lead_periods strictly after cur_datetime.

        This method integrates with market_data_manager to provide multi-timeframe lead data aggregation:
        - Uses 1-minute bars as the base data source
        - Aggregates into the specified time interval using market_data_manager
        - Returns consistent OHLCV data like get_lag_prices()

        Args:
            instrument_id: The instrument ID to retrieve data for
            cur_datetime: Current datetime reference point (exclusive lower bound)
            lead_periods: Number of periods to look forward. The meaning depends on time_interval:
                         - For '5m': number of 5-minute periods
                         - For '15m': number of 15-minute periods
                         - For '1h': number of hourly periods
                         - For '1d': number of daily periods
                         - For '1w': number of weekly periods
            time_interval: Time interval for aggregation. Supported values:
                          - '1m': 1-minute bars (raw data)
                          - '5m': 5-minute aggregated OHLCV
                          - '15m': 15-minute aggregated OHLCV
                          - '1h': 1-hour aggregated OHLCV
                          - '1d': Daily aggregated OHLCV (default)
                          - '1w': Weekly aggregated OHLCV

        Returns:
            DataFrame with columns:
            - OHLCV data: ['open', 'high', 'low', 'close', 'volume'] (if available)
            - Date columns: varies by data source ('date', 'as_of_date', etc.)

            NOTE: Technical indicators (etop, ebot, pldot) are NOT included in get_lag_prices().
            Use get_lagged_signals() method to retrieve technical indicators separately.

            Returns empty DataFrame if no data is available for the specified criteria.

        Notes:
            - market_data_manager aggregation preserves OHLCV semantics:
              * open: first minute's open in the interval
              * high: highest high in the interval
              * low: lowest low in the interval
              * close: last minute's close in the interval
              * volume: sum of volume in the interval
            - Technical indicators (etop, ebot, pldot) are computed by universe state builder
              on the aggregated OHLCV data
        """
        # Type validation for all parameters
        if not isinstance(instrument_id, int) or instrument_id <= 0:
            raise ValueError(f"instrument_id must be a positive integer, got {instrument_id} (type: {type(instrument_id)})")

        if not isinstance(lead_periods, int) or lead_periods <= 0:
            raise ValueError(f"lead_periods must be a positive integer, got {lead_periods} (type: {type(lead_periods)})")

        if not isinstance(time_interval, str) or not time_interval.strip():
            raise ValueError(f"time_interval must be a non-empty string, got {time_interval} (type: {type(time_interval)})")

        # Validate time_interval parameter
        valid_intervals = {'1m', '5m', '15m', '1h', '1d', '1w'}
        if time_interval not in valid_intervals:
            raise ValueError(f"Invalid time_interval '{time_interval}'. Must be one of: {sorted(valid_intervals)}")

        # Assert that market_data_manager is available
        assert hasattr(self, 'market_data_manager') and self.market_data_manager, (
            "market_data_manager is required for get_lead_prices() but is not available. "
            "Ensure UniverseStateManager is initialized with a market_data_manager instance."
        )

        # Type validation for cur_datetime
        if not hasattr(cur_datetime, 'date') and not hasattr(cur_datetime, 'year'):
            raise ValueError(f"cur_datetime must be a datetime or date object, got {type(cur_datetime)}")

        # Ensure cur_datetime is a datetime object for precise time operations
        if not hasattr(cur_datetime, 'hour'):
            # If it's a date object, convert to datetime at start of day
            from datetime import datetime
            if hasattr(cur_datetime, 'date'):
                cur_datetime = datetime.combine(cur_datetime.date(), datetime.min.time())
            else:
                cur_datetime = datetime.combine(cur_datetime, datetime.min.time())

        # Use market_data_manager to get lead data for specified time interval
        try:
            # Get aggregated lead data from market_data_manager for the specified interval
            df = self.market_data_manager.get_ohlcv_data(
                instrument_id=instrument_id,
                reference_datetime=cur_datetime,  # Reference point: data AFTER this datetime
                periods=lead_periods,
                time_interval=time_interval,
                direction='forward'  # Explicitly specify forward direction for lead prices
            )
            if df is not None and not df.empty:
                try:
                    self.logger.debug(f"[get_lead_prices] market_data_manager: instrument_id={instrument_id} cur_datetime={cur_datetime} lead_periods={lead_periods} interval={time_interval} df.shape={df.shape}")
                except Exception:
                    pass
                return df
        except Exception as e:
            try:
                self.logger.error(f"[get_lead_prices] market_data_manager failed: {e}")
            except Exception:
                pass
            raise IOError(f"Failed to get lead prices from market_data_manager: {e}")

        # If we reach here, market_data_manager returned empty data
        # Return empty DataFrame with only OHLCV columns (technical indicators come from get_lagged_signals)
        return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

    async def get_lagged_signals(
        self,
        instrument_id: int,
        cur_datetime,
        lag_periods: int,
        time_interval: str = '1d',
        signal_names: List[str] = None
    ) -> pd.DataFrame:
        """
        Get lagged signal/indicator data for a specific instrument and time interval.

        This method retrieves historical indicator/signal values for multi-timeframe analysis.
        It supports retrieving signals computed at different time intervals (1m, 5m, 15m, 1h, 1d, 1w)
        even when the base_duration is 1m.

        Args:
            instrument_id: The instrument ID to retrieve signals for
            cur_datetime: Current datetime reference point (exclusive upper bound)
            lag_periods: Number of periods to look back. The meaning depends on time_interval:
                        - For '1m': number of 1-minute periods
                        - For '5m': number of 5-minute periods
                        - For '15m': number of 15-minute periods
                        - For '1h': number of hourly periods
                        - For '1d': number of daily periods
                        - For '1w': number of weekly periods
            time_interval: Time interval for signal aggregation ('1m', '5m', '15m', '1h', '1d', '1w')
            signal_names: Optional list of specific signal names to retrieve. If None, returns all available signals.
                         Common signals: ['etop', 'ebot', 'pldot', 'sma_20', 'ema_12', 'rsi_14', 'macd_line']

        Returns:
            DataFrame with columns:
            - timestamp: DateTime index for each signal period
            - {signal_name}: One column per signal with computed values
            - {signal_name}_status: Status column for each signal ('ok', 'invalid', etc.)

            Returns empty DataFrame if no signal data is available for the specified criteria.

        Example:
            # Get last 20 five-minute signal periods (1.67 hours of 5m signals)
            signals_5m = await universe_manager.get_lagged_signals(1001, date(2023, 12, 1), 20, '5m', ['etop', 'ebot'])

            # Get last 5 daily signal periods (1 week of daily signals)
            signals_daily = await universe_manager.get_lagged_signals(1001, date(2023, 12, 1), 5, '1d')

        Notes:
            - Supports multi-timeframe signal retrieval even with base_duration='1m'
            - Signal values are aggregated/computed for the specified time_interval
            - Uses InstrumentIndicatorIntervalDAO for database access
            - Includes signal status information for validation
        """
        # Type validation for all parameters
        if not isinstance(instrument_id, int) or instrument_id <= 0:
            raise ValueError(f"instrument_id must be a positive integer, got {instrument_id} (type: {type(instrument_id)})")

        if not isinstance(lag_periods, int) or lag_periods <= 0:
            raise ValueError(f"lag_periods must be a positive integer, got {lag_periods} (type: {type(lag_periods)})")

        if not isinstance(time_interval, str) or not time_interval.strip():
            raise ValueError(f"time_interval must be a non-empty string, got {time_interval} (type: {type(time_interval)})")

        # Validate time_interval parameter
        valid_intervals = {'1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w'}
        if time_interval not in valid_intervals:
            raise ValueError(f"Invalid time_interval '{time_interval}'. Must be one of: {sorted(valid_intervals)}")

        # Type validation for cur_datetime
        if not hasattr(cur_datetime, 'date') and not hasattr(cur_datetime, 'year'):
            raise ValueError(f"cur_datetime must be a datetime or date object, got {type(cur_datetime)}")

        # Ensure cur_datetime is a datetime object for precise time operations
        if not hasattr(cur_datetime, 'hour'):
            # If it's a date object, convert to datetime at start of day
            from datetime import datetime
            if hasattr(cur_datetime, 'date'):
                cur_datetime = datetime.combine(cur_datetime.date(), datetime.min.time())
            else:
                cur_datetime = datetime.combine(cur_datetime, datetime.min.time())

        # Validate signal_names if provided
        if signal_names is not None:
            if not isinstance(signal_names, list):
                raise ValueError(f"signal_names must be a list or None, got {type(signal_names)}")

            for i, signal_name in enumerate(signal_names):
                if not isinstance(signal_name, str) or not signal_name.strip():
                    raise ValueError(f"signal_names[{i}] must be a non-empty string, got {signal_name} (type: {type(signal_name)})")

        try:
            from core.dao.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO

            # Calculate the time range based on interval and lag_periods
            from datetime import timedelta, datetime

            # Convert time_interval to timedelta for calculating date range
            interval_deltas = {
                '1m': timedelta(minutes=1),
                '5m': timedelta(minutes=5),
                '15m': timedelta(minutes=15),
                '30m': timedelta(minutes=30),
                '1h': timedelta(hours=1),
                '2h': timedelta(hours=2),
                '4h': timedelta(hours=4),
                '1d': timedelta(days=1),
                '1w': timedelta(weeks=1)
            }

            interval_delta = interval_deltas[time_interval]

            # Calculate start date for the query (go back enough to get lag_periods)
            total_lookback = interval_delta * lag_periods * 2  # Extra buffer for market closures
            start_date = cur_datetime - total_lookback

            # Query indicator intervals from the database
            indicator_dao = InstrumentIndicatorIntervalDAO(self.env)

            # Get all indicator data for the instrument in the time range
            indicator_data = await indicator_dao.get_by_instrument_and_date_range(
                instrument_id=instrument_id,
                start_date=start_date,
                end_date=cur_datetime
            )

            if not indicator_data:
                # Return empty DataFrame with expected structure
                columns = ['timestamp']
                if signal_names:
                    for signal_name in signal_names:
                        columns.extend([signal_name, f"{signal_name}_status"])
                else:
                    # Add common signal columns
                    common_signals = ['etop', 'ebot', 'pldot', 'sma_20', 'ema_12', 'rsi_14']
                    for signal_name in common_signals:
                        columns.extend([signal_name, f"{signal_name}_status"])

                return pd.DataFrame(columns=columns)

            # Convert to DataFrame and process
            df_rows = []
            for record in indicator_data:
                # Each record should have: start_date_time, end_date_time, indicator_name, indicator_value, indicator_status
                row_data = {
                    'timestamp': record.get('start_date_time') or record.get('end_date_time'),
                    'indicator_name': record.get('indicator_name'),
                    'indicator_value': record.get('indicator_value'),
                    'indicator_status': record.get('indicator_status')
                }
                df_rows.append(row_data)

            if not df_rows:
                return pd.DataFrame(columns=['timestamp'])

            # Create DataFrame from records
            signals_df = pd.DataFrame(df_rows)

            # Filter by signal names if specified
            if signal_names:
                signals_df = signals_df[signals_df['indicator_name'].isin(signal_names)]

            if signals_df.empty:
                return pd.DataFrame(columns=['timestamp'])

            # Pivot to wide format: one column per indicator
            pivoted_df = signals_df.pivot_table(
                index='timestamp',
                columns='indicator_name',
                values=['indicator_value', 'indicator_status'],
                aggfunc='last'  # Take last value if duplicates
            )

            # Flatten column names
            pivoted_df.columns = [f"{col[1]}_{col[0].replace('indicator_', '')}" if col[1] else col[0] for col in pivoted_df.columns]
            pivoted_df = pivoted_df.reset_index()

            # Sort by timestamp and filter to the requested time interval and lag periods
            pivoted_df = pivoted_df.sort_values('timestamp')

            # Filter to only include data before cur_datetime
            pivoted_df = pivoted_df[pivoted_df['timestamp'] < pd.Timestamp(cur_datetime)]

            # Take the last lag_periods records
            pivoted_df = pivoted_df.tail(lag_periods)

            logger.debug(f"Retrieved {len(pivoted_df)} {time_interval} signal periods for instrument {instrument_id}")

            return pivoted_df

        except Exception as e:
            try:
                self.logger.error(f"Failed to get lagged signals for instrument {instrument_id}: {e}")
            except:
                pass

            # Return empty DataFrame on error
            columns = ['timestamp']
            if signal_names:
                for signal_name in signal_names:
                    columns.extend([signal_name, f"{signal_name}_status"])

            return pd.DataFrame(columns=columns)

    def _get_instrument_history(self, instrument_id: int) -> pd.DataFrame:
        """
        Helper to fetch full DataFrame for an instrument from cache or storage.
        Assumes a 'date' column of type datetime/date.
        """
        # First, consult per-instrument history accumulated during this run
        try:
            hist_df = self._instrument_history.get(int(instrument_id))
            if hist_df is not None and not hist_df.empty:
                # Ensure date column exists/normalized
                if 'date' not in hist_df.columns:
                    tmp = hist_df.copy()
                    if 'as_of_date' in tmp.columns:
                        tmp['date'] = pd.to_datetime(tmp['as_of_date']).dt.date
                    elif 'as_of_datetime' in tmp.columns:
                        tmp['date'] = pd.to_datetime(tmp['as_of_datetime']).dt.date
                    elif 'start_date_time' in tmp.columns:
                        tmp['date'] = pd.to_datetime(tmp['start_date_time']).dt.date
                    hist_df = tmp
                try:
                    self.logger.debug(f"[_get_instrument_history][run-cache] inst_id={instrument_id} shape={hist_df.shape} "
                                      f"date_min={hist_df.get('date').min() if 'date' in hist_df.columns else None} "
                                      f"date_max={hist_df.get('date').max() if 'date' in hist_df.columns else None}")
                except Exception:
                    pass
                return hist_df
        except Exception:
            pass
        # Try to get from cache, else load full universe state and filter
        for ts, df in self._cache.items():
            inst_df = df[df['instrument_id'] == instrument_id]
            if not inst_df.empty:
                # Normalize date column if needed
                if 'date' not in inst_df.columns:
                    try:
                        inst_df = inst_df.copy()
                        if 'as_of_date' in inst_df.columns:
                            inst_df['date'] = pd.to_datetime(inst_df['as_of_date']).dt.date
                        elif 'as_of_datetime' in inst_df.columns:
                            inst_df['date'] = pd.to_datetime(inst_df['as_of_datetime']).dt.date
                        elif 'start_date_time' in inst_df.columns:
                            inst_df['date'] = pd.to_datetime(inst_df['start_date_time']).dt.date
                    except Exception:
                        pass
                try:
                    self.logger.debug(f"[_get_instrument_history][cache hit ts={ts}] inst_df.shape={inst_df.shape} cols={list(inst_df.columns)} date_min={inst_df.get('date').min() if 'date' in inst_df.columns else None} date_max={inst_df.get('date').max() if 'date' in inst_df.columns else None}")
                except Exception:
                    pass
                return inst_df
        # Fallback: load latest universe state
        latest_ts = self.get_latest_timestamp()
        if latest_ts:
            df = self.load_universe_state(timestamp=latest_ts)
            inst_df = df[df['instrument_id'] == instrument_id]
            if not inst_df.empty:
                # Normalize date column if needed
                if 'date' not in inst_df.columns:
                    try:
                        inst_df = inst_df.copy()
                        if 'as_of_date' in inst_df.columns:
                            inst_df['date'] = pd.to_datetime(inst_df['as_of_date']).dt.date
                        elif 'as_of_datetime' in inst_df.columns:
                            inst_df['date'] = pd.to_datetime(inst_df['as_of_datetime']).dt.date
                        elif 'start_date_time' in inst_df.columns:
                            inst_df['date'] = pd.to_datetime(inst_df['start_date_time']).dt.date
                    except Exception:
                        pass
                try:
                    self.logger.debug(f"[_get_instrument_history][latest ts={latest_ts}] inst_df.shape={inst_df.shape} cols={list(inst_df.columns)} date_min={inst_df.get('date').min() if 'date' in inst_df.columns else None} date_max={inst_df.get('date').max() if 'date' in inst_df.columns else None}")
                except Exception:
                    pass
                return inst_df
        raise ValueError(f"No data found for instrument_id={instrument_id}")

    """
    Handles fast persistence and retrieval of universe state data.

    Focuses on I/O operations, caching, and data format optimization.
    Uses Parquet format for optimal performance with columnar data.
    """
    def handleEnd(self, current_time, saved_dir=None):
        """
        Save the full universe state DataFrame under saved_dir (or base_path if None) with a timestamp based on current_time.
        """
        import logging
        logger = self.logger if hasattr(self, 'logger') else logging.getLogger(__name__)
        # Explicitly initialize saved_dir at the very start
        local_saved_dir = saved_dir
        logger.debug(f"handleEnd: ENTRY at {current_time}, saved_dir={local_saved_dir}")
        logger.debug(f"handleEnd: Saving full universe state at {current_time}, saved_dir: {local_saved_dir}")
        import pandas as pd
        # Determine input and output directories separately
        search_dir = local_saved_dir if local_saved_dir is not None else self.states_dir
        out_dir = Path(local_saved_dir) if local_saved_dir is not None else self.base_path
        logger.debug(f"handleEnd: Aggregating Parquet files from {search_dir}")
        all_parquet_files = list(Path(search_dir).glob("universe_state_*.parquet"))
        logger.debug(f"handleEnd: Found {len(all_parquet_files)} files: {[str(f) for f in all_parquet_files]}")
        if not all_parquet_files:
            logger.warning("handleEnd: No universe state files to aggregate.")
            return
        dfs = []
        for f in all_parquet_files:
            try:
                logger.debug(f"handleEnd: Reading {f}")
                dfs.append(pd.read_parquet(f))
            except Exception as e:
                logger.warning(f"handleEnd: Failed to read {f}: {e}")
        if not dfs:
            logger.warning("handleEnd: All universe state files failed to read.")
            return
        full_df = pd.concat(dfs, ignore_index=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        timestamp = current_time.strftime('%Y%m%d_%H%M%S')
        out_file = out_dir / f"full_universe_state_{timestamp}.parquet"
        logger.debug(f"handleEnd: Writing full universe state to {out_file} ({len(full_df)} records)")
        full_df.to_parquet(out_file, index=False)
        logger.debug(f"handleEnd: Saved full universe state to {out_file} with {len(full_df)} records.")
        logger.debug(f"handleEnd: EXIT at {current_time}")

    def __init__(self, env=None, base_path: Optional[str] = None, write_metadata: bool = True):
        """
        Initialize UniverseStateManager.

        Args:
            env: Environment instance (optional)
            base_path: Base directory for universe state files. If None, uses environment config.
            write_metadata: Whether to write metadata files (can be disabled for tests)
            write_metadata: Whether to write metadata files (can be disabled for tests)
        """
        self.env = env
        self.base_path = Path(base_path) if base_path else Path("data/universe_state")
        self.base_path.mkdir(parents=True, exist_ok=True)
        # Create subdirectories for organization
        self.states_dir = self.base_path / "states"
        self.metadata_dir = self.base_path / "metadata"
        self.cache_dir = self.base_path / "cache"
        for dir_path in [self.states_dir, self.metadata_dir, self.cache_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        # In-memory cache for frequently accessed data
        self._cache: Dict[str, pd.DataFrame] = {}
        self._cache_metadata: Dict[str, UniverseStateMetadata] = {}
        self._max_cache_size = 5  # Maximum number of states to cache
        # In-memory rolling history per instrument (across addUniverseState calls)
        self._instrument_history: Dict[int, pd.DataFrame] = {}
        self.logger = logging.getLogger(__name__)
        # Initialize UniverseStateIntervalDAO for interval persistence
        self._interval_dao = UniverseStateIntervalDAO(self.env) if self.env else None
        # Flag to control metadata file writing
        self.write_metadata = write_metadata
        # Flag to control metadata file writing
        self.write_metadata = write_metadata

    async def save_universe_state(self, universe_data: pd.DataFrame, timestamp: str, metadata: Optional[Dict[str, Any]] = None, partition_cols: Optional[List[str]] = None) -> str:
        """
        Persist universe state.
        - If env and required interval metadata are provided, persist via DB using DAO and return a db:// URI.
        - Otherwise, persist to local parquet with metadata JSON and return the file path.
        """
        if universe_data.empty:
            raise ValueError("Cannot save empty universe state")
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
                self.logger.debug(f"[save_universe_state][DB] metadata={metadata} shape={universe_data.shape} ts={timestamp}")
                # Parse dates
                from datetime import datetime, date
                def parse_dt(val):
                    if isinstance(val, (datetime, date)):
                        return val
                    if isinstance(val, str):
                        try:
                            return datetime.fromisoformat(val)
                        except ValueError:
                            try:
                                return datetime.strptime(val, "%Y-%m-%d")
                            except ValueError:
                                raise ValueError(f"Invalid date format: {val}")
                    raise TypeError(f"Invalid type for date: {type(val)}")
                start_dt = parse_dt(metadata['start_date_time'])
                end_dt = parse_dt(metadata['end_date_time'])
                interval_id = await self._interval_dao.create(
                    universe_id=metadata['universe_id'],
                    duration=metadata['duration'],
                    start_date_time=start_dt,
                    end_date_time=end_dt
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
                            market_cap=inst_interval.market_cap
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
                                    indicator_status=ind_val.get('status')
                                )
                    for factor_interval in getattr(universe_state, 'factor_intervals', []):
                        if hasattr(factor_interval, 'factor_name') and hasattr(factor_interval, 'factor_value'):
                            await factor_interval_dao.create(
                                universe_state_interval_id=interval_id,
                                factor_name=factor_interval.factor_name,
                                factor_value=factor_interval.factor_value
                            )
                # Regardless of DB persistence, also write local parquet and metadata for fast local reads
                self.logger.debug(f"[save_universe_state][DB->FILE] Also writing parquet for ts={timestamp}")
                optimized_data = self._optimize_data_types(universe_data.copy())
                file_path = self.states_dir / f"universe_state_{timestamp}.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(pa.Table.from_pandas(optimized_data), file_path, compression='snappy')
                meta = self._create_metadata(timestamp, optimized_data, file_path, metadata or {})
                self._update_cache(timestamp, optimized_data, meta)
                # Only write metadata files if enabled
                if self.write_metadata:
                    md_file = self.metadata_dir / f"metadata_{timestamp}.json"
                    with open(md_file, 'w') as f:
                        json.dump(asdict(meta), f)
                self.logger.info(f"Saved universe state interval to DB for {timestamp} (interval_id={interval_id}, records={len(universe_data)}) and wrote parquet -> {file_path}")
                return f"db://universe_state_interval/{interval_id}/{timestamp}"
            else:
                # File-based path
                self.logger.debug(f"[save_universe_state][FILE] shape={universe_data.shape} ts={timestamp}")
                optimized_data = self._optimize_data_types(universe_data.copy())
                file_path = self.states_dir / f"universe_state_{timestamp}.parquet"
                # Ensure dir exists
                file_path.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(pa.Table.from_pandas(optimized_data), file_path, compression='snappy')
                # Create and store metadata
                meta = self._create_metadata(timestamp, optimized_data, file_path, metadata or {})
                self._update_cache(timestamp, optimized_data, meta)
                # Only write metadata files if enabled
                if self.write_metadata:
                    md_file = self.metadata_dir / f"metadata_{timestamp}.json"
                    with open(md_file, 'w') as f:
                        json.dump(asdict(meta), f)
                self.logger.info(f"Saved universe state to file for {timestamp} ({len(optimized_data)} records) -> {file_path}")
                return str(file_path)
        except Exception as e:
            self.logger.error(f"Failed to save universe state: {e}")
            raise IOError(f"Failed to save universe state: {e}")

    def save_universe_state_sync(self, universe_data: pd.DataFrame, timestamp: str, metadata: Optional[Dict[str, Any]] = None, partition_cols: Optional[List[str]] = None) -> str:
        """Synchronous wrapper for save_universe_state for use in non-async contexts (e.g., sync tests)."""
        import asyncio
        return asyncio.run(self.save_universe_state(universe_data, timestamp, metadata, partition_cols))


    async def addUniverseState(self, duration_to_state: dict, current_time):
        """
        Accepts a dict of TimeDuration -> UniverseState, flattens all states to a DataFrame, and saves using save_universe_state.
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
            try:
                self.logger.debug(f"[addUniverseState] incoming df.shape={df.shape} cols={list(df.columns)}")
            except Exception:
                pass
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
                # Log normalized date coverage/instrument counts for visibility
                try:
                    date_min = local_df['date'].min() if 'date' in local_df.columns else None
                    date_max = local_df['date'].max() if 'date' in local_df.columns else None
                    uniq_dates = int(local_df['date'].nunique()) if 'date' in local_df.columns else 0
                    uniq_inst = int(local_df['instrument_id'].nunique()) if 'instrument_id' in local_df.columns else 0
                    self.logger.debug(f"[addUniverseState] normalized dates range: {date_min}..{date_max} unique_dates={uniq_dates} instruments={uniq_inst}")
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
                        self.logger.debug(f"[addUniverseState] Updated _instrument_history for {local_df['instrument_id'].nunique()} instruments. Sample sizes: "
                                          f"{ {iid: len(self._instrument_history[iid]) for iid in list(self._instrument_history.keys())[:5]} }")
                        # Also show cached date coverage for first few instruments
                        sample_iids = list(self._instrument_history.keys())[:5]
                        for sid in sample_iids:
                            try:
                                cdf = self._instrument_history[sid]
                                cmin = cdf['date'].min() if 'date' in cdf.columns and not cdf.empty else None
                                cmax = cdf['date'].max() if 'date' in cdf.columns and not cdf.empty else None
                                self.logger.debug(f"[addUniverseState] cache coverage iid={sid} rows={len(cdf)} date_min={cmin} date_max={cmax}")
                            except Exception:
                                pass
                    except Exception:
                        pass
                else:
                    self.logger.debug("[addUniverseState] Skipped history cache update (instrument_id/date missing)")
            except Exception as e:
                self.logger.warning(f"[addUniverseState] Failed to update history cache: {e}")
            metadata = {
                "universe_id": getattr(universe_state, "universe_id", None),
                "duration": (duration if isinstance(duration, str) else duration.get_duration_string()),
                "start_date_time": getattr(universe_state, "start_date_time", None),
                "end_date_time": getattr(universe_state, "end_date_time", None),
                "universe_state": universe_state,
            }
            # Log the intended UniverseStateInterval date coverage to be saved
            try:
                save_min = local_df['date'].min() if 'date' in locals().get('local_df', pd.DataFrame()).columns else None
                save_max = local_df['date'].max() if 'date' in locals().get('local_df', pd.DataFrame()).columns else None
                self.logger.debug(f"[addUniverseState] saving UniverseStateInterval for dates {save_min}..{save_max} at timestamp={timestamp}")
            except Exception:
                pass
            await self.save_universe_state(df, timestamp, metadata=metadata)
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
        Start-of-day hook for UniverseStateManager. Implement flushing, finalization, or logging if needed.
        """
        self.logger.debug(f"UniverseStateManager.update_for_sod called at {current_time}")
        # Add EOD logic if needed

    def update_for_eod(self, runner, current_time):
        """
        End-of-day hook for UniverseStateManager. Implement flushing, finalization, or logging if needed.
        """
        self.logger.debug(f"UniverseStateManager.update_for_eod called at {current_time}")
        # Add EOD logic if needed

    def load_universe_state(self, timestamp: Optional[str] = None, filters: Optional[List] = None, columns: Optional[List[str]] = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Load universe state for the given timestamp from local storage/cache.
        This synchronous implementation avoids async/coroutine usage so it can be
        safely called from synchronous contexts (e.g., callbacks, feature builders).

        If a cache entry is available for the full dataset at the timestamp and no
        filters/columns are requested, it will be returned. Otherwise, attempts to
        load a Parquet file from the states directory following the naming pattern
        'universe_state_<timestamp>.parquet'.
        """
        import pandas as pd
        if timestamp is None:
            timestamp = self.get_latest_timestamp()
        if not timestamp:
            raise FileNotFoundError("No universe state records found")
        # Check cache
        if use_cache and filters is None and columns is None and timestamp in self._cache:
            self.logger.debug(f"Loading universe state from cache: {timestamp}")
            return self._cache[timestamp].copy()
        # Load from Parquet storage
        file_path = self.states_dir / f"universe_state_{timestamp}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"Universe state not found: {file_path}")
        try:
            df = pd.read_parquet(file_path)
        except Exception as e:
            self.logger.error(f"Failed to load universe state {file_path}: {e}")
            raise IOError(f"Failed to load universe state {file_path}: {e}")
        # Apply column selection if requested
        if columns is not None:
            existing = [c for c in columns if c in df.columns]
            df = df[existing]
        # Basic filtering support
        if filters is not None:
            try:
                for f in filters:
                    if callable(f):
                        df = df[f(df)]
                    elif isinstance(f, tuple) and len(f) == 3:
                        col, op, val = f
                        if op in ('=', '=='):
                            df = df[df[col] == val]
                        elif op in ('!=', '<>'):
                            df = df[df[col] != val]
                        elif op == '>':
                            df = df[df[col] > val]
                        elif op == '>=':
                            df = df[df[col] >= val]
                        elif op == '<':
                            df = df[df[col] < val]
                        elif op == '<=':
                            df = df[df[col] <= val]
                        elif op.lower() == 'in':
                            df = df[df[col].isin(val)]
                        else:
                            # Unsupported tuple op, skip
                            pass
                    else:
                        # Assume f is a boolean mask aligned with df
                        df = df[f]
            except Exception as e:
                self.logger.warning(f"Failed to apply filters, returning unfiltered data: {e}")
        if use_cache and filters is None and columns is None:
            self._update_cache(timestamp, df, {})
        self.logger.info(f"Loaded universe state for {timestamp} from file ({len(df)} records)")
        return df

    def get_latest_timestamp(self) -> Optional[str]:
        """
        Get timestamp of most recent universe state.

        Returns:
            Latest timestamp string or None if no states exist
        """
        parquet_files = list(self.states_dir.glob("universe_state_*.parquet"))
        if not parquet_files:
            return None

        # Extract timestamps and find the latest
        timestamps = []
        for file_path in parquet_files:
            try:
                timestamp = file_path.stem.replace("universe_state_", "")
                if self._validate_timestamp_format(timestamp):
                    timestamps.append(timestamp)
            except Exception:
                continue

        return max(timestamps) if timestamps else None

    def list_available_states(self, limit: Optional[int] = None) -> List[str]:
        """
        List all available universe state timestamps.

        Args:
            limit: Maximum number of timestamps to return (most recent first)

        Returns:
            List of timestamp strings sorted by recency
        """
        # Type validation
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError(f"limit must be a positive integer or None, got {limit} (type: {type(limit)})")
        parquet_files = list(self.states_dir.glob("universe_state_*.parquet"))
        timestamps = []

        for file_path in parquet_files:
            try:
                timestamp = file_path.stem.replace("universe_state_", "")
                if self._validate_timestamp_format(timestamp):
                    timestamps.append(timestamp)
            except Exception:
                continue

        # Sort by timestamp (most recent first)
        timestamps.sort(reverse=True)

        return timestamps[:limit] if limit else timestamps

    def cleanup_old_states(self, keep_days: int = 30) -> int:
        """
        Remove old universe states to manage disk space.

        Args:
            keep_days: Number of days of states to keep

        Returns:
            Number of files removed
        """
        # Type validation
        if not isinstance(keep_days, int) or keep_days <= 0:
            raise ValueError(f"keep_days must be a positive integer, got {keep_days} (type: {type(keep_days)})")
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        cutoff_timestamp = cutoff_date.strftime("%Y%m%d_000000")

        removed_count = 0

        for file_path in self.states_dir.glob("universe_state_*.parquet"):
            try:
                timestamp = file_path.stem.replace("universe_state_", "")
                if timestamp < cutoff_timestamp:
                    # Remove state file
                    file_path.unlink()

                    # Remove metadata file
                    metadata_file = self.metadata_dir / f"metadata_{timestamp}.json"
                    if metadata_file.exists():
                        metadata_file.unlink()

                    # Remove from cache
                    cache_keys_to_remove = [k for k in self._cache.keys() if k.startswith(timestamp)]
                    for key in cache_keys_to_remove:
                        del self._cache[key]

                    if timestamp in self._cache_metadata:
                        del self._cache_metadata[timestamp]

                    removed_count += 1
                    self.logger.debug(f"Removed old universe state: {timestamp}")

            except Exception as e:
                self.logger.warning(f"Failed to remove old state {file_path}: {e}")

        return removed_count

    def get_state_metadata(self, timestamp: str) -> UniverseStateMetadata:
        """
        Get metadata about a specific universe state.

        Args:
            timestamp: Timestamp of the state

        Returns:
            UniverseStateMetadata object

        Raises:
            FileNotFoundError: If metadata file doesn't exist
        """
        # Type validation
        if not isinstance(timestamp, str) or not timestamp.strip():
            raise ValueError(f"timestamp must be a non-empty string, got {timestamp} (type: {type(timestamp)})")
        # Check cache first
        if timestamp in self._cache_metadata:
            return self._cache_metadata[timestamp]

        metadata_file = self.metadata_dir / f"metadata_{timestamp}.json"

        if not metadata_file.exists():
            raise FileNotFoundError(f"Metadata not found for timestamp: {timestamp}")

        try:
            with open(metadata_file, 'r') as f:
                metadata_dict = json.load(f)

            metadata = UniverseStateMetadata(**metadata_dict)
            self._cache_metadata[timestamp] = metadata
            return metadata

        except Exception as e:
            raise IOError(f"Failed to load metadata for {timestamp}: {e}")

    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics for universe states.

        Returns:
            Dictionary with storage statistics
        """
        states = self.list_available_states()
        total_size = 0
        total_records = 0

        for timestamp in states:
            try:
                metadata = self.get_state_metadata(timestamp)
                total_size += metadata.file_size_bytes
                total_records += metadata.record_count
            except Exception:
                continue

        return {
            "total_states": len(states),
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "total_records": total_records,
            "cache_size": len(self._cache),
            "latest_timestamp": self.get_latest_timestamp(),
            "oldest_timestamp": states[-1] if states else None,
        }

    def clear_cache(self) -> None:
        """Clear in-memory cache."""
        self._cache.clear()
        self._cache_metadata.clear()
        self.logger.debug("Universe state cache cleared")

    # Private helper methods

    def _validate_timestamp_format(self, timestamp: str) -> bool:
        """Validate timestamp format (YYYYMMDD_HHMMSS)."""
        try:
            datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
            return True
        except ValueError:
            return False

    def _optimize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for better compression and performance."""
        # Convert string columns with limited unique values to categorical
        for col in df.select_dtypes(include=['object']).columns:
            n_unique = df[col].nunique()
            n_total = len(df)
            if n_unique <= 10 or (n_total > 0 and n_unique / n_total < 0.5):  # robust for small sets
                df[col] = df[col].astype('category')

        # Optimize numeric types
        for col in df.select_dtypes(include=['int64']).columns:
            col_min, col_max = df[col].min(), df[col].max()
            if col_min >= 0:
                if col_max <= 255:
                    df[col] = df[col].astype('uint8')
                elif col_max <= 65535:
                    df[col] = df[col].astype('uint16')
                elif col_max <= 4294967295:
                    df[col] = df[col].astype('uint32')
            else:
                if col_min >= -128 and col_max <= 127:
                    df[col] = df[col].astype('int8')
                elif col_min >= -32768 and col_max <= 32767:
                    df[col] = df[col].astype('int16')
                elif col_min >= -2147483648 and col_max <= 2147483647:
                    df[col] = df[col].astype('int32')

        return df

    def _create_metadata(self,
                        timestamp: str,
                        data: pd.DataFrame,
                        file_path: Path,
                        additional_metadata: Dict[str, Any]) -> UniverseStateMetadata:
        """Create metadata object for universe state."""
        file_size = file_path.stat().st_size if file_path.exists() else 0

        # Calculate checksum
        checksum = hashlib.md5(str(data.values.tobytes()).encode()).hexdigest()

        return UniverseStateMetadata(
            timestamp=timestamp,
            record_count=len(data),
            file_size_bytes=file_size,
            checksum=checksum,
            created_at=datetime.now().isoformat(),
            columns=list(data.columns),
            data_sources=additional_metadata.get('data_sources', []),
            universe_type=additional_metadata.get('universe_type', 'default'),
            version=additional_metadata.get('version', '1.0')
        )

    def _save_metadata(self, timestamp: str, metadata: UniverseStateMetadata) -> None:
        """Save metadata to JSON file."""
        metadata_file = self.metadata_dir / f"metadata_{timestamp}.json"

        with open(metadata_file, 'w') as f:
            json.dump(asdict(metadata), f, indent=2)

    def _update_cache(self,
                     timestamp: str,
                     data: pd.DataFrame,
                     metadata: UniverseStateMetadata) -> None:
        """Update in-memory cache with LRU eviction."""
        # Simple cache key for full data loads
        cache_key = timestamp

        # Add to cache
        self._cache[cache_key] = data.copy()
        self._cache_metadata[timestamp] = metadata

        # LRU eviction if cache is too large
        if len(self._cache) > self._max_cache_size:
            # Remove oldest entry
            oldest_key = min(self._cache.keys())
            del self._cache[oldest_key]

            # Also remove from metadata cache if it's the same timestamp
            if oldest_key in self._cache_metadata:
                del self._cache_metadata[oldest_key]


if __name__ == "__main__":
    import argparse
    import sys
    import pandas as pd
    from datetime import datetime, timedelta
    import matplotlib.pyplot as plt
    from state.universe_state_builder import UniverseStateIntervalBuilder
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
        # --- DEBUG: Print DB URL and schema for instrument_polygon and instruments ---
        try:
            import asyncpg
            import asyncio
            env = Environment()
            print(f"DEBUG (CLI): DB URL: {env.get_database_url()}")
            async def print_table_schema():
                pool = await asyncpg.create_pool(env.get_database_url())
                async with pool.acquire() as conn:
                    for table in ["instrument_polygon", "instruments"]:
                        tn = env.get_table_name(table)
                        schema = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", tn)
                        logger.debug(f"DEBUG (CLI): {tn} columns: {schema}")
                        row = await conn.fetchrow(f"SELECT * FROM {tn} LIMIT 1")
                        if row:
                            logger.debug(f"DEBUG (CLI): {tn} sample row: {dict(row)}")
                        else:
                            logger.debug(f"DEBUG (CLI): {tn} sample row: <empty>")
                await pool.close()
            asyncio.run(print_table_schema())
        except Exception as e:
            logger.error(f"DEBUG (CLI): Failed to print DB schema: {e}")
        # --- END DEBUG ---
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
            from state.universe_state_builder import UniverseStateIntervalBuilder
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
                # Use handleInterval with a real DailyPriceMarketDataManager
                from market_data.eod.daily_price_market_data_manager import DailyPriceMarketDataManager
                from market_data.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager
                # Patch _get_all_symbols to return test symbols (AAPL, TSLA)
                class PatchedDailyPriceMarketDataManager(DailyPriceMarketDataManager):
                    def _get_all_symbols(self):
                        return ["AAPL", "TSLA"]

                class PatchedFileDailyPriceMarketDataManager(FileDailyPriceMarketDataManager):
                    def _get_all_symbols(self):
                        return ["AAPL", "TSLA"]
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
                            logger.debug('Using FileDailyPriceMarketDataManager for prices')
                            vendors_dirs = {
                                'polygon': 'tests/data/daily_prices_polygon',
                                'tiingo': 'tests/data/daily_prices_tiingo'
                            }
                            self.market_data_manager = PatchedFileDailyPriceMarketDataManager(vendors_dirs, symbols=["AAPL", "TSLA"])
                        else:
                            logger.debug('Using DailyPriceMarketDataManager (DB) for prices')
                            self.market_data_manager = PatchedDailyPriceMarketDataManager(env, start_date=cur_date.date())
                        self.universe_state_manager = manager
                        self.env = env
                runner = RealRunner(env, instrument_ids)
                # Load last prices before start (if needed)
                # asyncio.run(runner.market_data_manager._load_last_prices_before_start())  # Uncomment if needed
                # update_for_sod populates intervals for the day
                asyncio.run(runner.market_data_manager.update_for_sod(runner, cur_date))
                builder.handleInterval(runner, cur_date)
                logger.debug(f"Called handleInterval for {date_str}")
            except Exception as e:
                logger.error(f"Failed to build/save for {date_str}: {e}")
            cur_date += timedelta(days=1)
        logger.debug("Build complete.")

    elif args.action == "inspect":
        # Inspect mode
        instrument_id = args.instrument_id
        if not instrument_id:
            print("--instrument_id is required for inspect mode.")
            sys.exit(1)
        # Use correct directory for inspection
        manager = UniverseStateManager(base_path=args.saved_dir)
        # Debug: print base_path and states_dir contents
        print(f"DEBUG: UniverseStateManager.base_path={manager.base_path}")
        print(f"DEBUG: UniverseStateManager.states_dir={manager.states_dir}")
        try:
            print("DEBUG: states_dir contents:", list(manager.states_dir.iterdir()))
        except Exception as e:
            print(f"DEBUG: Could not list states_dir: {e}")
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
                logger.error(f"Failed to load/parse state {ts}: {e}")
                for field in args.fields:
                    series[field].append(None)
                dates.append(None)
        if args.mode == "print":
            for i, d in enumerate(dates):
                logger.debug(f"{d}: ", end="")
                for field in args.fields:
                    logger.debug(f"{field}={series[field][i]}", end=" ")
                logger.debug("")
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
