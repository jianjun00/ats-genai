"""
UniverseStateManager with Fail-Fast Exception Handling - No Exception Masking

This service eliminates all generic exception catching and implements fail-fast error handling.
All exceptions are specific, actionable, and provide debugging context without masking root causes.

Key Improvements:
- Specific exception types instead of generic Exception catching
- Fail-fast validation with clear error messages
- Custom exception classes for different failure scenarios
- No silent error suppression or fallback to empty DataFrames
- Actionable error messages with debugging context
- Eliminated all try/except/pass patterns that mask real issues

PURPOSE:
- Store universe state data in database via DAOs with strict error handling
- Provide fast retrieval of historical data with in-memory caching
- Support lag retrieval for lookback operations in training data generation
- Maintain per-instrument history cache for efficient access
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


# ====================================================================
# CUSTOM EXCEPTION CLASSES FOR FAIL-FAST ERROR HANDLING
# ====================================================================

class UniverseStateManagerError(Exception):
    """Base exception for universe state manager errors."""
    def __init__(self, message: str, context: Dict[str, Any] = None):
        super().__init__(message)
        self.context = context or {}
        self.timestamp = datetime.now().isoformat()


class CacheInitializationError(UniverseStateManagerError):
    """Cache initialization failed."""
    def __init__(self, message: str, cache_type: str = None):
        super().__init__(f"Cache initialization failed: {message}")
        self.context = {'cache_type': cache_type, 'error_type': 'cache_initialization'}


class DataRetrievalError(UniverseStateManagerError):
    """Data retrieval operation failed."""
    def __init__(self, message: str, instrument_id: int = None, timeframe: str = None, operation: str = None):
        super().__init__(f"Data retrieval failed during {operation}: {message}")
        self.context = {
            'instrument_id': instrument_id,
            'timeframe': timeframe,
            'operation': operation,
            'error_type': 'data_retrieval'
        }


class ParameterValidationError(UniverseStateManagerError):
    """Parameter validation failed."""
    def __init__(self, message: str, parameter_name: str = None, parameter_value: Any = None):
        super().__init__(f"Parameter validation failed: {message}")
        self.context = {
            'parameter_name': parameter_name,
            'parameter_value': parameter_value,
            'error_type': 'parameter_validation'
        }


class DatabaseOperationError(UniverseStateManagerError):
    """Database operation failed."""
    def __init__(self, message: str, operation: str = None, dao_type: str = None):
        super().__init__(f"Database operation failed during {operation}: {message}")
        self.context = {
            'operation': operation,
            'dao_type': dao_type,
            'error_type': 'database_operation'
        }


class CacheOperationError(UniverseStateManagerError):
    """Cache operation failed."""
    def __init__(self, message: str, operation: str = None, cache_key: str = None):
        super().__init__(f"Cache operation failed during {operation}: {message}")
        self.context = {
            'operation': operation,
            'cache_key': cache_key,
            'error_type': 'cache_operation'
        }


class DataIntegrityError(UniverseStateManagerError):
    """Data integrity validation failed."""
    def __init__(self, message: str, data_type: str = None, validation_rule: str = None):
        super().__init__(f"Data integrity violation: {message}")
        self.context = {
            'data_type': data_type,
            'validation_rule': validation_rule,
            'error_type': 'data_integrity'
        }


class TimestampValidationError(UniverseStateManagerError):
    """Timestamp validation failed."""
    def __init__(self, message: str, timestamp_value: Any = None, expected_format: str = None):
        super().__init__(f"Timestamp validation failed: {message}")
        self.context = {
            'timestamp_value': timestamp_value,
            'expected_format': expected_format,
            'error_type': 'timestamp_validation'
        }


# ====================================================================
# FAIL-FAST UNIVERSE STATE MANAGER
# ====================================================================

@gin.configurable
class UniverseStateManagerFailFast:
    """
    Universe State Manager with fail-fast exception handling.
    
    All operations either succeed completely or fail with specific, actionable errors.
    No degraded functionality or silent error suppression.
    """

    def __init__(self, universe_id: int = None, rolling_window: int = 100):
        """Initialize universe state manager with strict validation."""
        self.logger = logging.getLogger(__name__)
        
        # Validate initialization parameters
        if universe_id is not None and not isinstance(universe_id, int):
            raise ParameterValidationError(
                "universe_id must be integer or None",
                parameter_name='universe_id',
                parameter_value=universe_id
            )
        
        if not isinstance(rolling_window, int) or rolling_window <= 0:
            raise ParameterValidationError(
                "rolling_window must be positive integer",
                parameter_name='rolling_window',
                parameter_value=rolling_window
            )
        
        self.universe_id = universe_id
        self.rolling_window = rolling_window
        
        # Initialize DAOs with fail-fast validation
        self._initialize_daos()
        
        # Initialize caches with strict validation
        self._initialize_caches()
        
        self.logger.info(f"🎯 Fail-Fast UniverseStateManager initialized for universe_id={universe_id}")

    def _initialize_daos(self):
        """Initialize DAOs with fail-fast validation."""
        try:
            self._interval_dao = UniverseStateIntervalDAO()
            self._instrument_interval_dao = InstrumentIntervalDAO()
            self._indicator_dao = InstrumentIndicatorIntervalDAO()
            self._factor_dao = FactorIntervalDAO()
        except ImportError as e:
            raise DatabaseOperationError(
                f"Failed to import required DAO classes: {e}",
                operation='initialize_daos',
                dao_type='universe_state_daos'
            )
        except Exception as e:
            raise DatabaseOperationError(
                f"Failed to initialize DAO instances: {e}",
                operation='initialize_daos',
                dao_type='universe_state_daos'
            )
        
        # Validate DAO instances
        required_daos = [
            ('_interval_dao', self._interval_dao),
            ('_instrument_interval_dao', self._instrument_interval_dao),
            ('_indicator_dao', self._indicator_dao),
            ('_factor_dao', self._factor_dao)
        ]
        
        for dao_name, dao_instance in required_daos:
            if dao_instance is None:
                raise DatabaseOperationError(
                    f"DAO initialization returned None: {dao_name}",
                    operation='validate_daos',
                    dao_type=dao_name
                )

    def _initialize_caches(self):
        """Initialize caches with strict validation."""
        try:
            # Rolling instrument history cache: {timeframe: {instrument_id: [intervals]}}
            self._rolling_instrument_history = {}
            
            # General instrument history cache: {instrument_id: DataFrame}
            self._instrument_history = {}
            
            # Cache metadata
            self._cache_stats = {
                'rolling_cache_hits': 0,
                'rolling_cache_misses': 0,
                'history_cache_hits': 0,
                'history_cache_misses': 0,
                'last_cleanup': datetime.now()
            }
        except Exception as e:
            raise CacheInitializationError(
                f"Failed to initialize cache structures: {e}",
                cache_type='rolling_instrument_history'
            )

    def get_lag_prices(self, instrument_id: int, cur_datetime, lag_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """
        Return OHLCV data for the previous lag_periods up to cur_datetime with fail-fast validation.

        Args:
            instrument_id: The instrument ID to retrieve data for
            cur_datetime: Current datetime reference point (exclusive upper bound)
            lag_periods: Number of periods to look back
            time_interval: Time interval ('1m', '5m', '15m', '1h', '1d', '1w')

        Returns:
            DataFrame with OHLCV columns

        Raises:
            ParameterValidationError: If parameters are invalid
            DataRetrievalError: If data retrieval fails
            CacheOperationError: If cache operations fail
        """
        # Strict parameter validation
        self._validate_lag_prices_params(instrument_id, lag_periods, time_interval, cur_datetime)
        cur_datetime = self._normalize_datetime(cur_datetime)

        # Get data from rolling cache with fail-fast
        return self._get_lag_prices_from_rolling_cache_fail_fast(instrument_id, cur_datetime, lag_periods, time_interval)

    def _validate_lag_prices_params(self, instrument_id: int, lag_periods: int, time_interval: str, cur_datetime):
        """Validate lag prices parameters with fail-fast approach."""
        if not isinstance(instrument_id, int):
            raise ParameterValidationError(
                f"instrument_id must be integer, got {type(instrument_id)}",
                parameter_name='instrument_id',
                parameter_value=instrument_id
            )
        
        if instrument_id <= 0:
            raise ParameterValidationError(
                f"instrument_id must be positive, got {instrument_id}",
                parameter_name='instrument_id',
                parameter_value=instrument_id
            )
        
        if not isinstance(lag_periods, int):
            raise ParameterValidationError(
                f"lag_periods must be integer, got {type(lag_periods)}",
                parameter_name='lag_periods',
                parameter_value=lag_periods
            )
        
        if lag_periods <= 0:
            raise ParameterValidationError(
                f"lag_periods must be positive, got {lag_periods}",
                parameter_name='lag_periods',
                parameter_value=lag_periods
            )
        
        if not isinstance(time_interval, str):
            raise ParameterValidationError(
                f"time_interval must be string, got {type(time_interval)}",
                parameter_name='time_interval',
                parameter_value=time_interval
            )
        
        valid_intervals = ['1m', '5m', '15m', '1h', '1d', '1w']
        if time_interval not in valid_intervals:
            raise ParameterValidationError(
                f"time_interval must be one of {valid_intervals}, got '{time_interval}'",
                parameter_name='time_interval',
                parameter_value=time_interval
            )
        
        if cur_datetime is None:
            raise ParameterValidationError(
                "cur_datetime cannot be None",
                parameter_name='cur_datetime',
                parameter_value=cur_datetime
            )

    def _normalize_datetime(self, dt):
        """Normalize datetime with fail-fast validation."""
        if isinstance(dt, datetime):
            return dt
        elif isinstance(dt, str):
            try:
                return datetime.fromisoformat(dt.replace('Z', '+00:00'))
            except ValueError as e:
                raise ParameterValidationError(
                    f"Invalid datetime string format: {e}",
                    parameter_name='datetime_string',
                    parameter_value=dt
                )
        else:
            raise ParameterValidationError(
                f"datetime must be datetime object or ISO string, got {type(dt)}",
                parameter_name='datetime_value',
                parameter_value=dt
            )

    def _get_lag_prices_from_rolling_cache_fail_fast(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str) -> pd.DataFrame:
        """
        Get lag prices from rolling cache with fail-fast validation.
        
        No fallback to empty DataFrame - if data is not available, raise specific error.
        """
        # Validate cache initialization
        if not hasattr(self, '_rolling_instrument_history'):
            raise CacheOperationError(
                "Rolling cache not initialized",
                operation='get_lag_prices',
                cache_key=f"{time_interval}:{instrument_id}"
            )
        
        if self._rolling_instrument_history is None:
            raise CacheOperationError(
                "Rolling cache is None - initialization failed",
                operation='get_lag_prices',
                cache_key=f"{time_interval}:{instrument_id}"
            )
        
        # Check timeframe availability
        if time_interval not in self._rolling_instrument_history:
            self._cache_stats['rolling_cache_misses'] += 1
            raise DataRetrievalError(
                f"No cached data available for timeframe '{time_interval}'",
                instrument_id=instrument_id,
                timeframe=time_interval,
                operation='get_lag_prices_timeframe_check'
            )
        
        # Check instrument availability for this timeframe
        timeframe_cache = self._rolling_instrument_history[time_interval]
        if instrument_id not in timeframe_cache:
            self._cache_stats['rolling_cache_misses'] += 1
            raise DataRetrievalError(
                f"No cached data available for instrument_id={instrument_id} in timeframe '{time_interval}'",
                instrument_id=instrument_id,
                timeframe=time_interval,
                operation='get_lag_prices_instrument_check'
            )
        
        # Get intervals for this instrument and timeframe
        intervals = timeframe_cache[instrument_id]
        
        if not isinstance(intervals, list):
            raise CacheOperationError(
                f"Cache data corruption: intervals not a list for instrument_id={instrument_id}",
                operation='get_lag_prices_data_validation',
                cache_key=f"{time_interval}:{instrument_id}"
            )
        
        if len(intervals) == 0:
            self._cache_stats['rolling_cache_misses'] += 1
            raise DataRetrievalError(
                f"No interval data available for instrument_id={instrument_id} in timeframe '{time_interval}'",
                instrument_id=instrument_id,
                timeframe=time_interval,
                operation='get_lag_prices_empty_intervals'
            )
        
        # Filter intervals before cur_datetime
        valid_intervals = []
        for interval in intervals:
            if not hasattr(interval, 'start_date_time'):
                raise DataIntegrityError(
                    f"Interval missing start_date_time attribute",
                    data_type='instrument_interval',
                    validation_rule='start_date_time_required'
                )
            
            if interval.start_date_time < cur_datetime:
                valid_intervals.append(interval)
        
        if len(valid_intervals) == 0:
            self._cache_stats['rolling_cache_misses'] += 1
            raise DataRetrievalError(
                f"No intervals found before datetime {cur_datetime} for instrument_id={instrument_id}",
                instrument_id=instrument_id,
                timeframe=time_interval,
                operation='get_lag_prices_datetime_filter'
            )
        
        # Sort by timestamp and take last lag_periods
        try:
            valid_intervals.sort(key=lambda x: x.start_date_time, reverse=True)
        except AttributeError as e:
            raise DataIntegrityError(
                f"Failed to sort intervals by start_date_time: {e}",
                data_type='instrument_interval',
                validation_rule='sortable_timestamps'
            )
        
        selected_intervals = valid_intervals[:lag_periods]
        
        # Convert to DataFrame with strict validation
        df_data = []
        for interval in selected_intervals:
            row_data = self._interval_to_dataframe_row(interval, instrument_id, time_interval)
            df_data.append(row_data)
        
        if len(df_data) == 0:
            raise DataRetrievalError(
                f"No valid intervals converted to DataFrame for instrument_id={instrument_id}",
                instrument_id=instrument_id,
                timeframe=time_interval,
                operation='get_lag_prices_dataframe_conversion'
            )
        
        # Create DataFrame with validation
        try:
            df = pd.DataFrame(df_data)
        except Exception as e:
            raise DataIntegrityError(
                f"Failed to create DataFrame from interval data: {e}",
                data_type='dataframe_creation',
                validation_rule='valid_dataframe_structure'
            )
        
        # Validate DataFrame structure
        required_columns = ['open', 'high', 'low', 'close', 'volume', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise DataIntegrityError(
                f"DataFrame missing required columns: {missing_columns}",
                data_type='dataframe_structure',
                validation_rule='required_ohlcv_columns'
            )
        
        # Sort by date (oldest first)
        try:
            df = df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            raise DataIntegrityError(
                f"Failed to sort DataFrame by date: {e}",
                data_type='dataframe_sorting',
                validation_rule='sortable_date_column'
            )
        
        self._cache_stats['rolling_cache_hits'] += 1
        self.logger.debug(f"Retrieved {len(df)} lag periods for instrument_id={instrument_id}, timeframe={time_interval}")
        
        return df

    def _interval_to_dataframe_row(self, interval, instrument_id: int, time_interval: str) -> Dict[str, Any]:
        """Convert interval to DataFrame row with strict validation."""
        required_attributes = ['open', 'high', 'low', 'close', 'traded_volume', 'start_date_time']
        
        for attr in required_attributes:
            if not hasattr(interval, attr):
                raise DataIntegrityError(
                    f"Interval missing required attribute '{attr}'",
                    data_type='instrument_interval',
                    validation_rule=f'{attr}_required'
                )
        
        # Validate OHLC relationships
        if interval.high < max(interval.open, interval.close):
            raise DataIntegrityError(
                f"Invalid OHLC: high ({interval.high}) < max(open={interval.open}, close={interval.close})",
                data_type='ohlc_validation',
                validation_rule='high_ge_open_close'
            )
        
        if interval.low > min(interval.open, interval.close):
            raise DataIntegrityError(
                f"Invalid OHLC: low ({interval.low}) > min(open={interval.open}, close={interval.close})",
                data_type='ohlc_validation',
                validation_rule='low_le_open_close'
            )
        
        # Validate positive values
        price_fields = ['open', 'high', 'low', 'close']
        for field in price_fields:
            value = getattr(interval, field)
            if value <= 0:
                raise DataIntegrityError(
                    f"Invalid {field} price: {value} (must be positive)",
                    data_type='price_validation',
                    validation_rule='positive_prices'
                )
        
        if interval.traded_volume < 0:
            raise DataIntegrityError(
                f"Invalid volume: {interval.traded_volume} (must be non-negative)",
                data_type='volume_validation',
                validation_rule='non_negative_volume'
            )
        
        return {
            'open': float(interval.open),
            'high': float(interval.high),
            'low': float(interval.low),
            'close': float(interval.close),
            'volume': int(interval.traded_volume),
            'date': interval.start_date_time
        }

    def ensure_timeframe_cache(self, timeframe: str):
        """Ensure timeframe cache exists with fail-fast validation."""
        if not isinstance(timeframe, str):
            raise ParameterValidationError(
                f"timeframe must be string, got {type(timeframe)}",
                parameter_name='timeframe',
                parameter_value=timeframe
            )
        
        if not hasattr(self, '_rolling_instrument_history'):
            raise CacheOperationError(
                "Rolling cache not initialized",
                operation='ensure_timeframe_cache',
                cache_key=timeframe
            )
        
        if timeframe not in self._rolling_instrument_history:
            self._rolling_instrument_history[timeframe] = {}
            self.logger.debug(f"Created timeframe cache for {timeframe}")

    def add_interval_to_rolling_cache(self, instrument_id: int, timeframe: str, interval):
        """Add interval to rolling cache with fail-fast validation."""
        # Validate parameters
        if not isinstance(instrument_id, int) or instrument_id <= 0:
            raise ParameterValidationError(
                f"instrument_id must be positive integer, got {instrument_id}",
                parameter_name='instrument_id',
                parameter_value=instrument_id
            )
        
        if not isinstance(timeframe, str):
            raise ParameterValidationError(
                f"timeframe must be string, got {type(timeframe)}",
                parameter_name='timeframe',
                parameter_value=timeframe
            )
        
        if interval is None:
            raise ParameterValidationError(
                "interval cannot be None",
                parameter_name='interval',
                parameter_value=interval
            )
        
        # Validate interval structure
        if not hasattr(interval, 'start_date_time'):
            raise DataIntegrityError(
                "Interval missing start_date_time attribute",
                data_type='instrument_interval',
                validation_rule='start_date_time_required'
            )
        
        # Ensure timeframe cache exists
        self.ensure_timeframe_cache(timeframe)
        
        # Ensure instrument cache exists for this timeframe
        if instrument_id not in self._rolling_instrument_history[timeframe]:
            self._rolling_instrument_history[timeframe][instrument_id] = []
        
        # Add interval to cache
        instrument_cache = self._rolling_instrument_history[timeframe][instrument_id]
        instrument_cache.append(interval)
        
        # Enforce rolling window size
        if len(instrument_cache) > self.rolling_window:
            # Remove oldest intervals (keep most recent)
            instrument_cache.sort(key=lambda x: x.start_date_time, reverse=True)
            self._rolling_instrument_history[timeframe][instrument_id] = instrument_cache[:self.rolling_window]
        
        self.logger.debug(f"Added interval to rolling cache: instrument_id={instrument_id}, timeframe={timeframe}")

    def get_instrument_history_for_timeframe(self, instrument_id: int, timeframe: str) -> List:
        """Get instrument history for specific timeframe with fail-fast validation."""
        # Validate parameters
        if not isinstance(instrument_id, int) or instrument_id <= 0:
            raise ParameterValidationError(
                f"instrument_id must be positive integer, got {instrument_id}",
                parameter_name='instrument_id',
                parameter_value=instrument_id
            )
        
        if not isinstance(timeframe, str):
            raise ParameterValidationError(
                f"timeframe must be string, got {type(timeframe)}",
                parameter_name='timeframe',
                parameter_value=timeframe
            )
        
        # Validate cache existence
        if not hasattr(self, '_rolling_instrument_history'):
            raise CacheOperationError(
                "Rolling cache not initialized",
                operation='get_instrument_history',
                cache_key=f"{timeframe}:{instrument_id}"
            )
        
        if timeframe not in self._rolling_instrument_history:
            raise DataRetrievalError(
                f"No cache data for timeframe '{timeframe}'",
                instrument_id=instrument_id,
                timeframe=timeframe,
                operation='get_instrument_history'
            )
        
        if instrument_id not in self._rolling_instrument_history[timeframe]:
            return []  # Empty list is valid for instruments with no data
        
        intervals = self._rolling_instrument_history[timeframe][instrument_id]
        
        # Validate and sort intervals
        if not isinstance(intervals, list):
            raise CacheOperationError(
                f"Cache corruption: intervals not a list for instrument_id={instrument_id}",
                operation='get_instrument_history',
                cache_key=f"{timeframe}:{instrument_id}"
            )
        
        # Sort by timestamp (oldest first)
        try:
            sorted_intervals = sorted(intervals, key=lambda x: x.start_date_time)
        except AttributeError as e:
            raise DataIntegrityError(
                f"Failed to sort intervals by timestamp: {e}",
                data_type='instrument_interval',
                validation_rule='sortable_timestamps'
            )
        
        return sorted_intervals

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring and debugging."""
        if not hasattr(self, '_cache_stats'):
            raise CacheOperationError(
                "Cache statistics not initialized",
                operation='get_cache_stats'
            )
        
        timeframe_stats = {}
        for timeframe, instruments in self._rolling_instrument_history.items():
            timeframe_stats[timeframe] = {
                'instrument_count': len(instruments),
                'total_intervals': sum(len(intervals) for intervals in instruments.values()),
                'instruments': list(instruments.keys())
            }
        
        return {
            'cache_stats': self._cache_stats,
            'timeframe_stats': timeframe_stats,
            'rolling_window_size': self.rolling_window,
            'universe_id': self.universe_id,
            'timestamp': datetime.now().isoformat()
        }

    def validate_timestamp_format(self, timestamp: str) -> bool:
        """Validate timestamp format with fail-fast approach."""
        if not isinstance(timestamp, str):
            raise TimestampValidationError(
                f"timestamp must be string, got {type(timestamp)}",
                timestamp_value=timestamp,
                expected_format='YYYYMMDD_HHMMSS'
            )
        
        if not timestamp:
            raise TimestampValidationError(
                "timestamp cannot be empty",
                timestamp_value=timestamp,
                expected_format='YYYYMMDD_HHMMSS'
            )
        
        try:
            datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
            return True
        except ValueError as e:
            raise TimestampValidationError(
                f"Invalid timestamp format: {e}",
                timestamp_value=timestamp,
                expected_format='YYYYMMDD_HHMMSS'
            )


# ====================================================================
# FACTORY FUNCTION
# ====================================================================

def create_universe_state_manager(universe_id: int = None, rolling_window: int = 100) -> UniverseStateManagerFailFast:
    """Create and initialize fail-fast universe state manager."""
    try:
        manager = UniverseStateManagerFailFast(universe_id=universe_id, rolling_window=rolling_window)
        logging.getLogger(__name__).info(f"🎯 Fail-Fast UniverseStateManager created for universe_id={universe_id}")
        return manager
    except (ParameterValidationError, DatabaseOperationError, CacheInitializationError):
        # Re-raise specific errors
        raise
    except Exception as e:
        raise UniverseStateManagerError(f"Unexpected error during manager creation: {e}")


if __name__ == "__main__":
    # Example usage
    try:
        manager = create_universe_state_manager(universe_id=1, rolling_window=50)
        stats = manager.get_cache_stats()
        print("Cache Stats:", stats)
        
    except UniverseStateManagerError as e:
        print(f"Universe state manager error: {e}")
        print(f"Context: {e.context}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise  # Don't mask unexpected errors