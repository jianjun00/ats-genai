"""
File-Based Minute Market Data Manager

Provides minute-level OHLC data from file-based storage system, following the
MarketDataManager interface pattern used by daily price managers.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
import logging

from market_data.market_data_manager import MarketDataManager
from storage.file_based_minute_manager import FileBasedMinuteManager
from config.environment import Environment
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
import gin

logger = logging.getLogger(__name__)


@gin.configurable
class FileBasedMinuteMarketDataManager(MarketDataManager):
    """
    Market data manager that provides minute-level OHLC data from file-based storage.
    
    Follows the same interface patterns as DailyPriceMarketDataManager but operates
    on minute-level data stored in parquet files.
    
    Provides OHLC data aggregation across multiple timeframes.
    """
    
    def __init__(self, 
                 env: Environment, 
                 base_path: str = "/mnt/d/ats-data/minute-bars",
):
        """
        Initialize with environment and base path to minute bar storage.
        
        Args:
            env: Environment configuration
            base_path: Path to minute bar storage directory
        """
        self.env = env
        self.base_path = Path(base_path)
        self.minute_manager = FileBasedMinuteManager(self.base_path)
        self.xrefs_dao = InstrumentXrefsDAO(env) if env else None
        self._cache = {}  # Cache for recent queries
        
        logger.info(f"Initialized FileBasedMinuteMarketDataManager with path: {self.base_path}")
    
    async def get_ohlc_for_interval(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
        interval: str = '1m'
    ) -> Dict[str, pd.DataFrame]:
        """
        Get OHLC data for specified interval using standardized interval notation.
        
        Args:
            symbols: List of symbol strings (e.g., ['AAPL', 'TSLA'])
            start: Start datetime for data query
            end: End datetime for data query  
            interval: Interval specification ('1m', '5m', '15m', '1h', '1d', '1w')
            
        Returns:
            Dict mapping symbol to DataFrame with columns: timestamp, open, high, low, close, volume
        """
        # Type validation for parameters
        if not isinstance(symbols, list):
            raise ValueError(f"symbols must be a list, got {type(symbols)}")
        
        if not symbols:
            raise ValueError("symbols list cannot be empty")
        
        for i, symbol in enumerate(symbols):
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError(f"symbols[{i}] must be a non-empty string, got {symbol} (type: {type(symbol)})")
        
        # Validate datetime parameters
        if not hasattr(start, 'date') and not hasattr(start, 'year'):
            raise ValueError(f"start must be a datetime or date object, got {type(start)}")
        
        if not hasattr(end, 'date') and not hasattr(end, 'year'):
            raise ValueError(f"end must be a datetime or date object, got {type(end)}")
        
        if not isinstance(interval, str) or not interval.strip():
            raise ValueError(f"interval must be a non-empty string, got {interval} (type: {type(interval)})")
        
        # Validate date range
        start_date = start.date() if hasattr(start, 'date') else start
        end_date = end.date() if hasattr(end, 'date') else end
        if start_date >= end_date:
            raise ValueError(f"start date ({start_date}) must be before end date ({end_date})")
        # Convert interval string to minutes
        timeframe_minutes = self._parse_interval_to_minutes(interval)
        
        return await self.get_minute_ohlc_batch(
            symbols=symbols,
            start=start,
            end=end,
            timeframe_minutes=timeframe_minutes
        )
    
    def _parse_interval_to_minutes(self, interval: str) -> int:
        """
        Parse interval string to minutes.
        
        Args:
            interval: Interval string ('1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w')
            
        Returns:
            Number of minutes for the interval
        """
        interval = interval.lower().strip()
        
        # Handle minute intervals
        if interval.endswith('m'):
            return int(interval[:-1])
        
        # Handle hour intervals
        elif interval.endswith('h'):
            return int(interval[:-1]) * 60
        
        # Handle day intervals
        elif interval.endswith('d'):
            return int(interval[:-1]) * 1440  # 24 * 60 minutes
        
        # Handle week intervals
        elif interval.endswith('w'):
            return int(interval[:-1]) * 10080  # 7 * 24 * 60 minutes
        
        # Handle month intervals (approximate)
        elif interval.endswith('M'):
            return int(interval[:-1]) * 43800  # 30.4 * 24 * 60 minutes (average)
        
        else:
            raise ValueError(f"Unsupported interval format: {interval}. Use formats like '1m', '5m', '15m', '1h', '1d', '1w'")
    
    async def get_minute_ohlc_batch(
        self, 
        symbols: List[str], 
        start: datetime, 
        end: datetime,
        timeframe_minutes: int = 1
    ) -> Dict[str, pd.DataFrame]:
        """
        Get minute-level OHLC data for multiple symbols.
        
        Args:
            symbols: List of symbol strings (e.g., ['AAPL', 'TSLA'])
            start: Start datetime for data query
            end: End datetime for data query
            timeframe_minutes: Minutes per bar (1, 5, 15, 60, etc.)
            
        Returns:
            Dict mapping symbol to DataFrame with columns: timestamp, open, high, low, close, volume
        """
        # Type validation for parameters
        if not isinstance(symbols, list):
            raise ValueError(f"symbols must be a list, got {type(symbols)}")
        
        if not symbols:
            raise ValueError("symbols list cannot be empty")
        
        for i, symbol in enumerate(symbols):
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError(f"symbols[{i}] must be a non-empty string, got {symbol} (type: {type(symbol)})")
        
        # Validate datetime parameters
        if not hasattr(start, 'date') and not hasattr(start, 'year'):
            raise ValueError(f"start must be a datetime or date object, got {type(start)}")
        
        if not hasattr(end, 'date') and not hasattr(end, 'year'):
            raise ValueError(f"end must be a datetime or date object, got {type(end)}")
        
        if not isinstance(timeframe_minutes, int) or timeframe_minutes <= 0:
            raise ValueError(f"timeframe_minutes must be a positive integer, got {timeframe_minutes} (type: {type(timeframe_minutes)})")
        
        # Validate date range
        start_date = start.date() if hasattr(start, 'date') else start
        end_date = end.date() if hasattr(end, 'date') else end
        if start_date >= end_date:
            raise ValueError(f"start date ({start_date}) must be before end date ({end_date})")
        logger.debug(f"Getting minute OHLC for {len(symbols)} symbols from {start} to {end}")
        
        result = {}
        
        # Get data for each symbol
        for symbol in symbols:
            try:
                df = await self._get_symbol_minute_data(symbol, start, end)
                
                if not df.empty:
                    # Aggregate to target timeframe if needed
                    if timeframe_minutes > 1:
                        df = self._aggregate_to_timeframe(df, timeframe_minutes)
                    
                    if not df.empty:
                        result[symbol] = df
                        logger.debug(f"Retrieved {len(df)} {timeframe_minutes}-minute bars for {symbol}")
                    else:
                        logger.warning(f"No data after aggregation for {symbol}")
                else:
                    logger.warning(f"No minute data found for {symbol} in date range")
                    
            except Exception as e:
                logger.error(f"Error getting minute data for {symbol}: {e}")
                continue
        
        logger.info(f"Retrieved minute data for {len(result)}/{len(symbols)} symbols")
        return result
    
    
    async def _get_symbol_minute_data(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> pd.DataFrame:
        """Get 1-minute data for a single symbol."""
        
        # Check cache first
        cache_key = f"{symbol}_{start.date()}_{end.date()}"
        if cache_key in self._cache:
            cached_df = self._cache[cache_key]
            # Filter cached data to exact time range with timezone compatibility
            if cached_df['timestamp'].dt.tz is not None:
                # Cached data has timezone, convert filter dates to same timezone
                start_tz = pd.Timestamp(start).tz_localize('UTC') if start.tzinfo is None else pd.Timestamp(start)
                end_tz = pd.Timestamp(end).tz_localize('UTC') if end.tzinfo is None else pd.Timestamp(end)
            else:
                # Cached data is timezone-naive, use dates as-is
                start_tz = start
                end_tz = end
            mask = (cached_df['timestamp'] >= start_tz) & (cached_df['timestamp'] <= end_tz)
            return cached_df[mask].copy()
        
        # Query from storage
        df = await self.minute_manager.query_minute_data(
            symbol=symbol,
            start_date=start,
            end_date=end
        )
        
        if df.empty:
            return df
        
        # Ensure proper column names and types
        df = self._standardize_dataframe(df)
        
        # Cache the result (limit cache size)
        if len(self._cache) < 10:  # Simple cache size limit
            self._cache[cache_key] = df.copy()
        
        return df
    
    def _standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize DataFrame format and column names."""
        
        # Ensure timestamp is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Ensure required columns exist
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Ensure numeric types
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove any rows with NaN in OHLC
        df = df.dropna(subset=['open', 'high', 'low', 'close'])
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    
    def _aggregate_to_timeframe(self, df: pd.DataFrame, target_minutes: int) -> pd.DataFrame:
        """
        Aggregate 1-minute data to target timeframe.
        
        Args:
            df: DataFrame with 1-minute data
            target_minutes: Target timeframe in minutes (5, 15, 60, etc.)
            
        Returns:
            DataFrame with aggregated data
        """
        if df.empty:
            return df
        
        # Set timestamp as index for resampling
        df_copy = df.copy()
        df_copy = df_copy.set_index('timestamp')
        
        # Define aggregation rules
        agg_rules = {
            'open': 'first',   # First open of the period
            'high': 'max',     # Maximum high of the period
            'low': 'min',      # Minimum low of the period
            'close': 'last',   # Last close of the period
            'volume': 'sum'    # Total volume of the period
        }
        
        # Add any additional columns that might exist
        for col in df_copy.columns:
            if col not in agg_rules:
                if col in ['vwap', 'trade_count']:
                    agg_rules[col] = 'mean'  # Average for these fields
                elif col == 'vendor':
                    agg_rules[col] = 'first'  # First vendor
        
        # Resample to target timeframe
        resampled = df_copy.resample(f'{target_minutes}min').agg(agg_rules)
        
        # Remove periods with no data
        resampled = resampled.dropna(subset=['open', 'high', 'low', 'close'])
        
        # Reset index to get timestamp as column again
        result = resampled.reset_index()
        
        logger.debug(f"Aggregated from {len(df)} 1-minute bars to {len(result)} {target_minutes}-minute bars")
        
        return result
    
    async def get_ohlc_batch(
        self, 
        instrument_ids: List[int], 
        start: datetime, 
        end: datetime
    ) -> Dict[int, Optional[Dict[str, float]]]:
        """
        MarketDataManager interface implementation.
        
        Note: This converts symbols to instrument_ids - requires mapping logic.
        For now, we'll log a warning as this interface expects instrument_ids.
        """
        logger.warning(
            "get_ohlc_batch called with instrument_ids but FileBasedMinuteManager uses symbols. "
            "Use get_minute_ohlc_batch with symbols instead."
        )
        return {}
    
    def get_ohlc(
        self, 
        instrument_id: int, 
        start: datetime, 
        end: datetime
    ) -> Optional[Dict[str, float]]:
        """
        MarketDataManager interface implementation.
        
        Note: This is synchronous but our data access is async.
        Returns None and logs warning.
        """
        logger.warning(
            "get_ohlc called but FileBasedMinuteManager requires async access. "
            "Use get_minute_ohlc_batch instead."
        )
        return None
    
    async def get_symbols_for_date_range(
        self, 
        start: datetime, 
        end: datetime
    ) -> List[str]:
        """Get list of symbols that have data in the given date range."""
        # Type validation for datetime parameters
        if not hasattr(start, 'date') and not hasattr(start, 'year'):
            raise ValueError(f"start must be a datetime or date object, got {type(start)}")
        
        if not hasattr(end, 'date') and not hasattr(end, 'year'):
            raise ValueError(f"end must be a datetime or date object, got {type(end)}")
        
        # Validate date range
        start_date = start.date() if hasattr(start, 'date') else start
        end_date = end.date() if hasattr(end, 'date') else end
        if start_date >= end_date:
            raise ValueError(f"start date ({start_date}) must be before end date ({end_date})")
        
        # Look for symbol directories in the base path
        symbols = []
        
        try:
            for symbol_dir in self.base_path.iterdir():
                if symbol_dir.is_dir() and not symbol_dir.name.startswith('.'):
                    # Check if symbol has any data files in date range
                    has_data = await self._symbol_has_data_in_range(
                        symbol_dir.name, start, end
                    )
                    if has_data:
                        symbols.append(symbol_dir.name)
            
            logger.info(f"Found {len(symbols)} symbols with data in date range")
            return sorted(symbols)
            
        except Exception as e:
            logger.error(f"Error getting symbols for date range: {e}")
            return []
    
    async def _symbol_has_data_in_range(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> bool:
        """Check if a symbol has any data files in the given date range."""
        
        try:
            # Try to query a small amount of data
            df = await self.minute_manager.query_minute_data(
                symbol=symbol,
                start_date=start,
                end_date=min(end, start + timedelta(days=1))  # Just check first day
            )
            return not df.empty
            
        except Exception:
            return False
    
    async def get_multi_timeframe_data(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
        intervals: List[str] = None
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Get multi-timeframe OHLCV data for multiple symbols.
        
        Args:
            symbols: List of symbols
            start: Start datetime
            end: End datetime
            intervals: List of intervals ['1m', '5m', '15m', '1h', '1d', '1w']
            
        Returns:
            Nested dict: {symbol: {interval: DataFrame}}
        """
        # Type validation for parameters
        if not isinstance(symbols, list):
            raise ValueError(f"symbols must be a list, got {type(symbols)}")
        
        if not symbols:
            raise ValueError("symbols list cannot be empty")
        
        for i, symbol in enumerate(symbols):
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError(f"symbols[{i}] must be a non-empty string, got {symbol} (type: {type(symbol)})")
        
        # Validate datetime parameters
        if not hasattr(start, 'date') and not hasattr(start, 'year'):
            raise ValueError(f"start must be a datetime or date object, got {type(start)}")
        
        if not hasattr(end, 'date') and not hasattr(end, 'year'):
            raise ValueError(f"end must be a datetime or date object, got {type(end)}")
        
        # Validate intervals if provided
        if intervals is not None:
            if not isinstance(intervals, list):
                raise ValueError(f"intervals must be a list or None, got {type(intervals)}")
            
            valid_intervals = {'1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w'}
            for i, interval in enumerate(intervals):
                if not isinstance(interval, str) or not interval.strip():
                    raise ValueError(f"intervals[{i}] must be a non-empty string, got {interval} (type: {type(interval)})")
                if interval not in valid_intervals:
                    raise ValueError(f"intervals[{i}] must be one of {sorted(valid_intervals)}, got '{interval}'")
        
        # Validate date range
        start_date = start.date() if hasattr(start, 'date') else start
        end_date = end.date() if hasattr(end, 'date') else end
        if start_date >= end_date:
            raise ValueError(f"start date ({start_date}) must be before end date ({end_date})")
        if not intervals:
            intervals = ['5m', '15m', '1h', '1d', '1w']
        
        result = {}
        
        for symbol in symbols:
            result[symbol] = {}
            
            for interval in intervals:
                try:
                    # Get OHLC data for this timeframe
                    timeframe_data = await self.get_ohlc_for_interval(
                        symbols=[symbol],
                        start=start,
                        end=end,
                        interval=interval
                    )
                    
                    if symbol in timeframe_data:
                        result[symbol][interval] = timeframe_data[symbol]
                        logger.debug(f"Retrieved {interval} data for {symbol}: {len(timeframe_data[symbol])} bars")
                    else:
                        result[symbol][interval] = pd.DataFrame()  # Empty DataFrame
                        
                except Exception as e:
                    logger.error(f"Failed to get {interval} data for {symbol}: {e}")
                    result[symbol][interval] = pd.DataFrame()  # Empty DataFrame
        
        logger.info(f"Retrieved multi-timeframe data for {len(symbols)} symbols across {len(intervals)} intervals")
        return result
    
    def get_ohlcv_data(self, instrument_id: int, reference_datetime: datetime, periods: int, 
                           time_interval: str, direction: str = 'backward') -> pd.DataFrame:
        """
        Get OHLCV data for a specific instrument over multiple periods.
        
        Args:
            instrument_id: The instrument ID to retrieve data for
            reference_datetime: Reference datetime point (direction determines if we go back or forward from here)
            periods: Number of periods to retrieve
            time_interval: Time interval ('1m', '5m', '15m', '1h', '1d', '1w')
            direction: 'backward' for historical data, 'forward' for future data
            
        Returns:
            DataFrame with columns ['open', 'high', 'low', 'close', 'volume'] and datetime index
        """
        import asyncio
        
        # Run the async version in the current event loop or create one
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, we need to schedule this differently
                # For now, we'll create a synchronous fallback that logs a warning
                logger.warning("get_ohlcv_data called from within an async context - this may cause issues")
                return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            else:
                return loop.run_until_complete(self._get_ohlcv_data_async(
                    instrument_id, reference_datetime, periods, time_interval, direction
                ))
        except RuntimeError:
            # No event loop exists, create one
            return asyncio.run(self._get_ohlcv_data_async(
                instrument_id, reference_datetime, periods, time_interval, direction
            ))
    
    async def _get_ohlcv_data_async(self, instrument_id: int, reference_datetime: datetime, periods: int, 
                                   time_interval: str, direction: str = 'backward') -> pd.DataFrame:
        """
        Async implementation of get_ohlcv_data.
        """
        if not self.xrefs_dao:
            logger.warning("No xrefs_dao available for instrument_id to symbol mapping")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        
        try:
            # Map instrument_id to symbol
            symbol = await self.xrefs_dao.get_symbol_by_instrument_id(instrument_id)
            if not symbol:
                logger.warning(f"No symbol found for instrument_id {instrument_id}")
                return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            
            # Convert time_interval to minutes for date range calculation
            interval_minutes = self._parse_interval_to_minutes(time_interval)
            
            # Calculate date range based on periods and direction
            if direction == 'forward':
                # For future data: start from reference_datetime, go forward
                start_date = reference_datetime
                end_query_date = reference_datetime + timedelta(minutes=interval_minutes * periods * 2)  # Buffer for weekends/holidays
            else:
                # For historical data: end at reference_datetime, go backward
                start_date = reference_datetime - timedelta(minutes=interval_minutes * periods * 2)  # Buffer for weekends/holidays
                end_query_date = reference_datetime
            
            logger.debug(f"Getting OHLCV data for instrument_id={instrument_id} symbol={symbol} "
                        f"from {start_date} to {end_query_date} interval={time_interval} periods={periods}")
            
            # Use existing get_ohlc_for_interval method
            result = await self.get_ohlc_for_interval(
                symbols=[symbol],
                start=start_date,
                end=end_query_date,
                interval=time_interval
            )
            
            if symbol not in result or result[symbol].empty:
                logger.warning(f"No data returned for symbol {symbol}")
                return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            
            df = result[symbol].copy()
            
            # Sort by timestamp 
            df = df.sort_values('timestamp')
            
            # Filter based on direction and get the requested number of periods
            if direction == 'forward':
                # Get periods starting from reference_datetime
                df = df[df['timestamp'] >= pd.Timestamp(reference_datetime)]
                df = df.head(periods)
            else:
                # Get periods ending at reference_datetime
                df = df[df['timestamp'] < pd.Timestamp(reference_datetime)]
                df = df.tail(periods)
            
            # Ensure we have the expected columns and set timestamp as index
            expected_columns = ['open', 'high', 'low', 'close', 'volume']
            
            # Check if we have all expected columns
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing columns in data: {missing_columns}")
                for col in missing_columns:
                    df[col] = 0.0  # Add missing columns with default values
            
            # Select only the expected columns and ensure proper dtypes
            df = df[expected_columns].copy()
            for col in expected_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
            logger.debug(f"Returning {len(df)} rows of OHLCV data for instrument_id={instrument_id}")
            return df
            
        except Exception as e:
            logger.error(f"Error getting OHLCV data for instrument_id {instrument_id}: {e}")
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])

    async def close(self):
        """Clean up resources."""
        if hasattr(self.minute_manager, 'close'):
            await self.minute_manager.close()
        self._cache.clear()
        logger.info("FileBasedMinuteMarketDataManager closed")


# Convenience factory function
async def create_minute_manager(
    env: Environment, 
    base_path: str = "/mnt/d/ats-data/minute-bars"
) -> FileBasedMinuteMarketDataManager:
    """Create and initialize a file-based minute market data manager."""
    
    manager = FileBasedMinuteMarketDataManager(env, base_path)
    
    # Verify the base path exists
    if not manager.base_path.exists():
        raise FileNotFoundError(f"Minute bars path does not exist: {base_path}")
    
    logger.info(f"Created FileBasedMinuteMarketDataManager for path: {base_path}")
    return manager