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

from domains.market_data.services.market_data_manager import MarketDataManager
from infrastructure.storage.file_based_minute_manager import FileBasedMinuteManager
from shared.utils.environment import Environment

logger = logging.getLogger(__name__)


class FileBasedMinuteMarketDataManager(MarketDataManager):
    """
    Market data manager that provides minute-level OHLC data from file-based storage.
    
    Follows the same interface patterns as DailyPriceMarketDataManager but operates
    on minute-level data stored in parquet files.
    """
    
    def __init__(self, env: Environment, base_path: str = "/mnt/d/ats-data/minute-bars"):
        """
        Initialize with environment and base path to minute bar storage.
        
        Args:
            env: Environment configuration
            base_path: Path to minute bar storage directory
        """
        self.env = env
        self.base_path = Path(base_path)
        self.minute_manager = FileBasedMinuteManager(self.base_path)
        self._cache = {}  # Cache for recent queries
        
        logger.info(f"Initialized FileBasedMinuteMarketDataManager with path: {self.base_path}")
    
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