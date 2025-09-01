#!/usr/bin/env python3
"""
Multi-Scale Minute Manager

Enhanced FileBasedMinuteManager with HDF5 multi-scale caching capabilities.
Provides unified interface for storing and retrieving multi-scale temporal data.

Key Features:
- Extends existing FileBasedMinuteManager
- Integrates HDF5MultiScaleCache for performance
- Automatic scale aggregation and management
- Efficient multi-scale queries
- Transparent caching with fallback to Parquet files
"""

import os
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from pathlib import Path
import logging
from dataclasses import dataclass

from .file_based_minute_manager import (
    FileBasedMinuteManager, 
    MinuteBar, 
    FileMetadata,
    OverlapInfo
)
from .hdf5_multi_scale_cache import HDF5MultiScaleCache, CacheConfig
from .multi_scale_sequence import (
    MultiScaleSequence, 
    TimeScale, 
    ScaleFeatures,
    create_multi_scale_sequence
)

logger = logging.getLogger(__name__)


@dataclass
class MultiScaleConfig:
    """Configuration for multi-scale operations."""
    enable_hdf5_cache: bool = True
    cache_path: str = "/home/jianjun/ats-data/hdf5-cache"
    auto_aggregate_scales: bool = True
    preferred_cache_scales: List[TimeScale] = None
    cache_write_threshold: int = 1000  # Minimum records before caching
    
    def __post_init__(self):
        if self.preferred_cache_scales is None:
            self.preferred_cache_scales = [TimeScale.HOURLY, TimeScale.DAILY]


class MultiScaleMinuteManager:
    """
    Enhanced minute data manager with multi-scale capabilities.
    
    Combines the robustness of FileBasedMinuteManager with the performance
    of HDF5MultiScaleCache for optimal multi-scale data operations.
    """
    
    def __init__(
        self,
        base_path: str = "/home/jianjun/ats-data/minute-files",
        multi_scale_config: MultiScaleConfig = None,
        file_manager_kwargs: Dict[str, Any] = None,
        cache_config: CacheConfig = None
    ):
        self.config = multi_scale_config or MultiScaleConfig()
        
        # Initialize file-based manager (primary storage)
        file_kwargs = file_manager_kwargs or {}
        self.file_manager = FileBasedMinuteManager(base_path, **file_kwargs)
        
        # Initialize HDF5 cache (performance layer)
        self.hdf5_cache = None
        if self.config.enable_hdf5_cache:
            cache_cfg = cache_config or CacheConfig(cache_path=self.config.cache_path)
            self.hdf5_cache = HDF5MultiScaleCache(cache_cfg)
        
        logger.info(f"MultiScaleMinuteManager initialized with cache: {self.config.enable_hdf5_cache}")
    
    async def store_minute_data(
        self,
        symbol: str,
        bars: List[MinuteBar],
        overlap_strategy: str = 'merge',
        update_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Store minute bars with multi-scale processing.
        
        Args:
            symbol: Stock symbol
            bars: List of minute bars
            overlap_strategy: Strategy for handling overlaps
            update_cache: Whether to update HDF5 cache
        
        Returns:
            Combined storage and caching results
        """
        if not bars:
            return {'stored': 0, 'cached': 0}
        
        logger.info(f"Storing {len(bars)} minute bars for {symbol} with multi-scale processing")
        
        # Store in primary file system
        file_result = await self.file_manager.store_minute_data(
            symbol, bars, overlap_strategy
        )
        
        result = {
            'file_storage': file_result,
            'cache_storage': {}
        }
        
        # Update HDF5 cache if enabled and threshold met
        if (self.hdf5_cache is not None and 
            update_cache and 
            len(bars) >= self.config.cache_write_threshold):
            
            try:
                # Convert MinuteBar objects to DataFrame
                minute_df = self._bars_to_dataframe(bars)
                
                # Store in cache with auto-aggregation
                cache_result = await self.hdf5_cache.store_minute_data(
                    symbol, minute_df, auto_aggregate=self.config.auto_aggregate_scales
                )
                
                result['cache_storage'] = cache_result
                logger.info(f"Updated cache for {symbol}: {cache_result}")
                
            except Exception as e:
                logger.warning(f"Failed to update cache for {symbol}: {e}")
                result['cache_storage'] = {'error': str(e)}
        
        return result
    
    def _bars_to_dataframe(self, bars: List[MinuteBar]) -> pd.DataFrame:
        """Convert MinuteBar objects to DataFrame."""
        data = []
        for bar in bars:
            row = {
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
            }
            
            # Add optional fields
            if bar.vwap is not None:
                row['vwap'] = bar.vwap
            if bar.trade_count is not None:
                row['trade_count'] = bar.trade_count
                
            data.append(row)
        
        return pd.DataFrame(data)
    
    async def query_minute_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None,
        prefer_cache: bool = True
    ) -> pd.DataFrame:
        """
        Query minute data with cache-first strategy.
        
        Args:
            symbol: Stock symbol
            start_date: Start of time range
            end_date: End of time range
            columns: Specific columns to retrieve
            prefer_cache: Whether to try cache first
        
        Returns:
            DataFrame with minute data
        """
        logger.debug(f"Querying minute data for {symbol} from {start_date} to {end_date}")
        
        # Try cache first if enabled and preferred
        if self.hdf5_cache is not None and prefer_cache:
            try:
                cache_data = await self.hdf5_cache.get_data(
                    symbol, TimeScale.MINUTE, start_date, end_date, columns
                )
                
                if cache_data is not None and not cache_data.empty:
                    logger.debug(f"Retrieved {len(cache_data)} records from cache")
                    return cache_data
                
            except Exception as e:
                logger.warning(f"Cache query failed for {symbol}: {e}")
        
        # Fallback to file system
        logger.debug(f"Falling back to file system for {symbol}")
        return await self.file_manager.query_minute_data(
            symbol, start_date, end_date, columns
        )
    
    async def get_multi_scale_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        scales: List[TimeScale] = None,
        include_events: bool = False
    ) -> Optional[MultiScaleSequence]:
        """
        Get multi-scale data for symbol and time range.
        
        Args:
            symbol: Stock symbol
            start_date: Start of time range
            end_date: End of time range
            scales: List of scales to retrieve
            include_events: Whether to include event data
        
        Returns:
            MultiScaleSequence or None if no data available
        """
        if scales is None:
            scales = list(TimeScale)
        
        logger.info(f"Getting multi-scale data for {symbol} across {len(scales)} scales")
        
        # Try cache first for efficient multi-scale access
        if self.hdf5_cache is not None:
            try:
                sequence = await self.hdf5_cache.get_multi_scale_data(
                    symbol, (start_date, end_date), scales
                )
                
                if sequence is not None:
                    logger.debug(f"Retrieved multi-scale data from cache: {len(sequence.scales)} scales")
                    return sequence
                
            except Exception as e:
                logger.warning(f"Multi-scale cache query failed for {symbol}: {e}")
        
        # Fallback: construct from file system data
        return await self._construct_multi_scale_from_files(
            symbol, start_date, end_date, scales
        )
    
    async def _construct_multi_scale_from_files(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        scales: List[TimeScale]
    ) -> Optional[MultiScaleSequence]:
        """Construct multi-scale sequence from file system data."""
        
        # Get minute data from files
        minute_data = await self.file_manager.query_minute_data(
            symbol, start_date, end_date
        )
        
        if minute_data.empty:
            return None
        
        # Create aggregated scales
        scale_data = {}
        
        if TimeScale.MINUTE in scales:
            scale_data[TimeScale.MINUTE] = minute_data
        
        if TimeScale.HOURLY in scales and not minute_data.empty:
            scale_data[TimeScale.HOURLY] = self._aggregate_to_scale(minute_data, '1H')
        
        if TimeScale.DAILY in scales and not minute_data.empty:
            scale_data[TimeScale.DAILY] = self._aggregate_to_scale(minute_data, '1D')
        
        if TimeScale.WEEKLY in scales and not minute_data.empty:
            scale_data[TimeScale.WEEKLY] = self._aggregate_to_scale(minute_data, '1W')
        
        # Create multi-scale sequence
        return create_multi_scale_sequence(
            symbol=symbol,
            time_range=(start_date, end_date),
            minute_data=scale_data.get(TimeScale.MINUTE),
            hourly_data=scale_data.get(TimeScale.HOURLY),
            daily_data=scale_data.get(TimeScale.DAILY),
            weekly_data=scale_data.get(TimeScale.WEEKLY)
        )
    
    def _aggregate_to_scale(self, minute_data: pd.DataFrame, freq: str) -> pd.DataFrame:
        """Aggregate minute data to specified frequency."""
        if minute_data.empty:
            return pd.DataFrame()
        
        df = minute_data.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # Define aggregation functions
        agg_funcs = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        
        # Add aggregation for technical indicators (use mean as default)
        for col in df.columns:
            if col not in agg_funcs:
                agg_funcs[col] = 'mean'
        
        # Perform aggregation
        aggregated = df.resample(freq).agg(agg_funcs)
        aggregated = aggregated.dropna()
        
        return aggregated.reset_index()
    
    async def get_hourly_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get hourly data for symbol and time range."""
        if self.hdf5_cache is not None:
            try:
                data = await self.hdf5_cache.get_data(
                    symbol, TimeScale.HOURLY, start_date, end_date, columns
                )
                if data is not None:
                    return data
            except Exception as e:
                logger.warning(f"Hourly cache query failed: {e}")
        
        # Generate from minute data
        minute_data = await self.query_minute_data(symbol, start_date, end_date)
        if minute_data.empty:
            return pd.DataFrame()
        
        return self._aggregate_to_scale(minute_data, '1H')
    
    async def get_daily_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get daily data for symbol and time range."""
        if self.hdf5_cache is not None:
            try:
                data = await self.hdf5_cache.get_data(
                    symbol, TimeScale.DAILY, start_date, end_date, columns
                )
                if data is not None:
                    return data
            except Exception as e:
                logger.warning(f"Daily cache query failed: {e}")
        
        # Generate from minute data
        minute_data = await self.query_minute_data(symbol, start_date, end_date)
        if minute_data.empty:
            return pd.DataFrame()
        
        return self._aggregate_to_scale(minute_data, '1D')
    
    async def precompute_aggregations(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        scales: List[TimeScale] = None
    ) -> Dict[str, Any]:
        """
        Precompute and cache aggregations for specified time range.
        
        Useful for batch processing and performance optimization.
        """
        if self.hdf5_cache is None:
            return {'error': 'HDF5 cache not enabled'}
        
        if scales is None:
            scales = self.config.preferred_cache_scales
        
        logger.info(f"Precomputing aggregations for {symbol} across {len(scales)} scales")
        
        # Get minute data
        minute_data = await self.query_minute_data(symbol, start_date, end_date)
        if minute_data.empty:
            return {'error': 'No minute data available'}
        
        results = {}
        
        try:
            # Store minute data in cache if not already there
            cache_result = await self.hdf5_cache.store_minute_data(
                symbol, minute_data, auto_aggregate=True
            )
            results['cache_update'] = cache_result
            
        except Exception as e:
            logger.error(f"Error precomputing aggregations for {symbol}: {e}")
            results['error'] = str(e)
        
        return results
    
    async def get_storage_stats(self, include_cache: bool = True) -> Dict[str, Any]:
        """Get comprehensive storage statistics."""
        stats = {
            'file_storage': await self.file_manager.get_storage_stats(),
            'cache_storage': {}
        }
        
        if include_cache and self.hdf5_cache is not None:
            try:
                stats['cache_storage'] = await self.hdf5_cache.get_cache_stats()
            except Exception as e:
                stats['cache_storage'] = {'error': str(e)}
        
        return stats
    
    async def verify_data_integrity(
        self,
        symbol: str = None,
        check_cache: bool = True
    ) -> Dict[str, Any]:
        """Verify data integrity across file and cache systems."""
        results = {
            'file_verification': await self.file_manager.verify_data_integrity(symbol),
            'cache_verification': {}
        }
        
        # TODO: Implement cache-specific integrity checks
        if check_cache and self.hdf5_cache is not None:
            results['cache_verification'] = {'status': 'not_implemented'}
        
        return results
    
    async def sync_cache_with_files(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Synchronize HDF5 cache with file system data."""
        if self.hdf5_cache is None:
            return {'error': 'HDF5 cache not enabled'}
        
        logger.info(f"Synchronizing cache with files for {symbol}")
        
        try:
            # Get latest data from files
            minute_data = await self.file_manager.query_minute_data(
                symbol, start_date, end_date
            )
            
            if minute_data.empty:
                return {'synchronized': 0, 'message': 'No file data to sync'}
            
            # Update cache
            result = await self.hdf5_cache.store_minute_data(
                symbol, minute_data, auto_aggregate=True
            )
            
            logger.info(f"Synchronized {result.get('stored', 0)} records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error synchronizing cache for {symbol}: {e}")
            return {'error': str(e)}
    
    async def cleanup_resources(self, max_age_days: int = 30) -> Dict[str, Any]:
        """Clean up old resources across both storage systems."""
        results = {}
        
        # Clean up file system backups
        if hasattr(self.file_manager, 'cleanup_old_backups'):
            results['file_cleanup'] = await self.file_manager.cleanup_old_backups(max_age_days)
        
        # Clean up HDF5 cache
        if self.hdf5_cache is not None:
            results['cache_cleanup'] = await self.hdf5_cache.cleanup_cache(max_age_days)
        
        return results
    
    async def close(self):
        """Clean up all resources."""
        if hasattr(self.file_manager, 'close'):
            await self.file_manager.close()
        
        if self.hdf5_cache is not None:
            await self.hdf5_cache.close()
        
        logger.info("MultiScaleMinuteManager closed")


# Convenience functions
async def create_multi_scale_manager(
    base_path: str = None,
    enable_cache: bool = True,
    cache_path: str = None,
    **kwargs
) -> MultiScaleMinuteManager:
    """Create and initialize a multi-scale minute manager."""
    
    multi_scale_config = MultiScaleConfig(
        enable_hdf5_cache=enable_cache,
        cache_path=cache_path or "/home/jianjun/ats-data/hdf5-cache"
    )
    
    manager = MultiScaleMinuteManager(
        base_path=base_path or "/home/jianjun/ats-data/minute-files",
        multi_scale_config=multi_scale_config,
        **kwargs
    )
    
    return manager


# Example usage and testing
if __name__ == "__main__":
    import random
    
    async def example_usage():
        """Example usage of multi-scale minute manager."""
        
        # Create manager
        manager = await create_multi_scale_manager()
        
        # Create sample data
        sample_bars = []
        base_time = datetime(2024, 1, 15, 9, 30)
        
        for i in range(1000):  # More data to trigger caching
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=150.0 + random.uniform(-2, 2),
                high=150.0 + random.uniform(0, 3),
                low=150.0 + random.uniform(-3, 0),
                close=150.0 + random.uniform(-2, 2),
                volume=random.randint(1000, 10000),
                vendor='test'
            )
            sample_bars.append(bar)
        
        # Store data with multi-scale processing
        print("Storing sample data with multi-scale processing...")
        result = await manager.store_minute_data('AAPL', sample_bars)
        print(f"Storage result: {result}")
        
        # Query different scales
        query_start = base_time
        query_end = base_time + timedelta(hours=8)
        
        print("Querying minute data...")
        minute_data = await manager.query_minute_data('AAPL', query_start, query_end)
        print(f"Minute data: {len(minute_data)} records")
        
        print("Querying hourly data...")
        hourly_data = await manager.get_hourly_data('AAPL', query_start, query_end)
        print(f"Hourly data: {len(hourly_data)} records")
        
        print("Querying daily data...")
        daily_data = await manager.get_daily_data('AAPL', query_start, query_end)
        print(f"Daily data: {len(daily_data)} records")
        
        # Get multi-scale sequence
        print("Getting multi-scale sequence...")
        sequence = await manager.get_multi_scale_data(
            'AAPL', query_start, query_end
        )
        
        if sequence is not None:
            print("Multi-scale sequence summary:")
            summary = sequence.summary()
            for scale, info in summary['scales'].items():
                print(f"  {scale}: {info['n_timesteps']} timesteps")
        
        # Get storage statistics
        stats = await manager.get_storage_stats()
        print("Storage statistics:")
        print(f"  File storage symbols: {stats['file_storage'].get('symbols', 0)}")
        print(f"  Cache storage symbols: {stats['cache_storage'].get('symbols', 0)}")
        
        await manager.close()
    
    # Run example
    asyncio.run(example_usage())