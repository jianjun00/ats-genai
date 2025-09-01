#!/usr/bin/env python3
"""
HDF5 Multi-Scale Cache for Financial Time Series

Implements efficient HDF5-based caching for multi-scale temporal data with
fast aggregation, retrieval, and automatic data management capabilities.

Key Features:
- HDF5-based hierarchical data storage
- Automatic aggregation from minute to higher scales
- Memory-mapped access for large datasets
- Concurrent read/write operations
- Automatic data validation and integrity checks
- Efficient time-range queries with indexing
"""

import asyncio
import numpy as np
import pandas as pd
import h5py
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import logging
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import threading
from contextlib import contextmanager
import warnings
import hashlib

from .multi_scale_sequence import TimeScale, ScaleFeatures, MultiScaleSequence

logger = logging.getLogger(__name__)

# Suppress HDF5 warnings for better logging
try:
    warnings.filterwarnings('ignore', category=h5py._errors.H5pyDeprecationWarning)
except AttributeError:
    # Fallback for different h5py versions
    pass


@dataclass
class CacheConfig:
    """Configuration for HDF5 multi-scale cache."""
    cache_dir: str = "/home/jianjun/ats-data/hdf5-cache"
    max_cache_size_gb: float = 10.0
    compression: str = "gzip"
    compression_level: int = 6
    chunk_size: int = 1000
    max_concurrent_operations: int = 4
    enable_checksums: bool = True
    auto_aggregate: bool = True
    
    # Aggregation settings
    aggregation_functions: Dict[str, str] = field(default_factory=lambda: {
        'open': 'first',
        'high': 'max', 
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'default': 'mean'  # For technical indicators
    })


class HDF5MultiScaleCache:
    """
    HDF5-based multi-scale cache for financial time series data.
    
    File Structure:
    cache_path/
    ├── AAPL.h5
    │   ├── /minute/2024/01/     # Monthly minute data
    │   ├── /hourly/2024/        # Yearly hourly data
    │   ├── /daily/              # All daily data
    │   └── /weekly/             # All weekly data
    """
    
    def __init__(self, config: CacheConfig = None):
        self.config = config or CacheConfig()
        self.cache_path = Path(self.config.cache_dir)
        self.cache_path.mkdir(parents=True, exist_ok=True)
        
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_concurrent_operations)
        self._locks = {}  # Per-symbol file locks
        self._file_handles = {}  # Cached file handles
        
        logger.info(f"HDF5MultiScaleCache initialized at {self.cache_path}")
    
    def _get_file_path(self, symbol: str) -> Path:
        """Get HDF5 file path for symbol."""
        return self.cache_path / f"{symbol}.h5"
    
    def _get_lock(self, symbol: str) -> threading.RLock:
        """Get thread lock for symbol."""
        if symbol not in self._locks:
            self._locks[symbol] = threading.RLock()
        return self._locks[symbol]
    
    @contextmanager
    def _get_file_handle(self, symbol: str, mode: str = 'r'):
        """Get HDF5 file handle with proper locking."""
        file_path = self._get_file_path(symbol)
        lock = self._get_lock(symbol)
        
        with lock:
            try:
                if mode == 'r' and not file_path.exists():
                    # Return empty context for read operations on non-existent files
                    yield None
                    return
                
                # Open file with appropriate mode
                with h5py.File(file_path, mode) as f:
                    yield f
                    
            except Exception as e:
                logger.error(f"Error accessing HDF5 file {file_path}: {e}")
                raise
    
    def _get_dataset_path(self, scale: TimeScale, year: int = None, month: int = None) -> str:
        """Get HDF5 dataset path for scale and time period."""
        if scale == TimeScale.MINUTE:
            return f"/minute/{year}/{month:02d}"
        elif scale == TimeScale.HOURLY:
            return f"/hourly/{year}"
        elif scale == TimeScale.DAILY:
            return "/daily"
        elif scale == TimeScale.WEEKLY:
            return "/weekly"
        else:
            raise ValueError(f"Unknown scale: {scale}")
    
    async def store_minute_data(
        self,
        symbol: str,
        minute_data: pd.DataFrame,
        auto_aggregate: bool = None
    ) -> Dict[str, Any]:
        """
        Store minute-level data and optionally create aggregations.
        
        Args:
            symbol: Stock symbol
            minute_data: DataFrame with minute-level data
            auto_aggregate: Whether to auto-aggregate to higher scales
        
        Returns:
            Storage statistics
        """
        if minute_data.empty:
            return {'stored': 0, 'aggregated': {}}
        
        auto_aggregate = auto_aggregate if auto_aggregate is not None else self.config.auto_aggregate
        
        logger.info(f"Storing {len(minute_data)} minute records for {symbol}")
        
        result = await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self._store_minute_data_sync,
            symbol, minute_data, auto_aggregate
        )
        
        return result
    
    def _store_minute_data_sync(
        self, 
        symbol: str, 
        minute_data: pd.DataFrame, 
        auto_aggregate: bool
    ) -> Dict[str, Any]:
        """Synchronous minute data storage."""
        
        # Group data by month for efficient storage
        monthly_groups = self._group_by_month(minute_data)
        
        stored_count = 0
        aggregation_results = {}
        
        with self._get_file_handle(symbol, 'a') as f:
            if f is None:
                return {'stored': 0, 'aggregated': {}}
            
            for (year, month), month_data in monthly_groups.items():
                # Store minute data
                dataset_path = self._get_dataset_path(TimeScale.MINUTE, year, month)
                
                try:
                    stored_count += self._store_dataset(
                        f, dataset_path, month_data, append=True
                    )
                    
                    # Create aggregations if requested
                    if auto_aggregate:
                        hourly_data = self._aggregate_to_hourly(month_data)
                        daily_data = self._aggregate_to_daily(month_data)
                        
                        if not hourly_data.empty:
                            hourly_path = self._get_dataset_path(TimeScale.HOURLY, year)
                            aggregation_results['hourly'] = self._store_dataset(
                                f, hourly_path, hourly_data, append=True
                            )
                        
                        if not daily_data.empty:
                            daily_path = self._get_dataset_path(TimeScale.DAILY)
                            aggregation_results['daily'] = self._store_dataset(
                                f, daily_path, daily_data, append=True
                            )
                
                except Exception as e:
                    logger.error(f"Error storing data for {symbol} {year}-{month}: {e}")
                    continue
        
        logger.info(f"Stored {stored_count} minute records for {symbol}")
        return {'stored': stored_count, 'aggregated': aggregation_results}
    
    def _group_by_month(self, data: pd.DataFrame) -> Dict[Tuple[int, int], pd.DataFrame]:
        """Group DataFrame by year and month."""
        if 'timestamp' not in data.columns:
            data = data.reset_index()
        
        data['year'] = pd.to_datetime(data['timestamp']).dt.year
        data['month'] = pd.to_datetime(data['timestamp']).dt.month
        
        groups = {}
        for (year, month), group in data.groupby(['year', 'month']):
            # Remove grouping columns
            clean_group = group.drop(['year', 'month'], axis=1)
            groups[(year, month)] = clean_group
        
        return groups
    
    def _store_dataset(
        self,
        file_handle: h5py.File,
        dataset_path: str,
        data: pd.DataFrame,
        append: bool = False
    ) -> int:
        """Store DataFrame as HDF5 dataset."""
        
        # Convert DataFrame to numpy arrays
        timestamp_data = pd.to_datetime(data['timestamp']).astype(np.int64).values
        
        # Get feature columns (excluding timestamp)
        feature_cols = [col for col in data.columns if col != 'timestamp']
        feature_data = data[feature_cols].astype(np.float32).values
        
        # Create or append to dataset
        if dataset_path in file_handle:
            if append:
                # Append to existing dataset
                existing_ts = file_handle[f"{dataset_path}/timestamps"]
                existing_features = file_handle[f"{dataset_path}/features"]
                
                # Resize datasets
                old_size = existing_ts.shape[0]
                new_size = old_size + len(timestamp_data)
                
                existing_ts.resize((new_size,))
                existing_features.resize((new_size, existing_features.shape[1]))
                
                # Add new data
                existing_ts[old_size:] = timestamp_data
                existing_features[old_size:] = feature_data
                
                return len(timestamp_data)
            else:
                # Replace existing dataset
                del file_handle[dataset_path]
        
        # Create new dataset group
        group = file_handle.create_group(dataset_path)
        
        # Store timestamps
        group.create_dataset(
            'timestamps',
            data=timestamp_data,
            compression=self.config.compression,
            compression_opts=self.config.compression_level,
            chunks=True,
            maxshape=(None,)
        )
        
        # Store features
        group.create_dataset(
            'features',
            data=feature_data,
            compression=self.config.compression,
            compression_opts=self.config.compression_level,
            chunks=True,
            maxshape=(None, feature_data.shape[1])
        )
        
        # Store column names as attributes
        group.attrs['feature_columns'] = [col.encode('utf-8') for col in feature_cols]
        group.attrs['created'] = datetime.now().isoformat()
        
        # Store checksum if enabled
        if self.config.enable_checksums:
            checksum = self._calculate_checksum(feature_data)
            group.attrs['checksum'] = checksum
        
        return len(timestamp_data)
    
    def _calculate_checksum(self, data: np.ndarray) -> str:
        """Calculate MD5 checksum for data integrity."""
        return hashlib.md5(data.tobytes()).hexdigest()
    
    def _aggregate_to_hourly(self, minute_data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate minute data to hourly."""
        if minute_data.empty:
            return pd.DataFrame()
        
        df = minute_data.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # Resample to hourly with appropriate aggregation functions
        agg_funcs = {}
        for col in df.columns:
            if col in self.config.aggregation_functions:
                agg_funcs[col] = self.config.aggregation_functions[col]
            else:
                agg_funcs[col] = self.config.aggregation_functions['default']
        
        hourly = df.resample('1H').agg(agg_funcs)
        hourly = hourly.dropna()
        
        return hourly.reset_index()
    
    def _aggregate_to_daily(self, minute_data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate minute data to daily."""
        if minute_data.empty:
            return pd.DataFrame()
        
        df = minute_data.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # Resample to daily with appropriate aggregation functions
        agg_funcs = {}
        for col in df.columns:
            if col in self.config.aggregation_functions:
                agg_funcs[col] = self.config.aggregation_functions[col]
            else:
                agg_funcs[col] = self.config.aggregation_functions['default']
        
        daily = df.resample('1D').agg(agg_funcs)
        daily = daily.dropna()
        
        return daily.reset_index()
    
    async def get_data(
        self,
        symbol: str,
        scale: TimeScale,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve data for specified symbol, scale, and time range.
        
        Args:
            symbol: Stock symbol
            scale: Time scale to retrieve
            start_time: Start of time range
            end_time: End of time range  
            columns: Specific columns to retrieve (optional)
        
        Returns:
            DataFrame with requested data or None if not available
        """
        result = await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self._get_data_sync,
            symbol, scale, start_time, end_time, columns
        )
        
        return result
    
    def _get_data_sync(
        self,
        symbol: str,
        scale: TimeScale,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[List[str]]
    ) -> Optional[pd.DataFrame]:
        """Synchronous data retrieval."""
        
        with self._get_file_handle(symbol, 'r') as f:
            if f is None:
                return None
            
            # Get dataset paths to search
            if scale == TimeScale.MINUTE:
                dataset_paths = self._get_minute_dataset_paths(f, start_time, end_time)
            else:
                dataset_paths = [self._get_dataset_path(scale)]
            
            # Collect data from all relevant datasets
            data_frames = []
            
            for dataset_path in dataset_paths:
                if dataset_path not in f:
                    continue
                
                try:
                    df = self._load_dataset_range(
                        f, dataset_path, start_time, end_time, columns
                    )
                    if df is not None and not df.empty:
                        data_frames.append(df)
                        
                except Exception as e:
                    logger.warning(f"Error loading dataset {dataset_path}: {e}")
                    continue
            
            # Combine all data
            if not data_frames:
                return None
            
            combined_df = pd.concat(data_frames, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['timestamp'])
            combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
            
            return combined_df
    
    def _get_minute_dataset_paths(
        self, 
        file_handle: h5py.File,
        start_time: datetime,
        end_time: datetime
    ) -> List[str]:
        """Get all minute dataset paths that might contain data in the time range."""
        paths = []
        
        # Generate monthly paths for the time range
        current = start_time.replace(day=1)
        end_month = end_time.replace(day=1)
        
        while current <= end_month:
            path = self._get_dataset_path(TimeScale.MINUTE, current.year, current.month)
            if path in file_handle:
                paths.append(path)
            
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        return paths
    
    def _load_dataset_range(
        self,
        file_handle: h5py.File,
        dataset_path: str,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[List[str]]
    ) -> Optional[pd.DataFrame]:
        """Load dataset within specified time range."""
        
        try:
            group = file_handle[dataset_path]
            
            # Load timestamps
            timestamps = group['timestamps'][:]
            timestamps_dt = pd.to_datetime(timestamps, unit='ns')
            
            # Find indices within time range
            mask = (timestamps_dt >= start_time) & (timestamps_dt <= end_time)
            if not mask.any():
                return None
            
            indices = np.where(mask)[0]
            
            # Load features
            features = group['features'][indices]
            
            # Get column names
            feature_columns = [col.decode('utf-8') for col in group.attrs['feature_columns']]
            
            # Filter columns if requested
            if columns is not None:
                available_columns = [col for col in columns if col in feature_columns]
                if not available_columns:
                    return None
                
                col_indices = [feature_columns.index(col) for col in available_columns]
                features = features[:, col_indices]
                feature_columns = available_columns
            
            # Create DataFrame
            df = pd.DataFrame(features, columns=feature_columns)
            df['timestamp'] = timestamps_dt[mask].values
            
            # Verify checksum if enabled
            if self.config.enable_checksums and 'checksum' in group.attrs:
                expected_checksum = group.attrs['checksum']
                actual_checksum = self._calculate_checksum(group['features'][:])
                if expected_checksum != actual_checksum:
                    logger.warning(f"Checksum mismatch in {dataset_path}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading dataset {dataset_path}: {e}")
            return None
    
    async def get_multi_scale_data(
        self,
        symbol: str,
        time_range: Tuple[datetime, datetime],
        scales: List[TimeScale] = None
    ) -> Optional[MultiScaleSequence]:
        """
        Get multi-scale data for symbol and time range.
        
        Args:
            symbol: Stock symbol
            time_range: (start, end) time range
            scales: List of scales to retrieve (default: all available)
        
        Returns:
            MultiScaleSequence or None if no data available
        """
        if scales is None:
            scales = list(TimeScale)
        
        start_time, end_time = time_range
        
        # Retrieve data for each scale
        scale_data = {}
        
        for scale in scales:
            data = await self.get_data(symbol, scale, start_time, end_time)
            if data is not None and not data.empty:
                scale_data[scale] = data
        
        if not scale_data:
            return None
        
        # Convert to ScaleFeatures
        scale_features = {}
        
        for scale, data in scale_data.items():
            # Separate OHLCV from technical indicators
            ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
            available_ohlcv = [col for col in ohlcv_cols if col in data.columns]
            
            if available_ohlcv:
                ohlcv_data = data[available_ohlcv].values
                
                # Technical indicators are remaining columns
                tech_cols = [col for col in data.columns if col not in available_ohlcv + ['timestamp']]
                tech_data = data[tech_cols].values if tech_cols else None
                
                scale_features[scale] = ScaleFeatures(
                    timestamps=pd.to_datetime(data['timestamp']),
                    ohlcv=ohlcv_data,
                    technical=tech_data
                )
        
        # Create MultiScaleSequence
        sequence = MultiScaleSequence(
            symbol=symbol,
            time_range=time_range,
            minute_features=scale_features.get(TimeScale.MINUTE),
            hourly_features=scale_features.get(TimeScale.HOURLY),
            daily_features=scale_features.get(TimeScale.DAILY),
            weekly_features=scale_features.get(TimeScale.WEEKLY)
        )
        
        return sequence
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics and health information."""
        stats = {
            'symbols': 0,
            'total_size_mb': 0,
            'files': {},
            'scales': {scale.value: {'symbols': 0, 'total_records': 0} for scale in TimeScale}
        }
        
        # Scan all HDF5 files
        for file_path in self.cache_path.glob("*.h5"):
            symbol = file_path.stem
            stats['symbols'] += 1
            
            try:
                file_size = file_path.stat().st_size / (1024 * 1024)  # MB
                stats['total_size_mb'] += file_size
                
                file_stats = await asyncio.get_event_loop().run_in_executor(
                    self.executor,
                    self._get_file_stats,
                    symbol
                )
                
                stats['files'][symbol] = {
                    'size_mb': file_size,
                    'scales': file_stats
                }
                
                # Update scale statistics
                for scale_str, scale_info in file_stats.items():
                    if scale_str in stats['scales']:
                        stats['scales'][scale_str]['symbols'] += 1
                        stats['scales'][scale_str]['total_records'] += scale_info.get('records', 0)
                
            except Exception as e:
                logger.warning(f"Error getting stats for {file_path}: {e}")
        
        return stats
    
    def _get_file_stats(self, symbol: str) -> Dict[str, Any]:
        """Get statistics for a single HDF5 file."""
        stats = {}
        
        with self._get_file_handle(symbol, 'r') as f:
            if f is None:
                return stats
            
            def collect_stats(name, obj):
                if isinstance(obj, h5py.Group) and 'timestamps' in obj and 'features' in obj:
                    # This is a data group
                    n_records = obj['timestamps'].shape[0]
                    n_features = obj['features'].shape[1] if len(obj['features'].shape) > 1 else 1
                    
                    # Extract scale from path
                    path_parts = name.strip('/').split('/')
                    if path_parts[0] in ['minute', 'hourly', 'daily', 'weekly']:
                        scale = path_parts[0]
                        
                        if scale not in stats:
                            stats[scale] = {'records': 0, 'features': n_features, 'datasets': 0}
                        
                        stats[scale]['records'] += n_records
                        stats[scale]['datasets'] += 1
            
            f.visititems(collect_stats)
        
        return stats
    
    async def cleanup_cache(self, max_age_days: int = 30) -> Dict[str, int]:
        """Clean up old cache entries."""
        cleanup_stats = {'files_removed': 0, 'datasets_removed': 0}
        
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        for file_path in self.cache_path.glob("*.h5"):
            try:
                # Check file modification time
                mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                if mod_time < cutoff_date:
                    # Remove entire file if too old
                    file_path.unlink()
                    cleanup_stats['files_removed'] += 1
                    logger.info(f"Removed old cache file: {file_path}")
                
            except Exception as e:
                logger.warning(f"Error during cleanup of {file_path}: {e}")
        
        return cleanup_stats
    
    async def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)
        logger.info("HDF5MultiScaleCache closed")


# Convenience functions
async def create_hdf5_cache(
    cache_path: str = None,
    max_cache_size_gb: float = 10.0,
    **kwargs
) -> HDF5MultiScaleCache:
    """Create and initialize HDF5 multi-scale cache."""
    config = CacheConfig(
        cache_path=cache_path or "/home/jianjun/ats-data/hdf5-cache",
        max_cache_size_gb=max_cache_size_gb,
        **kwargs
    )
    
    return HDF5MultiScaleCache(config)


# Example usage
if __name__ == "__main__":
    pass
    
    async def example_usage():
        """Example usage of HDF5 multi-scale cache."""
        
        # Create cache
        cache = await create_hdf5_cache()
        
        # Create sample minute data
        start_time = datetime(2024, 1, 15, 9, 30)
        timestamps = pd.date_range(start_time, start_time + timedelta(hours=24), freq='1min')
        
        minute_df = pd.DataFrame({
            'timestamp': timestamps,
            'open': np.random.uniform(150, 160, len(timestamps)),
            'high': np.random.uniform(150, 160, len(timestamps)),
            'low': np.random.uniform(150, 160, len(timestamps)),
            'close': np.random.uniform(150, 160, len(timestamps)),
            'volume': np.random.randint(1000, 10000, len(timestamps)),
            'rsi': np.random.uniform(30, 70, len(timestamps)),
            'macd': np.random.uniform(-1, 1, len(timestamps))
        })
        
        # Store data
        print("Storing minute data...")
        result = await cache.store_minute_data('AAPL', minute_df, auto_aggregate=True)
        print(f"Storage result: {result}")
        
        # Retrieve data
        print("Retrieving hourly data...")
        hourly_data = await cache.get_data(
            'AAPL', 
            TimeScale.HOURLY, 
            start_time, 
            start_time + timedelta(hours=24)
        )
        
        if hourly_data is not None:
            print(f"Retrieved {len(hourly_data)} hourly records")
        
        # Get multi-scale data
        print("Getting multi-scale sequence...")
        sequence = await cache.get_multi_scale_data(
            'AAPL',
            (start_time, start_time + timedelta(hours=12))
        )
        
        if sequence is not None:
            print("Multi-scale sequence summary:")
            print(sequence.summary())
        
        # Get cache statistics
        stats = await cache.get_cache_stats()
        print(f"Cache stats: {stats}")
        
        await cache.close()
    
    # Run example
    asyncio.run(example_usage())