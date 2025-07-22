"""
UniverseStateManager - Fast persistence and retrieval of universe state data.

This module handles the storage layer for universe state data using optimized
Parquet format for fast I/O operations, caching, and data format optimization.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import logging
import json
import hashlib
from datetime import datetime, timedelta
import shutil
import os
from dataclasses import dataclass, asdict
from src.config.environment import get_environment


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


class UniverseStateManager:
    """
    Handles fast persistence and retrieval of universe state data.
    
    Focuses on I/O operations, caching, and data format optimization.
    Uses Parquet format for optimal performance with columnar data.
    """
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize UniverseStateManager.
        
        Args:
            base_path: Base directory for universe state files. If None, uses environment config.
        """
        self.env = get_environment()
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
        
        self.logger = logging.getLogger(__name__)
    
    def save_universe_state(self, 
                          universe_data: pd.DataFrame, 
                          timestamp: str,
                          metadata: Optional[Dict[str, Any]] = None,
                          partition_cols: Optional[List[str]] = None) -> str:
        """
        Save universe state with optimized format and compression.
        
        Args:
            universe_data: DataFrame containing universe state data
            timestamp: Timestamp string for versioning (YYYYMMDD_HHMMSS format)
            metadata: Additional metadata to store with the state
            partition_cols: Columns to partition by for faster queries
            
        Returns:
            File path of saved state
            
        Raises:
            ValueError: If timestamp format is invalid or data is empty
            IOError: If file cannot be written
        """
        if universe_data.empty:
            raise ValueError("Cannot save empty universe state")
        
        if not self._validate_timestamp_format(timestamp):
            raise ValueError(f"Invalid timestamp format: {timestamp}. Expected YYYYMMDD_HHMMSS")
        
        file_path = self.states_dir / f"universe_state_{timestamp}.parquet"
        
        try:
            # Optimize data types for better compression
            optimized_data = self._optimize_data_types(universe_data.copy())
            
            # Save with optimal Parquet settings
            optimized_data.to_parquet(
                file_path,
                engine='pyarrow',
                compression='snappy',  # Fast compression/decompression
                index=False,
                partition_cols=partition_cols,
                row_group_size=50000,  # Smaller row groups for faster filtering
                use_dictionary=True,   # Dictionary encoding for categorical data
                write_statistics=True,  # Enable statistics for better filtering
            )
            
            # Create and save metadata
            state_metadata = self._create_metadata(
                timestamp, optimized_data, file_path, metadata or {}
            )
            self._save_metadata(timestamp, state_metadata)
            
            # Update cache
            self._update_cache(timestamp, optimized_data, state_metadata)
            
            self.logger.info(f"Universe state saved: {file_path} ({len(optimized_data)} records)")
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Failed to save universe state: {e}")
            # Clean up partial files
            if file_path.exists():
                file_path.unlink()
            raise IOError(f"Failed to save universe state: {e}")
    
    def load_universe_state(self, 
                          timestamp: Optional[str] = None,
                          filters: Optional[List] = None,
                          columns: Optional[List[str]] = None,
                          use_cache: bool = True) -> pd.DataFrame:
        """
        Load universe state with fast filtering and caching.
        
        Args:
            timestamp: Specific timestamp to load (latest if None)
            filters: PyArrow filters for fast data filtering
            columns: Specific columns to load (all if None)
            use_cache: Whether to use in-memory cache
            
        Returns:
            DataFrame containing universe state data
            
        Raises:
            FileNotFoundError: If no universe states found or timestamp doesn't exist
            IOError: If file cannot be read
        """
        if timestamp is None:
            timestamp = self.get_latest_timestamp()
        
        if not timestamp:
            raise FileNotFoundError("No universe state files found")
        
        # Check cache first
        cache_key = f"{timestamp}_{hash(str(filters))}_{hash(str(columns))}"
        if use_cache and cache_key in self._cache:
            self.logger.debug(f"Loading universe state from cache: {timestamp}")
            return self._cache[cache_key].copy()
        
        file_path = self.states_dir / f"universe_state_{timestamp}.parquet"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Universe state not found: {timestamp}")
        
        try:
            # Fast filtered reading with column pruning
            data = pd.read_parquet(
                file_path,
                engine='pyarrow',
                filters=filters,
                columns=columns,
                use_threads=True  # Parallel reading
            )
            
            # Update cache if using full data load
            if use_cache and filters is None and columns is None:
                metadata = self.get_state_metadata(timestamp)
                self._update_cache(timestamp, data, metadata)
            
            self.logger.debug(f"Loaded universe state: {timestamp} ({len(data)} records)")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load universe state {timestamp}: {e}")
            raise IOError(f"Failed to load universe state {timestamp}: {e}")
    
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
                    self.logger.info(f"Removed old universe state: {timestamp}")
                    
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
        self.logger.info("Universe state cache cleared")
    
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
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.5:  # If less than 50% unique values
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
