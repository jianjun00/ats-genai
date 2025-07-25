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
from config.environment import get_environment


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
    def handleEnd(self, current_time, saved_dir=None):
        """
        Save the full universe state DataFrame under saved_dir (or base_path if None) with a timestamp based on current_time.
        """
        import logging
        logger = self.logger if hasattr(self, 'logger') else logging.getLogger(__name__)
        # Explicitly initialize saved_dir at the very start
        local_saved_dir = saved_dir
        logger.debug(f"handleEnd: ENTRY at {current_time}, saved_dir={local_saved_dir}")
        print(f"handleEnd: Saving full universe state at {current_time}, saved_dir: {local_saved_dir}")
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
        logger.info(f"handleEnd: Saved full universe state to {out_file} with {len(full_df)} records.")
        logger.debug(f"handleEnd: EXIT at {current_time}")
    
    def __init__(self, env=None, base_path: Optional[str] = None):
        """
        Initialize UniverseStateManager.

        Args:
            env: Environment instance (optional)
            base_path: Base directory for universe state files. If None, uses environment config.
        """
        self.env = env or get_environment()
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
        Writes a Parquet file to self.states_dir/universe_state_{timestamp}.parquet.
        Also generates and saves corresponding metadata JSON file.
        """
        self.states_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.states_dir / f"universe_state_{timestamp}.parquet"

        # Validate input
        if universe_data.empty:
            raise ValueError("Cannot save empty universe state")
        if not self._validate_timestamp_format(timestamp):
            raise ValueError(f"Invalid timestamp format: {timestamp}")

        try:
            universe_data.to_parquet(file_path, index=False)
        except Exception as e:
            raise IOError(f"Failed to save universe state: {e}")

        safe_metadata = metadata if metadata is not None else {}
        meta_obj = self._create_metadata(
            timestamp=timestamp,
            data=universe_data,
            file_path=file_path,
            additional_metadata=safe_metadata
        )
        try:
            self._save_metadata(timestamp, meta_obj)
        except Exception as e:
            raise IOError(f"Failed to save universe state metadata: {e}")
        # Update cache after successful save
        self._update_cache(timestamp, universe_data, meta_obj)
        return str(file_path)
    
    def addIntervals(self, intervals: dict, current_time):
        """
        Accepts a dict of duration string -> UniverseInterval, flattens to DataFrame, and saves using save_universe_state.
        """
        import pandas as pd
        rows = []
        for duration_str, universe_interval in intervals.items():
            self.logger.info(f"addIntervals: Adding intervals for {duration_str} at {current_time}")
            for inst_id, inst_interval in universe_interval.instrument_intervals.items():
                self.logger.info(f"addIntervals: Adding row for instr:{inst_id}, interval:{inst_interval}")
                row = {
                    'instrument_id': inst_id,
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
                if hasattr(inst_interval, 'symbol'):
                    row['symbol'] = inst_interval.symbol
                rows.append(row)
        df = pd.DataFrame(rows)
        if df.empty:
            self.logger.warning(f"addIntervals: No intervals to save at {current_time}")
            return
        timestamp = current_time.strftime('%Y%m%d_%H%M%S')
        self.save_universe_state(df, timestamp)
        self.logger.info(f"addIntervals: Saved universe state for {timestamp} with {len(df)} records.")

    def update_for_sod(self, runner, current_time):
        """
        Start-of-day hook for UniverseStateManager. Implement flushing, finalization, or logging if needed.
        """
        self.logger.info(f"UniverseStateManager.update_for_sod called at {current_time}")
        # Add EOD logic if needed

    def update_for_eod(self, runner, current_time):
        """
        End-of-day hook for UniverseStateManager. Implement flushing, finalization, or logging if needed.
        """
        self.logger.info(f"UniverseStateManager.update_for_eod called at {current_time}")
        # Add EOD logic if needed

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
        if use_cache and filters is None and columns is None and timestamp in self._cache:
            self.logger.debug(f"Loading universe state from cache: {timestamp}")
            return self._cache[timestamp].copy()
        
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
    from state.universe_state_builder import UniverseStateBuilder
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
            from config.environment import get_environment
            import asyncpg
            import asyncio
            env = get_environment()
            print(f"DEBUG (CLI): DB URL: {env.get_database_url()}")
            async def print_table_schema():
                pool = await asyncpg.create_pool(env.get_database_url())
                async with pool.acquire() as conn:
                    for table in ["instrument_polygon", "instruments"]:
                        tn = env.get_table_name(table)
                        schema = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1", tn)
                        print(f"DEBUG (CLI): {tn} columns:", schema)
                        row = await conn.fetchrow(f"SELECT * FROM {tn} LIMIT 1")
                        if row:
                            print(f"DEBUG (CLI): {tn} sample row:", dict(row))
                        else:
                            print(f"DEBUG (CLI): {tn} sample row: <empty>")
                await pool.close()
            asyncio.run(print_table_schema())
        except Exception as e:
            print(f"DEBUG (CLI): Failed to print DB schema: {e}")
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
            from state.universe_state_builder import UniverseStateBuilder
            BuilderClass = UniverseStateBuilder
        # TODO: Load actual Universe object by universe_id
        universe = None  # Replace with actual loading logic
        builder = BuilderClass(universe=universe, state_manager=manager)
        cur_date = start_date
        while cur_date <= end_date:
            date_str = cur_date.strftime("%Y-%m-%d")
            try:
                # Build and save universe state for this date
                # Support async build_universe_state if present
                import inspect, asyncio
                build_fn = builder.build_universe_state
                if inspect.iscoroutinefunction(build_fn):
                    df = asyncio.run(build_fn(date_str))
                else:
                    df = build_fn(date_str)
                timestamp = cur_date.strftime("%Y%m%d_000000")
                manager.save_universe_state(df, timestamp=timestamp)
                print(f"Built and saved universe state for {date_str}")
            except Exception as e:
                print(f"Failed to build/save for {date_str}: {e}")
            cur_date += timedelta(days=1)
        print("Build complete.")

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
