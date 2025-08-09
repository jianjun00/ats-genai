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


from state.universe_state import UniverseStateInterval
from state.instrument_interval import InstrumentInterval
from state.indicator_interval import IndicatorInterval

import gin
from dao.universe_state_interval_dao import UniverseStateIntervalDAO
from dao.instrument_interval_dao import InstrumentIntervalDAO
from dao.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from dao.factor_interval_dao import FactorIntervalDAO

@gin.configurable
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
    
    def __init__(self, env=None, base_path: Optional[str] = None):
        """
        Initialize UniverseStateManager.

        Args:
            env: Environment instance (optional)
            base_path: Base directory for universe state files. If None, uses environment config.
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
        self.logger = logging.getLogger(__name__)
        # Initialize UniverseStateIntervalDAO for interval persistence
        self._interval_dao = UniverseStateIntervalDAO(self.env) if self.env else None
    
    async def save_universe_state(self, universe_data: pd.DataFrame, timestamp: str, metadata: Optional[Dict[str, Any]] = None, partition_cols: Optional[List[str]] = None) -> str:
        """
        Persist universe state interval using UniverseStateIntervalDAO.
        Each interval is saved via the DAO, which handles DB logic.
        """
        if universe_data.empty:
            raise ValueError("Cannot save empty universe state")
        if not self._validate_timestamp_format(timestamp):
            raise ValueError(f"Invalid timestamp format: {timestamp}")
        if self._interval_dao is None:
            raise RuntimeError("UniverseStateIntervalDAO is not initialized (env missing)")
        # Example: Save a single interval for the universe (expand as needed for multiple intervals)
        # This assumes universe_id, duration, start_date_time, end_date_time columns exist or are provided
        # You may need to adapt this logic based on your DataFrame structure and interval semantics
        universe_id = metadata.get('universe_id') if metadata else None
        duration = metadata.get('duration') if metadata else None
        start_date_time = metadata.get('start_date_time') if metadata else None
        end_date_time = metadata.get('end_date_time') if metadata else None
        if not all([universe_id, duration, start_date_time, end_date_time]):
            raise ValueError("Missing required interval metadata: universe_id, duration, start_date_time, end_date_time")
        try:
            self.logger.debug(f"[save_universe_state] Called with metadata={metadata}, universe_data.shape={universe_data.shape}, timestamp={timestamp}")
            # Ensure start_date_time and end_date_time are datetime/date objects
            from datetime import datetime, date
            def parse_dt(val):
                if isinstance(val, (datetime, date)):
                    return val
                if isinstance(val, str):
                    # Try to parse as ISO format
                    try:
                        return datetime.fromisoformat(val)
                    except ValueError:
                        # Try date only
                        try:
                            return datetime.strptime(val, "%Y-%m-%d")
                        except ValueError:
                            raise ValueError(f"Invalid date format: {val}")
                raise TypeError(f"Invalid type for date: {type(val)}")
            start_dt = parse_dt(start_date_time)
            end_dt = parse_dt(end_date_time)
            self.logger.debug(f"[save_universe_state] Creating interval: universe_id={universe_id}, duration={duration}, start_dt={start_dt}, end_dt={end_dt}")
            interval_id = await self._interval_dao.create(
                universe_id=universe_id,
                duration=duration,
                start_date_time=start_dt,
                end_date_time=end_dt
            )
            self.logger.debug(f"[save_universe_state] Created interval_id={interval_id}")
            # --- Persist nested intervals ---
            # 1. Instrument intervals
            instrument_interval_dao = InstrumentIntervalDAO(self.env)
            instrument_interval_id_map = {}
            universe_state = metadata.get('universe_state') if metadata and 'universe_state' in metadata else None
            if universe_state is not None:
                # Persist instrument intervals
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
                # 2. Instrument indicator intervals
                indicator_interval_dao = InstrumentIndicatorIntervalDAO(self.env)
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
                # 3. Factor intervals
                factor_interval_dao = FactorIntervalDAO(self.env)
                for factor_interval in universe_state.factor_intervals:
                    # If factor_name/factor_value are available, persist; else skip
                    if hasattr(factor_interval, 'factor_name') and hasattr(factor_interval, 'factor_value'):
                        await factor_interval_dao.create(
                            universe_state_interval_id=interval_id,
                            factor_name=factor_interval.factor_name,
                            factor_value=factor_interval.factor_value
                        )
            # --- End persist nested intervals ---
            # Optionally update cache (in-memory, not persisted)
            self._update_cache(timestamp, universe_data, metadata or {})
            self.logger.info(f"Saved universe state interval to DB for {timestamp} (interval_id={interval_id}, records={len(universe_data)})")
            return f"db://universe_state_interval/{interval_id}/{timestamp}"
        except Exception as e:
            self.logger.error(f"Failed to save universe state interval to DB: {e}")
            raise IOError(f"Failed to save universe state interval to DB: {e}")
    

    async def addUniverseState(self, duration_to_state: dict, current_time):
        """
        Accepts a dict of TimeDuration -> UniverseState, flattens all states to a DataFrame, and saves using save_universe_state.
        """
        import pandas as pd
        self.logger.debug(f"addUniverseState: Adding UniverseStates for {len(duration_to_state)} durations at {current_time}")
        rows = []
        seen_keys = set()
        long_rows = []
        for duration, universe_state in duration_to_state.items():
            duration_str = duration.get_duration_string()
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
            assert hasattr(universe_state, 'to_dataframe'), (
                f"[addUniverseState] duration={duration} value type={type(universe_state)} does not have .to_dataframe(). Value: {universe_state}")
            df = universe_state.to_dataframe()
            if df.empty:
                self.logger.warning(f"addUniverseState: No data to save for duration {duration} at {current_time}")
                continue
            metadata = {
                "universe_id": getattr(universe_state, "universe_id", None),
                "duration": duration.get_duration_string(),
                "start_date_time": getattr(universe_state, "start_date_time", None),
                "end_date_time": getattr(universe_state, "end_date_time", None),
                "universe_state": universe_state,
            }
            await self.save_universe_state(df, timestamp, metadata=metadata)
            self.logger.debug(f"addUniverseState: Saved universe state for duration {duration} at {timestamp} with {len(df)} records.")
            saved_any = True
        if not saved_any:
            self.logger.warning(f"addUniverseState: No data saved for any duration at {current_time}")

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

    async def load_universe_state(self, timestamp: Optional[str] = None, filters: Optional[List] = None, columns: Optional[List[str]] = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Load universe state from TimescaleDB for the given timestamp (as_of_date).
        Optionally filter by columns.
        """
        import asyncpg
        import pandas as pd
        if timestamp is None:
            timestamp = await self.get_latest_timestamp()
        if not timestamp:
            raise FileNotFoundError("No universe state records found")
        # Check cache
        if use_cache and filters is None and columns is None and timestamp in self._cache:
            self.logger.debug(f"Loading universe state from cache: {timestamp}")
            return self._cache[timestamp].copy()
        as_of_date = timestamp[:8]
        as_of_date = f"{as_of_date[:4]}-{as_of_date[4:6]}-{as_of_date[6:8]}"
        db_url = self.env.get_database_config()['url'] if self.env else os.getenv('TSDB_URL')
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        try:
            async with pool.acquire() as conn:
                col_clause = ', '.join(columns) if columns else '*'
                table_name = self.env.get_table_name('universe_state_interval')
                query = f"SELECT {col_clause} FROM {table_name} WHERE as_of_date = $1"
                records = await conn.fetch(query, as_of_date)
                if not records:
                    raise FileNotFoundError(f"No universe state found for date: {as_of_date}")
                data = pd.DataFrame([dict(r) for r in records])
                if use_cache and filters is None and columns is None:
                    self._update_cache(timestamp, data, {})
                self.logger.info(f"Loaded universe state from DB for {timestamp} ({len(data)} records)")
                return data
        except Exception as e:
            self.logger.error(f"Failed to load universe state from DB: {e}")
            raise IOError(f"Failed to load universe state from DB: {e}")
        finally:
            await pool.close()
    
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
                import inspect, asyncio
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
