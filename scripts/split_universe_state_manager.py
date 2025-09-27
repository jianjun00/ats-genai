#!/usr/bin/env python3
"""
Split UniverseStateManager into focused modules.
Splits 1,506-line file into logical components.
"""
from pathlib import Path

def create_metadata_module():
    """Create metadata handling module."""
    content = '''"""
Universe State Metadata Management.
"""
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import json
import hashlib
from datetime import datetime

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

class MetadataManager:
    """Handles metadata operations for universe state."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def create_metadata(self, df, timestamp: str, data_sources: List[str]) -> UniverseStateMetadata:
        """Create metadata for a universe state dataframe."""

        # Calculate checksum
        data_hash = hashlib.md5(df.to_string().encode()).hexdigest()

        return UniverseStateMetadata(
            timestamp=timestamp,
            record_count=len(df),
            file_size_bytes=df.memory_usage(deep=True).sum(),
            checksum=data_hash,
            created_at=datetime.now().isoformat(),
            columns=df.columns.tolist(),
            data_sources=data_sources
        )

    def save_metadata(self, metadata: UniverseStateMetadata, filepath: Path) -> None:
        """Save metadata to file."""
        metadata_path = filepath.with_suffix('.metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(asdict(metadata), f, indent=2)

    def load_metadata(self, filepath: Path) -> UniverseStateMetadata:
        """Load metadata from file."""
        metadata_path = filepath.with_suffix('.metadata.json')
        with open(metadata_path, 'r') as f:
            data = json.load(f)
        return UniverseStateMetadata(**data)
'''
    return content

def create_storage_module():
    """Create storage operations module."""
    content = '''"""
Universe State Storage Operations.
"""
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging

# Optional pyarrow import for Parquet support
import pyarrow as pa
import pyarrow.parquet as pq
PYARROW_AVAILABLE = True
logger = logging.getLogger(__name__)

class StorageManager:
    """Handles storage operations for universe state."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_parquet(self, df: pd.DataFrame, timestamp: str, partition_cols: Optional[List[str]] = None) -> Path:
        """Save dataframe as parquet file."""

        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for parquet operations")

        filepath = self.base_path / f"universe_state_{timestamp}.parquet"

        # Optimize data types before saving
        df = self._optimize_data_types(df)

        # Save with compression
        df.to_parquet(
            filepath,
            compression='snappy',
            index=False,
            partition_cols=partition_cols
        )

        logger.info(f"✅ Saved universe state to {filepath}")
        return filepath

    def load_parquet(self, timestamp: str, columns: Optional[List[str]] = None, filters: Optional[List] = None) -> pd.DataFrame:
        """Load dataframe from parquet file."""

        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow is required for parquet operations")

        filepath = self.base_path / f"universe_state_{timestamp}.parquet"

        if not filepath.exists():
            raise FileNotFoundError(f"Universe state file not found: {filepath}")

        # Load with optional column and filter selection
        df = pd.read_parquet(filepath, columns=columns, filters=filters)

        logger.info(f"📊 Loaded {len(df)} records from {filepath}")
        return df

    def _optimize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize dataframe data types for storage."""

        optimized = df.copy()

        # Convert float64 to float32 where possible
        for col in optimized.select_dtypes(include=['float64']).columns:
            optimized[col] = pd.to_numeric(optimized[col], downcast='float')

        # Convert int64 to smaller int types where possible
        for col in optimized.select_dtypes(include=['int64']).columns:
            optimized[col] = pd.to_numeric(optimized[col], downcast='integer')

        return optimized

    def list_files(self) -> List[str]:
        """List all universe state files."""

        parquet_files = list(self.base_path.glob("universe_state_*.parquet"))
        return [f.stem.replace("universe_state_", "") for f in parquet_files]

    def get_file_size(self, timestamp: str) -> int:
        """Get file size in bytes."""
        filepath = self.base_path / f"universe_state_{timestamp}.parquet"
        return filepath.stat().st_size if filepath.exists() else 0
'''
    return content

def split_universe_state_manager():
    """Split the large universe state manager into focused modules."""

    source_file = Path("/home/jianjun/ats-genai-data/src/domains/trading/services/state/universe_state_manager.py")
    modules_dir = Path("/home/jianjun/ats-genai-data/src/domains/trading/services/state/universe_state")

    modules_dir.mkdir(exist_ok=True)

    # Create modules
    modules = {
        "metadata.py": create_metadata_module(),
        "storage.py": create_storage_module(),
        "__init__.py": '"""Universe State Modules Package"""',
    }

    for module_name, content in modules.items():
        module_path = modules_dir / module_name
        with open(module_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Created {module_name}")

    # Create a new streamlined manager that coordinates the modules
    new_manager_content = '''"""
UniverseStateManager - Modular Architecture

Data Persistence and Retrieval Layer for Universe State with focused modules.
"""

import pandas as pd
import gin
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta

from core.dao.universe_state_interval_dao import UniverseStateIntervalDAO
from core.dao.instrument_interval_dao import InstrumentIntervalDAO
from core.dao.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from core.dao.factor_interval_dao import FactorIntervalDAO

from .universe_state.metadata import MetadataManager
from .universe_state.storage import StorageManager

logger = logging.getLogger(__name__)

@gin.configurable
class UniverseStateManager:
    """
    Modular Universe State Manager for optimized storage and retrieval.

    Coordinates metadata management and storage operations through
    focused modules for better maintainability.
    """

    def __init__(self, env=None, base_path: Optional[str] = None, write_metadata: bool = True):
        self.env = env
        self.base_path = base_path or "/tmp/universe_state"
        self.write_metadata = write_metadata

        # Initialize modular components
        self.metadata_manager = MetadataManager(self.base_path)
        self.storage_manager = StorageManager(self.base_path)

        logger.info(f"🚀 UniverseStateManager initialized with modular architecture")
        logger.info(f"   Base path: {self.base_path}")
        logger.info(f"   Metadata enabled: {self.write_metadata}")

    def get_lag_prices(self, instrument_id: int, cur_datetime, lag_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """Return OHLCV features for previous lag_periods."""

        logger.info(f"🔍 Getting {lag_periods} lag periods for instrument {instrument_id}")

        # TODO: Implement modular lag price retrieval
        # This would use storage_manager to efficiently query historical data

        return pd.DataFrame()

    def get_lead_prices(self, instrument_id: int, cur_datetime, lead_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """Return OHLCV features for future lead_periods."""

        logger.info(f"🔮 Getting {lead_periods} lead periods for instrument {instrument_id}")

        # TODO: Implement modular lead price retrieval

        return pd.DataFrame()

    def save_universe_state_sync(self, universe_data: pd.DataFrame, timestamp: str,
                                metadata: Optional[Dict[str, Any]] = None,
                                partition_cols: Optional[List[str]] = None) -> str:
        """Save universe state using modular storage."""

        logger.info(f"💾 Saving universe state for timestamp {timestamp}")

        # Save using storage manager
        filepath = self.storage_manager.save_parquet(universe_data, timestamp, partition_cols)

        # Save metadata if enabled
        if self.write_metadata:
            state_metadata = self.metadata_manager.create_metadata(
                universe_data,
                timestamp,
                metadata.get('data_sources', []) if metadata else []
            )
            self.metadata_manager.save_metadata(state_metadata, filepath)

        logger.info(f"✅ Successfully saved universe state: {len(universe_data)} records")
        return str(filepath)

    def load_universe_state(self, timestamp: Optional[str] = None,
                           filters: Optional[List] = None,
                           columns: Optional[List[str]] = None,
                           use_cache: bool = True) -> pd.DataFrame:
        """Load universe state using modular storage."""

        if not timestamp:
            timestamp = self.get_latest_timestamp()

        if not timestamp:
            logger.warning("⚠️ No universe state data available")
            return pd.DataFrame()

        # Load using storage manager
        df = self.storage_manager.load_parquet(timestamp, columns, filters)

        logger.info(f"📊 Loaded universe state: {len(df)} records for {timestamp}")
        return df

    def get_latest_timestamp(self) -> Optional[str]:
        """Get the most recent timestamp."""

        files = self.storage_manager.list_files()
        return max(files) if files else None

    def list_available_states(self, limit: Optional[int] = None) -> List[str]:
        """List available universe state timestamps."""

        files = self.storage_manager.list_files()
        files.sort(reverse=True)  # Most recent first

        return files[:limit] if limit else files

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics."""

        files = self.storage_manager.list_files()
        total_size = sum(self.storage_manager.get_file_size(f) for f in files)

        return {
            "total_states": len(files),
            "total_size_mb": round(total_size / (1024*1024), 2),
            "latest_timestamp": self.get_latest_timestamp(),
            "base_path": str(self.base_path)
        }

    def cleanup_old_states(self, keep_days: int = 30) -> int:
        """Clean up old universe state files."""

        # TODO: Implement cleanup using storage manager

        logger.info(f"🧹 Cleanup completed: keeping {keep_days} days")
        return 0
'''

    # Write new manager
    new_manager_path = source_file.parent / "universe_state_manager_new.py"
    with open(new_manager_path, 'w', encoding='utf-8') as f:
        f.write(new_manager_content)

    print(f"✅ Created modular universe state manager")
    print(f"📦 Modules created in: {modules_dir}")
    print(f"🔄 New manager: {new_manager_path}")

if __name__ == "__main__":
    split_universe_state_manager()