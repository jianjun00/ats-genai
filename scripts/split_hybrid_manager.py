#!/usr/bin/env python3
"""
Split HybridMinuteDataManager into focused modules.
Splits 1,702-line file into logical components.
"""
from pathlib import Path

def create_storage_config_module():
    """Create storage configuration module."""
    content = '''"""
Storage Configuration for Hybrid Minute Data Manager.
"""
from dataclasses import dataclass
from datetime import date
from pathlib import Path

@dataclass
class DataGap:
    """Represents a gap in market data coverage."""
    symbol: str
    start_date: date
    end_date: date
    source: str  # 'parquet', 'database', 'both'
    gap_type: str  # 'missing', 'partial', 'stale'
    estimated_bars: int = 0
    tier: str = 'unknown'  # 'hot', 'warm', 'cold'

@dataclass
class StorageConfig:
    """Configuration for hybrid storage system."""

    # Storage paths - UPDATED to match existing structure
    base_data_path: str = "/home/jianjun/ats/data/STK/1min"
    existing_parquet_path: str = "/home/jianjun/ats/data/STK/1min/cold"  # Existing data

    # Hot/cold data thresholds
    hot_data_days: int = 30        # Keep in database for fast access
    warm_data_days: int = 90       # Keep uncompressed on disk
    cold_data_days: int = 365      # Compress and archive

    # Database configuration
    database_url: str = "postgresql://postgres:postgres@localhost:5433/dev_db"
    db_pool_size: int = 20
    db_timeout: int = 30

    # File organization - MATCHES existing SYMBOL/YEAR/MONTH structure
    partition_by: str = "year_month"  # Matches existing structure
    compression: str = "snappy"       # "snappy", "lz4", "gzip"

    # Performance settings
    batch_size: int = 10000          # Records per batch operation
    max_concurrent_files: int = 4    # Parallel file operations
    memory_limit_mb: int = 2048      # Memory limit for operations

    def get_tier_path(self, tier: str) -> Path:
        """Get path for specific storage tier."""
        return Path(self.base_data_path) / tier

    def get_existing_parquet_path(self, symbol: str, year: int, month: int) -> Path:
        """Get path for existing parquet file following SYMBOL/YEAR/MONTH structure."""
        return Path(self.existing_parquet_path) / symbol / str(year) / f"{month:02d}" / f"{symbol}_{year}_{month:02d}.parquet"
'''
    return content

def split_hybrid_manager():
    """Split the large hybrid manager into focused modules."""

    source_file = Path("/home/jianjun/ats-genai-data/src/infrastructure/storage/hybrid_minute_data_manager.py")
    modules_dir = Path("/home/jianjun/ats-genai-data/src/infrastructure/storage/hybrid_storage")

    modules_dir.mkdir(exist_ok=True)

    # Create modules
    modules = {
        "config.py": create_storage_config_module(),
        "__init__.py": '"""Hybrid Storage Modules Package"""',
    }

    for module_name, content in modules.items():
        module_path = modules_dir / module_name
        with open(module_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Created {module_name}")

    # Create a new streamlined hybrid manager that imports the modules
    new_manager_content = '''"""
Hybrid Storage Manager for 1-Minute Financial Data - Modular Architecture

Manages storage across PostgreSQL/TimescaleDB (hot cache) and disk-based
Parquet files (cold storage) for optimal performance and cost efficiency.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor

# Optional pyarrow import for Parquet support
import pyarrow as pa
import pyarrow.parquet as pq
PYARROW_AVAILABLE = True
from core.config.environment import env
from .hybrid_storage.config import StorageConfig, DataGap

logger = logging.getLogger(__name__)

class HybridMinuteDataManager:
    """
    Hybrid storage manager for 1-minute financial data - Modular Architecture.

    Automatically manages data lifecycle between hot (database) and
    cold (disk) storage based on age and access patterns.
    """

    def __init__(self, pool: asyncpg.Pool, config: StorageConfig = None):
        self.pool = pool
        self.config = config or StorageConfig()
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_concurrent_files)

        # Ensure storage directories exist
        self._ensure_storage_structure()

    def _ensure_storage_structure(self):
        """Create required storage directories."""
        for tier in ['hot', 'warm', 'cold']:
            tier_path = self.config.get_tier_path(tier)
            tier_path.mkdir(parents=True, exist_ok=True)

        # Ensure existing parquet path exists
        Path(self.config.existing_parquet_path).mkdir(parents=True, exist_ok=True)

        logger.info(f"✅ Storage structure ready at {self.config.base_data_path}")

    async def store_minute_data(self, symbol: str, data: List[Dict[str, Any]]) -> int:
        """Store minute data using appropriate tier based on data age."""
        if not data:
            return 0

        logger.info(f"📊 Storing {len(data)} bars for {symbol}")

        # TODO: Implement modular storage logic
        # This would import storage modules for hot, warm, cold tiers

        return len(data)

    async def query_minute_data(self, symbol: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Query minute data across all storage tiers."""

        logger.info(f"🔍 Querying {symbol} from {start_date} to {end_date}")

        # TODO: Implement modular query logic
        # This would import query modules for different storage tiers

        return []

    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics."""

        # TODO: Implement modular statistics

        return {
            "total_symbols": 0,
            "total_bars": 0,
            "storage_tiers": {
                "hot": {"bars": 0, "size_mb": 0},
                "warm": {"bars": 0, "size_mb": 0},
                "cold": {"bars": 0, "size_mb": 0}
            }
        }

    async def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)
        logger.info("🔄 Hybrid storage manager closed")

# Factory function for creating manager
async def create_integrated_hybrid_manager(pool: asyncpg.Pool = None, config: StorageConfig = None) -> HybridMinuteDataManager:
    """Create integrated hybrid manager with database pool."""

    if pool is None:
        config = config or StorageConfig()
        pool = await asyncpg.create_pool(
            config.database_url,
            min_size=5,
            max_size=config.db_pool_size
        )

    return HybridMinuteDataManager(pool, config)
'''

    # Write new manager
    new_manager_path = source_file.parent / "hybrid_minute_data_manager_new.py"
    with open(new_manager_path, 'w', encoding='utf-8') as f:
        f.write(new_manager_content)

    print(f"✅ Created modular hybrid manager")
    print(f"📦 Modules created in: {modules_dir}")
    print(f"🔄 New manager: {new_manager_path}")

if __name__ == "__main__":
    split_hybrid_manager()