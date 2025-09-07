"""
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
