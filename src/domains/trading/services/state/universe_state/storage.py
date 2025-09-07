"""
Universe State Storage Operations.
"""
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging

# Optional pyarrow import for Parquet support
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PYARROW_AVAILABLE = False

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
