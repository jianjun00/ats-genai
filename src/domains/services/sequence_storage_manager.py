"""
ArrayRecord Storage Manager for Sequence Training Data

Uses ArrayRecord format exclusively for training data storage per PRD/DRD requirements.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import asyncio
from dataclasses import dataclass

# ArrayRecord is the only supported format for training data
try:
    import array_record.python.array_record_module as array_record
    ARRAYRECORD_AVAILABLE = True
except ImportError:
    ARRAYRECORD_AVAILABLE = False
    logging.error("ArrayRecord not available - this is required for training data storage")


@dataclass
class ArrayRecordMetadata:
    """Simple metadata for ArrayRecord files."""
    file_path: str
    symbol: str
    timeframe: str
    record_count: int
    file_size_bytes: int
    creation_timestamp: datetime


@dataclass
class ArrayRecordConfig:
    """Configuration for ArrayRecord storage."""
    compression: str = "zstd"  # ArrayRecord compression
    group_size: int = 1        # ArrayRecord group size
    buffer_size: int = 64 * 1024 * 1024  # 64MB


class ArrayRecordStorageManager:
    """
    ArrayRecord-only storage manager for sequence training data.

    Stores training data exclusively in ArrayRecord format per PRD/DRD requirements.
    No JSON, pickle, or TensorFlow serialization anti-patterns.
    """

    def __init__(self,
                 base_path: str,
                 config: Optional[ArrayRecordConfig] = None):
        """
        Initialize ArrayRecord storage manager.

        Args:
            base_path: Base directory for ArrayRecord files
            config: ArrayRecord configuration
        """
        self.base_path = Path(base_path)
        self.config = config or ArrayRecordConfig()
        self.logger = logging.getLogger(__name__)

        # Create base directory
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Validate ArrayRecord availability
        if not ARRAYRECORD_AVAILABLE:
            raise ImportError("ArrayRecord is required but not available. Install array_record package.")

    def create_arrayrecord_writer(self, file_path: str) -> Any:
        """Create ArrayRecord writer with optimal configuration."""
        if not ARRAYRECORD_AVAILABLE:
            raise ImportError("ArrayRecord not available")

        return array_record.ArrayRecordWriter(
            file_path,
            f"group_size:{self.config.group_size}"
        )

    def write_binary_record(self, writer: Any, record_bytes: bytes):
        """Write binary record to ArrayRecord writer."""
        writer.write(record_bytes)

    async def save_arrayrecord_file(self,
                                   file_path: str,
                                   binary_records: List[bytes]) -> ArrayRecordMetadata:
        """
        Save binary records to ArrayRecord file.

        Args:
            file_path: Path to ArrayRecord file
            binary_records: List of binary record bytes

        Returns:
            ArrayRecord file metadata
        """
        if not binary_records:
            raise ValueError("No records to save")

        self.logger.info(f"Saving {len(binary_records)} records to {file_path}")

        def _write_records():
            writer = self.create_arrayrecord_writer(file_path)
            try:
                for record_bytes in binary_records:
                    self.write_binary_record(writer, record_bytes)
            finally:
                writer.close()

        # Run in thread pool to avoid blocking
        await asyncio.get_event_loop().run_in_executor(None, _write_records)

        # Get file stats
        file_path_obj = Path(file_path)
        file_size = file_path_obj.stat().st_size

        # Extract symbol and timeframe from path if possible
        parts = file_path_obj.stem.split('_')
        symbol = parts[0] if parts else "unknown"
        timeframe = parts[-1] if len(parts) > 1 else "unknown"

        return ArrayRecordMetadata(
            file_path=file_path,
            symbol=symbol,
            timeframe=timeframe,
            record_count=len(binary_records),
            file_size_bytes=file_size,
            creation_timestamp=datetime.now()
        )

    async def read_arrayrecord_file(self, file_path: str) -> List[bytes]:
        """
        Read binary records from ArrayRecord file.

        Args:
            file_path: Path to ArrayRecord file

        Returns:
            List of binary record bytes
        """
        def _read_records():
            records = []
            reader = array_record.ArrayRecordReader(file_path)
            try:
                for record_bytes in reader:
                    records.append(record_bytes)
            finally:
                reader.close()
            return records

        return await asyncio.get_event_loop().run_in_executor(None, _read_records)

    def get_file_metadata(self, file_path: str) -> Optional[ArrayRecordMetadata]:
        """Get metadata for an ArrayRecord file."""
        path_obj = Path(file_path)

        if not path_obj.exists():
            return None

        # Extract symbol and timeframe from filename
        parts = path_obj.stem.split('_')
        symbol = parts[0] if parts else "unknown"
        timeframe = parts[-1] if len(parts) > 1 else "unknown"

        # Get file stats
        stat = path_obj.stat()

        # Count records (requires reading file)
        try:
            reader = array_record.ArrayRecordReader(str(path_obj))
            record_count = sum(1 for _ in reader)
            reader.close()
        except Exception:
            record_count = 0

        return ArrayRecordMetadata(
            file_path=str(path_obj),
            symbol=symbol,
            timeframe=timeframe,
            record_count=record_count,
            file_size_bytes=stat.st_size,
            creation_timestamp=datetime.fromtimestamp(stat.st_ctime)
        )

    def list_arrayrecord_files(self) -> List[ArrayRecordMetadata]:
        """List all ArrayRecord files in base directory."""
        metadata_list = []

        for file_path in self.base_path.rglob("*.arrayrecord"):
            metadata = self.get_file_metadata(str(file_path))
            if metadata:
                metadata_list.append(metadata)

        return sorted(metadata_list, key=lambda x: x.creation_timestamp)

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for ArrayRecord files."""
        arrayrecord_files = list(self.base_path.rglob("*.arrayrecord"))

        total_size = sum(f.stat().st_size for f in arrayrecord_files)
        total_records = 0

        for file_path in arrayrecord_files:
            metadata = self.get_file_metadata(str(file_path))
            if metadata:
                total_records += metadata.record_count

        return {
            'total_files': len(arrayrecord_files),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'total_records': total_records,
            'storage_format': 'ArrayRecord',
            'base_path': str(self.base_path)
        }


# Backward compatibility alias
SequenceStorageManager = ArrayRecordStorageManager