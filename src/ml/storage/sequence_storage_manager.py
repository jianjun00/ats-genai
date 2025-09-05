"""
Hybrid Storage Manager for Sequence Training Data

Combines Riegeli (primary), Parquet (metadata), and Arrow (processing)
for optimal performance across different access patterns.
"""

import numpy as np
import pandas as pd

# Optional pyarrow import for Parquet support
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PYARROW_AVAILABLE = False
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
import hashlib
import json
from dataclasses import dataclass, asdict
import asyncio

from array_record.python.array_record_module import ArrayRecordWriter, ArrayRecordReader

import tensorflow as tf


@dataclass
class SequenceMetadata:
    """Metadata for a sequence training example."""
    example_id: str
    symbol: str
    prediction_timestamp: datetime
    instrument_id: int
    sequence_lengths: Dict[str, int]
    prediction_horizons: Dict[str, int]
    feature_count: int
    file_offset: int  # Offset in Riegeli file
    file_size: int    # Size in bytes
    checksum: str


@dataclass
class StorageConfig:
    """Configuration for sequence storage."""
    primary_format: str = "arrayrecord"  # arrayrecord only
    compression_level: int = 6
    chunk_size: int = 1000  # Examples per file
    enable_indexing: bool = True
    enable_checksums: bool = True
    buffer_size: int = 64 * 1024 * 1024  # 64MB


class SequenceStorageManager:
    """
    Hybrid storage manager optimized for sequence training data.
    
    Architecture:
    - Primary: Riegeli files for sequence data (optimal for ML training)
    - Secondary: Parquet files for metadata and indexing (optimal for analytics)
    - Processing: Arrow for in-memory operations (optimal for transformations)
    """
    
    def __init__(self, 
                 base_path: str,
                 config: Optional[StorageConfig] = None):
        """
        Initialize storage manager.
        
        Args:
            base_path: Base directory for storage
            config: Storage configuration
        """
        self.base_path = Path(base_path)
        self.config = config or StorageConfig()
        
        # Create directory structure
        self.sequence_dir = self.base_path / "sequences"
        self.metadata_dir = self.base_path / "metadata"
        self.index_dir = self.base_path / "index"
        
        for dir_path in [self.sequence_dir, self.metadata_dir, self.index_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
        # All required packages must be available - no fallbacks
    
    def _generate_example_id(self, symbol: str, timestamp: datetime) -> str:
        """Generate unique example ID."""
        key = f"{symbol}_{timestamp.isoformat()}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate checksum for data integrity."""
        return hashlib.sha256(data).hexdigest()
    
    def _json_serializer(self, obj):
        """JSON serializer for objects not serializable by default json code."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)
    
    async def save_sequence_batch(self, 
                                 examples: List[Any],
                                 batch_id: str) -> Dict[str, Any]:
        """
        Save a batch of sequence examples using optimal storage format.
        
        Args:
            examples: List of SequenceTrainingExample objects
            batch_id: Unique identifier for this batch
            
        Returns:
            Storage statistics and metadata
        """
        if not examples:
            return {}
        
        self.logger.info(f"Saving batch {batch_id} with {len(examples)} examples")
        
        # Determine output files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sequence_file = self.sequence_dir / f"sequences_{batch_id}_{timestamp}"
        metadata_file = self.metadata_dir / f"metadata_{batch_id}_{timestamp}.arrayrecord"
        
        # Convert examples to storage-optimized format
        sequence_data, metadata_records = self._prepare_data_for_storage(examples)
        
        # Save sequence data using ArrayRecord format
        sequence_stats = await self._save_sequence_data(
            sequence_data, sequence_file, "arrayrecord"
        )
        
        # Save metadata using Riegeli (not Parquet)
        metadata_stats = await self._save_metadata_arrayrecord(metadata_records, metadata_file)
        
        # Update index if enabled
        if self.config.enable_indexing:
            await self._update_index(batch_id, metadata_records)
        
        return {
            'batch_id': batch_id,
            'timestamp': timestamp,
            'example_count': len(examples),
            'sequence_file': str(sequence_file),
            'metadata_file': str(metadata_file),
            'sequence_stats': sequence_stats,
            'metadata_stats': metadata_stats
        }
    
    def _prepare_data_for_storage(self, examples: List[Any]) -> Tuple[List[Dict], List[SequenceMetadata]]:
        """
        Convert training examples to storage-optimized format.
        
        Returns:
            Tuple of (sequence_data, metadata_records)
        """
        sequence_data = []
        metadata_records = []
        current_offset = 0
        
        for example in examples:
            # Generate unique ID
            example_id = self._generate_example_id(example.symbol, example.prediction_timestamp)
            
            # Prepare sequence data for storage
            sequence_record = {
                'example_id': example_id,
                'symbol': example.symbol,
                'prediction_timestamp': example.prediction_timestamp.isoformat(),
                'instrument_id': example.instrument_id,
                
                # Base features
                'base_features': example.base_features,
                
                # Sequence features (variable-length arrays)
                'sequence_5m': self._serialize_sequence(example.sequence_5m),
                'sequence_15m': self._serialize_sequence(example.sequence_15m),
                'sequence_1h': self._serialize_sequence(example.sequence_1h),
                'sequence_1d': self._serialize_sequence(example.sequence_1d),
                
                # Timeframe features
                'timeframe_features': example.timeframe_features,
                
                # Prediction targets
                'future_1h': self._serialize_sequence(example.future_1h),
                'future_1d': self._serialize_sequence(example.future_1d),
            }
            
            # Estimate size for metadata
            serialized_data = json.dumps(sequence_record, default=self._json_serializer).encode()
            data_size = len(serialized_data)
            
            # Calculate checksum if enabled
            checksum = ""
            if self.config.enable_checksums:
                checksum = self._calculate_checksum(serialized_data)
            
            # Create metadata record
            metadata = SequenceMetadata(
                example_id=example_id,
                symbol=example.symbol,
                prediction_timestamp=example.prediction_timestamp,
                instrument_id=example.instrument_id,
                sequence_lengths=example.sequence_length,
                prediction_horizons=example.prediction_horizon,
                feature_count=len(example.base_features),
                file_offset=current_offset,
                file_size=data_size,
                checksum=checksum
            )
            
            sequence_data.append(sequence_record)
            metadata_records.append(metadata)
            current_offset += data_size
        
        return sequence_data, metadata_records
    
    def _serialize_sequence(self, sequence: List[Dict]) -> bytes:
        """Serialize sequence data efficiently."""
        if not sequence:
            return b''
        
        # Convert to Arrow format for efficient serialization
        try:
            # Convert list of dicts to pandas DataFrame
            df = pd.DataFrame(sequence)
            
            # Convert to Arrow table
            table = pa.Table.from_pandas(df)
            
            # Serialize to bytes
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            
            return sink.getvalue().to_pybytes()
            
        except Exception as e:
            self.logger.warning(f"Arrow serialization failed, using JSON: {e}")
            # Fallback to JSON
            return json.dumps(sequence).encode()
    
    def _deserialize_sequence(self, data: bytes) -> List[Dict]:
        """Deserialize sequence data."""
        if not data:
            return []
        
        try:
            # Try Arrow format first
            buffer = pa.py_buffer(data)
            reader = pa.ipc.open_stream(buffer)
            table = reader.read_all()
            df = table.to_pandas()
            return df.to_dict('records')
            
        except Exception:
            # Fallback to JSON
            return json.loads(data.decode())
    
    async def _save_sequence_data(self, 
                                 sequence_data: List[Dict], 
                                 file_path: Path,
                                 format_type: str) -> Dict[str, Any]:
        """Save sequence data using specified format."""
        if format_type in ["arrayrecord"]:
            extension = '.arrayrecord'
            return await self._save_arrayrecord(sequence_data, file_path.with_suffix(extension))
        elif format_type == "tfrecord":
            return await self._save_tfrecord(sequence_data, file_path.with_suffix('.tfrecord'))
        else:
            return await self._save_pickle(sequence_data, file_path.with_suffix('.pkl'))
    
    async def _save_arrayrecord(self, sequence_data: List[Dict], file_path: Path) -> Dict[str, Any]:
        """Save data using ArrayRecord format."""
        
        def _write_arrayrecord():
            with ArrayRecordWriter(
                str(file_path),
                'group_size:1'
            ) as writer:
                for record in sequence_data:
                    # Serialize record to bytes
                    data = json.dumps(record, default=self._json_serializer).encode()
                    writer.write_record(data)
        
        # Run in thread pool to avoid blocking
        await asyncio.get_event_loop().run_in_executor(None, _write_arrayrecord)
        
        file_size = file_path.stat().st_size
        return {
            'format': 'arrayrecord',
            'file_size': file_size,
            'compression_ratio': self._estimate_compression_ratio(sequence_data, file_size),
            'records_written': len(sequence_data)
        }
    
    async def _save_tfrecord(self, sequence_data: List[Dict], file_path: Path) -> Dict[str, Any]:
        """Save data using TFRecord format."""
        if not TFRECORD_AVAILABLE:
            raise ImportError("TensorFlow not available")
        
        def _write_tfrecord():
            with tf.io.TFRecordWriter(
                str(file_path),
                options=tf.io.TFRecordOptions(compression_type="GZIP")
            ) as writer:
                for record in sequence_data:
                    # Convert to TF Example
                    example = self._dict_to_tf_example(record)
                    writer.write(example.SerializeToString())
        
        await asyncio.get_event_loop().run_in_executor(None, _write_tfrecord)
        
        file_size = file_path.stat().st_size
        return {
            'format': 'tfrecord',
            'file_size': file_size,
            'compression_ratio': self._estimate_compression_ratio(sequence_data, file_size),
            'records_written': len(sequence_data)
        }
    
    async def _save_pickle(self, sequence_data: List[Dict], file_path: Path) -> Dict[str, Any]:
        """Save data using pickle format (fallback)."""
        import pickle
        
        def _write_pickle():
            with open(file_path, 'wb') as f:
                pickle.dump(sequence_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        await asyncio.get_event_loop().run_in_executor(None, _write_pickle)
        
        file_size = file_path.stat().st_size
        return {
            'format': 'pickle',
            'file_size': file_size,
            'compression_ratio': self._estimate_compression_ratio(sequence_data, file_size),
            'records_written': len(sequence_data)
        }
    
    def _dict_to_tf_example(self, record: Dict) -> Any:
        """Convert dictionary to TensorFlow Example."""
        feature = {}
        
        for key, value in record.items():
            if isinstance(value, (int, np.integer)):
                feature[key] = tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))
            elif isinstance(value, (float, np.floating)):
                feature[key] = tf.train.Feature(float_list=tf.train.FloatList(value=[value]))
            elif isinstance(value, str):
                feature[key] = tf.train.Feature(bytes_list=tf.train.BytesList(value=[value.encode()]))
            elif isinstance(value, bytes):
                feature[key] = tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))
            else:
                # Serialize complex objects as JSON bytes
                json_bytes = json.dumps(value, default=self._json_serializer).encode()
                feature[key] = tf.train.Feature(bytes_list=tf.train.BytesList(value=[json_bytes]))
        
        return tf.train.Example(features=tf.train.Features(feature=feature))
    
    async def _save_metadata_arrayrecord(self, metadata_records: List[SequenceMetadata], file_path: Path) -> Dict[str, Any]:
        """Save metadata using Riegeli format instead of Parquet."""
        
        def _write_metadata_arrayrecord():
            with ArrayRecordWriter(
                str(file_path),
                'group_size:1'
            ) as writer:
                for record in metadata_records:
                    # Convert metadata record to dictionary and serialize
                    metadata_dict = asdict(record)
                    # Convert datetime to ISO string for JSON serialization
                    metadata_dict['prediction_timestamp'] = metadata_dict['prediction_timestamp'].isoformat()
                    data = json.dumps(metadata_dict, default=self._json_serializer).encode()
                    writer.write_record(data)
        
        await asyncio.get_event_loop().run_in_executor(None, _write_metadata_arrayrecord)
        
        file_size = file_path.stat().st_size
        return {
            'format': 'arrayrecord',
            'file_size': file_size,
            'records_written': len(metadata_records)
        }
    
    async def _update_index(self, batch_id: str, metadata_records: List[SequenceMetadata]):
        """Update search index for efficient querying."""
        index_file = self.index_dir / f"index_{batch_id}.arrayrecord"
        
        # Create index DataFrame with key fields for fast lookup
        index_data = []
        for record in metadata_records:
            index_data.append({
                'example_id': record.example_id,
                'symbol': record.symbol,
                'prediction_timestamp': record.prediction_timestamp,
                'instrument_id': record.instrument_id,
                'batch_id': batch_id,
                'file_offset': record.file_offset,
                'file_size': record.file_size
            })
        
        # Save index using ArrayRecord instead of Parquet
        def _write_index_arrayrecord():
            with ArrayRecordWriter(
                str(index_file),
                'group_size:1'
            ) as writer:
                for index_record in index_data:
                    # Convert datetime to ISO string for JSON serialization
                    index_record['prediction_timestamp'] = index_record['prediction_timestamp'].isoformat()
                    data = json.dumps(index_record, default=self._json_serializer).encode()
                    writer.write_record(data)
        
        await asyncio.get_event_loop().run_in_executor(None, _write_index_arrayrecord)
        
        self.logger.debug(f"Updated Riegeli index for batch {batch_id}")
    
    def _estimate_compression_ratio(self, data: List[Dict], compressed_size: int) -> float:
        """Estimate compression ratio."""
        try:
            # Estimate uncompressed size
            sample_size = len(json.dumps(data[0], default=self._json_serializer).encode()) if data else 0
            estimated_uncompressed = sample_size * len(data)
            
            if estimated_uncompressed > 0:
                return compressed_size / estimated_uncompressed
            else:
                return 1.0
        except Exception:
            return 1.0
    
    async def load_sequence_batch(self, batch_id: str) -> List[Dict]:
        """Load sequence batch by ID."""
        # Find batch files
        sequence_files = list(self.sequence_dir.glob(f"sequences_{batch_id}_*"))
        if not sequence_files:
            raise FileNotFoundError(f"No sequence files found for batch {batch_id}")
        
        sequence_file = sequence_files[0]  # Take the first match
        
        # Determine format and load
        if sequence_file.suffix == '.arrayrecord':
            return await self._load_arrayrecord(sequence_file)
        elif sequence_file.suffix == '.tfrecord':
            return await self._load_tfrecord(sequence_file)
        else:
            return await self._load_pickle(sequence_file)
    
    async def _load_arrayrecord(self, file_path: Path) -> List[Dict]:
        """Load data from Riegeli format."""
        
        def _read_arrayrecord():
            records = []
            with ArrayRecordReader(str(file_path)) as reader:
                for record_bytes in reader:
                    record = json.loads(record_bytes.decode())
                    records.append(record)
            return records
        
        return await asyncio.get_event_loop().run_in_executor(None, _read_arrayrecord)
    
    async def query_by_symbol(self, symbol: str, 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None) -> List[SequenceMetadata]:
        """Query examples by symbol and date range using metadata."""
        # Load all metadata files
        metadata_files = list(self.metadata_dir.glob("metadata_*.arrayrecord"))
        
        if not metadata_files:
            return []
        
        # Query metadata from Riegeli files
        def _query_metadata():
            all_records = []
            for file_path in metadata_files:
                with ArrayRecordReader(str(file_path)) as reader:
                    for record_bytes in reader:
                        record = json.loads(record_bytes.decode())
                        # Parse timestamp back to datetime for filtering
                        record['prediction_timestamp'] = datetime.fromisoformat(record['prediction_timestamp'])
                        all_records.append(record)
            
            # Apply filters
            filtered_records = []
            for record in all_records:
                if record['symbol'] == symbol:
                    if start_date and record['prediction_timestamp'] < start_date:
                        continue
                    if end_date and record['prediction_timestamp'] > end_date:
                        continue
                    filtered_records.append(record)
            
            return filtered_records
        
        records = await asyncio.get_event_loop().run_in_executor(None, _query_metadata)
        
        # Convert back to metadata objects
        metadata_list = []
        for record in records:
            metadata = SequenceMetadata(
                example_id=record['example_id'],
                symbol=record['symbol'],
                prediction_timestamp=record['prediction_timestamp'],
                instrument_id=record['instrument_id'],
                sequence_lengths=record['sequence_lengths'],
                prediction_horizons=record['prediction_horizons'],
                feature_count=record['feature_count'],
                file_offset=record['file_offset'],
                file_size=record['file_size'],
                checksum=record['checksum']
            )
            metadata_list.append(metadata)
        
        return metadata_list
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics."""
        stats = {
            'sequence_files': len(list(self.sequence_dir.glob("*"))),
            'metadata_files': len(list(self.metadata_dir.glob("*.arrayrecord"))),
            'index_files': len(list(self.index_dir.glob("*.arrayrecord"))),
            'total_size_bytes': 0,
            'format_breakdown': {}
        }
        
        # Calculate total size and format breakdown
        for file_path in self.base_path.rglob("*"):
            if file_path.is_file():
                size = file_path.stat().st_size
                stats['total_size_bytes'] += size
                
                suffix = file_path.suffix
                if suffix not in stats['format_breakdown']:
                    stats['format_breakdown'][suffix] = {'count': 0, 'size': 0}
                
                stats['format_breakdown'][suffix]['count'] += 1
                stats['format_breakdown'][suffix]['size'] += size
        
        stats['total_size_mb'] = round(stats['total_size_bytes'] / (1024 * 1024), 2)
        
        return stats