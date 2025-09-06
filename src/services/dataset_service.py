#!/usr/bin/env python3
"""
Dataset Service - Centralized metadata management for training datasets
Provides unified interface for dataset discovery, metadata, and file location resolution.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

@dataclass
class DatasetMetadata:
    """Comprehensive dataset metadata structure."""
    
    # Core Identification
    dataset_id: int
    dataset_name: str
    dataset_type: str  # 'training', 'eda', 'validation', 'test'
    
    # Data Characteristics
    symbols: List[str]
    total_sequences: int
    total_records: int
    feature_count: int
    label_count: int
    
    # File Locations
    file_paths: List[str]
    base_directory: str
    file_format: str  # 'riegeli', 'parquet', 'npy', 'csv'
    
    # Quality Metrics
    data_quality_score: float
    feature_completeness: float
    label_completeness: float
    
    # Technical Details
    file_size_mb: float
    technical_indicators: List[str]
    sequence_length: int
    timeframes: List[str]  # ['5m', '15m', '1h', '1d', '1w']
    
    # Temporal Information
    date_range_start: str
    date_range_end: str
    creation_timestamp: datetime
    last_updated: datetime
    
    # Processing Metadata
    run_id: Optional[int] = None
    data_source: str = 'firstrate'
    processing_config: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert datetime objects to ISO strings
        if isinstance(result['creation_timestamp'], datetime):
            result['creation_timestamp'] = result['creation_timestamp'].isoformat()
        if isinstance(result['last_updated'], datetime):
            result['last_updated'] = result['last_updated'].isoformat()
        return result

@dataclass 
class DatasetFileIterator:
    """Iterator configuration for dataset files."""
    
    file_path: str
    record_count: int
    file_size_bytes: int
    estimated_memory_mb: float
    batch_size_recommendation: int
    
    def get_iterator_config(self) -> Dict[str, Any]:
        """Get recommended iterator configuration."""
        return {
            'file_path': self.file_path,
            'batch_size': self.batch_size_recommendation,
            'estimated_memory_mb': self.estimated_memory_mb,
            'total_records': self.record_count,
            'file_format': Path(self.file_path).suffix.lower()
        }

class DatasetService:
    """Centralized dataset metadata management service."""
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """Initialize dataset service with database configuration."""
        
        self.db_config = db_config or self._get_default_db_config()
        
        # Cache for frequently accessed metadata
        self._metadata_cache = {}
        self._cache_ttl = 300  # 5 minutes
        
        logger.info("✅ Dataset Service initialized")
    
    def _get_default_db_config(self) -> Dict[str, Any]:
        """Get default database configuration."""
        return {
            'host': os.environ.get('DATABASE_HOST', 'localhost'),
            'port': int(os.environ.get('DATABASE_PORT', '3432')),
            'database': os.environ.get('DATABASE_NAME', 'dev_db'),
            'user': os.environ.get('DATABASE_USER', 'postgres'),
            'password': os.environ.get('DATABASE_PASSWORD', 'dev_password')
        }
    
    def register_dataset(self, metadata: DatasetMetadata) -> int:
        """Register a new dataset and return dataset_id."""
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    
                    # Insert into dev_training_dataset table
                    cur.execute("""
                        INSERT INTO dev_training_dataset (
                            dataset_name, symbols, total_sequences, feature_count,
                            label_count, data_quality_score, feature_completeness,
                            label_completeness, file_size_mb, technical_indicators,
                            creation_timestamp, sequence_length, date_range_start,
                            date_range_end, run_id, processing_config
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) RETURNING id
                    """, (
                        metadata.dataset_name,
                        ','.join(metadata.symbols),
                        metadata.total_sequences,
                        metadata.feature_count,
                        metadata.label_count,
                        metadata.data_quality_score,
                        metadata.feature_completeness,
                        metadata.label_completeness,
                        metadata.file_size_mb,
                        ','.join(metadata.technical_indicators),
                        metadata.creation_timestamp,
                        metadata.sequence_length,
                        metadata.date_range_start,
                        metadata.date_range_end,
                        metadata.run_id,
                        json.dumps(metadata.processing_config) if metadata.processing_config else None
                    ))
                    
                    dataset_id = cur.fetchone()['id']
                    
                    # Insert file paths into dev_training_dataset_files table
                    for file_path in metadata.file_paths:
                        cur.execute("""
                            INSERT INTO dev_training_dataset_files (
                                dataset_id, file_path, file_format, file_size_mb
                            ) VALUES (%s, %s, %s, %s)
                        """, (
                            dataset_id,
                            file_path,
                            metadata.file_format,
                            metadata.file_size_mb / len(metadata.file_paths)
                        ))
                    
                    # Update metadata with assigned ID
                    metadata.dataset_id = dataset_id
                    
                    logger.info(f"✅ Dataset registered: ID {dataset_id}, Name: {metadata.dataset_name}")
                    return dataset_id
                    
        except Exception as e:
            logger.error(f"❌ Failed to register dataset: {e}")
            raise
    
    def get_dataset_metadata(self, dataset_id: int) -> Optional[DatasetMetadata]:
        """Get complete dataset metadata by ID."""
        
        # Check cache first
        cache_key = f"metadata_{dataset_id}"
        if cache_key in self._metadata_cache:
            cached_data, timestamp = self._metadata_cache[cache_key]
            if (datetime.now() - timestamp).seconds < self._cache_ttl:
                return cached_data
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    
                    # Get main dataset metadata
                    cur.execute("""
                        SELECT * FROM dev_training_dataset WHERE id = %s
                    """, (dataset_id,))
                    
                    dataset_row = cur.fetchone()
                    if not dataset_row:
                        return None
                    
                    # Get associated file paths
                    cur.execute("""
                        SELECT file_path, file_format, file_size_mb 
                        FROM dev_training_dataset_files 
                        WHERE dataset_id = %s
                    """, (dataset_id,))
                    
                    file_rows = cur.fetchall()
                    
                    # Build metadata object
                    metadata = DatasetMetadata(
                        dataset_id=dataset_row['id'],
                        dataset_name=dataset_row['dataset_name'],
                        dataset_type='training',  # Default for dev_training_dataset
                        symbols=dataset_row['symbols'].split(',') if dataset_row['symbols'] else [],
                        total_sequences=dataset_row['total_sequences'] or 0,
                        total_records=dataset_row.get('total_records', 0) or dataset_row['total_sequences'] or 0,
                        feature_count=dataset_row['feature_count'] or 0,
                        label_count=dataset_row['label_count'] or 0,
                        file_paths=[row['file_path'] for row in file_rows],
                        base_directory=str(Path(file_rows[0]['file_path']).parent) if file_rows else '',
                        file_format=file_rows[0]['file_format'] if file_rows else 'unknown',
                        data_quality_score=dataset_row['data_quality_score'] or 0.0,
                        feature_completeness=dataset_row['feature_completeness'] or 0.0,
                        label_completeness=dataset_row['label_completeness'] or 0.0,
                        file_size_mb=dataset_row['file_size_mb'] or 0.0,
                        technical_indicators=dataset_row['technical_indicators'].split(',') if dataset_row.get('technical_indicators') else [],
                        sequence_length=dataset_row.get('sequence_length', 100),
                        timeframes=dataset_row.get('timeframes', '5m,15m,1h,1d').split(',') if dataset_row.get('timeframes') else ['1h'],
                        date_range_start=dataset_row.get('date_range_start', '2025-07-01'),
                        date_range_end=dataset_row.get('date_range_end', '2025-07-31'),
                        creation_timestamp=dataset_row['creation_timestamp'] or datetime.now(),
                        last_updated=dataset_row.get('last_updated', dataset_row['creation_timestamp']) or datetime.now(),
                        run_id=dataset_row.get('run_id'),
                        data_source=dataset_row.get('data_source', 'firstrate'),
                        processing_config=json.loads(dataset_row['processing_config']) if dataset_row.get('processing_config') else None
                    )
                    
                    # Cache the result
                    self._metadata_cache[cache_key] = (metadata, datetime.now())
                    
                    logger.info(f"✅ Retrieved metadata for dataset {dataset_id}: {metadata.dataset_name}")
                    return metadata
                    
        except Exception as e:
            logger.error(f"❌ Failed to get dataset metadata for ID {dataset_id}: {e}")
            return None
    
    def get_dataset_by_name(self, dataset_name: str) -> Optional[DatasetMetadata]:
        """Get dataset metadata by name."""
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    
                    cur.execute("""
                        SELECT id FROM dev_training_dataset 
                        WHERE dataset_name = %s 
                        ORDER BY creation_timestamp DESC 
                        LIMIT 1
                    """, (dataset_name,))
                    
                    result = cur.fetchone()
                    if result:
                        return self.get_dataset_metadata(result['id'])
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Failed to get dataset by name '{dataset_name}': {e}")
            return None
    
    def list_datasets(self, dataset_type: Optional[str] = None, 
                     symbols: Optional[List[str]] = None,
                     limit: int = 50) -> List[DatasetMetadata]:
        """List available datasets with optional filtering."""
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    
                    # Build dynamic query
                    where_conditions = []
                    params = []
                    
                    if symbols:
                        # Match any of the requested symbols
                        symbol_conditions = []
                        for symbol in symbols:
                            symbol_conditions.append("symbols LIKE %s")
                            params.append(f"%{symbol}%")
                        where_conditions.append(f"({' OR '.join(symbol_conditions)})")
                    
                    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
                    params.append(limit)
                    
                    cur.execute(f"""
                        SELECT id FROM dev_training_dataset 
                        {where_clause}
                        ORDER BY creation_timestamp DESC 
                        LIMIT %s
                    """, params)
                    
                    dataset_ids = [row['id'] for row in cur.fetchall()]
                    
                    # Get full metadata for each dataset
                    datasets = []
                    for dataset_id in dataset_ids:
                        metadata = self.get_dataset_metadata(dataset_id)
                        if metadata:
                            datasets.append(metadata)
                    
                    logger.info(f"✅ Listed {len(datasets)} datasets")
                    return datasets
                    
        except Exception as e:
            logger.error(f"❌ Failed to list datasets: {e}")
            return []
    
    def get_file_iterators(self, dataset_id: int, 
                          batch_size: Optional[int] = None) -> List[DatasetFileIterator]:
        """Get file iterator configurations for dataset."""
        
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return []
        
        iterators = []
        
        for file_path in metadata.file_paths:
            if not os.path.exists(file_path):
                logger.warning(f"⚠️ File not found: {file_path}")
                continue
            
            # Calculate file statistics
            file_size_bytes = os.path.getsize(file_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            
            # Estimate record count based on file size and metadata
            if metadata.total_records > 0 and len(metadata.file_paths) > 0:
                records_per_file = metadata.total_records // len(metadata.file_paths)
            else:
                # Rough estimate for different file formats
                if file_path.endswith('.parquet'):
                    records_per_file = int(file_size_mb * 1000)  # ~1000 records per MB
                elif file_path.endswith('.npy'):
                    records_per_file = int(file_size_mb * 10000)  # Compressed numpy
                else:
                    records_per_file = int(file_size_mb * 100)   # Conservative estimate
            
            # Estimate memory usage (typically 2-3x file size when loaded)
            estimated_memory_mb = file_size_mb * 2.5
            
            # Recommend batch size based on memory
            if batch_size is None:
                if estimated_memory_mb < 100:
                    recommended_batch_size = 1000
                elif estimated_memory_mb < 500:
                    recommended_batch_size = 500
                else:
                    recommended_batch_size = 100
            else:
                recommended_batch_size = batch_size
            
            iterator = DatasetFileIterator(
                file_path=file_path,
                record_count=records_per_file,
                file_size_bytes=file_size_bytes,
                estimated_memory_mb=estimated_memory_mb,
                batch_size_recommendation=recommended_batch_size
            )
            
            iterators.append(iterator)
        
        logger.info(f"✅ Created {len(iterators)} file iterators for dataset {dataset_id}")
        return iterators
    
    def get_dataset_statistics(self, dataset_id: int) -> Dict[str, Any]:
        """Get comprehensive statistics for a dataset."""
        
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return {}
        
        iterators = self.get_file_iterators(dataset_id)
        
        # Calculate aggregate statistics
        total_file_size_mb = sum(it.file_size_bytes for it in iterators) / (1024 * 1024)
        total_estimated_memory_mb = sum(it.estimated_memory_mb for it in iterators)
        total_records = sum(it.record_count for it in iterators)
        
        statistics = {
            'dataset_info': {
                'id': metadata.dataset_id,
                'name': metadata.dataset_name,
                'type': metadata.dataset_type,
                'symbols': metadata.symbols,
                'creation_date': metadata.creation_timestamp.isoformat()
            },
            'data_volume': {
                'total_sequences': metadata.total_sequences,
                'total_records': total_records,
                'file_count': len(metadata.file_paths),
                'total_file_size_mb': total_file_size_mb,
                'estimated_memory_mb': total_estimated_memory_mb
            },
            'data_quality': {
                'quality_score': metadata.data_quality_score,
                'feature_completeness': metadata.feature_completeness,
                'label_completeness': metadata.label_completeness
            },
            'data_characteristics': {
                'feature_count': metadata.feature_count,
                'label_count': metadata.label_count,
                'sequence_length': metadata.sequence_length,
                'timeframes': metadata.timeframes,
                'technical_indicators': metadata.technical_indicators
            },
            'processing_info': {
                'data_source': metadata.data_source,
                'date_range': f"{metadata.date_range_start} to {metadata.date_range_end}",
                'run_id': metadata.run_id,
                'file_format': metadata.file_format
            },
            'recommended_usage': {
                'batch_size_range': f"{min(it.batch_size_recommendation for it in iterators)} - {max(it.batch_size_recommendation for it in iterators)}" if iterators else "32",
                'memory_requirement_mb': total_estimated_memory_mb,
                'parallel_loading_recommended': len(iterators) > 1
            }
        }
        
        return statistics
    
    def search_datasets(self, query: str, limit: int = 20) -> List[DatasetMetadata]:
        """Search datasets by name, symbols, or indicators."""
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    
                    cur.execute("""
                        SELECT id FROM dev_training_dataset 
                        WHERE dataset_name ILIKE %s 
                           OR symbols ILIKE %s 
                           OR technical_indicators ILIKE %s
                        ORDER BY creation_timestamp DESC 
                        LIMIT %s
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", limit))
                    
                    dataset_ids = [row['id'] for row in cur.fetchall()]
                    
                    # Get full metadata
                    results = []
                    for dataset_id in dataset_ids:
                        metadata = self.get_dataset_metadata(dataset_id)
                        if metadata:
                            results.append(metadata)
                    
                    logger.info(f"✅ Found {len(results)} datasets matching '{query}'")
                    return results
                    
        except Exception as e:
            logger.error(f"❌ Dataset search failed: {e}")
            return []
    
    def validate_dataset_availability(self, dataset_id: int) -> Dict[str, Any]:
        """Validate that all dataset files are accessible and readable."""
        
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return {'valid': False, 'error': f'Dataset {dataset_id} not found'}
        
        validation_results = {
            'valid': True,
            'dataset_id': dataset_id,
            'dataset_name': metadata.dataset_name,
            'total_files': len(metadata.file_paths),
            'accessible_files': 0,
            'missing_files': [],
            'file_details': [],
            'total_size_mb': 0
        }
        
        for file_path in metadata.file_paths:
            file_detail = {
                'path': file_path,
                'exists': os.path.exists(file_path),
                'readable': False,
                'size_mb': 0
            }
            
            if file_detail['exists']:
                try:
                    file_detail['size_mb'] = os.path.getsize(file_path) / (1024 * 1024)
                    file_detail['readable'] = os.access(file_path, os.R_OK)
                    if file_detail['readable']:
                        validation_results['accessible_files'] += 1
                        validation_results['total_size_mb'] += file_detail['size_mb']
                except Exception as e:
                    file_detail['error'] = str(e)
                    validation_results['valid'] = False
            else:
                validation_results['missing_files'].append(file_path)
                validation_results['valid'] = False
            
            validation_results['file_details'].append(file_detail)
        
        logger.info(f"✅ Validated dataset {dataset_id}: {validation_results['accessible_files']}/{validation_results['total_files']} files accessible")
        return validation_results