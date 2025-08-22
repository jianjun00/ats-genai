"""
Training Dataset DAO for managing training dataset records in the database.

This module provides data access operations for the dev_training_dataset table,
including CRUD operations, querying, and linking with runs table.
"""

import asyncio
import asyncpg
from typing import List, Dict, Optional, Any
from datetime import datetime, date
from dataclasses import dataclass, field
import json
import logging

from config.environment import Environment

logger = logging.getLogger(__name__)

@dataclass
class TrainingDatasetRecord:
    """Data class representing a training dataset record."""
    
    id: Optional[int] = None
    dataset_name: str = ""
    run_id: int = 0
    creation_timestamp: Optional[datetime] = None
    
    # Data structure information
    total_sequences: int = 0
    sequence_length: int = 0
    prediction_horizon: int = 0
    feature_count: int = 0
    label_count: int = 0
    
    # Symbol and date range information
    symbols: List[str] = field(default_factory=list)
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    
    # File storage information
    features_file_path: Optional[str] = None
    labels_file_path: Optional[str] = None
    metadata_file_path: Optional[str] = None
    
    # Generation configuration
    gin_config_path: Optional[str] = None
    generation_parameters: Optional[Dict[str, Any]] = None
    
    # Data quality metrics
    data_quality_score: float = 0.0
    feature_completeness: float = 0.0
    label_completeness: float = 0.0
    outlier_ratio: float = 0.0
    missing_data_ratio: float = 0.0
    
    # Processing metrics
    generation_duration_seconds: int = 0
    file_size_mb: float = 0.0
    
    # Data sources used
    data_sources: List[str] = field(default_factory=list)
    
    # Status and validation
    status: str = "created"
    validation_results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    # Versioning and lineage
    parent_dataset_id: Optional[int] = None
    version_tag: Optional[str] = None
    
    # Audit fields
    created_by: str = "system"
    last_modified: Optional[datetime] = None

@dataclass
class TrainingDatasetSummary:
    """Summary view of training dataset with run information."""
    
    id: int
    dataset_name: str
    run_id: int
    run_type: Optional[str] = None
    run_start_time: Optional[datetime] = None
    run_status: Optional[str] = None
    creation_timestamp: Optional[datetime] = None
    total_sequences: int = 0
    sequence_length: int = 0
    prediction_horizon: int = 0
    feature_count: int = 0
    label_count: int = 0
    symbol_count: int = 0
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    data_quality_score: float = 0.0
    feature_completeness: float = 0.0
    label_completeness: float = 0.0
    generation_duration_seconds: int = 0
    file_size_mb: float = 0.0
    status: str = "created"
    version_tag: Optional[str] = None
    parent_dataset_id: Optional[int] = None

class TrainingDatasetDAO:
    """Data Access Object for training dataset operations."""
    
    def __init__(self, env: Optional[Environment] = None):
        self.env = env or Environment()
        self.table_name = self.env.get_table_name("training_dataset")
        self.runs_table_name = self.env.get_table_name("runs")
        self.summary_view_name = f"{self.table_name}_summary"
        
    async def create_training_dataset(self, 
                                    dataset: TrainingDatasetRecord,
                                    conn: Optional[asyncpg.Connection] = None) -> int:
        """Create a new training dataset record."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"""
            INSERT INTO {self.table_name} (
                dataset_name, run_id, total_sequences, sequence_length, prediction_horizon,
                feature_count, label_count, symbols, date_range_start, date_range_end,
                features_file_path, labels_file_path, metadata_file_path,
                gin_config_path, generation_parameters, data_quality_score,
                feature_completeness, label_completeness, outlier_ratio,
                missing_data_ratio, generation_duration_seconds, file_size_mb,
                data_sources, status, validation_results, error_message,
                parent_dataset_id, version_tag, created_by
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
            ) RETURNING id
            """
            
            dataset_id = await conn.fetchval(
                query,
                dataset.dataset_name,
                dataset.run_id,
                dataset.total_sequences,
                dataset.sequence_length,
                dataset.prediction_horizon,
                dataset.feature_count,
                dataset.label_count,
                dataset.symbols,
                dataset.date_range_start,
                dataset.date_range_end,
                dataset.features_file_path,
                dataset.labels_file_path,
                dataset.metadata_file_path,
                dataset.gin_config_path,
                json.dumps(dataset.generation_parameters) if dataset.generation_parameters else None,
                dataset.data_quality_score,
                dataset.feature_completeness,
                dataset.label_completeness,
                dataset.outlier_ratio,
                dataset.missing_data_ratio,
                dataset.generation_duration_seconds,
                dataset.file_size_mb,
                dataset.data_sources,
                dataset.status,
                json.dumps(dataset.validation_results) if dataset.validation_results else None,
                dataset.error_message,
                dataset.parent_dataset_id,
                dataset.version_tag,
                dataset.created_by
            )
            
            logger.info(f"Created training dataset '{dataset.dataset_name}' with ID {dataset_id}")
            return dataset_id
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def get_training_dataset_by_id(self, 
                                       dataset_id: int,
                                       conn: Optional[asyncpg.Connection] = None) -> Optional[TrainingDatasetRecord]:
        """Get training dataset by ID."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"SELECT * FROM {self.table_name} WHERE id = $1"
            record = await conn.fetchrow(query, dataset_id)
            
            if record:
                return self._record_to_dataset(record)
            return None
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def get_training_dataset_by_name(self, 
                                         dataset_name: str,
                                         conn: Optional[asyncpg.Connection] = None) -> Optional[TrainingDatasetRecord]:
        """Get training dataset by name."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"SELECT * FROM {self.table_name} WHERE dataset_name = $1"
            record = await conn.fetchrow(query, dataset_name)
            
            if record:
                return self._record_to_dataset(record)
            return None
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def list_training_datasets(self, 
                                   limit: int = 50,
                                   offset: int = 0,
                                   run_id: Optional[int] = None,
                                   status: Optional[str] = None,
                                   conn: Optional[asyncpg.Connection] = None) -> List[TrainingDatasetRecord]:
        """List training datasets with optional filtering."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            conditions = []
            params = []
            param_count = 0
            
            if run_id is not None:
                param_count += 1
                conditions.append(f"run_id = ${param_count}")
                params.append(run_id)
            
            if status is not None:
                param_count += 1
                conditions.append(f"status = ${param_count}")
                params.append(status)
            
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            param_count += 1
            params.append(limit)
            limit_clause = f" LIMIT ${param_count}"
            
            param_count += 1
            params.append(offset)
            offset_clause = f" OFFSET ${param_count}"
            
            query = f"""
            SELECT * FROM {self.table_name}
            {where_clause}
            ORDER BY creation_timestamp DESC
            {limit_clause}
            {offset_clause}
            """
            
            records = await conn.fetch(query, *params)
            return [self._record_to_dataset(record) for record in records]
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def list_training_dataset_summaries(self, 
                                            limit: int = 50,
                                            offset: int = 0,
                                            run_type: Optional[str] = None,
                                            status: Optional[str] = None,
                                            conn: Optional[asyncpg.Connection] = None) -> List[TrainingDatasetSummary]:
        """List training dataset summaries with run information."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            conditions = []
            params = []
            param_count = 0
            
            if run_type is not None:
                param_count += 1
                conditions.append(f"run_type = ${param_count}")
                params.append(run_type)
            
            if status is not None:
                param_count += 1
                conditions.append(f"status = ${param_count}")
                params.append(status)
            
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            param_count += 1
            params.append(limit)
            limit_clause = f" LIMIT ${param_count}"
            
            param_count += 1
            params.append(offset)
            offset_clause = f" OFFSET ${param_count}"
            
            query = f"""
            SELECT * FROM {self.summary_view_name}
            {where_clause}
            ORDER BY creation_timestamp DESC
            {limit_clause}
            {offset_clause}
            """
            
            records = await conn.fetch(query, *params)
            return [self._record_to_summary(record) for record in records]
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def update_training_dataset_status(self, 
                                           dataset_id: int,
                                           status: str,
                                           error_message: Optional[str] = None,
                                           validation_results: Optional[Dict[str, Any]] = None,
                                           conn: Optional[asyncpg.Connection] = None) -> bool:
        """Update training dataset status and validation information."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"""
            UPDATE {self.table_name}
            SET status = $1,
                error_message = $2,
                validation_results = $3,
                last_modified = NOW()
            WHERE id = $4
            """
            
            result = await conn.execute(
                query,
                status,
                error_message,
                json.dumps(validation_results) if validation_results else None,
                dataset_id
            )
            
            return result == "UPDATE 1"
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def update_dataset_quality_metrics(self, 
                                           dataset_id: int,
                                           data_quality_score: float,
                                           feature_completeness: float,
                                           label_completeness: float,
                                           outlier_ratio: float,
                                           missing_data_ratio: float,
                                           conn: Optional[asyncpg.Connection] = None) -> bool:
        """Update data quality metrics for a dataset."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"""
            UPDATE {self.table_name}
            SET data_quality_score = $1,
                feature_completeness = $2,
                label_completeness = $3,
                outlier_ratio = $4,
                missing_data_ratio = $5,
                last_modified = NOW()
            WHERE id = $6
            """
            
            result = await conn.execute(
                query,
                data_quality_score,
                feature_completeness,
                label_completeness,
                outlier_ratio,
                missing_data_ratio,
                dataset_id
            )
            
            return result == "UPDATE 1"
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def get_datasets_by_run_id(self, 
                                   run_id: int,
                                   conn: Optional[asyncpg.Connection] = None) -> List[TrainingDatasetRecord]:
        """Get all datasets generated by a specific run."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"""
            SELECT * FROM {self.table_name} 
            WHERE run_id = $1 
            ORDER BY creation_timestamp DESC
            """
            
            records = await conn.fetch(query, run_id)
            return [self._record_to_dataset(record) for record in records]
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def get_dataset_statistics(self, 
                                   conn: Optional[asyncpg.Connection] = None) -> Dict[str, Any]:
        """Get overall training dataset statistics."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_datasets,
                COUNT(CASE WHEN status = 'created' THEN 1 END) as created_count,
                COUNT(CASE WHEN status = 'validated' THEN 1 END) as validated_count,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count,
                COUNT(CASE WHEN status = 'archived' THEN 1 END) as archived_count,
                AVG(data_quality_score) as avg_quality_score,
                SUM(total_sequences) as total_sequences_generated,
                SUM(file_size_mb) as total_file_size_mb,
                AVG(generation_duration_seconds) as avg_generation_duration,
                COUNT(DISTINCT run_id) as unique_runs_count
            FROM {self.table_name}
            """
            
            record = await conn.fetchrow(query)
            return dict(record) if record else {}
            
        finally:
            if should_close_conn:
                await conn.close()
    
    async def delete_training_dataset(self, 
                                    dataset_id: int,
                                    conn: Optional[asyncpg.Connection] = None) -> bool:
        """Delete a training dataset record."""
        
        should_close_conn = conn is None
        if conn is None:
            conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            query = f"DELETE FROM {self.table_name} WHERE id = $1"
            result = await conn.execute(query, dataset_id)
            return result == "DELETE 1"
            
        finally:
            if should_close_conn:
                await conn.close()
    
    def _record_to_dataset(self, record: asyncpg.Record) -> TrainingDatasetRecord:
        """Convert database record to TrainingDatasetRecord."""
        
        generation_parameters = None
        if record['generation_parameters']:
            try:
                generation_parameters = json.loads(record['generation_parameters'])
            except (json.JSONDecodeError, TypeError):
                generation_parameters = None
        
        validation_results = None
        if record['validation_results']:
            try:
                validation_results = json.loads(record['validation_results'])
            except (json.JSONDecodeError, TypeError):
                validation_results = None
        
        return TrainingDatasetRecord(
            id=record['id'],
            dataset_name=record['dataset_name'],
            run_id=record['run_id'],
            creation_timestamp=record['creation_timestamp'],
            total_sequences=record['total_sequences'],
            sequence_length=record['sequence_length'],
            prediction_horizon=record['prediction_horizon'],
            feature_count=record['feature_count'],
            label_count=record['label_count'],
            symbols=record['symbols'] or [],
            date_range_start=record['date_range_start'],
            date_range_end=record['date_range_end'],
            features_file_path=record['features_file_path'],
            labels_file_path=record['labels_file_path'],
            metadata_file_path=record['metadata_file_path'],
            gin_config_path=record['gin_config_path'],
            generation_parameters=generation_parameters,
            data_quality_score=float(record['data_quality_score'] or 0.0),
            feature_completeness=float(record['feature_completeness'] or 0.0),
            label_completeness=float(record['label_completeness'] or 0.0),
            outlier_ratio=float(record['outlier_ratio'] or 0.0),
            missing_data_ratio=float(record['missing_data_ratio'] or 0.0),
            generation_duration_seconds=record['generation_duration_seconds'],
            file_size_mb=float(record['file_size_mb'] or 0.0),
            data_sources=record['data_sources'] or [],
            status=record['status'],
            validation_results=validation_results,
            error_message=record['error_message'],
            parent_dataset_id=record['parent_dataset_id'],
            version_tag=record['version_tag'],
            created_by=record['created_by'],
            last_modified=record['last_modified']
        )
    
    def _record_to_summary(self, record: asyncpg.Record) -> TrainingDatasetSummary:
        """Convert database record to TrainingDatasetSummary."""
        
        return TrainingDatasetSummary(
            id=record['id'],
            dataset_name=record['dataset_name'],
            run_id=record['run_id'],
            run_type=record['run_type'],
            run_start_time=record['run_start_time'],
            run_status=record['run_status'],
            creation_timestamp=record['creation_timestamp'],
            total_sequences=record['total_sequences'],
            sequence_length=record['sequence_length'],
            prediction_horizon=record['prediction_horizon'],
            feature_count=record['feature_count'],
            label_count=record['label_count'],
            symbol_count=record['symbol_count'] or 0,
            date_range_start=record['date_range_start'],
            date_range_end=record['date_range_end'],
            data_quality_score=float(record['data_quality_score'] or 0.0),
            feature_completeness=float(record['feature_completeness'] or 0.0),
            label_completeness=float(record['label_completeness'] or 0.0),
            generation_duration_seconds=record['generation_duration_seconds'],
            file_size_mb=float(record['file_size_mb'] or 0.0),
            status=record['status'],
            version_tag=record['version_tag'],
            parent_dataset_id=record['parent_dataset_id']
        )