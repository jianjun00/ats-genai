"""
Dataset Metadata Service
Unified metadata management for all dataset types with automatic statistics computation
"""

import asyncio
import asyncpg
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class DatasetType(Enum):
    DATABASE_TABLE = "database_table"
    SINGLE_FILE = "single_file"
    SHARDED_FILES = "sharded_files"
    TRAINING_DATASET = "training_dataset"

@dataclass
class DatasetMetadata:
    name: str
    display_name: str
    dataset_type: DatasetType
    total_rows: Optional[int] = None
    total_columns: Optional[int] = None
    size_bytes: Optional[int] = None
    stats_computed: bool = False
    last_accessed_at: Optional[datetime] = None
    
    # Type-specific location info
    table_name: Optional[str] = None
    file_path: Optional[str] = None
    directory_path: Optional[str] = None
    training_config: Optional[Dict] = None

@dataclass
class ColumnMetadata:
    column_name: str
    data_type: str
    semantic_type: Optional[str] = None
    total_count: Optional[int] = None
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None

class DatasetMetadataService:
    """
    Unified metadata service that automatically computes and caches statistics
    for all dataset types on first access
    """
    
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self._pool: Optional[asyncpg.Pool] = None
        
    async def get_connection_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool"""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(**self.connection_params)
        return self._pool
        
    async def close(self):
        """Close database connection pool"""
        if self._pool:
            await self._pool.close()
    
    async def get_or_create_dataset_metadata(
        self, 
        dataset_name: str,
        dataset_type: DatasetType,
        **location_info
    ) -> DatasetMetadata:
        """
        Get existing dataset metadata or create new entry.
        Automatically triggers statistics computation on first access.
        """
        pool = await self.get_connection_pool()
        
        async with pool.acquire() as conn:
            # Check if dataset already exists
            existing = await conn.fetchrow(
                "SELECT * FROM dev_datasets WHERE name = $1",
                dataset_name
            )
            
            if existing:
                # Update last_accessed_at
                await conn.execute(
                    "UPDATE dev_datasets SET last_accessed_at = NOW() WHERE name = $1",
                    dataset_name
                )
                
                metadata = DatasetMetadata(
                    name=existing['name'],
                    display_name=existing['display_name'],
                    dataset_type=DatasetType(existing['dataset_type']),
                    total_rows=existing['total_rows'],
                    total_columns=existing['total_columns'],
                    size_bytes=existing['size_bytes'],
                    stats_computed=existing['stats_computed'],
                    last_accessed_at=existing['last_accessed_at'],
                    table_name=existing['table_name'],
                    file_path=existing['file_path'],
                    directory_path=existing['directory_path'],
                    training_config=existing['training_config']
                )
                
                # Trigger automatic stats computation if not done yet
                if not existing['stats_computed']:
                    logger.info(f"🔄 Auto-triggering statistics computation for {dataset_name}")
                    asyncio.create_task(self._compute_dataset_statistics(dataset_name))
                
                return metadata
            
            else:
                # Create new dataset entry
                display_name = dataset_name.replace('_', ' ').title()
                
                # Insert new dataset
                dataset_id = await conn.fetchval(
                    """
                    INSERT INTO dev_datasets (
                        name, display_name, dataset_type, table_name, file_path, 
                        directory_path, training_config, first_accessed_at, last_accessed_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW()) 
                    RETURNING id
                    """,
                    dataset_name,
                    display_name, 
                    dataset_type.value,
                    location_info.get('table_name'),
                    location_info.get('file_path'),
                    location_info.get('directory_path'),
                    json.dumps(location_info.get('training_config')) if location_info.get('training_config') else None
                )
                
                logger.info(f"📝 Created new dataset metadata: {dataset_name} (ID: {dataset_id})")
                
                # Immediately trigger stats computation in background
                logger.info(f"🚀 Auto-triggering statistics computation for new dataset: {dataset_name}")
                asyncio.create_task(self._compute_dataset_statistics(dataset_name))
                
                return DatasetMetadata(
                    name=dataset_name,
                    display_name=display_name,
                    dataset_type=dataset_type,
                    stats_computed=False,
                    **location_info
                )
    
    async def _compute_dataset_statistics(self, dataset_name: str):
        """
        Background task to compute comprehensive dataset statistics
        """
        pool = await self.get_connection_pool()
        start_time = time.time()
        
        try:
            async with pool.acquire() as conn:
                # Mark computation as started
                await conn.execute(
                    "UPDATE dev_datasets SET stats_computation_started_at = NOW() WHERE name = $1",
                    dataset_name
                )
                
                # Get dataset info
                dataset = await conn.fetchrow(
                    "SELECT * FROM dev_datasets WHERE name = $1",
                    dataset_name
                )
                
                if not dataset:
                    logger.error(f"Dataset {dataset_name} not found for statistics computation")
                    return
                
                dataset_type = DatasetType(dataset['dataset_type'])
                
                if dataset_type == DatasetType.DATABASE_TABLE:
                    await self._compute_database_table_stats(conn, dataset_name, dataset['table_name'])
                elif dataset_type == DatasetType.SINGLE_FILE:
                    await self._compute_file_stats(conn, dataset_name, dataset['file_path'])
                elif dataset_type == DatasetType.SHARDED_FILES:
                    await self._compute_sharded_files_stats(conn, dataset_name, dataset['directory_path'])
                elif dataset_type == DatasetType.TRAINING_DATASET:
                    await self._compute_training_dataset_stats(conn, dataset_name, dataset['training_config'])
                
                # Mark computation as completed
                computation_time = time.time() - start_time
                await conn.execute(
                    """
                    UPDATE dev_datasets 
                    SET stats_computed = TRUE,
                        stats_computation_completed_at = NOW(),
                        stats_computation_duration_seconds = $2
                    WHERE name = $1
                    """,
                    dataset_name,
                    computation_time
                )
                
                logger.info(f"✅ Statistics computation completed for {dataset_name} in {computation_time:.2f}s")
                
        except Exception as e:
            logger.error(f"❌ Statistics computation failed for {dataset_name}: {e}")
            # Mark as failed but don't set stats_computed=True
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE dev_datasets SET updated_at = NOW() WHERE name = $1",
                    dataset_name
                )
    
    async def _compute_database_table_stats(self, conn: asyncpg.Connection, dataset_name: str, table_name: str):
        """Compute statistics for database table"""
        logger.info(f"🔍 Computing database table statistics: {table_name}")
        
        # Get basic table info
        table_info = await conn.fetchrow(
            """
            SELECT 
                (SELECT reltuples::bigint FROM pg_class WHERE relname = $1) as estimated_rows,
                (SELECT count(*) FROM information_schema.columns WHERE table_name = $1) as column_count
            """,
            table_name
        )
        
        if not table_info:
            logger.warning(f"Table {table_name} not found")
            return
        
        # Get exact row count for smaller tables, use estimate for large ones
        if table_info['estimated_rows'] and table_info['estimated_rows'] < 10_000_000:
            actual_rows = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
        else:
            actual_rows = table_info['estimated_rows']
        
        # Update dataset metadata
        dataset_id = await conn.fetchval(
            """
            UPDATE dev_datasets 
            SET total_rows = $2, column_count = $3
            WHERE name = $1 
            RETURNING id
            """,
            dataset_name, actual_rows, table_info['column_count']
        )
        
        # Get column information
        columns_info = await conn.fetch(
            """
            SELECT 
                column_name, data_type, is_nullable, ordinal_position,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
            """,
            table_name
        )
        
        # Process each column
        for col in columns_info:
            await self._analyze_database_column(
                conn, dataset_id, table_name, col, actual_rows
            )
    
    async def _analyze_database_column(
        self, 
        conn: asyncpg.Connection, 
        dataset_id: int, 
        table_name: str, 
        col_info: dict,
        total_rows: int
    ):
        """Analyze individual database column statistics"""
        column_name = col_info['column_name']
        
        # Determine semantic type based on column name and data type
        semantic_type = self._infer_semantic_type(column_name, col_info['data_type'])
        
        try:
            # Get basic column statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT({column_name}) as non_null_count,
                COUNT(*) - COUNT({column_name}) as null_count,
                COUNT(DISTINCT {column_name}) as unique_count
            FROM {table_name}
            """
            
            basic_stats = await conn.fetchrow(stats_query)
            
            # Get min/max for appropriate types
            min_val, max_val, mean_val, std_val = None, None, None, None
            
            if col_info['data_type'] in ['integer', 'bigint', 'numeric', 'real', 'double precision']:
                numeric_stats = await conn.fetchrow(
                    f"SELECT MIN({column_name})::text, MAX({column_name})::text, AVG({column_name}), STDDEV({column_name}) FROM {table_name}"
                )
                if numeric_stats:
                    min_val, max_val, mean_val, std_val = numeric_stats
            
            elif col_info['data_type'] in ['character varying', 'text', 'character']:
                text_stats = await conn.fetchrow(
                    f"SELECT MIN({column_name}), MAX({column_name}) FROM {table_name}"
                )
                if text_stats:
                    min_val, max_val = text_stats
            
            # Get top values for categorical columns
            top_values = None
            if semantic_type in ['categorical', 'identifier'] and basic_stats['unique_count'] < 1000:
                top_values_raw = await conn.fetch(
                    f"""
                    SELECT {column_name}::text as value, COUNT(*) as count 
                    FROM {table_name} 
                    WHERE {column_name} IS NOT NULL
                    GROUP BY {column_name} 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 20
                    """
                )
                top_values = {row['value']: row['count'] for row in top_values_raw}
            
            # Insert column metadata
            await conn.execute(
                """
                INSERT INTO dev_dataset_columns (
                    dataset_id, column_name, ordinal_position, data_type, semantic_type,
                    is_nullable, total_count, null_count, unique_count, 
                    min_value, max_value, mean_value, std_value,
                    completeness_ratio, cardinality_ratio, top_values
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                """,
                dataset_id, column_name, col_info['ordinal_position'], 
                col_info['data_type'], semantic_type,
                col_info['is_nullable'] == 'YES',
                basic_stats['total_count'], basic_stats['null_count'], basic_stats['unique_count'],
                str(min_val) if min_val is not None else None,
                str(max_val) if max_val is not None else None,
                float(mean_val) if mean_val is not None else None,
                float(std_val) if std_val is not None else None,
                basic_stats['non_null_count'] / basic_stats['total_count'] if basic_stats['total_count'] > 0 else 0,
                basic_stats['unique_count'] / basic_stats['total_count'] if basic_stats['total_count'] > 0 else 0,
                json.dumps(top_values) if top_values else None
            )
            
            logger.debug(f"  ✓ Analyzed column: {column_name} ({semantic_type})")
            
        except Exception as e:
            logger.warning(f"  ⚠️  Failed to analyze column {column_name}: {e}")
    
    def _infer_semantic_type(self, column_name: str, data_type: str) -> str:
        """Infer semantic type from column name and PostgreSQL data type"""
        name_lower = column_name.lower()
        
        # Identifier columns
        if any(keyword in name_lower for keyword in ['id', 'uuid', 'key', 'symbol', 'ticker', 'code']):
            return 'identifier'
        
        # Date/timestamp columns
        if any(keyword in name_lower for keyword in ['date', 'time', 'created', 'updated', 'timestamp']):
            return 'date'
        
        # Boolean columns
        if data_type == 'boolean' or any(keyword in name_lower for keyword in ['is_', 'has_', 'active', 'enabled']):
            return 'boolean'
        
        # Numeric columns
        if data_type in ['integer', 'bigint', 'numeric', 'real', 'double precision']:
            if any(keyword in name_lower for keyword in ['price', 'amount', 'value', 'count', 'volume', 'rate', 'ratio']):
                return 'numeric'
            return 'numeric'
        
        # Text columns that should be categorical
        if data_type in ['character varying', 'text', 'character']:
            if any(keyword in name_lower for keyword in ['type', 'status', 'category', 'exchange', 'currency', 'vendor']):
                return 'categorical'
            if len(column_name) <= 50:  # Short text fields likely categorical
                return 'categorical'
            return 'text'
        
        return 'unknown'
    
    async def _compute_file_stats(self, conn: asyncpg.Connection, dataset_name: str, file_path: str):
        """Compute statistics for single file dataset"""
        logger.info(f"📁 Computing file statistics: {file_path}")
        # Implementation for file-based datasets
        pass
    
    async def _compute_sharded_files_stats(self, conn: asyncpg.Connection, dataset_name: str, directory_path: str):
        """Compute statistics for sharded files dataset"""
        logger.info(f"📁 Computing sharded files statistics: {directory_path}")
        # Implementation for sharded files
        pass
    
    async def _compute_training_dataset_stats(self, conn: asyncpg.Connection, dataset_name: str, training_config: dict):
        """Compute statistics for training dataset"""
        logger.info(f"🎯 Computing training dataset statistics: {training_config}")
        # Implementation for training datasets
        pass
    
    async def get_dataset_list(self, include_training: bool = False) -> List[Dict[str, Any]]:
        """Get list of all datasets with their metadata"""
        pool = await self.get_connection_pool()
        
        async with pool.acquire() as conn:
            query = """
            SELECT 
                name, display_name, dataset_type, total_rows, column_count,
                size_bytes, stats_computed, last_accessed_at, created_at,
                table_name, file_path, directory_path
            FROM dev_datasets
            """
            
            if not include_training:
                query += " WHERE dataset_type != 'training_dataset'"
                
            query += " ORDER BY last_accessed_at DESC NULLS LAST, name"
            
            datasets = await conn.fetch(query)
            
            result = []
            for ds in datasets:
                result.append({
                    'name': ds['name'],
                    'display_name': ds['display_name'],
                    'dataset_type': ds['dataset_type'],
                    'row_count': ds['total_rows'],
                    'column_count': ds['column_count'],
                    'size': self._format_size(ds['size_bytes']) if ds['size_bytes'] else 'Unknown',
                    'stats_computed': ds['stats_computed'],
                    'last_accessed': ds['last_accessed_at'],
                    'table_name': ds['table_name'],
                    'file_path': ds['file_path'],
                    'directory_path': ds['directory_path']
                })
            
            return result
    
    def _format_size(self, size_bytes: int) -> str:
        """Format byte size in human readable format"""
        for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    async def register_existing_tables(self):
        """Register all existing database tables as datasets"""
        pool = await self.get_connection_pool()
        
        async with pool.acquire() as conn:
            # Get all existing tables
            tables = await conn.fetch(
                """
                SELECT 
                    table_name,
                    (SELECT reltuples::bigint FROM pg_class WHERE relname = table_name) as estimated_rows
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                  AND table_name LIKE 'dev_%'
                ORDER BY table_name
                """
            )
            
            registered_count = 0
            for table in tables:
                table_name = table['table_name']
                
                # Check if already registered
                exists = await conn.fetchval(
                    "SELECT 1 FROM dev_datasets WHERE table_name = $1",
                    table_name
                )
                
                if not exists:
                    await self.get_or_create_dataset_metadata(
                        dataset_name=table_name,
                        dataset_type=DatasetType.DATABASE_TABLE,
                        table_name=table_name
                    )
                    registered_count += 1
            
            logger.info(f"📊 Registered {registered_count} new database tables as datasets")
            return registered_count

# Global service instance
_metadata_service: Optional[DatasetMetadataService] = None

def get_metadata_service(connection_params: Dict[str, str] = None) -> DatasetMetadataService:
    """Get or create global metadata service instance"""
    global _metadata_service
    
    if _metadata_service is None:
        if connection_params is None:
            # Default connection parameters
            connection_params = {
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres', 
                'password': 'dev_password',
                'database': 'dev_db'
            }
        
        _metadata_service = DatasetMetadataService(connection_params)
    
    return _metadata_service