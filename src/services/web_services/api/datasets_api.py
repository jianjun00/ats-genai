#!/usr/bin/env python3
"""
Datasets API for EDA table functionality
Provides endpoints for database table discovery and basic EDA
"""

from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncpg
import logging

# Import dataset service for feature metadata functionality  
from services.dataset_service import DatasetService

logger = logging.getLogger(__name__)

# Rename to avoid conflict with main datasets router
datasets_router = APIRouter()

# Response Models
class DatasetInfo(BaseModel):
    name: str
    row_count: Optional[int] = 0
    column_count: Optional[int] = 0
    table_type: str = "table"

class DatasetListResponse(BaseModel):
    datasets: List[DatasetInfo]
    total_count: int

class ColumnStats(BaseModel):
    data_type: str
    non_null_count: Optional[int] = 0
    unique_count: Optional[int] = 0
    mean: Optional[float] = None
    std: Optional[float] = None
    min_value: Optional[str] = None
    max_value: Optional[str] = None

class DatasetDistributionsResponse(BaseModel):
    columns: Dict[str, ColumnStats]
    distributions: Dict[str, Any] = {}

# Feature Metadata Response Models
class FeatureMetadataResponse(BaseModel):
    features: List[Dict[str, Any]]
    labels: List[Dict[str, Any]]
    metadata_version: str
    data_quality_metrics: Dict[str, Any]

class DatasetSearchResponse(BaseModel):
    datasets: List[Dict[str, Any]]
    total_count: int

class FeatureComparisonResponse(BaseModel):
    compatible: bool
    common_features: List[str]
    missing_in_dataset_1: List[str]
    missing_in_dataset_2: List[str]
    type_mismatches: List[Dict[str, Any]]
    shape_mismatches: List[Dict[str, Any]]

async def get_db_connection():
    """Get database connection with environment detection"""
    try:
        # Try dev environment first
        return await asyncpg.connect(
            host='ats-dev-postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
    except Exception:
        # Fallback to localhost
        return await asyncpg.connect(
            host='localhost',
            port=3432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )

@datasets_router.get("/", response_model=DatasetListResponse)
async def list_datasets():
    """List available database tables for EDA"""
    try:
        conn = await get_db_connection()
        
        # Query for tables with row counts
        tables_query = """
        SELECT 
            schemaname,
            tablename as name,
            COALESCE(n_tup_ins - n_tup_del, 0) as estimated_rows
        FROM pg_stat_user_tables 
        WHERE schemaname = 'public'
        AND tablename LIKE 'dev_%'
        ORDER BY tablename
        """
        
        tables = await conn.fetch(tables_query)
        
        datasets = []
        for table in tables:
            # Get actual row count and column count
            try:
                row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table['name']}")
                
                columns_query = """
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = $1
                """
                column_count = await conn.fetchval(columns_query, table['name'])
                
                datasets.append(DatasetInfo(
                    name=table['name'],
                    row_count=row_count,
                    column_count=column_count,
                    table_type="table"
                ))
            except Exception as e:
                logger.warning(f"Error getting stats for table {table['name']}: {e}")
                datasets.append(DatasetInfo(
                    name=table['name'],
                    row_count=table['estimated_rows'],
                    column_count=0,
                    table_type="table"
                ))
        
        await conn.close()
        
        return DatasetListResponse(
            datasets=datasets,
            total_count=len(datasets)
        )
        
    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list datasets: {str(e)}")

@datasets_router.get("/{table_name}/distributions", response_model=DatasetDistributionsResponse)
async def get_table_distributions(table_name: str):
    """Get column statistics and distributions for a table"""
    try:
        conn = await get_db_connection()
        
        # Validate table exists and get column info
        columns_query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = 'public'
        ORDER BY ordinal_position
        """
        
        columns = await conn.fetch(columns_query, table_name)
        if not columns:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        column_stats = {}
        distributions = {}
        
        for column in columns:
            col_name = column['column_name']
            col_type = column['data_type']
            
            try:
                # Basic stats for all columns
                non_null_count = await conn.fetchval(
                    f"SELECT COUNT({col_name}) FROM {table_name}"
                )
                unique_count = await conn.fetchval(
                    f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name}"
                )
                
                stats = ColumnStats(
                    data_type=col_type,
                    non_null_count=non_null_count,
                    unique_count=unique_count
                )
                
                # Numeric column additional stats
                if col_type in ['integer', 'bigint', 'numeric', 'double precision', 'real']:
                    try:
                        numeric_stats = await conn.fetchrow(
                            f"SELECT AVG({col_name}::numeric) as mean, STDDEV({col_name}::numeric) as std, MIN({col_name}::numeric) as min, MAX({col_name}::numeric) as max FROM {table_name}"
                        )
                        if numeric_stats:
                            stats.mean = float(numeric_stats['mean']) if numeric_stats['mean'] else None
                            stats.std = float(numeric_stats['std']) if numeric_stats['std'] else None
                            stats.min_value = str(numeric_stats['min']) if numeric_stats['min'] is not None else None
                            stats.max_value = str(numeric_stats['max']) if numeric_stats['max'] is not None else None
                        
                        # Simple histogram for numeric columns
                        if non_null_count > 0 and unique_count > 1:
                            histogram_query = f"""
                            WITH stats AS (
                                SELECT MIN({col_name}::numeric) as min_val, MAX({col_name}::numeric) as max_val
                                FROM {table_name}
                                WHERE {col_name} IS NOT NULL
                            ),
                            bins AS (
                                SELECT generate_series(0, 9) as bin_num,
                                       min_val + (max_val - min_val) * generate_series(0, 9) / 10.0 as bin_start,
                                       min_val + (max_val - min_val) * generate_series(1, 10) / 10.0 as bin_end
                                FROM stats
                            )
                            SELECT bin_num, bin_start, bin_end,
                                   COUNT(t.{col_name}) as count
                            FROM bins b
                            LEFT JOIN {table_name} t ON t.{col_name}::numeric >= b.bin_start AND t.{col_name}::numeric < b.bin_end
                            GROUP BY bin_num, bin_start, bin_end
                            ORDER BY bin_num
                            """
                            
                            histogram_data = await conn.fetch(histogram_query)
                            distributions[col_name] = {
                                "histogram": [
                                    {"bin": f"{row['bin_start']:.2f}-{row['bin_end']:.2f}", "count": row['count']}
                                    for row in histogram_data
                                ]
                            }
                    except Exception as e:
                        logger.warning(f"Error computing numeric stats for {col_name}: {e}")
                
                column_stats[col_name] = stats
                
            except Exception as e:
                logger.warning(f"Error processing column {col_name}: {e}")
                column_stats[col_name] = ColumnStats(
                    data_type=col_type,
                    non_null_count=0,
                    unique_count=0
                )
        
        await conn.close()
        
        return DatasetDistributionsResponse(
            columns=column_stats,
            distributions=distributions
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting table distributions for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get table distributions: {str(e)}")

# Initialize dataset service for feature metadata operations
dataset_service = None

def get_dataset_service():
    """Get or create dataset service instance"""
    global dataset_service
    if dataset_service is None:
        db_config = {
            'host': 'localhost',
            'port': 3432,
            'database': 'dev_db',
            'user': 'postgres',
            'password': 'dev_password'
        }
        dataset_service = DatasetService(db_config)
    return dataset_service

# Feature Metadata API Endpoints

@datasets_router.get("/training-datasets/{dataset_id}/feature-metadata", response_model=FeatureMetadataResponse)
async def get_training_dataset_feature_metadata(dataset_id: int = Path(..., description="Training dataset ID")):
    """Retrieve comprehensive feature metadata for a training dataset"""
    try:
        service = get_dataset_service()
        metadata = service.get_feature_metadata(dataset_id)
        
        if 'error' in metadata:
            raise HTTPException(status_code=404, detail=metadata['error'])
            
        return FeatureMetadataResponse(**metadata)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving feature metadata for dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve feature metadata: {str(e)}")

@datasets_router.get("/training-datasets/search", response_model=DatasetSearchResponse)  
async def search_datasets_by_features(
    features: List[str] = Query(..., description="Required feature names"),
    feature_types: Optional[List[str]] = Query(None, description="Feature types to filter by")
):
    """Find training datasets containing specific features or feature types"""
    try:
        service = get_dataset_service()
        datasets = service.find_datasets_by_features(features, feature_types)
        
        # Convert DatasetMetadata objects to dictionaries
        dataset_dicts = []
        for dataset in datasets:
            dataset_dict = {
                'id': dataset.id,
                'dataset_name': dataset.dataset_name,
                'symbols': dataset.symbols,
                'total_sequences': dataset.total_sequences,
                'sequence_length': dataset.sequence_length,
                'feature_count': dataset.feature_count,
                'label_count': dataset.label_count,
                'data_quality_score': dataset.data_quality_score,
                'creation_timestamp': dataset.creation_timestamp.isoformat() if dataset.creation_timestamp else None
            }
            dataset_dicts.append(dataset_dict)
        
        return DatasetSearchResponse(
            datasets=dataset_dicts,
            total_count=len(dataset_dicts)
        )
        
    except Exception as e:
        logger.error(f"Error searching datasets by features {features}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search datasets: {str(e)}")

@datasets_router.get("/training-datasets/{dataset_id_1}/compare/{dataset_id_2}", response_model=FeatureComparisonResponse)
async def compare_training_dataset_features(
    dataset_id_1: int = Path(..., description="First dataset ID"),
    dataset_id_2: int = Path(..., description="Second dataset ID")
):
    """Compare feature schemas between two training datasets for compatibility"""
    try:
        service = get_dataset_service()
        comparison = service.compare_feature_schemas(dataset_id_1, dataset_id_2)
        
        return FeatureComparisonResponse(**comparison)
        
    except Exception as e:
        logger.error(f"Error comparing datasets {dataset_id_1} and {dataset_id_2}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compare datasets: {str(e)}")

@datasets_router.get("/training-datasets", response_model=DatasetListResponse)
async def list_training_datasets():
    """List all available training datasets with basic metadata"""
    try:
        service = get_dataset_service()
        datasets = service.list_datasets()
        
        # Convert to DatasetInfo format
        dataset_infos = []
        for dataset in datasets:
            dataset_infos.append(DatasetInfo(
                name=dataset.dataset_name,
                row_count=dataset.total_sequences,
                column_count=dataset.feature_count + dataset.label_count,
                table_type="training_dataset"
            ))
        
        return DatasetListResponse(
            datasets=dataset_infos,
            total_count=len(dataset_infos)
        )
        
    except Exception as e:
        logger.error(f"Error listing training datasets: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list training datasets: {str(e)}")

@datasets_router.get("/training-datasets/{dataset_id}", response_model=Dict[str, Any])
async def get_training_dataset_details(dataset_id: int = Path(..., description="Training dataset ID")):
    """Get detailed information about a specific training dataset"""
    try:
        service = get_dataset_service()
        dataset = service.get_dataset(dataset_id)
        
        if not dataset:
            raise HTTPException(status_code=404, detail=f"Training dataset {dataset_id} not found")
        
        # Convert DatasetMetadata to dictionary with feature metadata
        result = {
            'id': dataset.id,
            'dataset_name': dataset.dataset_name,
            'symbols': dataset.symbols,
            'total_sequences': dataset.total_sequences,
            'sequence_length': dataset.sequence_length,
            'feature_count': dataset.feature_count,
            'label_count': dataset.label_count,
            'data_quality_score': dataset.data_quality_score,
            'creation_timestamp': dataset.creation_timestamp.isoformat() if dataset.creation_timestamp else None,
            'date_range_start': dataset.date_range_start.isoformat() if dataset.date_range_start else None,
            'date_range_end': dataset.date_range_end.isoformat() if dataset.date_range_end else None,
            'file_size_mb': dataset.file_size_mb,
            'feature_completeness': dataset.feature_completeness,
            'label_completeness': dataset.label_completeness
        }
        
        # Add feature metadata if available
        try:
            feature_metadata = service.get_feature_metadata(dataset_id)
            if 'error' not in feature_metadata:
                result['feature_metadata'] = feature_metadata
        except Exception as e:
            logger.warning(f"Could not retrieve feature metadata for dataset {dataset_id}: {e}")
            result['feature_metadata'] = None
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving training dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve training dataset: {str(e)}")