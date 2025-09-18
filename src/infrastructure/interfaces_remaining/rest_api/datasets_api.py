#!/usr/bin/env python3
"""
Datasets API for EDA table functionality
Provides endpoints for database table discovery and basic EDA
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncpg
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

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

@router.get("/", response_model=DatasetListResponse)
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

@router.get("/{table_name}/distributions", response_model=DatasetDistributionsResponse)
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