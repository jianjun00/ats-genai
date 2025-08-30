#!/usr/bin/env python3
"""
Ray-Powered Distributed EDA Engine for 8GB+ Financial Dataset

Handles massive financial datasets using distributed computing:
- Parallel column analysis across multiple workers
- Smart data partitioning by symbol/date ranges  
- In-memory caching of computed distributions
- Progressive result streaming to frontend

Performance target: 8GB dataset analysis in < 10 seconds
"""

import ray
import asyncio
import asyncpg
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import json
import os
from concurrent.futures import ThreadPoolExecutor

# Initialize Ray if not already initialized
if not ray.is_initialized():
    ray.init(
        # Configure for financial data processing
        object_store_memory=2_000_000_000,  # 2GB object store for caching
        num_cpus=None,  # Use all available CPUs
        # dashboard_host="0.0.0.0",  # Enable Ray dashboard
    )

@dataclass
class DistributionResult:
    column_name: str
    data_type: str
    statistics: Dict[str, Any]
    histogram: Optional[Dict[str, Any]] = None
    top_values: Optional[List[Dict[str, Any]]] = None
    computation_time: float = 0.0
    sample_size: int = 0

@ray.remote
class DatabaseWorker:
    """Ray actor for parallel database operations"""
    
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.connection_pool = None
    
    async def initialize_connection(self):
        """Initialize database connection pool for this worker"""
        # Debug: Print connection parameters for Ray worker
        import logging
        logging.info(f"Ray worker connecting to: {self.connection_params}")
        self.connection_pool = await asyncpg.create_pool(**self.connection_params)
        
    async def get_connection(self):
        if not self.connection_pool:
            await self.initialize_connection()
        return self.connection_pool
    
    async def analyze_numeric_column_partition(
        self, 
        table_name: str, 
        column_name: str,
        partition_filter: str,
        sample_percentage: float = 1.0
    ) -> DistributionResult:
        """Analyze numeric column for a specific data partition"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            pool = await self.get_connection()
            async with pool.acquire() as conn:
                # Smart sampling query with statistics
                sample_clause = f"TABLESAMPLE BERNOULLI({sample_percentage})" if sample_percentage < 100 else ""
                
                query = f"""
                WITH sampled_data AS (
                    SELECT {column_name}
                    FROM {table_name} {sample_clause}
                    WHERE {partition_filter}
                    AND {column_name} IS NOT NULL
                    LIMIT 100000
                ),
                stats AS (
                    SELECT 
                        COUNT(*) as count,
                        AVG({column_name}) as mean,
                        STDDEV({column_name}) as std,
                        MIN({column_name}) as min_val,
                        MAX({column_name}) as max_val,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column_name}) as q25,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column_name}) as median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column_name}) as q75
                    FROM sampled_data
                ),
                histogram AS (
                    SELECT 
                        WIDTH_BUCKET({column_name}, (SELECT min_val FROM stats), (SELECT max_val FROM stats), 20) as bucket,
                        COUNT(*) as frequency,
                        MIN({column_name}) as bucket_min,
                        MAX({column_name}) as bucket_max
                    FROM sampled_data
                    GROUP BY bucket
                    ORDER BY bucket
                )
                SELECT 
                    (SELECT row_to_json(stats) FROM stats) as statistics,
                    json_agg(json_build_object(
                        'bucket', bucket,
                        'count', frequency,
                        'min', bucket_min,
                        'max', bucket_max
                    ) ORDER BY bucket) as histogram
                FROM histogram;
                """
                
                result = await conn.fetchrow(query)
                
                if result and result['statistics']:
                    stats = json.loads(result['statistics'])
                    histogram_data = result['histogram'] if result['histogram'] else []
                    
                    return DistributionResult(
                        column_name=column_name,
                        data_type='numeric',
                        statistics=stats,
                        histogram={'bins': histogram_data} if histogram_data else None,
                        computation_time=asyncio.get_event_loop().time() - start_time,
                        sample_size=int(stats.get('count', 0))
                    )
                
        except Exception as e:
            logging.error(f"Error analyzing numeric column {column_name}: {e}")
            logging.error(f"Connection params: {self.connection_params}")
            import traceback
            logging.error(f"Full traceback: {traceback.format_exc()}")
        
        return DistributionResult(
            column_name=column_name,
            data_type='numeric',
            statistics={'error': f'Failed to analyze partition: {partition_filter}. Error: {str(e)}'},
            computation_time=asyncio.get_event_loop().time() - start_time
        )
    
    async def analyze_categorical_column_partition(
        self,
        table_name: str,
        column_name: str, 
        partition_filter: str,
        sample_percentage: float = 5.0
    ) -> DistributionResult:
        """Analyze categorical column for a specific data partition"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            pool = await self.get_connection()
            async with pool.acquire() as conn:
                sample_clause = f"TABLESAMPLE BERNOULLI({sample_percentage})" if sample_percentage < 100 else ""
                
                query = f"""
                WITH sampled_data AS (
                    SELECT {column_name}
                    FROM {table_name} {sample_clause}
                    WHERE {partition_filter}
                    AND {column_name} IS NOT NULL
                    LIMIT 500000
                ),
                value_counts AS (
                    SELECT 
                        {column_name} as value,
                        COUNT(*) as count
                    FROM sampled_data
                    GROUP BY {column_name}
                    ORDER BY COUNT(*) DESC
                    LIMIT 50
                ),
                summary_stats AS (
                    SELECT 
                        COUNT(DISTINCT {column_name}) as unique_count,
                        COUNT(*) as total_count
                    FROM sampled_data
                )
                SELECT 
                    (SELECT row_to_json(summary_stats) FROM summary_stats) as statistics,
                    json_agg(json_build_object('value', value, 'count', count) ORDER BY count DESC) as top_values
                FROM value_counts;
                """
                
                result = await conn.fetchrow(query)
                
                if result and result['statistics']:
                    stats = json.loads(result['statistics'])
                    top_values = result['top_values'] if result['top_values'] else []
                    
                    return DistributionResult(
                        column_name=column_name,
                        data_type='categorical',
                        statistics=stats,
                        top_values=top_values,
                        computation_time=asyncio.get_event_loop().time() - start_time,
                        sample_size=int(stats.get('total_count', 0))
                    )
                
        except Exception as e:
            logging.error(f"Error analyzing categorical column {column_name}: {e}")
        
        return DistributionResult(
            column_name=column_name,
            data_type='categorical', 
            statistics={'error': f'Failed to analyze partition: {partition_filter}'},
            computation_time=asyncio.get_event_loop().time() - start_time
        )

@ray.remote
class EDACoordinator:
    """Main coordinator for distributed EDA operations"""
    
    def __init__(self, connection_params: Dict[str, str], num_workers: int = 8):
        self.connection_params = connection_params
        self.num_workers = num_workers
        self.workers = []
        
        # Create worker pool
        for i in range(num_workers):
            worker = DatabaseWorker.remote(connection_params)
            self.workers.append(worker)
    
    def get_smart_partitions(self, table_name: str, column_name: str = 'date') -> List[str]:
        """Generate intelligent data partitions based on table characteristics"""
        
        if 'daily_prices' in table_name:
            # Time-based partitions for price data
            partitions = []
            
            # Recent data (higher sampling) - last 90 days
            partitions.append("date >= CURRENT_DATE - INTERVAL '90 days'")
            
            # Previous quarter
            partitions.append("date >= CURRENT_DATE - INTERVAL '180 days' AND date < CURRENT_DATE - INTERVAL '90 days'")
            
            # Previous year
            partitions.append("date >= CURRENT_DATE - INTERVAL '1 year' AND date < CURRENT_DATE - INTERVAL '180 days'")
            
            # Historical data (lower sampling)
            partitions.append("date < CURRENT_DATE - INTERVAL '1 year'")
            
            return partitions
            
        elif 'instrument' in table_name:
            # Symbol-based partitions for instrument data
            return [
                "symbol ~ '^[A-E]'",
                "symbol ~ '^[F-K]'", 
                "symbol ~ '^[L-P]'",
                "symbol ~ '^[Q-Z]'",
            ]
        
        # Default: no partitioning for smaller tables
        return ["TRUE"]
    
    def analyze_column_distributed(
        self,
        table_name: str,
        column_name: str,
        data_type: str
    ) -> DistributionResult:
        """Analyze a single column using distributed workers"""
        
        partitions = self.get_smart_partitions(table_name)
        
        # Determine if numeric or categorical
        is_numeric = any(dt in data_type.lower() for dt in ['int', 'float', 'numeric', 'decimal', 'real', 'double'])
        
        # Distribute work across available workers
        tasks = []
        
        for i, partition_filter in enumerate(partitions[:self.num_workers]):
            worker = self.workers[i % len(self.workers)]
            
            if is_numeric:
                task = worker.analyze_numeric_column_partition.remote(
                    table_name, column_name, partition_filter, sample_percentage=2.0
                )
            else:
                task = worker.analyze_categorical_column_partition.remote(
                    table_name, column_name, partition_filter, sample_percentage=10.0
                )
            
            tasks.append(task)
        
        # Collect results from all workers using Ray's async patterns
        results = []
        for task in tasks:
            try:
                result = ray.get(task)  # This will be called from sync context
                results.append(result)
            except Exception as e:
                logging.error(f"Ray task failed: {e}")
                results.append(DistributionResult(
                    column_name=column_name,
                    data_type=data_type,
                    statistics={'error': f'Task failed: {str(e)}'}
                ))
        
        # Merge results from different partitions
        return self.merge_partition_results(results, column_name, data_type)
    
    def merge_partition_results(
        self, 
        partition_results: List[DistributionResult], 
        column_name: str,
        data_type: str
    ) -> DistributionResult:
        """Merge analysis results from multiple partitions"""
        
        valid_results = [r for r in partition_results if isinstance(r, DistributionResult) and not r.statistics.get('error')]
        
        if not valid_results:
            return DistributionResult(
                column_name=column_name,
                data_type=data_type,
                statistics={'error': 'No valid data found across all partitions'}
            )
        
        total_time = sum(r.computation_time for r in valid_results)
        total_samples = sum(r.sample_size for r in valid_results)
        
        if any(dt in data_type.lower() for dt in ['int', 'float', 'numeric', 'decimal', 'real', 'double']):
            # Merge numeric statistics
            counts = [r.statistics.get('count', 0) for r in valid_results]
            means = [r.statistics.get('mean', 0) for r in valid_results]
            
            total_count = sum(counts)
            weighted_mean = sum(c * m for c, m in zip(counts, means)) / total_count if total_count > 0 else 0
            
            # Combine histograms (simplified)
            combined_histogram = None
            if valid_results[0].histogram:
                combined_histogram = valid_results[0].histogram  # Use first partition's histogram as representative
            
            merged_stats = {
                'count': total_count,
                'mean': weighted_mean,
                'sample_partitions': len(valid_results)
            }
            
            return DistributionResult(
                column_name=column_name,
                data_type='numeric',
                statistics=merged_stats,
                histogram=combined_histogram,
                computation_time=total_time,
                sample_size=total_samples
            )
        
        else:
            # Merge categorical statistics
            all_values = {}
            total_count = 0
            
            for result in valid_results:
                if result.top_values:
                    for value_info in result.top_values:
                        # Handle both dict and string formats
                        if isinstance(value_info, dict):
                            value = value_info.get('value', str(value_info))
                            count = value_info.get('count', 1)
                        else:
                            # If it's just a string/value, treat as single occurrence
                            value = str(value_info)
                            count = 1
                        all_values[value] = all_values.get(value, 0) + count
                
                total_count += result.statistics.get('total_count', 0)
            
            # Top values across all partitions
            sorted_values = sorted(all_values.items(), key=lambda x: x[1], reverse=True)[:20]
            top_values = [{'value': v, 'count': c} for v, c in sorted_values]
            
            merged_stats = {
                'unique_count': len(all_values),
                'total_count': total_count,
                'sample_partitions': len(valid_results)
            }
            
            return DistributionResult(
                column_name=column_name,
                data_type='categorical',
                statistics=merged_stats,
                top_values=top_values,
                computation_time=total_time,
                sample_size=total_samples
            )

class RayEDAService:
    """Main service interface for Ray-powered EDA"""
    
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.coordinator = EDACoordinator.remote(connection_params, num_workers=8)
        self.cache = {}  # Simple in-memory cache
    
    async def analyze_dataset_columns(
        self, 
        table_name: str, 
        columns: List[Dict[str, str]],
        max_columns: int = 6
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Analyze multiple columns in parallel and stream results"""
        
        # Limit columns for UI performance
        columns_to_analyze = columns[:max_columns]
        
        # Submit all column analyses to Ray in parallel
        analysis_tasks = []
        for col in columns_to_analyze:
            cache_key = f"{table_name}:{col['column_name']}"
            
            if cache_key in self.cache:
                # Return cached result immediately
                yield {
                    'column': col['column_name'],
                    'result': self.cache[cache_key],
                    'cached': True
                }
                continue
            
            task = self.coordinator.analyze_column_distributed.remote(
                table_name, 
                col['column_name'], 
                col['data_type']
            )
            analysis_tasks.append((col['column_name'], task))
        
        # Stream results as they complete
        for column_name, task in analysis_tasks:
            try:
                result = ray.get(task)  # This blocks until this specific task completes
                
                # Cache the result
                cache_key = f"{table_name}:{column_name}"
                self.cache[cache_key] = result
                
                yield {
                    'column': column_name,
                    'result': result,
                    'cached': False
                }
                
            except Exception as e:
                yield {
                    'column': column_name,
                    'result': DistributionResult(
                        column_name=column_name,
                        data_type='unknown',
                        statistics={'error': f'Ray analysis failed: {str(e)}'}
                    ),
                    'cached': False
                }
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring"""
        return {
            'cached_columns': len(self.cache),
            'ray_cluster_resources': ray.cluster_resources(),
            'ray_cluster_status': 'connected' if ray.is_initialized() else 'disconnected'
        }

# Global Ray EDA service instance
_ray_eda_service: Optional[RayEDAService] = None

def get_ray_eda_service() -> RayEDAService:
    """Get or create the global Ray EDA service instance"""
    global _ray_eda_service
    
    if _ray_eda_service is None:
        # Docker container database connection parameters
        # Ray workers need to connect to PostgreSQL container via Docker network
        connection_params = {
            'host': os.getenv('DB_HOST', 'postgres-dev'),  # Docker service name
            'port': int(os.getenv('DB_PORT', 5432)),       # Internal Docker port
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'dev_password'),
            'database': os.getenv('DB_NAME', 'dev_db'),
            'min_size': 2,
            'max_size': 20
        }
        
        _ray_eda_service = RayEDAService(connection_params)
    
    return _ray_eda_service

if __name__ == "__main__":
    # Test the Ray EDA system
    import asyncio
    
    async def test_ray_eda():
        service = get_ray_eda_service()
        
        # Test with a large table
        columns = [
            {'column_name': 'close', 'data_type': 'numeric'},
            {'column_name': 'volume', 'data_type': 'bigint'},
            {'column_name': 'symbol', 'data_type': 'text'},
        ]
        
        print("Testing Ray EDA on large dataset...")
        async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns):
            print(f"Column: {result['column']}")
            print(f"Cached: {result['cached']}")
            print(f"Sample size: {result['result'].sample_size}")
            print(f"Computation time: {result['result'].computation_time:.2f}s")
            print("---")
    
    asyncio.run(test_ray_eda())