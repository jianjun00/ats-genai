#!/usr/bin/env python3
"""
Pre-computed Statistics Engine for Interactive EDA
Inspired by TensorFlow Data Validation (TFDV) histogram generation approach

Key Features:
- Pre-computes histogram statistics for all table columns
- Uses quantile-based binning for robust outlier handling
- Stores intermediate results for fast interactive filtering
- Integrates with Ray framework for distributed computation
- Supports incremental updates when data changes

Performance target: Interactive filtering in <100ms using pre-computed stats
"""

import ray
import asyncio
import asyncpg
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging
import json
import os
import pickle
import hashlib
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class HistogramBin:
    """Single histogram bin with bounds and statistics"""
    lower_bound: float
    upper_bound: float  # Inclusive upper bound (TFDV style)
    count: int
    frequency: float
    sample_values: List[Any] = None  # Store sample values for this bin

@dataclass
class PrecomputedHistogram:
    """Pre-computed histogram statistics for fast interactive filtering"""
    column_name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: int

    # Multiple histogram types (TFDV approach)
    standard_histogram: List[HistogramBin]  # Equal-width bins
    quantile_histogram: List[HistogramBin]  # Quantile-based bins

    # Summary statistics
    mean: Optional[float] = None
    std: Optional[float] = None
    min_val: Optional[Union[float, str]] = None
    max_val: Optional[Union[float, str]] = None
    median: Optional[float] = None

    # Top values for categorical/string data
    top_values: Optional[List[Dict[str, Any]]] = None

    # Metadata
    computation_time: float = 0.0
    last_updated: datetime = None
    data_hash: str = None  # Hash of source data for invalidation

@dataclass
class TableStatsProfile:
    """Complete statistical profile for a database table"""
    table_name: str
    column_profiles: Dict[str, PrecomputedHistogram]
    row_count: int
    creation_time: datetime
    last_updated: datetime
    schema_hash: str  # Hash of table schema for invalidation

@ray.remote
class HistogramComputeWorker:
    """Ray actor for parallel histogram computation"""

    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
        self.logger = logging.getLogger(f"{__name__}.HistogramWorker")

    async def compute_column_histogram(
        self,
        table_name: str,
        column_name: str,
        data_type: str,
        num_standard_bins: int = 20,
        num_quantile_bins: int = 10,
        max_top_values: int = 100
    ) -> PrecomputedHistogram:
        """
        Compute comprehensive histogram statistics for a single column
        Following TFDV's approach with both standard and quantile histograms
        """
        start_time = asyncio.get_event_loop().time()

        try:
            # Connect to database
            conn = await asyncpg.connect(**self.connection_params)

            # Basic statistics query
            basic_stats_query = f"""
                SELECT
                    COUNT(*) as total_count,
                    COUNT({column_name}) as non_null_count,
                    COUNT(DISTINCT {column_name}) as unique_count
                FROM {table_name}
            """

            basic_stats = await conn.fetchrow(basic_stats_query)
            total_count = basic_stats['total_count']
            non_null_count = basic_stats['non_null_count']
            null_count = total_count - non_null_count
            unique_count = basic_stats['unique_count']

            # Determine if column is numeric
            is_numeric = data_type.lower() in ['integer', 'bigint', 'numeric', 'decimal', 'real', 'double precision', 'float']

            if is_numeric and non_null_count > 0:
                # Numeric column: compute both histogram types
                standard_histogram = await self._compute_standard_histogram(
                    conn, table_name, column_name, num_standard_bins
                )
                quantile_histogram = await self._compute_quantile_histogram(
                    conn, table_name, column_name, num_quantile_bins
                )

                # Additional numeric statistics
                stats_query = f"""
                    SELECT
                        AVG({column_name}::NUMERIC) as mean,
                        STDDEV({column_name}::NUMERIC) as std,
                        MIN({column_name}::NUMERIC) as min_val,
                        MAX({column_name}::NUMERIC) as max_val,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column_name}::NUMERIC) as median
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                """
                stats_result = await conn.fetchrow(stats_query)

                histogram = PrecomputedHistogram(
                    column_name=column_name,
                    data_type=data_type,
                    total_count=total_count,
                    null_count=null_count,
                    unique_count=unique_count,
                    standard_histogram=standard_histogram,
                    quantile_histogram=quantile_histogram,
                    mean=float(stats_result['mean']) if stats_result['mean'] else None,
                    std=float(stats_result['std']) if stats_result['std'] else None,
                    min_val=float(stats_result['min_val']) if stats_result['min_val'] else None,
                    max_val=float(stats_result['max_val']) if stats_result['max_val'] else None,
                    median=float(stats_result['median']) if stats_result['median'] else None,
                    computation_time=asyncio.get_event_loop().time() - start_time,
                    last_updated=datetime.now()
                )

            else:
                # Categorical/string column: compute top values
                top_values = await self._compute_top_values(
                    conn, table_name, column_name, max_top_values
                )

                histogram = PrecomputedHistogram(
                    column_name=column_name,
                    data_type=data_type,
                    total_count=total_count,
                    null_count=null_count,
                    unique_count=unique_count,
                    standard_histogram=[],  # Empty for non-numeric
                    quantile_histogram=[],  # Empty for non-numeric
                    top_values=top_values,
                    computation_time=asyncio.get_event_loop().time() - start_time,
                    last_updated=datetime.now()
                )

            await conn.close()
            return histogram

        except Exception as e:
            self.logger.error(f"Error computing histogram for {table_name}.{column_name}: {e}")
            # Return empty histogram on error
            return PrecomputedHistogram(
                column_name=column_name,
                data_type=data_type,
                total_count=0,
                null_count=0,
                unique_count=0,
                standard_histogram=[],
                quantile_histogram=[],
                computation_time=asyncio.get_event_loop().time() - start_time,
                last_updated=datetime.now()
            )

    async def _compute_standard_histogram(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        column_name: str,
        num_bins: int
    ) -> List[HistogramBin]:
        """Compute equal-width histogram (TFDV standard histogram)"""

        # Get min/max values
        range_query = f"""
            SELECT
                MIN({column_name}::NUMERIC) as min_val,
                MAX({column_name}::NUMERIC) as max_val
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
        """
        range_result = await conn.fetchrow(range_query)
        min_val = float(range_result['min_val'])
        max_val = float(range_result['max_val'])

        if min_val == max_val:
            # Single value case
            return [HistogramBin(
                lower_bound=min_val,
                upper_bound=max_val,
                count=1,
                frequency=1.0
            )]

        # Create equal-width bins
        bin_width = (max_val - min_val) / num_bins
        bins = []

        for i in range(num_bins):
            lower = min_val + i * bin_width
            upper = min_val + (i + 1) * bin_width if i < num_bins - 1 else max_val

            # Count values in this bin (inclusive upper bound)
            if i == num_bins - 1:
                # Last bin includes max value
                bin_query = f"""
                    SELECT COUNT(*) as count
                    FROM {table_name}
                    WHERE {column_name}::NUMERIC >= {lower} AND {column_name}::NUMERIC <= {upper}
                """
            else:
                bin_query = f"""
                    SELECT COUNT(*) as count
                    FROM {table_name}
                    WHERE {column_name}::NUMERIC >= {lower} AND {column_name}::NUMERIC < {upper}
                """

            bin_result = await conn.fetchrow(bin_query)
            count = bin_result['count']

            bins.append(HistogramBin(
                lower_bound=lower,
                upper_bound=upper,
                count=count,
                frequency=count  # Will be normalized later
            ))

        # Normalize frequencies
        total_count = sum(b.count for b in bins)
        if total_count > 0:
            for bin in bins:
                bin.frequency = bin.count / total_count

        return bins

    async def _compute_quantile_histogram(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        column_name: str,
        num_bins: int
    ) -> List[HistogramBin]:
        """Compute quantile-based histogram (TFDV quantile histogram)"""

        # Compute quantile boundaries
        quantiles = [i / num_bins for i in range(num_bins + 1)]
        quantile_values = []

        for q in quantiles:
            quantile_query = f"""
                SELECT PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY {column_name}::NUMERIC) as quantile_val
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            """
            result = await conn.fetchrow(quantile_query)
            quantile_values.append(float(result['quantile_val']))

        # Create quantile bins
        bins = []
        for i in range(num_bins):
            lower = quantile_values[i]
            upper = quantile_values[i + 1]

            # Count values in this quantile bin
            if i == num_bins - 1:
                # Last bin includes max value
                bin_query = f"""
                    SELECT COUNT(*) as count
                    FROM {table_name}
                    WHERE {column_name}::NUMERIC >= {lower} AND {column_name}::NUMERIC <= {upper}
                """
            else:
                bin_query = f"""
                    SELECT COUNT(*) as count
                    FROM {table_name}
                    WHERE {column_name}::NUMERIC >= {lower} AND {column_name}::NUMERIC < {upper}
                """

            bin_result = await conn.fetchrow(bin_query)
            count = bin_result['count']

            bins.append(HistogramBin(
                lower_bound=lower,
                upper_bound=upper,
                count=count,
                frequency=count / num_bins  # Equal frequency by design
            ))

        return bins

    async def _compute_top_values(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        column_name: str,
        max_values: int
    ) -> List[Dict[str, Any]]:
        """Compute top values for categorical/string columns"""

        top_values_query = f"""
            SELECT
                {column_name} as value,
                COUNT(*) as count,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL) as frequency
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            GROUP BY {column_name}
            ORDER BY COUNT(*) DESC
            LIMIT {max_values}
        """

        results = await conn.fetch(top_values_query)
        return [
            {
                'value': str(row['value']),
                'count': row['count'],
                'frequency': float(row['frequency'])
            }
            for row in results
        ]

class PrecomputedStatsEngine:
    """
    Main engine for managing pre-computed statistics
    Integrates with Ray framework for distributed computation
    """

    def __init__(self, connection_params: Dict[str, str], cache_dir: str = "/tmp/ats_stats_cache"):
        self.connection_params = connection_params
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)

        # Initialize Ray workers
        self.num_workers = min(os.cpu_count() or 4, 8)  # Limit to 8 workers max
        self.workers = [
            HistogramComputeWorker.remote(connection_params)
            for _ in range(self.num_workers)
        ]

    async def compute_table_profile(
        self,
        table_name: str,
        force_recompute: bool = False,
        num_standard_bins: int = 20,
        num_quantile_bins: int = 10
    ) -> TableStatsProfile:
        """
        Compute complete statistical profile for a table
        Uses distributed Ray workers for parallel column analysis
        """
        self.logger.info(f"Computing statistical profile for table: {table_name}")

        # Check cache first
        if not force_recompute:
            cached_profile = await self._load_cached_profile(table_name)
            if cached_profile and await self._is_cache_valid(cached_profile):
                self.logger.info(f"Using cached profile for {table_name}")
                return cached_profile

        start_time = asyncio.get_event_loop().time()

        try:
            # Get table schema
            conn = await asyncpg.connect(**self.connection_params)

            schema_query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """
            columns = await conn.fetch(schema_query, table_name)

            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            row_count_result = await conn.fetchrow(count_query)
            row_count = row_count_result['row_count']

            await conn.close()

            # Distribute column analysis across Ray workers
            column_tasks = []
            for i, column_info in enumerate(columns):
                worker = self.workers[i % len(self.workers)]
                task = worker.compute_column_histogram.remote(
                    table_name=table_name,
                    column_name=column_info['column_name'],
                    data_type=column_info['data_type'],
                    num_standard_bins=num_standard_bins,
                    num_quantile_bins=num_quantile_bins
                )
                column_tasks.append(task)

            # Wait for all column analyses to complete
            column_histograms = await asyncio.gather(*[
                asyncio.wrap_future(ray.get_async(task)) for task in column_tasks
            ])

            # Build complete profile
            column_profiles = {
                hist.column_name: hist for hist in column_histograms
            }

            # Create schema hash for cache invalidation
            schema_hash = hashlib.md5(
                json.dumps([(c['column_name'], c['data_type']) for c in columns]).encode()
            ).hexdigest()

            profile = TableStatsProfile(
                table_name=table_name,
                column_profiles=column_profiles,
                row_count=row_count,
                creation_time=datetime.now(),
                last_updated=datetime.now(),
                schema_hash=schema_hash
            )

            # Cache the profile
            await self._save_cached_profile(profile)

            computation_time = asyncio.get_event_loop().time() - start_time
            self.logger.info(
                f"Computed profile for {table_name}: "
                f"{len(columns)} columns, {row_count:,} rows in {computation_time:.2f}s"
            )

            return profile

        except Exception as e:
            self.logger.error(f"Error computing profile for {table_name}: {e}")
            raise

    async def get_filtered_statistics(
        self,
        table_name: str,
        column_name: str,
        filters: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Get statistics for interactive filtering using pre-computed histograms
        This is the fast path - uses pre-computed stats instead of live queries
        """
        profile = await self.compute_table_profile(table_name)

        if column_name not in profile.column_profiles:
            raise ValueError(f"Column {column_name} not found in {table_name}")

        column_profile = profile.column_profiles[column_name]

        # For now, return the pre-computed statistics
        # TODO: Apply filters to histogram bins for interactive filtering
        result = {
            'column': column_name,
            'data_type': column_profile.data_type,
            'total_count': column_profile.total_count,
            'null_count': column_profile.null_count,
            'unique_count': column_profile.unique_count,
            'statistics': {
                'mean': column_profile.mean,
                'std': column_profile.std,
                'min': column_profile.min_val,
                'max': column_profile.max_val,
                'median': column_profile.median
            },
            'histogram': {
                'standard': [asdict(bin) for bin in column_profile.standard_histogram],
                'quantile': [asdict(bin) for bin in column_profile.quantile_histogram]
            },
            'top_values': column_profile.top_values,
            'computation_time': column_profile.computation_time,
            'cache_hit': True
        }

        return result

    async def _load_cached_profile(self, table_name: str) -> Optional[TableStatsProfile]:
        """Load cached profile from disk"""
        cache_file = self.cache_dir / f"{table_name}_profile.pkl"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load cache for {table_name}: {e}")
            return None

    async def _save_cached_profile(self, profile: TableStatsProfile) -> None:
        """Save profile to disk cache"""
        cache_file = self.cache_dir / f"{profile.table_name}_profile.pkl"

        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(profile, f)
        except Exception as e:
            self.logger.warning(f"Failed to save cache for {profile.table_name}: {e}")

    async def _is_cache_valid(self, profile: TableStatsProfile) -> bool:
        """Check if cached profile is still valid"""
        # For now, consider cache valid for 1 hour
        # TODO: Add more sophisticated validation (row count changes, schema changes)
        cache_age = datetime.now() - profile.last_updated
        return cache_age < timedelta(hours=1)

# Global instance for easy access
_precomputed_engine: Optional[PrecomputedStatsEngine] = None

def get_precomputed_stats_engine(connection_params: Dict[str, str] = None) -> PrecomputedStatsEngine:
    """Get or create the global precomputed stats engine"""
    global _precomputed_engine

    if _precomputed_engine is None and connection_params:
        _precomputed_engine = PrecomputedStatsEngine(connection_params)

    return _precomputed_engine