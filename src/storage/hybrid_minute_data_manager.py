"""
Hybrid Storage Manager for 1-Minute Financial Data

Manages storage across PostgreSQL/TimescaleDB (hot cache) and disk-based 
Parquet files (cold storage) for optimal performance and cost efficiency.
"""

import os
import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, Union
from pathlib import Path
import logging
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
import gc

from config.environment import env

logger = logging.getLogger(__name__)


@dataclass
class StorageConfig:
    """Configuration for hybrid storage system."""
    
    # Storage paths
    base_data_path: str = "/home/jianjun/ats/data/STK/1min"
    
    # Hot/cold data thresholds
    hot_data_days: int = 30        # Keep in database for fast access
    warm_data_days: int = 90       # Keep uncompressed on disk
    cold_data_days: int = 365      # Compress and archive
    
    # Database configuration
    db_pool_size: int = 20
    db_timeout: int = 30
    
    # File organization
    partition_by: str = "year_month"  # "year", "year_month", or "year_month_day"
    compression: str = "snappy"       # "snappy", "lz4", "gzip"
    
    # Performance settings
    batch_size: int = 10000          # Records per batch operation
    max_concurrent_files: int = 4    # Parallel file operations
    memory_limit_mb: int = 2048      # Memory limit for operations


class HybridMinuteDataManager:
    """
    Hybrid storage manager for 1-minute financial data.
    
    Automatically manages data lifecycle between hot (database) and 
    cold (disk) storage based on age and access patterns.
    """
    
    def __init__(self, pool: asyncpg.Pool, config: StorageConfig = None):
        self.pool = pool
        self.config = config or StorageConfig()
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_concurrent_files)
        
        # Ensure storage directories exist
        self._ensure_storage_structure()
    
    def _ensure_storage_structure(self):
        """Create storage directory structure."""
        base_path = Path(self.config.base_data_path)
        base_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (base_path / "hot").mkdir(exist_ok=True)
        (base_path / "warm").mkdir(exist_ok=True) 
        (base_path / "cold").mkdir(exist_ok=True)
        (base_path / "archive").mkdir(exist_ok=True)
        
        logger.info(f"Storage structure ensured at {base_path}")
    
    def _get_file_path(
        self, 
        symbol: str, 
        timestamp: datetime, 
        storage_tier: str = "warm"
    ) -> Path:
        """Generate file path based on partitioning scheme."""
        base_path = Path(self.config.base_data_path) / storage_tier
        
        if self.config.partition_by == "year":
            path = base_path / symbol / f"{timestamp.year}"
            filename = f"{symbol}_{timestamp.year}.parquet"
        elif self.config.partition_by == "year_month":
            path = base_path / symbol / f"{timestamp.year}" / f"{timestamp.month:02d}"
            filename = f"{symbol}_{timestamp.year}_{timestamp.month:02d}.parquet"
        else:  # year_month_day
            path = base_path / symbol / f"{timestamp.year}" / f"{timestamp.month:02d}"
            filename = f"{symbol}_{timestamp.year}_{timestamp.month:02d}_{timestamp.day:02d}.parquet"
        
        path.mkdir(parents=True, exist_ok=True)
        return path / filename
    
    async def store_minute_data(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        force_tier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Store minute data using hybrid strategy.
        
        Args:
            symbol: Stock symbol
            data: List of minute bar dictionaries
            force_tier: Force storage to specific tier ('hot', 'warm', 'cold')
        
        Returns:
            Storage statistics
        """
        if not data:
            return {'stored_hot': 0, 'stored_cold': 0, 'errors': 0}
        
        # Sort data by timestamp
        data = sorted(data, key=lambda x: x['timestamp'])
        
        # Determine storage tier based on data age
        hot_cutoff = datetime.now() - timedelta(days=self.config.hot_data_days)
        warm_cutoff = datetime.now() - timedelta(days=self.config.warm_data_days)
        
        hot_data = []
        warm_data = []
        cold_data = []
        
        for bar in data:
            bar_time = bar['timestamp']
            if isinstance(bar_time, str):
                bar_time = pd.to_datetime(bar_time)
            
            if force_tier:
                if force_tier == 'hot':
                    hot_data.append(bar)
                elif force_tier == 'warm':
                    warm_data.append(bar)
                else:
                    cold_data.append(bar)
            elif bar_time > hot_cutoff:
                hot_data.append(bar)
            elif bar_time > warm_cutoff:
                warm_data.append(bar)
            else:
                cold_data.append(bar)
        
        results = {'stored_hot': 0, 'stored_cold': 0, 'stored_warm': 0, 'errors': 0}
        
        # Store hot data in database
        if hot_data:
            try:
                stored_count = await self._store_hot_data(symbol, hot_data)
                results['stored_hot'] = stored_count
            except Exception as e:
                logger.error(f"Error storing hot data for {symbol}: {e}")
                results['errors'] += len(hot_data)
        
        # Store warm data to disk (uncompressed)
        if warm_data:
            try:
                stored_count = await self._store_warm_data(symbol, warm_data)
                results['stored_warm'] = stored_count
            except Exception as e:
                logger.error(f"Error storing warm data for {symbol}: {e}")
                results['errors'] += len(warm_data)
        
        # Store cold data to disk (compressed)
        if cold_data:
            try:
                stored_count = await self._store_cold_data(symbol, cold_data)
                results['stored_cold'] = stored_count
            except Exception as e:
                logger.error(f"Error storing cold data for {symbol}: {e}")
                results['errors'] += len(cold_data)
        
        return results
    
    async def _store_hot_data(self, symbol: str, data: List[Dict[str, Any]]) -> int:
        """Store recent data in PostgreSQL/TimescaleDB for fast access."""
        if not data:
            return 0
        
        table_name = env.get_table_name('minute_bars')
        
        # Prepare insert query (from minute_data_pipeline.py)
        insert_query = f"""
        INSERT INTO {table_name} (
            symbol, timestamp, open, high, low, close, volume, vwap, trade_count, vendor,
            returns, sma_5, sma_20, ema_12, ema_26, macd, macd_signal, rsi,
            bb_upper, bb_middle, bb_lower, volume_sma, volume_ratio, volatility,
            quality_score, is_validated, data_source_flags
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18,
            $19, $20, $21, $22, $23, $24,
            $25, $26, $27
        ) ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            trade_count = EXCLUDED.trade_count,
            returns = EXCLUDED.returns,
            sma_5 = EXCLUDED.sma_5,
            sma_20 = EXCLUDED.sma_20,
            ema_12 = EXCLUDED.ema_12,
            ema_26 = EXCLUDED.ema_26,
            macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            rsi = EXCLUDED.rsi,
            bb_upper = EXCLUDED.bb_upper,
            bb_middle = EXCLUDED.bb_middle,
            bb_lower = EXCLUDED.bb_lower,
            volume_sma = EXCLUDED.volume_sma,
            volume_ratio = EXCLUDED.volume_ratio,
            volatility = EXCLUDED.volatility,
            quality_score = EXCLUDED.quality_score,
            is_validated = EXCLUDED.is_validated,
            data_source_flags = EXCLUDED.data_source_flags,
            updated_at = CURRENT_TIMESTAMP
        """
        
        stored_count = 0
        
        # Process in batches
        for i in range(0, len(data), self.config.batch_size):
            batch = data[i:i + self.config.batch_size]
            
            # Prepare batch data
            batch_data = []
            for bar in batch:
                batch_data.append((
                    bar.get('symbol', symbol),
                    bar.get('timestamp'),
                    bar.get('open'),
                    bar.get('high'),
                    bar.get('low'),
                    bar.get('close'),
                    bar.get('volume'),
                    bar.get('vwap'),
                    bar.get('trade_count'),
                    bar.get('vendor', 'polygon'),
                    bar.get('returns'),
                    bar.get('sma_5'),
                    bar.get('sma_20'),
                    bar.get('ema_12'),
                    bar.get('ema_26'),
                    bar.get('macd'),
                    bar.get('macd_signal'),
                    bar.get('rsi'),
                    bar.get('bb_upper'),
                    bar.get('bb_middle'),
                    bar.get('bb_lower'),
                    bar.get('volume_sma'),
                    bar.get('volume_ratio'),
                    bar.get('volatility'),
                    bar.get('quality_score', 0.5),
                    True,  # is_validated
                    bar.get('data_source_flags', {})
                ))
            
            # Execute batch insert
            async with self.pool.acquire() as conn:
                await conn.executemany(insert_query, batch_data)
                stored_count += len(batch_data)
        
        return stored_count
    
    async def _store_warm_data(self, symbol: str, data: List[Dict[str, Any]]) -> int:
        """Store warm data to disk in Parquet format (uncompressed)."""
        return await self._store_parquet_data(symbol, data, "warm", compress=False)
    
    async def _store_cold_data(self, symbol: str, data: List[Dict[str, Any]]) -> int:
        """Store cold data to disk in Parquet format (compressed)."""
        return await self._store_parquet_data(symbol, data, "cold", compress=True)\n    \n    async def _store_parquet_data(\n        self, \n        symbol: str, \n        data: List[Dict[str, Any]], \n        tier: str,\n        compress: bool = True\n    ) -> int:\n        \"\"\"Store data to Parquet files.\"\"\"\n        if not data:\n            return 0\n        \n        # Group data by partition\n        partitions = {}\n        for bar in data:\n            timestamp = bar['timestamp']\n            if isinstance(timestamp, str):\n                timestamp = pd.to_datetime(timestamp)\n            \n            partition_key = self._get_partition_key(timestamp)\n            if partition_key not in partitions:\n                partitions[partition_key] = []\n            partitions[partition_key].append(bar)\n        \n        total_stored = 0\n        \n        # Store each partition\n        for partition_key, partition_data in partitions.items():\n            try:\n                stored = await self._write_parquet_partition(\n                    symbol, partition_data, tier, compress\n                )\n                total_stored += stored\n            except Exception as e:\n                logger.error(f\"Error writing partition {partition_key} for {symbol}: {e}\")\n                continue\n        \n        return total_stored\n    \n    def _get_partition_key(self, timestamp: datetime) -> str:\n        \"\"\"Generate partition key based on timestamp.\"\"\"\n        if self.config.partition_by == \"year\":\n            return f\"{timestamp.year}\"\n        elif self.config.partition_by == \"year_month\":\n            return f\"{timestamp.year}_{timestamp.month:02d}\"\n        else:  # year_month_day\n            return f\"{timestamp.year}_{timestamp.month:02d}_{timestamp.day:02d}\"\n    \n    async def _write_parquet_partition(\n        self,\n        symbol: str,\n        data: List[Dict[str, Any]], \n        tier: str,\n        compress: bool\n    ) -> int:\n        \"\"\"Write a partition of data to Parquet file.\"\"\"\n        if not data:\n            return 0\n        \n        # Convert to DataFrame\n        df = pd.DataFrame(data)\n        \n        # Ensure proper data types\n        df['timestamp'] = pd.to_datetime(df['timestamp'])\n        df = df.sort_values('timestamp')\n        \n        # Get file path\n        file_path = self._get_file_path(symbol, df['timestamp'].iloc[0], tier)\n        \n        # Compression settings\n        compression = self.config.compression if compress else None\n        \n        # Write to file in thread pool to avoid blocking\n        loop = asyncio.get_event_loop()\n        await loop.run_in_executor(\n            self.executor,\n            self._write_parquet_file,\n            df, file_path, compression\n        )\n        \n        return len(df)\n    \n    def _write_parquet_file(self, df: pd.DataFrame, file_path: Path, compression: str):\n        \"\"\"Write DataFrame to Parquet file (runs in thread pool).\"\"\"\n        # Handle existing file (append or overwrite)\n        if file_path.exists():\n            # Read existing data\n            existing_df = pd.read_parquet(file_path)\n            \n            # Merge with new data (deduplicate by timestamp)\n            combined_df = pd.concat([existing_df, df])\n            combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')\n            combined_df = combined_df.sort_values('timestamp')\n            df = combined_df\n        \n        # Write to Parquet\n        df.to_parquet(\n            file_path,\n            compression=compression,\n            index=False,\n            engine='pyarrow'\n        )\n        \n        logger.debug(f\"Wrote {len(df)} records to {file_path}\")\n    \n    async def query_minute_data(\n        self,\n        symbol: str,\n        start_date: datetime,\n        end_date: datetime,\n        columns: Optional[List[str]] = None\n    ) -> pd.DataFrame:\n        \"\"\"Query minute data across hot and cold storage.\"\"\"\n        \n        # Determine which storage tiers to query\n        hot_cutoff = datetime.now() - timedelta(days=self.config.hot_data_days)\n        \n        hot_data = None\n        cold_data = None\n        \n        # Query hot data (database) if needed\n        if end_date > hot_cutoff:\n            hot_start = max(start_date, hot_cutoff)\n            try:\n                hot_data = await self._query_hot_data(symbol, hot_start, end_date, columns)\n            except Exception as e:\n                logger.error(f\"Error querying hot data: {e}\")\n        \n        # Query cold data (files) if needed  \n        if start_date < hot_cutoff:\n            cold_end = min(end_date, hot_cutoff)\n            try:\n                cold_data = await self._query_cold_data(symbol, start_date, cold_end, columns)\n            except Exception as e:\n                logger.error(f\"Error querying cold data: {e}\")\n        \n        # Combine results\n        dfs = []\n        if cold_data is not None and not cold_data.empty:\n            dfs.append(cold_data)\n        if hot_data is not None and not hot_data.empty:\n            dfs.append(hot_data)\n        \n        if not dfs:\n            return pd.DataFrame()\n        \n        combined_df = pd.concat(dfs, ignore_index=True)\n        combined_df = combined_df.sort_values('timestamp')\n        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')\n        \n        return combined_df\n    \n    async def _query_hot_data(\n        self,\n        symbol: str,\n        start_date: datetime,\n        end_date: datetime,\n        columns: Optional[List[str]] = None\n    ) -> pd.DataFrame:\n        \"\"\"Query hot data from database.\"\"\"\n        table_name = env.get_table_name('minute_bars')\n        \n        if columns:\n            column_list = ', '.join(columns)\n        else:\n            column_list = '*'\n        \n        query = f\"\"\"\n        SELECT {column_list}\n        FROM {table_name}\n        WHERE symbol = $1 \n          AND timestamp >= $2 \n          AND timestamp <= $3\n        ORDER BY timestamp\n        \"\"\"\n        \n        async with self.pool.acquire() as conn:\n            rows = await conn.fetch(query, symbol, start_date, end_date)\n        \n        if not rows:\n            return pd.DataFrame()\n        \n        # Convert to DataFrame\n        df = pd.DataFrame([dict(row) for row in rows])\n        return df\n    \n    async def _query_cold_data(\n        self,\n        symbol: str,\n        start_date: datetime,\n        end_date: datetime,\n        columns: Optional[List[str]] = None\n    ) -> pd.DataFrame:\n        \"\"\"Query cold data from Parquet files.\"\"\"\n        \n        # Find relevant files\n        files_to_read = self._find_relevant_files(symbol, start_date, end_date)\n        \n        if not files_to_read:\n            return pd.DataFrame()\n        \n        # Read files in parallel\n        dfs = []\n        for file_path in files_to_read:\n            try:\n                df = await self._read_parquet_file(file_path, columns)\n                if df is not None and not df.empty:\n                    # Filter by date range\n                    df = df[\n                        (df['timestamp'] >= start_date) &\n                        (df['timestamp'] <= end_date) &\n                        (df['symbol'] == symbol)\n                    ]\n                    if not df.empty:\n                        dfs.append(df)\n            except Exception as e:\n                logger.error(f\"Error reading {file_path}: {e}\")\n                continue\n        \n        if not dfs:\n            return pd.DataFrame()\n        \n        # Combine all DataFrames\n        combined_df = pd.concat(dfs, ignore_index=True)\n        combined_df = combined_df.sort_values('timestamp')\n        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')\n        \n        return combined_df\n    \n    def _find_relevant_files(\n        self,\n        symbol: str,\n        start_date: datetime,\n        end_date: datetime\n    ) -> List[Path]:\n        \"\"\"Find Parquet files that might contain data in the date range.\"\"\"\n        files = []\n        \n        # Check both warm and cold storage\n        for tier in ['warm', 'cold']:\n            base_path = Path(self.config.base_data_path) / tier / symbol\n            \n            if not base_path.exists():\n                continue\n            \n            # Walk directory structure based on partitioning scheme\n            if self.config.partition_by == \"year\":\n                for year in range(start_date.year, end_date.year + 1):\n                    year_path = base_path / str(year)\n                    if year_path.exists():\n                        files.extend(year_path.glob(\"*.parquet\"))\n            \n            elif self.config.partition_by == \"year_month\":\n                current = start_date.replace(day=1)\n                end = end_date.replace(day=1)\n                \n                while current <= end:\n                    month_path = base_path / str(current.year) / f\"{current.month:02d}\"\n                    if month_path.exists():\n                        files.extend(month_path.glob(\"*.parquet\"))\n                    \n                    # Move to next month\n                    if current.month == 12:\n                        current = current.replace(year=current.year + 1, month=1)\n                    else:\n                        current = current.replace(month=current.month + 1)\n            \n            else:  # year_month_day\n                current = start_date.date()\n                while current <= end_date.date():\n                    day_path = base_path / str(current.year) / f\"{current.month:02d}\"\n                    if day_path.exists():\n                        pattern = f\"*_{current.year}_{current.month:02d}_{current.day:02d}.parquet\"\n                        files.extend(day_path.glob(pattern))\n                    \n                    current += timedelta(days=1)\n        \n        return sorted(set(files))\n    \n    async def _read_parquet_file(\n        self,\n        file_path: Path,\n        columns: Optional[List[str]] = None\n    ) -> Optional[pd.DataFrame]:\n        \"\"\"Read Parquet file in thread pool.\"\"\"\n        loop = asyncio.get_event_loop()\n        \n        try:\n            df = await loop.run_in_executor(\n                self.executor,\n                self._read_parquet_sync,\n                file_path, columns\n            )\n            return df\n        except Exception as e:\n            logger.error(f\"Error reading {file_path}: {e}\")\n            return None\n    \n    def _read_parquet_sync(self, file_path: Path, columns: Optional[List[str]]) -> pd.DataFrame:\n        \"\"\"Synchronous Parquet file read.\"\"\"\n        return pd.read_parquet(file_path, columns=columns)\n    \n    async def archive_old_data(self, days_old: int = 365) -> Dict[str, Any]:\n        \"\"\"Archive data older than specified days.\"\"\"\n        cutoff_date = datetime.now() - timedelta(days=days_old)\n        \n        # Move data from hot to cold storage\n        hot_archived = await self._archive_hot_data(cutoff_date)\n        \n        # Compress warm storage files\n        warm_compressed = await self._compress_warm_data(cutoff_date)\n        \n        return {\n            'cutoff_date': cutoff_date,\n            'hot_records_archived': hot_archived,\n            'warm_files_compressed': warm_compressed\n        }\n    \n    async def _archive_hot_data(self, cutoff_date: datetime) -> int:\n        \"\"\"Move old data from database to cold storage.\"\"\"\n        table_name = env.get_table_name('minute_bars')\n        \n        # Query old data\n        query = f\"\"\"\n        SELECT * FROM {table_name}\n        WHERE timestamp < $1\n        ORDER BY symbol, timestamp\n        \"\"\"\n        \n        archived_count = 0\n        \n        async with self.pool.acquire() as conn:\n            # Process in batches to avoid memory issues\n            async with conn.transaction():\n                cursor = await conn.cursor(query, cutoff_date)\n                \n                batch = []\n                current_symbol = None\n                \n                async for row in cursor:\n                    row_dict = dict(row)\n                    symbol = row_dict['symbol']\n                    \n                    # Process batches by symbol\n                    if current_symbol and current_symbol != symbol:\n                        if batch:\n                            await self._store_cold_data(current_symbol, batch)\n                            archived_count += len(batch)\n                            batch = []\n                    \n                    current_symbol = symbol\n                    batch.append(row_dict)\n                    \n                    if len(batch) >= self.config.batch_size:\n                        await self._store_cold_data(current_symbol, batch)\n                        archived_count += len(batch)\n                        batch = []\n                \n                # Process remaining batch\n                if batch and current_symbol:\n                    await self._store_cold_data(current_symbol, batch)\n                    archived_count += len(batch)\n                \n                # Delete archived data from database\n                if archived_count > 0:\n                    delete_query = f\"DELETE FROM {table_name} WHERE timestamp < $1\"\n                    await conn.execute(delete_query, cutoff_date)\n        \n        return archived_count\n    \n    async def _compress_warm_data(self, cutoff_date: datetime) -> int:\n        \"\"\"Compress warm data files to cold storage.\"\"\"\n        warm_path = Path(self.config.base_data_path) / \"warm\"\n        compressed_count = 0\n        \n        if not warm_path.exists():\n            return 0\n        \n        # Find old warm files\n        for parquet_file in warm_path.rglob(\"*.parquet\"):\n            try:\n                # Check file modification time\n                file_mtime = datetime.fromtimestamp(parquet_file.stat().st_mtime)\n                \n                if file_mtime < cutoff_date:\n                    # Read, compress, and move to cold storage\n                    df = pd.read_parquet(parquet_file)\n                    \n                    if not df.empty:\n                        symbol = df['symbol'].iloc[0]\n                        data = df.to_dict('records')\n                        \n                        # Store in cold storage (compressed)\n                        await self._store_cold_data(symbol, data)\n                        \n                        # Remove from warm storage\n                        parquet_file.unlink()\n                        compressed_count += 1\n                        \n                        logger.info(f\"Compressed and moved {parquet_file} to cold storage\")\n                        \n            except Exception as e:\n                logger.error(f\"Error compressing {parquet_file}: {e}\")\n                continue\n        \n        return compressed_count\n    \n    async def get_storage_stats(self) -> Dict[str, Any]:\n        \"\"\"Get storage statistics across all tiers.\"\"\"\n        stats = {\n            'hot_storage': await self._get_hot_storage_stats(),\n            'cold_storage': await self._get_cold_storage_stats()\n        }\n        \n        return stats\n    \n    async def _get_hot_storage_stats(self) -> Dict[str, Any]:\n        \"\"\"Get hot storage (database) statistics.\"\"\"\n        table_name = env.get_table_name('minute_bars')\n        \n        queries = {\n            'total_records': f\"SELECT COUNT(*) FROM {table_name}\",\n            'unique_symbols': f\"SELECT COUNT(DISTINCT symbol) FROM {table_name}\",\n            'date_range': f\"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}\",\n            'table_size': f\"SELECT pg_total_relation_size('{table_name}')\" \n        }\n        \n        stats = {}\n        \n        async with self.pool.acquire() as conn:\n            for stat_name, query in queries.items():\n                try:\n                    result = await conn.fetchval(query)\n                    stats[stat_name] = result\n                except Exception as e:\n                    logger.error(f\"Error getting {stat_name}: {e}\")\n                    stats[stat_name] = None\n        \n        return stats\n    \n    async def _get_cold_storage_stats(self) -> Dict[str, Any]:\n        \"\"\"Get cold storage (disk) statistics.\"\"\"\n        base_path = Path(self.config.base_data_path)\n        \n        stats = {\n            'total_files': 0,\n            'total_size_bytes': 0,\n            'symbols': set(),\n            'tiers': {}\n        }\n        \n        for tier in ['warm', 'cold', 'archive']:\n            tier_path = base_path / tier\n            tier_stats = {'files': 0, 'size_bytes': 0, 'symbols': set()}\n            \n            if tier_path.exists():\n                for parquet_file in tier_path.rglob(\"*.parquet\"):\n                    tier_stats['files'] += 1\n                    tier_stats['size_bytes'] += parquet_file.stat().st_size\n                    \n                    # Extract symbol from path\n                    symbol = parquet_file.parts[-3] if len(parquet_file.parts) >= 3 else 'unknown'\n                    tier_stats['symbols'].add(symbol)\n                    stats['symbols'].add(symbol)\n            \n            stats['tiers'][tier] = {\n                'files': tier_stats['files'],\n                'size_bytes': tier_stats['size_bytes'],\n                'size_mb': tier_stats['size_bytes'] / (1024 * 1024),\n                'symbols': len(tier_stats['symbols'])\n            }\n            \n            stats['total_files'] += tier_stats['files']\n            stats['total_size_bytes'] += tier_stats['size_bytes']\n        \n        stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)\n        stats['total_symbols'] = len(stats['symbols'])\n        \n        return stats\n    \n    async def close(self):\n        \"\"\"Clean up resources.\"\"\"\n        self.executor.shutdown(wait=True)


# Convenience functions\nasync def create_hybrid_manager(\n    db_url: str,\n    config: StorageConfig = None\n) -> HybridMinuteDataManager:\n    \"\"\"Create hybrid storage manager with database connection.\"\"\"\n    pool = await asyncpg.create_pool(db_url, min_size=5, max_size=20)\n    return HybridMinuteDataManager(pool, config)\n\n\nasync def migrate_existing_data(\n    manager: HybridMinuteDataManager,\n    source_path: str,\n    symbol_mapping: Dict[str, str] = None\n) -> Dict[str, Any]:\n    \"\"\"Migrate existing data from /home/jianjun/ats to new storage format.\"\"\"\n    source_base = Path(source_path)\n    migrated = {'symbols': 0, 'files': 0, 'records': 0}\n    \n    if not source_base.exists():\n        logger.warning(f\"Source path {source_path} does not exist\")\n        return migrated\n    \n    # Process futures data (convert to stock-like format)\n    futures_path = source_base / \"FUT\" / \"30min\"\n    if futures_path.exists():\n        for symbol_dir in futures_path.iterdir():\n            if symbol_dir.is_dir():\n                symbol = symbol_dir.name\n                \n                # Map futures symbols if needed\n                if symbol_mapping and symbol in symbol_mapping:\n                    target_symbol = symbol_mapping[symbol]\n                else:\n                    target_symbol = symbol\n                \n                logger.info(f\"Migrating futures data for {symbol} -> {target_symbol}\")\n                \n                try:\n                    records = await _process_futures_symbol(symbol_dir, target_symbol)\n                    if records:\n                        # Store as cold data (historical)\n                        await manager.store_minute_data(target_symbol, records, force_tier='cold')\n                        migrated['records'] += len(records)\n                        migrated['files'] += 1\n                    \n                    migrated['symbols'] += 1\n                    \n                except Exception as e:\n                    logger.error(f\"Error migrating {symbol}: {e}\")\n                    continue\n    \n    return migrated\n\n\nasync def _process_futures_symbol(symbol_dir: Path, target_symbol: str) -> List[Dict[str, Any]]:\n    \"\"\"Process futures data files for a symbol.\"\"\"\n    records = []\n    \n    for month_dir in symbol_dir.iterdir():\n        if month_dir.is_dir():\n            for parquet_file in month_dir.glob(\"*.parquet\"):\n                try:\n                    df = pd.read_parquet(parquet_file)\n                    \n                    # Convert 30-minute to 1-minute (interpolate)\n                    df_1min = _interpolate_to_1min(df, target_symbol)\n                    \n                    if not df_1min.empty:\n                        records.extend(df_1min.to_dict('records'))\n                        \n                except Exception as e:\n                    logger.error(f\"Error processing {parquet_file}: {e}\")\n                    continue\n    \n    return records\n\n\ndef _interpolate_to_1min(df: pd.DataFrame, symbol: str) -> pd.DataFrame:\n    \"\"\"Interpolate 30-minute data to 1-minute bars.\"\"\"\n    if df.empty:\n        return df\n    \n    # Create 1-minute time index\n    start_time = df.index.min()\n    end_time = df.index.max()\n    \n    minute_index = pd.date_range(\n        start=start_time,\n        end=end_time,\n        freq='1min'\n    )\n    \n    # Reindex and interpolate\n    df_reindexed = df.reindex(minute_index)\n    \n    # Forward fill OHLC data within each 30-minute period\n    df_reindexed['open'] = df_reindexed['open'].fillna(method='ffill')\n    df_reindexed['high'] = df_reindexed['high'].fillna(method='ffill')\n    df_reindexed['low'] = df_reindexed['low'].fillna(method='ffill')\n    df_reindexed['close'] = df_reindexed['close'].fillna(method='ffill')\n    \n    # Distribute volume evenly across 30 minutes\n    df_reindexed['volume'] = df_reindexed['volume'].fillna(0) / 30\n    \n    # Add required fields\n    df_reindexed['symbol'] = symbol\n    df_reindexed['vendor'] = 'futures_converted'\n    df_reindexed['timestamp'] = df_reindexed.index\n    \n    return df_reindexed.dropna(subset=['open', 'high', 'low', 'close'])