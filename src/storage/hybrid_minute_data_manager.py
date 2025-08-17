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
        return await self._store_parquet_data(symbol, data, "cold", compress=True)
    
    async def _store_parquet_data(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        tier: str,
        compress: bool = True
    ) -> int:
        """Store data to Parquet files."""
        if not data:
            return 0
        
        # Group data by partition
        partitions = {}
        for bar in data:
            timestamp = bar['timestamp']
            if isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp)
            
            partition_key = self._get_partition_key(timestamp)
            if partition_key not in partitions:
                partitions[partition_key] = []
            partitions[partition_key].append(bar)
        
        total_stored = 0
        
        # Store each partition
        for partition_key, partition_data in partitions.items():
            try:
                stored = await self._write_parquet_partition(
                    symbol, partition_data, tier, compress
                )
                total_stored += stored
            except Exception as e:
                logger.error(f"Error writing partition {partition_key} for {symbol}: {e}")
                continue
        
        return total_stored
    
    def _get_partition_key(self, timestamp: datetime) -> str:
        """Generate partition key based on timestamp."""
        if self.config.partition_by == "year":
            return f"{timestamp.year}"
        elif self.config.partition_by == "year_month":
            return f"{timestamp.year}_{timestamp.month:02d}"
        else:  # year_month_day
            return f"{timestamp.year}_{timestamp.month:02d}_{timestamp.day:02d}"
    
    async def _write_parquet_partition(
        self,
        symbol: str,
        data: List[Dict[str, Any]], 
        tier: str,
        compress: bool
    ) -> int:
        """Write a partition of data to Parquet file."""
        if not data:
            return 0
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Ensure proper data types
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Get file path
        file_path = self._get_file_path(symbol, df['timestamp'].iloc[0], tier)
        
        # Compression settings
        compression = self.config.compression if compress else None
        
        # Write to file in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.executor,
            self._write_parquet_file,
            df, file_path, compression
        )
        
        return len(df)
    
    def _write_parquet_file(self, df: pd.DataFrame, file_path: Path, compression: str):
        """Write DataFrame to Parquet file (runs in thread pool)."""
        # Handle existing file (append or overwrite)
        if file_path.exists():
            # Read existing data
            existing_df = pd.read_parquet(file_path)
            
            # Merge with new data (deduplicate by timestamp)
            combined_df = pd.concat([existing_df, df])
            combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
            combined_df = combined_df.sort_values('timestamp')
            df = combined_df
        
        # Write to Parquet
        df.to_parquet(
            file_path,
            compression=compression,
            index=False,
            engine='pyarrow'
        )
        
        logger.debug(f"Wrote {len(df)} records to {file_path}")
    
    async def query_minute_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Query minute data across hot and cold storage."""
        
        # Determine which storage tiers to query
        hot_cutoff = datetime.now() - timedelta(days=self.config.hot_data_days)
        
        hot_data = None
        cold_data = None
        
        # Query hot data (database) if needed
        if end_date > hot_cutoff:
            hot_start = max(start_date, hot_cutoff)
            try:
                hot_data = await self._query_hot_data(symbol, hot_start, end_date, columns)
            except Exception as e:
                logger.error(f"Error querying hot data: {e}")
        
        # Query cold data (files) if needed  
        if start_date < hot_cutoff:
            cold_end = min(end_date, hot_cutoff)
            try:
                cold_data = await self._query_cold_data(symbol, start_date, cold_end, columns)
            except Exception as e:
                logger.error(f"Error querying cold data: {e}")
        
        # Combine results
        dfs = []
        if cold_data is not None and not cold_data.empty:
            dfs.append(cold_data)
        if hot_data is not None and not hot_data.empty:
            dfs.append(hot_data)
        
        if not dfs:
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.sort_values('timestamp')
        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
        
        return combined_df
    
    async def _query_hot_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Query hot data from database."""
        table_name = env.get_table_name('minute_bars')
        
        if columns:
            column_list = ', '.join(columns)
        else:
            column_list = '*'
        
        query = f"""
        SELECT {column_list}
        FROM {table_name}
        WHERE symbol = $1 
          AND timestamp >= $2 
          AND timestamp <= $3
        ORDER BY timestamp
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, start_date, end_date)
        
        if not rows:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in rows])
        return df
    
    async def _query_cold_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Query cold data from Parquet files."""
        
        # Find relevant files
        files_to_read = self._find_relevant_files(symbol, start_date, end_date)
        
        if not files_to_read:
            return pd.DataFrame()
        
        # Read files in parallel
        dfs = []
        for file_path in files_to_read:
            try:
                df = await self._read_parquet_file(file_path, columns)
                if df is not None and not df.empty:
                    # Filter by date range
                    df = df[
                        (df['timestamp'] >= start_date) &
                        (df['timestamp'] <= end_date) &
                        (df['symbol'] == symbol)
                    ]
                    if not df.empty:
                        dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue
        
        if not dfs:
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.sort_values('timestamp')
        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
        
        return combined_df
    
    def _find_relevant_files(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Path]:
        """Find Parquet files that might contain data in the date range."""
        files = []
        
        # Check both warm and cold storage
        for tier in ['warm', 'cold']:
            base_path = Path(self.config.base_data_path) / tier / symbol
            
            if not base_path.exists():
                continue
            
            # Walk directory structure based on partitioning scheme
            if self.config.partition_by == "year":
                for year in range(start_date.year, end_date.year + 1):
                    year_path = base_path / str(year)
                    if year_path.exists():
                        files.extend(year_path.glob("*.parquet"))
            
            elif self.config.partition_by == "year_month":
                current = start_date.replace(day=1)
                end = end_date.replace(day=1)
                
                while current <= end:
                    month_path = base_path / str(current.year) / f"{current.month:02d}"
                    if month_path.exists():
                        files.extend(month_path.glob("*.parquet"))
                    
                    # Move to next month
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1)
                    else:
                        current = current.replace(month=current.month + 1)
            
            else:  # year_month_day
                current = start_date.date()
                while current <= end_date.date():
                    day_path = base_path / str(current.year) / f"{current.month:02d}"
                    if day_path.exists():
                        pattern = f"*_{current.year}_{current.month:02d}_{current.day:02d}.parquet"
                        files.extend(day_path.glob(pattern))
                    
                    current += timedelta(days=1)
        
        return sorted(set(files))
    
    async def _read_parquet_file(
        self,
        file_path: Path,
        columns: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Read Parquet file in thread pool."""
        loop = asyncio.get_event_loop()
        
        try:
            df = await loop.run_in_executor(
                self.executor,
                self._read_parquet_sync,
                file_path, columns
            )
            return df
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None
    
    def _read_parquet_sync(self, file_path: Path, columns: Optional[List[str]]) -> pd.DataFrame:
        """Synchronous Parquet file read."""
        return pd.read_parquet(file_path, columns=columns)
    
    async def archive_old_data(self, days_old: int = 365) -> Dict[str, Any]:
        """Archive data older than specified days."""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        # Move data from hot to cold storage
        hot_archived = await self._archive_hot_data(cutoff_date)
        
        # Compress warm storage files
        warm_compressed = await self._compress_warm_data(cutoff_date)
        
        return {
            'cutoff_date': cutoff_date,
            'hot_records_archived': hot_archived,
            'warm_files_compressed': warm_compressed
        }
    
    async def _archive_hot_data(self, cutoff_date: datetime) -> int:
        """Move old data from database to cold storage."""
        table_name = env.get_table_name('minute_bars')
        
        # Query old data
        query = f"""
        SELECT * FROM {table_name}
        WHERE timestamp < $1
        ORDER BY symbol, timestamp
        """
        
        archived_count = 0
        
        async with self.pool.acquire() as conn:
            # Process in batches to avoid memory issues
            async with conn.transaction():
                cursor = await conn.cursor(query, cutoff_date)
                
                batch = []
                current_symbol = None
                
                async for row in cursor:
                    row_dict = dict(row)
                    symbol = row_dict['symbol']
                    
                    # Process batches by symbol
                    if current_symbol and current_symbol != symbol:
                        if batch:
                            await self._store_cold_data(current_symbol, batch)
                            archived_count += len(batch)
                            batch = []
                    
                    current_symbol = symbol
                    batch.append(row_dict)
                    
                    if len(batch) >= self.config.batch_size:
                        await self._store_cold_data(current_symbol, batch)
                        archived_count += len(batch)
                        batch = []
                
                # Process remaining batch
                if batch and current_symbol:
                    await self._store_cold_data(current_symbol, batch)
                    archived_count += len(batch)
                
                # Delete archived data from database
                if archived_count > 0:
                    delete_query = f"DELETE FROM {table_name} WHERE timestamp < $1"
                    await conn.execute(delete_query, cutoff_date)
        
        return archived_count
    
    async def _compress_warm_data(self, cutoff_date: datetime) -> int:
        """Compress warm data files to cold storage."""
        warm_path = Path(self.config.base_data_path) / "warm"
        compressed_count = 0
        
        if not warm_path.exists():
            return 0
        
        # Find old warm files
        for parquet_file in warm_path.rglob("*.parquet"):
            try:
                # Check file modification time
                file_mtime = datetime.fromtimestamp(parquet_file.stat().st_mtime)
                
                if file_mtime < cutoff_date:
                    # Read, compress, and move to cold storage
                    df = pd.read_parquet(parquet_file)
                    
                    if not df.empty:
                        symbol = df['symbol'].iloc[0]
                        data = df.to_dict('records')
                        
                        # Store in cold storage (compressed)
                        await self._store_cold_data(symbol, data)
                        
                        # Remove from warm storage
                        parquet_file.unlink()
                        compressed_count += 1
                        
                        logger.info(f"Compressed and moved {parquet_file} to cold storage")
                        
            except Exception as e:
                logger.error(f"Error compressing {parquet_file}: {e}")
                continue
        
        return compressed_count
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics across all tiers."""
        stats = {
            'hot_storage': await self._get_hot_storage_stats(),
            'cold_storage': await self._get_cold_storage_stats()
        }
        
        return stats
    
    async def _get_hot_storage_stats(self) -> Dict[str, Any]:
        """Get hot storage (database) statistics."""
        table_name = env.get_table_name('minute_bars')
        
        queries = {
            'total_records': f"SELECT COUNT(*) FROM {table_name}",
            'unique_symbols': f"SELECT COUNT(DISTINCT symbol) FROM {table_name}",
            'date_range': f"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name}",
            'table_size': f"SELECT pg_total_relation_size('{table_name}')" 
        }
        
        stats = {}
        
        async with self.pool.acquire() as conn:
            for stat_name, query in queries.items():
                try:
                    result = await conn.fetchval(query)
                    stats[stat_name] = result
                except Exception as e:
                    logger.error(f"Error getting {stat_name}: {e}")
                    stats[stat_name] = None
        
        return stats
    
    async def _get_cold_storage_stats(self) -> Dict[str, Any]:
        """Get cold storage (disk) statistics."""
        base_path = Path(self.config.base_data_path)
        
        stats = {
            'total_files': 0,
            'total_size_bytes': 0,
            'symbols': set(),
            'tiers': {}
        }
        
        for tier in ['warm', 'cold', 'archive']:
            tier_path = base_path / tier
            tier_stats = {'files': 0, 'size_bytes': 0, 'symbols': set()}
            
            if tier_path.exists():
                for parquet_file in tier_path.rglob("*.parquet"):
                    tier_stats['files'] += 1
                    tier_stats['size_bytes'] += parquet_file.stat().st_size
                    
                    # Extract symbol from path
                    symbol = parquet_file.parts[-3] if len(parquet_file.parts) >= 3 else 'unknown'
                    tier_stats['symbols'].add(symbol)
                    stats['symbols'].add(symbol)
            
            stats['tiers'][tier] = {
                'files': tier_stats['files'],
                'size_bytes': tier_stats['size_bytes'],
                'size_mb': tier_stats['size_bytes'] / (1024 * 1024),
                'symbols': len(tier_stats['symbols'])
            }
            
            stats['total_files'] += tier_stats['files']
            stats['total_size_bytes'] += tier_stats['size_bytes']
        
        stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
        stats['total_symbols'] = len(stats['symbols'])
        
        return stats
    
    async def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)


# Convenience functions
async def create_hybrid_manager(
    db_url: str,
    config: StorageConfig = None
) -> HybridMinuteDataManager:
    """Create hybrid storage manager with database connection."""
    pool = await asyncpg.create_pool(db_url, min_size=5, max_size=20)
    return HybridMinuteDataManager(pool, config)


async def migrate_existing_data(
    manager: HybridMinuteDataManager,
    source_path: str,
    symbol_mapping: Dict[str, str] = None
) -> Dict[str, Any]:
    """Migrate existing data from /home/jianjun/ats to new storage format."""
    source_base = Path(source_path)
    migrated = {'symbols': 0, 'files': 0, 'records': 0}
    
    if not source_base.exists():
        logger.warning(f"Source path {source_path} does not exist")
        return migrated
    
    # Process futures data (convert to stock-like format)
    futures_path = source_base / "FUT" / "30min"
    if futures_path.exists():
        for symbol_dir in futures_path.iterdir():
            if symbol_dir.is_dir():
                symbol = symbol_dir.name
                
                # Map futures symbols if needed
                if symbol_mapping and symbol in symbol_mapping:
                    target_symbol = symbol_mapping[symbol]
                else:
                    target_symbol = symbol
                
                logger.info(f"Migrating futures data for {symbol} -> {target_symbol}")
                
                try:
                    records = await _process_futures_symbol(symbol_dir, target_symbol)
                    if records:
                        # Store as cold data (historical)
                        await manager.store_minute_data(target_symbol, records, force_tier='cold')
                        migrated['records'] += len(records)
                        migrated['files'] += 1
                    
                    migrated['symbols'] += 1
                    
                except Exception as e:
                    logger.error(f"Error migrating {symbol}: {e}")
                    continue
    
    return migrated


async def _process_futures_symbol(symbol_dir: Path, target_symbol: str) -> List[Dict[str, Any]]:
    """Process futures data files for a symbol."""
    records = []
    
    for month_dir in symbol_dir.iterdir():
        if month_dir.is_dir():
            for parquet_file in month_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(parquet_file)
                    
                    # Convert 30-minute to 1-minute (interpolate)
                    df_1min = _interpolate_to_1min(df, target_symbol)
                    
                    if not df_1min.empty:
                        records.extend(df_1min.to_dict('records'))
                        
                except Exception as e:
                    logger.error(f"Error processing {parquet_file}: {e}")
                    continue
    
    return records


def _interpolate_to_1min(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Interpolate 30-minute data to 1-minute bars."""
    if df.empty:
        return df
    
    # Create 1-minute time index
    start_time = df.index.min()
    end_time = df.index.max()
    
    minute_index = pd.date_range(
        start=start_time,
        end=end_time,
        freq='1min'
    )
    
    # Reindex and interpolate
    df_reindexed = df.reindex(minute_index)
    
    # Forward fill OHLC data within each 30-minute period
    df_reindexed['open'] = df_reindexed['open'].fillna(method='ffill')
    df_reindexed['high'] = df_reindexed['high'].fillna(method='ffill')
    df_reindexed['low'] = df_reindexed['low'].fillna(method='ffill')
    df_reindexed['close'] = df_reindexed['close'].fillna(method='ffill')
    
    # Distribute volume evenly across 30 minutes
    df_reindexed['volume'] = df_reindexed['volume'].fillna(0) / 30
    
    # Add required fields
    df_reindexed['symbol'] = symbol
    df_reindexed['vendor'] = 'futures_converted'
    df_reindexed['timestamp'] = df_reindexed.index
    
    return df_reindexed.dropna(subset=['open', 'high', 'low', 'close'])