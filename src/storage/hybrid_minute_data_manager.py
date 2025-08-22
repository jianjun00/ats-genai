"""
Hybrid Storage Manager for 1-Minute Financial Data

Manages storage across PostgreSQL/TimescaleDB (hot cache) and disk-based 
Parquet files (cold storage) for optimal performance and cost efficiency.

ENHANCED with integration for existing data structure at /home/jianjun/ats/data:
- Gap analysis across existing parquet files and database
- Smart backfill targeting only missing data periods
- Integration with existing SYMBOL/YEAR/MONTH parquet structure
- Resume capability with fine-grained checkpoints
"""

import os
import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, Union, Tuple
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
class DataGap:
    """Represents a gap in market data coverage."""
    symbol: str
    start_date: date
    end_date: date
    source: str  # 'parquet', 'database', 'both'
    gap_type: str  # 'missing', 'partial', 'stale'
    estimated_bars: int = 0
    tier: str = 'unknown'  # 'hot', 'warm', 'cold'

@dataclass
class StorageConfig:
    """Configuration for hybrid storage system."""
    
    # Storage paths - UPDATED to match existing structure
    base_data_path: str = "/home/jianjun/ats/data/STK/1min"
    existing_parquet_path: str = "/home/jianjun/ats/data/STK/1min/cold"  # Existing data
    
    # Hot/cold data thresholds
    hot_data_days: int = 30        # Keep in database for fast access
    warm_data_days: int = 90       # Keep uncompressed on disk
    cold_data_days: int = 365      # Compress and archive
    
    # Database configuration
    database_url: str = "postgresql://postgres:postgres@localhost:5433/dev_db"
    db_pool_size: int = 20
    db_timeout: int = 30
    
    # File organization - MATCHES existing SYMBOL/YEAR/MONTH structure
    partition_by: str = "year_month"  # Matches existing structure
    compression: str = "snappy"       # "snappy", "lz4", "gzip"
    
    # Performance settings
    batch_size: int = 10000          # Records per batch operation
    max_concurrent_files: int = 4    # Parallel file operations
    memory_limit_mb: int = 2048      # Memory limit for operations
    
    def get_tier_path(self, tier: str) -> Path:
        """Get path for specific storage tier."""
        return Path(self.base_data_path) / tier
    
    def get_existing_parquet_path(self, symbol: str, year: int, month: int) -> Path:
        """Get path for existing parquet file following SYMBOL/YEAR/MONTH structure."""
        return Path(self.existing_parquet_path) / symbol / str(year) / f"{month:02d}" / f"{symbol}_{year}_{month:02d}.parquet"


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
    
    async def analyze_data_gaps(self, symbols: List[str], start_date: date, end_date: date) -> List[DataGap]:
        """
        Analyze gaps in data coverage across all storage tiers and existing parquet files.
        
        Returns comprehensive list of missing data periods that need backfilling.
        """
        gaps = []
        
        logger.info(f"🔍 Analyzing data gaps for {len(symbols)} symbols from {start_date} to {end_date}")
        logger.info(f"   Checking existing parquet structure at: {self.config.existing_parquet_path}")
        
        for symbol in symbols:
            # Analyze existing parquet data coverage
            existing_gaps = await self._analyze_existing_parquet_gaps(symbol, start_date, end_date)
            
            # Analyze database data coverage  
            database_gaps = await self._analyze_database_gaps(symbol, start_date, end_date)
            
            # Merge and deduplicate gaps
            merged_gaps = self._merge_gaps(existing_gaps, database_gaps, symbol)
            gaps.extend(merged_gaps)
        
        logger.info(f"📊 Found {len(gaps)} data gaps requiring backfill")
        return gaps
    
    async def _analyze_existing_parquet_gaps(self, symbol: str, start_date: date, end_date: date) -> List[Tuple[date, date, str]]:
        """Analyze gaps in existing parquet file coverage."""
        gaps = []
        current_date = start_date
        
        logger.debug(f"Analyzing existing parquet gaps for {symbol}")
        
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            
            # Check existing parquet structure
            existing_parquet = self.config.get_existing_parquet_path(symbol, year, month)
            
            if not existing_parquet.exists():
                # Calculate month end for gap
                month_end = min(
                    end_date,
                    date(year, month, 1) + timedelta(days=32) - timedelta(days=1)
                )
                gaps.append((current_date, month_end, 'parquet'))
                logger.debug(f"Gap found: {symbol} {current_date} to {month_end} (missing parquet)")
            else:
                # File exists, check if it has data for our date range
                try:
                    df = pd.read_parquet(existing_parquet)
                    if df.empty:
                        month_end = min(
                            end_date,
                            date(year, month, 1) + timedelta(days=32) - timedelta(days=1)
                        )
                        gaps.append((current_date, month_end, 'parquet'))
                        logger.debug(f"Gap found: {symbol} {current_date} to {month_end} (empty parquet)")
                except Exception as e:
                    logger.warning(f"Could not read {existing_parquet}: {e}")
                    month_end = min(
                        end_date,
                        date(year, month, 1) + timedelta(days=32) - timedelta(days=1)
                    )
                    gaps.append((current_date, month_end, 'parquet'))
            
            # Move to next month
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)
        
        return gaps
    
    async def _analyze_database_gaps(self, symbol: str, start_date: date, end_date: date) -> List[Tuple[date, date, str]]:
        """Analyze gaps in database coverage."""
        gaps = []
        table_name = env.get_table_name('minute_bars')
        
        # Check if symbol exists in database
        async with self.pool.acquire() as conn:
            existing_dates = await conn.fetch(f"""
                SELECT DISTINCT DATE(timestamp) as date_only
                FROM {table_name}
                WHERE symbol = $1 
                AND DATE(timestamp) BETWEEN $2 AND $3
                ORDER BY date_only
            """, symbol, start_date, end_date)
        
        if not existing_dates:
            # Entire symbol missing from database
            gaps.append((start_date, end_date, 'database'))
            return gaps
        
        existing_dates_set = {row['date_only'] for row in existing_dates}
        
        # Find missing dates
        current_date = start_date
        gap_start = None
        
        while current_date <= end_date:
            if current_date not in existing_dates_set:
                if gap_start is None:
                    gap_start = current_date
            else:
                if gap_start is not None:
                    gaps.append((gap_start, current_date - timedelta(days=1), 'database'))
                    gap_start = None
            
            current_date += timedelta(days=1)
        
        # Handle gap extending to end date
        if gap_start is not None:
            gaps.append((gap_start, end_date, 'database'))
        
        return gaps
    
    def _merge_gaps(self, parquet_gaps: List[Tuple[date, date, str]], 
                   database_gaps: List[Tuple[date, date, str]], symbol: str) -> List[DataGap]:
        """Merge and prioritize gaps from different sources."""
        merged_gaps = []
        
        # Convert to DataGap objects and merge overlapping periods
        all_gaps = []
        
        for start, end, source in parquet_gaps:
            all_gaps.append(DataGap(
                symbol=symbol,
                start_date=start,
                end_date=end,
                source='parquet',
                gap_type='missing',
                estimated_bars=self._estimate_bars(start, end),
                tier='cold'
            ))
        
        for start, end, source in database_gaps:
            all_gaps.append(DataGap(
                symbol=symbol,
                start_date=start,
                end_date=end,
                source='database',
                gap_type='missing',
                estimated_bars=self._estimate_bars(start, end),
                tier='hot'
            ))
        
        # Sort by start date and merge overlapping gaps
        all_gaps.sort(key=lambda x: x.start_date)
        
        if not all_gaps:
            return []
        
        merged = [all_gaps[0]]
        
        for gap in all_gaps[1:]:
            last_gap = merged[-1]
            
            # Check if gaps overlap or are adjacent
            if gap.start_date <= last_gap.end_date + timedelta(days=1):
                # Merge gaps
                last_gap.end_date = max(last_gap.end_date, gap.end_date)
                last_gap.source = 'both' if last_gap.source != gap.source else last_gap.source
                last_gap.estimated_bars = self._estimate_bars(last_gap.start_date, last_gap.end_date)
            else:
                merged.append(gap)
        
        return merged
    
    def _estimate_bars(self, start_date: date, end_date: date) -> int:
        """Estimate number of minute bars in date range."""
        days = (end_date - start_date).days + 1
        trading_days = days * 0.72  # Approximate trading days (weekdays, holidays)
        return int(trading_days * 390)  # 390 minutes per trading day
    
    async def backfill_gaps_integrated(self, gaps: List[DataGap], 
                                     polygon_adapter, tiingo_adapter,
                                     batch_size: int = 10) -> Dict[str, Any]:
        """
        Backfill identified data gaps using integrated storage approach.
        
        Writes data to both existing parquet structure and database for comprehensive coverage.
        """
        stats = {
            'total_gaps': len(gaps),
            'completed_gaps': 0,
            'failed_gaps': 0,
            'total_bars_fetched': 0,
            'total_bars_stored_parquet': 0,
            'total_bars_stored_database': 0,
            'start_time': datetime.now(),
            'errors': []
        }
        
        logger.info(f"🚀 Starting integrated backfill of {len(gaps)} data gaps")
        logger.info(f"   Will write to both existing parquet structure and database")
        
        # Process gaps in batches
        for batch_start in range(0, len(gaps), batch_size):
            batch_end = min(batch_start + batch_size, len(gaps))
            batch_gaps = gaps[batch_start:batch_end]
            
            logger.info(f"📦 Processing gap batch {batch_start//batch_size + 1}: {len(batch_gaps)} gaps")
            
            # Process batch concurrently
            tasks = [
                self._backfill_single_gap_integrated(gap, polygon_adapter, tiingo_adapter, stats)
                for gap in batch_gaps
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update stats
            for result in results:
                if isinstance(result, Exception):
                    stats['failed_gaps'] += 1
                    stats['errors'].append(str(result))
                else:
                    stats['completed_gaps'] += 1
            
            # Save checkpoint after each batch
            await self._save_checkpoint_integrated(stats, batch_end)
            
            # Brief pause between batches
            await asyncio.sleep(0.5)
        
        # Final statistics
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
        
        logger.info(f"✅ Integrated backfill completed: {stats['completed_gaps']}/{stats['total_gaps']} successful")
        logger.info(f"   Parquet bars: {stats['total_bars_stored_parquet']:,}")
        logger.info(f"   Database bars: {stats['total_bars_stored_database']:,}")
        return stats
    
    async def _backfill_single_gap_integrated(self, gap: DataGap, polygon_adapter, tiingo_adapter, stats: Dict) -> bool:
        """Backfill a single data gap using separate vendor storage approach."""
        try:
            logger.info(f"🔄 Backfilling {gap.symbol}: {gap.start_date} to {gap.end_date} ({gap.estimated_bars} est. bars)")
            
            # Fetch data from vendors
            start_datetime = datetime.combine(gap.start_date, datetime.min.time())
            end_datetime = datetime.combine(gap.end_date, datetime.min.time())
            
            polygon_data, tiingo_data = await asyncio.gather(
                polygon_adapter.fetch_minute_bars_async(gap.symbol, start_datetime, end_datetime),
                tiingo_adapter.fetch_minute_bars_async(gap.symbol, start_datetime, end_datetime),
                return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(polygon_data, Exception):
                logger.warning(f"Polygon fetch failed for {gap.symbol}: {polygon_data}")
                polygon_data = []
            
            if isinstance(tiingo_data, Exception):
                logger.warning(f"Tiingo fetch failed for {gap.symbol}: {tiingo_data}")
                tiingo_data = []
            
            if not polygon_data and not tiingo_data:
                logger.warning(f"No data available for {gap.symbol} in gap period")
                return False
            
            total_bars_fetched = len(polygon_data) + len(tiingo_data)
            stats['total_bars_fetched'] += total_bars_fetched
            
            # Store vendor data separately in parquet (for data lineage)
            parquet_stored = await self._store_vendors_in_parquet_separately(gap.symbol, polygon_data, tiingo_data)
            
            # Store vendor data separately in database (key change!)
            database_results = await self._store_vendor_data_separately(gap.symbol, polygon_data, tiingo_data)
            
            total_parquet_stored = parquet_stored.get('polygon_stored', 0) + parquet_stored.get('tiingo_stored', 0)
            total_database_stored = database_results.get('polygon_stored', 0) + database_results.get('tiingo_stored', 0)
            
            stats['total_bars_stored_parquet'] += total_parquet_stored
            stats['total_bars_stored_database'] += total_database_stored
            
            # Enhanced logging showing separate vendor storage
            logger.info(f"✅ {gap.symbol}: P:{len(polygon_data)} T:{len(tiingo_data)} bars → "
                       f"Parquet(P:{parquet_stored.get('polygon_stored', 0)} T:{parquet_stored.get('tiingo_stored', 0)}) "
                       f"DB(P:{database_results.get('polygon_stored', 0)} T:{database_results.get('tiingo_stored', 0)})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error backfilling {gap.symbol}: {e}")
            return False
    
    async def _store_in_existing_parquet_structure(self, symbol: str, bars: List) -> int:
        """Store minute bars in existing parquet structure (SYMBOL/YEAR/MONTH)."""
        if not bars:
            return 0
        
        try:
            # Group bars by year/month to match existing structure
            bars_by_month = {}
            for bar in bars:
                year_month = (bar.timestamp.year, bar.timestamp.month)
                if year_month not in bars_by_month:
                    bars_by_month[year_month] = []
                bars_by_month[year_month].append(bar)
            
            total_stored = 0
            
            for (year, month), month_bars in bars_by_month.items():
                # Get existing parquet file path
                parquet_file = self.config.get_existing_parquet_path(symbol, year, month)
                
                # Ensure directory exists
                parquet_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Convert to DataFrame
                df_data = []
                for bar in month_bars:
                    df_data.append({
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vwap': getattr(bar, 'vwap', None),
                        'trade_count': getattr(bar, 'trade_count', None)
                    })
                
                df = pd.DataFrame(df_data)
                
                # Append to existing file or create new one
                if parquet_file.exists():
                    # Read existing data and merge
                    existing_df = pd.read_parquet(parquet_file)
                    combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['timestamp'])
                    combined_df = combined_df.sort_values('timestamp')
                else:
                    combined_df = df.sort_values('timestamp')
                
                # Write back to parquet using existing structure
                combined_df.to_parquet(parquet_file, index=False, compression='snappy')
                total_stored += len(df)
                
                logger.debug(f"📁 Stored {len(df)} bars to existing structure: {parquet_file}")
            
            return total_stored
            
        except Exception as e:
            logger.error(f"Failed to store in existing parquet structure for {symbol}: {e}")
            return 0
    
    async def _store_vendor_data_separately(self, symbol: str, polygon_data: List, tiingo_data: List) -> Dict[str, int]:
        """Store vendor data separately to preserve data lineage."""
        results = {'polygon_stored': 0, 'tiingo_stored': 0}
        
        # Store Polygon data with vendor = "polygon"
        if polygon_data:
            results['polygon_stored'] = await self._store_vendor_specific_data(symbol, polygon_data, "polygon")
        
        # Store Tiingo data with vendor = "tiingo"  
        if tiingo_data:
            results['tiingo_stored'] = await self._store_vendor_specific_data(symbol, tiingo_data, "tiingo")
        
        return results
    
    async def _store_vendor_specific_data(self, symbol: str, bars: List, vendor: str) -> int:
        """Store minute bars for a specific vendor."""
        if not bars:
            return 0
        
        try:
            table_name = env.get_table_name('minute_bars')
            
            # Prepare records for batch insertion with vendor specificity
            records = []
            for bar in bars:
                records.append((
                    symbol,
                    bar.timestamp,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    getattr(bar, 'vwap', None),
                    getattr(bar, 'trade_count', None),
                    vendor,  # Store actual vendor name
                    None,  # returns
                    None,  # sma_5
                    None,  # sma_20
                    None,  # ema_12
                    None,  # ema_26
                    None,  # macd
                    None,  # macd_signal
                    None,  # rsi
                    None,  # bb_upper
                    None,  # bb_middle
                    None,  # bb_lower
                    None,  # volume_sma
                    None,  # volume_ratio
                    None,  # volatility
                    0.8 if vendor == 'polygon' else 0.7,   # Higher quality score for primary vendor
                    True,  # is_validated
                    {'source_vendor': vendor, 'ingestion_time': datetime.now().isoformat()}  # Enhanced metadata
                ))
            
            # Batch insert with conflict resolution - IMPORTANT: conflict on (symbol, timestamp, vendor)
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
                ) ON CONFLICT (symbol, timestamp, vendor) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    vwap = EXCLUDED.vwap,
                    trade_count = EXCLUDED.trade_count,
                    quality_score = EXCLUDED.quality_score,
                    data_source_flags = EXCLUDED.data_source_flags,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            async with self.pool.acquire() as conn:
                await conn.executemany(insert_query, records)
            
            logger.debug(f"📊 Stored {len(records)} {vendor} bars for {symbol}")
            return len(records)
            
        except Exception as e:
            logger.error(f"Failed to store {vendor} data for {symbol}: {e}")
            return 0
    
    async def _store_vendors_in_parquet_separately(self, symbol: str, polygon_data: List, tiingo_data: List) -> Dict[str, int]:
        """Store vendor data separately in parquet files for complete data lineage."""
        results = {'polygon_stored': 0, 'tiingo_stored': 0}
        
        # Store Polygon data in separate vendor-specific parquet files
        if polygon_data:
            results['polygon_stored'] = await self._store_vendor_parquet(symbol, polygon_data, "polygon")
        
        # Store Tiingo data in separate vendor-specific parquet files
        if tiingo_data:
            results['tiingo_stored'] = await self._store_vendor_parquet(symbol, tiingo_data, "tiingo")
        
        return results
    
    async def _store_vendor_parquet(self, symbol: str, bars: List, vendor: str) -> int:
        """Store minute bars in vendor-specific parquet files."""
        if not bars:
            return 0
        
        try:
            # Group bars by year/month to match existing structure
            bars_by_month = {}
            for bar in bars:
                year_month = (bar.timestamp.year, bar.timestamp.month)
                if year_month not in bars_by_month:
                    bars_by_month[year_month] = []
                bars_by_month[year_month].append(bar)
            
            total_stored = 0
            
            for (year, month), month_bars in bars_by_month.items():
                # Create vendor-specific parquet file path: SYMBOL/YEAR/MONTH/SYMBOL_YEAR_MONTH_VENDOR.parquet
                parquet_dir = Path(self.config.existing_parquet_path) / symbol / str(year) / f"{month:02d}"
                parquet_dir.mkdir(parents=True, exist_ok=True)
                
                parquet_file = parquet_dir / f"{symbol}_{year}_{month:02d}_{vendor}.parquet"
                
                # Convert to DataFrame with vendor metadata
                df_data = []
                for bar in month_bars:
                    df_data.append({
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vwap': getattr(bar, 'vwap', None),
                        'trade_count': getattr(bar, 'trade_count', None),
                        'vendor': vendor,  # Include vendor in parquet data
                        'quality_score': 0.8 if vendor == 'polygon' else 0.7,
                        'ingestion_time': datetime.now().isoformat()
                    })
                
                df = pd.DataFrame(df_data)
                
                # Append to existing vendor file or create new one
                if parquet_file.exists():
                    existing_df = pd.read_parquet(parquet_file)
                    combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['timestamp'])
                    combined_df = combined_df.sort_values('timestamp')
                else:
                    combined_df = df.sort_values('timestamp')
                
                # Write vendor-specific parquet file
                combined_df.to_parquet(parquet_file, index=False, compression='snappy')
                total_stored += len(df)
                
                logger.debug(f"📁 Stored {len(df)} {vendor} bars to: {parquet_file}")
            
            return total_stored
            
        except Exception as e:
            logger.error(f"Failed to store {vendor} parquet data for {symbol}: {e}")
            return 0
    
    async def _save_checkpoint_integrated(self, stats: Dict, processed_count: int):
        """Save progress checkpoint for integrated backfill."""
        try:
            checkpoint_file = Path("checkpoints/integrated_backfill_checkpoint.json")
            checkpoint_file.parent.mkdir(exist_ok=True)
            
            checkpoint_data = {
                'timestamp': datetime.now().isoformat(),
                'processed_gaps': processed_count,
                'stats': {
                    'completed_gaps': stats['completed_gaps'],
                    'failed_gaps': stats['failed_gaps'],
                    'total_bars_fetched': stats['total_bars_fetched'],
                    'total_bars_stored_parquet': stats['total_bars_stored_parquet'],
                    'total_bars_stored_database': stats['total_bars_stored_database']
                }
            }
            
            import json
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    async def query_reconciled_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        reconciliation_strategy: str = "best_quality",
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Query data with on-the-fly reconciliation between vendors.
        
        Args:
            symbol: Stock symbol
            start_date: Start datetime
            end_date: End datetime  
            reconciliation_strategy: How to combine vendor data:
                - "best_quality": Use highest quality score data per timestamp
                - "polygon_priority": Prefer Polygon, fallback to Tiingo
                - "tiingo_priority": Prefer Tiingo, fallback to Polygon
                - "both": Return all vendor data with vendor column
            columns: Optional list of columns to return
        
        Returns:
            Reconciled DataFrame
        """
        table_name = env.get_table_name('minute_bars')
        
        if columns:
            column_list = ', '.join(columns)
        else:
            column_list = '*'
        
        # Query all vendor data for the symbol/timerange
        query = f"""
        SELECT {column_list}
        FROM {table_name}
        WHERE symbol = $1 
          AND timestamp >= $2 
          AND timestamp <= $3
          AND vendor IN ('polygon', 'tiingo')
        ORDER BY timestamp, vendor
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, start_date, end_date)
        
        if not rows:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in rows])
        
        # Apply reconciliation strategy
        if reconciliation_strategy == "both":
            # Return all data with vendor information
            return df
        
        elif reconciliation_strategy == "best_quality":
            # For each timestamp, pick the record with highest quality_score
            reconciled = df.loc[df.groupby('timestamp')['quality_score'].idxmax()]
            
        elif reconciliation_strategy == "polygon_priority":
            # Prefer Polygon, fallback to Tiingo
            polygon_data = df[df['vendor'] == 'polygon'].set_index('timestamp')
            tiingo_data = df[df['vendor'] == 'tiingo'].set_index('timestamp')
            
            # Combine with Polygon taking priority
            combined_index = polygon_data.index.union(tiingo_data.index)
            reconciled_data = []
            
            for timestamp in combined_index:
                if timestamp in polygon_data.index:
                    reconciled_data.append(polygon_data.loc[timestamp])
                else:
                    reconciled_data.append(tiingo_data.loc[timestamp])
            
            reconciled = pd.DataFrame(reconciled_data)
            reconciled['timestamp'] = reconciled.index
            
        elif reconciliation_strategy == "tiingo_priority":
            # Prefer Tiingo, fallback to Polygon  
            polygon_data = df[df['vendor'] == 'polygon'].set_index('timestamp')
            tiingo_data = df[df['vendor'] == 'tiingo'].set_index('timestamp')
            
            # Combine with Tiingo taking priority
            combined_index = polygon_data.index.union(tiingo_data.index)
            reconciled_data = []
            
            for timestamp in combined_index:
                if timestamp in tiingo_data.index:
                    reconciled_data.append(tiingo_data.loc[timestamp])
                else:
                    reconciled_data.append(polygon_data.loc[timestamp])
            
            reconciled = pd.DataFrame(reconciled_data)
            reconciled['timestamp'] = reconciled.index
            
        else:
            raise ValueError(f"Unknown reconciliation strategy: {reconciliation_strategy}")
        
        # Clean up and sort
        if 'timestamp' in reconciled.columns:
            reconciled = reconciled.sort_values('timestamp').drop_duplicates(subset=['timestamp'])
        
        return reconciled
    
    async def get_vendor_comparison(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Compare data quality and coverage between vendors for analysis.
        
        Returns detailed comparison metrics for data quality assessment.
        """
        # Get all vendor data
        df = await self.query_reconciled_data(symbol, start_date, end_date, "both")
        
        if df.empty:
            return {'error': 'No data found for comparison'}
        
        # Split by vendor
        polygon_df = df[df['vendor'] == 'polygon']
        tiingo_df = df[df['vendor'] == 'tiingo']
        
        # Calculate coverage and quality metrics
        comparison = {
            'symbol': symbol,
            'date_range': f"{start_date} to {end_date}",
            'polygon': {
                'record_count': len(polygon_df),
                'date_coverage': {
                    'start': polygon_df['timestamp'].min() if not polygon_df.empty else None,
                    'end': polygon_df['timestamp'].max() if not polygon_df.empty else None
                },
                'avg_quality_score': polygon_df['quality_score'].mean() if not polygon_df.empty else 0,
                'avg_volume': polygon_df['volume'].mean() if not polygon_df.empty else 0,
                'data_completeness': len(polygon_df) / ((end_date - start_date).days * 390) if not polygon_df.empty else 0
            },
            'tiingo': {
                'record_count': len(tiingo_df),
                'date_coverage': {
                    'start': tiingo_df['timestamp'].min() if not tiingo_df.empty else None,
                    'end': tiingo_df['timestamp'].max() if not tiingo_df.empty else None
                },
                'avg_quality_score': tiingo_df['quality_score'].mean() if not tiingo_df.empty else 0,
                'avg_volume': tiingo_df['volume'].mean() if not tiingo_df.empty else 0,
                'data_completeness': len(tiingo_df) / ((end_date - start_date).days * 390) if not tiingo_df.empty else 0
            }
        }
        
        # Calculate overlap and differences
        if not polygon_df.empty and not tiingo_df.empty:
            polygon_timestamps = set(polygon_df['timestamp'])
            tiingo_timestamps = set(tiingo_df['timestamp'])
            
            overlap = polygon_timestamps.intersection(tiingo_timestamps)
            comparison['overlap'] = {
                'overlapping_records': len(overlap),
                'polygon_only': len(polygon_timestamps - tiingo_timestamps),
                'tiingo_only': len(tiingo_timestamps - polygon_timestamps),
                'overlap_percentage': len(overlap) / len(polygon_timestamps.union(tiingo_timestamps)) * 100
            }
        
        return comparison
    
    async def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)


# Convenience functions for integrated backfill
async def create_integrated_hybrid_manager(
    db_url: str = None,
    config: StorageConfig = None
) -> HybridMinuteDataManager:
    """Create hybrid storage manager with database connection for integrated backfill."""
    if config is None:
        config = StorageConfig()
    
    if db_url is None:
        db_url = config.database_url
    
    pool = await asyncpg.create_pool(db_url, min_size=5, max_size=20)
    return HybridMinuteDataManager(pool, config)

async def run_integrated_gap_analysis_and_backfill(
    symbols: List[str],
    start_date: date,
    end_date: date,
    polygon_adapter,
    tiingo_adapter,
    config: StorageConfig = None
) -> Dict[str, Any]:
    """
    Complete integrated workflow: analyze gaps and backfill missing data.
    
    This is the main entry point for integrated backfill operations.
    """
    results = {
        'analysis': {},
        'backfill': {},
        'total_gaps_found': 0,
        'total_gaps_filled': 0,
        'start_time': datetime.now()
    }
    
    logger.info(f"🎯 Starting integrated gap analysis and backfill")
    logger.info(f"   Symbols: {len(symbols)} ({', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''})")
    logger.info(f"   Date range: {start_date} to {end_date}")
    
    async with await create_integrated_hybrid_manager(config=config) as manager:
        # 1. Analyze data gaps
        logger.info("📊 Phase 1: Analyzing data gaps...")
        gaps = await manager.analyze_data_gaps(symbols, start_date, end_date)
        
        results['analysis'] = {
            'gaps_found': len(gaps),
            'symbols_analyzed': len(symbols),
            'date_range_days': (end_date - start_date).days + 1
        }
        results['total_gaps_found'] = len(gaps)
        
        if not gaps:
            logger.info("✅ No data gaps found - all data is complete!")
            results['end_time'] = datetime.now()
            results['duration'] = results['end_time'] - results['start_time']
            return results
        
        # Show gap summary
        gap_summary = {}
        for gap in gaps:
            if gap.symbol not in gap_summary:
                gap_summary[gap.symbol] = {'count': 0, 'estimated_bars': 0}
            gap_summary[gap.symbol]['count'] += 1
            gap_summary[gap.symbol]['estimated_bars'] += gap.estimated_bars
        
        logger.info(f"📋 Gap Analysis Summary:")
        for symbol, summary in list(gap_summary.items())[:10]:  # Show first 10
            logger.info(f"   {symbol}: {summary['count']} gaps, ~{summary['estimated_bars']:,} bars")
        
        if len(gap_summary) > 10:
            logger.info(f"   ... and {len(gap_summary) - 10} more symbols")
        
        # 2. Backfill gaps
        logger.info("🚀 Phase 2: Backfilling data gaps...")
        backfill_results = await manager.backfill_gaps_integrated(
            gaps, polygon_adapter, tiingo_adapter, batch_size=10
        )
        
        results['backfill'] = backfill_results
        results['total_gaps_filled'] = backfill_results['completed_gaps']
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    # Final summary
    logger.info(f"🎉 Integrated backfill completed!")
    logger.info(f"   Duration: {results['duration']}")
    logger.info(f"   Gaps found: {results['total_gaps_found']}")
    logger.info(f"   Gaps filled: {results['total_gaps_filled']}")
    logger.info(f"   Success rate: {(results['total_gaps_filled']/results['total_gaps_found']*100) if results['total_gaps_found'] > 0 else 100:.1f}%")
    
    return results


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