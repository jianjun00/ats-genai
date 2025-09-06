#!/usr/bin/env python3
"""
30-Year Historical Daily Price Backfill (1995-2020)

High-performance backfill system for populating 30 years of historical daily prices
from multiple vendors with checkpoint support and intelligent resume capabilities.

Target: Complete 30-year backfill for thousands of symbols in 1-2 days.
"""

import os
import asyncio
import argparse
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Set, Optional, Tuple
from dataclasses import dataclass, field
import json
from pathlib import Path
import hashlib
import ray
import asyncpg
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class HistoricalBackfillConfig:
    """Configuration for 30-year historical backfill."""
    
    # Date range (1995-2020)
    start_date: date = date(1995, 1, 1)
    end_date: date = date(2020, 12, 31)
    
    # API credentials
    polygon_api_key: Optional[str] = None
    tiingo_api_key: Optional[str] = None
    
    # Symbol selection
    symbols: List[str] = None
    exclude_symbols: Set[str] = field(default_factory=set)
    
    # Parallel processing
    max_ray_workers: int = 20
    max_concurrent_symbols: int = 50
    batch_size_symbols: int = 100  # Symbols per batch
    
    # Year chunking for manageable processing
    chunk_size_years: int = 5  # Process 5 years at a time
    
    # Database configuration
    db_host: str = "localhost"
    db_port: int = 5433
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_name: str = "dev_db"
    
    # Checkpoint configuration
    checkpoint_file: Optional[str] = None
    checkpoint_interval_minutes: int = 10
    auto_checkpoint_batch_count: int = 10  # Checkpoint every N batches
    
    # Error handling
    max_retries_per_symbol: int = 3
    continue_on_error: bool = True
    failure_threshold: float = 0.15  # Fail if > 15% of symbols fail
    
    # Performance tuning
    rate_limit_delay: float = 0.05  # 50ms between API calls
    batch_insert_size: int = 1000  # Records per database batch
    use_tiingo_fallback: bool = True  # Use Tiingo if Polygon fails


@dataclass
class SymbolJob:
    """Represents a symbol processing job."""
    symbol: str
    instrument_id: int
    start_date: date
    end_date: date
    status: str = "pending"  # pending, processing, completed, failed
    attempt_count: int = 0
    error_message: Optional[str] = None
    records_fetched: Dict[str, int] = field(default_factory=dict)  # vendor -> count
    records_stored: int = 0
    processing_time: float = 0.0
    
    @property
    def job_id(self) -> str:
        """Generate unique job ID."""
        content = f"{self.symbol}_{self.start_date}_{self.end_date}"
        return hashlib.md5(content.encode()).hexdigest()[:12]


@dataclass
class BackfillProgress:
    """Track 30-year backfill progress."""
    start_time: datetime = field(default_factory=datetime.now)
    last_checkpoint: Optional[datetime] = None
    
    # Job tracking
    total_jobs: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    jobs_pending: int = 0
    
    # Data tracking
    total_records_fetched: Dict[str, int] = field(default_factory=lambda: {"polygon": 0, "tiingo": 0})
    total_records_stored: int = 0
    
    # Performance tracking
    symbols_per_hour: float = 0.0
    avg_processing_time_per_symbol: float = 0.0
    estimated_completion: Optional[datetime] = None
    
    # Error tracking
    recent_errors: List[str] = field(default_factory=list)
    failed_symbols: List[str] = field(default_factory=list)
    
    def update_estimates(self):
        """Update performance estimates."""
        if self.jobs_completed > 0:
            elapsed_hours = (datetime.now() - self.start_time).total_seconds() / 3600
            self.symbols_per_hour = self.jobs_completed / elapsed_hours
            
            if self.symbols_per_hour > 0:
                remaining_hours = self.jobs_pending / self.symbols_per_hour
                self.estimated_completion = datetime.now() + timedelta(hours=remaining_hours)


@ray.remote
class HistoricalPolygonWorker:
    """Ray actor for historical Polygon data fetching."""
    
    def __init__(self, api_key: str, rate_limit_delay: float = 0.05):
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        
    async def fetch_symbol_historical(self, symbol: str, start_date: str, end_date: str, instrument_id: int) -> Dict[str, Any]:
        """Fetch historical data for a symbol with optimized chunking."""
        import aiohttp
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        results = []
        current_date = start_dt
        
        logger.debug(f"Fetching Polygon data for {symbol}: {start_date} to {end_date}")
        
        async with aiohttp.ClientSession() as session:
            while current_date <= end_dt:
                # Process 2 years at a time for optimal API usage
                batch_end = min(current_date + timedelta(days=730), end_dt)
                
                url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{current_date}/{batch_end}"
                params = {
                    'adjusted': 'true',
                    'sort': 'asc',
                    'limit': 50000,
                    'apikey': self.api_key
                }
                
                try:
                    await asyncio.sleep(self.rate_limit_delay)
                    
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'results' in data and data['results']:
                                for result in data['results']:
                                    # Convert timestamp to date
                                    record_date = datetime.utcfromtimestamp(result['t']/1000).date()
                                    
                                    results.append({
                                        'date': record_date,
                                        'instrument_id': instrument_id,
                                        'open': float(result['o']),
                                        'high': float(result['h']), 
                                        'low': float(result['l']),
                                        'close': float(result['c']),
                                        'volume': int(result['v']),
                                        'vendor': 'polygon'
                                    })
                        elif response.status == 429:
                            # Rate limited - wait longer
                            logger.warning(f"Rate limited for {symbol}, waiting...")
                            await asyncio.sleep(5)
                            continue
                        else:
                            logger.warning(f"Polygon API error for {symbol}: HTTP {response.status}")
                            
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout fetching {symbol} data for {current_date}-{batch_end}")
                except Exception as e:
                    logger.error(f"Error fetching Polygon data for {symbol} {current_date}-{batch_end}: {e}")
                
                current_date = batch_end + timedelta(days=1)
        
        return {
            'symbol': symbol,
            'instrument_id': instrument_id,
            'vendor': 'polygon',
            'records': results,
            'count': len(results)
        }


@ray.remote  
class HistoricalTiingoWorker:
    """Ray actor for historical Tiingo data fetching."""
    
    def __init__(self, api_key: str, rate_limit_delay: float = 0.1):
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        
    async def fetch_symbol_historical(self, symbol: str, start_date: str, end_date: str, instrument_id: int) -> Dict[str, Any]:
        """Fetch historical data from Tiingo for entire date range."""
        import aiohttp
        
        logger.debug(f"Fetching Tiingo data for {symbol}: {start_date} to {end_date}")
        
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'format': 'json',
            'token': self.api_key
        }
        
        results = []
        
        try:
            await asyncio.sleep(self.rate_limit_delay)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=60) as response:
                    if response.status == 200:
                        data = await response.json()
                        for item in data:
                            # Tiingo date format: "2020-12-31T00:00:00.000Z"
                            record_date = datetime.strptime(item['date'][:10], "%Y-%m-%d").date()
                            
                            # Handle potential None values
                            open_price = item.get('open')
                            high_price = item.get('high')
                            low_price = item.get('low')
                            close_price = item.get('close')
                            volume = item.get('volume')
                            
                            if all(v is not None for v in [open_price, high_price, low_price, close_price]):
                                results.append({
                                    'date': record_date,
                                    'instrument_id': instrument_id,
                                    'open': float(open_price),
                                    'high': float(high_price),
                                    'low': float(low_price), 
                                    'close': float(close_price),
                                    'volume': int(volume) if volume is not None else 0,
                                    'vendor': 'tiingo'
                                })
                    elif response.status == 429:
                        logger.warning(f"Tiingo rate limited for {symbol}")
                    else:
                        logger.warning(f"Tiingo API error for {symbol}: HTTP {response.status}")
                        
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching Tiingo data for {symbol}")
        except Exception as e:
            logger.error(f"Error fetching Tiingo data for {symbol}: {e}")
            
        return {
            'symbol': symbol,
            'instrument_id': instrument_id,
            'vendor': 'tiingo',
            'records': results,
            'count': len(results)
        }


@ray.remote
class HistoricalDatabaseInserter:
    """Ray actor for batch database insertions."""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        
    async def batch_insert_daily_prices(self, vendor: str, records: List[Dict[str, Any]]) -> int:
        """Batch insert daily price records."""
        if not records:
            return 0
            
        # Create database connection
        conn = await asyncpg.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database']
        )
        
        try:
            # Use environment-aware table naming
            from core.config.environment import Environment
            env = Environment()
            
            if vendor == 'polygon':
                table_name = env.get_table_name('daily_prices_polygon')
                columns = ['date', 'instrument_id', 'open', 'high', 'low', 'close', 'volume']
            else:  # tiingo
                table_name = env.get_table_name('daily_prices_tiingo')
                columns = ['date', 'instrument_id', 'open', 'high', 'low', 'close', 'volume']
            
            # Prepare data for bulk insert
            values = []
            for record in records:
                values.append((
                    record['date'],
                    record['instrument_id'],
                    record['open'],
                    record['high'],
                    record['low'],
                    record['close'],
                    record['volume']
                ))
            
            # Use COPY for maximum performance
            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                SELECT * FROM UNNEST($1::date[], $2::int[], $3::numeric[], $4::numeric[], 
                                   $5::numeric[], $6::numeric[], $7::bigint[])
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """
            
            # Unpack values into separate arrays
            dates, instrument_ids, opens, highs, lows, closes, volumes = zip(*values)
            
            await conn.execute(
                query,
                list(dates), list(instrument_ids), list(opens), list(highs),
                list(lows), list(closes), list(volumes)
            )
            
            logger.debug(f"Inserted {len(records)} {vendor} records")
            return len(records)
            
        except Exception as e:
            logger.error(f"Database insert error for {vendor}: {e}")
            return 0
        finally:
            await conn.close()


class Historical30YearBackfill:
    """
    Main orchestrator for 30-year historical daily price backfill.
    
    Features:
    - Parallel processing with Ray
    - Checkpoint/resume capability
    - Multi-vendor support (Polygon + Tiingo fallback)
    - Progress tracking and ETA calculation
    - Error handling and recovery
    """
    
    def __init__(self, config: HistoricalBackfillConfig):
        self.config = config
        self.progress = BackfillProgress()
        self.symbol_jobs: Dict[str, SymbolJob] = {}
        
        # Initialize Ray if not already done
        if not ray.is_initialized():
            ray.init(num_cpus=config.max_ray_workers)
        
        # Create workers
        self.polygon_workers = []
        self.tiingo_workers = []
        self.db_inserters = []
        
        self._setup_workers()
        self._load_checkpoint()
    
    def _setup_workers(self):
        """Initialize Ray workers."""
        db_config = {
            'host': self.config.db_host,
            'port': self.config.db_port,
            'user': self.config.db_user,
            'password': self.config.db_password,
            'database': self.config.db_name
        }
        
        # Create workers
        num_workers = min(self.config.max_ray_workers, 10)
        
        if self.config.polygon_api_key:
            for _ in range(num_workers):
                worker = HistoricalPolygonWorker.remote(
                    self.config.polygon_api_key,
                    self.config.rate_limit_delay
                )
                self.polygon_workers.append(worker)
        
        if self.config.tiingo_api_key:
            for _ in range(num_workers):
                worker = HistoricalTiingoWorker.remote(
                    self.config.tiingo_api_key,
                    self.config.rate_limit_delay * 2  # Slower rate for Tiingo
                )
                self.tiingo_workers.append(worker)
        
        # Database inserters
        for _ in range(5):  # Fewer DB workers to avoid connection limits
            inserter = HistoricalDatabaseInserter.remote(db_config)
            self.db_inserters.append(inserter)
        
        logger.info(f"Created {len(self.polygon_workers)} Polygon workers, "
                   f"{len(self.tiingo_workers)} Tiingo workers, "
                   f"{len(self.db_inserters)} DB inserters")
    
    async def run_backfill(self) -> Dict[str, Any]:
        """Execute the complete 30-year historical backfill."""
        self.progress.start_time = datetime.now()
        
        logger.info("🚀 Starting 30-year historical daily price backfill (1995-2020)")
        logger.info(f"Target symbols: {len(self.config.symbols or []) or 'All available'}")
        logger.info(f"Date range: {self.config.start_date} to {self.config.end_date}")
        
        try:
            # Generate symbol jobs if not resuming
            if not self.symbol_jobs:
                await self._generate_symbol_jobs()
            
            # Start auto-checkpoint task
            checkpoint_task = asyncio.create_task(self._auto_checkpoint_loop())
            
            try:
                # Execute backfill in year chunks
                await self._execute_chunked_backfill()
                
                # Generate final statistics
                final_stats = await self._generate_final_statistics()
                
                logger.info("✅ 30-year historical backfill completed!")
                return final_stats
                
            finally:
                checkpoint_task.cancel()
                try:
                    await checkpoint_task
                except asyncio.CancelledError:
                    pass
            
        except Exception as e:
            logger.error(f"❌ Historical backfill failed: {e}")
            self._save_checkpoint()
            raise
        finally:
            # Cleanup Ray workers
            ray.shutdown()
    
    async def _generate_symbol_jobs(self):
        """Generate symbol jobs from available instruments."""
        # Get symbols from database or use provided list
        symbols = self.config.symbols
        if not symbols:
            symbols = await self._get_available_symbols()
        
        # Filter out excluded symbols
        symbols = [s for s in symbols if s not in self.config.exclude_symbols]
        
        logger.info(f"Generating jobs for {len(symbols)} symbols")
        
        # Get instrument IDs
        instrument_map = await self._get_instrument_mapping(symbols)
        
        # Create jobs for each symbol
        for symbol in symbols:
            if symbol in instrument_map:
                job = SymbolJob(
                    symbol=symbol,
                    instrument_id=instrument_map[symbol],
                    start_date=self.config.start_date,
                    end_date=self.config.end_date
                )
                self.symbol_jobs[symbol] = job
        
        self.progress.total_jobs = len(self.symbol_jobs)
        self.progress.jobs_pending = len(self.symbol_jobs)
        
        logger.info(f"Generated {self.progress.total_jobs} symbol jobs")
    
    async def _get_available_symbols(self) -> List[str]:
        """Get available symbols from instrument table."""
        conn = await asyncpg.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            user=self.config.db_user,
            password=self.config.db_password,
            database=self.config.db_name
        )
        
        try:
            from core.config.environment import Environment
            env = Environment()
            table_name = env.get_table_name('instrument')
            
            query = f"""
                SELECT symbol FROM {table_name} 
                WHERE asset_class = 'STK' 
                AND is_active = true
                ORDER BY symbol
            """
            
            rows = await conn.fetch(query)
            symbols = [row['symbol'] for row in rows]
            
            logger.info(f"Found {len(symbols)} available symbols in database")
            return symbols
            
        finally:
            await conn.close()
    
    async def _get_instrument_mapping(self, symbols: List[str]) -> Dict[str, int]:
        """Get instrument ID mapping for symbols."""
        conn = await asyncpg.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            user=self.config.db_user,
            password=self.config.db_password,
            database=self.config.db_name
        )
        
        try:
            from core.config.environment import Environment
            env = Environment()
            table_name = env.get_table_name('instrument')
            
            query = f"""
                SELECT symbol, instrument_id FROM {table_name} 
                WHERE symbol = ANY($1)
            """
            
            rows = await conn.fetch(query, symbols)
            mapping = {row['symbol']: row['instrument_id'] for row in rows}
            
            logger.info(f"Mapped {len(mapping)} symbols to instrument IDs")
            return mapping
            
        finally:
            await conn.close()
    
    async def _execute_chunked_backfill(self):
        """Execute backfill in manageable year chunks."""
        # Group jobs by year chunks
        year_chunks = self._create_year_chunks()
        
        for chunk_idx, (chunk_start, chunk_end) in enumerate(year_chunks):
            logger.info(f"Processing year chunk {chunk_idx + 1}/{len(year_chunks)}: "
                       f"{chunk_start.year}-{chunk_end.year}")
            
            # Get jobs for this chunk
            chunk_jobs = [
                job for job in self.symbol_jobs.values()
                if job.status == "pending"
            ]
            
            if not chunk_jobs:
                continue
            
            # Process jobs in symbol batches
            await self._process_symbol_batches(chunk_jobs, chunk_start, chunk_end)
            
            # Checkpoint after each year chunk
            self._save_checkpoint()
    
    def _create_year_chunks(self) -> List[Tuple[date, date]]:
        """Create year chunks for processing."""
        chunks = []
        current_year = self.config.start_date.year
        end_year = self.config.end_date.year
        
        while current_year <= end_year:
            chunk_end_year = min(current_year + self.config.chunk_size_years - 1, end_year)
            
            chunk_start = date(current_year, 1, 1)
            chunk_end = date(chunk_end_year, 12, 31)
            
            # Don't exceed configured end date
            if chunk_end > self.config.end_date:
                chunk_end = self.config.end_date
            
            chunks.append((chunk_start, chunk_end))
            current_year = chunk_end_year + 1
        
        return chunks
    
    async def _process_symbol_batches(self, jobs: List[SymbolJob], chunk_start: date, chunk_end: date):
        """Process symbols in batches with parallel workers."""
        
        # Create batches
        for i in range(0, len(jobs), self.config.batch_size_symbols):
            batch = jobs[i:i + self.config.batch_size_symbols]
            
            logger.info(f"Processing batch {i//self.config.batch_size_symbols + 1}: "
                       f"{len(batch)} symbols")
            
            # Process batch in parallel
            await self._process_job_batch(batch, chunk_start, chunk_end)
            
            # Update progress
            self.progress.update_estimates()
            
            # Auto-checkpoint
            if (i // self.config.batch_size_symbols) % self.config.auto_checkpoint_batch_count == 0:
                self._save_checkpoint()
    
    async def _process_job_batch(self, jobs: List[SymbolJob], chunk_start: date, chunk_end: date):
        """Process a batch of symbol jobs in parallel."""
        
        # Create tasks for parallel processing
        tasks = []
        
        for job in jobs:
            if job.status != "pending":
                continue
                
            # Mark as processing
            job.status = "processing"
            self.progress.jobs_pending -= 1
            
            # Create processing task
            task = self._process_single_symbol_job(job, chunk_start, chunk_end)
            tasks.append(task)
        
        # Execute tasks with controlled concurrency
        if tasks:
            semaphore = asyncio.Semaphore(self.config.max_concurrent_symbols)
            
            async def limited_task(task):
                async with semaphore:
                    return await task
            
            results = await asyncio.gather(
                *[limited_task(task) for task in tasks],
                return_exceptions=True
            )
            
            # Process results
            for job, result in zip(jobs, results):
                if isinstance(result, Exception):
                    logger.error(f"Job {job.symbol} failed: {result}")
                    job.status = "failed"
                    job.error_message = str(result)
                    job.attempt_count += 1
                    
                    self.progress.jobs_failed += 1
                    self.progress.failed_symbols.append(job.symbol)
                    self.progress.recent_errors.append(f"{job.symbol}: {result}")
                else:
                    logger.info(f"Job {job.symbol} completed: {result.get('records_stored', 0)} records")
    
    async def _process_single_symbol_job(self, job: SymbolJob, chunk_start: date, chunk_end: date) -> Dict[str, Any]:
        """Process a single symbol job with vendor fallback."""
        
        start_time = time.time()
        
        try:
            # Try Polygon first
            polygon_data = None
            if self.polygon_workers:
                try:
                    worker = self.polygon_workers[hash(job.symbol) % len(self.polygon_workers)]
                    polygon_result = await worker.fetch_symbol_historical.remote(
                        job.symbol, 
                        chunk_start.isoformat(), 
                        chunk_end.isoformat(),
                        job.instrument_id
                    )
                    polygon_data = ray.get(polygon_result)
                    job.records_fetched['polygon'] = polygon_data['count']
                    
                except Exception as e:
                    logger.warning(f"Polygon fetch failed for {job.symbol}: {e}")
            
            # Try Tiingo as fallback or primary
            tiingo_data = None
            if self.tiingo_workers and (not polygon_data or self.config.use_tiingo_fallback):
                try:
                    worker = self.tiingo_workers[hash(job.symbol) % len(self.tiingo_workers)]
                    tiingo_result = await worker.fetch_symbol_historical.remote(
                        job.symbol,
                        chunk_start.isoformat(),
                        chunk_end.isoformat(), 
                        job.instrument_id
                    )
                    tiingo_data = ray.get(tiingo_result)
                    job.records_fetched['tiingo'] = tiingo_data['count']
                    
                except Exception as e:
                    logger.warning(f"Tiingo fetch failed for {job.symbol}: {e}")
            
            # Choose best data source
            primary_data = None
            if polygon_data and polygon_data['count'] > 0:
                primary_data = polygon_data
            elif tiingo_data and tiingo_data['count'] > 0:
                primary_data = tiingo_data
            
            if not primary_data:
                raise ValueError(f"No data available from any vendor for {job.symbol}")
            
            # Store data
            if primary_data['records']:
                inserter = self.db_inserters[hash(job.symbol) % len(self.db_inserters)]
                insert_result = await inserter.batch_insert_daily_prices.remote(
                    primary_data['vendor'],
                    primary_data['records']
                )
                job.records_stored = ray.get(insert_result)
            
            # Mark as completed
            job.status = "completed"
            job.processing_time = time.time() - start_time
            
            self.progress.jobs_completed += 1
            self.progress.total_records_fetched[primary_data['vendor']] += primary_data['count']
            self.progress.total_records_stored += job.records_stored
            
            return {
                'symbol': job.symbol,
                'status': 'completed',
                'records_stored': job.records_stored,
                'processing_time': job.processing_time
            }
            
        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.processing_time = time.time() - start_time
            raise
    
    async def _auto_checkpoint_loop(self):
        """Automatic checkpoint saving loop."""
        while True:
            try:
                await asyncio.sleep(self.config.checkpoint_interval_minutes * 60)
                self._save_checkpoint()
                logger.debug("Auto-checkpoint saved")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-checkpoint error: {e}")
    
    def _save_checkpoint(self):
        """Save checkpoint with job progress."""
        if not self.config.checkpoint_file:
            return
        
        checkpoint_data = {
            'config': {
                'start_date': self.config.start_date.isoformat(),
                'end_date': self.config.end_date.isoformat(),
                'symbols': self.config.symbols,
                'chunk_size_years': self.config.chunk_size_years
            },
            'progress': {
                'start_time': self.progress.start_time.isoformat(),
                'last_checkpoint': datetime.now().isoformat(),
                'total_jobs': self.progress.total_jobs,
                'jobs_completed': self.progress.jobs_completed,
                'jobs_failed': self.progress.jobs_failed,
                'jobs_pending': self.progress.jobs_pending,
                'total_records_fetched': self.progress.total_records_fetched,
                'total_records_stored': self.progress.total_records_stored,
                'symbols_per_hour': self.progress.symbols_per_hour,
                'recent_errors': self.progress.recent_errors[-20:],
                'failed_symbols': self.progress.failed_symbols
            },
            'jobs': {
                symbol: {
                    'symbol': job.symbol,
                    'instrument_id': job.instrument_id,
                    'start_date': job.start_date.isoformat(),
                    'end_date': job.end_date.isoformat(),
                    'status': job.status,
                    'attempt_count': job.attempt_count,
                    'error_message': job.error_message,
                    'records_fetched': job.records_fetched,
                    'records_stored': job.records_stored,
                    'processing_time': job.processing_time
                }
                for symbol, job in self.symbol_jobs.items()
            }
        }
        
        try:
            checkpoint_path = Path(self.config.checkpoint_file)
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.config.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            self.progress.last_checkpoint = datetime.now()
            logger.info(f"Checkpoint saved: {self.progress.jobs_completed}/{self.progress.total_jobs} "
                       f"jobs completed ({self.progress.jobs_completed/self.progress.total_jobs*100:.1f}%)")
            
        except Exception as e:
            logger.error(f"Checkpoint save failed: {e}")
    
    def _load_checkpoint(self):
        """Load checkpoint to resume progress."""
        if not self.config.checkpoint_file or not Path(self.config.checkpoint_file).exists():
            return
        
        try:
            with open(self.config.checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            # Restore progress
            progress_data = checkpoint_data.get('progress', {})
            if progress_data.get('start_time'):
                self.progress.start_time = datetime.fromisoformat(progress_data['start_time'])
            
            self.progress.total_jobs = progress_data.get('total_jobs', 0)
            self.progress.jobs_completed = progress_data.get('jobs_completed', 0)
            self.progress.jobs_failed = progress_data.get('jobs_failed', 0)
            self.progress.jobs_pending = progress_data.get('jobs_pending', 0)
            self.progress.total_records_fetched = progress_data.get('total_records_fetched', {"polygon": 0, "tiingo": 0})
            self.progress.total_records_stored = progress_data.get('total_records_stored', 0)
            self.progress.recent_errors = progress_data.get('recent_errors', [])
            self.progress.failed_symbols = progress_data.get('failed_symbols', [])
            
            # Restore jobs
            jobs_data = checkpoint_data.get('jobs', {})
            for symbol, job_data in jobs_data.items():
                job = SymbolJob(
                    symbol=job_data['symbol'],
                    instrument_id=job_data['instrument_id'],
                    start_date=date.fromisoformat(job_data['start_date']),
                    end_date=date.fromisoformat(job_data['end_date']),
                    status=job_data.get('status', 'pending'),
                    attempt_count=job_data.get('attempt_count', 0),
                    error_message=job_data.get('error_message'),
                    records_stored=job_data.get('records_stored', 0),
                    processing_time=job_data.get('processing_time', 0.0)
                )
                job.records_fetched = job_data.get('records_fetched', {})
                self.symbol_jobs[symbol] = job
            
            # Reset processing jobs to pending
            processing_count = 0
            for job in self.symbol_jobs.values():
                if job.status == "processing":
                    job.status = "pending"
                    processing_count += 1
            
            if processing_count > 0:
                self.progress.jobs_pending += processing_count
            
            logger.info(f"Checkpoint loaded: {self.progress.jobs_completed} completed, "
                       f"{self.progress.jobs_failed} failed, {self.progress.jobs_pending} pending")
            
        except Exception as e:
            logger.error(f"Checkpoint load failed: {e}")
    
    async def _generate_final_statistics(self) -> Dict[str, Any]:
        """Generate comprehensive final statistics."""
        
        end_time = datetime.now()
        duration_hours = (end_time - self.progress.start_time).total_seconds() / 3600
        
        success_rate = self.progress.jobs_completed / self.progress.total_jobs if self.progress.total_jobs > 0 else 0
        failure_rate = self.progress.jobs_failed / self.progress.total_jobs if self.progress.total_jobs > 0 else 0
        
        total_records_fetched = sum(self.progress.total_records_fetched.values())
        
        return {
            'backfill_summary': {
                'start_time': self.progress.start_time,
                'end_time': end_time,
                'duration_hours': duration_hours,
                'date_range': f"{self.config.start_date} to {self.config.end_date}",
                'years_processed': (self.config.end_date.year - self.config.start_date.year) + 1
            },
            'job_statistics': {
                'total_symbols': self.progress.total_jobs,
                'symbols_completed': self.progress.jobs_completed,
                'symbols_failed': self.progress.jobs_failed,
                'success_rate': success_rate,
                'failure_rate': failure_rate
            },
            'data_statistics': {
                'total_records_fetched': total_records_fetched,
                'records_by_vendor': self.progress.total_records_fetched,
                'total_records_stored': self.progress.total_records_stored,
                'storage_efficiency': self.progress.total_records_stored / max(total_records_fetched, 1)
            },
            'performance_metrics': {
                'symbols_per_hour': self.progress.symbols_per_hour,
                'records_per_hour': self.progress.total_records_stored / duration_hours if duration_hours > 0 else 0,
                'avg_records_per_symbol': self.progress.total_records_stored / max(self.progress.jobs_completed, 1)
            },
            'error_analysis': {
                'failed_symbols': self.progress.failed_symbols,
                'recent_errors': self.progress.recent_errors[-10:],
                'error_rate': len(self.progress.recent_errors) / max(self.progress.total_jobs, 1)
            }
        }


# Convenience function
async def run_30_year_historical_backfill(
    polygon_api_key: str,
    tiingo_api_key: str,
    symbols: Optional[List[str]] = None,
    db_host: str = "localhost",
    db_port: int = 5433,
    db_user: str = "postgres", 
    db_password: str = "postgres",
    db_name: str = "dev_db",
    checkpoint_file: Optional[str] = None,
    max_workers: int = 20
) -> Dict[str, Any]:
    """
    Convenience function to run 30-year historical backfill.
    
    Args:
        polygon_api_key: Polygon API key
        tiingo_api_key: Tiingo API key
        symbols: Optional list of symbols (uses all if None)
        db_host: Database host
        db_port: Database port
        db_user: Database user
        db_password: Database password
        db_name: Database name
        checkpoint_file: Checkpoint file path
        max_workers: Maximum Ray workers
    
    Returns:
        Final statistics
    """
    
    config = HistoricalBackfillConfig(
        polygon_api_key=polygon_api_key,
        tiingo_api_key=tiingo_api_key,
        symbols=symbols,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        checkpoint_file=checkpoint_file,
        max_ray_workers=max_workers
    )
    
    backfill = Historical30YearBackfill(config)
    return await backfill.run_backfill()


if __name__ == "__main__":
    # Simple CLI for testing
    parser = argparse.ArgumentParser(description="30-Year Historical Daily Price Backfill")
    parser.add_argument("--symbols", help="Comma-separated symbols")
    parser.add_argument("--workers", type=int, default=20, help="Max workers")
    parser.add_argument("--checkpoint", help="Checkpoint file")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    symbols = args.symbols.split(',') if args.symbols else None
    
    polygon_key = os.getenv("POLYGON_API_KEY")
    tiingo_key = os.getenv("TIINGO_API_KEY")
    
    if not polygon_key or not tiingo_key:
        logger.error("POLYGON_API_KEY and TIINGO_API_KEY required")
        exit(1)
    
    result = asyncio.run(run_30_year_historical_backfill(
        polygon_api_key=polygon_key,
        tiingo_api_key=tiingo_key,
        symbols=symbols,
        checkpoint_file=args.checkpoint,
        max_workers=args.workers
    ))
    
    print(f"✅ Backfill completed: {result['job_statistics']}")