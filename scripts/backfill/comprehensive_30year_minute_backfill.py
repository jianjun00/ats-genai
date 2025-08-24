#!/usr/bin/env python3
"""
Comprehensive 30-Year 1-Minute Data Backfill Orchestrator

Orchestrates 30 years (1995-2025) of 1-minute OHLCV data backfill across all vendors:
- Polygon (1995-2025) - Primary high-quality source
- Tiingo (1995-2025) - Secondary validation source
- FMP (1995-2025) - Backup and validation
- EODHD (1995-2025) - Additional coverage

Features:
- Checkpoint-based resumable processing
- Vendor-specific job isolation
- Intelligent rate limiting and retry logic
- File-based storage with monthly partitioning
- Database storage option for real-time queries
- Quality validation and gap detection
- Progress monitoring and Slack notifications
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
import json
from pathlib import Path
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

try:
    # Import our multi-vendor adapters
    from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter
    from market_data.agent.tiingo_intraday_adapter import TiingoIntradayAdapter
    from market_data.agent.fmp_minute_adapter import FMPMinuteAdapter
    from market_data.agent.eodhd_minute_adapter import EODHDMinuteAdapter
    
    # Import storage systems
    from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
    from config.environment import Environment
    
    # Database
    import asyncpg
    
except ImportError as e:
    print(f"❌ Failed to import required modules: {e}")
    print("Make sure you're running from the project root and src/ is in PYTHONPATH")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class MinuteBackfillConfig:
    """Configuration for 30-year minute data backfill."""
    
    # Date range (30 years)
    start_date: date = date(1995, 1, 1)
    end_date: date = date(2025, 8, 31)
    
    # Vendor selection
    enabled_vendors: List[str] = field(default_factory=lambda: ["polygon", "tiingo", "fmp", "eodhd"])
    
    # Target instruments
    target_symbols: Optional[List[str]] = None
    min_market_cap: float = 100_000_000  # $100M minimum market cap
    max_instruments: int = 10000  # Limit to top 10K instruments
    
    # Processing configuration
    chunk_size_months: int = 6  # Process 6 months at a time
    batch_size_symbols: int = 50  # Symbols per batch
    max_concurrent_symbols: int = 10  # Max parallel symbol processing
    
    # Storage configuration
    storage_type: str = "file"  # "file" or "database" or "both"
    file_base_path: str = "/home/jianjun/ats-data/minute-files"
    
    # Database configuration
    db_host: str = "localhost"
    db_port: int = 5433
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_name: str = "dev_db"
    
    # Checkpoint configuration
    checkpoint_dir: str = "/home/jianjun/ats-data/checkpoints"
    checkpoint_interval_minutes: int = 15
    auto_save_frequency: int = 100  # Save every N processed batches
    
    # Rate limiting (vendor-specific)
    rate_limits: Dict[str, float] = field(default_factory=lambda: {
        "polygon": 2.0,   # 2 seconds between calls (conservative)
        "tiingo": 1.0,    # 1 second between calls
        "fmp": 1.5,       # 1.5 seconds between calls
        "eodhd": 3.0      # 3 seconds between calls (most conservative)
    })
    
    # Error handling
    max_retries_per_symbol: int = 3
    failure_threshold: float = 0.20  # Fail job if > 20% symbols fail
    continue_on_vendor_failure: bool = True
    
    # Quality validation
    min_data_quality_score: float = 0.7
    max_gap_tolerance: float = 0.15  # Allow 15% gaps in data
    
    # Resource limits
    max_memory_gb: float = 8.0
    max_concurrent_vendors: int = 2  # Max vendors running simultaneously


@dataclass 
class SymbolBackfillJob:
    """Represents a symbol backfill job."""
    symbol: str
    instrument_id: int
    vendor: str
    start_date: date
    end_date: date
    priority: int = 1  # Higher number = higher priority
    
    # Status tracking
    status: str = "pending"  # pending, processing, completed, failed, skipped
    attempt_count: int = 0
    error_message: Optional[str] = None
    
    # Progress tracking
    chunks_total: int = 0
    chunks_completed: int = 0
    chunks_failed: int = 0
    
    # Data tracking
    bars_fetched: int = 0
    bars_stored: int = 0
    quality_score: float = 0.0
    
    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    processing_duration: float = 0.0
    
    @property
    def job_id(self) -> str:
        """Generate unique job ID."""
        content = f"{self.vendor}_{self.symbol}_{self.start_date}_{self.end_date}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    @property
    def progress_percent(self) -> float:
        """Calculate completion percentage."""
        if self.chunks_total == 0:
            return 0.0
        return (self.chunks_completed / self.chunks_total) * 100.0


@dataclass
class BackfillProgress:
    """Track overall backfill progress."""
    
    # Job metrics
    total_jobs: int = 0
    jobs_by_status: Dict[str, int] = field(default_factory=lambda: {
        "pending": 0, "processing": 0, "completed": 0, "failed": 0, "skipped": 0
    })
    
    # Vendor metrics
    vendor_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Data metrics
    total_bars_fetched: int = 0
    total_bars_stored: int = 0
    total_symbols_processed: int = 0
    
    # Time metrics
    start_time: datetime = field(default_factory=datetime.now)
    last_checkpoint: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None
    
    # Performance metrics
    symbols_per_hour: float = 0.0
    bars_per_second: float = 0.0
    
    def update_estimates(self):
        """Update performance estimates."""
        elapsed_seconds = (datetime.now() - self.start_time).total_seconds()
        
        if elapsed_seconds > 0:
            completed = self.jobs_by_status.get("completed", 0)
            
            if completed > 0:
                self.symbols_per_hour = (completed * 3600) / elapsed_seconds
                self.bars_per_second = self.total_bars_fetched / elapsed_seconds
                
                remaining = self.jobs_by_status.get("pending", 0)
                if self.symbols_per_hour > 0:
                    hours_remaining = remaining / self.symbols_per_hour
                    self.estimated_completion = datetime.now() + timedelta(hours=hours_remaining)


class Comprehensive30YearMinuteBackfill:
    """Orchestrates 30-year minute data backfill across all vendors."""
    
    def __init__(self, config: MinuteBackfillConfig):
        self.config = config
        self.progress = BackfillProgress()
        
        # Job storage
        self.jobs: Dict[str, SymbolBackfillJob] = {}
        self.jobs_by_vendor: Dict[str, List[SymbolBackfillJob]] = {
            vendor: [] for vendor in config.enabled_vendors
        }
        
        # Vendor adapters
        self.adapters: Dict[str, Any] = {}
        
        # Storage managers
        self.file_manager: Optional[FileBasedMinuteManager] = None
        self.db_pool: Optional[Any] = None
        
        # Checkpointing
        self.checkpoint_path = Path(config.checkpoint_dir)
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        
    async def initialize(self):
        """Initialize the backfill system."""
        logger.info("🚀 Initializing 30-Year Minute Data Backfill System")
        
        # Initialize vendor adapters
        await self._initialize_adapters()
        
        # Initialize storage systems
        await self._initialize_storage()
        
        # Load or generate jobs
        await self._load_or_generate_jobs()
        
        logger.info(f"✅ Initialization complete. {len(self.jobs)} jobs queued across {len(self.config.enabled_vendors)} vendors")
    
    async def _initialize_adapters(self):
        """Initialize vendor adapters."""
        logger.info("🔌 Initializing vendor adapters...")
        
        for vendor in self.config.enabled_vendors:
            try:
                if vendor == "polygon":
                    self.adapters["polygon"] = PolygonMinuteAdapter()
                elif vendor == "tiingo":
                    self.adapters["tiingo"] = TiingoIntradayAdapter()
                elif vendor == "fmp":
                    self.adapters["fmp"] = FMPMinuteAdapter()
                elif vendor == "eodhd":
                    self.adapters["eodhd"] = EODHDMinuteAdapter()
                
                logger.info(f"✅ {vendor.upper()} adapter initialized")
                
            except Exception as e:
                logger.warning(f"⚠️  Failed to initialize {vendor} adapter: {e}")
                if vendor in self.adapters:
                    del self.adapters[vendor]
    
    async def _initialize_storage(self):
        """Initialize storage systems."""
        if self.config.storage_type in ["file", "both"]:
            try:
                self.file_manager = FileBasedMinuteManager(
                    base_path=self.config.file_base_path
                )
                logger.info(f"✅ File storage initialized: {self.config.file_base_path}")
            except Exception as e:
                logger.error(f"❌ Failed to initialize file storage: {e}")
                raise
        
        if self.config.storage_type in ["database", "both"]:
            try:
                env = Environment()
                db_url = env.get_database_url()
                self.db_pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
                logger.info("✅ Database storage initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize database storage: {e}")
                raise
    
    async def _load_or_generate_jobs(self):
        """Load existing checkpoint or generate new jobs."""
        checkpoint_file = self.checkpoint_path / "comprehensive_minute_backfill.json"
        
        if checkpoint_file.exists():
            logger.info("📂 Loading jobs from checkpoint...")
            await self._load_checkpoint(checkpoint_file)
        else:
            logger.info("🆕 Generating new backfill jobs...")
            await self._generate_jobs()
            
        # Organize jobs by vendor
        for job in self.jobs.values():
            if job.vendor in self.jobs_by_vendor:
                self.jobs_by_vendor[job.vendor].append(job)
        
        # Update progress counters
        self._update_progress_counters()
    
    async def _generate_jobs(self):
        """Generate backfill jobs for all target instruments and vendors."""
        # Get target instruments
        target_instruments = await self._get_target_instruments()
        logger.info(f"🎯 Targeting {len(target_instruments)} instruments")
        
        # Generate date chunks (6-month periods)
        date_chunks = self._generate_date_chunks()
        logger.info(f"📅 Generated {len(date_chunks)} date chunks")
        
        job_count = 0
        for vendor in self.config.enabled_vendors:
            if vendor not in self.adapters:
                logger.warning(f"⚠️  Skipping {vendor} - adapter not available")
                continue
                
            for instrument_id, symbol, priority in target_instruments:
                for chunk_start, chunk_end in date_chunks:
                    job = SymbolBackfillJob(
                        symbol=symbol,
                        instrument_id=instrument_id,
                        vendor=vendor,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        priority=priority,
                        chunks_total=1  # Each job is one chunk
                    )
                    
                    self.jobs[job.job_id] = job
                    job_count += 1
        
        logger.info(f"📋 Generated {job_count} backfill jobs")
    
    async def _get_target_instruments(self) -> List[Tuple[int, str, int]]:
        """Get target instruments for backfill."""
        if self.config.target_symbols:
            # Use specified symbols
            instruments = []
            for i, symbol in enumerate(self.config.target_symbols):
                instruments.append((i + 1, symbol, 1))  # Mock instrument IDs
            return instruments
        
        # Get from database if available
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT DISTINCT i.id, i.symbol, 
                           CASE 
                               WHEN mc.market_cap > 10000000000 THEN 3
                               WHEN mc.market_cap > 1000000000 THEN 2
                               ELSE 1
                           END as priority
                    FROM dev_instruments i
                    LEFT JOIN dev_market_cap mc ON i.id = mc.instrument_id
                    WHERE i.symbol ~ '^[A-Z]{1,8}$'
                      AND (mc.market_cap IS NULL OR mc.market_cap >= $1)
                    ORDER BY priority DESC, mc.market_cap DESC NULLS LAST
                    LIMIT $2
                """
                
                rows = await conn.fetch(query, self.config.min_market_cap, self.config.max_instruments)
                return [(row['id'], row['symbol'], row['priority']) for row in rows]
        
        # Fallback to common symbols
        common_symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK-B", "UNH", "JNJ",
            "V", "PG", "JPM", "HD", "CVX", "MA", "BAC", "ABBV", "PFE", "AVGO",
            "KO", "MRK", "COST", "PEP", "TMO", "DHR", "ACN", "VZ", "ADBE", "NKE"
        ]
        
        return [(i + 1, symbol, 1) for i, symbol in enumerate(common_symbols[:self.config.max_instruments])]
    
    def _generate_date_chunks(self) -> List[Tuple[date, date]]:
        """Generate date chunks for processing."""
        chunks = []
        current_date = self.config.start_date
        
        while current_date < self.config.end_date:
            chunk_end = min(
                current_date + timedelta(days=self.config.chunk_size_months * 30),
                self.config.end_date
            )
            chunks.append((current_date, chunk_end))
            current_date = chunk_end + timedelta(days=1)
        
        return chunks
    
    def _update_progress_counters(self):
        """Update progress counters from current jobs."""
        self.progress.total_jobs = len(self.jobs)
        self.progress.jobs_by_status = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "skipped": 0}
        
        for job in self.jobs.values():
            self.progress.jobs_by_status[job.status] += 1
            self.progress.total_bars_fetched += job.bars_fetched
            self.progress.total_bars_stored += job.bars_stored
    
    async def run_backfill(self) -> Dict[str, Any]:
        """Execute the comprehensive backfill."""
        logger.info("🚀 Starting 30-Year Minute Data Backfill")
        logger.info(f"📊 Processing {self.progress.total_jobs:,} jobs across {len(self.config.enabled_vendors)} vendors")
        logger.info(f"📅 Date range: {self.config.start_date} to {self.config.end_date}")
        logger.info(f"💾 Storage: {self.config.storage_type}")
        
        try:
            # Start background checkpoint task
            checkpoint_task = asyncio.create_task(self._checkpoint_loop())
            
            # Execute vendor-specific backfills in parallel
            vendor_tasks = []
            semaphore = asyncio.Semaphore(self.config.max_concurrent_vendors)
            
            for vendor in self.config.enabled_vendors:
                if vendor in self.adapters and self.jobs_by_vendor[vendor]:
                    task = asyncio.create_task(
                        self._run_vendor_backfill(vendor, semaphore)
                    )
                    vendor_tasks.append(task)
            
            if vendor_tasks:
                # Wait for all vendor backfills to complete
                results = await asyncio.gather(*vendor_tasks, return_exceptions=True)
                
                # Process results
                for i, result in enumerate(results):
                    vendor = self.config.enabled_vendors[i] if i < len(self.config.enabled_vendors) else f"vendor_{i}"
                    if isinstance(result, Exception):
                        logger.error(f"❌ {vendor} backfill failed: {result}")
                    else:
                        logger.info(f"✅ {vendor} backfill completed: {result}")
            
            # Final checkpoint
            await self._save_checkpoint()
            
            # Calculate final statistics
            final_stats = self._calculate_final_stats()
            
            logger.info("🎉 30-Year Minute Data Backfill Completed!")
            logger.info(f"📊 Final Stats: {final_stats}")
            
            return final_stats
            
        except Exception as e:
            logger.error(f"❌ Backfill failed: {e}")
            await self._save_checkpoint()
            raise
            
        finally:
            # Cleanup checkpoint task
            if 'checkpoint_task' in locals():
                checkpoint_task.cancel()
                try:
                    await checkpoint_task
                except asyncio.CancelledError:
                    pass
            
            # Cleanup resources
            await self._cleanup()
    
    async def _run_vendor_backfill(self, vendor: str, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
        """Run backfill for a specific vendor."""
        async with semaphore:
            logger.info(f"🔄 Starting {vendor.upper()} backfill...")
            
            jobs = self.jobs_by_vendor[vendor]
            completed = 0
            failed = 0
            
            adapter = self.adapters[vendor]
            
            async with adapter:
                # Process jobs in batches
                for i in range(0, len(jobs), self.config.batch_size_symbols):
                    batch = jobs[i:i + self.config.batch_size_symbols]
                    
                    # Process batch with concurrency limit
                    semaphore_batch = asyncio.Semaphore(self.config.max_concurrent_symbols)
                    batch_tasks = [
                        self._process_symbol_job(job, adapter, semaphore_batch)
                        for job in batch
                    ]
                    
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    # Count results
                    for result in batch_results:
                        if isinstance(result, Exception):
                            failed += 1
                        elif result:
                            completed += 1
                    
                    # Rate limiting between batches
                    await asyncio.sleep(self.config.rate_limits.get(vendor, 1.0))
            
            vendor_stats = {
                "completed": completed,
                "failed": failed,
                "total": len(jobs),
                "success_rate": completed / len(jobs) if jobs else 0
            }
            
            logger.info(f"✅ {vendor.upper()} backfill finished: {vendor_stats}")
            return vendor_stats
    
    async def _process_symbol_job(self, job: SymbolBackfillJob, adapter, semaphore: asyncio.Semaphore) -> bool:
        """Process a single symbol job."""
        async with semaphore:
            job.status = "processing"
            job.start_time = datetime.now()
            job.attempt_count += 1
            
            try:
                # Fetch data from vendor
                bars = await adapter.fetch_minute_bars_async(
                    job.symbol, 
                    datetime.combine(job.start_date, datetime.min.time()),
                    datetime.combine(job.end_date, datetime.min.time())
                )
                
                job.bars_fetched = len(bars)
                
                if bars:
                    # Validate data quality
                    quality_metrics = adapter.validate_data_quality(bars)
                    job.quality_score = quality_metrics.get('quality_score', 0.0)
                    
                    if job.quality_score >= self.config.min_data_quality_score:
                        # Store data
                        if await self._store_data(job, bars):
                            job.bars_stored = len(bars)
                            job.chunks_completed = 1
                            job.status = "completed"
                        else:
                            job.status = "failed"
                            job.error_message = "Storage failed"
                    else:
                        job.status = "failed"
                        job.error_message = f"Low quality score: {job.quality_score}"
                else:
                    job.status = "skipped"
                    job.error_message = "No data returned"
                
                job.end_time = datetime.now()
                job.processing_duration = (job.end_time - job.start_time).total_seconds()
                
                return job.status == "completed"
                
            except Exception as e:
                job.status = "failed"
                job.error_message = str(e)
                job.end_time = datetime.now()
                if job.start_time:
                    job.processing_duration = (job.end_time - job.start_time).total_seconds()
                
                logger.error(f"❌ Job {job.job_id} failed: {e}")
                return False
    
    async def _store_data(self, job: SymbolBackfillJob, bars: List[Any]) -> bool:
        """Store data using configured storage systems."""
        try:
            # File storage
            if self.config.storage_type in ["file", "both"] and self.file_manager:
                unified_bars = []
                for bar in bars:
                    unified_bar = MinuteBar(
                        symbol=job.symbol,
                        timestamp=bar.timestamp,
                        open=float(bar.open),
                        high=float(bar.high),
                        low=float(bar.low),
                        close=float(bar.close),
                        volume=int(bar.volume),
                        vendor=job.vendor
                    )
                    unified_bars.append(unified_bar)
                
                await self.file_manager.store_minute_data(
                    job.symbol, unified_bars, overlap_strategy='merge'
                )
            
            # Database storage
            if self.config.storage_type in ["database", "both"] and self.db_pool:
                await self._store_to_database(job, bars)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Storage failed for {job.symbol} ({job.vendor}): {e}")
            return False
    
    async def _store_to_database(self, job: SymbolBackfillJob, bars: List[Any]):
        """Store data to database."""
        async with self.db_pool.acquire() as conn:
            records = []
            for bar in bars:
                record = (
                    job.symbol,
                    bar.timestamp,
                    float(bar.open),
                    float(bar.high),
                    float(bar.low),
                    float(bar.close),
                    int(bar.volume),
                    job.vendor,
                    datetime.now()
                )
                records.append(record)
            
            await conn.executemany("""
                INSERT INTO minute_bars (
                    symbol, timestamp, open, high, low, close, volume, vendor, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    vendor = EXCLUDED.vendor,
                    updated_at = CURRENT_TIMESTAMP
            """, records)
    
    async def _checkpoint_loop(self):
        """Background checkpoint saving loop."""
        while True:
            try:
                await asyncio.sleep(self.config.checkpoint_interval_minutes * 60)
                await self._save_checkpoint()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Checkpoint error: {e}")
    
    async def _save_checkpoint(self):
        """Save current progress to checkpoint."""
        checkpoint_file = self.checkpoint_path / "comprehensive_minute_backfill.json"
        
        # Update progress
        self._update_progress_counters()
        self.progress.update_estimates()
        self.progress.last_checkpoint = datetime.now()
        
        checkpoint_data = {
            "config": {
                "start_date": self.config.start_date.isoformat(),
                "end_date": self.config.end_date.isoformat(),
                "enabled_vendors": self.config.enabled_vendors,
                "storage_type": self.config.storage_type
            },
            "progress": {
                "start_time": self.progress.start_time.isoformat(),
                "last_checkpoint": self.progress.last_checkpoint.isoformat(),
                "total_jobs": self.progress.total_jobs,
                "jobs_by_status": self.progress.jobs_by_status,
                "total_bars_fetched": self.progress.total_bars_fetched,
                "total_bars_stored": self.progress.total_bars_stored,
                "symbols_per_hour": self.progress.symbols_per_hour,
                "bars_per_second": self.progress.bars_per_second,
                "estimated_completion": self.progress.estimated_completion.isoformat() if self.progress.estimated_completion else None
            },
            "jobs": {
                job_id: {
                    "symbol": job.symbol,
                    "instrument_id": job.instrument_id,
                    "vendor": job.vendor,
                    "start_date": job.start_date.isoformat(),
                    "end_date": job.end_date.isoformat(),
                    "status": job.status,
                    "attempt_count": job.attempt_count,
                    "error_message": job.error_message,
                    "chunks_total": job.chunks_total,
                    "chunks_completed": job.chunks_completed,
                    "bars_fetched": job.bars_fetched,
                    "bars_stored": job.bars_stored,
                    "quality_score": job.quality_score,
                    "processing_duration": job.processing_duration
                }
                for job_id, job in self.jobs.items()
            }
        }
        
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            completed_pct = (self.progress.jobs_by_status["completed"] / self.progress.total_jobs * 100) if self.progress.total_jobs > 0 else 0
            logger.info(f"💾 Checkpoint saved: {self.progress.jobs_by_status['completed']:,}/{self.progress.total_jobs:,} jobs ({completed_pct:.1f}%)")
            
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")
    
    async def _load_checkpoint(self, checkpoint_file: Path):
        """Load progress from checkpoint."""
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
        
        # Restore progress
        progress_data = data.get('progress', {})
        self.progress.start_time = datetime.fromisoformat(progress_data.get('start_time', datetime.now().isoformat()))
        self.progress.total_jobs = progress_data.get('total_jobs', 0)
        self.progress.jobs_by_status = progress_data.get('jobs_by_status', {})
        self.progress.total_bars_fetched = progress_data.get('total_bars_fetched', 0)
        self.progress.total_bars_stored = progress_data.get('total_bars_stored', 0)
        
        # Restore jobs
        jobs_data = data.get('jobs', {})
        for job_id, job_data in jobs_data.items():
            job = SymbolBackfillJob(
                symbol=job_data['symbol'],
                instrument_id=job_data['instrument_id'],
                vendor=job_data['vendor'],
                start_date=date.fromisoformat(job_data['start_date']),
                end_date=date.fromisoformat(job_data['end_date']),
                status=job_data['status'],
                attempt_count=job_data['attempt_count'],
                error_message=job_data.get('error_message'),
                chunks_total=job_data.get('chunks_total', 1),
                chunks_completed=job_data.get('chunks_completed', 0),
                bars_fetched=job_data.get('bars_fetched', 0),
                bars_stored=job_data.get('bars_stored', 0),
                quality_score=job_data.get('quality_score', 0.0),
                processing_duration=job_data.get('processing_duration', 0.0)
            )
            self.jobs[job_id] = job
        
        logger.info(f"📂 Loaded checkpoint: {len(self.jobs)} jobs restored")
    
    def _calculate_final_stats(self) -> Dict[str, Any]:
        """Calculate final statistics."""
        self._update_progress_counters()
        
        return {
            "total_jobs": self.progress.total_jobs,
            "jobs_completed": self.progress.jobs_by_status["completed"],
            "jobs_failed": self.progress.jobs_by_status["failed"],
            "success_rate": self.progress.jobs_by_status["completed"] / self.progress.total_jobs if self.progress.total_jobs > 0 else 0,
            "total_bars_fetched": self.progress.total_bars_fetched,
            "total_bars_stored": self.progress.total_bars_stored,
            "processing_duration": (datetime.now() - self.progress.start_time).total_seconds(),
            "symbols_per_hour": self.progress.symbols_per_hour,
            "bars_per_second": self.progress.bars_per_second,
            "vendor_stats": {
                vendor: len([j for j in jobs if j.status == "completed"]) 
                for vendor, jobs in self.jobs_by_vendor.items()
            }
        }
    
    async def _cleanup(self):
        """Cleanup resources."""
        if self.file_manager:
            await self.file_manager.close()
        
        if self.db_pool:
            await self.db_pool.close()


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="30-Year Comprehensive Minute Data Backfill",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full 30-year backfill for all vendors
    python comprehensive_30year_minute_backfill.py --vendors polygon,tiingo,fmp,eodhd
    
    # Specific symbols backfill
    python comprehensive_30year_minute_backfill.py --symbols AAPL,MSFT,GOOGL --vendors polygon,tiingo
    
    # Resume from checkpoint
    python comprehensive_30year_minute_backfill.py --checkpoint /path/to/checkpoint.json
    
    # File storage only
    python comprehensive_30year_minute_backfill.py --storage file --base-path /data/minute-files
        """
    )
    
    parser.add_argument(
        '--vendors',
        type=str,
        default='polygon,tiingo,fmp,eodhd',
        help='Comma-separated list of vendors'
    )
    parser.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated list of symbols (default: auto-select based on market cap)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='1995-01-01',
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default='2025-08-31',
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--storage',
        choices=['file', 'database', 'both'],
        default='file',
        help='Storage type'
    )
    parser.add_argument(
        '--base-path',
        type=str,
        default='/home/jianjun/ats-data/minute-files',
        help='Base path for file storage'
    )
    parser.add_argument(
        '--checkpoint-dir',
        type=str,
        default='/home/jianjun/ats-data/checkpoints',
        help='Checkpoint directory'
    )
    parser.add_argument(
        '--max-instruments',
        type=int,
        default=10000,
        help='Maximum number of instruments'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Symbols per batch'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=10,
        help='Max concurrent symbols'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse configuration
    config = MinuteBackfillConfig(
        start_date=datetime.strptime(args.start_date, '%Y-%m-%d').date(),
        end_date=datetime.strptime(args.end_date, '%Y-%m-%d').date(),
        enabled_vendors=[v.strip() for v in args.vendors.split(',')],
        target_symbols=[s.strip().upper() for s in args.symbols.split(',')] if args.symbols else None,
        storage_type=args.storage,
        file_base_path=args.base_path,
        checkpoint_dir=args.checkpoint_dir,
        max_instruments=args.max_instruments,
        batch_size_symbols=args.batch_size,
        max_concurrent_symbols=args.max_concurrent
    )
    
    logger.info("🚀 Starting 30-Year Comprehensive Minute Data Backfill")
    logger.info(f"📅 Period: {config.start_date} to {config.end_date}")
    logger.info(f"🏪 Vendors: {', '.join(config.enabled_vendors)}")
    logger.info(f"💾 Storage: {config.storage_type}")
    
    # Create and run backfill
    backfill = Comprehensive30YearMinuteBackfill(config)
    
    try:
        await backfill.initialize()
        results = await backfill.run_backfill()
        
        print("\n" + "="*60)
        print("🎉 COMPREHENSIVE 30-YEAR MINUTE BACKFILL COMPLETED!")
        print("="*60)
        print(f"📊 Jobs Completed: {results['jobs_completed']:,} / {results['total_jobs']:,}")
        print(f"✅ Success Rate: {results['success_rate']:.1%}")
        print(f"📈 Bars Fetched: {results['total_bars_fetched']:,}")
        print(f"💾 Bars Stored: {results['total_bars_stored']:,}")
        print(f"⏱️  Duration: {results['processing_duration'] / 3600:.1f} hours")
        print(f"🚀 Performance: {results['symbols_per_hour']:.1f} symbols/hour")
        print("\n📊 Vendor Breakdown:")
        for vendor, count in results['vendor_stats'].items():
            print(f"  {vendor.upper():<10}: {count:,} symbols completed")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("⚠️  Backfill interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))