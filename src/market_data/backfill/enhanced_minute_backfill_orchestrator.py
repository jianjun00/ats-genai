"""
Enhanced Minute-Level Backfill Orchestrator with Advanced Checkpointing

Provides fine-grained checkpointing and parallel processing for large-scale
1-minute data backfills from Polygon and Tiingo with intelligent resume capabilities.
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, Set, Tuple, NamedTuple
from dataclasses import dataclass, field
import logging
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import hashlib
import uuid
from enum import Enum

from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter
from market_data.agent.tiingo_intraday_adapter import TiingoIntradayAdapter
from market_data.reconciliation.cross_vendor_reconciler import (
    CrossVendorReconciler, 
    ReconciliationConfig,
    ReconciliationMethod
)
from storage.hybrid_minute_data_manager import HybridMinuteDataManager, StorageConfig
from config.environment import env

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Status of a backfill job segment."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class JobSegment:
    """A single unit of work in the backfill process."""
    segment_id: str
    symbol: str
    start_date: datetime
    end_date: datetime
    status: JobStatus = JobStatus.PENDING
    attempt_count: int = 0
    last_attempt: Optional[datetime] = None
    error_message: Optional[str] = None
    bars_fetched: Dict[str, int] = field(default_factory=dict)  # vendor -> count
    bars_reconciled: int = 0
    bars_stored: int = 0
    
    def __post_init__(self):
        if not self.segment_id:
            # Generate deterministic segment ID from symbol and date range
            content = f"{self.symbol}_{self.start_date.date()}_{self.end_date.date()}"
            self.segment_id = hashlib.md5(content.encode()).hexdigest()[:12]


@dataclass
class EnhancedBackfillConfig:
    """Enhanced configuration for minute-level backfill with parallel processing."""
    
    # Time range
    start_date: datetime
    end_date: datetime
    
    # API credentials
    polygon_api_key: Optional[str] = None
    tiingo_api_key: Optional[str] = None
    
    # Symbol selection
    symbols: List[str] = None
    
    # Parallel processing configuration
    max_concurrent_symbols: int = 5  # Concurrent symbols being processed
    max_concurrent_date_ranges: int = 3  # Concurrent date ranges per symbol
    max_total_workers: int = 15  # Total concurrent API calls
    
    # Chunking configuration
    chunk_size_days: int = 7  # Smaller chunks for minute data
    min_chunk_size_days: int = 1  # Minimum chunk size
    
    # Storage configuration
    storage_base_path: str = "/home/jianjun/ats/data/STK/1min"
    
    # Reconciliation configuration
    reconciliation_method: ReconciliationMethod = ReconciliationMethod.WEIGHTED_AVERAGE
    require_both_vendors: bool = False
    
    # Error handling
    max_retries_per_segment: int = 3
    retry_delay_seconds: int = 300  # 5 minutes
    continue_on_error: bool = True
    failure_threshold: float = 0.1  # Fail if > 10% of segments fail
    
    # Checkpointing configuration
    checkpoint_file: Optional[str] = None
    checkpoint_interval_minutes: int = 5  # Save checkpoint every 5 minutes
    auto_checkpoint_segment_count: int = 50  # Auto-checkpoint every N segments
    
    # Progress reporting
    progress_reporting_interval: int = 100
    detailed_logging: bool = True


@dataclass
class BackfillProgress:
    """Enhanced progress tracking with fine-grained checkpointing."""
    job_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    start_time: Optional[datetime] = None
    last_checkpoint_time: Optional[datetime] = None
    
    # Segment tracking
    total_segments: int = 0
    segments_completed: int = 0
    segments_failed: int = 0
    segments_in_progress: int = 0
    segments_pending: int = 0
    
    # Data tracking
    total_bars_fetched: Dict[str, int] = field(default_factory=lambda: {"polygon": 0, "tiingo": 0})
    total_bars_reconciled: int = 0
    total_bars_stored: int = 0
    
    # Error tracking
    recent_errors: List[str] = field(default_factory=list)
    failed_segments: List[str] = field(default_factory=list)
    
    # Performance tracking
    avg_processing_time_per_segment: float = 0.0
    estimated_completion_time: Optional[datetime] = None
    
    def update_completion_estimate(self):
        """Update estimated completion time based on current progress."""
        if self.segments_completed > 0 and self.avg_processing_time_per_segment > 0:
            remaining_segments = self.total_segments - self.segments_completed
            estimated_seconds = remaining_segments * self.avg_processing_time_per_segment
            self.estimated_completion_time = datetime.now() + timedelta(seconds=estimated_seconds)


class EnhancedMinuteBackfillOrchestrator:
    """
    Advanced orchestrator for minute-level backfill with enhanced checkpointing.
    
    Features:
    - Fine-grained checkpointing at segment level
    - Intelligent parallel processing with rate limiting
    - Smart resume from exact failure point
    - Progressive checkpoint saving
    - Detailed progress tracking and ETA calculation
    - Configurable failure tolerance
    """
    
    def __init__(
        self, 
        pool: asyncpg.Pool, 
        config: EnhancedBackfillConfig,
        storage_config: StorageConfig = None
    ):
        self.pool = pool
        self.config = config
        self.storage_config = storage_config or StorageConfig(
            base_data_path=config.storage_base_path
        )
        
        # Initialize components
        self.polygon_adapter = None
        self.tiingo_adapter = None
        self.reconciler = CrossVendorReconciler(
            ReconciliationConfig(method=config.reconciliation_method)
        )
        self.storage_manager = HybridMinuteDataManager(pool, self.storage_config)
        
        # Job management
        self.progress = BackfillProgress()
        self.job_segments: Dict[str, JobSegment] = {}
        self.segment_processing_times: List[float] = []
        
        # Checkpoint management
        self.last_checkpoint_time = datetime.now()
        
        # Load existing checkpoint if available
        self.load_checkpoint()
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.polygon_adapter = PolygonMinuteAdapter(self.config.polygon_api_key)
        await self.polygon_adapter.__aenter__()
        
        self.tiingo_adapter = TiingoIntradayAdapter(self.config.tiingo_api_key)
        await self.tiingo_adapter.__aenter__()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.polygon_adapter:
            await self.polygon_adapter.__aexit__(exc_type, exc_val, exc_tb)
        if self.tiingo_adapter:
            await self.tiingo_adapter.__aexit__(exc_type, exc_val, exc_tb)
        
        # Final checkpoint save
        self.save_checkpoint()
        
        self.reconciler.close()
    
    async def run_backfill(self) -> Dict[str, Any]:
        """
        Execute enhanced minute-level backfill with fine-grained checkpointing.
        
        Returns:
            Comprehensive statistics and results
        """
        self.progress.start_time = datetime.now()
        
        logger.info(f"Starting enhanced minute backfill job {self.progress.job_id}")
        logger.info(f"Date range: {self.config.start_date} to {self.config.end_date}")
        logger.info(f"Symbols: {len(self.config.symbols or self._get_default_symbols())}")
        logger.info(f"Parallel config: {self.config.max_concurrent_symbols} symbols, "
                   f"{self.config.max_concurrent_date_ranges} date ranges per symbol")
        
        try:
            # Generate job segments
            if not self.job_segments:  # Only generate if not resuming
                self._generate_job_segments()
            
            # Start checkpoint auto-save task
            checkpoint_task = asyncio.create_task(self._auto_checkpoint_loop())
            
            try:
                # Execute backfill with enhanced parallel processing
                await self._execute_parallel_backfill()
                
                # Generate final statistics
                final_stats = await self._generate_final_statistics()
                
                logger.info(f"Enhanced minute backfill {self.progress.job_id} completed successfully")
                return final_stats
                
            finally:
                # Cancel checkpoint task
                checkpoint_task.cancel()
                try:
                    await checkpoint_task
                except asyncio.CancelledError:
                    pass
            
        except Exception as e:
            logger.error(f"Enhanced backfill {self.progress.job_id} failed: {e}")
            self.progress.recent_errors.append(f"Critical error: {e}")
            self.save_checkpoint()  # Save progress before failing
            raise
    
    def _generate_job_segments(self):
        """Generate fine-grained job segments for processing."""
        symbols = self.config.symbols or self._get_default_symbols()
        
        # Create segments for each symbol+date_range combination
        for symbol in symbols:
            date_chunks = self._create_date_chunks_for_symbol(symbol)
            
            for start_date, end_date in date_chunks:
                segment = JobSegment(
                    segment_id="",  # Will be auto-generated
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
                self.job_segments[segment.segment_id] = segment
        
        # Update progress totals
        self.progress.total_segments = len(self.job_segments)
        self.progress.segments_pending = len(self.job_segments)
        
        logger.info(f"Generated {self.progress.total_segments} job segments")
    
    def _create_date_chunks_for_symbol(self, symbol: str) -> List[Tuple[datetime, datetime]]:
        """Create optimized date chunks for a symbol based on data density."""
        chunks = []
        current_date = self.config.start_date
        
        while current_date < self.config.end_date:
            # Adaptive chunk sizing based on market days vs weekends
            if current_date.weekday() < 5:  # Monday-Friday
                chunk_days = self.config.chunk_size_days
            else:
                chunk_days = self.config.min_chunk_size_days  # Smaller chunks for weekends
            
            chunk_end = min(
                current_date + timedelta(days=chunk_days),
                self.config.end_date
            )
            
            chunks.append((current_date, chunk_end))
            current_date = chunk_end
        
        return chunks
    
    def _get_default_symbols(self) -> List[str]:
        """Get default symbols optimized for minute data backfill."""
        return [
            # High-volume liquid stocks - priority for minute data
            'SPY', 'QQQ', 'IWM', 'VTI',  # ETFs
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA',  # Tech giants
            'META', 'NFLX', 'CRM', 'ADBE', 'ORCL',  # More tech
            'JPM', 'BAC', 'WFC', 'GS',  # Banks
            'JNJ', 'PG', 'KO', 'WMT'   # Consumer staples
        ]
    
    async def _execute_parallel_backfill(self):
        """Execute backfill with enhanced parallel processing."""
        
        # Create processing semaphores
        symbol_semaphore = asyncio.Semaphore(self.config.max_concurrent_symbols)
        total_semaphore = asyncio.Semaphore(self.config.max_total_workers)
        
        # Group segments by symbol for organized processing
        segments_by_symbol = {}
        for segment in self.job_segments.values():
            if segment.status in [JobStatus.PENDING, JobStatus.FAILED]:
                if segment.symbol not in segments_by_symbol:
                    segments_by_symbol[segment.symbol] = []
                segments_by_symbol[segment.symbol].append(segment)
        
        # Create symbol processing tasks
        symbol_tasks = []
        for symbol, segments in segments_by_symbol.items():
            task = self._process_symbol_with_parallelism(
                symbol, segments, symbol_semaphore, total_semaphore
            )
            symbol_tasks.append(task)
        
        # Execute all symbol tasks concurrently
        logger.info(f"Starting parallel processing of {len(symbol_tasks)} symbols")
        
        symbol_results = await asyncio.gather(
            *symbol_tasks, return_exceptions=True
        )
        
        # Process results and update statistics
        successful_symbols = 0
        for i, result in enumerate(symbol_results):
            if isinstance(result, Exception):
                symbol = list(segments_by_symbol.keys())[i]
                logger.error(f"Symbol {symbol} processing failed: {result}")
                self.progress.recent_errors.append(f"Symbol {symbol}: {result}")
            else:
                successful_symbols += 1
        
        logger.info(f"Completed processing: {successful_symbols}/{len(symbol_tasks)} symbols successful")
        
        # Check failure threshold
        failure_rate = 1 - (successful_symbols / len(symbol_tasks))
        if failure_rate > self.config.failure_threshold:
            raise RuntimeError(f"Failure rate {failure_rate:.1%} exceeds threshold {self.config.failure_threshold:.1%}")
    
    async def _process_symbol_with_parallelism(
        self, 
        symbol: str, 
        segments: List[JobSegment],
        symbol_semaphore: asyncio.Semaphore,
        total_semaphore: asyncio.Semaphore
    ):
        """Process all segments for a symbol with controlled parallelism."""
        
        async with symbol_semaphore:
            logger.info(f"Starting parallel processing of {len(segments)} segments for {symbol}")
            
            # Create date range processing tasks
            date_semaphore = asyncio.Semaphore(self.config.max_concurrent_date_ranges)
            
            segment_tasks = []
            for segment in segments:
                task = self._process_single_segment_with_limits(
                    segment, date_semaphore, total_semaphore
                )
                segment_tasks.append(task)
            
            # Execute segment tasks concurrently
            segment_results = await asyncio.gather(
                *segment_tasks, return_exceptions=True
            )
            
            # Update symbol-level statistics
            successful_segments = sum(1 for r in segment_results if not isinstance(r, Exception))
            logger.info(f"Symbol {symbol} completed: {successful_segments}/{len(segments)} segments successful")
            
            return {
                'symbol': symbol,
                'total_segments': len(segments),
                'successful_segments': successful_segments,
                'errors': [str(r) for r in segment_results if isinstance(r, Exception)]
            }
    
    async def _process_single_segment_with_limits(
        self,
        segment: JobSegment,
        date_semaphore: asyncio.Semaphore,
        total_semaphore: asyncio.Semaphore
    ):
        """Process a single segment with rate limiting."""
        
        async with date_semaphore:
            async with total_semaphore:
                return await self._process_single_segment(segment)
    
    async def _process_single_segment(self, segment: JobSegment) -> Dict[str, Any]:
        """Process a single job segment with retry logic."""
        
        segment.status = JobStatus.IN_PROGRESS
        segment.last_attempt = datetime.now()
        self.progress.segments_in_progress += 1
        self.progress.segments_pending -= 1
        
        start_time = datetime.now()
        
        try:
            # Fetch data from both vendors concurrently
            polygon_task = self._fetch_polygon_data_with_retry(segment)
            tiingo_task = self._fetch_tiingo_data_with_retry(segment)
            
            polygon_data, tiingo_data = await asyncio.gather(
                polygon_task, tiingo_task, return_exceptions=True
            )
            
            # Handle fetch results
            polygon_bars = polygon_data if not isinstance(polygon_data, Exception) else []
            tiingo_bars = tiingo_data if not isinstance(tiingo_data, Exception) else []
            
            segment.bars_fetched = {
                'polygon': len(polygon_bars),
                'tiingo': len(tiingo_bars)
            }
            
            # Check data requirements
            if not polygon_bars and not tiingo_bars:
                raise ValueError(f"No data from either vendor")
            
            if self.config.require_both_vendors and (not polygon_bars or not tiingo_bars):
                raise ValueError(f"Missing required vendor data")
            
            # Reconcile data
            reconciled_bars = await self.reconciler.reconcile_minute_data(
                polygon_bars, tiingo_bars, segment.symbol
            )
            
            segment.bars_reconciled = len(reconciled_bars)
            
            if reconciled_bars:
                # Store data
                storage_data = self._convert_for_storage(reconciled_bars)
                storage_result = await self.storage_manager.store_minute_data(
                    segment.symbol, storage_data, force_tier='cold'
                )
                segment.bars_stored = storage_result.get('stored_cold', 0)
            
            # Mark success
            segment.status = JobStatus.COMPLETED
            self.progress.segments_completed += 1
            self.progress.segments_in_progress -= 1
            
            # Update global statistics
            self.progress.total_bars_fetched['polygon'] += segment.bars_fetched['polygon']
            self.progress.total_bars_fetched['tiingo'] += segment.bars_fetched['tiingo']
            self.progress.total_bars_reconciled += segment.bars_reconciled
            self.progress.total_bars_stored += segment.bars_stored
            
            # Update timing statistics
            processing_time = (datetime.now() - start_time).total_seconds()
            self.segment_processing_times.append(processing_time)
            
            # Update average processing time (rolling average)
            if len(self.segment_processing_times) > 100:
                self.segment_processing_times = self.segment_processing_times[-100:]
            
            self.progress.avg_processing_time_per_segment = sum(self.segment_processing_times) / len(self.segment_processing_times)
            self.progress.update_completion_estimate()
            
            # Check for auto-checkpoint
            if self.progress.segments_completed % self.config.auto_checkpoint_segment_count == 0:
                self.save_checkpoint()
            
            if self.config.detailed_logging:
                logger.debug(f"Completed segment {segment.segment_id}: {segment.symbol} "
                           f"{segment.start_date.date()}-{segment.end_date.date()} "
                           f"({segment.bars_reconciled} bars, {processing_time:.1f}s)")
            
            return {
                'segment_id': segment.segment_id,
                'status': 'success',
                'bars_processed': segment.bars_reconciled,
                'processing_time': processing_time
            }
            
        except Exception as e:
            # Handle failure
            segment.attempt_count += 1
            segment.error_message = str(e)
            
            if segment.attempt_count >= self.config.max_retries_per_segment:
                segment.status = JobStatus.FAILED
                self.progress.segments_failed += 1
                self.progress.failed_segments.append(segment.segment_id)
                logger.error(f"Segment {segment.segment_id} failed permanently: {e}")
            else:
                segment.status = JobStatus.PENDING
                self.progress.segments_pending += 1
                logger.warning(f"Segment {segment.segment_id} failed (attempt {segment.attempt_count}): {e}")
            
            self.progress.segments_in_progress -= 1
            self.progress.recent_errors.append(f"{segment.segment_id}: {e}")
            
            # Keep only recent errors
            if len(self.progress.recent_errors) > 100:
                self.progress.recent_errors = self.progress.recent_errors[-100:]
            
            if not self.config.continue_on_error:
                raise
            
            return {
                'segment_id': segment.segment_id,
                'status': 'failed',
                'error': str(e),
                'attempt': segment.attempt_count
            }
    
    async def _fetch_polygon_data_with_retry(self, segment: JobSegment):
        """Fetch Polygon data with segment-specific retry logic."""
        for attempt in range(self.config.max_retries_per_segment):
            try:
                return await self.polygon_adapter.fetch_minute_bars_async(
                    segment.symbol, segment.start_date, segment.end_date
                )
            except Exception as e:
                if attempt < self.config.max_retries_per_segment - 1:
                    await asyncio.sleep(self.config.retry_delay_seconds)
                else:
                    raise
    
    async def _fetch_tiingo_data_with_retry(self, segment: JobSegment):
        """Fetch Tiingo data with segment-specific retry logic."""
        for attempt in range(self.config.max_retries_per_segment):
            try:
                return await self.tiingo_adapter.fetch_minute_bars_async(
                    segment.symbol, segment.start_date, segment.end_date
                )
            except Exception as e:
                if attempt < self.config.max_retries_per_segment - 1:
                    await asyncio.sleep(self.config.retry_delay_seconds)
                else:
                    raise
    
    def _convert_for_storage(self, reconciled_bars) -> List[Dict[str, Any]]:
        """Convert reconciled bars to storage format."""
        storage_data = []
        
        for bar in reconciled_bars:
            storage_data.append({
                'symbol': bar.symbol,
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vendor': 'unified',
                'quality_score': getattr(bar, 'quality_score', 1.0),
                'data_source_flags': {
                    'reconciliation_method': getattr(bar, 'reconciliation_method', 'weighted_average'),
                    'source_vendors': getattr(bar, 'source_vendors', ['polygon', 'tiingo']),
                    'vendor_count': getattr(bar, 'vendor_count', 2)
                }
            })
        
        return storage_data
    
    async def _auto_checkpoint_loop(self):
        """Automatic checkpoint saving loop."""
        while True:
            try:
                await asyncio.sleep(self.config.checkpoint_interval_minutes * 60)
                self.save_checkpoint()
                logger.debug(f"Auto-checkpoint saved for job {self.progress.job_id}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-checkpoint failed: {e}")
    
    def save_checkpoint(self):
        """Save enhanced checkpoint with segment-level progress."""
        if not self.config.checkpoint_file:
            return
        
        # Prepare checkpoint data
        checkpoint_data = {
            'job_id': self.progress.job_id,
            'config': {
                'start_date': self.config.start_date.isoformat(),
                'end_date': self.config.end_date.isoformat(),
                'symbols': self.config.symbols,
                'chunk_size_days': self.config.chunk_size_days,
                'reconciliation_method': self.config.reconciliation_method.value,
                'require_both_vendors': self.config.require_both_vendors
            },
            'progress': {
                'start_time': self.progress.start_time.isoformat() if self.progress.start_time else None,
                'last_checkpoint_time': datetime.now().isoformat(),
                'total_segments': self.progress.total_segments,
                'segments_completed': self.progress.segments_completed,
                'segments_failed': self.progress.segments_failed,
                'segments_in_progress': 0,  # Reset in-progress on checkpoint
                'segments_pending': self.progress.segments_pending + self.progress.segments_in_progress,
                'total_bars_fetched': self.progress.total_bars_fetched,
                'total_bars_reconciled': self.progress.total_bars_reconciled,
                'total_bars_stored': self.progress.total_bars_stored,
                'recent_errors': self.progress.recent_errors[-50:],
                'failed_segments': self.progress.failed_segments,
                'avg_processing_time_per_segment': self.progress.avg_processing_time_per_segment,
                'estimated_completion_time': self.progress.estimated_completion_time.isoformat() if self.progress.estimated_completion_time else None
            },
            'segments': {
                segment_id: {
                    'segment_id': segment.segment_id,
                    'symbol': segment.symbol,
                    'start_date': segment.start_date.isoformat(),
                    'end_date': segment.end_date.isoformat(),
                    'status': segment.status.value,
                    'attempt_count': segment.attempt_count,
                    'last_attempt': segment.last_attempt.isoformat() if segment.last_attempt else None,
                    'error_message': segment.error_message,
                    'bars_fetched': segment.bars_fetched,
                    'bars_reconciled': segment.bars_reconciled,
                    'bars_stored': segment.bars_stored
                }
                for segment_id, segment in self.job_segments.items()
            }
        }
        
        try:
            # Create checkpoint directory if needed
            checkpoint_path = Path(self.config.checkpoint_file)
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with atomic write
            temp_file = f"{self.config.checkpoint_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
            
            # Atomic move
            Path(temp_file).rename(checkpoint_path)
            
            self.progress.last_checkpoint_time = datetime.now()
            
            logger.info(f"Enhanced checkpoint saved: {self.progress.segments_completed}/{self.progress.total_segments} "
                       f"segments completed ({self.progress.segments_completed/self.progress.total_segments*100:.1f}%)")
            
        except Exception as e:
            logger.error(f"Failed to save enhanced checkpoint: {e}")
    
    def load_checkpoint(self):
        """Load enhanced checkpoint with segment-level resume."""
        if not self.config.checkpoint_file or not Path(self.config.checkpoint_file).exists():
            return
        
        try:
            with open(self.config.checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            # Restore progress
            progress_data = checkpoint_data.get('progress', {})
            if progress_data.get('start_time'):
                self.progress.start_time = datetime.fromisoformat(progress_data['start_time'])
            
            self.progress.job_id = checkpoint_data.get('job_id', self.progress.job_id)
            self.progress.total_segments = progress_data.get('total_segments', 0)
            self.progress.segments_completed = progress_data.get('segments_completed', 0)
            self.progress.segments_failed = progress_data.get('segments_failed', 0)
            self.progress.segments_pending = progress_data.get('segments_pending', 0)
            self.progress.total_bars_fetched = progress_data.get('total_bars_fetched', {"polygon": 0, "tiingo": 0})
            self.progress.total_bars_reconciled = progress_data.get('total_bars_reconciled', 0)
            self.progress.total_bars_stored = progress_data.get('total_bars_stored', 0)
            self.progress.recent_errors = progress_data.get('recent_errors', [])
            self.progress.failed_segments = progress_data.get('failed_segments', [])
            self.progress.avg_processing_time_per_segment = progress_data.get('avg_processing_time_per_segment', 0.0)
            
            if progress_data.get('estimated_completion_time'):
                self.progress.estimated_completion_time = datetime.fromisoformat(progress_data['estimated_completion_time'])
            
            # Restore segments
            segments_data = checkpoint_data.get('segments', {})
            for segment_id, segment_data in segments_data.items():
                segment = JobSegment(
                    segment_id=segment_data['segment_id'],
                    symbol=segment_data['symbol'],
                    start_date=datetime.fromisoformat(segment_data['start_date']),
                    end_date=datetime.fromisoformat(segment_data['end_date']),
                    status=JobStatus(segment_data['status']),
                    attempt_count=segment_data.get('attempt_count', 0),
                    error_message=segment_data.get('error_message'),
                    bars_fetched=segment_data.get('bars_fetched', {}),
                    bars_reconciled=segment_data.get('bars_reconciled', 0),
                    bars_stored=segment_data.get('bars_stored', 0)
                )
                
                if segment_data.get('last_attempt'):
                    segment.last_attempt = datetime.fromisoformat(segment_data['last_attempt'])
                
                self.job_segments[segment_id] = segment
            
            # Reset in-progress segments to pending for resume
            in_progress_count = 0
            for segment in self.job_segments.values():
                if segment.status == JobStatus.IN_PROGRESS:
                    segment.status = JobStatus.PENDING
                    in_progress_count += 1
            
            if in_progress_count > 0:
                self.progress.segments_pending += in_progress_count
                self.progress.segments_in_progress = 0
            
            logger.info(f"Enhanced checkpoint loaded for job {self.progress.job_id}")
            logger.info(f"Resume state: {self.progress.segments_completed} completed, "
                       f"{self.progress.segments_failed} failed, "
                       f"{self.progress.segments_pending} pending segments")
            
            if self.progress.estimated_completion_time:
                logger.info(f"Estimated completion: {self.progress.estimated_completion_time}")
            
        except Exception as e:
            logger.error(f"Failed to load enhanced checkpoint: {e}")
            # Continue with fresh start
    
    async def _generate_final_statistics(self) -> Dict[str, Any]:
        """Generate comprehensive final statistics."""
        
        end_time = datetime.now()
        duration_hours = (end_time - self.progress.start_time).total_seconds() / 3600
        
        # Calculate success rates
        total_segments = self.progress.total_segments
        success_rate = self.progress.segments_completed / total_segments if total_segments > 0 else 0
        failure_rate = self.progress.segments_failed / total_segments if total_segments > 0 else 0
        
        # Calculate data efficiency
        total_bars_fetched = sum(self.progress.total_bars_fetched.values())
        reconciliation_rate = (
            self.progress.total_bars_reconciled / total_bars_fetched 
            if total_bars_fetched > 0 else 0
        )
        storage_efficiency = (
            self.progress.total_bars_stored / self.progress.total_bars_reconciled
            if self.progress.total_bars_reconciled > 0 else 0
        )
        
        # Calculate throughput
        bars_per_hour = self.progress.total_bars_stored / duration_hours if duration_hours > 0 else 0
        segments_per_hour = self.progress.segments_completed / duration_hours if duration_hours > 0 else 0
        
        # Get storage statistics
        storage_stats = await self.storage_manager.get_storage_stats()
        
        final_stats = {
            'job_summary': {
                'job_id': self.progress.job_id,
                'start_time': self.progress.start_time,
                'end_time': end_time,
                'duration_hours': duration_hours,
                'status': 'completed' if success_rate > (1 - self.config.failure_threshold) else 'partial_failure'
            },
            'segment_statistics': {
                'total_segments': total_segments,
                'segments_completed': self.progress.segments_completed,
                'segments_failed': self.progress.segments_failed,
                'success_rate': success_rate,
                'failure_rate': failure_rate
            },
            'data_statistics': {
                'total_bars_fetched_by_vendor': self.progress.total_bars_fetched,
                'total_bars_fetched': total_bars_fetched,
                'total_bars_reconciled': self.progress.total_bars_reconciled,
                'total_bars_stored': self.progress.total_bars_stored,
                'reconciliation_rate': reconciliation_rate,
                'storage_efficiency': storage_efficiency
            },
            'performance_metrics': {
                'bars_per_hour': bars_per_hour,
                'segments_per_hour': segments_per_hour,
                'avg_processing_time_per_segment': self.progress.avg_processing_time_per_segment,
                'parallel_efficiency': {
                    'max_concurrent_symbols': self.config.max_concurrent_symbols,
                    'max_concurrent_date_ranges': self.config.max_concurrent_date_ranges,
                    'max_total_workers': self.config.max_total_workers
                }
            },
            'error_analysis': {
                'total_errors': len(self.progress.recent_errors),
                'failed_segments': self.progress.failed_segments,
                'recent_errors': self.progress.recent_errors[-20:],
                'error_rate_per_segment': len(self.progress.recent_errors) / max(total_segments, 1)
            },
            'storage_statistics': storage_stats,
            'configuration': {
                'date_range': f"{self.config.start_date} to {self.config.end_date}",
                'symbols_count': len(self.config.symbols or self._get_default_symbols()),
                'chunk_size_days': self.config.chunk_size_days,
                'reconciliation_method': self.config.reconciliation_method.value,
                'parallel_configuration': {
                    'max_concurrent_symbols': self.config.max_concurrent_symbols,
                    'max_concurrent_date_ranges': self.config.max_concurrent_date_ranges,
                    'max_total_workers': self.config.max_total_workers
                }
            }
        }
        
        return final_stats


# Convenience function for enhanced minute backfill
async def run_enhanced_minute_backfill(
    db_url: str,
    symbols: List[str],
    polygon_api_key: str,
    tiingo_api_key: str,
    start_date: datetime,
    end_date: datetime,
    storage_path: str = "/home/jianjun/ats/data/STK/1min",
    checkpoint_file: str = None,
    max_workers: int = 15
) -> Dict[str, Any]:
    """
    Convenience function for enhanced minute-level backfill.
    
    Args:
        db_url: Database connection URL
        symbols: List of symbols to backfill
        polygon_api_key: Polygon API key
        tiingo_api_key: Tiingo API key
        start_date: Start date for backfill
        end_date: End date for backfill
        storage_path: Base path for storage
        checkpoint_file: Path for checkpoint file
        max_workers: Maximum concurrent workers
    
    Returns:
        Final statistics
    """
    
    # Default checkpoint file
    if not checkpoint_file:
        checkpoint_file = f"{storage_path}/enhanced_minute_backfill_checkpoint.json"
    
    # Create enhanced configuration
    config = EnhancedBackfillConfig(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        polygon_api_key=polygon_api_key,
        tiingo_api_key=tiingo_api_key,
        storage_base_path=storage_path,
        checkpoint_file=checkpoint_file,
        max_total_workers=max_workers,
        max_concurrent_symbols=min(5, max_workers // 3),
        max_concurrent_date_ranges=3,
        chunk_size_days=7,  # Optimized for minute data
        checkpoint_interval_minutes=5,
        auto_checkpoint_segment_count=50
    )
    
    # Create database pool
    pool = await asyncpg.create_pool(db_url, min_size=5, max_size=25)
    
    try:
        # Run enhanced backfill
        async with EnhancedMinuteBackfillOrchestrator(pool, config) as orchestrator:
            return await orchestrator.run_backfill()
    finally:
        await pool.close()