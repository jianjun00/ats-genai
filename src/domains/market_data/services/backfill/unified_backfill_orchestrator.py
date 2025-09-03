"""
Unified 5-Year Backfill Orchestrator

Orchestrates the backfilling of 5 years of 1-minute data from both Polygon and Tiingo,
with cross-vendor reconciliation and hybrid storage management.
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Set, Tuple
from dataclasses import dataclass
import logging
import json
from pathlib import Path

from domains.market_data.services.agent.polygon_minute_adapter import PolygonMinuteAdapter
from domains.market_data.services.agent.tiingo_intraday_adapter import TiingoIntradayAdapter
from domains.market_data.services.reconciliation.cross_vendor_reconciler import (
    CrossVendorReconciler, 
    ReconciliationConfig,
    ReconciliationMethod
)
from infrastructure.storage.hybrid_minute_data_manager import HybridMinuteDataManager, StorageConfig

logger = logging.getLogger(__name__)


@dataclass
class BackfillConfig:
    """Configuration for unified backfill process."""
    
    # Time range
    start_date: datetime
    end_date: datetime
    
    # API credentials
    polygon_api_key: Optional[str] = None
    tiingo_api_key: Optional[str] = None
    
    # Processing configuration
    symbols: List[str] = None
    batch_size: int = 10  # Symbols per batch
    chunk_size_days: int = 30  # Days per chunk
    max_concurrent_symbols: int = 3
    
    # Storage configuration
    storage_base_path: str = "/home/jianjun/ats/data/STK/1min"
    
    # Reconciliation configuration
    reconciliation_method: ReconciliationMethod = ReconciliationMethod.WEIGHTED_AVERAGE
    require_both_vendors: bool = False  # False = use single vendor if other fails
    
    # Error handling
    max_retries: int = 3
    retry_delay_seconds: int = 300  # 5 minutes
    continue_on_error: bool = True
    
    # Progress tracking
    checkpoint_file: Optional[str] = None
    progress_reporting_interval: int = 100  # Bars


@dataclass
class BackfillProgress:
    """Progress tracking for backfill operation."""
    symbols_completed: Set[str] = None
    symbols_failed: Set[str] = None
    current_symbol: Optional[str] = None
    current_date_range: Optional[tuple] = None
    bars_processed: int = 0
    bars_reconciled: int = 0
    errors: List[str] = None
    start_time: Optional[datetime] = None
    last_checkpoint: Optional[datetime] = None
    
    def __post_init__(self):
        if self.symbols_completed is None:
            self.symbols_completed = set()
        if self.symbols_failed is None:
            self.symbols_failed = set()
        if self.errors is None:
            self.errors = []


class UnifiedBackfillOrchestrator:
    """
    Orchestrates unified 5-year backfill from multiple vendors.
    
    Features:
    - Parallel data fetching from Polygon and Tiingo
    - Cross-vendor data reconciliation
    - Hybrid storage management (hot/warm/cold)
    - Progress tracking and checkpointing
    - Error handling and recovery
    - Data quality validation
    """
    
    def __init__(
        self, 
        pool: asyncpg.Pool, 
        config: BackfillConfig,
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
        
        # Progress tracking
        self.progress = BackfillProgress()
        self.load_checkpoint()
        
        # Statistics
        self.stats = {
            'symbols_processed': 0,
            'total_bars_fetched': {'polygon': 0, 'tiingo': 0},
            'total_bars_reconciled': 0,
            'total_bars_stored': 0,
            'data_quality_summary': {},
            'vendor_success_rates': {'polygon': 0.0, 'tiingo': 0.0},
            'reconciliation_stats': {}
        }
    
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
        
        self.reconciler.close()
    
    async def run_backfill(self) -> Dict[str, Any]:
        """
        Execute the complete 5-year backfill process.
        
        Returns:
            Comprehensive statistics and results
        """
        self.progress.start_time = datetime.now()
        logger.info(f"Starting unified 5-year backfill for {len(self.config.symbols)} symbols")
        logger.info(f"Date range: {self.config.start_date} to {self.config.end_date}")
        
        try:
            # Process symbols in batches
            symbol_batches = self._create_symbol_batches()
            
            for batch_idx, symbol_batch in enumerate(symbol_batches):
                logger.info(f"Processing batch {batch_idx + 1}/{len(symbol_batches)}: {symbol_batch}")
                
                # Process batch with date chunking
                await self._process_symbol_batch(symbol_batch)
                
                # Save checkpoint after each batch
                self.save_checkpoint()
            
            # Generate final statistics
            final_stats = await self._generate_final_statistics()
            
            logger.info("Unified backfill completed successfully")
            return final_stats
            
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            self.progress.errors.append(f"Critical error: {e}")
            raise
    
    def _create_symbol_batches(self) -> List[List[str]]:
        """Create batches of symbols for processing."""
        symbols = self.config.symbols or self._get_default_symbols()
        
        # Filter out already completed symbols
        remaining_symbols = [s for s in symbols if s not in self.progress.symbols_completed]
        
        # Create batches
        batches = []
        for i in range(0, len(remaining_symbols), self.config.batch_size):
            batch = remaining_symbols[i:i + self.config.batch_size]
            batches.append(batch)
        
        return batches
    
    def _get_default_symbols(self) -> List[str]:
        """Get default symbol list (S&P 500 or similar)."""
        # Default to major ETFs and tech stocks for testing
        return [
            'SPY', 'QQQ', 'IWM', 'VTI', 'VOO',  # ETFs
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',  # Tech giants
            'NVDA', 'META', 'NFLX', 'CRM', 'ADBE',  # More tech
            'JPM', 'BAC', 'WFC', 'GS', 'C',  # Banks
            'JNJ', 'PG', 'KO', 'PFE', 'WMT'   # Consumer/Healthcare
        ]
    
    async def _process_symbol_batch(self, symbols: List[str]):
        """Process a batch of symbols with date chunking."""
        
        # Create date chunks
        date_chunks = self._create_date_chunks()
        
        for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
            logger.info(f"Processing date chunk {chunk_idx + 1}/{len(date_chunks)}: "
                       f"{chunk_start} to {chunk_end}")
            
            # Process all symbols for this date chunk
            chunk_tasks = []
            for symbol in symbols:
                if symbol not in self.progress.symbols_completed:
                    task = self._process_symbol_date_chunk(symbol, chunk_start, chunk_end)
                    chunk_tasks.append(task)
            
            # Execute with concurrency limit
            semaphore = asyncio.Semaphore(self.config.max_concurrent_symbols)
            
            async def limited_task(task):
                async with semaphore:
                    return await task
            
            # Wait for all symbols in this date chunk
            chunk_results = await asyncio.gather(
                *[limited_task(task) for task in chunk_tasks],
                return_exceptions=True
            )
            
            # Process results
            for i, result in enumerate(chunk_results):
                symbol = symbols[i] if i < len(symbols) else "unknown"
                if isinstance(result, Exception):
                    logger.error(f"Error processing {symbol} for chunk {chunk_start}-{chunk_end}: {result}")
                    self.progress.errors.append(f"{symbol}: {result}")
                else:
                    logger.info(f"Completed {symbol} for chunk {chunk_start}-{chunk_end}")
    
    def _create_date_chunks(self) -> List[Tuple[datetime, datetime]]:
        """Create date chunks for processing."""
        chunks = []
        current_date = self.config.start_date
        
        while current_date < self.config.end_date:
            chunk_end = min(
                current_date + timedelta(days=self.config.chunk_size_days),
                self.config.end_date
            )
            chunks.append((current_date, chunk_end))
            current_date = chunk_end
        
        return chunks
    
    async def _process_symbol_date_chunk(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Process a single symbol for a specific date chunk."""
        
        self.progress.current_symbol = symbol
        self.progress.current_date_range = (start_date, end_date)
        
        chunk_stats = {
            'symbol': symbol,
            'date_range': (start_date, end_date),
            'polygon_bars': 0,
            'tiingo_bars': 0,
            'reconciled_bars': 0,
            'stored_bars': 0,
            'errors': []
        }
        
        try:
            # Fetch data from both vendors concurrently
            polygon_task = self._fetch_polygon_data(symbol, start_date, end_date)
            tiingo_task = self._fetch_tiingo_data(symbol, start_date, end_date)
            
            polygon_data, tiingo_data = await asyncio.gather(
                polygon_task, tiingo_task, return_exceptions=True
            )
            
            # Handle fetch results
            if isinstance(polygon_data, Exception):
                logger.warning(f"Polygon fetch failed for {symbol}: {polygon_data}")
                polygon_data = []
                chunk_stats['errors'].append(f"Polygon: {polygon_data}")
            
            if isinstance(tiingo_data, Exception):
                logger.warning(f"Tiingo fetch failed for {symbol}: {tiingo_data}")
                tiingo_data = []
                chunk_stats['errors'].append(f"Tiingo: {tiingo_data}")
            
            chunk_stats['polygon_bars'] = len(polygon_data)
            chunk_stats['tiingo_bars'] = len(tiingo_data)
            
            # Check if we have any data
            if not polygon_data and not tiingo_data:
                raise ValueError(f"No data from either vendor for {symbol}")
            
            if self.config.require_both_vendors and (not polygon_data or not tiingo_data):
                raise ValueError(f"Missing data from required vendor for {symbol}")
            
            # Reconcile data
            reconciled_bars = await self.reconciler.reconcile_minute_data(
                polygon_data, tiingo_data, symbol
            )
            
            chunk_stats['reconciled_bars'] = len(reconciled_bars)
            
            if not reconciled_bars:
                logger.warning(f"No reconciled data for {symbol}")
                return chunk_stats
            
            # Convert to storage format
            storage_data = self._convert_for_storage(reconciled_bars)
            
            # Store data using hybrid storage manager
            storage_result = await self.storage_manager.store_minute_data(
                symbol, storage_data, force_tier='cold'  # Historical data goes to cold storage
            )
            
            chunk_stats['stored_bars'] = storage_result.get('stored_cold', 0)
            
            # Update progress
            self.progress.bars_processed += chunk_stats['polygon_bars'] + chunk_stats['tiingo_bars']
            self.progress.bars_reconciled += chunk_stats['reconciled_bars']
            
            # Update statistics
            self.stats['total_bars_fetched']['polygon'] += chunk_stats['polygon_bars']
            self.stats['total_bars_fetched']['tiingo'] += chunk_stats['tiingo_bars']
            self.stats['total_bars_reconciled'] += chunk_stats['reconciled_bars']
            self.stats['total_bars_stored'] += chunk_stats['stored_bars']
            
            return chunk_stats
            
        except Exception as e:
            logger.error(f"Error processing {symbol} chunk {start_date}-{end_date}: {e}")
            chunk_stats['errors'].append(str(e))
            self.progress.errors.append(f"{symbol} ({start_date}-{end_date}): {e}")
            
            if not self.config.continue_on_error:
                raise
            
            return chunk_stats
    
    async def _fetch_polygon_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch data from Polygon with retry logic."""
        
        for attempt in range(self.config.max_retries):
            try:
                bars = await self.polygon_adapter.fetch_minute_bars_async(
                    symbol, start_date, end_date
                )
                
                # Convert to dict format
                return [
                    {
                        'symbol': bar.symbol,
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vwap': bar.vwap,
                        'trade_count': bar.trade_count,
                        'vendor': bar.vendor
                    }
                    for bar in bars
                ]
                
            except Exception as e:
                logger.warning(f"Polygon fetch attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay_seconds)
                else:
                    raise
    
    async def _fetch_tiingo_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch data from Tiingo with retry logic."""
        
        for attempt in range(self.config.max_retries):
            try:
                bars = await self.tiingo_adapter.fetch_minute_bars_async(
                    symbol, start_date, end_date
                )
                
                # Convert to dict format
                return [
                    {
                        'symbol': bar.symbol,
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vendor': bar.vendor
                    }
                    for bar in bars
                ]
                
            except Exception as e:
                logger.warning(f"Tiingo fetch attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt < self.config.max_retries - 1:
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
                'quality_score': bar.quality_score,
                'data_source_flags': {
                    'reconciliation_method': bar.reconciliation_method,
                    'source_vendors': bar.source_vendors,
                    'vendor_count': bar.vendor_count,
                    'price_variance': bar.price_variance,
                    'volume_variance': bar.volume_variance
                }
            })
        
        return storage_data
    
    def save_checkpoint(self):
        """Save progress checkpoint to disk."""
        if not self.config.checkpoint_file:
            return
        
        checkpoint_data = {
            'symbols_completed': list(self.progress.symbols_completed),
            'symbols_failed': list(self.progress.symbols_failed),
            'bars_processed': self.progress.bars_processed,
            'bars_reconciled': self.progress.bars_reconciled,
            'errors': self.progress.errors[-50:],  # Keep last 50 errors
            'last_checkpoint': datetime.now().isoformat(),
            'stats': self.stats
        }
        
        try:
            with open(self.config.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
            logger.info(f"Checkpoint saved to {self.config.checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self):
        """Load progress checkpoint from disk."""
        if not self.config.checkpoint_file or not Path(self.config.checkpoint_file).exists():
            return
        
        try:
            with open(self.config.checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            self.progress.symbols_completed = set(checkpoint_data.get('symbols_completed', []))
            self.progress.symbols_failed = set(checkpoint_data.get('symbols_failed', []))
            self.progress.bars_processed = checkpoint_data.get('bars_processed', 0)
            self.progress.bars_reconciled = checkpoint_data.get('bars_reconciled', 0)
            self.progress.errors = checkpoint_data.get('errors', [])
            
            if 'stats' in checkpoint_data:
                self.stats.update(checkpoint_data['stats'])
            
            logger.info(f"Checkpoint loaded from {self.config.checkpoint_file}")
            logger.info(f"Resuming: {len(self.progress.symbols_completed)} symbols completed, "
                       f"{self.progress.bars_processed} bars processed")
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
    
    async def _generate_final_statistics(self) -> Dict[str, Any]:
        """Generate comprehensive final statistics."""
        
        # Calculate vendor success rates
        total_attempts = self.stats['symbols_processed']
        if total_attempts > 0:
            self.stats['vendor_success_rates']['polygon'] = (
                self.stats['total_bars_fetched']['polygon'] > 0
            ) * 100 / total_attempts
            self.stats['vendor_success_rates']['tiingo'] = (
                self.stats['total_bars_fetched']['tiingo'] > 0
            ) * 100 / total_attempts
        
        # Get storage statistics
        storage_stats = await self.storage_manager.get_storage_stats()
        
        # Generate reconciliation statistics
        reconciliation_stats = self.reconciler.get_reconciliation_stats([])  # Would need all bars
        
        final_stats = {
            'execution_summary': {
                'start_time': self.progress.start_time,
                'end_time': datetime.now(),
                'duration_hours': (datetime.now() - self.progress.start_time).total_seconds() / 3600,
                'symbols_completed': len(self.progress.symbols_completed),
                'symbols_failed': len(self.progress.symbols_failed),
                'total_errors': len(self.progress.errors)
            },
            'data_summary': {
                'total_bars_fetched': self.stats['total_bars_fetched'],
                'total_bars_reconciled': self.stats['total_bars_reconciled'],
                'total_bars_stored': self.stats['total_bars_stored'],
                'reconciliation_rate': (
                    self.stats['total_bars_reconciled'] / 
                    sum(self.stats['total_bars_fetched'].values())
                    if sum(self.stats['total_bars_fetched'].values()) > 0 else 0
                )
            },
            'vendor_performance': {
                'success_rates': self.stats['vendor_success_rates'],
                'data_coverage': self.stats['total_bars_fetched']
            },
            'storage_statistics': storage_stats,
            'quality_metrics': {
                'error_rate': len(self.progress.errors) / max(self.progress.bars_processed, 1),
                'data_completeness': self.stats['total_bars_stored'] / max(self.stats['total_bars_reconciled'], 1)
            },
            'recent_errors': self.progress.errors[-20:],  # Last 20 errors
            'configuration': {
                'date_range': f"{self.config.start_date} to {self.config.end_date}",
                'reconciliation_method': self.config.reconciliation_method.value,
                'chunk_size_days': self.config.chunk_size_days,
                'batch_size': self.config.batch_size
            }
        }
        
        return final_stats


# Convenience functions
async def run_5_year_backfill(
    db_url: str,
    symbols: List[str],
    polygon_api_key: str,
    tiingo_api_key: str,
    storage_path: str = "/home/jianjun/ats/data/STK/1min"
) -> Dict[str, Any]:
    """
    Convenience function to run complete 5-year backfill.
    
    Args:
        db_url: Database connection URL
        symbols: List of symbols to backfill
        polygon_api_key: Polygon API key
        tiingo_api_key: Tiingo API key
        storage_path: Base path for storage
    
    Returns:
        Final statistics
    """
    
    # Create configuration
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5 * 365)  # 5 years
    
    config = BackfillConfig(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        polygon_api_key=polygon_api_key,
        tiingo_api_key=tiingo_api_key,
        storage_base_path=storage_path,
        checkpoint_file=f"{storage_path}/backfill_checkpoint.json"
    )
    
    # Create database pool
    pool = await asyncpg.create_pool(db_url, min_size=5, max_size=20)
    
    try:
        # Run backfill
        async with UnifiedBackfillOrchestrator(pool, config) as orchestrator:
            return await orchestrator.run_backfill()
    finally:
        await pool.close()