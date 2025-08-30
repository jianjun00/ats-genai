#!/usr/bin/env python3
"""
FirstRate Minute Bar Backfill Script

Backfills 1-minute historical data from FirstRate's zip files into the ATS database.
Supports parallel processing, checkpointing, and data validation.

Usage:
    python scripts/run_firstrate_minute_backfill.py --symbols AAPL,MSFT,GOOGL
    python scripts/run_firstrate_minute_backfill.py --all-symbols  
    python scripts/run_firstrate_minute_backfill.py --letter A --start-date 2020-01-01
"""

import asyncio
import asyncpg
import argparse
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging
import sys
import os

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from market_data.agent.firstrate_minute_adapter import (
    FirstRateMinuteAdapter, 
    FirstRateMinuteBar,
    FirstRateParsingStats
)
from config.environment import Environment
from core.logging.logger_config import setup_logging

logger = logging.getLogger(__name__)


class FirstRateBackfillOrchestrator:
    """
    Orchestrator for FirstRate minute bar backfill process.
    
    Features:
    - Parallel processing by symbol
    - Progress checkpointing
    - Data validation and deduplication
    - Comprehensive progress reporting
    - Batch database insertion for performance
    """
    
    def __init__(self, pool: asyncpg.Pool, data_path: str = "/mnt/d/ats-data/firstrate-data/stock"):
        self.pool = pool
        self.adapter = FirstRateMinuteAdapter(data_path)
        
        # Processing configuration
        self.batch_size = 1000  # Insert batch size
        self.max_concurrent_symbols = 4  # Limit concurrent symbols for memory
        self.checkpoint_interval = 5000  # Bars per checkpoint
        
        # Statistics tracking
        self.stats = {
            'symbols_processed': 0,
            'symbols_completed': 0,
            'symbols_failed': 0,
            'total_bars_inserted': 0,
            'total_bars_skipped': 0,
            'processing_errors': [],
            'start_time': None,
            'end_time': None
        }
        
        # Checkpoint data
        self.checkpoint_file = "/tmp/firstrate_backfill_checkpoint.json"
        self.completed_symbols = set()
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.adapter.__aenter__()
        self.load_checkpoint()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.adapter.__aexit__(exc_type, exc_val, exc_tb)
        self.save_checkpoint()
    
    async def run_backfill(
        self,
        symbols: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        force_overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Run FirstRate minute bar backfill for specified symbols.
        
        Args:
            symbols: List of symbols to backfill
            start_date: Optional start date filter
            end_date: Optional end date filter
            force_overwrite: Whether to overwrite existing data
            
        Returns:
            Final processing statistics
        """
        
        self.stats['start_time'] = datetime.now()
        logger.info(f"Starting FirstRate backfill for {len(symbols)} symbols")
        
        if start_date:
            logger.info(f"Date filter: {start_date} to {end_date or 'present'}")
        
        # Filter out already completed symbols unless force_overwrite
        if not force_overwrite:
            symbols = [s for s in symbols if s not in self.completed_symbols]
            logger.info(f"Filtered to {len(symbols)} symbols (excluding completed)")
        
        if not symbols:
            logger.info("No symbols to process")
            return self.stats
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.max_concurrent_symbols)
        
        # Create processing tasks
        tasks = []
        for symbol in symbols:
            task = asyncio.create_task(
                self._process_symbol_with_semaphore(
                    semaphore, symbol, start_date, end_date, force_overwrite
                )
            )
            tasks.append((symbol, task))
        
        # Process symbols concurrently
        logger.info(f"Processing {len(tasks)} symbols with {self.max_concurrent_symbols} concurrent workers")
        
        # Wait for all tasks with progress reporting
        completed_count = 0
        for symbol, task in tasks:
            try:
                result = await task
                if result['success']:
                    self.completed_symbols.add(symbol)
                    self.stats['symbols_completed'] += 1
                    self.stats['total_bars_inserted'] += result['bars_inserted']
                    self.stats['total_bars_skipped'] += result['bars_skipped']
                    logger.info(f"✅ {symbol}: {result['bars_inserted']} bars inserted, {result['bars_skipped']} skipped")
                else:
                    self.stats['symbols_failed'] += 1
                    self.stats['processing_errors'].append(f"{symbol}: {result['error']}")
                    logger.error(f"❌ {symbol}: {result['error']}")
                    
            except Exception as e:
                self.stats['symbols_failed'] += 1
                self.stats['processing_errors'].append(f"{symbol}: {e}")
                logger.error(f"❌ {symbol}: Unexpected error: {e}")
            
            completed_count += 1
            self.stats['symbols_processed'] = completed_count
            
            # Progress checkpoint
            if completed_count % 10 == 0:
                self.save_checkpoint()
                logger.info(f"Progress: {completed_count}/{len(symbols)} symbols completed")
        
        self.stats['end_time'] = datetime.now()
        
        # Final statistics
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        bars_per_second = self.stats['total_bars_inserted'] / max(duration, 1)
        
        logger.info("=" * 60)
        logger.info("FIRSTRATE BACKFILL COMPLETED")
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"Symbols completed: {self.stats['symbols_completed']}")
        logger.info(f"Symbols failed: {self.stats['symbols_failed']}")
        logger.info(f"Total bars inserted: {self.stats['total_bars_inserted']:,}")
        logger.info(f"Total bars skipped: {self.stats['total_bars_skipped']:,}")
        logger.info(f"Processing rate: {bars_per_second:.0f} bars/second")
        logger.info("=" * 60)
        
        return self.stats
    
    async def _process_symbol_with_semaphore(
        self, 
        semaphore: asyncio.Semaphore, 
        symbol: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        force_overwrite: bool
    ) -> Dict[str, Any]:
        """Process single symbol with concurrency control."""
        
        async with semaphore:
            return await self._process_single_symbol(symbol, start_date, end_date, force_overwrite)
    
    async def _process_single_symbol(
        self,
        symbol: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime], 
        force_overwrite: bool
    ) -> Dict[str, Any]:
        """Process minute bars for a single symbol."""
        
        logger.info(f"Processing {symbol}...")
        
        try:
            # Check for existing data unless force_overwrite
            if not force_overwrite:
                existing_count = await self._check_existing_data(symbol, start_date, end_date)
                if existing_count > 0:
                    logger.info(f"Skipping {symbol}: {existing_count} bars already exist")
                    return {
                        'success': True,
                        'bars_inserted': 0,
                        'bars_skipped': existing_count,
                        'error': None
                    }
            
            # Fetch bars from FirstRate adapter
            bars = []
            async for bar in self.adapter.fetch_minute_bars_async(
                [symbol], start_date, end_date
            ):
                bars.append(bar)
                
                # Process in batches for memory efficiency
                if len(bars) >= self.batch_size:
                    await self._insert_bars_batch(bars, force_overwrite)
                    bars = []  # Clear batch
            
            # Process remaining bars
            if bars:
                await self._insert_bars_batch(bars, force_overwrite)
            
            # Get final count
            total_bars = await self._count_symbol_bars(symbol, start_date, end_date)
            
            return {
                'success': True,
                'bars_inserted': total_bars,
                'bars_skipped': 0,
                'error': None
            }
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return {
                'success': False,
                'bars_inserted': 0,
                'bars_skipped': 0,
                'error': str(e)
            }
    
    async def _check_existing_data(
        self, 
        symbol: str, 
        start_date: Optional[datetime], 
        end_date: Optional[datetime]
    ) -> int:
        """Check how many bars already exist for symbol in date range."""
        
        query = """
        SELECT COUNT(*) 
        FROM minute_bars 
        WHERE symbol = $1 AND vendor = 'firstrate'
        """
        params = [symbol]
        
        if start_date:
            query += " AND timestamp >= $2"
            params.append(start_date)
        
        if end_date:
            query += f" AND timestamp <= ${len(params) + 1}"
            params.append(end_date)
        
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *params)
    
    async def _count_symbol_bars(
        self, 
        symbol: str, 
        start_date: Optional[datetime], 
        end_date: Optional[datetime]
    ) -> int:
        """Count total bars for symbol in date range."""
        return await self._check_existing_data(symbol, start_date, end_date)
    
    async def _insert_bars_batch(self, bars: List[FirstRateMinuteBar], force_overwrite: bool):
        """Insert batch of bars into database."""
        
        if not bars:
            return
        
        # Prepare data for insertion
        records = []
        for bar in bars:
            bar_dict = bar.to_dict()
            records.append((
                bar_dict['symbol'],
                bar_dict['timestamp'],
                bar_dict['open'],
                bar_dict['high'], 
                bar_dict['low'],
                bar_dict['close'],
                bar_dict['volume'],
                bar_dict.get('vwap'),
                bar_dict.get('returns'),
                bar_dict['quality_score'],
                bar_dict['vendor'],
                bar_dict.get('data_source_flags', {})
            ))
        
        # Insert query with conflict handling
        if force_overwrite:
            query = """
            INSERT INTO minute_bars (
                symbol, timestamp, open, high, low, close, volume, 
                vwap, returns, quality_score, vendor, data_source_flags
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (symbol, timestamp) 
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                vwap = EXCLUDED.vwap,
                returns = EXCLUDED.returns,
                quality_score = EXCLUDED.quality_score,
                vendor = EXCLUDED.vendor,
                data_source_flags = EXCLUDED.data_source_flags,
                updated_at = CURRENT_TIMESTAMP
            """
        else:
            query = """
            INSERT INTO minute_bars (
                symbol, timestamp, open, high, low, close, volume,
                vwap, returns, quality_score, vendor, data_source_flags  
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (symbol, timestamp) DO NOTHING
            """
        
        async with self.pool.acquire() as conn:
            await conn.executemany(query, records)
    
    def save_checkpoint(self):
        """Save progress checkpoint."""
        checkpoint_data = {
            'completed_symbols': list(self.completed_symbols),
            'stats': self.stats.copy(),
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
            logger.debug(f"Checkpoint saved: {len(self.completed_symbols)} symbols completed")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self):
        """Load progress checkpoint."""
        if not Path(self.checkpoint_file).exists():
            return
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            self.completed_symbols = set(checkpoint_data.get('completed_symbols', []))
            
            if self.completed_symbols:
                logger.info(f"Loaded checkpoint: {len(self.completed_symbols)} symbols already completed")
                
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")


async def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(description="FirstRate Minute Bar Backfill")
    parser.add_argument('--symbols', type=str, help='Comma-separated list of symbols (e.g. AAPL,MSFT,GOOGL)')
    parser.add_argument('--all-symbols', action='store_true', help='Process all available symbols')
    parser.add_argument('--letter', type=str, help='Process all symbols starting with letter (e.g. A)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--force-overwrite', action='store_true', help='Overwrite existing data')
    parser.add_argument('--data-path', type=str, default='/mnt/d/ats-data/firstrate-data/stock',
                       help='Path to FirstRate data directory')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without doing it')
    parser.add_argument('--checkpoint-file', type=str, help='Custom checkpoint file path')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging('INFO')
    
    # Parse dates
    start_date = None
    end_date = None
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        logger.info(f"Start date filter: {start_date}")
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')  
        logger.info(f"End date filter: {end_date}")
    
    # Determine symbols to process
    symbols = []
    
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
    elif args.letter:
        # Get all symbols for the letter
        adapter = FirstRateMinuteAdapter(args.data_path)
        async with adapter:
            symbols_by_letter = await adapter.get_available_symbols()
            symbols = symbols_by_letter.get(args.letter.upper(), [])
            logger.info(f"Found {len(symbols)} symbols for letter {args.letter.upper()}")
    elif args.all_symbols:
        # Get all available symbols
        adapter = FirstRateMinuteAdapter(args.data_path)
        async with adapter:
            symbols_by_letter = await adapter.get_available_symbols()
            symbols = []
            for letter_symbols in symbols_by_letter.values():
                symbols.extend(letter_symbols)
            logger.info(f"Found {len(symbols)} total symbols")
    else:
        parser.error("Must specify --symbols, --letter, or --all-symbols")
    
    if not symbols:
        logger.error("No symbols found to process")
        return
        
    logger.info(f"Selected {len(symbols)} symbols: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")
    
    if args.dry_run:
        logger.info("DRY RUN - No data will be processed")
        return
    
    # Setup database connection
    env = Environment()
    db_url = env.get_database_url()
    
    logger.info(f"Connecting to database: {db_url.split('@')[1] if '@' in db_url else 'localhost'}")
    
    pool = await asyncpg.create_pool(
        db_url,
        min_size=2,
        max_size=8,
        command_timeout=60
    )
    
    try:
        # Create orchestrator and run backfill
        async with FirstRateBackfillOrchestrator(pool, args.data_path) as orchestrator:
            if args.checkpoint_file:
                orchestrator.checkpoint_file = args.checkpoint_file
                
            stats = await orchestrator.run_backfill(
                symbols,
                start_date,
                end_date, 
                args.force_overwrite
            )
            
            # Print final summary
            if stats['processing_errors']:
                logger.warning("Processing errors occurred:")
                for error in stats['processing_errors'][-10:]:  # Show last 10 errors
                    logger.warning(f"  {error}")
    
    finally:
        await pool.close()


if __name__ == '__main__':
    asyncio.run(main())