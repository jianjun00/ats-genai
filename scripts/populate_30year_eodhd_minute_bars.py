#!/usr/bin/env python3
"""
EODHD 30-Year Minute Bar Population Script

Populates minute-level OHLCV data for all instruments over the past 30 years
from EODHD API, storing data in monthly Parquet files on D: drive.

Features:
- Checkpoint-based resumable processing for massive scale
- Rate limiting compliant with EODHD API constraints
- File-based storage using existing FileBasedMinuteManager
- Progress tracking and error recovery
- Quality validation and gap detection
- Supports both full and incremental population modes

Usage:
    python scripts/populate_30year_eodhd_minute_bars.py --mode full --limit 10
    python scripts/populate_30year_eodhd_minute_bars.py --mode incremental --symbols AAPL,MSFT
    python scripts/populate_30year_eodhd_minute_bars.py --resume --checkpoint-file last_run.json
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
import argparse
import pandas as pd
from dataclasses import dataclass, asdict
import tempfile
import aiofiles
import asyncpg
import subprocess
import platform
from calendar import monthrange
import pyarrow.parquet as pq
import pyarrow as pa

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Set environment type for Gin config system - use DEV for development
os.environ['ENVIRONMENT_TYPE'] = 'dev'

<<<<<<< Updated upstream
# Note: This script is deprecated and uses non-existent dependencies
# Use populate_firstrate_minute_bars.py or other working scripts instead
import logging

def get_logger(name):
    return logging.getLogger(name)

class EODHDMinuteAdapter:
    """Stub - real implementation doesn't exist"""
    def __init__(self, api_key):
        self.api_key = api_key

class MinuteBar:
    """Stub - use pandas DataFrame instead"""
    def __init__(self, **kwargs):
        pass

=======
from market_data.agent.eodhd_minute_adapter import EODHDMinuteAdapter, EODHDMinuteBar
from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
from core.logging.logger_config import get_logger
from core.run_context import RunContext
>>>>>>> Stashed changes
logger = get_logger(__name__)

@dataclass
class PopulationCheckpoint:
    """Checkpoint data for resumable processing"""
    start_date: str
    end_date: str
    total_symbols: int
    processed_symbols: int
    current_symbol: str
    current_date: str
    symbols_completed: List[str]
    symbols_failed: List[str]
    total_bars_stored: int
    total_files_created: int
    last_update_timestamp: str
    errors: List[Dict]
    processing_stats: Dict
    run_id: Optional[int] = None  # Link to runs table
    missing_months: List[str] = None  # Track missing months per symbol
    existing_months: List[str] = None  # Track existing months per symbol

    def __post_init__(self):
        if self.missing_months is None:
            self.missing_months = []
        if self.existing_months is None:
            self.existing_months = []

class RunsTableManager:
    """Manager for runs table integration"""

    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.run_id = None
        self.connection = None

    async def connect(self):
        """Connect to database"""
        if self.environment == 'dev':
            conn_params = {
                'host': 'localhost',
                'port': 3432,
                'user': 'postgres',
                'password': 'dev_password',
                'database': 'dev_db'
            }
        else:  # intg environment
            conn_params = {
                'host': 'localhost',
                'port': 4432,
                'user': 'postgres',
                'password': 'intg_password',
                'database': 'intg_db'
            }

        self.connection = await asyncpg.connect(**conn_params)

    async def create_run(self, parameters: Dict) -> int:
        """Create new run entry and return run_id"""
        if not self.connection:
            await self.connect()

        # Get git info
        git_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=os.path.dirname(__file__)).decode().strip()
        git_branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], cwd=os.path.dirname(__file__)).decode().strip()
        host_info = {
            'hostname': platform.node(),
            'platform': platform.platform(),
            'python_version': platform.python_version()
        }

        command_line = ' '.join(sys.argv)
        working_directory = os.getcwd()

        query = f"""
        INSERT INTO {self.environment}_runs (
            run_type, status, start_time, parameters, created_by,
            command_line, git_commit_hash, git_branch, environment,
            host_info, working_directory, python_version
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING id
        """

        self.run_id = await self.connection.fetchval(
            query,
            'eodhd_minute_backfill',  # run_type
            'running',                 # status
            datetime.now(),           # start_time
            json.dumps(parameters),   # parameters
            'eodhd_backfill_script', # created_by
            command_line,             # command_line
            git_commit,              # git_commit_hash
            git_branch,              # git_branch
            self.environment,        # environment
            json.dumps(host_info),   # host_info
            working_directory,       # working_directory
            platform.python_version() # python_version
        )

        logger.info(f"Created run entry with ID: {self.run_id}")
        return self.run_id

    async def update_run_progress(self, checkpoint: PopulationCheckpoint):
        """Update run with current progress"""
        if not self.connection or not self.run_id:
            return

        results = {
            'processed_symbols': checkpoint.processed_symbols,
            'total_symbols': checkpoint.total_symbols,
            'symbols_completed': len(checkpoint.symbols_completed),
            'symbols_failed': len(checkpoint.symbols_failed),
            'total_bars_stored': checkpoint.total_bars_stored,
            'total_files_created': checkpoint.total_files_created,
            'current_symbol': checkpoint.current_symbol,
            'current_date': checkpoint.current_date,
            'processing_stats': checkpoint.processing_stats,
            'last_update': checkpoint.last_update_timestamp
        }

        query = f"""
        UPDATE {self.environment}_runs
        SET results = $1, total_symbols = $2, successful_unifications = $3
        WHERE id = $4
        """

        await self.connection.execute(
            query,
            json.dumps(results),
            checkpoint.total_symbols,
            checkpoint.processed_symbols,
            self.run_id
        )

    async def complete_run(self, checkpoint: PopulationCheckpoint, success: bool = True):
        """Mark run as completed"""
        if not self.connection or not self.run_id:
            return

        status = 'completed' if success else 'failed'

        results = {
            'processed_symbols': checkpoint.processed_symbols,
            'total_symbols': checkpoint.total_symbols,
            'symbols_completed': checkpoint.symbols_completed,
            'symbols_failed': checkpoint.symbols_failed,
            'total_bars_stored': checkpoint.total_bars_stored,
            'total_files_created': checkpoint.total_files_created,
            'errors': checkpoint.errors,
            'processing_stats': checkpoint.processing_stats,
            'completion_time': datetime.now().isoformat()
        }

        performance_summary = f"Processed {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols, {checkpoint.total_bars_stored} bars stored"
        quality_summary = f"Success rate: {(checkpoint.processed_symbols - len(checkpoint.symbols_failed))/checkpoint.processed_symbols*100:.1f}%" if checkpoint.processed_symbols > 0 else "No symbols processed"

        query = f"""
        UPDATE {self.environment}_runs
        SET status = $1, end_time = $2, results = $3,
            performance_summary = $4, quality_summary = $5
        WHERE id = $6
        """

        await self.connection.execute(
            query,
            status,
            datetime.now(),
            json.dumps(results),
            performance_summary,
            quality_summary,
            self.run_id
        )

        logger.info(f"Run {self.run_id} marked as {status}")

    async def close(self):
        """Close database connection"""
        if self.connection:
            await self.connection.close()

class EODHD30YearPopulator:
    """Main class for 30-year EODHD minute bar population"""

    def __init__(self,
                 storage_path: str = "/mnt/d/ats-data",
                 checkpoint_file: str = "eodhd_30year_checkpoint.json",
                 max_concurrent: int = 1,  # Conservative for EODHD rate limits
                 debug: bool = False,
                 environment: str = 'dev'):

        self.storage_path = Path(storage_path)
        self.checkpoint_file = Path(checkpoint_file)
        self.max_concurrent = max_concurrent
        self.debug = debug
        self.environment = environment

        # Initialize runs table manager
        self.runs_manager = RunsTableManager(environment)

        # Initialize storage manager for D: drive (if available)
        # Use EODHD-specific storage path to avoid mixing with other vendors
        eodhd_storage_path = self.storage_path / "minute-bars" / "eodhd"
        eodhd_storage_path.mkdir(parents=True, exist_ok=True)

        self.file_manager = FileBasedMinuteManager(
            base_path=str(eodhd_storage_path),
            max_concurrent_operations=max_concurrent,
            backup_enabled=True,
            compression='snappy'
        )
        self.eodhd_adapter = None

        # Processing state
        self.checkpoint = None
        self.universe_symbols: Set[str] = set()
        self.start_date = None
        self.end_date = None

        # Statistics
        self.stats = {
            'symbols_processed': 0,
            'symbols_completed': 0,
            'symbols_failed': 0,
            'total_bars_collected': 0,
            'total_bars_stored': 0,
            'total_files_created': 0,
            'total_api_calls': 0,
            'processing_time_seconds': 0,
            'errors': []
        }

        # Create storage directories
        self.storage_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized EODHD 30-year populator with storage: {self.storage_path}")

        # Gap detection settings
        self.skip_existing = True  # Only process missing data
        self.force_refresh = False  # Force reprocess existing data
        self.gap_analysis = True   # Perform gap analysis before processing

    async def initialize(self):
        """Initialize the populator components"""
        logger.info("Initializing EODHD 30-year populator...")

        # Initialize EODHD adapter - use centralized API key management
        from config.environment import env
        if env:
            api_key = env.get_api_key('eodhd')
        else:
            # Fallback if env not initialized
            api_key = os.getenv('EODHD_API_KEY')
        if not api_key:
            logger.error("❌ No EODHD API key available. Set EODHD_API_KEY environment variable.")
            raise ValueError("EODHD API key required for data collection")

        self.eodhd_adapter = EODHDMinuteAdapter(api_key)
        logger.info("EODHD adapter initialized")
        if not self.checkpoint:
            await self._load_universe()

        logger.info(f"Populator initialization complete. Universe size: {len(self.universe_symbols)}")

    async def _load_universe(self):
        """Load the complete universe of instruments from EODHD"""
        logger.info("Loading instrument universe from EODHD...")

        instruments = self.eodhd_adapter.fetch_instruments()
        self.universe_symbols = {inst.symbol for inst in instruments if inst.symbol}
        logger.info(f"Loaded {len(self.universe_symbols)} instruments from EODHD")

        if self.debug:
            # Limit to small subset for debugging
            self.universe_symbols = set(list(self.universe_symbols)[:10])
            logger.info(f"DEBUG mode: Limited to {len(self.universe_symbols)} symbols")

    async def create_checkpoint(self,
                                start_date: date,
                                end_date: date,
                                symbols: Optional[Set[str]] = None) -> PopulationCheckpoint:
        """Create initial checkpoint for processing"""

        if symbols is None:
            symbols = self.universe_symbols

        # Create run entry in database
        parameters = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'total_symbols': len(symbols),
            'max_concurrent': self.max_concurrent,
            'debug_mode': self.debug,
            'storage_path': str(self.storage_path)
        }

        run_id = await self.runs_manager.create_run(parameters)

        checkpoint = PopulationCheckpoint(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            total_symbols=len(symbols),
            processed_symbols=0,
            current_symbol="",
            current_date=start_date.isoformat(),
            symbols_completed=[],
            symbols_failed=[],
            total_bars_stored=0,
            total_files_created=0,
            last_update_timestamp=datetime.now().isoformat(),
            errors=[],
            processing_stats={},
            run_id=run_id
        )

        await self.save_checkpoint(checkpoint)
        return checkpoint

    async def load_checkpoint(self, checkpoint_file: Optional[Path] = None, run_id: Optional[int] = None) -> Optional[PopulationCheckpoint]:
        """Load checkpoint from file or runs table"""

        # Try to load from runs table first if run_id provided
        if run_id:
            await self.runs_manager.connect()
            query = f"SELECT results, parameters FROM {self.environment}_runs WHERE id = $1"
            row = await self.runs_manager.connection.fetchrow(query, run_id)

            if row and row['results']:
                results = json.loads(row['results'])
                parameters = json.loads(row['parameters'])

                # Reconstruct checkpoint from runs table data
                checkpoint = PopulationCheckpoint(
                    start_date=parameters.get('start_date', ''),
                    end_date=parameters.get('end_date', ''),
                    total_symbols=parameters.get('total_symbols', 0),
                    processed_symbols=results.get('processed_symbols', 0),
                    current_symbol=results.get('current_symbol', ''),
                    current_date=results.get('current_date', ''),
                    symbols_completed=results.get('symbols_completed', []),
                    symbols_failed=results.get('symbols_failed', []),
                    total_bars_stored=results.get('total_bars_stored', 0),
                    total_files_created=results.get('total_files_created', 0),
                    last_update_timestamp=results.get('last_update', datetime.now().isoformat()),
                    errors=results.get('errors', []),
                    processing_stats=results.get('processing_stats', {}),
                    run_id=run_id
                )

                logger.info(f"Loaded checkpoint from runs table: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols processed")
                return checkpoint
        file_path = checkpoint_file or self.checkpoint_file

        if not file_path.exists():
            logger.info(f"No checkpoint file found at {file_path}")
            return None

        async with aiofiles.open(file_path, 'r') as f:
            data = json.loads(await f.read())

        # Handle legacy checkpoints without run_id
        if 'run_id' not in data:
            data['run_id'] = None

        checkpoint = PopulationCheckpoint(**data)
        logger.info(f"Loaded checkpoint from file: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols processed")
        return checkpoint

    async def save_checkpoint(self, checkpoint: PopulationCheckpoint):
        """Save checkpoint to file and update runs table"""

        # Update runs table if run_id exists
        if checkpoint.run_id:
            await self.runs_manager.update_run_progress(checkpoint)

        # Also save to file for backup
        checkpoint.last_update_timestamp = datetime.now().isoformat()

        async with aiofiles.open(self.checkpoint_file, 'w') as f:
            await f.write(json.dumps(asdict(checkpoint), indent=2))

        if self.debug:
            logger.debug(f"Checkpoint saved: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols")

    async def populate_symbol_data(self,
                                   symbol: str,
                                   start_date: date,
                                   end_date: date) -> Dict:
        """Populate minute data for a single symbol across date range"""

        symbol_stats = {
            'symbol': symbol,
            'bars_collected': 0,
            'bars_stored': 0,
            'files_created': 0,
            'api_calls': 0,
            'processing_time': 0,
            'errors': []
        }

        start_time = datetime.now()
        logger.info(f"Starting {symbol} population: {start_date} to {end_date}")

        async with self.eodhd_adapter:
            # Fetch all minute bars for the date range
            bars = await self.eodhd_adapter.fetch_minute_bars_async(
                symbol,
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.min.time())
            )

            symbol_stats['bars_collected'] = len(bars)
            symbol_stats['api_calls'] = (end_date - start_date).days + 1
            self.stats['total_api_calls'] += symbol_stats['api_calls']

            if bars:
                # Convert EODHD bars to MinuteBar format
                minute_bars = []
                for bar in bars:
                    minute_bar = MinuteBar(
                        symbol=bar.symbol,
                        timestamp=bar.timestamp,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        vendor=bar.vendor,
                        quality_score=0.9  # EODHD default quality
                    )
                    minute_bars.append(minute_bar)

                # Store data using file manager
                store_result = await self.file_manager.store_minute_data(
                    symbol,
                    minute_bars,
                    overlap_strategy='merge'  # Handle overlaps gracefully
                )

                symbol_stats['bars_stored'] = store_result.get('stored', 0)
                symbol_stats['files_created'] = store_result.get('files_created', 0)

                self.stats['total_bars_collected'] += symbol_stats['bars_collected']
                self.stats['total_bars_stored'] += symbol_stats['bars_stored']
                self.stats['total_files_created'] += symbol_stats['files_created']

                logger.info(f"{symbol}: {symbol_stats['bars_collected']} bars collected, "
                           f"{symbol_stats['bars_stored']} stored, "
                           f"{symbol_stats['files_created']} files created")
            else:
                logger.warning(f"{symbol}: No data available for date range")

        symbol_stats['processing_time'] = (datetime.now() - start_time).total_seconds()
        return symbol_stats

    async def analyze_existing_data(self, symbol: str, start_date: date, end_date: date) -> Dict:
        """Analyze existing data for a symbol to identify gaps"""
        analysis = {
            'existing': [],
            'missing': [],
            'total_expected_months': 0
        }

        # Generate expected month-year combinations
        current_date = start_date.replace(day=1)
        end_month = end_date.replace(day=1)

        expected_months = []
        while current_date <= end_month:
            expected_months.append((current_date.year, current_date.month))
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        analysis['total_expected_months'] = len(expected_months)

        # Check which files exist
        for year, month in expected_months:
            file_path = self.file_manager._get_monthly_file_path(symbol, year, month)
            if file_path.exists():
                analysis['existing'].append((year, month))
            else:
                analysis['missing'].append((year, month))

        return analysis

    async def populate_missing_data_only(self,
                                        symbol: str,
                                        start_date: date,
                                        end_date: date) -> Dict:
        """Populate only missing data gaps for a symbol (incremental backfill)"""

        symbol_stats = {
            'symbol': symbol,
            'bars_collected': 0,
            'bars_stored': 0,
            'files_created': 0,
            'api_calls': 0,
            'processing_time': 0,
            'errors': [],
            'months_processed': 0,
            'months_skipped': 0
        }

        start_time = datetime.now()
        logger.info(f"Starting incremental backfill for {symbol}: {start_date} to {end_date}")

        # Analyze existing data to find gaps
        analysis = await self.analyze_existing_data(symbol, start_date, end_date)
        missing_months = analysis['missing']
        existing_months = analysis['existing']

        if not missing_months:
            logger.info(f"✅ {symbol}: No missing data - skipping")
            symbol_stats['months_skipped'] = len(existing_months)
            return symbol_stats

        logger.info(f"📝 {symbol}: Processing {len(missing_months)} missing months, "
                   f"skipping {len(existing_months)} existing months")

        async with self.eodhd_adapter:
            # Process only missing months
            for year, month in missing_months:
                month_start = date(year, month, 1)
                # Get last day of month
                if month == 12:
                    month_end = date(year + 1, 1, 1) - timedelta(days=1)
                else:
                    month_end = date(year, month + 1, 1) - timedelta(days=1)

                logger.info(f"📊 Fetching {symbol} data for {year}-{month:02d}")

                # Fetch data for this specific month only
                month_bars = await self.eodhd_adapter.fetch_minute_bars_async(
                    symbol,
                    datetime.combine(month_start, datetime.min.time()),
                    datetime.combine(month_end, datetime.min.time())
                )

                month_api_calls = (month_end - month_start).days + 1
                symbol_stats['api_calls'] += month_api_calls
                self.stats['total_api_calls'] += month_api_calls

                if month_bars:
                    # Convert to MinuteBar format
                    minute_bars = []
                    for bar in month_bars:
                        minute_bar = MinuteBar(
                            symbol=bar.symbol,
                            timestamp=bar.timestamp,
                            open=bar.open,
                            high=bar.high,
                            low=bar.low,
                            close=bar.close,
                            volume=bar.volume,
                            vendor="eodhd",  # Explicitly mark as EODHD data
                            quality_score=0.9
                        )
                        minute_bars.append(minute_bar)

                    # Store month data
                    store_result = await self.file_manager.store_minute_data(
                        symbol,
                        minute_bars,
                        overlap_strategy='skip'  # Skip overlaps for incremental
                    )

                    symbol_stats['bars_collected'] += len(month_bars)
                    symbol_stats['bars_stored'] += store_result.get('stored', 0)
                    symbol_stats['files_created'] += store_result.get('files_created', 0)
                    symbol_stats['months_processed'] += 1

                    logger.info(f"✅ {symbol} {year}-{month:02d}: {len(month_bars)} bars collected, "
                               f"{store_result.get('stored', 0)} stored")
                else:
                    logger.warning(f"⚠️ {symbol} {year}-{month:02d}: No data available")

                # Rate limiting between months
                await asyncio.sleep(1.0)

        self.stats['total_bars_collected'] += symbol_stats['bars_collected']
        self.stats['total_bars_stored'] += symbol_stats['bars_stored']
        self.stats['total_files_created'] += symbol_stats['files_created']

        symbol_stats['processing_time'] = (datetime.now() - start_time).total_seconds()

        logger.info(f"🏁 {symbol} incremental backfill complete: "
                   f"{symbol_stats['months_processed']} months processed, "
                   f"{symbol_stats['months_skipped']} months skipped")

        return symbol_stats

    async def run_full_population(self,
                                  start_date: date,
                                  end_date: date,
                                  limit: Optional[int] = None,
                                  symbols: Optional[List[str]] = None):
        """Run full 30-year population"""

        logger.info(f"Starting full population: {start_date} to {end_date}")

        # Determine symbol list
        if symbols:
            target_symbols = set(symbols)
        else:
            target_symbols = self.universe_symbols

        if limit:
            target_symbols = set(list(target_symbols)[:limit])

        logger.info(f"Processing {len(target_symbols)} symbols")

        # Create initial checkpoint
        self.checkpoint = await self.create_checkpoint(start_date, end_date, target_symbols)

        # Process each symbol
        for symbol in sorted(target_symbols):
            self.checkpoint.current_symbol = symbol
            await self.save_checkpoint(self.checkpoint)

            # Use incremental backfill - only process missing data
            symbol_stats = await self.populate_missing_data_only(symbol, start_date, end_date)

            if symbol_stats['errors']:
                self.checkpoint.symbols_failed.append(symbol)
                self.stats['symbols_failed'] += 1
            else:
                self.checkpoint.symbols_completed.append(symbol)
                self.stats['symbols_completed'] += 1

            self.checkpoint.processed_symbols += 1
            self.stats['symbols_processed'] += 1

            # Update checkpoint every symbol
            await self.save_checkpoint(self.checkpoint)

            # Progress report
            progress = (self.checkpoint.processed_symbols / self.checkpoint.total_symbols) * 100
            logger.info(f"Progress: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} "
                       f"({progress:.1f}%) - Current: {symbol}")

        logger.info("Full population complete")
        await self._print_final_stats()

        # Mark run as completed
        if self.checkpoint and self.checkpoint.run_id:
            success = len(self.checkpoint.symbols_failed) == 0
            await self.runs_manager.complete_run(self.checkpoint, success)

    async def resume_population(self, checkpoint_file: Optional[str] = None):
        """Resume population from checkpoint"""

        checkpoint_path = Path(checkpoint_file) if checkpoint_file else self.checkpoint_file
        self.checkpoint = await self.load_checkpoint(checkpoint_path)

        if not self.checkpoint:
            logger.error("No valid checkpoint found for resume")
            return

        logger.info(f"Resuming population from checkpoint: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} symbols processed")

        # Reconstruct remaining symbols
        all_symbols = set(self.checkpoint.symbols_completed + self.checkpoint.symbols_failed)
        if len(all_symbols) < self.checkpoint.total_symbols:
            # We need to determine what symbols were being processed
            # This is a limitation - we should store the full symbol list in checkpoint
            logger.warning("Checkpoint doesn't contain full symbol list - using current universe")
            if not self.universe_symbols:
                await self._load_universe()
            remaining_symbols = self.universe_symbols - all_symbols
        else:
            remaining_symbols = set()

        if not remaining_symbols:
            logger.info("No remaining symbols to process")
            await self._print_final_stats()
            return

        start_date = date.fromisoformat(self.checkpoint.start_date)
        end_date = date.fromisoformat(self.checkpoint.end_date)

        # Continue processing remaining symbols
        for symbol in sorted(remaining_symbols):
            self.checkpoint.current_symbol = symbol
            await self.save_checkpoint(self.checkpoint)

            symbol_stats = await self.populate_symbol_data(symbol, start_date, end_date)

            if symbol_stats['errors']:
                self.checkpoint.symbols_failed.append(symbol)
                self.stats['symbols_failed'] += 1
            else:
                self.checkpoint.symbols_completed.append(symbol)
                self.stats['symbols_completed'] += 1

            self.checkpoint.processed_symbols += 1
            self.stats['symbols_processed'] += 1

            await self.save_checkpoint(self.checkpoint)

            progress = (self.checkpoint.processed_symbols / self.checkpoint.total_symbols) * 100
            logger.info(f"Progress: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} "
                       f"({progress:.1f}%) - Current: {symbol}")

        logger.info("Resume population complete")
        await self._print_final_stats()

        # Mark run as completed
        if self.checkpoint and self.checkpoint.run_id:
            success = len(self.checkpoint.symbols_failed) == 0
            await self.runs_manager.complete_run(self.checkpoint, success)

    async def _print_final_stats(self):
        """Print comprehensive final statistics"""

        logger.info("=" * 80)
        logger.info("EODHD 30-YEAR POPULATION FINAL STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"Symbols completed: {self.stats['symbols_completed']}")
        logger.info(f"Symbols failed: {self.stats['symbols_failed']}")
        logger.info(f"Total bars collected: {self.stats['total_bars_collected']:,}")
        logger.info(f"Total bars stored: {self.stats['total_bars_stored']:,}")
        logger.info(f"Total files created: {self.stats['total_files_created']}")
        logger.info(f"Total API calls: {self.stats['total_api_calls']:,}")

        if self.stats['errors']:
            logger.info(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                logger.info(f"  - {error}")
            if len(self.stats['errors']) > 5:
                logger.info(f"  ... and {len(self.stats['errors']) - 5} more errors")

        # Get storage stats
        storage_stats = await self.file_manager.get_storage_stats()
        logger.info(f"Storage statistics:")
        logger.info(f"  - Total files: {storage_stats.get('files', 0)}")
        logger.info(f"  - Total symbols: {storage_stats.get('symbols', 0)}")
        logger.info(f"  - Total size: {storage_stats.get('total_size_mb', 0):.2f} MB")
        logger.info("=" * 80)

    async def close(self):
        """Clean up resources"""
        if self.file_manager:
            await self.file_manager.close()
        logger.info("Resources cleaned up successfully")
    async def _store_data_direct(self, symbol: str, minute_bars: List) -> Dict:
        """Direct parquet storage fallback when file manager not available"""

        storage_stats = {'stored': 0, 'files_created': 0}

        # Group bars by month
        monthly_data = {}
        for bar in minute_bars:
            month_key = bar.timestamp.strftime('%Y_%m')
            if month_key not in monthly_data:
                monthly_data[month_key] = []
            monthly_data[month_key].append({
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'symbol': symbol
            })

        # Store each month
        for month_key, bars_data in monthly_data.items():
            year, month = month_key.split('_')
            output_dir = self.storage_path / "minute-bars" / symbol / year / month
            output_dir.mkdir(parents=True, exist_ok=True)

            output_file = output_dir / f"{symbol}_{month_key}.parquet"

            # Convert to DataFrame and save
            df = pd.DataFrame(bars_data)
            df.to_parquet(output_file, index=False)

            storage_stats['stored'] += len(bars_data)
            storage_stats['files_created'] += 1

            logger.info(f"Stored {len(bars_data)} bars for {symbol} {month_key}")

        return storage_stats

    async def _generate_gap_report(self):
        """Generate comprehensive gap analysis report"""

        logger.info("\n" + "=" * 80)
        logger.info("EODHD MINUTE BAR GAP ANALYSIS REPORT")
        logger.info("=" * 80)

        if not self.checkpoint:
            logger.warning("No checkpoint available for gap report")
            return

        # Analyze gaps for completed symbols
        total_gaps = 0
        symbols_with_gaps = 0

        for symbol in self.checkpoint.symbols_completed[:10]:  # Sample first 10
            start_date = date.fromisoformat(self.checkpoint.start_date)
            end_date = date.fromisoformat(self.checkpoint.end_date)

            data_analysis = await self.analyze_existing_data(symbol, start_date, end_date)
            missing_count = len(data_analysis['missing'])
            existing_count = len(data_analysis['existing'])

            if missing_count > 0:
                symbols_with_gaps += 1
                total_gaps += missing_count

            coverage = (existing_count / (existing_count + missing_count)) * 100 if (existing_count + missing_count) > 0 else 0
            logger.info(f"{symbol:8}: {coverage:6.1f}% coverage ({existing_count:3} months complete, {missing_count:3} missing)")

        logger.info(f"\nSummary: {symbols_with_gaps} symbols with gaps, {total_gaps} total missing months")
        logger.info("=" * 80 + "\n")

async def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(description="EODHD 30-Year Minute Bar Population")
    parser.add_argument('--mode', choices=['full', 'incremental'], default='full',
                        help='Population mode')
    parser.add_argument('--start-date', type=str,
                        default=(datetime.now() - timedelta(days=30*365)).strftime('%Y-%m-%d'),
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                        default=datetime.now().strftime('%Y-%m-%d'),
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--symbols', type=str,
                        help='Comma-separated list of symbols (optional)')
    parser.add_argument('--limit', type=int,
                        help='Limit number of symbols to process')
    parser.add_argument('--storage-path', type=str,
                        default=os.getenv('STORAGE_PATH', '/data'),
                        help='Storage path for minute bar files')
    parser.add_argument('--checkpoint-file', type=str, default='eodhd_30year_checkpoint.json',
                        help='Checkpoint file for resumable processing')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode (limited symbols)')
    parser.add_argument('--concurrent', type=int, default=1,
                        help='Max concurrent operations (be conservative with EODHD)')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                        help='Skip existing data and only fetch missing months (default: True)')
    parser.add_argument('--force-refresh', action='store_true',
                        help='Force refresh all data, ignoring existing files')
    parser.add_argument('--gap-analysis', action='store_true', default=True,
                        help='Perform gap analysis before processing (default: True)')
    parser.add_argument('--run-id', type=int,
                        help='Resume from specific run ID in runs table')
    parser.add_argument('--environment', type=str, default='dev', choices=['dev', 'intg'],
                        help='Database environment')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting EODHD 30-year minute bar population")
    logger.info(f"Arguments: {vars(args)}")

    # Initialize populator
    populator = EODHD30YearPopulator(
        storage_path=args.storage_path,
        checkpoint_file=args.checkpoint_file,
        max_concurrent=args.concurrent,
        debug=args.debug,
        environment=args.environment
    )

    # Configure incremental processing settings
    populator.skip_existing = args.skip_existing and not args.force_refresh
    populator.force_refresh = args.force_refresh
    populator.gap_analysis = args.gap_analysis

    await populator.initialize()

    if args.resume:
        # Resume from checkpoint
        if args.run_id:
            # Load checkpoint from runs table
            populator.checkpoint = await populator.load_checkpoint(run_id=args.run_id)
            if populator.checkpoint:
                await populator.resume_population()
            else:
                logger.error(f"No checkpoint found for run ID {args.run_id}")
        else:
            await populator.resume_population(args.checkpoint_file)
    else:
        # Start new population
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)

        symbols = None
        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(',')]

        await populator.run_full_population(
            start_date=start_date,
            end_date=end_date,
            limit=args.limit,
            symbols=symbols
        )

    logger.info("EODHD 30-year population script completed")

if __name__ == "__main__":
    asyncio.run(main())