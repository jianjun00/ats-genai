#!/usr/bin/env python3
"""
Polygon 30-Year Minute Bar Population Script

Populates minute-level OHLCV data for all instruments over the past 30 years
from Polygon API, storing data in monthly Parquet files on D: drive.

Key Features:
- Uses existing PolygonMinuteAdapter for API integration
- Leverages FileBasedMinuteManager for storage
- Checkpoint-based resumable processing for massive scale
- Rate limiting compliant with Polygon API constraints
- Quality validation and technical indicator calculation
- Progress tracking and comprehensive error recovery

Usage:
    python scripts/populate_30year_polygon_minute_bars.py --mode full --limit 10
    python scripts/populate_30year_polygon_minute_bars.py --mode incremental --symbols AAPL,MSFT
    python scripts/populate_30year_polygon_minute_bars.py --resume --checkpoint-file last_run.json
    python scripts/populate_30year_polygon_minute_bars.py --debug --concurrent 2
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Optional, Set
import argparse
import pandas as pd
from dataclasses import dataclass, asdict
import tempfile
import aiofiles
import time

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter, MinuteBar as PolygonMinuteBar
from market_data.agent.polygon_adapter import PolygonAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
from core.logging.logger_config import get_logger
from config.database import Database
import asyncpg

logger = get_logger(__name__)

@dataclass
class PolygonPopulationCheckpoint:
    """Checkpoint data for resumable Polygon processing"""
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
    total_api_calls: int
    last_update_timestamp: str
    rate_limit_delays: int
    quality_scores: Dict[str, float]
    errors: List[Dict]
    processing_stats: Dict

class Polygon30YearPopulator:
    """Main class for 30-year Polygon minute bar population"""
    
    def __init__(self, 
                 storage_path: str = "/mnt/d/ats-data",
                 checkpoint_file: str = "polygon_30year_checkpoint.json",
                 max_concurrent: int = 2,  # Conservative for Polygon rate limits
                 premium_plan: bool = False,
                 debug: bool = False):
        
        self.storage_path = Path(storage_path)
        self.checkpoint_file = Path(checkpoint_file)
        self.max_concurrent = max_concurrent
        self.premium_plan = premium_plan
        self.debug = debug
        
        # Initialize storage manager for D: drive with Polygon-specific path
        self.file_manager = FileBasedMinuteManager(
            base_path=str(self.storage_path / "polygon" / "minute-bars"),
            max_concurrent_operations=max_concurrent,
            backup_enabled=True,
            compression='snappy'
        )
        
        # Initialize Polygon adapters
        self.polygon_minute_adapter = None
        self.polygon_adapter = None
        
        # Processing state
        self.checkpoint = None
        self.universe_symbols: Set[str] = set()
        self.start_date = None
        self.end_date = None
        
        # Rate limiting configuration based on plan
        if premium_plan:
            self.requests_per_minute = 100
            self.delay_between_requests = 0.6  # seconds
        else:
            self.requests_per_minute = 5
            self.delay_between_requests = 12  # seconds (conservative)
        
        # Statistics
        self.stats = {
            'symbols_processed': 0,
            'symbols_completed': 0,
            'symbols_failed': 0,
            'total_bars_collected': 0,
            'total_bars_stored': 0,
            'total_files_created': 0,
            'total_api_calls': 0,
            'rate_limit_delays': 0,
            'processing_time_seconds': 0,
            'average_quality_score': 0.0,
            'errors': []
        }
        
        # Create storage directories
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized Polygon 30-year populator with storage: {self.storage_path}")
        logger.info(f"Rate limiting: {self.requests_per_minute} req/min, {self.delay_between_requests}s delay")
    
    async def initialize(self):
        """Initialize the populator components"""
        logger.info("Initializing Polygon 30-year populator...")
        
        # Initialize Polygon adapters
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            raise ValueError("POLYGON_API_KEY environment variable must be set")
        
        self.polygon_minute_adapter = PolygonMinuteAdapter(api_key)
        self.polygon_adapter = PolygonAdapter(api_key)
        logger.info("Polygon adapters initialized")
        
        # Load universe if not resuming
        if not self.checkpoint:
            await self._load_universe()
        
        logger.info(f"Populator initialization complete. Universe size: {len(self.universe_symbols)}")
    
    async def _load_universe(self):
        """Load the complete universe of instruments from dev database"""
        logger.info("Loading instrument universe from dev database...")
        
        try:
            # Initialize database connection - use Docker container directly
            pool = await asyncpg.create_pool(
                host='localhost',
                port=5432,
                user='postgres',
                password='',  # Docker container uses no password
                database='dev_db',
                min_size=1,
                max_size=5
            )
            
            # Query for active US equity symbols directly from dev_instruments  
            query = """
            SELECT DISTINCT symbol 
            FROM dev_instruments
            WHERE active = true
              AND symbol ~ '^[A-Z]{1,5}$'
              AND symbol IS NOT NULL
              AND symbol != ''
            ORDER BY symbol
            """
            
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                self.universe_symbols = {row['symbol'] for row in rows}
            
            await pool.close()
            logger.info(f"Loaded {len(self.universe_symbols)} US equity symbols from dev database")
            
            if self.debug:
                # Limit to small subset for debugging
                self.universe_symbols = set(list(self.universe_symbols)[:10])
                logger.info(f"DEBUG mode: Limited to {len(self.universe_symbols)} symbols")
                
        except Exception as e:
            logger.error(f"Failed to load universe from database: {e}")
            logger.warning("Falling back to Polygon API universe loading...")
            
            # Fallback to Polygon API approach
            try:
                instruments = self.polygon_adapter.fetch_instruments()
                us_equities = [
                    inst for inst in instruments 
                    if inst.symbol and (inst.exchange in ['XNYS', 'XNAS', 'ARCX', 'BATS', 'NYSE', 'NASDAQ', 'NYSEARCA', 'NYSEMKT'] or inst.exchange is None)
                ]
                self.universe_symbols = {inst.symbol for inst in us_equities}
                logger.info(f"Loaded {len(self.universe_symbols)} US equity symbols from Polygon API fallback")
            except Exception as e2:
                logger.error(f"Polygon API fallback also failed: {e2}")
                # Final fallback to major symbols
                self.universe_symbols = {
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 
                    'JNJ', 'V', 'PG', 'UNH', 'DIS', 'NFLX', 'CRM', 'ADBE', 'KO',
                    'WMT', 'BAC', 'HD', 'PFE', 'MA', 'T', 'VZ', 'MRK', 'INTC'
                }
                logger.warning(f"Using final fallback universe: {len(self.universe_symbols)} symbols")
    
    async def create_checkpoint(self, 
                                start_date: date, 
                                end_date: date,
                                symbols: Optional[Set[str]] = None) -> PolygonPopulationCheckpoint:
        """Create initial checkpoint for processing"""
        
        if symbols is None:
            symbols = self.universe_symbols
        
        checkpoint = PolygonPopulationCheckpoint(
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
            total_api_calls=0,
            last_update_timestamp=datetime.now().isoformat(),
            rate_limit_delays=0,
            quality_scores={},
            errors=[],
            processing_stats={}
        )
        
        await self.save_checkpoint(checkpoint)
        return checkpoint
    
    async def load_checkpoint(self, checkpoint_file: Optional[Path] = None) -> Optional[PolygonPopulationCheckpoint]:
        """Load checkpoint from file"""
        file_path = checkpoint_file or self.checkpoint_file
        
        if not file_path.exists():
            logger.info(f"No checkpoint file found at {file_path}")
            return None
        
        try:
            async with aiofiles.open(file_path, 'r') as f:
                data = json.loads(await f.read())
            
            checkpoint = PolygonPopulationCheckpoint(**data)
            logger.info(f"Loaded checkpoint: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols processed")
            return checkpoint
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    async def save_checkpoint(self, checkpoint: PolygonPopulationCheckpoint):
        """Save checkpoint to file"""
        try:
            checkpoint.last_update_timestamp = datetime.now().isoformat()
            
            async with aiofiles.open(self.checkpoint_file, 'w') as f:
                await f.write(json.dumps(asdict(checkpoint), indent=2))
            
            if self.debug:
                logger.debug(f"Checkpoint saved: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols")
                
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
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
            'quality_score': 0.0,
            'rate_limit_delays': 0,
            'errors': []
        }
        
        start_time = time.time()
        logger.info(f"Starting {symbol} population: {start_date} to {end_date}")
        
        try:
            async with self.polygon_minute_adapter:
                # Split into smaller chunks to respect API limits and reduce memory usage
                chunk_days = 30 if not self.premium_plan else 90  # Smaller chunks for free tier
                current_date = start_date
                all_bars = []
                
                while current_date <= end_date:
                    chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
                    
                    logger.info(f"{symbol}: Fetching {current_date} to {chunk_end}")
                    
                    # Add delay between requests for rate limiting
                    await asyncio.sleep(self.delay_between_requests)
                    
                    try:
                        chunk_bars = await self.polygon_minute_adapter.fetch_minute_bars_async(
                            symbol,
                            datetime.combine(current_date, datetime.min.time()),
                            datetime.combine(chunk_end, datetime.min.time())
                        )
                        
                        all_bars.extend(chunk_bars)
                        symbol_stats['api_calls'] += 1
                        self.stats['total_api_calls'] += 1
                        
                        logger.info(f"{symbol}: Got {len(chunk_bars)} bars for chunk {current_date} to {chunk_end}")
                        
                    except Exception as e:
                        if "rate limit" in str(e).lower() or "429" in str(e):
                            logger.warning(f"{symbol}: Rate limit hit, waiting 60 seconds...")
                            await asyncio.sleep(60)
                            symbol_stats['rate_limit_delays'] += 1
                            self.stats['rate_limit_delays'] += 1
                            continue
                        else:
                            raise e
                    
                    current_date = chunk_end + timedelta(days=1)
                
                symbol_stats['bars_collected'] = len(all_bars)
                
                if all_bars:
                    # Validate data quality
                    quality_metrics = self.polygon_minute_adapter.validate_data_quality(all_bars)
                    symbol_stats['quality_score'] = 1.0 if quality_metrics['valid'] else 0.5
                    
                    # Convert Polygon bars to FileManager format
                    minute_bars = []
                    for bar in all_bars:
                        minute_bar = MinuteBar(
                            symbol=bar.symbol,
                            timestamp=bar.timestamp,
                            open=bar.open,
                            high=bar.high,
                            low=bar.low,
                            close=bar.close,
                            volume=bar.volume,
                            vwap=bar.vwap,
                            trade_count=bar.trade_count,
                            vendor='polygon',
                            quality_score=symbol_stats['quality_score']
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
                               f"{symbol_stats['files_created']} files created, "
                               f"quality: {symbol_stats['quality_score']:.2f}")
                else:
                    logger.warning(f"{symbol}: No data available for date range")
                    
        except Exception as e:
            error_msg = f"Error processing {symbol}: {e}"
            logger.error(error_msg)
            symbol_stats['errors'].append({
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            })
            self.stats['errors'].append(error_msg)
        
        symbol_stats['processing_time'] = time.time() - start_time
        return symbol_stats
    
    async def run_full_population(self, 
                                  start_date: date,
                                  end_date: date,
                                  limit: Optional[int] = None,
                                  symbols: Optional[List[str]] = None):
        """Run full 30-year population"""
        
        logger.info(f"Starting full Polygon population: {start_date} to {end_date}")
        
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
        total_quality_score = 0
        for i, symbol in enumerate(sorted(target_symbols)):
            try:
                self.checkpoint.current_symbol = symbol
                await self.save_checkpoint(self.checkpoint)
                
                symbol_stats = await self.populate_symbol_data(symbol, start_date, end_date)
                
                if symbol_stats['errors']:
                    self.checkpoint.symbols_failed.append(symbol)
                    self.stats['symbols_failed'] += 1
                else:
                    self.checkpoint.symbols_completed.append(symbol)
                    self.stats['symbols_completed'] += 1
                    total_quality_score += symbol_stats['quality_score']
                
                self.checkpoint.processed_symbols += 1
                self.checkpoint.total_bars_stored = self.stats['total_bars_stored']
                self.checkpoint.total_files_created = self.stats['total_files_created']
                self.checkpoint.total_api_calls = self.stats['total_api_calls']
                self.checkpoint.rate_limit_delays = self.stats['rate_limit_delays']
                self.checkpoint.quality_scores[symbol] = symbol_stats['quality_score']
                
                self.stats['symbols_processed'] += 1
                
                # Calculate average quality score
                if self.stats['symbols_completed'] > 0:
                    self.stats['average_quality_score'] = total_quality_score / self.stats['symbols_completed']
                
                # Update checkpoint every symbol
                await self.save_checkpoint(self.checkpoint)
                
                # Progress report
                progress = (self.checkpoint.processed_symbols / self.checkpoint.total_symbols) * 100
                estimated_remaining = (len(target_symbols) - i - 1) * symbol_stats['processing_time']
                estimated_remaining_hours = estimated_remaining / 3600
                
                logger.info(f"Progress: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} "
                           f"({progress:.1f}%) - Current: {symbol}")
                logger.info(f"Quality: {symbol_stats['quality_score']:.2f}, "
                           f"API calls: {self.stats['total_api_calls']:,}, "
                           f"Estimated remaining: {estimated_remaining_hours:.1f}h")
                
            except Exception as e:
                logger.error(f"Critical error processing {symbol}: {e}")
                self.checkpoint.symbols_failed.append(symbol)
                self.stats['symbols_failed'] += 1
        
        logger.info("Full Polygon population complete")
        await self._print_final_stats()
    
    async def resume_population(self, checkpoint_file: Optional[str] = None):
        """Resume population from checkpoint"""
        
        checkpoint_path = Path(checkpoint_file) if checkpoint_file else self.checkpoint_file
        self.checkpoint = await self.load_checkpoint(checkpoint_path)
        
        if not self.checkpoint:
            logger.error("No valid checkpoint found for resume")
            return
        
        logger.info(f"Resuming Polygon population from checkpoint: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} symbols processed")
        
        # Load existing stats from checkpoint
        self.stats['total_bars_stored'] = self.checkpoint.total_bars_stored
        self.stats['total_files_created'] = self.checkpoint.total_files_created
        self.stats['total_api_calls'] = self.checkpoint.total_api_calls
        self.stats['rate_limit_delays'] = self.checkpoint.rate_limit_delays
        
        # Determine remaining symbols
        completed_symbols = set(self.checkpoint.symbols_completed + self.checkpoint.symbols_failed)
        if not self.universe_symbols:
            await self._load_universe()
        remaining_symbols = self.universe_symbols - completed_symbols
        
        if not remaining_symbols:
            logger.info("No remaining symbols to process")
            await self._print_final_stats()
            return
        
        start_date = date.fromisoformat(self.checkpoint.start_date)
        end_date = date.fromisoformat(self.checkpoint.end_date)
        
        logger.info(f"Resuming with {len(remaining_symbols)} remaining symbols")
        
        # Continue processing remaining symbols
        total_quality_score = sum(self.checkpoint.quality_scores.values())
        for symbol in sorted(remaining_symbols):
            try:
                self.checkpoint.current_symbol = symbol
                await self.save_checkpoint(self.checkpoint)
                
                symbol_stats = await self.populate_symbol_data(symbol, start_date, end_date)
                
                if symbol_stats['errors']:
                    self.checkpoint.symbols_failed.append(symbol)
                    self.stats['symbols_failed'] += 1
                else:
                    self.checkpoint.symbols_completed.append(symbol)
                    self.stats['symbols_completed'] += 1
                    total_quality_score += symbol_stats['quality_score']
                
                self.checkpoint.processed_symbols += 1
                self.checkpoint.total_bars_stored = self.stats['total_bars_stored']
                self.checkpoint.total_files_created = self.stats['total_files_created']
                self.checkpoint.total_api_calls = self.stats['total_api_calls']
                self.checkpoint.rate_limit_delays = self.stats['rate_limit_delays']
                self.checkpoint.quality_scores[symbol] = symbol_stats['quality_score']
                
                self.stats['symbols_processed'] += 1
                
                if self.stats['symbols_completed'] > 0:
                    self.stats['average_quality_score'] = total_quality_score / self.stats['symbols_completed']
                
                await self.save_checkpoint(self.checkpoint)
                
                progress = (self.checkpoint.processed_symbols / self.checkpoint.total_symbols) * 100
                logger.info(f"Progress: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} "
                           f"({progress:.1f}%) - Current: {symbol}")
                
            except Exception as e:
                logger.error(f"Critical error processing {symbol}: {e}")
                self.checkpoint.symbols_failed.append(symbol)
                self.stats['symbols_failed'] += 1
        
        logger.info("Resume Polygon population complete")
        await self._print_final_stats()
    
    async def _print_final_stats(self):
        """Print comprehensive final statistics"""
        
        logger.info("=" * 80)
        logger.info("POLYGON 30-YEAR POPULATION FINAL STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"Symbols completed: {self.stats['symbols_completed']}")
        logger.info(f"Symbols failed: {self.stats['symbols_failed']}")
        logger.info(f"Total bars collected: {self.stats['total_bars_collected']:,}")
        logger.info(f"Total bars stored: {self.stats['total_bars_stored']:,}")
        logger.info(f"Total files created: {self.stats['total_files_created']}")
        logger.info(f"Total API calls: {self.stats['total_api_calls']:,}")
        logger.info(f"Rate limit delays: {self.stats['rate_limit_delays']}")
        logger.info(f"Average quality score: {self.stats['average_quality_score']:.3f}")
        
        if self.stats['errors']:
            logger.info(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                logger.info(f"  - {error}")
            if len(self.stats['errors']) > 5:
                logger.info(f"  ... and {len(self.stats['errors']) - 5} more errors")
        
        # Get storage stats
        try:
            storage_stats = await self.file_manager.get_storage_stats()
            logger.info(f"Storage statistics:")
            logger.info(f"  - Total files: {storage_stats.get('files', 0)}")
            logger.info(f"  - Total symbols: {storage_stats.get('symbols', 0)}")
            logger.info(f"  - Total size: {storage_stats.get('total_size_mb', 0):.2f} MB")
            
            if self.checkpoint:
                # Quality summary
                quality_scores = list(self.checkpoint.quality_scores.values())
                if quality_scores:
                    logger.info(f"Data quality summary:")
                    logger.info(f"  - Min quality: {min(quality_scores):.3f}")
                    logger.info(f"  - Max quality: {max(quality_scores):.3f}")
                    logger.info(f"  - Avg quality: {sum(quality_scores) / len(quality_scores):.3f}")
                    
        except Exception as e:
            logger.warning(f"Could not retrieve storage stats: {e}")
        
        logger.info("=" * 80)
    
    async def close(self):
        """Clean up resources"""
        try:
            if self.file_manager:
                await self.file_manager.close()
            logger.info("Resources cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(description="Polygon 30-Year Minute Bar Population")
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
    parser.add_argument('--storage-path', type=str, default='/mnt/d/ats-data',
                        help='Storage path for minute bar files')
    parser.add_argument('--checkpoint-file', type=str, default='polygon_30year_checkpoint.json',
                        help='Checkpoint file for resumable processing')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode (limited symbols)')
    parser.add_argument('--concurrent', type=int, default=2,
                        help='Max concurrent operations (be conservative with Polygon)')
    parser.add_argument('--premium', action='store_true',
                        help='Use premium plan rate limits (higher throughput)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting Polygon 30-year minute bar population")
    logger.info(f"Arguments: {vars(args)}")
    
    # Initialize populator
    populator = Polygon30YearPopulator(
        storage_path=args.storage_path,
        checkpoint_file=args.checkpoint_file,
        max_concurrent=args.concurrent,
        premium_plan=args.premium,
        debug=args.debug
    )
    
    try:
        await populator.initialize()
        
        if args.resume:
            # Resume from checkpoint
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
        
    except KeyboardInterrupt:
        logger.info("Population interrupted by user")
    except Exception as e:
        logger.error(f"Population failed: {e}")
    finally:
        await populator.close()
    
    logger.info("Polygon 30-year population script completed")

if __name__ == "__main__":
    asyncio.run(main())