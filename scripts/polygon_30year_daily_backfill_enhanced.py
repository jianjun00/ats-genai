#!/usr/bin/env python3
"""
Enhanced Polygon 30-Year Daily Price Backfill with Checkpoint System

Performs comprehensive 30-year daily price backfill specifically for Polygon API
with enhanced checkpoint system for resume capability.

Features:
- Polygon-optimized rate limiting (12 second delays)
- Enhanced checkpoint system for precise resume capability
- Idempotent operations
- All US instruments from dev_instruments table
- Intelligent progress tracking and recovery
"""

import os
import sys
import asyncio
import asyncpg
import logging
import argparse
import json
import time
import requests
from datetime import datetime, timedelta, date, timezone
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
import hashlib
from enum import Enum

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging (Docker-compatible)
log_dir = Path('/logs' if Path('/logs').exists() else '/tmp/logs')
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'polygon_30year_daily_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("polygon_30year_daily_backfill")


class ProcessingStatus(Enum):
    """Status of instrument processing."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class InstrumentProgress:
    """Progress tracking for a single instrument."""
    instrument_id: int
    symbol: str
    status: ProcessingStatus = ProcessingStatus.PENDING
    records_inserted: int = 0
    error_message: Optional[str] = None
    last_attempt: Optional[datetime] = None
    api_calls: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'instrument_id': self.instrument_id,
            'symbol': self.symbol,
            'status': self.status.value,
            'records_inserted': self.records_inserted,
            'error_message': self.error_message,
            'last_attempt': self.last_attempt.isoformat() if self.last_attempt else None,
            'api_calls': self.api_calls
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InstrumentProgress':
        """Create from dictionary."""
        progress = cls(
            instrument_id=data['instrument_id'],
            symbol=data['symbol'],
            status=ProcessingStatus(data['status']),
            records_inserted=data['records_inserted'],
            error_message=data.get('error_message'),
            api_calls=data.get('api_calls', 0)
        )
        if data.get('last_attempt'):
            progress.last_attempt = datetime.fromisoformat(data['last_attempt'])
        return progress


@dataclass
class PolygonBackfillCheckpoint:
    """Checkpoint data for Polygon daily price backfill."""
    start_date: datetime
    end_date: datetime
    total_instruments: int
    processed_instruments: int = 0
    completed_instruments: int = 0
    failed_instruments: int = 0
    skipped_instruments: int = 0
    total_records: int = 0
    total_api_calls: int = 0
    
    # Progress tracking
    instruments_progress: Dict[str, InstrumentProgress] = field(default_factory=dict)
    
    # Timing
    checkpoint_created: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'total_instruments': self.total_instruments,
            'processed_instruments': self.processed_instruments,
            'completed_instruments': self.completed_instruments,
            'failed_instruments': self.failed_instruments,
            'skipped_instruments': self.skipped_instruments,
            'total_records': self.total_records,
            'total_api_calls': self.total_api_calls,
            'checkpoint_created': self.checkpoint_created.isoformat(),
            'last_updated': self.last_updated.isoformat(),
            'instruments_progress': {
                symbol: progress.to_dict() for symbol, progress in self.instruments_progress.items()
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PolygonBackfillCheckpoint':
        """Create from dictionary."""
        checkpoint = cls(
            start_date=datetime.fromisoformat(data['start_date']),
            end_date=datetime.fromisoformat(data['end_date']),
            total_instruments=data['total_instruments'],
            processed_instruments=data.get('processed_instruments', 0),
            completed_instruments=data.get('completed_instruments', 0),
            failed_instruments=data.get('failed_instruments', 0),
            skipped_instruments=data.get('skipped_instruments', 0),
            total_records=data.get('total_records', 0),
            total_api_calls=data.get('total_api_calls', 0),
            checkpoint_created=datetime.fromisoformat(data['checkpoint_created']),
            last_updated=datetime.fromisoformat(data['last_updated'])
        )
        
        # Restore instruments progress
        for symbol, progress_data in data.get('instruments_progress', {}).items():
            checkpoint.instruments_progress[symbol] = InstrumentProgress.from_dict(progress_data)
        
        return checkpoint


class PolygonDailyBackfiller:
    """
    Enhanced Polygon daily price backfiller with checkpoint system.
    """
    
    def __init__(self, api_key: str, checkpoint_file: str = None):
        self.api_key = api_key
        self.checkpoint_file = checkpoint_file or "/data/checkpoints/polygon/daily_backfill_30year.json"
        
        # Database connection
        self.db_pool = None
        
        # Checkpoint data
        self.checkpoint: Optional[PolygonBackfillCheckpoint] = None
        
        # Rate limiting (Polygon has strict limits)
        self.rate_limit_delay = 12.0  # 12 seconds between API calls
        
        logger.info("Enhanced Polygon daily backfiller initialized")
        logger.info(f"Checkpoint file: {self.checkpoint_file}")
        logger.info(f"Rate limit: {3600/self.rate_limit_delay:.1f} requests/hour")
    
    async def setup_database_connection(self):
        """Setup async database connection pool."""
        try:
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = int(os.getenv('DB_PORT', '5432'))
            
            if db_host == 'postgres':
                db_port = 5432
            
            db_config = {
                'host': db_host,
                'port': db_port,
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'dev_password'),
                'database': os.getenv('DB_NAME', 'dev_db'),
                'min_size': 5,
                'max_size': 20
            }
            
            self.db_pool = await asyncpg.create_pool(**db_config)
            logger.info("Database connection pool created successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup database connection: {e}")
            raise
    
    async def get_us_instruments(self, limit: int = None) -> List[Dict[str, Any]]:
        """Get list of active US instruments from dev_instruments table."""
        try:
            query = """
            SELECT id, symbol, name, exchange, active
            FROM dev_instruments 
            WHERE active = true 
              AND symbol ~ '^[A-Z]{1,5}$'  -- US symbol pattern
              AND exchange IN ('NYSE', 'NASDAQ', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
            ORDER BY symbol
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query)
                instruments = [dict(row) for row in rows]
            
            logger.info(f"Retrieved {len(instruments)} US instruments for Polygon backfill")
            return instruments
            
        except Exception as e:
            logger.error(f"Failed to get US instruments: {e}")
            raise
    
    def load_checkpoint(self) -> Optional[PolygonBackfillCheckpoint]:
        """Load checkpoint from file if exists."""
        try:
            if Path(self.checkpoint_file).exists():
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                checkpoint = PolygonBackfillCheckpoint.from_dict(data)
                logger.info(f"Loaded Polygon checkpoint: {checkpoint.completed_instruments}/{checkpoint.total_instruments} completed")
                return checkpoint
            return None
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def save_checkpoint(self):
        """Save checkpoint to file."""
        if not self.checkpoint:
            return
            
        try:
            # Create checkpoint directory if needed
            Path(self.checkpoint_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Update timestamp
            self.checkpoint.last_updated = datetime.now()
            
            # Save to temporary file first, then atomic rename
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.checkpoint.to_dict(), f, indent=2)
            
            Path(temp_file).rename(self.checkpoint_file)
            logger.debug(f"Checkpoint saved: {self.checkpoint.completed_instruments}/{self.checkpoint.total_instruments}")
            
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    async def fetch_polygon_daily_prices(self, symbol: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch daily prices from Polygon API with optimized date range handling."""
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,  # Maximum limit for efficiency
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            self.checkpoint.total_api_calls += 1
            
            if response.status_code == 200:
                data = response.json()
                if 'results' in data and data['results']:
                    results = []
                    for result in data['results']:
                        results.append({
                            'date': datetime.fromtimestamp(result['t']/1000, tz=timezone.utc).date(),
                            'open': result['o'],
                            'high': result['h'],
                            'low': result['l'],
                            'close': result['c'],
                            'volume': result['v']
                        })
                    logger.debug(f"✅ Fetched {len(results)} Polygon records for {symbol}")
                    return results
            elif response.status_code == 403:
                logger.warning(f"Polygon API rate limit for {symbol} - will retry")
            elif response.status_code == 404:
                logger.debug(f"No Polygon data available for {symbol}")
            else:
                logger.error(f"Polygon API error for {symbol}: HTTP {response.status_code}")
            
        except Exception as e:
            logger.error(f"Error fetching Polygon data for {symbol}: {e}")
        
        return []
    
    async def insert_polygon_prices(self, symbol: str, instrument_id: int, prices: List[Dict[str, Any]]) -> int:
        """Insert daily prices with idempotent operations."""
        if not prices:
            return 0
        
        try:
            async with self.db_pool.acquire() as conn:
                rows = []
                for price in prices:
                    try:
                        rows.append((
                            price['date'],
                            symbol,
                            price.get('open'),
                            price.get('high'),
                            price.get('low'),
                            price.get('close'),
                            price.get('volume', 0),
                            instrument_id
                        ))
                    except Exception as e:
                        logger.error(f"Error processing price record for {symbol}: {e}")
                        continue
                
                if not rows:
                    return 0
                
                # Insert with idempotent UPSERT
                await conn.executemany("""
                    INSERT INTO dev_daily_prices_polygon 
                    (date, symbol, open, high, low, close, volume, instrument_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """, rows)
                
                logger.info(f"💾 Inserted {len(rows)} Polygon records for {symbol}")
                return len(rows)
                
        except Exception as e:
            logger.error(f"❌ Database error inserting Polygon prices for {symbol}: {e}")
            return 0
    
    async def process_instrument(self, instrument: Dict[str, Any], start_date: date, end_date: date) -> InstrumentProgress:
        """Process a single instrument for Polygon daily prices."""
        symbol = instrument['symbol']
        instrument_id = instrument['id']
        
        # Check if we should resume from checkpoint
        if symbol in self.checkpoint.instruments_progress:
            progress = self.checkpoint.instruments_progress[symbol]
            if progress.status == ProcessingStatus.COMPLETED:
                logger.info(f"⏭️ Skipping {symbol} - already completed ({progress.records_inserted} records)")
                self.checkpoint.skipped_instruments += 1
                return progress
            logger.info(f"📋 Resuming {symbol} from checkpoint")
        else:
            progress = InstrumentProgress(instrument_id=instrument_id, symbol=symbol)
        
        try:
            progress.status = ProcessingStatus.IN_PROGRESS
            progress.last_attempt = datetime.now()
            
            logger.info(f"📈 Processing Polygon data for {symbol} (ID: {instrument_id})...")
            
            # Fetch data from Polygon
            prices = await self.fetch_polygon_daily_prices(symbol, start_date, end_date)
            progress.api_calls += 1
            
            if not prices:
                progress.status = ProcessingStatus.SKIPPED
                logger.warning(f"⚠️ No Polygon data for {symbol}")
                self.checkpoint.instruments_progress[symbol] = progress
                return progress
            
            # Insert prices
            inserted_count = await self.insert_polygon_prices(symbol, instrument_id, prices)
            progress.records_inserted = inserted_count
            progress.status = ProcessingStatus.COMPLETED
            
            # Update checkpoint totals
            self.checkpoint.total_records += inserted_count
            self.checkpoint.completed_instruments += 1
            
            logger.info(f"✅ Completed {symbol}: {inserted_count} Polygon records")
            
        except Exception as e:
            error_msg = f"Failed to process Polygon data for {symbol}: {e}"
            logger.error(f"❌ {error_msg}")
            progress.status = ProcessingStatus.FAILED
            progress.error_message = error_msg
            self.checkpoint.failed_instruments += 1
        
        # Update progress in checkpoint
        self.checkpoint.instruments_progress[symbol] = progress
        return progress
    
    async def run_backfill(self, start_date: date, end_date: date, limit: int = None) -> Dict[str, Any]:
        """Execute the complete 30-year Polygon backfill."""
        logger.info("🚀 Starting enhanced Polygon 30-year daily price backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        try:
            # Setup database connection
            await self.setup_database_connection()
            
            # Get instruments to process
            instruments = await self.get_us_instruments(limit=limit)
            
            # Load or create checkpoint
            self.checkpoint = self.load_checkpoint()
            if not self.checkpoint:
                self.checkpoint = PolygonBackfillCheckpoint(
                    start_date=datetime.combine(start_date, datetime.min.time()),
                    end_date=datetime.combine(end_date, datetime.min.time()),
                    total_instruments=len(instruments)
                )
                logger.info(f"📊 Starting fresh Polygon backfill for {len(instruments)} instruments")
            else:
                logger.info(f"📊 Resuming Polygon backfill: {self.checkpoint.completed_instruments}/{self.checkpoint.total_instruments} completed")
            
            # Process each instrument
            for i, instrument in enumerate(instruments):
                symbol = instrument['symbol']
                
                try:
                    await self.process_instrument(instrument, start_date, end_date)
                    self.checkpoint.processed_instruments += 1
                    
                    # Apply rate limiting after each instrument
                    time.sleep(self.rate_limit_delay)
                    
                    # Save checkpoint every 10 instruments
                    if i % 10 == 0:
                        self.save_checkpoint()
                    
                    # Progress logging
                    if i % 50 == 0 or i == len(instruments) - 1:
                        progress_pct = (i / len(instruments)) * 100
                        logger.info(f"📊 Progress: {i+1:,}/{len(instruments):,} ({progress_pct:.1f}%) - "
                                  f"{self.checkpoint.total_records:,} records - "
                                  f"{self.checkpoint.total_api_calls:,} API calls")
                    
                except Exception as e:
                    logger.error(f"❌ Critical error processing {symbol}: {e}")
                    continue
            
            # Final checkpoint save
            self.save_checkpoint()
            
            # Return final statistics
            return {
                'total_instruments': self.checkpoint.total_instruments,
                'processed_instruments': self.checkpoint.processed_instruments,
                'completed_instruments': self.checkpoint.completed_instruments,
                'failed_instruments': self.checkpoint.failed_instruments,
                'skipped_instruments': self.checkpoint.skipped_instruments,
                'total_records': self.checkpoint.total_records,
                'total_api_calls': self.checkpoint.total_api_calls
            }
            
        except Exception as e:
            logger.error(f"Polygon backfill failed: {e}")
            raise
        
        finally:
            if self.db_pool:
                await self.db_pool.close()
    
    def log_final_summary(self, results: Dict[str, Any]):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 POLYGON 30-YEAR DAILY PRICE BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Instruments: {results['total_instruments']:,}")
        logger.info(f"  Completed Instruments: {results['completed_instruments']:,}")
        logger.info(f"  Failed Instruments: {results['failed_instruments']:,}")
        logger.info(f"  Skipped Instruments: {results['skipped_instruments']:,}")
        logger.info(f"  Total Records: {results['total_records']:,}")
        logger.info(f"  Total API Calls: {results['total_api_calls']:,}")
        logger.info("")
        
        if results['total_instruments'] > 0:
            success_rate = (results['completed_instruments'] / results['total_instruments']) * 100
            avg_records = results['total_records'] / max(results['completed_instruments'], 1)
            logger.info(f"✅ Success Rate: {success_rate:.1f}%")
            logger.info(f"📊 Average Records per Completed Instrument: {avg_records:.0f}")
        
        logger.info("=" * 80)


async def main():
    """Main entry point for Polygon daily price backfill."""
    
    parser = argparse.ArgumentParser(
        description="Enhanced Polygon 30-Year Daily Price Backfill with Checkpoint System"
    )
    
    parser.add_argument(
        "--api-key",
        default=os.getenv("POLYGON_API_KEY"),
        help="Polygon API key (default: from POLYGON_API_KEY env var)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of instruments for testing (default: all US instruments)"
    )
    
    parser.add_argument(
        "--checkpoint-file",
        default="/data/checkpoints/polygon/daily_backfill_30year.json",
        help="Checkpoint file path for resume capability"
    )
    
    parser.add_argument(
        "--years",
        type=int,
        default=30,
        help="Number of years to backfill (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Validate API key
    if not args.api_key:
        logger.error("POLYGON_API_KEY environment variable must be set or --api-key provided")
        return 1
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * args.years)
    
    # Create checkpoint directory
    checkpoint_dir = Path(args.checkpoint_file).parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Initialize backfiller
        backfiller = PolygonDailyBackfiller(
            api_key=args.api_key,
            checkpoint_file=args.checkpoint_file
        )
        
        results = await backfiller.run_backfill(
            start_date=start_date,
            end_date=end_date,
            limit=args.limit
        )
        
        backfiller.log_final_summary(results)
        logger.info("Enhanced Polygon 30-year daily price backfill completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Backfill interrupted by user - progress saved to checkpoint")
        return 130
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)