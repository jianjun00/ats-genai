#!/usr/bin/env python3
"""
Multi-Vendor 30-Year Daily Price Backfill with Enhanced Checkpointing

Performs comprehensive 30-year daily price backfill for all three vendors:
- Polygon API
- Tiingo API  
- EODHD API

Features:
- Enhanced checkpoint system for resume capability
- Parallel vendor processing
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
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, NamedTuple
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
        logging.FileHandler(log_dir / 'multi_vendor_daily_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("multi_vendor_30year_daily_backfill")


class VendorStatus(Enum):
    """Status of vendor processing for an instrument."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class InstrumentProgress:
    """Progress tracking for a single instrument across all vendors."""
    instrument_id: int
    symbol: str
    vendors: Dict[str, VendorStatus] = field(default_factory=lambda: {
        'polygon': VendorStatus.PENDING,
        'tiingo': VendorStatus.PENDING,
        'eodhd': VendorStatus.PENDING
    })
    records_inserted: Dict[str, int] = field(default_factory=lambda: {
        'polygon': 0,
        'tiingo': 0,
        'eodhd': 0
    })
    errors: Dict[str, str] = field(default_factory=dict)
    last_attempt: Dict[str, Optional[datetime]] = field(default_factory=lambda: {
        'polygon': None,
        'tiingo': None,
        'eodhd': None
    })
    
    def is_complete(self) -> bool:
        """Check if all vendors are complete for this instrument."""
        return all(status == VendorStatus.COMPLETED for status in self.vendors.values())
    
    def get_total_records(self) -> int:
        """Get total records inserted across all vendors."""
        return sum(self.records_inserted.values())


@dataclass
class BackfillCheckpoint:
    """Enhanced checkpoint data for multi-vendor backfill."""
    start_date: datetime
    end_date: datetime
    total_instruments: int
    processed_instruments: int = 0
    completed_instruments: int = 0
    failed_instruments: int = 0
    instruments_progress: Dict[str, InstrumentProgress] = field(default_factory=dict)
    
    # Global statistics
    total_records_polygon: int = 0
    total_records_tiingo: int = 0
    total_records_eodhd: int = 0
    
    # Error tracking
    vendor_errors: Dict[str, int] = field(default_factory=lambda: {
        'polygon': 0,
        'tiingo': 0,
        'eodhd': 0
    })
    
    # Timing
    checkpoint_created: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert checkpoint to dictionary for JSON serialization."""
        return {
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'total_instruments': self.total_instruments,
            'processed_instruments': self.processed_instruments,
            'completed_instruments': self.completed_instruments,
            'failed_instruments': self.failed_instruments,
            'total_records_polygon': self.total_records_polygon,
            'total_records_tiingo': self.total_records_tiingo,
            'total_records_eodhd': self.total_records_eodhd,
            'vendor_errors': self.vendor_errors,
            'checkpoint_created': self.checkpoint_created.isoformat(),
            'last_updated': self.last_updated.isoformat(),
            'instruments_progress': {
                symbol: {
                    'instrument_id': progress.instrument_id,
                    'symbol': progress.symbol,
                    'vendors': {k: v.value for k, v in progress.vendors.items()},
                    'records_inserted': progress.records_inserted,
                    'errors': progress.errors,
                    'last_attempt': {k: v.isoformat() if v else None for k, v in progress.last_attempt.items()}
                }
                for symbol, progress in self.instruments_progress.items()
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BackfillCheckpoint':
        """Create checkpoint from dictionary."""
        checkpoint = cls(
            start_date=datetime.fromisoformat(data['start_date']),
            end_date=datetime.fromisoformat(data['end_date']),
            total_instruments=data['total_instruments'],
            processed_instruments=data.get('processed_instruments', 0),
            completed_instruments=data.get('completed_instruments', 0),
            failed_instruments=data.get('failed_instruments', 0),
            total_records_polygon=data.get('total_records_polygon', 0),
            total_records_tiingo=data.get('total_records_tiingo', 0),
            total_records_eodhd=data.get('total_records_eodhd', 0),
            vendor_errors=data.get('vendor_errors', {'polygon': 0, 'tiingo': 0, 'eodhd': 0}),
            checkpoint_created=datetime.fromisoformat(data['checkpoint_created']),
            last_updated=datetime.fromisoformat(data['last_updated'])
        )
        
        # Restore instruments progress
        for symbol, progress_data in data.get('instruments_progress', {}).items():
            progress = InstrumentProgress(
                instrument_id=progress_data['instrument_id'],
                symbol=progress_data['symbol'],
                vendors={k: VendorStatus(v) for k, v in progress_data['vendors'].items()},
                records_inserted=progress_data['records_inserted'],
                errors=progress_data['errors']
            )
            # Restore last_attempt dates
            for vendor, attempt_str in progress_data.get('last_attempt', {}).items():
                progress.last_attempt[vendor] = datetime.fromisoformat(attempt_str) if attempt_str else None
            
            checkpoint.instruments_progress[symbol] = progress
        
        return checkpoint


class MultiVendorDailyBackfiller:
    """
    Multi-vendor daily price backfiller with enhanced checkpoint system.
    """
    
    def __init__(self, polygon_api_key: str = None, tiingo_api_key: str = None, 
                 eodhd_api_key: str = None, checkpoint_file: str = None):
        self.polygon_api_key = polygon_api_key
        self.tiingo_api_key = tiingo_api_key
        self.eodhd_api_key = eodhd_api_key
        self.checkpoint_file = checkpoint_file or "/data/checkpoints/multi_vendor_daily_backfill_30year.json"
        
        # Database connection
        self.db_pool = None
        
        # Checkpoint data
        self.checkpoint: Optional[BackfillCheckpoint] = None
        
        # Rate limiting delays (seconds)
        self.rate_limits = {
            'polygon': 12.0,  # 12 second delay between calls
            'tiingo': 1.0,    # 1 second delay for paid plans
            'eodhd': 3.0      # 3 second delay (20 calls/minute)
        }
        
        logger.info("Multi-vendor daily backfiller initialized")
        logger.info(f"Checkpoint file: {self.checkpoint_file}")
    
    async def setup_database_connection(self):
        """Setup async database connection pool."""
        try:
            # Use Docker-compatible database connection
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
            
            logger.info(f"Retrieved {len(instruments)} US instruments for backfill")
            return instruments
            
        except Exception as e:
            logger.error(f"Failed to get US instruments: {e}")
            raise
    
    def load_checkpoint(self) -> Optional[BackfillCheckpoint]:
        """Load checkpoint from file if exists."""
        try:
            if Path(self.checkpoint_file).exists():
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                checkpoint = BackfillCheckpoint.from_dict(data)
                logger.info(f"Loaded checkpoint: {checkpoint.processed_instruments}/{checkpoint.total_instruments} instruments processed")
                return checkpoint
            return None
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def save_checkpoint(self, checkpoint: BackfillCheckpoint):
        """Save checkpoint to file."""
        try:
            # Create checkpoint directory if needed
            Path(self.checkpoint_file).parent.mkdir(parents=True, exist_ok=True)
            
            # Update timestamp
            checkpoint.last_updated = datetime.now()
            
            # Save to temporary file first, then atomic rename
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(checkpoint.to_dict(), f, indent=2)
            
            Path(temp_file).rename(self.checkpoint_file)
            logger.debug(f"Checkpoint saved: {checkpoint.processed_instruments}/{checkpoint.total_instruments}")
            
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    async def fetch_polygon_daily_prices(self, symbol: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch daily prices from Polygon API."""
        if not self.polygon_api_key:
            return []
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apikey': self.polygon_api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data and data['results']:
                    results = []
                    for result in data['results']:
                        results.append({
                            'date': datetime.utcfromtimestamp(result['t']/1000).date(),
                            'open': result['o'],
                            'high': result['h'],
                            'low': result['l'],
                            'close': result['c'],
                            'volume': result['v']
                        })
                    return results
            elif response.status_code == 403:
                logger.warning(f"Polygon API rate limit for {symbol}")
            else:
                logger.error(f"Polygon API error for {symbol}: {response.status_code}")
            
        except Exception as e:
            logger.error(f"Error fetching Polygon data for {symbol}: {e}")
        
        return []
    
    async def fetch_tiingo_daily_prices(self, symbol: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch daily prices from Tiingo API."""
        if not self.tiingo_api_key:
            return []
        
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json',
            'token': self.tiingo_api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                results = []
                for item in data:
                    results.append({
                        'date': datetime.strptime(item['date'][:10], "%Y-%m-%d").date(),
                        'open': item.get('open'),
                        'high': item.get('high'),
                        'low': item.get('low'),
                        'close': item.get('close'),
                        'volume': item.get('volume')
                    })
                return results
            else:
                logger.error(f"Tiingo API error for {symbol}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error fetching Tiingo data for {symbol}: {e}")
        
        return []
    
    async def fetch_eodhd_daily_prices(self, symbol: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch daily prices from EODHD API."""
        if not self.eodhd_api_key:
            return []
        
        url = f"https://eodhistoricaldata.com/api/eod/{symbol}.US"
        params = {
            'api_token': self.eodhd_api_key,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'period': 'd',
            'fmt': 'json'
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    results = []
                    for item in data:
                        results.append({
                            'date': datetime.strptime(item['date'], "%Y-%m-%d").date(),
                            'open': item.get('open'),
                            'high': item.get('high'),
                            'low': item.get('low'),
                            'close': item.get('close'),
                            'volume': item.get('volume', 0),
                            'adjusted_close': item.get('adjusted_close', item.get('close'))
                        })
                    return results
            else:
                logger.error(f"EODHD API error for {symbol}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error fetching EODHD data for {symbol}: {e}")
        
        return []
    
    async def insert_vendor_prices(self, vendor: str, symbol: str, instrument_id: int, prices: List[Dict[str, Any]]) -> int:
        """Insert daily prices for a specific vendor with idempotent operations."""
        if not prices:
            return 0
        
        table_name = f"dev_daily_prices_{vendor}"
        
        try:
            async with self.db_pool.acquire() as conn:
                rows = []
                for price in prices:
                    try:
                        if vendor == 'eodhd':
                            rows.append((
                                price['date'],
                                symbol,
                                price.get('open'),
                                price.get('high'),
                                price.get('low'),
                                price.get('close'),
                                price.get('volume', 0),
                                price.get('adjusted_close'),
                                instrument_id
                            ))
                        else:
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
                if vendor == 'eodhd':
                    await conn.executemany(f"""
                        INSERT INTO {table_name} 
                        (date, symbol, open, high, low, close, volume, adjusted_close, instrument_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (date, instrument_id) DO UPDATE SET
                            symbol = EXCLUDED.symbol,
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            adjusted_close = EXCLUDED.adjusted_close
                    """, rows)
                else:
                    await conn.executemany(f"""
                        INSERT INTO {table_name} 
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
                
                logger.info(f"💾 Inserted {len(rows)} {vendor} records for {symbol}")
                return len(rows)
                
        except Exception as e:
            logger.error(f"❌ Database error inserting {vendor} prices for {symbol}: {e}")
            return 0
    
    async def process_instrument_vendor(self, instrument: Dict[str, Any], vendor: str, 
                                      start_date: date, end_date: date, progress: InstrumentProgress) -> int:
        """Process a single instrument for a specific vendor."""
        symbol = instrument['symbol']
        instrument_id = instrument['id']
        
        try:
            progress.vendors[vendor] = VendorStatus.IN_PROGRESS
            progress.last_attempt[vendor] = datetime.now()
            
            # Fetch data based on vendor
            if vendor == 'polygon':
                prices = await self.fetch_polygon_daily_prices(symbol, start_date, end_date)
                time.sleep(self.rate_limits['polygon'])
            elif vendor == 'tiingo':
                prices = await self.fetch_tiingo_daily_prices(symbol, start_date, end_date)
                time.sleep(self.rate_limits['tiingo'])
            elif vendor == 'eodhd':
                prices = await self.fetch_eodhd_daily_prices(symbol, start_date, end_date)
                time.sleep(self.rate_limits['eodhd'])
            else:
                raise ValueError(f"Unknown vendor: {vendor}")
            
            if not prices:
                progress.vendors[vendor] = VendorStatus.SKIPPED
                logger.warning(f"⚠️ No {vendor} data for {symbol}")
                return 0
            
            # Insert prices
            inserted_count = await self.insert_vendor_prices(vendor, symbol, instrument_id, prices)
            progress.records_inserted[vendor] = inserted_count
            progress.vendors[vendor] = VendorStatus.COMPLETED
            
            return inserted_count
            
        except Exception as e:
            error_msg = f"Failed to process {vendor} data for {symbol}: {e}"
            logger.error(f"❌ {error_msg}")
            progress.vendors[vendor] = VendorStatus.FAILED
            progress.errors[vendor] = error_msg
            return 0
    
    async def process_instrument(self, instrument: Dict[str, Any], start_date: date, end_date: date) -> InstrumentProgress:
        """Process a single instrument across all vendors."""
        symbol = instrument['symbol']
        instrument_id = instrument['id']
        
        # Check if we should resume from checkpoint
        if self.checkpoint and symbol in self.checkpoint.instruments_progress:
            progress = self.checkpoint.instruments_progress[symbol]
            logger.info(f"📋 Resuming {symbol} from checkpoint")
        else:
            progress = InstrumentProgress(instrument_id=instrument_id, symbol=symbol)
        
        logger.info(f"📈 Processing {symbol} (ID: {instrument_id}) across all vendors...")
        
        # Process each vendor that hasn't completed yet
        tasks = []
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            if progress.vendors[vendor] not in [VendorStatus.COMPLETED, VendorStatus.SKIPPED]:
                # Process vendor synchronously to respect rate limits
                await self.process_instrument_vendor(instrument, vendor, start_date, end_date, progress)
        
        # Update checkpoint progress
        self.checkpoint.instruments_progress[symbol] = progress
        
        if progress.is_complete():
            self.checkpoint.completed_instruments += 1
            logger.info(f"✅ Completed {symbol}: {progress.get_total_records()} total records")
        else:
            self.checkpoint.failed_instruments += 1
            logger.warning(f"⚠️ Partially completed {symbol}: {progress.get_total_records()} total records")
        
        return progress
    
    async def run_backfill(self, start_date: date, end_date: date, limit: int = None) -> Dict[str, Any]:
        """Execute the complete 30-year multi-vendor backfill."""
        logger.info("🚀 Starting multi-vendor 30-year daily price backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        try:
            # Setup database connection
            await self.setup_database_connection()
            
            # Get instruments to process
            instruments = await self.get_us_instruments(limit=limit)
            
            # Load or create checkpoint
            self.checkpoint = self.load_checkpoint()
            if not self.checkpoint:
                self.checkpoint = BackfillCheckpoint(
                    start_date=datetime.combine(start_date, datetime.min.time()),
                    end_date=datetime.combine(end_date, datetime.min.time()),
                    total_instruments=len(instruments)
                )
                logger.info(f"📊 Starting fresh backfill for {len(instruments)} instruments")
            else:
                logger.info(f"📊 Resuming backfill: {self.checkpoint.processed_instruments}/{self.checkpoint.total_instruments} processed")
            
            # Process each instrument
            for i, instrument in enumerate(instruments):
                symbol = instrument['symbol']
                
                # Skip if already completed
                if symbol in self.checkpoint.instruments_progress:
                    existing_progress = self.checkpoint.instruments_progress[symbol]
                    if existing_progress.is_complete():
                        logger.info(f"⏭️ Skipping {symbol} - already completed")
                        continue
                
                try:
                    await self.process_instrument(instrument, start_date, end_date)
                    self.checkpoint.processed_instruments += 1
                    
                    # Update global statistics
                    progress = self.checkpoint.instruments_progress[symbol]
                    self.checkpoint.total_records_polygon += progress.records_inserted['polygon']
                    self.checkpoint.total_records_tiingo += progress.records_inserted['tiingo']
                    self.checkpoint.total_records_eodhd += progress.records_inserted['eodhd']
                    
                    # Save checkpoint every 10 instruments
                    if i % 10 == 0:
                        self.save_checkpoint(self.checkpoint)
                    
                    # Progress logging
                    if i % 50 == 0 or i == len(instruments) - 1:
                        progress_pct = (i / len(instruments)) * 100
                        total_records = (self.checkpoint.total_records_polygon + 
                                       self.checkpoint.total_records_tiingo + 
                                       self.checkpoint.total_records_eodhd)
                        logger.info(f"📊 Progress: {i+1:,}/{len(instruments):,} ({progress_pct:.1f}%) - "
                                  f"{total_records:,} total records")
                    
                except Exception as e:
                    logger.error(f"❌ Critical error processing {symbol}: {e}")
                    continue
            
            # Final checkpoint save
            self.save_checkpoint(self.checkpoint)
            
            # Return final statistics
            return {
                'total_instruments': self.checkpoint.total_instruments,
                'processed_instruments': self.checkpoint.processed_instruments,
                'completed_instruments': self.checkpoint.completed_instruments,
                'failed_instruments': self.checkpoint.failed_instruments,
                'total_records_polygon': self.checkpoint.total_records_polygon,
                'total_records_tiingo': self.checkpoint.total_records_tiingo,
                'total_records_eodhd': self.checkpoint.total_records_eodhd,
                'vendor_errors': self.checkpoint.vendor_errors
            }
            
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            raise
        
        finally:
            if self.db_pool:
                await self.db_pool.close()
    
    def log_final_summary(self, results: Dict[str, Any]):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 MULTI-VENDOR 30-YEAR DAILY PRICE BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Instruments: {results['total_instruments']:,}")
        logger.info(f"  Completed Instruments: {results['completed_instruments']:,}")
        logger.info(f"  Failed Instruments: {results['failed_instruments']:,}")
        logger.info("")
        logger.info(f"📈 RECORDS BY VENDOR:")
        logger.info(f"  Polygon: {results['total_records_polygon']:,}")
        logger.info(f"  Tiingo: {results['total_records_tiingo']:,}")
        logger.info(f"  EODHD: {results['total_records_eodhd']:,}")
        logger.info("")
        total_records = (results['total_records_polygon'] + 
                        results['total_records_tiingo'] + 
                        results['total_records_eodhd'])
        logger.info(f"🏆 TOTAL RECORDS: {total_records:,}")
        
        if results['total_instruments'] > 0:
            success_rate = (results['completed_instruments'] / results['total_instruments']) * 100
            avg_records = total_records / results['total_instruments']
            logger.info(f"✅ Success Rate: {success_rate:.1f}%")
            logger.info(f"📊 Average Records per Instrument: {avg_records:.0f}")
        
        logger.info("=" * 80)


async def main():
    """Main entry point for multi-vendor daily price backfill."""
    
    parser = argparse.ArgumentParser(
        description="Multi-Vendor 30-Year Daily Price Backfill with Enhanced Checkpointing"
    )
    
    parser.add_argument(
        "--polygon-api-key",
        default=os.getenv("POLYGON_API_KEY"),
        help="Polygon API key (default: from POLYGON_API_KEY env var)"
    )
    
    parser.add_argument(
        "--tiingo-api-key",
        default=os.getenv("TIINGO_API_KEY"),
        help="Tiingo API key (default: from TIINGO_API_KEY env var)"
    )
    
    parser.add_argument(
        "--eodhd-api-key",
        default=os.getenv("EODHD_API_KEY"),
        help="EODHD API key (default: from EODHD_API_KEY env var)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of instruments for testing (default: all US instruments)"
    )
    
    parser.add_argument(
        "--checkpoint-file",
        default="/data/checkpoints/multi_vendor_daily_backfill_30year.json",
        help="Checkpoint file path for resume capability"
    )
    
    parser.add_argument(
        "--years",
        type=int,
        default=30,
        help="Number of years to backfill (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Calculate 30-year date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * args.years)
    
    # Create checkpoint directory
    checkpoint_dir = Path(args.checkpoint_file).parent
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Initialize backfiller
        backfiller = MultiVendorDailyBackfiller(
            polygon_api_key=args.polygon_api_key,
            tiingo_api_key=args.tiingo_api_key,
            eodhd_api_key=args.eodhd_api_key,
            checkpoint_file=args.checkpoint_file
        )
        
        results = await backfiller.run_backfill(
            start_date=start_date,
            end_date=end_date,
            limit=args.limit
        )
        
        backfiller.log_final_summary(results)
        logger.info("Multi-vendor 30-year daily price backfill completed successfully!")
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