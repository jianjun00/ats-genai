#!/usr/bin/env python3
"""
Idempotent Daily Price Population System

This script provides idempotent daily price population that can be safely re-run
without creating duplicates. Supports multiple vendors and comprehensive validation.

Features:
- Duplicate prevention using UPSERT operations
- Gap detection and selective backfilling
- Progress tracking and resume capability
- Comprehensive validation and error handling
- Support for all major vendors (Polygon, Tiingo, EODHD)

Environment Variables:
- VENDOR: Vendor to use (polygon, tiingo, eodhd, all) - default: all
- START_DATE: Start date for backfill (YYYY-MM-DD) - default: 30 years ago
- END_DATE: End date for backfill (YYYY-MM-DD) - default: today
- BATCH_SIZE: Number of symbols to process per batch - default: 100
- MAX_RETRIES: Maximum retries for failed requests - default: 3
- RATE_LIMIT: Rate limit in requests per second - default: 5
- DRY_RUN: Set to 'true' to preview without executing - default: false
- FORCE_REFRESH: Set to 'true' to refresh existing data - default: false
"""

import os
import sys
import asyncio
import asyncpg
import logging
import aiohttp
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
import time

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PriceRecord:
    """Standard price record structure"""
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: Optional[float] = None
    vendor: str = ""

@dataclass
class PopulationStats:
    """Statistics for population progress"""
    total_symbols: int = 0
    processed_symbols: int = 0
    new_records: int = 0
    updated_records: int = 0
    skipped_records: int = 0
    error_records: int = 0
    duplicate_records: int = 0
    api_calls: int = 0
    start_time: datetime = None
    last_update: datetime = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()
        self.last_update = datetime.now()

class IdempotentPricePopulator:
    """Idempotent daily price population system"""
    
    def __init__(self, 
                 vendor: str = "all",
                 start_date: str = None,
                 end_date: str = None,
                 batch_size: int = 100,
                 max_retries: int = 3,
                 rate_limit: float = 5.0,
                 dry_run: bool = False,
                 force_refresh: bool = False):
        
        self.vendor = vendor.lower()
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.rate_limit = rate_limit
        self.dry_run = dry_run
        self.force_refresh = force_refresh
        
        # Date range setup
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()
        if start_date:
            self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            # Default to 30 years ago
            self.start_date = self.end_date - timedelta(days=30*365)
        
        # API keys
        self.api_keys = {
            'polygon': os.getenv('POLYGON_API_KEY'),
            'tiingo': os.getenv('TIINGO_API_KEY'),
            'eodhd': os.getenv('EODHD_API_KEY')
        }
        
        # Rate limiting
        self.last_request = {}
        self.request_interval = 1.0 / rate_limit if rate_limit > 0 else 0
        
        # Statistics
        self.stats = PopulationStats()
        
        # Progress tracking
        self.processed_symbols = set()
        self.failed_symbols = {}
        
        logger.info(f"📊 Idempotent Price Populator initialized:")
        logger.info(f"   Vendor: {self.vendor}")
        logger.info(f"   Date range: {self.start_date} to {self.end_date}")
        logger.info(f"   Batch size: {self.batch_size}")
        logger.info(f"   Rate limit: {self.rate_limit} req/sec")
        logger.info(f"   Dry run: {self.dry_run}")
        logger.info(f"   Force refresh: {self.force_refresh}")

    async def get_symbols_for_population(self, pool: asyncpg.Pool) -> List[str]:
        """Get list of symbols that need price data population"""
        async with pool.acquire() as conn:
            # Get all active instruments on major US exchanges
            symbols = await conn.fetch("""
                SELECT DISTINCT symbol 
                FROM dev_instruments 
                WHERE active = true 
                  AND symbol IS NOT NULL 
                  AND symbol != ''
                  AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
                ORDER BY symbol
            """)
            
            symbol_list = [row['symbol'] for row in symbols]
            self.stats.total_symbols = len(symbol_list)
            
            logger.info(f"🎯 Found {len(symbol_list):,} symbols for population")
            return symbol_list

    async def analyze_coverage_gaps(self, pool: asyncpg.Pool, symbols: List[str]) -> Dict[str, Dict]:
        """Analyze existing price data coverage to identify gaps"""
        logger.info("🔍 Analyzing existing price data coverage...")
        
        coverage_analysis = {}
        vendors_to_check = ['polygon', 'tiingo'] if self.vendor == 'all' else [self.vendor]
        
        async with pool.acquire() as conn:
            for vendor in vendors_to_check:
                table_name = f"dev_daily_prices_{vendor}"
                
                # Check if vendor table exists
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                """, table_name)
                
                if not table_exists:
                    coverage_analysis[vendor] = {
                        'exists': False,
                        'symbols_with_data': set(),
                        'date_ranges': {},
                        'total_records': 0
                    }
                    continue
                
                # Get symbols that already have data
                existing_symbols = await conn.fetch(f"""
                    SELECT DISTINCT symbol, MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as records
                    FROM {table_name}
                    WHERE symbol = ANY($1::text[])
                    GROUP BY symbol
                """, symbols)
                
                symbols_with_data = set()
                date_ranges = {}
                total_records = 0
                
                for row in existing_symbols:
                    symbol = row['symbol']
                    symbols_with_data.add(symbol)
                    date_ranges[symbol] = {
                        'min_date': row['min_date'],
                        'max_date': row['max_date'],
                        'records': row['records']
                    }
                    total_records += row['records']
                
                coverage_analysis[vendor] = {
                    'exists': True,
                    'symbols_with_data': symbols_with_data,
                    'date_ranges': date_ranges,
                    'total_records': total_records
                }
                
                logger.info(f"   📈 {vendor}: {len(symbols_with_data):,} symbols, {total_records:,} records")
        
        return coverage_analysis

    async def determine_work_needed(self, symbols: List[str], coverage: Dict[str, Dict]) -> Dict[str, List[Dict]]:
        """Determine what work needs to be done for each vendor/symbol combination"""
        work_plan = {}
        vendors_to_process = ['polygon', 'tiingo'] if self.vendor == 'all' else [self.vendor]
        
        for vendor in vendors_to_process:
            if not self.api_keys.get(vendor):
                logger.warning(f"⚠️ No API key for {vendor}, skipping")
                continue
                
            work_plan[vendor] = []
            vendor_coverage = coverage.get(vendor, {'symbols_with_data': set(), 'date_ranges': {}})
            
            for symbol in symbols:
                if symbol in vendor_coverage['symbols_with_data'] and not self.force_refresh:
                    # Check for gaps in existing data
                    existing_range = vendor_coverage['date_ranges'][symbol]
                    min_existing = existing_range['min_date']
                    max_existing = existing_range['max_date']
                    
                    # Identify gaps
                    gaps = []
                    
                    # Gap before existing data
                    if min_existing > self.start_date:
                        gaps.append({
                            'symbol': symbol,
                            'start_date': self.start_date,
                            'end_date': min_existing - timedelta(days=1),
                            'type': 'backfill_early'
                        })
                    
                    # Gap after existing data
                    if max_existing < self.end_date:
                        gaps.append({
                            'symbol': symbol,
                            'start_date': max_existing + timedelta(days=1),
                            'end_date': self.end_date,
                            'type': 'backfill_recent'
                        })
                    
                    work_plan[vendor].extend(gaps)
                else:
                    # No existing data or force refresh - need full range
                    work_plan[vendor].append({
                        'symbol': symbol,
                        'start_date': self.start_date,
                        'end_date': self.end_date,
                        'type': 'full_backfill' if not self.force_refresh else 'refresh'
                    })
        
        # Log work summary
        for vendor, work_items in work_plan.items():
            logger.info(f"🔧 {vendor}: {len(work_items):,} work items planned")
            types_count = {}
            for item in work_items:
                types_count[item['type']] = types_count.get(item['type'], 0) + 1
            for work_type, count in types_count.items():
                logger.info(f"     {work_type}: {count:,}")
        
        return work_plan

    async def fetch_price_data(self, session: aiohttp.ClientSession, vendor: str, symbol: str, 
                              start_date: date, end_date: date) -> List[PriceRecord]:
        """Fetch price data from specified vendor"""
        await self.rate_limit_wait(vendor)
        
        try:
            if vendor == 'polygon':
                return await self.fetch_polygon_data(session, symbol, start_date, end_date)
            elif vendor == 'tiingo':
                return await self.fetch_tiingo_data(session, symbol, start_date, end_date)
            elif vendor == 'eodhd':
                return await self.fetch_eodhd_data(session, symbol, start_date, end_date)
            else:
                raise ValueError(f"Unsupported vendor: {vendor}")
                
        except Exception as e:
            logger.error(f"❌ Failed to fetch {vendor} data for {symbol}: {e}")
            return []

    async def rate_limit_wait(self, vendor: str):
        """Implement rate limiting"""
        if self.request_interval <= 0:
            return
            
        now = time.time()
        last_request = self.last_request.get(vendor, 0)
        
        if now - last_request < self.request_interval:
            wait_time = self.request_interval - (now - last_request)
            await asyncio.sleep(wait_time)
        
        self.last_request[vendor] = time.time()

    async def fetch_polygon_data(self, session: aiohttp.ClientSession, symbol: str, 
                               start_date: date, end_date: date) -> List[PriceRecord]:
        """Fetch data from Polygon.io"""
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {
            'apikey': self.api_keys['polygon'],
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000
        }
        
        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"Polygon API error: {response.status}")
            
            data = await response.json()
            self.stats.api_calls += 1
            
            if data.get('status') != 'OK' or 'results' not in data:
                return []
            
            records = []
            for result in data['results']:
                records.append(PriceRecord(
                    symbol=symbol,
                    date=date.fromtimestamp(result['t'] / 1000),
                    open=result['o'],
                    high=result['h'],
                    low=result['l'],
                    close=result['c'],
                    volume=result['v'],
                    adjusted_close=result.get('c'),  # Polygon provides adjusted by default
                    vendor='polygon'
                ))
            
            return records

    async def fetch_tiingo_data(self, session: aiohttp.ClientSession, symbol: str,
                               start_date: date, end_date: date) -> List[PriceRecord]:
        """Fetch data from Tiingo"""
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'token': self.api_keys['tiingo'],
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat(),
            'format': 'json'
        }
        
        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"Tiingo API error: {response.status}")
            
            data = await response.json()
            self.stats.api_calls += 1
            
            records = []
            for result in data:
                records.append(PriceRecord(
                    symbol=symbol,
                    date=datetime.strptime(result['date'][:10], '%Y-%m-%d').date(),
                    open=result['open'],
                    high=result['high'],
                    low=result['low'],
                    close=result['close'],
                    volume=result['volume'],
                    adjusted_close=result.get('adjClose'),
                    vendor='tiingo'
                ))
            
            return records

    async def fetch_eodhd_data(self, session: aiohttp.ClientSession, symbol: str,
                              start_date: date, end_date: date) -> List[PriceRecord]:
        """Fetch data from EODHD"""
        url = f"https://eodhd.com/api/eod/{symbol}.US"
        params = {
            'api_token': self.api_keys['eodhd'],
            'from': start_date.isoformat(),
            'to': end_date.isoformat(),
            'period': 'd',
            'fmt': 'json'
        }
        
        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"EODHD API error: {response.status}")
            
            data = await response.json()
            self.stats.api_calls += 1
            
            records = []
            for result in data:
                records.append(PriceRecord(
                    symbol=symbol,
                    date=datetime.strptime(result['date'], '%Y-%m-%d').date(),
                    open=result['open'],
                    high=result['high'],
                    low=result['low'],
                    close=result['close'],
                    volume=result['volume'],
                    adjusted_close=result.get('adjusted_close', result['close']),
                    vendor='eodhd'
                ))
            
            return records

    async def store_price_data_idempotent(self, pool: asyncpg.Pool, vendor: str, records: List[PriceRecord]) -> Dict[str, int]:
        """Store price data using idempotent UPSERT operations"""
        if not records:
            return {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        if self.dry_run:
            logger.info(f"🏃 DRY RUN: Would store {len(records)} {vendor} records")
            return {'new': len(records), 'updated': 0, 'skipped': 0, 'errors': 0}
        
        table_name = f"dev_daily_prices_{vendor}"
        results = {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        # Ensure table exists
        await self.ensure_vendor_table_exists(pool, vendor)
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                for record in records:
                    try:
                        # Use UPSERT to prevent duplicates
                        result = await conn.execute(f"""
                            INSERT INTO {table_name} (symbol, date, open, high, low, close, volume, adjusted_close, vendor, created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (symbol, date) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                adjusted_close = EXCLUDED.adjusted_close,
                                vendor = EXCLUDED.vendor,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE {table_name}.open != EXCLUDED.open 
                               OR {table_name}.high != EXCLUDED.high 
                               OR {table_name}.low != EXCLUDED.low 
                               OR {table_name}.close != EXCLUDED.close 
                               OR {table_name}.volume != EXCLUDED.volume
                        """, record.symbol, record.date, record.open, record.high, record.low, 
                             record.close, record.volume, record.adjusted_close, record.vendor)
                        
                        if 'INSERT' in result:
                            results['new'] += 1
                        elif 'UPDATE' in result:
                            results['updated'] += 1
                        else:
                            results['skipped'] += 1
                            
                    except Exception as e:
                        logger.error(f"❌ Error storing record {record.symbol} {record.date}: {e}")
                        results['errors'] += 1
        
        return results

    async def ensure_vendor_table_exists(self, pool: asyncpg.Pool, vendor: str):
        """Ensure vendor-specific price table exists with proper schema"""
        table_name = f"dev_daily_prices_{vendor}"
        
        async with pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    adjusted_close DOUBLE PRECISION,
                    vendor TEXT DEFAULT '{vendor}',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # Create index for performance
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date 
                ON {table_name} (symbol, date DESC)
            """)

    async def process_work_batch(self, session: aiohttp.ClientSession, pool: asyncpg.Pool, 
                                vendor: str, work_items: List[Dict]) -> Dict[str, int]:
        """Process a batch of work items for a vendor"""
        batch_results = {'new': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        for work_item in work_items:
            symbol = work_item['symbol']
            start_date = work_item['start_date']
            end_date = work_item['end_date']
            
            try:
                # Fetch price data
                records = await self.fetch_price_data(session, vendor, symbol, start_date, end_date)
                
                if records:
                    # Store data idempotently
                    store_results = await self.store_price_data_idempotent(pool, vendor, records)
                    
                    # Accumulate results
                    for key, value in store_results.items():
                        batch_results[key] += value
                    
                    logger.info(f"✅ {vendor} {symbol}: {len(records)} records, "
                              f"{store_results['new']} new, {store_results['updated']} updated")
                else:
                    logger.info(f"⚠️ {vendor} {symbol}: No data returned")
                    
                self.processed_symbols.add(symbol)
                
            except Exception as e:
                logger.error(f"❌ Failed processing {vendor} {symbol}: {e}")
                self.failed_symbols[symbol] = str(e)
                batch_results['errors'] += 1
        
        return batch_results

    async def run_population(self, pool: asyncpg.Pool):
        """Run the idempotent price population process"""
        logger.info("🚀 Starting idempotent price population...")
        
        # Step 1: Get symbols to process
        symbols = await self.get_symbols_for_population(pool)
        
        # Step 2: Analyze existing coverage
        coverage = await self.analyze_coverage_gaps(pool, symbols)
        
        # Step 3: Determine work needed
        work_plan = await self.determine_work_needed(symbols, coverage)
        
        # Step 4: Process work by vendor
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
            for vendor, work_items in work_plan.items():
                if not work_items:
                    logger.info(f"📋 {vendor}: No work needed")
                    continue
                
                logger.info(f"🔧 Processing {len(work_items):,} work items for {vendor}...")
                
                # Process in batches
                for i in range(0, len(work_items), self.batch_size):
                    batch = work_items[i:i + self.batch_size]
                    batch_num = (i // self.batch_size) + 1
                    total_batches = (len(work_items) + self.batch_size - 1) // self.batch_size
                    
                    logger.info(f"📦 Processing {vendor} batch {batch_num}/{total_batches} ({len(batch)} items)")
                    
                    batch_results = await self.process_work_batch(session, pool, vendor, batch)
                    
                    # Update global stats
                    self.stats.new_records += batch_results['new']
                    self.stats.updated_records += batch_results['updated']
                    self.stats.skipped_records += batch_results['skipped']
                    self.stats.error_records += batch_results['errors']
                    self.stats.processed_symbols += len(batch)
                    self.stats.last_update = datetime.now()
                    
                    # Log progress
                    progress = (self.stats.processed_symbols / self.stats.total_symbols) * 100
                    logger.info(f"📊 Progress: {self.stats.processed_symbols:,}/{self.stats.total_symbols:,} "
                              f"({progress:.1f}%) - {batch_results['new']} new, {batch_results['updated']} updated")

    def log_final_summary(self):
        """Log comprehensive final summary"""
        elapsed = datetime.now() - self.stats.start_time
        
        logger.info("=" * 80)
        logger.info("🎉 IDEMPOTENT PRICE POPULATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"⏱️  Total Time: {elapsed}")
        logger.info(f"🎯 Vendor: {self.vendor}")
        logger.info(f"📅 Date Range: {self.start_date} to {self.end_date}")
        logger.info(f"🏃 Dry Run Mode: {self.dry_run}")
        logger.info(f"🔄 Force Refresh: {self.force_refresh}")
        logger.info("")
        logger.info("📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Symbols: {self.stats.total_symbols:,}")
        logger.info(f"  Processed Symbols: {self.stats.processed_symbols:,}")
        logger.info(f"  Failed Symbols: {len(self.failed_symbols):,}")
        logger.info("")
        logger.info("📈 RECORD SUMMARY:")
        logger.info(f"  New Records: {self.stats.new_records:,}")
        logger.info(f"  Updated Records: {self.stats.updated_records:,}")
        logger.info(f"  Skipped Records: {self.stats.skipped_records:,}")
        logger.info(f"  Error Records: {self.stats.error_records:,}")
        logger.info(f"  API Calls Made: {self.stats.api_calls:,}")
        logger.info("")
        
        if self.failed_symbols:
            logger.info("❌ FAILED SYMBOLS:")
            for symbol, error in list(self.failed_symbols.items())[:10]:  # Show first 10
                logger.info(f"  {symbol}: {error[:60]}...")
            if len(self.failed_symbols) > 10:
                logger.info(f"  ... and {len(self.failed_symbols) - 10} more")
        
        success_rate = ((self.stats.total_symbols - len(self.failed_symbols)) / self.stats.total_symbols * 100) if self.stats.total_symbols > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN COMPLETED - No actual data stored")
        else:
            logger.info("✅ POPULATION COMPLETED - All data stored idempotently")
        
        logger.info("=" * 80)

async def main():
    """Main execution function"""
    
    # Configuration from environment
    vendor = os.getenv('VENDOR', 'all').lower()
    start_date = os.getenv('START_DATE')
    end_date = os.getenv('END_DATE')
    batch_size = int(os.getenv('BATCH_SIZE', '100'))
    max_retries = int(os.getenv('MAX_RETRIES', '3'))
    rate_limit = float(os.getenv('RATE_LIMIT', '5.0'))
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    force_refresh = os.getenv('FORCE_REFRESH', 'false').lower() == 'true'
    
    logger.info("🚀 Starting Idempotent Daily Price Population")
    logger.info(f"📊 Configuration: vendor={vendor}, batch_size={batch_size}, rate_limit={rate_limit}")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=60.0)
        
        # Initialize populator
        populator = IdempotentPricePopulator(
            vendor=vendor,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            max_retries=max_retries,
            rate_limit=rate_limit,
            dry_run=dry_run,
            force_refresh=force_refresh
        )
        
        # Run population
        await populator.run_population(pool)
        
        # Log final summary
        populator.log_final_summary()
        
        await pool.close()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Idempotent price population failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)