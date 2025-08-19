#!/usr/bin/env python3
"""
10-Year Minute Data Backfill (2010-2020)

Extends our minute data coverage from 2010-2020 for both Polygon and Tiingo.
This will provide 15 years total of minute data (2010-2025).

Features:
- Intelligent chunking by year and month
- Dual vendor support (Polygon + Tiingo)
- Rate limiting and error handling
- Progress tracking and resumption
- Database connection pooling
"""

import asyncio
import asyncpg
import aiohttp
import logging
import os
import json
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import time

@dataclass
class MinuteDataPoint:
    instrument_id: int
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    vwap: Optional[float] = None
    transactions: Optional[int] = None
    otc: Optional[bool] = False

class SimpleEnvironment:
    def get_table_name(self, base_name):
        return f"dev_{base_name}"
    
    def get_database_url(self):
        host = os.environ.get('DB_HOST', 'postgres-simple')
        port = os.environ.get('DB_PORT', '5432')
        user = os.environ.get('DB_USER', 'postgres')
        password = os.environ.get('DB_PASSWORD', 'dev_password')
        database = os.environ.get('DB_NAME', 'dev_db')
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

class TenYearMinuteBackfillOrchestrator:
    def __init__(self, vendor: str):
        self.vendor = vendor.lower()
        self.env = SimpleEnvironment()
        self.db_url = self.env.get_database_url()
        
        # API configuration
        if self.vendor == 'polygon':
            self.api_key = os.getenv('POLYGON_API_KEY')
            self.base_url = 'https://api.polygon.io'
            self.rate_limit_delay = 0.2  # 5 calls/min = 12 seconds between calls
        elif self.vendor == 'tiingo':
            self.api_key = os.getenv('TIINGO_API_KEY')
            self.base_url = 'https://api.tiingo.com'
            self.rate_limit_delay = 0.1  # More generous rate limits
        else:
            raise ValueError(f"Unsupported vendor: {vendor}")
        
        if not self.api_key:
            raise ValueError(f"{vendor.upper()}_API_KEY environment variable required")
        
        self.logger = logging.getLogger(f"{vendor}_minute_backfill")
        self.target_table = f"dev_minute_prices_{self.vendor}"
        
        # Backfill configuration
        self.start_year = 2010
        self.end_year = 2020
        self.chunk_size_days = 30  # Process 1 month at a time
        self.batch_size = 100      # Instruments per batch
        
    async def get_target_instruments(self) -> List[Tuple[int, str]]:
        """Get high-priority instruments for minute data backfill"""
        
        pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=5)
        
        try:
            async with pool.acquire() as conn:
                # Prioritize instruments that already have some minute data (likely active stocks)
                rows = await conn.fetch("""
                    WITH active_instruments AS (
                        SELECT DISTINCT i.id, i.symbol
                        FROM dev_instruments i
                        WHERE EXISTS (
                            SELECT 1 FROM dev_minute_prices_polygon mp 
                            WHERE mp.instrument_id = i.id
                            UNION ALL
                            SELECT 1 FROM dev_minute_prices_tiingo mt
                            WHERE mt.instrument_id = i.id
                        )
                        AND i.symbol ~ '^[A-Z]{1,5}$'  -- Regular stock symbols
                        AND i.symbol NOT LIKE '%-%'    -- No options
                        AND i.symbol NOT LIKE '%.%'    -- No some ETFs
                    ),
                    liquid_instruments AS (
                        SELECT DISTINCT i.id, i.symbol
                        FROM dev_instruments i
                        JOIN dev_daily_prices_polygon dp ON i.id = dp.instrument_id
                        WHERE dp.volume > 1000000  -- High volume stocks
                        AND dp.date >= '2020-01-01'
                        AND i.symbol ~ '^[A-Z]{1,5}$'
                    )
                    SELECT COALESCE(a.id, l.id) as id, COALESCE(a.symbol, l.symbol) as symbol
                    FROM active_instruments a
                    FULL OUTER JOIN liquid_instruments l ON a.id = l.id
                    ORDER BY 
                        CASE WHEN a.id IS NOT NULL AND l.id IS NOT NULL THEN 1  -- Both active and liquid
                             WHEN a.id IS NOT NULL THEN 2                       -- Active
                             WHEN l.id IS NOT NULL THEN 3                       -- Liquid
                             ELSE 4 END,
                        symbol
                    LIMIT 5000  -- Focus on top 5K instruments
                """)
                
                instruments = [(row['id'], row['symbol']) for row in rows]
                self.logger.info(f"Selected {len(instruments)} instruments for {self.vendor} minute backfill")
                
                return instruments
        
        finally:
            await pool.close()
    
    async def check_existing_coverage(self, instrument_id: int, start_date: date, end_date: date) -> bool:
        """Check if we already have data for this instrument and date range"""
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
        
        try:
            async with pool.acquire() as conn:
                count = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {self.target_table}
                    WHERE instrument_id = $1
                      AND DATE(timestamp) >= $2
                      AND DATE(timestamp) <= $3
                """, instrument_id, start_date, end_date)
                
                # If we have any data for this period, consider it covered
                return count > 0
        
        finally:
            await pool.close()
    
    async def fetch_polygon_minute_data(self, symbol: str, start_date: date, end_date: date, 
                                      session: aiohttp.ClientSession) -> List[MinuteDataPoint]:
        """Fetch minute data from Polygon API"""
        
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000  # Max limit
        }
        headers = {'Authorization': f'Bearer {self.api_key}'}
        
        try:
            await asyncio.sleep(self.rate_limit_delay)
            
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    self.logger.warning(f"Rate limit hit for {symbol}, waiting...")
                    await asyncio.sleep(60)
                    return []
                
                if response.status != 200:
                    self.logger.warning(f"HTTP {response.status} for {symbol} {start_date}-{end_date}")
                    return []
                
                data = await response.json()
                
                if 'results' not in data or not data['results']:
                    self.logger.debug(f"No data for {symbol} {start_date}-{end_date}")
                    return []
                
                # Get instrument_id (we'll need to resolve this)
                # For now, return data without instrument_id - will resolve later
                results = []
                for bar in data['results']:
                    # Polygon timestamp is in milliseconds
                    timestamp = datetime.fromtimestamp(bar['t'] / 1000)
                    
                    minute_data = MinuteDataPoint(
                        instrument_id=0,  # Will be resolved later
                        timestamp=timestamp,
                        open_price=float(bar['o']),
                        high_price=float(bar['h']),
                        low_price=float(bar['l']),
                        close_price=float(bar['c']),
                        volume=int(bar['v']),
                        vwap=bar.get('vw'),
                        transactions=bar.get('n'),
                        otc=False
                    )
                    results.append(minute_data)
                
                self.logger.debug(f"Fetched {len(results)} minute bars for {symbol} {start_date}-{end_date}")
                return results
        
        except Exception as e:
            self.logger.error(f"Error fetching {symbol} {start_date}-{end_date}: {e}")
            return []
    
    async def fetch_tiingo_minute_data(self, symbol: str, start_date: date, end_date: date,
                                     session: aiohttp.ClientSession) -> List[MinuteDataPoint]:
        """Fetch minute data from Tiingo API"""
        
        url = f"{self.base_url}/iex/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'resampleFreq': '1min',
            'token': self.api_key
        }
        
        try:
            await asyncio.sleep(self.rate_limit_delay)
            
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    self.logger.warning(f"Rate limit hit for {symbol}, waiting...")
                    await asyncio.sleep(30)
                    return []
                
                if response.status != 200:
                    self.logger.warning(f"HTTP {response.status} for {symbol} {start_date}-{end_date}")
                    return []
                
                data = await response.json()
                
                if not data:
                    self.logger.debug(f"No data for {symbol} {start_date}-{end_date}")
                    return []
                
                results = []
                for bar in data:
                    # Tiingo timestamp format: "2020-01-02T09:30:00+00:00"
                    timestamp = datetime.fromisoformat(bar['date'].replace('Z', '+00:00'))
                    
                    minute_data = MinuteDataPoint(
                        instrument_id=0,  # Will be resolved later
                        timestamp=timestamp,
                        open_price=float(bar['open']),
                        high_price=float(bar['high']),
                        low_price=float(bar['low']),
                        close_price=float(bar['close']),
                        volume=int(bar['volume']),
                        vwap=None,  # Tiingo doesn't provide VWAP in minute data
                        transactions=None,
                        otc=False
                    )
                    results.append(minute_data)
                
                self.logger.debug(f"Fetched {len(results)} minute bars for {symbol} {start_date}-{end_date}")
                return results
        
        except Exception as e:
            self.logger.error(f"Error fetching {symbol} {start_date}-{end_date}: {e}")
            return []
    
    async def save_minute_data_batch(self, instrument_id: int, minute_data: List[MinuteDataPoint]) -> int:
        """Save batch of minute data to database"""
        
        if not minute_data:
            return 0
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
        
        try:
            async with pool.acquire() as conn:
                # Update instrument_id for all data points
                for data_point in minute_data:
                    data_point.instrument_id = instrument_id
                
                # Batch insert
                values = [
                    (
                        dp.instrument_id,
                        dp.timestamp,
                        dp.open_price,
                        dp.high_price, 
                        dp.low_price,
                        dp.close_price,
                        dp.volume,
                        dp.vwap,
                        dp.transactions,
                        dp.otc,
                        datetime.now()  # created_at
                    )
                    for dp in minute_data
                ]
                
                await conn.executemany(f"""
                    INSERT INTO {self.target_table} 
                    (instrument_id, timestamp, open_price, high_price, low_price, close_price, 
                     volume, vwap, transactions, otc, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (instrument_id, timestamp) DO NOTHING
                """, values)
                
                return len(values)
        
        finally:
            await pool.close()
    
    async def process_instrument_chunk(self, instrument_id: int, symbol: str, 
                                     start_date: date, end_date: date,
                                     session: aiohttp.ClientSession) -> Dict:
        """Process one instrument for a specific date range"""
        
        # Check if we already have data
        if await self.check_existing_coverage(instrument_id, start_date, end_date):
            self.logger.debug(f"Skipping {symbol} {start_date}-{end_date} (already have data)")
            return {'symbol': symbol, 'status': 'skipped', 'records': 0}
        
        # Fetch data based on vendor
        if self.vendor == 'polygon':
            minute_data = await self.fetch_polygon_minute_data(symbol, start_date, end_date, session)
        elif self.vendor == 'tiingo':
            minute_data = await self.fetch_tiingo_minute_data(symbol, start_date, end_date, session)
        else:
            return {'symbol': symbol, 'status': 'error', 'records': 0, 'error': f'Unknown vendor: {self.vendor}'}
        
        if not minute_data:
            return {'symbol': symbol, 'status': 'no_data', 'records': 0}
        
        # Save to database
        try:
            saved_count = await self.save_minute_data_batch(instrument_id, minute_data)
            return {'symbol': symbol, 'status': 'success', 'records': saved_count}
        except Exception as e:
            self.logger.error(f"Error saving {symbol} data: {e}")
            return {'symbol': symbol, 'status': 'error', 'records': 0, 'error': str(e)}
    
    async def run_backfill(self) -> Dict:
        """Run the complete 10-year minute data backfill"""
        
        start_time = datetime.now()
        self.logger.info(f"🚀 Starting {self.vendor} 10-year minute backfill (2010-2020)")
        
        # Get target instruments
        instruments = await self.get_target_instruments()
        total_instruments = len(instruments)
        
        if not instruments:
            self.logger.error("No instruments found for backfill")
            return {'error': 'No instruments found'}
        
        # Generate date chunks (monthly)
        date_chunks = []
        current_date = date(self.start_year, 1, 1)
        end_date = date(self.end_year, 12, 31)
        
        while current_date <= end_date:
            # Get end of month
            if current_date.month == 12:
                chunk_end = date(current_date.year + 1, 1, 1) - timedelta(days=1)
            else:
                chunk_end = date(current_date.year, current_date.month + 1, 1) - timedelta(days=1)
            
            chunk_end = min(chunk_end, end_date)
            date_chunks.append((current_date, chunk_end))
            
            # Move to next month
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)
        
        total_chunks = len(date_chunks)
        self.logger.info(f"📊 Processing {total_instruments} instruments across {total_chunks} monthly chunks")
        
        # Progress tracking
        total_processed = 0
        total_success = 0
        total_records = 0
        
        # Process in batches
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
                chunk_num = chunk_idx + 1
                self.logger.info(f"🗓️  Processing chunk {chunk_num}/{total_chunks}: {chunk_start} to {chunk_end}")
                
                chunk_start_time = datetime.now()
                chunk_success = 0
                chunk_records = 0
                
                # Process instruments in batches for this date chunk
                for i in range(0, total_instruments, self.batch_size):
                    batch = instruments[i:i + self.batch_size]
                    batch_num = (i // self.batch_size) + 1
                    total_batches = (total_instruments + self.batch_size - 1) // self.batch_size
                    
                    self.logger.info(f"  📦 Batch {batch_num}/{total_batches} ({len(batch)} instruments)")
                    
                    # Process batch concurrently (but with rate limiting)
                    tasks = []
                    for instrument_id, symbol in batch:
                        task = self.process_instrument_chunk(
                            instrument_id, symbol, chunk_start, chunk_end, session
                        )
                        tasks.append(task)
                    
                    try:
                        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Process results
                        for result in batch_results:
                            if isinstance(result, Exception):
                                self.logger.error(f"Batch processing error: {result}")
                                continue
                            
                            total_processed += 1
                            if result['status'] == 'success':
                                total_success += 1
                                chunk_success += 1
                                chunk_records += result['records']
                                total_records += result['records']
                        
                        # Brief delay between batches
                        await asyncio.sleep(2)
                    
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
                        continue
                
                # Chunk summary
                chunk_duration = datetime.now() - chunk_start_time
                success_rate = (chunk_success / len(instruments)) * 100
                
                self.logger.info(f"  ✅ Chunk {chunk_num} completed: {chunk_success}/{len(instruments)} successful ({success_rate:.1f}%)")
                self.logger.info(f"  📈 Records added: {chunk_records:,} | Duration: {chunk_duration}")
        
        # Final summary
        end_time = datetime.now()
        total_duration = end_time - start_time
        overall_success_rate = (total_success / total_processed) * 100 if total_processed > 0 else 0
        
        summary = {
            'vendor': self.vendor,
            'period': f"{self.start_year}-{self.end_year}",
            'total_instruments': total_instruments,
            'total_chunks': total_chunks,
            'total_processed': total_processed,
            'total_success': total_success,
            'total_records': total_records,
            'success_rate': overall_success_rate,
            'duration': str(total_duration)
        }
        
        self.logger.info(f"🎉 {self.vendor} 10-year minute backfill completed!")
        self.logger.info(f"📊 Final results: {total_success}/{total_processed} successful ({overall_success_rate:.1f}%)")
        self.logger.info(f"📈 Total records added: {total_records:,}")
        self.logger.info(f"⏱️  Total duration: {total_duration}")
        
        return summary

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get vendor from environment variable
    vendor = os.getenv('VENDOR', 'polygon').lower()
    
    if vendor not in ['polygon', 'tiingo']:
        print(f"❌ Unsupported vendor: {vendor}")
        print("Set VENDOR environment variable to 'polygon' or 'tiingo'")
        return
    
    try:
        orchestrator = TenYearMinuteBackfillOrchestrator(vendor)
        summary = await orchestrator.run_backfill()
        
        print(f"\n📋 FINAL SUMMARY - {vendor.upper()}")
        print("=" * 60)
        for key, value in summary.items():
            print(f"{key}: {value}")
            
    except Exception as e:
        print(f"❌ Backfill failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())