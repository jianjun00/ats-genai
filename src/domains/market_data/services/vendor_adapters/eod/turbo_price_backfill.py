#!/usr/bin/env python3
"""
Turbo-charged daily price backfill using asyncio concurrency.
Target: 5000+ symbols with 5-year history in 1-2 days.
No Ray overhead, pure asyncio performance.
"""

import os
import asyncio
import argparse
import logging
from datetime import datetime, date
import aiohttp
import asyncpg
from typing import List, Dict, Any
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TurboPolygonFetcher:
    """High-performance Polygon API fetcher with massive concurrency."""

    def __init__(self, api_key: str, max_concurrent: int = 100):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=100)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_symbol_year(self, symbol: str, start_date: date, end_date: date, instrument_id: int) -> List[Dict[str, Any]]:
        """Fetch one year of data for a symbol with exponential backoff retry."""
        async with self.semaphore:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {
                'adjusted': 'true',
                'sort': 'asc',
                'limit': 50000,
                'apikey': self.api_key
            }

            max_retries = 5
            base_delay = 1.0

            for attempt in range(max_retries):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = []
                            if 'results' in data and data['results']:
                                for item in data['results']:
                                    results.append({
                                        'date': datetime.utcfromtimestamp(item['t']/1000).date(),
                                        'instrument_id': instrument_id,
                                        'open': item['o'],
                                        'high': item['h'],
                                        'low': item['l'],
                                        'close': item['c'],
                                        'volume': item['v']
                                    })
                            return results
                        elif response.status == 429:  # Rate limited
                            delay = base_delay * (2 ** attempt) + (0.1 * attempt)  # Exponential backoff with jitter
                            logger.warning(f"Polygon rate limit for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status >= 500:  # Server errors
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Polygon server error {response.status} for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Polygon API error for {symbol}: HTTP {response.status}")
                            return []

                except asyncio.TimeoutError:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Polygon timeout for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Polygon max retries exceeded for {symbol}")
                        return []
                except Exception as e:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Polygon error for {symbol}: {e}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Polygon max retries exceeded for {symbol}: {e}")
                        return []

            return []

class TurboTiingoFetcher:
    """High-performance Tiingo API fetcher with massive concurrency."""

    def __init__(self, api_key: str, max_concurrent: int = 50):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_symbol_data(self, symbol: str, start_date: str, end_date: str, instrument_id: int) -> List[Dict[str, Any]]:
        """Fetch 5-year data for a symbol with exponential backoff retry."""
        async with self.semaphore:
            url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
            params = {
                'startDate': start_date,
                'endDate': end_date,
                'format': 'json',
                'token': self.api_key
            }

            max_retries = 5
            base_delay = 2.0  # Tiingo has stricter rate limits

            for attempt in range(max_retries):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = []
                            for item in data:
                                results.append({
                                    'date': datetime.strptime(item['date'][:10], "%Y-%m-%d").date(),
                                    'instrument_id': instrument_id,
                                    'open': item.get('open'),
                                    'high': item.get('high'),
                                    'low': item.get('low'),
                                    'close': item.get('close'),
                                    'volume': item.get('volume')
                                })
                            return results
                        elif response.status == 429:  # Rate limited
                            delay = base_delay * (2 ** attempt) + (0.2 * attempt)  # Longer backoff for Tiingo
                            logger.warning(f"Tiingo rate limit for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status >= 500:  # Server errors
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Tiingo server error {response.status} for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Tiingo API error for {symbol}: HTTP {response.status}")
                            return []

                except asyncio.TimeoutError:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Tiingo timeout for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Tiingo max retries exceeded for {symbol}")
                        return []
                except Exception as e:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Tiingo error for {symbol}: {e}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Tiingo max retries exceeded for {symbol}: {e}")
                        return []

            return []

class TurboDatabaseInserter:
    """High-performance database inserter with connection pooling."""

    def __init__(self, db_config: dict, pool_size: int = 20):
        self.db_config = db_config
        self.pool_size = pool_size
        self.pool = None

    async def __aenter__(self):
        self.pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=self.pool_size,
            max_size=self.pool_size * 2,
            server_settings={'jit': 'off'}  # Disable JIT for batch operations
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()

    async def bulk_insert_polygon(self, data_batch: List[Dict[str, Any]]) -> int:
        """Ultra-fast bulk insert using COPY."""
        if not data_batch:
            return 0

        async with self.pool.acquire() as conn:
            try:
                # Use INSERT with ON CONFLICT for duplicate handling
                records = [(
                    item['date'],
                    item['instrument_id'],
                    item['open'],
                    item['high'],
                    item['low'],
                    item['close'],
                    item['volume']
                ) for item in data_batch]

                await conn.executemany("""
                    INSERT INTO dev_daily_prices_polygon
                    (date, instrument_id, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (date, instrument_id) DO NOTHING
                """, records)
                return len(data_batch)
            except Exception as e:
                logger.error(f"Polygon database insert error: {e}")
                return 0

    async def bulk_insert_tiingo(self, data_batch: List[Dict[str, Any]]) -> int:
        """Ultra-fast bulk insert using COPY."""
        if not data_batch:
            return 0

        async with self.pool.acquire() as conn:
            try:
                # Use INSERT with ON CONFLICT for duplicate handling
                records = [(
                    item['date'],
                    item['instrument_id'],
                    item['open'],
                    item['high'],
                    item['low'],
                    item['close'],
                    item['volume']
                ) for item in data_batch]

                await conn.executemany("""
                    INSERT INTO dev_daily_prices_tiingo
                    (date, instrument_id, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (date, instrument_id) DO NOTHING
                """, records)
                return len(data_batch)
            except Exception as e:
                logger.error(f"Tiingo database insert error: {e}")
                return 0

async def process_polygon_symbols(symbols_data: List[tuple], start_date: str, end_date: str,
                                 polygon_api_key: str, db_inserter: TurboDatabaseInserter):
    """Process all symbols for Polygon with massive concurrency."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    async with TurboPolygonFetcher(polygon_api_key, max_concurrent=100) as fetcher:
        tasks = []

        # Create tasks for each symbol-year combination
        for symbol, instrument_id in symbols_data:
            current_year = start_dt.year
            while current_year <= end_dt.year:
                year_start = max(date(current_year, 1, 1), start_dt)
                year_end = min(date(current_year, 12, 31), end_dt)

                task = fetcher.fetch_symbol_year(symbol, year_start, year_end, instrument_id)
                tasks.append(task)
                current_year += 1

        logger.info(f"Starting {len(tasks)} Polygon API calls...")

        # Process in chunks to manage memory
        chunk_size = 500
        total_records = 0

        for i in range(0, len(tasks), chunk_size):
            chunk_tasks = tasks[i:i+chunk_size]
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

            # Collect all valid results
            batch_data = []
            for result in results:
                if isinstance(result, list):
                    batch_data.extend(result)

            # Bulk insert
            if batch_data:
                inserted = await db_inserter.bulk_insert_polygon(batch_data)
                total_records += inserted
                logger.info(f"Polygon chunk {i//chunk_size + 1}: {inserted} records inserted")

        logger.info(f"Polygon COMPLETE: {total_records} total records")
        return total_records

async def process_tiingo_symbols(symbols_data: List[tuple], start_date: str, end_date: str,
                                tiingo_api_key: str, db_inserter: TurboDatabaseInserter):
    """Process all symbols for Tiingo with high concurrency."""
    async with TurboTiingoFetcher(tiingo_api_key, max_concurrent=50) as fetcher:
        tasks = []

        # Create tasks for each symbol
        for symbol, instrument_id in symbols_data:
            task = fetcher.fetch_symbol_data(symbol, start_date, end_date, instrument_id)
            tasks.append(task)

        logger.info(f"Starting {len(tasks)} Tiingo API calls...")

        # Process in chunks
        chunk_size = 250
        total_records = 0

        for i in range(0, len(tasks), chunk_size):
            chunk_tasks = tasks[i:i+chunk_size]
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

            # Collect all valid results
            batch_data = []
            for result in results:
                if isinstance(result, list):
                    batch_data.extend(result)

            # Bulk insert
            if batch_data:
                inserted = await db_inserter.bulk_insert_tiingo(batch_data)
                total_records += inserted
                logger.info(f"Tiingo chunk {i//chunk_size + 1}: {inserted} records inserted")

            # Brief pause for Tiingo rate limits
            await asyncio.sleep(1)

        logger.info(f"Tiingo COMPLETE: {total_records} total records")
        return total_records

async def main():
    parser = argparse.ArgumentParser(description="Turbo daily price backfill")
    parser.add_argument('--start_date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--environment', default='dev', choices=['test', 'intg', 'prod', 'dev'])
    parser.add_argument('--gin_config', default='config/app_dev.gin', help='Gin config file')
    parser.add_argument('--limit', type=int, help='Limit number of symbols (for testing)')
    args = parser.parse_args()

    # Setup environment
    from shared.utils.environment import Environment
    env = Environment(gin_config_path=args.gin_config)

    # Get API keys
    polygon_api_key = env.get_polygon_api_key() or os.getenv("POLYGON_API_KEY")
    tiingo_api_key = os.getenv("TIINGO_API_KEY")

    if not polygon_api_key or not tiingo_api_key:
        logger.error("Missing API keys!")
        return

    # Database config
    db_config = {
        'host': os.getenv("DB_HOST", "localhost"),
        'port': int(os.getenv("DB_PORT", "5433")),
        'user': os.getenv("DB_USER", "postgres"),
        'password': os.getenv("DB_PASSWORD", "postgres"),
        'database': os.getenv("DB_NAME", "dev_db")
    }

    # Get symbols
    from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)

    symbols_data = []
    all_symbols = await xrefs_dao.get_all_symbols()

    if args.limit:
        all_symbols = all_symbols[:args.limit]

    logger.info(f"Processing {len(all_symbols)} symbols...")

    for symbol in all_symbols:
        instrument_id = await xrefs_dao.resolve_instrument_id(symbol)
        if instrument_id:
            symbols_data.append((symbol, instrument_id))

    logger.info(f"Found {len(symbols_data)} symbols with instrument_ids")

    # Initialize database inserter
    async with TurboDatabaseInserter(db_config, pool_size=20) as db_inserter:
        start_time = time.time()

        # Process both vendors concurrently
        polygon_task = process_polygon_symbols(symbols_data, args.start_date, args.end_date, polygon_api_key, db_inserter)
        tiingo_task = process_tiingo_symbols(symbols_data, args.start_date, args.end_date, tiingo_api_key, db_inserter)

        polygon_total, tiingo_total = await asyncio.gather(polygon_task, tiingo_task)

        elapsed = time.time() - start_time
        total_records = polygon_total + tiingo_total

        logger.info(f"🚀 TURBO BACKFILL COMPLETE!")
        logger.info(f"⏱️  Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        logger.info(f"📊 Records: {total_records:,} total ({polygon_total:,} Polygon + {tiingo_total:,} Tiingo)")
        logger.info(f"🔥 Rate: {total_records/elapsed:.0f} records/second")

if __name__ == "__main__":
    asyncio.run(main())