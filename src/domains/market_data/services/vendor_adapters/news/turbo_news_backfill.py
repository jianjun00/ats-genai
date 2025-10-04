#!/usr/bin/env python3
"""
Turbo-charged news backfill using asyncio concurrency.
Target: 5-year news history for all symbols with high performance.
No Ray overhead, pure asyncio performance.
"""

import os
import asyncio
import argparse
import logging
from datetime import datetime
import aiohttp
from typing import List, Dict, Any
import time
import json
import sys

# Add src to Python path for shared utilities
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'src'))

# Import shared utilities
from core.shared.vendor_api_keys import get_polygon_api_key
from core.shared.database_connections import get_database_pool, get_table_name
from core.shared.utils_core.backfill_framework import BackfillStats, VendorRateLimiters

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TurboPolygonNewsFetcher:
    """High-performance Polygon News API fetcher with massive concurrency and shared utilities."""

    def __init__(self, api_key: str = None, max_concurrent: int = 50):
        # Use shared utilities for robust API key management
        self.api_key = api_key or get_polygon_api_key()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

        # Initialize shared utilities for monitoring and rate limiting
        self.stats = BackfillStats()
        self.rate_limiter = VendorRateLimiters.polygon_free()  # Default to free tier

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_news_for_symbol(self, symbol: str, published_gte: str = None, published_lte: str = None) -> List[Dict[str, Any]]:
        """Fetch news for a symbol using shared rate limiting and statistics."""
        async with self.semaphore:
            # Use shared rate limiting to prevent 429 errors
            await self.rate_limiter.wait_if_needed()

            url = "https://api.polygon.io/v2/reference/news"
            params = {
                'ticker': symbol,
                'limit': 1000,  # Maximum limit
                'apikey': self.api_key
            }

            if published_gte:
                params['published_utc.gte'] = published_gte
            if published_lte:
                params['published_utc.lte'] = published_lte

            # Track API call with shared statistics
            self.stats.api_calls_made += 1

            max_retries = 3  # Reduced since rate limiting is handled by shared utilities
            base_delay = 1.0

            for attempt in range(max_retries):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = []
                            if 'results' in data and data['results']:
                                for item in data['results']:
                                    # Convert to our standardized format
                                    news_item = {
                                        'polygon_id': item.get('id'),
                                        'title': item.get('title', ''),
                                        'description': item.get('description', ''),
                                        'author': item.get('author'),
                                        'published_utc': datetime.fromisoformat(item['published_utc'].replace('Z', '+00:00')),
                                        'article_url': item.get('article_url'),
                                        'image_url': item.get('image_url'),
                                        'publisher_name': item.get('publisher', {}).get('name'),
                                        'publisher_homepage_url': item.get('publisher', {}).get('homepage_url'),
                                        'publisher_logo_url': item.get('publisher', {}).get('logo_url'),
                                        'publisher_favicon_url': item.get('publisher', {}).get('favicon_url'),
                                        'keywords': item.get('keywords', []),
                                        'tickers': item.get('tickers', []),
                                        'insights': item.get('insights'),
                                        'data': item  # Store full original data
                                    }
                                    results.append(news_item)

                            # Track successful records fetched
                            self.stats.records_fetched += len(results)
                            return results
                        elif response.status == 429:  # Rate limited
                            self.stats.api_errors += 1
                            delay = base_delay * (2 ** attempt) + (0.1 * attempt)
                            logger.warning(f"Rate limit hit despite shared limiter for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status >= 500:  # Server errors
                            self.stats.api_errors += 1
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Server error {response.status} for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            self.stats.api_errors += 1
                            logger.warning(f"API error for {symbol}: HTTP {response.status}")
                            return []

                except asyncio.TimeoutError:
                    self.stats.api_errors += 1
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Timeout for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Max retries exceeded for {symbol} (timeout)")
                        return []
                except Exception as e:
                    self.stats.api_errors += 1
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Error for {symbol}: {e}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Max retries exceeded for {symbol}: {e}")
                        return []

            return []

    def get_statistics_summary(self) -> dict:
        """Get comprehensive statistics summary using shared framework"""
        return {
            "vendor": "polygon",
            "api_calls_made": self.stats.api_calls_made,
            "api_errors": self.stats.api_errors,
            "records_fetched": self.stats.records_fetched,
            "success_rate": self.stats.success_rate,
            "rate_limiter_calls": getattr(self.rate_limiter, 'calls_made', 0)
        }

    def log_final_summary(self, logger):
        """Log comprehensive operation summary using shared framework"""
        logger.info(f"🔢 Polygon News Backfill Statistics:")
        self.stats.log_final_summary(logger)

class TurboTiingoNewsFetcher:
    """High-performance Tiingo News API fetcher with massive concurrency."""

    def __init__(self, api_key: str, max_concurrent: int = 30):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=80, limit_per_host=30)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_news_for_symbol(self, symbol: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Fetch news for a symbol with exponential backoff retry."""
        async with self.semaphore:
            url = "https://api.tiingo.com/tiingo/news"
            params = {
                'tickers': symbol,
                'token': self.api_key
            }

            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date

            max_retries = 5
            base_delay = 2.0  # Tiingo has stricter rate limits

            for attempt in range(max_retries):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = []
                            for item in data:
                                # Convert to our standardized format
                                news_item = {
                                    'tiingo_id': item.get('id'),
                                    'title': item.get('title', ''),
                                    'description': item.get('description', ''),
                                    'published_date': datetime.fromisoformat(item['publishedDate'].replace('Z', '+00:00')),
                                    'crawl_date': datetime.fromisoformat(item['crawlDate'].replace('Z', '+00:00')),
                                    'url': item.get('url'),
                                    'source': item.get('source'),
                                    'tags': item.get('tags', []),
                                    'tickers': item.get('tickers', []),
                                    'data': item  # Store full original data
                                }
                                results.append(news_item)
                            return results
                        elif response.status == 429:  # Rate limited
                            delay = base_delay * (2 ** attempt) + (0.2 * attempt)
                            logger.warning(f"Tiingo rate limit for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status >= 500:  # Server errors
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Tiingo server error {response.status} for {symbol}, attempt {attempt+1}/{max_retries}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"Tiingo News API error for {symbol}: HTTP {response.status}")
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

class TurboNewsDatabaseInserter:
    """High-performance database inserter for news data with connection pooling."""

    def __init__(self, environment: str = 'dev', pool_size: int = 20):
        self.environment = environment
        self.pool_size = pool_size
        self.pool = None
        self.stats = BackfillStats()

    async def __aenter__(self):
        # Use shared utilities for robust database connection
        self.pool = await get_database_pool(environment=self.environment)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()

    async def bulk_insert_polygon_news(self, data_batch: List[Dict[str, Any]]) -> int:
        """Ultra-fast bulk insert for Polygon news using executemany."""
        if not data_batch:
            return 0

        async with self.pool.acquire() as conn:
            try:
                records = [
                    (
                        item['polygon_id'],
                        item['title'],
                        item['description'],
                        item['author'],
                        item['published_utc'],
                        item['article_url'],
                        item['image_url'],
                        item['publisher_name'],
                        item['publisher_homepage_url'],
                        item['publisher_logo_url'],
                        item['publisher_favicon_url'],
                        item['keywords'],
                        item['tickers'],
                        json.dumps(item['insights']) if item['insights'] else None,
                        json.dumps(item['data'])
                    )
                    for item in data_batch
                ]

                table_name = get_table_name('news_polygon', self.environment)
                await conn.executemany(f"""
                    INSERT INTO {table_name} (
                        polygon_id, title, description, author, published_utc,
                        article_url, image_url, publisher_name, publisher_homepage_url,
                        publisher_logo_url, publisher_favicon_url, keywords, tickers,
                        insights, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (polygon_id) DO NOTHING
                """, records)
                return len(data_batch)
            except Exception as e:
                logger.error(f"Polygon news database insert error: {e}")
                return 0

    async def bulk_insert_tiingo_news(self, data_batch: List[Dict[str, Any]]) -> int:
        """Ultra-fast bulk insert for Tiingo news using executemany."""
        if not data_batch:
            return 0

        async with self.pool.acquire() as conn:
            try:
                records = [
                    (
                        item['tiingo_id'],
                        item['title'],
                        item['description'],
                        item['published_date'],
                        item['crawl_date'],
                        item['url'],
                        item['source'],
                        item['tags'],
                        item['tickers'],
                        json.dumps(item['data'])
                    )
                    for item in data_batch
                ]

                table_name_tiingo = get_table_name('news_tiingo', self.environment)
                await conn.executemany(f"""
                    INSERT INTO {table_name_tiingo} (
                        tiingo_id, title, description, published_date, crawl_date,
                        url, source, tags, tickers, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (tiingo_id) DO NOTHING
                """, records)
                return len(data_batch)
            except Exception as e:
                logger.error(f"Tiingo news database insert error: {e}")
                return 0

async def process_polygon_news(symbols: List[str], start_date: str, end_date: str,
                              polygon_api_key: str, db_inserter: TurboNewsDatabaseInserter):
    """Process all symbols for Polygon news with massive concurrency."""
    async with TurboPolygonNewsFetcher(polygon_api_key, max_concurrent=50) as fetcher:
        tasks = []

        # Create tasks for each symbol
        for symbol in symbols:
            task = fetcher.fetch_news_for_symbol(
                symbol,
                published_gte=start_date,
                published_lte=end_date
            )
            tasks.append(task)

        logger.info(f"Starting {len(tasks)} Polygon News API calls...")

        # Process in chunks to manage memory
        chunk_size = 50
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
                inserted = await db_inserter.bulk_insert_polygon_news(batch_data)
                total_records += inserted
                logger.info(f"Polygon News chunk {i//chunk_size + 1}: {inserted} records inserted")

        logger.info(f"Polygon News COMPLETE: {total_records} total records")
        return total_records

async def process_tiingo_news(symbols: List[str], start_date: str, end_date: str,
                             tiingo_api_key: str, db_inserter: TurboNewsDatabaseInserter):
    """Process all symbols for Tiingo news with high concurrency."""
    async with TurboTiingoNewsFetcher(tiingo_api_key, max_concurrent=30) as fetcher:
        tasks = []

        # Create tasks for each symbol
        for symbol in symbols:
            task = fetcher.fetch_news_for_symbol(symbol, start_date, end_date)
            tasks.append(task)

        logger.info(f"Starting {len(tasks)} Tiingo News API calls...")

        # Process in chunks
        chunk_size = 30
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
                inserted = await db_inserter.bulk_insert_tiingo_news(batch_data)
                total_records += inserted
                logger.info(f"Tiingo News chunk {i//chunk_size + 1}: {inserted} records inserted")

            # Brief pause for Tiingo rate limits
            await asyncio.sleep(2)

        logger.info(f"Tiingo News COMPLETE: {total_records} total records")
        return total_records

async def main():
    parser = argparse.ArgumentParser(description="Turbo news backfill")
    parser.add_argument('--start_date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--environment', default='dev', choices=['test', 'intg', 'prod', 'dev'])
    parser.add_argument('--gin_config', default='config/app_dev.gin', help='Gin config file')
    parser.add_argument('--limit', type=int, help='Limit number of symbols (for testing)')
    args = parser.parse_args()

    # Setup environment
    from core.platform.config_env.environment import Environment
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

    # Get symbols from existing instruments
    from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)

    symbols = []
    all_symbols = await xrefs_dao.get_all_symbols()

    if args.limit:
        all_symbols = all_symbols[:args.limit]

    logger.info(f"Processing {len(all_symbols)} symbols for news...")

    # Convert symbols to ensure they're strings
    symbols = [str(symbol) for symbol in all_symbols]

    # Initialize database inserter
    async with TurboNewsDatabaseInserter(db_config, pool_size=20) as db_inserter:
        start_time = time.time()

        # Process both vendors concurrently
        polygon_task = process_polygon_news(symbols, args.start_date, args.end_date, polygon_api_key, db_inserter)
        tiingo_task = process_tiingo_news(symbols, args.start_date, args.end_date, tiingo_api_key, db_inserter)

        polygon_total, tiingo_total = await asyncio.gather(polygon_task, tiingo_task)

        elapsed = time.time() - start_time
        total_records = polygon_total + tiingo_total

        logger.info(f"🚀 TURBO NEWS BACKFILL COMPLETE!")
        logger.info(f"⏱️  Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        logger.info(f"📰 Records: {total_records:,} total ({polygon_total:,} Polygon + {tiingo_total:,} Tiingo)")
        logger.info(f"🔥 Rate: {total_records/elapsed:.0f} records/second")

if __name__ == "__main__":
    asyncio.run(main())