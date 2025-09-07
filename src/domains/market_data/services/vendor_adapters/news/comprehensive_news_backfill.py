#!/usr/bin/env python3
"""
Comprehensive Multi-Vendor News Backfill System
Targets: Polygon, Tiingo, EODHD historical news as far back as possible.
Designed for maximum historical coverage with intelligent rate limiting and error handling.
"""

import asyncio
import aiohttp
import asyncpg
import logging
import json
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NewsSource:
    """Configuration for each news data source"""
    name: str
    api_key_env: str
    base_url: str
    max_concurrent: int
    rate_limit_delay: float
    max_retries: int
    historical_limit_years: int
    cost_per_request: float
    table_name: str

class ComprehensiveNewsBackfiller:
    """Multi-vendor news backfill system with intelligent coordination"""

    NEWS_SOURCES = {
        'polygon': NewsSource(
            name='polygon',
            api_key_env='POLYGON_API_KEY',
            base_url='https://api.polygon.io/v2/reference/news',
            max_concurrent=50,
            rate_limit_delay=0.1,  # 100ms between requests
            max_retries=5,
            historical_limit_years=5,  # Polygon typically has 5+ years
            cost_per_request=0.002,
            table_name='news_polygon'
        ),
        'tiingo': NewsSource(
            name='tiingo',
            api_key_env='TIINGO_API_KEY',
            base_url='https://api.tiingo.com/tiingo/news',
            max_concurrent=30,
            rate_limit_delay=0.5,  # 500ms between requests
            max_retries=5,
            historical_limit_years=7,  # Tiingo has extensive history
            cost_per_request=0.001,
            table_name='news_tiingo'
        ),
        'eodhd': NewsSource(
            name='eodhd',
            api_key_env='EODHD_API_KEY',
            base_url='https://eodhd.com/api/news',
            max_concurrent=20,
            rate_limit_delay=1.0,  # 1 second between requests (conservative)
            max_retries=5,
            historical_limit_years=10,  # EODHD claims extensive historical data
            cost_per_request=0.005,  # 5 API calls per request
            table_name='news_eodhd'
        )
    }

    def __init__(self, db_config: Dict[str, Any], symbols: List[str]):
        self.db_config = db_config
        self.symbols = symbols
        self.db_pool = None

        # Load API keys
        self.api_keys = {}
        for source_name, source_config in self.NEWS_SOURCES.items():
            api_key = os.getenv(source_config.api_key_env)
            if api_key:
                self.api_keys[source_name] = api_key
                logger.info(f"✅ {source_name.upper()} API key loaded")
            else:
                logger.warning(f"⚠️ {source_name.upper()} API key not found")

    async def __aenter__(self):
        """Initialize database connection pool"""
        self.db_pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=20,
            max_size=40,
            server_settings={'jit': 'off'}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup database connection pool"""
        if self.db_pool:
            await self.db_pool.close()

    async def create_eodhd_table_if_not_exists(self):
        """Create EODHD news table to match the existing schema pattern"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS news_eodhd (
                    id BIGSERIAL PRIMARY KEY,
                    eodhd_id VARCHAR(255) UNIQUE,
                    title TEXT NOT NULL,
                    content TEXT,
                    url TEXT,
                    published_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    tags TEXT[],
                    symbols TEXT[],
                    sentiment_score DECIMAL(7,4),
                    data JSONB NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Create performance indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_eodhd_published_date
                ON news_eodhd(published_date DESC)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_eodhd_symbols
                ON news_eodhd USING GIN(symbols)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_eodhd_tags
                ON news_eodhd USING GIN(tags)
            """)

    def get_historical_date_range(self, source_name: str) -> Tuple[date, date]:
        """Get the optimal historical date range for each source"""
        source = self.NEWS_SOURCES[source_name]
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * source.historical_limit_years)
        return start_date, end_date

    async def fetch_polygon_news(self, symbols: List[str], start_date: str, end_date: str) -> int:
        """Fetch news from Polygon API with optimized concurrency"""
        if 'polygon' not in self.api_keys:
            logger.warning("Polygon API key not available, skipping")
            return 0

        source = self.NEWS_SOURCES['polygon']
        semaphore = asyncio.Semaphore(source.max_concurrent)

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=50),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:

            async def fetch_symbol_news(symbol: str) -> List[Dict[str, Any]]:
                async with semaphore:
                    url = source.base_url
                    params = {
                        'ticker': symbol,
                        'published_utc.gte': start_date,
                        'published_utc.lte': end_date,
                        'limit': 1000,
                        'apikey': self.api_keys['polygon']
                    }

                    for attempt in range(source.max_retries):
                        try:
                            await asyncio.sleep(source.rate_limit_delay)
                            async with session.get(url, params=params) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    results = data.get('results', [])

                                    news_items = []
                                    for item in results:
                                        news_item = {
                                            'polygon_id': item.get('id'),
                                            'title': item.get('title', ''),
                                            'description': item.get('description', ''),
                                            'author': item.get('author'),
                                            'published_utc': datetime.fromisoformat(
                                                item['published_utc'].replace('Z', '+00:00')
                                            ),
                                            'article_url': item.get('article_url'),
                                            'image_url': item.get('image_url'),
                                            'publisher_name': item.get('publisher', {}).get('name'),
                                            'publisher_homepage_url': item.get('publisher', {}).get('homepage_url'),
                                            'publisher_logo_url': item.get('publisher', {}).get('logo_url'),
                                            'publisher_favicon_url': item.get('publisher', {}).get('favicon_url'),
                                            'keywords': item.get('keywords', []),
                                            'tickers': item.get('tickers', []),
                                            'insights': item.get('insights'),
                                            'data': item
                                        }
                                        news_items.append(news_item)

                                    logger.info(f"Polygon {symbol}: {len(news_items)} articles")
                                    return news_items

                                elif response.status == 429:
                                    delay = 2 ** attempt
                                    logger.warning(f"Polygon rate limit {symbol}, retry in {delay}s")
                                    await asyncio.sleep(delay)
                                    continue
                                else:
                                    logger.warning(f"Polygon API error for {symbol}: {response.status}")
                                    return []

                        except Exception as e:
                            delay = 2 ** attempt
                            logger.warning(f"Polygon error {symbol}: {e}, retry in {delay}s")
                            if attempt < source.max_retries - 1:
                                await asyncio.sleep(delay)
                            continue

                    return []

            # Process symbols in chunks
            total_inserted = 0
            chunk_size = 50

            for i in range(0, len(symbols), chunk_size):
                chunk_symbols = symbols[i:i+chunk_size]
                tasks = [fetch_symbol_news(symbol) for symbol in chunk_symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Collect all news items
                batch_data = []
                for result in results:
                    if isinstance(result, list):
                        batch_data.extend(result)

                # Bulk insert
                if batch_data:
                    inserted = await self.bulk_insert_polygon_news(batch_data)
                    total_inserted += inserted
                    logger.info(f"Polygon chunk {i//chunk_size + 1}: {inserted} records inserted")

            logger.info(f"🔶 Polygon COMPLETE: {total_inserted} total records")
            return total_inserted

    async def fetch_tiingo_news(self, symbols: List[str], start_date: str, end_date: str) -> int:
        """Fetch news from Tiingo API with optimized concurrency"""
        if 'tiingo' not in self.api_keys:
            logger.warning("Tiingo API key not available, skipping")
            return 0

        source = self.NEWS_SOURCES['tiingo']
        semaphore = asyncio.Semaphore(source.max_concurrent)

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=80, limit_per_host=30),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:

            async def fetch_symbol_news(symbol: str) -> List[Dict[str, Any]]:
                async with semaphore:
                    url = source.base_url
                    params = {
                        'tickers': symbol,
                        'startDate': start_date,
                        'endDate': end_date,
                        'token': self.api_keys['tiingo']
                    }

                    for attempt in range(source.max_retries):
                        try:
                            await asyncio.sleep(source.rate_limit_delay)
                            async with session.get(url, params=params) as response:
                                if response.status == 200:
                                    data = await response.json()

                                    news_items = []
                                    for item in data:
                                        news_item = {
                                            'tiingo_id': item.get('id'),
                                            'title': item.get('title', ''),
                                            'description': item.get('description', ''),
                                            'published_date': datetime.fromisoformat(
                                                item['publishedDate'].replace('Z', '+00:00')
                                            ),
                                            'crawl_date': datetime.fromisoformat(
                                                item['crawlDate'].replace('Z', '+00:00')
                                            ),
                                            'url': item.get('url'),
                                            'source': item.get('source'),
                                            'tags': item.get('tags', []),
                                            'tickers': item.get('tickers', []),
                                            'data': item
                                        }
                                        news_items.append(news_item)

                                    logger.info(f"Tiingo {symbol}: {len(news_items)} articles")
                                    return news_items

                                elif response.status == 429:
                                    delay = 2 ** attempt
                                    logger.warning(f"Tiingo rate limit {symbol}, retry in {delay}s")
                                    await asyncio.sleep(delay)
                                    continue
                                else:
                                    logger.warning(f"Tiingo API error for {symbol}: {response.status}")
                                    return []

                        except Exception as e:
                            delay = 2 ** attempt
                            logger.warning(f"Tiingo error {symbol}: {e}, retry in {delay}s")
                            if attempt < source.max_retries - 1:
                                await asyncio.sleep(delay)
                            continue

                    return []

            # Process symbols in chunks with longer delays for Tiingo
            total_inserted = 0
            chunk_size = 30

            for i in range(0, len(symbols), chunk_size):
                chunk_symbols = symbols[i:i+chunk_size]
                tasks = [fetch_symbol_news(symbol) for symbol in chunk_symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Collect all news items
                batch_data = []
                for result in results:
                    if isinstance(result, list):
                        batch_data.extend(result)

                # Bulk insert
                if batch_data:
                    inserted = await self.bulk_insert_tiingo_news(batch_data)
                    total_inserted += inserted
                    logger.info(f"Tiingo chunk {i//chunk_size + 1}: {inserted} records inserted")

                # Additional delay between chunks for Tiingo rate limits
                await asyncio.sleep(2)

            logger.info(f"🔷 Tiingo COMPLETE: {total_inserted} total records")
            return total_inserted

    async def fetch_eodhd_news(self, symbols: List[str], start_date: str, end_date: str) -> int:
        """Fetch news from EODHD API with conservative rate limiting"""
        if 'eodhd' not in self.api_keys:
            logger.warning("EODHD API key not available, skipping")
            return 0

        # Create table first
        await self.create_eodhd_table_if_not_exists()

        source = self.NEWS_SOURCES['eodhd']
        semaphore = asyncio.Semaphore(source.max_concurrent)

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=40, limit_per_host=20),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:

            async def fetch_symbol_news(symbol: str) -> List[Dict[str, Any]]:
                async with semaphore:
                    url = source.base_url
                    params = {
                        's': symbol,
                        'from': start_date,
                        'to': end_date,
                        'limit': 1000,
                        'api_token': self.api_keys['eodhd']
                    }

                    for attempt in range(source.max_retries):
                        try:
                            await asyncio.sleep(source.rate_limit_delay)
                            async with session.get(url, params=params) as response:
                                if response.status == 200:
                                    data = await response.json()

                                    news_items = []
                                    for item in data:
                                        # Convert EODHD timestamp to datetime
                                        published_date = None
                                        if 'date' in item:
                                            try:
                                                published_date = datetime.fromtimestamp(
                                                    int(item['date'])
                                                ).replace(tzinfo=None)
                                            except (ValueError, TypeError):
                                                published_date = datetime.now()

                                        news_item = {
                                            'eodhd_id': item.get('id') or f"{symbol}_{item.get('date', '')}",
                                            'title': item.get('title', ''),
                                            'content': item.get('content', ''),
                                            'url': item.get('link'),
                                            'published_date': published_date,
                                            'tags': item.get('tags', []),
                                            'symbols': item.get('symbols', [symbol]),
                                            'sentiment_score': item.get('sentiment'),
                                            'data': item
                                        }
                                        news_items.append(news_item)

                                    logger.info(f"EODHD {symbol}: {len(news_items)} articles")
                                    return news_items

                                elif response.status == 429:
                                    delay = 5 * (2 ** attempt)  # Longer delays for EODHD
                                    logger.warning(f"EODHD rate limit {symbol}, retry in {delay}s")
                                    await asyncio.sleep(delay)
                                    continue
                                else:
                                    logger.warning(f"EODHD API error for {symbol}: {response.status}")
                                    return []

                        except Exception as e:
                            delay = 5 * (2 ** attempt)
                            logger.warning(f"EODHD error {symbol}: {e}, retry in {delay}s")
                            if attempt < source.max_retries - 1:
                                await asyncio.sleep(delay)
                            continue

                    return []

            # Process symbols in small chunks with conservative rate limiting
            total_inserted = 0
            chunk_size = 20

            for i in range(0, len(symbols), chunk_size):
                chunk_symbols = symbols[i:i+chunk_size]
                tasks = [fetch_symbol_news(symbol) for symbol in chunk_symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Collect all news items
                batch_data = []
                for result in results:
                    if isinstance(result, list):
                        batch_data.extend(result)

                # Bulk insert
                if batch_data:
                    inserted = await self.bulk_insert_eodhd_news(batch_data)
                    total_inserted += inserted
                    logger.info(f"EODHD chunk {i//chunk_size + 1}: {inserted} records inserted")

                # Conservative delay between chunks for EODHD
                await asyncio.sleep(5)

            logger.info(f"🟦 EODHD COMPLETE: {total_inserted} total records")
            return total_inserted

    async def bulk_insert_polygon_news(self, news_items: List[Dict[str, Any]]) -> int:
        """Bulk insert Polygon news data"""
        if not news_items:
            return 0

        async with self.db_pool.acquire() as conn:
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
                    for item in news_items
                ]

                await conn.executemany("""
                    INSERT INTO news_polygon (
                        polygon_id, title, description, author, published_utc,
                        article_url, image_url, publisher_name, publisher_homepage_url,
                        publisher_logo_url, publisher_favicon_url, keywords, tickers,
                        insights, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (polygon_id) DO NOTHING
                """, records)
                return len(news_items)
            except Exception as e:
                logger.error(f"Polygon news insert error: {e}")
                return 0

    async def bulk_insert_tiingo_news(self, news_items: List[Dict[str, Any]]) -> int:
        """Bulk insert Tiingo news data"""
        if not news_items:
            return 0

        async with self.db_pool.acquire() as conn:
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
                    for item in news_items
                ]

                await conn.executemany("""
                    INSERT INTO news_tiingo (
                        tiingo_id, title, description, published_date, crawl_date,
                        url, source, tags, tickers, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (tiingo_id) DO NOTHING
                """, records)
                return len(news_items)
            except Exception as e:
                logger.error(f"Tiingo news insert error: {e}")
                return 0

    async def bulk_insert_eodhd_news(self, news_items: List[Dict[str, Any]]) -> int:
        """Bulk insert EODHD news data"""
        if not news_items:
            return 0

        async with self.db_pool.acquire() as conn:
            try:
                records = [
                    (
                        item['eodhd_id'],
                        item['title'],
                        item['content'],
                        item['url'],
                        item['published_date'],
                        item['tags'],
                        item['symbols'],
                        item['sentiment_score'],
                        json.dumps(item['data'])
                    )
                    for item in news_items
                ]

                await conn.executemany("""
                    INSERT INTO news_eodhd (
                        eodhd_id, title, content, url, published_date,
                        tags, symbols, sentiment_score, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (eodhd_id) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                """, records)
                return len(news_items)
            except Exception as e:
                logger.error(f"EODHD news insert error: {e}")
                return 0

    async def run_comprehensive_backfill(self, limit_symbols: Optional[int] = None) -> Dict[str, int]:
        """Execute comprehensive multi-vendor news backfill"""

        # Limit symbols if specified
        symbols_to_process = self.symbols[:limit_symbols] if limit_symbols else self.symbols

        logger.info(f"🚀 COMPREHENSIVE NEWS BACKFILL STARTING")
        logger.info(f"📊 Processing {len(symbols_to_process)} symbols across {len(self.api_keys)} vendors")

        start_time = time.time()
        results = {}

        # Process each vendor with their optimal date ranges
        tasks = []

        # Polygon (5-year history)
        if 'polygon' in self.api_keys:
            start_date, end_date = self.get_historical_date_range('polygon')
            task = self.fetch_polygon_news(
                symbols_to_process,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            tasks.append(('polygon', task))

        # Tiingo (7-year history)
        if 'tiingo' in self.api_keys:
            start_date, end_date = self.get_historical_date_range('tiingo')
            task = self.fetch_tiingo_news(
                symbols_to_process,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            tasks.append(('tiingo', task))

        # EODHD (10-year history)
        if 'eodhd' in self.api_keys:
            start_date, end_date = self.get_historical_date_range('eodhd')
            task = self.fetch_eodhd_news(
                symbols_to_process,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            tasks.append(('eodhd', task))

        # Execute all vendors concurrently
        if tasks:
            task_results = await asyncio.gather(*[task for _, task in tasks])
            for i, (vendor_name, _) in enumerate(tasks):
                results[vendor_name] = task_results[i]

        elapsed = time.time() - start_time
        total_records = sum(results.values())

        # Summary report
        logger.info(f"")
        logger.info(f"🎉 COMPREHENSIVE NEWS BACKFILL COMPLETE!")
        logger.info(f"⏱️  Total Time: {elapsed:.1f}s ({elapsed/60:.1f}min)")
        logger.info(f"📰 Total Records: {total_records:,}")
        logger.info(f"🔥 Overall Rate: {total_records/elapsed:.0f} records/second")
        logger.info(f"")
        logger.info("📊 Vendor Breakdown:")
        for vendor, count in results.items():
            logger.info(f"   {vendor.upper()}: {count:,} records")
        logger.info(f"")

        return results

async def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description="Comprehensive Multi-Vendor News Backfill")
    parser.add_argument('--limit', type=int, help='Limit number of symbols for testing')
    parser.add_argument('--environment', default='dev', choices=['test', 'intg', 'prod', 'dev'])
    args = parser.parse_args()

    # Database configuration
    db_config = {
        'host': os.getenv("DB_HOST", "localhost"),
        'port': int(os.getenv("DB_PORT", "5433")),
        'user': os.getenv("DB_USER", "postgres"),
        'password': os.getenv("DB_PASSWORD", "postgres"),
        'database': os.getenv("DB_NAME", "dev_db")
    }

    # Get symbols list (you'll need to implement this based on your instrument system)
    # For now, using a sample of major symbols
    major_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'BRK.A', 'V', 'JNJ',
        'WMT', 'JPM', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'BAC', 'ADBE', 'CRM',
        'NFLX', 'PYPL', 'INTC', 'CMCSA', 'KO', 'VZ', 'ABT', 'NKE', 'T', 'PFE',
        'CSCO', 'TMO', 'XOM', 'ACN', 'DHR', 'AVGO', 'LLY', 'TXN', 'NEE', 'CVX'
    ]

    if args.limit:
        major_symbols = major_symbols[:args.limit]

    # Execute backfill
    async with ComprehensiveNewsBackfiller(db_config, major_symbols) as backfiller:
        results = await backfiller.run_comprehensive_backfill(args.limit)

        # Output final results
        print(f"\n🎯 BACKFILL SUMMARY:")
        print(f"Total articles collected: {sum(results.values()):,}")
        for vendor, count in results.items():
            print(f"{vendor.upper()}: {count:,} articles")

if __name__ == "__main__":
    asyncio.run(main())