#!/usr/bin/env python3
"""
Polygon News Backfill Script
Backfills news data from Polygon.io starting from a specified date.

USAGE EXAMPLES:
===============

Basic backfill from August 25, 2025:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25

Backfill specific date range:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --end-date 2025-09-01

Backfill for specific symbols only:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --symbols AAPL,TSLA,MSFT

Limited backfill for testing:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --limit-per-request 50 --max-requests 5

Debug mode with detailed logging:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --debug

Test mode without API calls or database writes:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --dry-run

Production environment:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --environment prod

API KEY MANAGEMENT:
==================

This script uses the existing codebase API key management system. It will automatically
try to find the Polygon API key from multiple sources in the following order:

1. POLYGON_API_KEY environment variable
2. Polygon utils system (infrastructure.vendor.polygon.utils)
3. Gin configuration files (env.get_api_key('polygon'))

You do NOT need to provide the API key on the command line. The script integrates
with the existing infrastructure for API key management.

If you need to set the API key manually, use:
    export POLYGON_API_KEY="your_api_key_here"

Or configure it in the appropriate gin configuration file for your environment.

ARGUMENTS:
==========
"""

import os
import sys
import asyncio
import asyncpg
import aiohttp
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import hashlib
import json
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import gin
from shared.utils.vendor_api_keys import get_polygon_api_key
from shared.utils.database_connections import get_database_pool, get_table_name
from shared.utils.backfill_framework import BackfillStats, VendorRateLimiters

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("polygon_news_backfill")

# Using BackfillStats from shared.utils.backfill_framework
# Custom wrapper for news-specific metrics
class NewsBackfillStats(BackfillStats):
    """News-specific backfill statistics"""

    @property
    def total_articles_fetched(self) -> int:
        """Alias for records_fetched"""
        return self.records_fetched

    @property
    def articles_inserted(self) -> int:
        """Alias for records_inserted"""
        return self.records_inserted

    @property
    def articles_updated(self) -> int:
        """Alias for records_updated"""
        return self.records_updated

    @property
    def articles_skipped(self) -> int:
        """Alias for records_skipped"""
        return self.records_skipped

class PolygonNewsBackfill:
    """Polygon.io News Backfill Service"""

    def __init__(self, api_key: str, pool: asyncpg.Pool, table_name: str = "dev_news"):
        self.api_key = api_key
        self.pool = pool
        self.base_url = "https://api.polygon.io/v2/reference/news"
        self.table_name = table_name
        self.stats = NewsBackfillStats()
        self.rate_limiter = VendorRateLimiters.polygon_free()

    async def backfill_news_data(self,
                                start_date: datetime,
                                end_date: Optional[datetime] = None,
                                symbols: Optional[List[str]] = None,
                                limit_per_request: int = 1000,
                                max_requests: Optional[int] = None) -> BackfillStats:
        """
        Backfill news data from Polygon.io

        Args:
            start_date: Start date for backfill
            end_date: End date for backfill (defaults to now)
            symbols: Optional list of symbols to filter by
            limit_per_request: Max articles per API request
            max_requests: Optional limit on total API requests
        """
        if not end_date:
            end_date = datetime.now()

        logger.info(f"Starting Polygon news backfill from {start_date} to {end_date}")
        if symbols:
            logger.info(f"Filtering for symbols: {symbols}")

        # Table should already exist - skip creation for existing environments
        # await self._ensure_news_table_exists()

        # Create session with timeout
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:

            if symbols:
                # Backfill for specific symbols
                for symbol in symbols:
                    await self._backfill_symbol_news(
                        session, symbol, start_date, end_date,
                        limit_per_request, max_requests
                    )
            else:
                # General news backfill (all market news)
                await self._backfill_general_news(
                    session, start_date, end_date,
                    limit_per_request, max_requests
                )

        logger.info("Backfill completed!")
        self.stats.log_progress(logger)
        return self.stats

    async def _backfill_symbol_news(self,
                                   session: aiohttp.ClientSession,
                                   symbol: str,
                                   start_date: datetime,
                                   end_date: datetime,
                                   limit_per_request: int,
                                   max_requests: Optional[int]):
        """Backfill news for a specific symbol"""
        logger.info(f"Backfilling news for symbol: {symbol}")

        # Polygon API parameters for symbol-specific news
        params = {
            "ticker": symbol,
            "published_utc.gte": start_date.strftime("%Y-%m-%d"),
            "published_utc.lt": end_date.strftime("%Y-%m-%d"),
            "limit": min(limit_per_request, 1000),
            "sort": "published_utc",
            "apikey": self.api_key
        }

        await self._fetch_paginated_news(session, params, max_requests, f"symbol {symbol}")

    async def _backfill_general_news(self,
                                    session: aiohttp.ClientSession,
                                    start_date: datetime,
                                    end_date: datetime,
                                    limit_per_request: int,
                                    max_requests: Optional[int]):
        """Backfill general market news"""
        logger.info("Backfilling general market news")

        # Break down into daily chunks to avoid API limits
        current_date = start_date
        while current_date < end_date:
            next_date = min(current_date + timedelta(days=1), end_date)

            params = {
                "published_utc.gte": current_date.strftime("%Y-%m-%d"),
                "published_utc.lt": next_date.strftime("%Y-%m-%d"),
                "limit": min(limit_per_request, 1000),
                "sort": "published_utc",
                "apikey": self.api_key
            }

            await self._fetch_paginated_news(
                session, params, max_requests,
                f"date {current_date.strftime('%Y-%m-%d')}"
            )

            current_date = next_date

            # Respect rate limits
            await asyncio.sleep(0.1)

    async def _fetch_paginated_news(self,
                                   session: aiohttp.ClientSession,
                                   base_params: Dict[str, Any],
                                   max_requests: Optional[int],
                                   description: str):
        """Fetch paginated news data"""
        next_url = None
        request_count = 0

        while True:
            if max_requests and request_count >= max_requests:
                logger.warning(f"Reached max requests limit ({max_requests}) for {description}")
                break

            try:
                # Use next_url if available, otherwise construct URL
                if next_url:
                    url = next_url
                    # Ensure API key is in the URL
                    if "apikey=" not in url:
                        url += f"&apikey={self.api_key}"
                else:
                    url = self.base_url
                    params = base_params

                # Make API request
                if next_url:
                    async with session.get(url) as response:
                        self.stats.api_calls_made += 1
                        request_count += 1

                        if response.status == 429:
                            logger.warning("Rate limited, waiting 12 seconds...")
                            await asyncio.sleep(12)
                            continue
                        elif response.status != 200:
                            logger.error(f"API error {response.status}: {await response.text()}")
                            self.stats.api_errors += 1
                            break

                        data = await response.json()
                else:
                    async with session.get(url, params=params) as response:
                        self.stats.api_calls_made += 1
                        request_count += 1

                        if response.status == 429:
                            logger.warning("Rate limited, waiting 12 seconds...")
                            await asyncio.sleep(12)
                            continue
                        elif response.status != 200:
                            logger.error(f"API error {response.status}: {await response.text()}")
                            self.stats.api_errors += 1
                            break

                        data = await response.json()

                # Process results
                results = data.get("results", [])
                if not results:
                    logger.info(f"No more results for {description}")
                    break

                # Process and store articles
                processed_articles = []
                for article in results:
                    processed_article = self._process_polygon_article(article)
                    if processed_article:
                        processed_articles.append(processed_article)

                if processed_articles:
                    inserted, updated, skipped = await self._store_articles(processed_articles)
                    self.stats.records_fetched += len(processed_articles)
                    self.stats.records_inserted += inserted
                    self.stats.records_updated += updated
                    self.stats.records_skipped += skipped

                logger.info(f"Processed {len(processed_articles)} articles for {description}")
                self.stats.log_progress(logger)

                # Get next page URL
                next_url = data.get("next_url")
                if not next_url:
                    logger.info(f"No more pages for {description}")
                    break

                # Rate limiting using shared utility
                await self.rate_limiter.wait_if_needed()

            except Exception as e:
                logger.error(f"Error processing {description}: {e}")
                self.stats.api_errors += 1
                break

    def _process_polygon_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a Polygon news article into our standard format"""
        try:
            # Generate unique ID from URL or content hash
            article_id = article.get("id") or self._generate_article_id(article)

            # Parse publication date
            published_date = self._parse_polygon_date(article.get("published_utc"))
            if not published_date:
                return None

            # Extract tickers
            tickers = []
            for ticker_obj in article.get("tickers", []):
                if isinstance(ticker_obj, dict):
                    ticker = ticker_obj.get("ticker")
                    if ticker:
                        tickers.append(ticker)
                elif isinstance(ticker_obj, str):
                    tickers.append(ticker_obj)

            return {
                "vendor_id": article_id,
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "article_url": article.get("article_url"),
                "author": article.get("author"),
                "published_utc": published_date,
                "publisher_name": article.get("publisher", {}).get("name", "") if isinstance(article.get("publisher"), dict) else str(article.get("publisher", "")),
                "tickers": tickers,
                "keywords": article.get("keywords", []),
                "data": article  # raw data goes in 'data' column
            }
        except Exception as e:
            logger.warning(f"Error processing Polygon article: {e}")
            return None

    def _generate_article_id(self, article: Dict[str, Any]) -> str:
        """Generate a unique ID for an article based on its content"""
        content = f"{article.get('title', '')}{article.get('article_url', '')}{article.get('published_utc', '')}"
        return hashlib.md5(content.encode()).hexdigest()

    def _parse_polygon_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse Polygon date format"""
        if not date_str:
            return None
        try:
            # Handle both Z and +00:00 timezone formats
            if date_str.endswith('Z'):
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(date_str)
        except ValueError:
            try:
                # Try parsing without timezone info
                return datetime.fromisoformat(date_str.split('+')[0].split('Z')[0])
            except:
                logger.warning(f"Could not parse date: {date_str}")
                return None

    async def _store_articles(self, articles: List[Dict[str, Any]]) -> tuple[int, int, int]:
        """Store articles in database, returning (inserted, updated, skipped) counts"""
        if not articles:
            return 0, 0, 0

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        try:
            async with self.pool.acquire() as conn:
                # Get initial count for validation
                initial_count = await conn.fetchval(f"SELECT COUNT(*) FROM {self.table_name}")
                
                # Prepare records for insertion
                news_records = []
                article_ids_to_check = []
                for article in articles:
                    news_records.append((
                        article["vendor_id"],
                        article["title"],
                        article["description"],
                        article["article_url"],
                        article["author"],
                        article["published_utc"],
                        article["publisher_name"],
                        article["tickers"],
                        article["keywords"],
                        json.dumps(article["data"])
                    ))
                    article_ids_to_check.append(article["vendor_id"])

                # Check which articles already exist for accurate counting
                existing_articles = await conn.fetch(f"""
                    SELECT vendor_id FROM {self.table_name} 
                    WHERE vendor_id = ANY($1)
                """, article_ids_to_check)
                
                existing_ids = {row['vendor_id'] for row in existing_articles}
                expected_new_inserts = len(article_ids_to_check) - len(existing_ids)

                # Use ON CONFLICT to handle duplicates (vendor_id is unique)
                await conn.executemany(f"""
                    INSERT INTO {self.table_name}
                    (vendor_id, title, description, article_url, author,
                     published_utc, publisher_name, tickers, keywords, data)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (vendor_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    article_url = EXCLUDED.article_url,
                    author = EXCLUDED.author,
                    published_utc = EXCLUDED.published_utc,
                    publisher_name = EXCLUDED.publisher_name,
                    tickers = EXCLUDED.tickers,
                    keywords = EXCLUDED.keywords,
                    data = EXCLUDED.data::jsonb,
                    updated_at = CURRENT_TIMESTAMP
                    WHERE {self.table_name}.title IS DISTINCT FROM EXCLUDED.title
                    OR {self.table_name}.description IS DISTINCT FROM EXCLUDED.description
                """, news_records)

                # Validate actual database changes
                final_count = await conn.fetchval(f"SELECT COUNT(*) FROM {self.table_name}")
                actual_inserted = final_count - initial_count
                
                # Accurate counting based on actual database changes
                inserted_count = actual_inserted
                updated_count = len(existing_ids)
                skipped_count = len(news_records) - inserted_count - updated_count
                
                # Log validation info for debugging
                if actual_inserted != expected_new_inserts:
                    logger.warning(f"Insert count mismatch: expected {expected_new_inserts}, actual {actual_inserted}")
                    logger.debug(f"Articles processed: {len(news_records)}, existing: {len(existing_ids)}, final DB change: {actual_inserted}")

        except Exception as e:
            logger.error(f"Error storing articles: {e}")
            raise

        return inserted_count, updated_count, skipped_count

    async def _ensure_news_table_exists(self):
        """Ensure the news table exists with proper schema"""
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id SERIAL PRIMARY KEY,
                    article_id VARCHAR(255) NOT NULL,
                    title TEXT,
                    description TEXT,
                    content TEXT,
                    url TEXT,
                    author VARCHAR(255),
                    published_date TIMESTAMP WITH TIME ZONE,
                    source VARCHAR(255),
                    tickers TEXT[],
                    keywords TEXT[],
                    vendor VARCHAR(100) NOT NULL,
                    raw_data JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE(article_id, vendor)
                )
            """)

            # Create indexes for performance
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_news_published_date
                ON {self.table_name}(published_date DESC)
            """)

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_news_vendor
                ON {self.table_name}(vendor)
            """)

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_news_tickers
                ON {self.table_name} USING GIN(tickers)
            """)

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_news_source
                ON {self.table_name}(source)
            """)

async def main():
    parser = argparse.ArgumentParser(
        description="Backfill news data from Polygon.io",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
USAGE EXAMPLES:

Basic backfill from August 25, 2025:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25

Backfill specific date range:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --end-date 2025-09-01

Backfill for specific symbols only:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --symbols AAPL,TSLA,MSFT

Limited backfill for testing:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --limit-per-request 50 --max-requests 5

Debug mode with detailed logging:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --debug

Test mode without API calls or database writes:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --dry-run

Production environment:
    python3 scripts/polygon_news_backfill.py --start-date 2025-08-25 --environment prod

API KEY MANAGEMENT:

This script automatically finds the Polygon API key from:
1. POLYGON_API_KEY environment variable
2. Polygon utils system (infrastructure.vendor.polygon.utils)
3. Gin configuration files (env.get_api_key('polygon'))

No need to provide API key on command line - it uses existing codebase infrastructure.
        """
    )

    parser.add_argument('--start-date', type=str, default='2025-08-25',
                       help='Start date for backfill (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date for backfill (YYYY-MM-DD), defaults to now')
    parser.add_argument('--symbols', type=str, default=None,
                       help='Comma-separated symbols to filter by (optional)')
    parser.add_argument('--environment', type=str, default='dev',
                       choices=['dev', 'test', 'intg', 'prod'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--limit-per-request', type=int, default=1000,
                       help='Maximum articles per API request (default: 1000)')
    parser.add_argument('--max-requests', type=int, default=None,
                       help='Maximum number of API requests (optional, no limit by default)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--dry-run', action='store_true',
                       help='Test mode: simulate operations without API calls or database writes')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return 1

    # Parse symbols
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]

    # Setup environment
    os.environ["ENVIRONMENT"] = args.environment

    # Load gin config if available
    gin_config_map = {
        'test': 'config/app_test.gin',
        'intg': 'config/app_intg.gin',
        'prod': 'config/app_prod.gin',
        'dev': 'config/app_dev.gin',
    }

    gin_config_path = gin_config_map.get(args.environment)
    if gin_config_path and os.path.exists(gin_config_path):
        try:
            gin.parse_config_file(gin_config_path)
            logger.info(f"Loaded gin config: {gin_config_path}")
        except Exception as e:
            logger.warning(f"Failed to load gin config: {e}")

    # API Key Management - Uses shared utility
    polygon_api_key = get_polygon_api_key(required=not args.dry_run)

    if not polygon_api_key and args.dry_run:
        logger.warning("POLYGON_API_KEY not available, but running in dry-run mode")
        polygon_api_key = "test-key"
    elif not polygon_api_key:
        logger.error("No Polygon API key found. Use shared.utils.vendor_api_keys for details.")
        return 1

    try:
        # Create database connection pool using shared utility
        logger.info("Creating database connection pool...")
        pool = await get_database_pool(args.environment, max_retries=3, timeout=10.0)
        table_name = get_table_name("news_polygon", args.environment)

        try:
            # Create backfill service
            backfill_service = PolygonNewsBackfill(polygon_api_key, pool, table_name)

            if args.dry_run:
                logger.info("DRY RUN MODE - No data will be stored")
                # In dry run, mock API calls and return test results
                mock_stats = NewsBackfillStats()
                mock_stats.records_fetched = 150
                mock_stats.records_inserted = 150
                mock_stats.api_calls_made = 2
                mock_stats.add_custom_metric("symbols_processed", ["AAPL", "TSLA", "MSFT"])
                logger.info("DRY RUN: Simulated fetching news articles for test")
                stats = mock_stats
            else:
                # Run backfill
                stats = await backfill_service.backfill_news_data(
                    start_date=start_date,
                    end_date=end_date,
                    symbols=symbols,
                    limit_per_request=args.limit_per_request,
                    max_requests=args.max_requests
                )

            logger.info("Backfill completed successfully!")
            stats.log_final_summary(logger)

            return 0

        finally:
            await pool.close()

    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)