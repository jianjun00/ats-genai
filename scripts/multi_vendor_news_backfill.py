#!/usr/bin/env python3
"""
Multi-Vendor News Backfill System for ATS-INTG

Comprehensive news events backfill system supporting:
- Tiingo: Financial news and analysis
- Polygon: Market news and events
- EODHD: Economic events and financial news

Features:
- 30-day historical backfill capability
- Rate-limited API calls respecting vendor limits
- Sentiment analysis and keyword extraction
- Deduplication and conflict resolution
- Comprehensive monitoring and metrics
- Slack notifications for critical issues

Usage:
    python3 scripts/multi_vendor_news_backfill.py --days 30 --vendors tiingo,polygon,eodhd
    python3 scripts/multi_vendor_news_backfill.py --vendors tiingo --symbols AAPL,TSLA --debug
    python3 scripts/multi_vendor_news_backfill.py --start-date 2025-08-10 --end-date 2025-09-09
"""

import asyncio
import asyncpg
import aiohttp
import json
import logging
import os
import re
import sys
from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, asdict
import argparse
import time
import hashlib
from urllib.parse import urlencode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class NewsArticle:
    """Standardized news article structure."""
    article_id: str
    title: str
    summary: str
    content: str
    published_utc: datetime
    author: Optional[str]
    article_url: str
    image_url: Optional[str]
    vendor: str
    tickers: List[str]
    sentiment_score: Optional[float]
    keywords: List[str]
    category: Optional[str]

@dataclass
class VendorNewsConfig:
    """Configuration for news vendor APIs."""
    name: str
    api_key_env: str
    base_url: str
    rate_limit_seconds: float
    requests_per_minute: int
    max_articles_per_request: int
    supports_symbols_filter: bool
    supports_date_range: bool

@dataclass
class NewsBackfillResult:
    """Results from news backfill operation."""
    vendor: str
    articles_fetched: int
    articles_stored: int
    articles_updated: int
    articles_skipped: int
    api_calls_made: int
    execution_time_seconds: float
    errors: List[str]
    date_range_start: date
    date_range_end: date

class NewsVendorCollector:
    """Base class for vendor-specific news collection."""

    def __init__(self, config: VendorNewsConfig):
        self.config = config
        self.api_key = os.getenv(config.api_key_env)
        if not self.api_key:
            raise ValueError(f"API key not found: {config.api_key_env}")

        self.session = None
        self.db_conn = None
        self.articles_cache = set()  # For deduplication

    async def initialize(self, db_conn):
        """Initialize the collector."""
        self.db_conn = db_conn
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'User-Agent': 'ATS-INTG-News-Collector/1.0'}
        )

    async def cleanup(self):
        """Clean up resources."""
        if self.session:
            await self.session.close()

    def generate_article_id(self, vendor: str, title: str, published_utc: datetime) -> str:
        """Generate unique article ID for deduplication."""
        content = f"{vendor}:{title}:{published_utc.isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()

    def extract_tickers(self, text: str) -> List[str]:
        """Extract ticker symbols from text."""
        # Match common ticker patterns like $AAPL, AAPL, (AAPL)
        ticker_patterns = [
            r'\$([A-Z]{1,5})',  # $AAPL
            r'\b([A-Z]{2,5})\b',  # AAPL (standalone)
            r'\(([A-Z]{1,5})\)',  # (AAPL)
        ]

        tickers = set()
        for pattern in ticker_patterns:
            matches = re.findall(pattern, text.upper())
            tickers.update(matches)

        # Filter out common false positives
        common_words = {'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HAD', 'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'DAY', 'GET', 'HAS', 'HIM', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW', 'OLD', 'SEE', 'TWO', 'WAY', 'WHO', 'BOY', 'DID', 'GOT', 'LET', 'MAN', 'PUT', 'SAY', 'SHE', 'TOO', 'USE'}
        tickers = [t for t in tickers if len(t) <= 5 and t not in common_words]

        return list(tickers)[:10]  # Limit to 10 tickers per article

    def calculate_sentiment(self, title: str, summary: str) -> float:
        """Simple sentiment analysis based on keywords."""
        positive_words = {'positive', 'growth', 'profit', 'gain', 'surge', 'rally', 'bull', 'rise', 'up', 'strong', 'beat', 'exceed'}
        negative_words = {'negative', 'loss', 'decline', 'fall', 'crash', 'bear', 'down', 'weak', 'miss', 'fail', 'drop'}

        text = f"{title} {summary}".lower()
        positive_count = sum(1 for word in positive_words if word in text)
        negative_count = sum(1 for word in negative_words if word in text)

        if positive_count + negative_count == 0:
            return 0.0  # Neutral

        return (positive_count - negative_count) / (positive_count + negative_count)

    async def store_article(self, article: NewsArticle) -> Tuple[bool, str]:
        """Store article in database."""
        try:
            # Check if article already exists
            existing = await self.db_conn.fetchrow(
                "SELECT id FROM intg_realtime_news WHERE article_id = $1",
                article.article_id
            )

            if existing:
                # Update existing article
                await self.db_conn.execute("""
                    UPDATE intg_realtime_news
                    SET title = $2, summary = $3, content = $4, author = $5,
                        article_url = $6, image_url = $7, tickers = $8,
                        sentiment_score = $9, keywords = $10, category = $11,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE article_id = $1
                """, article.article_id, article.title, article.summary, article.content,
                    article.author, article.article_url, article.image_url, article.tickers,
                    article.sentiment_score, article.keywords, article.category)
                return True, "updated"
            else:
                # Insert new article
                await self.db_conn.execute("""
                    INSERT INTO intg_realtime_news
                    (article_id, title, summary, content, published_utc, author, article_url,
                     image_url, vendor, tickers, sentiment_score, keywords, category, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, article.article_id, article.title, article.summary, article.content,
                    article.published_utc, article.author, article.article_url, article.image_url,
                    article.vendor, article.tickers, article.sentiment_score, article.keywords, article.category)
                return True, "inserted"

        except Exception as e:
            logger.warning(f"Error storing article {article.article_id}: {e}")
            return False, f"error: {e}"

class TiingoNewsCollector(NewsVendorCollector):
    """Tiingo news collector."""

    async def fetch_news(self, start_date: date, end_date: date, symbols: Optional[List[str]] = None) -> List[NewsArticle]:
        """Fetch news from Tiingo API."""
        articles = []

        # Tiingo news endpoint
        url = f"https://api.tiingo.com/tiingo/news"

        params = {
            'token': self.api_key,
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'limit': 1000  # Maximum articles per request
        }

        if symbols:
            params['tickers'] = ','.join(symbols)

        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    for item in data:
                        try:
                            published_utc = datetime.fromisoformat(item['publishedDate'].replace('Z', '+00:00'))

                            article = NewsArticle(
                                article_id=self.generate_article_id('tiingo', item['title'], published_utc),
                                title=item['title'],
                                summary=item.get('description', '')[:1000],  # Limit length
                                content=item.get('description', ''),
                                published_utc=published_utc,
                                author=item.get('source', 'Tiingo'),
                                article_url=item.get('url', ''),
                                image_url=None,
                                vendor='tiingo',
                                tickers=item.get('tickers', []),
                                sentiment_score=self.calculate_sentiment(item['title'], item.get('description', '')),
                                keywords=self.extract_keywords(item['title'] + ' ' + item.get('description', '')),
                                category='financial'
                            )

                            articles.append(article)

                        except Exception as e:
                            logger.warning(f"Error parsing Tiingo article: {e}")
                            continue

                else:
                    logger.error(f"Tiingo API error: {response.status}")

        except Exception as e:
            logger.error(f"Tiingo API request failed: {e}")

        return articles

    def extract_keywords(self, text: str) -> List[str]:
        """Extract keywords from text."""
        # Simple keyword extraction - in production would use NLP libraries
        common_financial_terms = [
            'earnings', 'revenue', 'profit', 'loss', 'merger', 'acquisition',
            'ipo', 'dividend', 'buyback', 'guidance', 'forecast', 'analyst',
            'upgrade', 'downgrade', 'target', 'price', 'stock', 'shares'
        ]

        text_lower = text.lower()
        keywords = [term for term in common_financial_terms if term in text_lower]
        return keywords[:5]  # Limit to 5 keywords

class PolygonNewsCollector(NewsVendorCollector):
    """Polygon news collector."""

    async def fetch_news(self, start_date: date, end_date: date, symbols: Optional[List[str]] = None) -> List[NewsArticle]:
        """Fetch news from Polygon API."""
        articles = []

        # Polygon news endpoint
        base_url = "https://api.polygon.io/v2/reference/news"

        params = {
            'apikey': self.api_key,
            'published_utc.gte': start_date.strftime('%Y-%m-%d'),
            'published_utc.lt': (end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
            'limit': 1000,
            'order': 'desc'
        }

        if symbols:
            params['ticker'] = ','.join(symbols)

        try:
            async with self.session.get(base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    if data.get('status') == 'OK' and data.get('results'):
                        for item in data['results']:
                            try:
                                published_utc = datetime.fromisoformat(item['published_utc'].replace('Z', '+00:00'))

                                article = NewsArticle(
                                    article_id=self.generate_article_id('polygon', item['title'], published_utc),
                                    title=item['title'],
                                    summary=item.get('description', '')[:1000],
                                    content=item.get('description', ''),
                                    published_utc=published_utc,
                                    author=item.get('author', item.get('publisher', {}).get('name', 'Polygon')),
                                    article_url=item.get('article_url', ''),
                                    image_url=item.get('image_url'),
                                    vendor='polygon',
                                    tickers=item.get('tickers', []),
                                    sentiment_score=self.calculate_sentiment(item['title'], item.get('description', '')),
                                    keywords=self.extract_keywords(item['title'] + ' ' + item.get('description', '')),
                                    category=item.get('keywords', [{}])[0].get('keyword') if item.get('keywords') else 'market'
                                )

                                articles.append(article)

                            except Exception as e:
                                logger.warning(f"Error parsing Polygon article: {e}")
                                continue
                else:
                    logger.error(f"Polygon API error: {response.status}")

        except Exception as e:
            logger.error(f"Polygon API request failed: {e}")

        return articles

    def extract_keywords(self, text: str) -> List[str]:
        """Extract keywords from text."""
        market_terms = [
            'market', 'trading', 'volatility', 'volume', 'options', 'futures',
            'crypto', 'bitcoin', 'ethereum', 'fed', 'interest', 'rates',
            'inflation', 'gdp', 'unemployment', 'sector', 'index', 'etf'
        ]

        text_lower = text.lower()
        keywords = [term for term in market_terms if term in text_lower]
        return keywords[:5]

class EODHDNewsCollector(NewsVendorCollector):
    """EODHD news collector."""

    async def fetch_news(self, start_date: date, end_date: date, symbols: Optional[List[str]] = None) -> List[NewsArticle]:
        """Fetch news from EODHD API."""
        articles = []

        # EODHD has economic events and financial news
        url = f"https://eodhd.com/api/news"

        params = {
            'api_token': self.api_key,
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'limit': 1000,
            'fmt': 'json'
        }

        if symbols:
            params['s'] = ','.join([f"{s}.US" for s in symbols])  # EODHD uses .US format

        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    if isinstance(data, list):
                        for item in data:
                            try:
                                published_utc = datetime.fromtimestamp(item['date'], tz=timezone.utc)

                                article = NewsArticle(
                                    article_id=self.generate_article_id('eodhd', item['title'], published_utc),
                                    title=item['title'],
                                    summary=item.get('content', '')[:1000],
                                    content=item.get('content', ''),
                                    published_utc=published_utc,
                                    author=item.get('author', 'EODHD'),
                                    article_url=item.get('link', ''),
                                    image_url=None,
                                    vendor='eodhd',
                                    tickers=self.extract_tickers(item.get('title', '') + ' ' + item.get('content', '')),
                                    sentiment_score=self.calculate_sentiment(item['title'], item.get('content', '')),
                                    keywords=self.extract_keywords(item['title'] + ' ' + item.get('content', '')),
                                    category='economic'
                                )

                                articles.append(article)

                            except Exception as e:
                                logger.warning(f"Error parsing EODHD article: {e}")
                                continue
                else:
                    logger.error(f"EODHD API error: {response.status}")

        except Exception as e:
            logger.error(f"EODHD API request failed: {e}")

        return articles

    def extract_keywords(self, text: str) -> List[str]:
        """Extract keywords from text."""
        economic_terms = [
            'economy', 'economic', 'gdp', 'inflation', 'employment', 'consumer',
            'retail', 'manufacturing', 'services', 'housing', 'construction',
            'spending', 'income', 'wages', 'productivity', 'trade', 'exports'
        ]

        text_lower = text.lower()
        keywords = [term for term in economic_terms if term in text_lower]
        return keywords[:5]

class MultiVendorNewsBackfill:
    """Main coordinator for multi-vendor news backfill."""

    def __init__(self):
        self.vendor_configs = {
            'tiingo': VendorNewsConfig(
                name='tiingo',
                api_key_env='TIINGO_API_KEY',
                base_url='https://api.tiingo.com',
                rate_limit_seconds=1.0,  # 1000 requests/hour
                requests_per_minute=60,
                max_articles_per_request=1000,
                supports_symbols_filter=True,
                supports_date_range=True
            ),
            'polygon': VendorNewsConfig(
                name='polygon',
                api_key_env='POLYGON_API_KEY',
                base_url='https://api.polygon.io',
                rate_limit_seconds=12.0,  # 5 requests/minute
                requests_per_minute=5,
                max_articles_per_request=1000,
                supports_symbols_filter=True,
                supports_date_range=True
            ),
            'eodhd': VendorNewsConfig(
                name='eodhd',
                api_key_env='EODHD_API_KEY',
                base_url='https://eodhd.com',
                rate_limit_seconds=3.0,  # 20 requests/minute
                requests_per_minute=20,
                max_articles_per_request=1000,
                supports_symbols_filter=True,
                supports_date_range=True
            )
        }

        self.db_pool = None

    async def initialize(self):
        """Initialize database connection pool."""
        db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

        self.db_pool = await asyncpg.create_pool(
            db_url,
            min_size=2,
            max_size=10,
            command_timeout=300
        )

        logger.info("✅ Database connection pool initialized")

    async def cleanup(self):
        """Clean up resources."""
        if self.db_pool:
            await self.db_pool.close()

    async def backfill_vendor_news(self, vendor_name: str, start_date: date, end_date: date,
                                  symbols: Optional[List[str]] = None) -> NewsBackfillResult:
        """Backfill news from a specific vendor."""
        config = self.vendor_configs[vendor_name]
        start_time = time.time()

        result = NewsBackfillResult(
            vendor=vendor_name,
            articles_fetched=0,
            articles_stored=0,
            articles_updated=0,
            articles_skipped=0,
            api_calls_made=0,
            execution_time_seconds=0,
            errors=[],
            date_range_start=start_date,
            date_range_end=end_date
        )

        # Create collector
        if vendor_name == 'tiingo':
            collector = TiingoNewsCollector(config)
        elif vendor_name == 'polygon':
            collector = PolygonNewsCollector(config)
        elif vendor_name == 'eodhd':
            collector = EODHDNewsCollector(config)
        else:
            result.errors.append(f"Unknown vendor: {vendor_name}")
            return result

        async with self.db_pool.acquire() as conn:
            try:
                await collector.initialize(conn)

                logger.info(f"🔄 Starting {vendor_name} news backfill from {start_date} to {end_date}")

                # Fetch articles
                articles = await collector.fetch_news(start_date, end_date, symbols)
                result.articles_fetched = len(articles)
                result.api_calls_made = 1

                logger.info(f"📰 Fetched {len(articles)} articles from {vendor_name}")

                # Store articles
                for article in articles:
                    try:
                        stored, action = await collector.store_article(article)
                        if stored:
                            if action == "inserted":
                                result.articles_stored += 1
                            elif action == "updated":
                                result.articles_updated += 1
                        else:
                            result.articles_skipped += 1
                            if not action.startswith("error"):
                                result.errors.append(action)
                    except Exception as e:
                        result.errors.append(f"Store error: {e}")
                        result.articles_skipped += 1

                # Rate limiting
                await asyncio.sleep(config.rate_limit_seconds)

            except Exception as e:
                error_msg = f"{vendor_name} backfill failed: {e}"
                logger.error(error_msg)
                result.errors.append(error_msg)

            finally:
                await collector.cleanup()

        result.execution_time_seconds = time.time() - start_time
        logger.info(f"✅ {vendor_name} backfill completed: {result.articles_stored} stored, {result.articles_updated} updated")

        return result

    async def run_backfill(self, vendors: List[str], start_date: date, end_date: date,
                          symbols: Optional[List[str]] = None) -> Dict[str, NewsBackfillResult]:
        """Run backfill for specified vendors."""
        logger.info("🚀 Starting multi-vendor news backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"🎯 Vendors: {', '.join(vendors)}")
        if symbols:
            logger.info(f"📊 Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

        results = {}

        for vendor in vendors:
            if vendor not in self.vendor_configs:
                logger.error(f"❌ Unknown vendor: {vendor}")
                continue

            try:
                result = await self.backfill_vendor_news(vendor, start_date, end_date, symbols)
                results[vendor] = result

                if result.errors:
                    logger.warning(f"⚠️ {vendor} had {len(result.errors)} errors")

            except Exception as e:
                logger.error(f"❌ {vendor} backfill failed: {e}")
                results[vendor] = NewsBackfillResult(
                    vendor=vendor,
                    articles_fetched=0,
                    articles_stored=0,
                    articles_updated=0,
                    articles_skipped=0,
                    api_calls_made=0,
                    execution_time_seconds=0,
                    errors=[str(e)],
                    date_range_start=start_date,
                    date_range_end=end_date
                )

        return results

async def send_slack_notification(results: Dict[str, NewsBackfillResult]):
    """Send Slack notification with backfill results."""
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        logger.info("SLACK_WEBHOOK_URL not configured - skipping notification")
        return

    # Calculate totals
    total_fetched = sum(r.articles_fetched for r in results.values())
    total_stored = sum(r.articles_stored for r in results.values())
    total_updated = sum(r.articles_updated for r in results.values())
    total_errors = sum(len(r.errors) for r in results.values())

    # Create message
    message = f"📰 **ATS-INTG News Backfill Report**\n"
    message += f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    message += f"**Summary:**\n"
    message += f"• Articles fetched: {total_fetched}\n"
    message += f"• Articles stored: {total_stored}\n"
    message += f"• Articles updated: {total_updated}\n"
    message += f"• Total errors: {total_errors}\n\n"

    message += f"**Vendor Results:**\n"
    for vendor, result in results.items():
        status = "✅" if result.articles_fetched > 0 else "❌"
        message += f"• {status} {vendor.upper()}: {result.articles_fetched} fetched, "
        message += f"{result.articles_stored} stored, {result.articles_updated} updated"
        if result.errors:
            message += f" ({len(result.errors)} errors)"
        message += "\n"

    if total_errors > 5:
        message += f"\n⚠️ **Warning: {total_errors} errors detected - please review logs**"

    try:
        async with aiohttp.ClientSession() as session:
            payload = {"text": message}
            async with session.post(webhook_url, json=payload) as resp:
                if resp.status == 200:
                    logger.info("✅ Slack notification sent")
                else:
                    logger.error(f"❌ Failed to send Slack notification: {resp.status}")
    except Exception as e:
        logger.error(f"❌ Error sending Slack notification: {e}")

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Multi-Vendor News Backfill System')
    parser.add_argument('--vendors', type=str, default='tiingo,polygon,eodhd',
                       help='Comma-separated list of vendors (default: tiingo,polygon,eodhd)')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to backfill (default: 30)')
    parser.add_argument('--start-date', type=str,
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--symbols', type=str,
                       help='Comma-separated list of symbols to filter')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--no-slack', action='store_true',
                       help='Disable Slack notifications')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse vendors
    vendors = [v.strip().lower() for v in args.vendors.split(',')]

    # Parse date range
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=args.days)

    # Parse symbols
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]

    logger.info("="*80)
    logger.info("ATS-INTG MULTI-VENDOR NEWS BACKFILL")
    logger.info("="*80)
    logger.info(f"Vendors: {', '.join(vendors)}")
    logger.info(f"Date range: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    logger.info(f"Symbols filter: {symbols[:5] if symbols else 'None'}{'...' if symbols and len(symbols) > 5 else ''}")
    logger.info(f"Debug mode: {args.debug}")

    # Run backfill
    backfill = MultiVendorNewsBackfill()
    try:
        await backfill.initialize()
        results = await backfill.run_backfill(vendors, start_date, end_date, symbols)

        # Send notifications
        if not args.no_slack and results:
            await send_slack_notification(results)

        # Print final summary
        logger.info("\n" + "="*80)
        logger.info("NEWS BACKFILL COMPLETED")
        logger.info("="*80)

        total_fetched = 0
        total_stored = 0
        total_errors = 0

        for vendor, result in results.items():
            logger.info(f"{vendor.upper()}: {result.articles_fetched} fetched, "
                       f"{result.articles_stored} stored, {result.articles_updated} updated, "
                       f"{len(result.errors)} errors")
            total_fetched += result.articles_fetched
            total_stored += result.articles_stored
            total_errors += len(result.errors)

        logger.info(f"\nTOTALS: {total_fetched} fetched, {total_stored} stored, {total_errors} errors")

        # Exit with appropriate code
        if total_errors > 0:
            logger.warning(f"⚠️ Completed with {total_errors} errors")
            exit(1)
        else:
            logger.info("✅ News backfill completed successfully")
            exit(0)

    finally:
        await backfill.cleanup()

if __name__ == "__main__":
    asyncio.run(main())