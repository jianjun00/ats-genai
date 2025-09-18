#!/usr/bin/env python3
"""
News Frontfill Job.
Continuously updates news data every 5 minutes from multiple sources.
"""

import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
import hashlib

from frontfill.base_frontfill_job import BaseFrontfillJob, FrontfillConfig, CheckpointType
from core.platform.config.environment import Environment
import asyncpg

logger = logging.getLogger(__name__)


class NewsFrontfillJob(BaseFrontfillJob):
    """Frontfill job for news data."""

    def __init__(self, config: FrontfillConfig, connection_pool: asyncpg.Pool,
                 env: Environment, api_key: str):
        super().__init__(config, connection_pool, env)
        self.api_key = api_key
        self.table_name = env.get_table_name("news")

        # Vendor-specific configurations
        if config.vendor.lower() == "polygon":
            self.base_url = "https://api.polygon.io/v2/reference/news"
        elif config.vendor.lower() == "tiingo":
            self.base_url = "https://api.tiingo.com/tiingo/news"
        elif config.vendor.lower() == "finnhub":
            self.base_url = "https://finnhub.io/api/v1/news"
        else:
            raise ValueError(f"Unsupported vendor: {config.vendor}")

    async def get_default_starting_checkpoint(self) -> str:
        """Get default starting checkpoint - 5 minutes ago."""
        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        return five_minutes_ago.isoformat()

    async def fetch_data_batch(self, checkpoint: str, batch_size: int) -> Tuple[List[Dict[str, Any]], str]:
        """Fetch news articles since the checkpoint."""
        try:
            # Parse checkpoint as datetime
            checkpoint_dt = datetime.fromisoformat(checkpoint.replace('Z', '+00:00'))

            # Fetch news from the last 5 minutes
            end_time = datetime.now()

            news_data = await self._fetch_news_articles(checkpoint_dt, end_time, batch_size)

            # Next checkpoint is current time
            next_checkpoint = end_time.isoformat()

            return news_data, next_checkpoint

        except Exception as e:
            logger.error(f"Error fetching news batch: {e}")
            raise

    async def _fetch_news_articles(self, start_time: datetime, end_time: datetime,
                                 limit: int) -> List[Dict[str, Any]]:
        """Fetch news articles from the API."""
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            if self.config.vendor.lower() == "polygon":
                return await self._fetch_polygon_news(session, start_time, end_time, limit)
            elif self.config.vendor.lower() == "tiingo":
                return await self._fetch_tiingo_news(session, start_time, end_time, limit)
            elif self.config.vendor.lower() == "finnhub":
                return await self._fetch_finnhub_news(session, start_time, end_time, limit)
            else:
                return []

    async def _fetch_polygon_news(self, session: aiohttp.ClientSession,
                                start_time: datetime, end_time: datetime,
                                limit: int) -> List[Dict[str, Any]]:
        """Fetch news from Polygon API."""
        params = {
            "apikey": self.api_key,
            "published_utc.gte": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "published_utc.lt": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "limit": min(limit, 1000),  # Polygon's max limit
            "sort": "published_utc"
        }

        try:
            async with session.get(self.base_url, params=params) as response:
                if response.status == 429:
                    await self.handle_rate_limit("Polygon", 12)
                    return []
                elif response.status != 200:
                    logger.warning(f"Polygon news API error: {response.status}")
                    return []

                data = await response.json()
                results = data.get("results", [])

                news_articles = []
                for article in results:
                    processed_article = self._process_polygon_article(article)
                    if processed_article:
                        news_articles.append(processed_article)

                logger.info(f"Fetched {len(news_articles)} articles from Polygon")
                return news_articles

        except Exception as e:
            logger.error(f"Error fetching Polygon news: {e}")
            return []

    async def _fetch_tiingo_news(self, session: aiohttp.ClientSession,
                               start_time: datetime, end_time: datetime,
                               limit: int) -> List[Dict[str, Any]]:
        """Fetch news from Tiingo API."""
        params = {
            "token": self.api_key,
            "startDate": start_time.strftime("%Y-%m-%d"),
            "endDate": end_time.strftime("%Y-%m-%d"),
            "limit": min(limit, 1000)
        }

        try:
            async with session.get(self.base_url, params=params) as response:
                if response.status == 429:
                    await self.handle_rate_limit("Tiingo", 5)
                    return []
                elif response.status != 200:
                    logger.warning(f"Tiingo news API error: {response.status}")
                    return []

                data = await response.json()

                news_articles = []
                for article in data:
                    processed_article = self._process_tiingo_article(article)
                    if processed_article:
                        news_articles.append(processed_article)

                logger.info(f"Fetched {len(news_articles)} articles from Tiingo")
                return news_articles

        except Exception as e:
            logger.error(f"Error fetching Tiingo news: {e}")
            return []

    async def _fetch_finnhub_news(self, session: aiohttp.ClientSession,
                                start_time: datetime, end_time: datetime,
                                limit: int) -> List[Dict[str, Any]]:
        """Fetch news from Finnhub API."""
        params = {
            "token": self.api_key,
            "category": "general",
            "minId": 0  # Can be used for pagination
        }

        try:
            async with session.get(self.base_url, params=params) as response:
                if response.status == 429:
                    await self.handle_rate_limit("Finnhub", 60)
                    return []
                elif response.status != 200:
                    logger.warning(f"Finnhub news API error: {response.status}")
                    return []

                data = await response.json()

                news_articles = []
                for article in data:
                    # Filter by time range
                    article_time = datetime.fromtimestamp(article.get("datetime", 0))
                    if start_time <= article_time <= end_time:
                        processed_article = self._process_finnhub_article(article)
                        if processed_article:
                            news_articles.append(processed_article)

                logger.info(f"Fetched {len(news_articles)} articles from Finnhub")
                return news_articles[:limit]  # Limit results

        except Exception as e:
            logger.error(f"Error fetching Finnhub news: {e}")
            return []

    def _process_polygon_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a Polygon news article."""
        try:
            # Generate unique ID from URL or content hash
            article_id = article.get("id") or self._generate_article_id(article)

            return {
                "article_id": article_id,
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "content": article.get("article_url", ""),  # Polygon doesn't provide full content
                "url": article.get("article_url"),
                "author": article.get("author"),
                "published_date": self._parse_polygon_date(article.get("published_utc")),
                "source": article.get("publisher", {}).get("name", ""),
                "tickers": [ticker.get("ticker") for ticker in article.get("tickers", [])],
                "keywords": article.get("keywords", []),
                "vendor": "polygon",
                "raw_data": article
            }
        except Exception as e:
            logger.warning(f"Error processing Polygon article: {e}")
            return None

    def _process_tiingo_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a Tiingo news article."""
        try:
            article_id = article.get("id") or self._generate_article_id(article)

            return {
                "article_id": article_id,
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "content": article.get("content", ""),
                "url": article.get("url"),
                "author": None,  # Tiingo doesn't provide author
                "published_date": self._parse_tiingo_date(article.get("publishedDate")),
                "source": article.get("source", ""),
                "tickers": article.get("tickers", []),
                "keywords": article.get("tags", []),
                "vendor": "tiingo",
                "raw_data": article
            }
        except Exception as e:
            logger.warning(f"Error processing Tiingo article: {e}")
            return None

    def _process_finnhub_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a Finnhub news article."""
        try:
            article_id = str(article.get("id", self._generate_article_id(article)))

            return {
                "article_id": article_id,
                "title": article.get("headline", ""),
                "description": article.get("summary", ""),
                "content": "",  # Finnhub doesn't provide full content
                "url": article.get("url"),
                "author": None,
                "published_date": datetime.fromtimestamp(article.get("datetime", 0)),
                "source": article.get("source", ""),
                "tickers": [article.get("symbol")] if article.get("symbol") else [],
                "keywords": [],
                "vendor": "finnhub",
                "raw_data": article
            }
        except Exception as e:
            logger.warning(f"Error processing Finnhub article: {e}")
            return None

    def _generate_article_id(self, article: Dict[str, Any]) -> str:
        """Generate a unique ID for an article based on its content."""
        content = f"{article.get('title', '')}{article.get('url', '')}{article.get('publishedDate', '')}"
        return hashlib.md5(content.encode()).hexdigest()

    def _parse_polygon_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse Polygon date format."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None

    def _parse_tiingo_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse Tiingo date format."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None

    async def process_data_batch(self, batch_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Process and store news articles."""
        if not batch_data:
            return 0, 0

        inserted_count = 0
        updated_count = 0

        try:
            async with self.pool.acquire() as conn:
                # Check if news table exists, create if not
                await self._ensure_news_table_exists(conn)

                # Prepare records for insertion
                news_records = []
                for article in batch_data:
                    news_records.append((
                        article["article_id"],
                        article["title"],
                        article["description"],
                        article["content"],
                        article["url"],
                        article["author"],
                        article["published_date"],
                        article["source"],
                        article["tickers"],
                        article["keywords"],
                        article["vendor"],
                        article["raw_data"]
                    ))

                # Insert with conflict resolution
                await conn.executemany(f"""
                    INSERT INTO {self.table_name}
                    (article_id, title, description, content, url, author,
                     published_date, source, tickers, keywords, vendor, raw_data)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (article_id, vendor) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    content = EXCLUDED.content,
                    updated_at = CURRENT_TIMESTAMP
                """, news_records)

                inserted_count = len(news_records)
                logger.info(f"Processed {inserted_count} news articles for {self.config.vendor}")

        except Exception as e:
            logger.error(f"Error processing news batch: {e}")
            raise

        return inserted_count, updated_count

    async def _ensure_news_table_exists(self, conn: asyncpg.Connection):
        """Ensure news table exists."""
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

        # Create indexes
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_news_published_date
            ON {self.table_name}(published_date DESC)
        """)

        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_news_vendor
            ON {self.table_name}(vendor)
        """)


# Factory function to create news frontfill jobs
async def create_news_frontfill_jobs(connection_pool: asyncpg.Pool,
                                   env: Environment,
                                   polygon_api_key: str,
                                   tiingo_api_key: str,
                                   finnhub_api_key: Optional[str] = None) -> List[NewsFrontfillJob]:
    """Create news frontfill jobs for available vendors."""
    jobs = []

    # Polygon news job
    polygon_config = FrontfillConfig(
        job_name="news_polygon_frontfill",
        job_type="news",
        vendor="polygon",
        checkpoint_type=CheckpointType.TIMESTAMP,
        batch_size=100,
        rate_limit_delay=0.1,  # 100ms between API calls
        duplicate_check_hours=12  # Check last 12 hours for duplicates
    )

    polygon_job = NewsFrontfillJob(polygon_config, connection_pool, env, polygon_api_key)
    jobs.append(polygon_job)

    # Tiingo news job
    tiingo_config = FrontfillConfig(
        job_name="news_tiingo_frontfill",
        job_type="news",
        vendor="tiingo",
        checkpoint_type=CheckpointType.TIMESTAMP,
        batch_size=50,
        rate_limit_delay=0.5,  # 500ms between API calls
        duplicate_check_hours=12
    )

    tiingo_job = NewsFrontfillJob(tiingo_config, connection_pool, env, tiingo_api_key)
    jobs.append(tiingo_job)

    # Finnhub news job (if API key provided)
    if finnhub_api_key:
        finnhub_config = FrontfillConfig(
            job_name="news_finnhub_frontfill",
            job_type="news",
            vendor="finnhub",
            checkpoint_type=CheckpointType.TIMESTAMP,
            batch_size=50,
            rate_limit_delay=1.0,  # 1 second between API calls (free tier limit)
            duplicate_check_hours=12
        )

        finnhub_job = NewsFrontfillJob(finnhub_config, connection_pool, env, finnhub_api_key)
        jobs.append(finnhub_job)

    return jobs