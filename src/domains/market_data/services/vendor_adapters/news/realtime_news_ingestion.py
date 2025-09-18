#!/usr/bin/env python3
"""
Real-Time News Ingestion Pipeline

This service provides real-time news streaming with immediate LLM processing
for critical signal extraction. It integrates with existing frontfill infrastructure
but adds streaming capabilities with sub-30 second processing latency.

Key Features:
- Multi-vendor real-time streaming (Polygon, Tiingo, Alpha Vantage, FMP, Benzinga)
- Content-based deduplication across vendors
- Priority-based processing (market hours vs after-hours)
- Immediate LLM trigger for high-impact news
- Circuit breaker pattern for vendor API failures
- Structured logging with performance metrics
"""

import asyncio
import aiohttp
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import time
from collections import defaultdict
import re

import asyncpg
from src.core.platform.config.environment import Environment
from src.domains.market_data.services.llm.enhanced_news_llm_processor import EnhancedLLMNewsProcessor, NewsAnalysisResult
from src.domains.market_data.services.llm.news_llm_processor import NewsArticle
from src.infrastructure.llm.multi_provider_client import MultiProviderLLMClient

logger = logging.getLogger(__name__)


class NewsUrgency(Enum):
    """News urgency levels for processing priority."""
    CRITICAL = "critical"  # Earnings, M&A, FDA approvals
    HIGH = "high"         # Analyst ratings, insider trading
    MEDIUM = "medium"     # General company news
    LOW = "low"           # Market commentary, general news


class MarketSession(Enum):
    """Market session types for processing prioritization."""
    PRE_MARKET = "pre_market"      # 4:00-9:30 AM ET
    MARKET_HOURS = "market_hours"  # 9:30-4:00 PM ET
    AFTER_HOURS = "after_hours"    # 4:00-8:00 PM ET
    CLOSED = "closed"              # 8:00 PM - 4:00 AM ET


@dataclass
class NewsVendorConfig:
    """Configuration for a news vendor."""
    name: str
    api_key: str
    base_url: str
    rate_limit_rpm: int = 60  # Requests per minute
    priority_weight: float = 1.0  # Higher = more trusted
    enabled: bool = True

    # API-specific settings
    batch_size: int = 100
    timeout_seconds: int = 30
    retry_count: int = 3
    retry_backoff: float = 2.0


@dataclass
class ProcessedNewsArticle:
    """Standardized news article structure for internal processing."""
    article_id: str
    vendor: str
    title: str
    content: str
    summary: str
    url: str
    author: Optional[str]
    published_date: datetime
    discovered_date: datetime

    # Classification
    urgency: NewsUrgency
    market_session: MarketSession
    tickers: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)

    # Content analysis
    content_hash: str = ""
    similarity_score: float = 0.0

    # Processing metadata
    processing_latency_ms: int = 0
    llm_triggered: bool = False
    signal_generated: bool = False

    raw_data: Dict[str, Any] = field(default_factory=dict)


class ContentDeduplicator:
    """Advanced content deduplication using multiple techniques."""

    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.recent_articles: Dict[str, ProcessedNewsArticle] = {}
        self.content_hashes: Set[str] = set()
        self.title_hashes: Set[str] = set()

        # Cleanup old articles every hour
        self.last_cleanup = datetime.now()
        self.cleanup_interval = timedelta(hours=1)

    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison."""
        # Remove extra whitespace, normalize case
        text = re.sub(r'\s+', ' ', text.lower().strip())
        # Remove common stopwords and punctuation
        text = re.sub(r'[^\w\s]', '', text)
        return text

    def _generate_content_hash(self, article: ProcessedNewsArticle) -> str:
        """Generate hash for content deduplication."""
        # Use normalized title + first 200 chars of content
        normalized_title = self._normalize_text(article.title)
        normalized_content = self._normalize_text(article.content[:200])
        combined = f"{normalized_title}|{normalized_content}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def _calculate_similarity(self, article1: ProcessedNewsArticle,
                            article2: ProcessedNewsArticle) -> float:
        """Calculate content similarity between two articles."""
        # Simple Jaccard similarity on normalized words
        words1 = set(self._normalize_text(f"{article1.title} {article1.content}").split())
        words2 = set(self._normalize_text(f"{article2.title} {article2.content}").split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    def is_duplicate(self, article: ProcessedNewsArticle) -> Tuple[bool, Optional[str]]:
        """Check if article is a duplicate. Returns (is_duplicate, reason)."""

        # Generate content hash
        content_hash = self._generate_content_hash(article)
        article.content_hash = content_hash

        # Exact content match
        if content_hash in self.content_hashes:
            return True, "exact_content_match"

        # Title similarity check
        title_hash = hashlib.sha256(self._normalize_text(article.title).encode()).hexdigest()[:16]
        if title_hash in self.title_hashes:
            return True, "exact_title_match"

        # Content similarity check against recent articles
        for existing_hash, existing_article in self.recent_articles.items():
            # Only compare articles from last 6 hours
            if (article.discovered_date - existing_article.discovered_date).total_seconds() > 6 * 3600:
                continue

            similarity = self._calculate_similarity(article, existing_article)
            article.similarity_score = max(article.similarity_score, similarity)

            if similarity >= self.similarity_threshold:
                return True, f"content_similarity_{similarity:.3f}"

        # Not a duplicate - add to tracking
        self.content_hashes.add(content_hash)
        self.title_hashes.add(title_hash)
        self.recent_articles[content_hash] = article

        # Periodic cleanup
        if (datetime.now() - self.last_cleanup) > self.cleanup_interval:
            self._cleanup_old_articles()

        return False, None

    def _cleanup_old_articles(self):
        """Remove old articles from deduplication cache."""
        cutoff = datetime.now() - timedelta(hours=12)

        # Remove old articles
        old_hashes = [
            hash_key for hash_key, article in self.recent_articles.items()
            if article.discovered_date < cutoff
        ]

        for hash_key in old_hashes:
            self.recent_articles.pop(hash_key, None)
            self.content_hashes.discard(hash_key)

        self.last_cleanup = datetime.now()
        logger.info(f"Cleaned up {len(old_hashes)} old articles from deduplication cache")


class RealTimeNewsIngestionService:
    """Real-time news ingestion service with enhanced LLM processing."""

    def __init__(self, db_pool: asyncpg.Pool, env: Environment,
                 llm_processor: EnhancedLLMNewsProcessor):
        self.db_pool = db_pool
        self.env = env
        self.llm_processor = llm_processor
        self.deduplicator = ContentDeduplicator()

        # Vendor configurations
        self.vendors: Dict[str, NewsVendorConfig] = {}
        self.vendor_sessions: Dict[str, aiohttp.ClientSession] = {}

        # Processing queues
        self.high_priority_queue = asyncio.Queue(maxsize=1000)
        self.normal_priority_queue = asyncio.Queue(maxsize=5000)

        # Performance metrics
        self.metrics = {
            'articles_processed': 0,
            'articles_deduplicated': 0,
            'llm_processing_triggered': 0,
            'signals_generated': 0,
            'avg_processing_latency_ms': 0.0,
            'vendor_errors': defaultdict(int),
            'last_processed_timestamp': datetime.now()
        }

        # Circuit breaker states
        self.circuit_breakers = defaultdict(lambda: {
            'failures': 0, 'last_failure': None, 'state': 'closed'
        })

        self._running = False
        self._tasks: List[asyncio.Task] = []

    def configure_vendor(self, config: NewsVendorConfig):
        """Configure a news vendor."""
        self.vendors[config.name] = config
        logger.info(f"Configured vendor: {config.name} (enabled: {config.enabled})")

    async def start(self):
        """Start the real-time ingestion service."""
        if self._running:
            logger.warning("Service already running")
            return

        self._running = True
        logger.info("Starting Real-Time News Ingestion Service")

        # Initialize vendor HTTP sessions
        await self._initialize_vendor_sessions()

        # Start processing tasks
        self._tasks = [
            asyncio.create_task(self._vendor_polling_loop()),
            asyncio.create_task(self._high_priority_processor()),
            asyncio.create_task(self._normal_priority_processor()),
            asyncio.create_task(self._metrics_reporter()),
            asyncio.create_task(self._health_checker())
        ]

        logger.info(f"Started {len(self._tasks)} service tasks")

    async def stop(self):
        """Stop the ingestion service."""
        if not self._running:
            return

        logger.info("Stopping Real-Time News Ingestion Service")
        self._running = False

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close vendor sessions
        for session in self.vendor_sessions.values():
            if session and not session.closed:
                await session.close()

        logger.info("Service stopped")

    async def _initialize_vendor_sessions(self):
        """Initialize HTTP sessions for all vendors."""
        for name, config in self.vendors.items():
            if not config.enabled:
                continue

            timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)

            self.vendor_sessions[name] = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'ATS-RealTime-News-Ingestion/1.0',
                    'Accept': 'application/json'
                }
            )

    def _get_market_session(self) -> MarketSession:
        """Determine current market session."""
        now = datetime.now()
        hour = now.hour
        weekday = now.weekday()

        # Weekend = always closed
        if weekday >= 5:  # Saturday/Sunday
            return MarketSession.CLOSED

        # Market hours (9:30 AM - 4:00 PM ET)
        if 9 <= hour < 16:
            return MarketSession.MARKET_HOURS
        elif 4 <= hour < 9:
            return MarketSession.PRE_MARKET
        elif 16 <= hour < 20:
            return MarketSession.AFTER_HOURS
        else:
            return MarketSession.CLOSED

    def _classify_news_urgency(self, article: ProcessedNewsArticle) -> NewsUrgency:
        """Classify news urgency based on content analysis."""
        title = article.title.lower()
        content = article.content.lower()
        combined = f"{title} {content}"

        # Critical urgency patterns
        critical_patterns = [
            r'\bearnings?\b.*\b(beat|miss|surprise)\b',
            r'\b(acquisition|merger|buyout)\b',
            r'\bfda\b.*\b(approval|rejection)\b',
            r'\b(bankruptcy|chapter 11)\b',
            r'\b(ceo|cfo|coo).*\b(resign|fired|steps down)\b',
            r'\b(halted|suspended)\b.*\btrading\b',
            r'\b(investigation|sec|doj)\b',
        ]

        # High urgency patterns
        high_patterns = [
            r'\b(upgrade|downgrade|price target)\b',
            r'\banalyst\b.*\b(rating|recommendation)\b',
            r'\binsider\b.*\b(buy|sell|trading)\b',
            r'\b(guidance|outlook)\b.*\b(raised|lowered|updated)\b',
            r'\b(dividend|split|spinoff)\b',
            r'\b(partnership|joint venture|collaboration)\b',
        ]

        # Check for critical patterns
        for pattern in critical_patterns:
            if re.search(pattern, combined):
                return NewsUrgency.CRITICAL

        # Check for high urgency patterns
        for pattern in high_patterns:
            if re.search(pattern, combined):
                return NewsUrgency.HIGH

        # If multiple tickers mentioned = medium urgency
        if len(article.tickers) > 1:
            return NewsUrgency.MEDIUM

        return NewsUrgency.LOW

    async def _vendor_polling_loop(self):
        """Main vendor polling loop."""
        while self._running:
            try:
                # Poll all enabled vendors concurrently
                tasks = []
                for name, config in self.vendors.items():
                    if config.enabled and self._can_call_vendor(name):
                        tasks.append(self._poll_vendor(name, config))

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process results
                    for i, result in enumerate(results):
                        vendor_name = list(self.vendors.keys())[i]
                        if isinstance(result, Exception):
                            self._record_vendor_error(vendor_name, result)
                        elif result:
                            await self._process_vendor_articles(vendor_name, result)

                # Wait based on fastest vendor rate limit
                min_delay = min([60 / config.rate_limit_rpm for config in self.vendors.values()
                               if config.enabled], default=10)
                await asyncio.sleep(min_delay)

            except Exception as e:
                logger.error(f"Error in vendor polling loop: {e}")
                await asyncio.sleep(30)  # Back off on errors

    def _can_call_vendor(self, vendor_name: str) -> bool:
        """Check if vendor can be called based on circuit breaker state."""
        breaker = self.circuit_breakers[vendor_name]

        if breaker['state'] == 'open':
            # Check if enough time has passed to try again
            if breaker['last_failure']:
                time_since_failure = (datetime.now() - breaker['last_failure']).total_seconds()
                if time_since_failure > 300:  # 5 minutes
                    breaker['state'] = 'half_open'
                    return True
            return False

        return True

    def _record_vendor_error(self, vendor_name: str, error: Exception):
        """Record vendor error and update circuit breaker."""
        self.metrics['vendor_errors'][vendor_name] += 1
        breaker = self.circuit_breakers[vendor_name]
        breaker['failures'] += 1
        breaker['last_failure'] = datetime.now()

        # Open circuit breaker if too many failures
        if breaker['failures'] >= 5:
            breaker['state'] = 'open'
            logger.warning(f"Circuit breaker opened for vendor {vendor_name}: {error}")
        else:
            logger.error(f"Vendor {vendor_name} error: {error}")

    async def _poll_vendor(self, vendor_name: str, config: NewsVendorConfig) -> List[Dict[str, Any]]:
        """Poll a specific vendor for new articles."""
        try:
            session = self.vendor_sessions.get(vendor_name)
            if not session:
                return []

            # Get articles from last 2 minutes to ensure we don't miss any
            since = datetime.now() - timedelta(minutes=2)

            if vendor_name == "polygon":
                return await self._fetch_polygon_news(session, config, since)
            elif vendor_name == "tiingo":
                return await self._fetch_tiingo_news(session, config, since)
            elif vendor_name == "alpha_vantage":
                return await self._fetch_alpha_vantage_news(session, config, since)
            elif vendor_name == "fmp":
                return await self._fetch_fmp_news(session, config, since)
            elif vendor_name == "benzinga":
                return await self._fetch_benzinga_news(session, config, since)
            else:
                logger.warning(f"Unknown vendor: {vendor_name}")
                return []

        except Exception as e:
            raise Exception(f"Failed to poll {vendor_name}: {str(e)}")

    async def _fetch_polygon_news(self, session: aiohttp.ClientSession,
                                config: NewsVendorConfig, since: datetime) -> List[Dict[str, Any]]:
        """Fetch news from Polygon API."""
        params = {
            "apikey": config.api_key,
            "published_utc.gte": since.strftime("%Y-%m-%dT%H:%M:%S"),
            "limit": config.batch_size,
            "sort": "published_utc"
        }

        async with session.get(f"{config.base_url}/reference/news", params=params) as response:
            if response.status == 429:
                raise Exception("Rate limit exceeded")
            elif response.status != 200:
                raise Exception(f"HTTP {response.status}")

            data = await response.json()
            return data.get("results", [])

    async def _fetch_tiingo_news(self, session: aiohttp.ClientSession,
                               config: NewsVendorConfig, since: datetime) -> List[Dict[str, Any]]:
        """Fetch news from Tiingo API."""
        params = {
            "token": config.api_key,
            "startDate": since.strftime("%Y-%m-%d"),
            "endDate": datetime.now().strftime("%Y-%m-%d"),
            "limit": config.batch_size
        }

        async with session.get(f"{config.base_url}/tiingo/news", params=params) as response:
            if response.status == 429:
                raise Exception("Rate limit exceeded")
            elif response.status != 200:
                raise Exception(f"HTTP {response.status}")

            return await response.json()

    async def _fetch_alpha_vantage_news(self, session: aiohttp.ClientSession,
                                      config: NewsVendorConfig, since: datetime) -> List[Dict[str, Any]]:
        """Fetch news from Alpha Vantage API."""
        params = {
            "function": "NEWS_SENTIMENT",
            "apikey": config.api_key,
            "time_from": since.strftime("%Y%m%dT%H%M"),
            "limit": config.batch_size
        }

        async with session.get(config.base_url, params=params) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}")

            data = await response.json()
            return data.get("feed", [])

    async def _fetch_fmp_news(self, session: aiohttp.ClientSession,
                            config: NewsVendorConfig, since: datetime) -> List[Dict[str, Any]]:
        """Fetch news from Financial Modeling Prep API."""
        params = {
            "apikey": config.api_key,
            "page": 0,
            "size": config.batch_size
        }

        async with session.get(f"{config.base_url}/v3/stock_news", params=params) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}")

            # Filter for recent articles
            data = await response.json()
            recent_articles = []

            for article in data:
                pub_date = datetime.fromisoformat(article.get("publishedDate", "").replace("Z", "+00:00"))
                if pub_date >= since:
                    recent_articles.append(article)

            return recent_articles

    async def _fetch_benzinga_news(self, session: aiohttp.ClientSession,
                                 config: NewsVendorConfig, since: datetime) -> List[Dict[str, Any]]:
        """Fetch news from Benzinga API."""
        params = {
            "token": config.api_key,
            "dateFrom": since.strftime("%Y-%m-%d"),
            "dateTo": datetime.now().strftime("%Y-%m-%d"),
            "displayOutput": "full",
            "pageSize": config.batch_size
        }

        async with session.get(f"{config.base_url}/news", params=params) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}")

            data = await response.json()
            return data.get("data", [])

    async def _process_vendor_articles(self, vendor_name: str, raw_articles: List[Dict[str, Any]]):
        """Process articles from a vendor."""
        if not raw_articles:
            return

        processed_articles = []

        for raw_article in raw_articles:
            try:
                # Convert to standardized format
                article = await self._standardize_article(vendor_name, raw_article)
                if article:
                    processed_articles.append(article)
            except Exception as e:
                logger.warning(f"Failed to process article from {vendor_name}: {e}")

        # Apply deduplication
        unique_articles = []
        for article in processed_articles:
            is_duplicate, reason = self.deduplicator.is_duplicate(article)

            if is_duplicate:
                self.metrics['articles_deduplicated'] += 1
                logger.debug(f"Duplicate article filtered: {reason}")
            else:
                unique_articles.append(article)

        # Queue for processing based on urgency
        for article in unique_articles:
            if article.urgency in [NewsUrgency.CRITICAL, NewsUrgency.HIGH]:
                await self.high_priority_queue.put(article)
            else:
                await self.normal_priority_queue.put(article)

        logger.info(f"Processed {len(unique_articles)}/{len(processed_articles)} unique articles from {vendor_name}")

    async def _standardize_article(self, vendor_name: str, raw_article: Dict[str, Any]) -> Optional[ProcessedNewsArticle]:
        """Convert vendor-specific article format to standardized format."""

        start_time = time.time()

        try:
            if vendor_name == "polygon":
                article = self._standardize_polygon_article(raw_article)
            elif vendor_name == "tiingo":
                article = self._standardize_tiingo_article(raw_article)
            elif vendor_name == "alpha_vantage":
                article = self._standardize_alpha_vantage_article(raw_article)
            elif vendor_name == "fmp":
                article = self._standardize_fmp_article(raw_article)
            elif vendor_name == "benzinga":
                article = self._standardize_benzinga_article(raw_article)
            else:
                return None

            if article:
                # Add common processing metadata
                article.vendor = vendor_name
                article.discovered_date = datetime.now()
                article.market_session = self._get_market_session()
                article.urgency = self._classify_news_urgency(article)
                article.processing_latency_ms = int((time.time() - start_time) * 1000)

                return article

        except Exception as e:
            logger.warning(f"Failed to standardize {vendor_name} article: {e}")
            return None

    def _standardize_polygon_article(self, data: Dict[str, Any]) -> ProcessedNewsArticle:
        """Standardize Polygon article."""
        return ProcessedNewsArticle(
            article_id=data.get("id", ""),
            vendor="polygon",
            title=data.get("title", ""),
            content=data.get("description", ""),  # Polygon doesn't provide full content
            summary=data.get("description", "")[:500],
            url=data.get("article_url", ""),
            author=data.get("author", ""),
            published_date=datetime.fromisoformat(data.get("published_utc", "").replace("Z", "+00:00")),
            discovered_date=datetime.now(),
            tickers=[ticker.get("ticker") for ticker in data.get("tickers", [])],
            keywords=data.get("keywords", []),
            raw_data=data,
            urgency=NewsUrgency.LOW,  # Will be reclassified
            market_session=MarketSession.CLOSED  # Will be updated
        )

    def _standardize_tiingo_article(self, data: Dict[str, Any]) -> ProcessedNewsArticle:
        """Standardize Tiingo article."""
        return ProcessedNewsArticle(
            article_id=str(data.get("id", "")),
            vendor="tiingo",
            title=data.get("title", ""),
            content=data.get("description", ""),
            summary=data.get("description", "")[:500],
            url=data.get("url", ""),
            author=None,
            published_date=datetime.fromisoformat(data.get("publishedDate", "").replace("Z", "+00:00")),
            discovered_date=datetime.now(),
            tickers=data.get("tickers", []),
            keywords=data.get("tags", []),
            raw_data=data,
            urgency=NewsUrgency.LOW,
            market_session=MarketSession.CLOSED
        )

    def _standardize_alpha_vantage_article(self, data: Dict[str, Any]) -> ProcessedNewsArticle:
        """Standardize Alpha Vantage article."""
        return ProcessedNewsArticle(
            article_id=data.get("url", "").split("/")[-1][:20],
            vendor="alpha_vantage",
            title=data.get("title", ""),
            content=data.get("summary", ""),
            summary=data.get("summary", "")[:500],
            url=data.get("url", ""),
            author=data.get("authors", [None])[0] if data.get("authors") else None,
            published_date=datetime.fromisoformat(data.get("time_published", "").replace("Z", "+00:00")),
            discovered_date=datetime.now(),
            tickers=[ticker["ticker"] for ticker in data.get("ticker_sentiment", [])],
            keywords=data.get("topics", []),
            raw_data=data,
            urgency=NewsUrgency.LOW,
            market_session=MarketSession.CLOSED
        )

    def _standardize_fmp_article(self, data: Dict[str, Any]) -> ProcessedNewsArticle:
        """Standardize Financial Modeling Prep article."""
        return ProcessedNewsArticle(
            article_id=data.get("url", "").split("/")[-1][:20],
            vendor="fmp",
            title=data.get("title", ""),
            content=data.get("text", ""),
            summary=data.get("text", "")[:500],
            url=data.get("url", ""),
            author=None,
            published_date=datetime.fromisoformat(data.get("publishedDate", "").replace("Z", "+00:00")),
            discovered_date=datetime.now(),
            tickers=[data.get("symbol")] if data.get("symbol") else [],
            keywords=[],
            raw_data=data,
            urgency=NewsUrgency.LOW,
            market_session=MarketSession.CLOSED
        )

    def _standardize_benzinga_article(self, data: Dict[str, Any]) -> ProcessedNewsArticle:
        """Standardize Benzinga article."""
        return ProcessedNewsArticle(
            article_id=str(data.get("id", "")),
            vendor="benzinga",
            title=data.get("title", ""),
            content=data.get("body", ""),
            summary=data.get("teaser", "")[:500],
            url=data.get("url", ""),
            author=data.get("author", ""),
            published_date=datetime.fromisoformat(data.get("created", "").replace("Z", "+00:00")),
            discovered_date=datetime.now(),
            tickers=[stock["name"] for stock in data.get("stocks", [])],
            keywords=[tag["name"] for tag in data.get("tags", [])],
            raw_data=data,
            urgency=NewsUrgency.LOW,
            market_session=MarketSession.CLOSED
        )

    async def _high_priority_processor(self):
        """Process high-priority news articles."""
        while self._running:
            try:
                # Wait for high priority articles
                article = await self.high_priority_queue.get()
                await self._process_article_for_signals(article, high_priority=True)
                self.high_priority_queue.task_done()
            except Exception as e:
                logger.error(f"Error in high priority processor: {e}")
                await asyncio.sleep(1)

    async def _normal_priority_processor(self):
        """Process normal priority news articles."""
        while self._running:
            try:
                # Wait for normal articles with timeout
                article = await asyncio.wait_for(self.normal_priority_queue.get(), timeout=5.0)
                await self._process_article_for_signals(article, high_priority=False)
                self.normal_priority_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in normal priority processor: {e}")
                await asyncio.sleep(1)

    async def _process_article_for_signals(self, article: ProcessedNewsArticle, high_priority: bool):
        """Process article through enhanced LLM pipeline for signal generation."""

        start_time = time.time()

        try:
            # Store article in database first
            await self._store_article(article)

            # Convert to NewsArticle format for enhanced LLM processing
            news_article = NewsArticle(
                id=article.article_id,
                title=article.title,
                content=article.content,
                summary=article.summary,
                url=article.url,
                published_date=article.published_date,
                source=article.vendor,
                tickers=article.tickers,
                raw_data=article.raw_data
            )

            # Create processing context
            context = {
                'market_session': article.market_session.value,
                'urgency': article.urgency.value,
                'high_priority': high_priority,
                'discovery_time': article.discovered_date,
                'processing_latency_ms': article.processing_latency_ms
            }

            # Trigger enhanced LLM processing for comprehensive analysis
            logger.info(f"Triggering enhanced LLM processing for article: {article.title[:50]}... "
                       f"(urgency: {article.urgency.value}, session: {article.market_session.value})")

            analysis_result = await self.llm_processor.process_article(news_article, context)
            article.llm_triggered = True
            self.metrics['llm_processing_triggered'] += 1

            # Check if analysis generated actionable signals
            if analysis_result and self._is_actionable_signal(analysis_result):
                article.signal_generated = True
                self.metrics['signals_generated'] += 1

                logger.info(f"Actionable signal generated from article: {article.title[:50]}... "
                           f"(confidence: {analysis_result.ensemble_confidence:.2f})")
            elif analysis_result and analysis_result.signal_generated:
                logger.info(f"Signal generated (not actionable) from article: {article.title[:50]}...")

            # Update processing latency in database
            await self._update_llm_processing_status(article, analysis_result)

            # Update metrics
            processing_time = time.time() - start_time
            self.metrics['articles_processed'] += 1
            self.metrics['avg_processing_latency_ms'] = (
                (self.metrics['avg_processing_latency_ms'] * (self.metrics['articles_processed'] - 1) +
                 processing_time * 1000) / self.metrics['articles_processed']
            )

            logger.debug(f"Enhanced processing completed in {processing_time*1000:.1f}ms "
                        f"(total latency: {analysis_result.processing_time_ms}ms)")

        except Exception as e:
            logger.error(f"Failed to process article {article.article_id}: {e}")
            # Update error status in database
            await self._update_processing_error(article, str(e))

    def _is_actionable_signal(self, analysis_result: NewsAnalysisResult) -> bool:
        """Determine if enhanced LLM analysis resulted in actionable signals."""
        if not analysis_result:
            return False

        # Check if marked as actionable by the enhanced processor
        return analysis_result.actionable_signal

    async def _update_llm_processing_status(self, article: ProcessedNewsArticle,
                                          analysis_result: NewsAnalysisResult):
        """Update LLM processing status in the database."""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE dev_news_realtime
                    SET llm_processing_triggered = TRUE,
                        llm_processing_completed = TRUE,
                        signal_generated = $1,
                        quality_score = $2,
                        updated_at = NOW()
                    WHERE article_id = $3 AND vendor = $4
                """,
                analysis_result.signal_generated,
                analysis_result.ensemble_confidence,
                article.article_id,
                article.vendor)
        except Exception as e:
            logger.error(f"Failed to update LLM processing status: {e}")

    async def _update_processing_error(self, article: ProcessedNewsArticle, error_msg: str):
        """Update processing error status in database."""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE dev_news_realtime
                    SET llm_processing_triggered = TRUE,
                        llm_processing_completed = FALSE,
                        quality_score = 0.0,
                        updated_at = NOW()
                    WHERE article_id = $1 AND vendor = $2
                """, article.article_id, article.vendor)
        except Exception as e:
            logger.error(f"Failed to update processing error status: {e}")

    async def _store_article(self, article: ProcessedNewsArticle):
        """Store processed article in database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Use the existing news table structure but add real-time metadata
                await conn.execute("""
                    INSERT INTO news_realtime
                    (article_id, vendor, title, content, summary, url, author,
                     published_date, discovered_date, urgency, market_session,
                     tickers, keywords, processing_latency_ms, raw_data, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
                    ON CONFLICT (article_id, vendor) DO UPDATE SET
                    content = EXCLUDED.content,
                    processing_latency_ms = EXCLUDED.processing_latency_ms,
                    updated_at = NOW()
                """,
                article.article_id, article.vendor, article.title, article.content,
                article.summary, article.url, article.author, article.published_date,
                article.discovered_date, article.urgency.value, article.market_session.value,
                article.tickers, article.keywords, article.processing_latency_ms,
                json.dumps(article.raw_data))

        except Exception as e:
            logger.error(f"Failed to store article {article.article_id}: {e}")

    async def _metrics_reporter(self):
        """Periodically report performance metrics."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Report every minute

                self.metrics['last_processed_timestamp'] = datetime.now()

                logger.info(f"News Ingestion Metrics: "
                          f"processed={self.metrics['articles_processed']}, "
                          f"deduplicated={self.metrics['articles_deduplicated']}, "
                          f"llm_triggered={self.metrics['llm_processing_triggered']}, "
                          f"signals={self.metrics['signals_generated']}, "
                          f"avg_latency={self.metrics['avg_processing_latency_ms']:.1f}ms")

                # Report vendor errors
                if self.metrics['vendor_errors']:
                    logger.warning(f"Vendor errors: {dict(self.metrics['vendor_errors'])}")

            except Exception as e:
                logger.error(f"Error reporting metrics: {e}")

    async def _health_checker(self):
        """Monitor service health and circuit breaker states."""
        while self._running:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes

                # Check circuit breaker states
                for vendor_name, breaker in self.circuit_breakers.items():
                    if breaker['state'] == 'open':
                        logger.warning(f"Vendor {vendor_name} circuit breaker is OPEN (failures: {breaker['failures']})")
                    elif breaker['state'] == 'half_open':
                        logger.info(f"Vendor {vendor_name} circuit breaker is HALF_OPEN, testing...")

                # Check queue sizes
                high_priority_size = self.high_priority_queue.qsize()
                normal_priority_size = self.normal_priority_queue.qsize()

                if high_priority_size > 500:
                    logger.warning(f"High priority queue is large: {high_priority_size}")

                if normal_priority_size > 2500:
                    logger.warning(f"Normal priority queue is large: {normal_priority_size}")

            except Exception as e:
                logger.error(f"Error in health checker: {e}")


# Factory function to create and configure the service
async def create_realtime_news_service(
    db_pool: asyncpg.Pool,
    env: Environment,
    llm_client: MultiProviderLLMClient,
    api_keys: Dict[str, str]
) -> RealTimeNewsIngestionService:
    """Create and configure the real-time news ingestion service."""

    # Create enhanced LLM processor
    from src.domains.market_data.services.llm.enhanced_news_llm_processor import create_enhanced_llm_processor
    llm_processor = await create_enhanced_llm_processor(llm_client, db_pool, env)

    # Create service
    service = RealTimeNewsIngestionService(db_pool, env, llm_processor)

    # Configure vendors
    vendors = [
        NewsVendorConfig(
            name="polygon",
            api_key=api_keys.get("polygon", ""),
            base_url="https://api.polygon.io/v2",
            rate_limit_rpm=300,  # 5 calls per second
            priority_weight=0.9,
            enabled=bool(api_keys.get("polygon"))
        ),
        NewsVendorConfig(
            name="tiingo",
            api_key=api_keys.get("tiingo", ""),
            base_url="https://api.tiingo.com",
            rate_limit_rpm=120,  # 2 calls per second
            priority_weight=0.8,
            enabled=bool(api_keys.get("tiingo"))
        ),
        NewsVendorConfig(
            name="alpha_vantage",
            api_key=api_keys.get("alpha_vantage", ""),
            base_url="https://www.alphavantage.co/query",
            rate_limit_rpm=60,   # 1 call per second
            priority_weight=0.7,
            enabled=bool(api_keys.get("alpha_vantage"))
        ),
        NewsVendorConfig(
            name="fmp",
            api_key=api_keys.get("fmp", ""),
            base_url="https://financialmodelingprep.com/api",
            rate_limit_rpm=300,
            priority_weight=0.85,
            enabled=bool(api_keys.get("fmp"))
        ),
        NewsVendorConfig(
            name="benzinga",
            api_key=api_keys.get("benzinga", ""),
            base_url="https://api.benzinga.com/api/v2",
            rate_limit_rpm=120,
            priority_weight=0.95,
            enabled=bool(api_keys.get("benzinga"))
        )
    ]

    for vendor_config in vendors:
        service.configure_vendor(vendor_config)

    return service