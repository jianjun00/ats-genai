#!/usr/bin/env python3
"""
EODHD News API Adapter
Integrates with EODHD's News API for comprehensive financial news coverage.
Handles rate limits, sentiment analysis, and historical data retrieval.
"""

import asyncio
import aiohttp
import logging
import json
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class EODHDNewsConfig:
    """Configuration for EODHD News API"""
    api_key: str
    base_url: str = "https://eodhd.com/api/news"
    max_concurrent: int = 20
    rate_limit_delay: float = 1.0  # Conservative 1 second between requests
    max_retries: int = 3
    default_limit: int = 1000

class EODHDNewsAdapter:
    """
    EODHD News API adapter with advanced features:
    - Historical news retrieval (up to 10+ years)
    - Sentiment analysis included
    - Symbol-specific and topic-based filtering
    - Intelligent rate limiting and error handling
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('EODHD_API_KEY')
        if not self.api_key:
            raise ValueError("EODHD API key is required")
        
        self.config = EODHDNewsConfig(api_key=self.api_key)
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
    async def __aenter__(self):
        """Initialize HTTP session"""
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent * 2,
            limit_per_host=self.config.max_concurrent
        )
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup HTTP session"""
        if hasattr(self, 'session'):
            await self.session.close()
    
    async def fetch_news_for_symbol(self, symbol: str, from_date: date, to_date: date,
                                  limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch news for a specific symbol from EODHD
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            from_date: Start date for news retrieval
            to_date: End date for news retrieval
            limit: Maximum number of articles to retrieve
            
        Returns:
            List of news articles with sentiment analysis
        """
        async with self.semaphore:
            params = {
                's': symbol,
                'from': from_date.strftime('%Y-%m-%d'),
                'to': to_date.strftime('%Y-%m-%d'),
                'limit': limit or self.config.default_limit,
                'api_token': self.api_key
            }
            
            for attempt in range(self.config.max_retries):
                try:
                    await asyncio.sleep(self.config.rate_limit_delay)
                    
                    async with self.session.get(self.config.base_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Process EODHD response format
                            articles = []
                            for item in data:
                                article = self._process_eodhd_article(item, symbol)
                                if article:
                                    articles.append(article)
                            
                            logger.info(f"EODHD {symbol}: {len(articles)} articles retrieved")
                            return articles
                            
                        elif response.status == 429:
                            # Rate limited - exponential backoff
                            delay = self.config.rate_limit_delay * (2 ** attempt)
                            logger.warning(f"EODHD rate limited for {symbol}, retrying in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue
                            
                        elif response.status == 402:
                            # Payment/plan limit exceeded
                            logger.error(f"EODHD API limit exceeded for {symbol}")
                            return []
                            
                        else:
                            logger.warning(f"EODHD API error for {symbol}: HTTP {response.status}")
                            error_text = await response.text()
                            logger.debug(f"Error response: {error_text}")
                            return []
                            
                except asyncio.TimeoutError:
                    delay = self.config.rate_limit_delay * (2 ** attempt)
                    logger.warning(f"EODHD timeout for {symbol}, attempt {attempt+1}/{self.config.max_retries}")
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(delay)
                    continue
                    
                except Exception as e:
                    delay = self.config.rate_limit_delay * (2 ** attempt)
                    logger.warning(f"EODHD error for {symbol}: {e}")
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(delay)
                    continue
            
            logger.error(f"EODHD max retries exceeded for {symbol}")
            return []
    
    def _process_eodhd_article(self, raw_article: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """
        Process raw EODHD article data into standardized format
        
        EODHD News API Response Format:
        {
            "date": "1609459200",  # Unix timestamp
            "title": "Article Title",
            "content": "Full article content...",
            "link": "https://example.com/article",
            "symbols": ["AAPL", "MSFT"],
            "tags": ["earnings", "technology"],
            "sentiment": 0.65  # Sentiment score from -1 to 1
        }
        """
        try:
            # Parse publication date from Unix timestamp
            published_date = None
            if 'date' in raw_article:
                try:
                    timestamp = int(raw_article['date'])
                    published_date = datetime.fromtimestamp(timestamp)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid timestamp for article: {e}")
                    published_date = datetime.now()
            
            # Generate unique ID (EODHD doesn't provide article IDs)
            article_id = self._generate_article_id(raw_article, symbol)
            
            # Extract symbols list
            symbols = raw_article.get('symbols', [])
            if symbol not in symbols:
                symbols.append(symbol)
            
            # Process sentiment score
            sentiment_score = raw_article.get('sentiment')
            if sentiment_score is not None:
                try:
                    sentiment_score = float(sentiment_score)
                    # Ensure sentiment is in valid range [-1, 1]
                    sentiment_score = max(-1.0, min(1.0, sentiment_score))
                except (ValueError, TypeError):
                    sentiment_score = None
            
            return {
                'eodhd_id': article_id,
                'title': raw_article.get('title', '').strip(),
                'content': raw_article.get('content', '').strip(),
                'url': raw_article.get('link', '').strip(),
                'published_date': published_date,
                'symbols': symbols,
                'tags': raw_article.get('tags', []),
                'sentiment_score': sentiment_score,
                'data': raw_article  # Store original data
            }
            
        except Exception as e:
            logger.warning(f"Error processing EODHD article: {e}")
            return None
    
    def _generate_article_id(self, article: Dict[str, Any], symbol: str) -> str:
        """Generate unique article ID from content hash"""
        import hashlib
        
        # Create content string for hashing
        content_parts = [
            article.get('title', ''),
            article.get('link', ''),
            str(article.get('date', '')),
            symbol
        ]
        content_str = '|'.join(content_parts)
        
        # Generate MD5 hash
        hash_obj = hashlib.md5(content_str.encode('utf-8'))
        return f"eodhd_{hash_obj.hexdigest()}"
    
    async def fetch_news_batch(self, symbols: List[str], from_date: date, to_date: date,
                             limit_per_symbol: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch news for multiple symbols concurrently
        
        Args:
            symbols: List of stock symbols
            from_date: Start date for news retrieval
            to_date: End date for news retrieval
            limit_per_symbol: Max articles per symbol
            
        Returns:
            Combined list of all news articles
        """
        logger.info(f"Fetching EODHD news for {len(symbols)} symbols from {from_date} to {to_date}")
        
        # Create tasks for all symbols
        tasks = [
            self.fetch_news_for_symbol(symbol, from_date, to_date, limit_per_symbol)
            for symbol in symbols
        ]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine all articles
        all_articles = []
        for i, result in enumerate(results):
            if isinstance(result, list):
                all_articles.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Error fetching news for {symbols[i]}: {result}")
        
        logger.info(f"EODHD batch complete: {len(all_articles)} total articles")
        return all_articles
    
    async def get_historical_coverage(self, symbol: str, max_years: int = 10) -> Dict[str, Any]:
        """
        Get information about historical news coverage for a symbol
        
        Args:
            symbol: Stock symbol to check
            max_years: Maximum years to look back
            
        Returns:
            Coverage statistics and date range information
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * max_years)
        
        # Test fetch with small limit to check coverage
        sample_articles = await self.fetch_news_for_symbol(symbol, start_date, end_date, limit=100)
        
        coverage_info = {
            'symbol': symbol,
            'total_sample_articles': len(sample_articles),
            'date_range': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            },
            'has_sentiment': any(article.get('sentiment_score') is not None for article in sample_articles),
            'avg_articles_per_year': len(sample_articles) * max_years / 100 if sample_articles else 0
        }
        
        if sample_articles:
            # Find actual date range from articles
            article_dates = [
                article['published_date'] for article in sample_articles 
                if article.get('published_date')
            ]
            if article_dates:
                coverage_info['actual_date_range'] = {
                    'earliest': min(article_dates).isoformat(),
                    'latest': max(article_dates).isoformat()
                }
        
        return coverage_info
    
    @staticmethod
    def get_sentiment_label(sentiment_score: Optional[float]) -> str:
        """Convert sentiment score to human-readable label"""
        if sentiment_score is None:
            return "neutral"
        elif sentiment_score > 0.3:
            return "positive"
        elif sentiment_score < -0.3:
            return "negative"
        else:
            return "neutral"


async def test_eodhd_adapter():
    """Test function for EODHD News Adapter"""
    import os
    
    # Test only if API key is available
    api_key = os.getenv('EODHD_API_KEY')
    if not api_key:
        print("⚠️ EODHD_API_KEY not found - skipping test")
        return
    
    print("🔧 Testing EODHD News Adapter...")
    
    async with EODHDNewsAdapter(api_key) as adapter:
        # Test single symbol
        test_symbol = 'AAPL'
        end_date = date.today()
        start_date = end_date - timedelta(days=30)  # Last 30 days
        
        print(f"📰 Fetching news for {test_symbol} from {start_date} to {end_date}")
        
        articles = await adapter.fetch_news_for_symbol(test_symbol, start_date, end_date, limit=10)
        
        print(f"✅ Retrieved {len(articles)} articles")
        
        if articles:
            sample_article = articles[0]
            print(f"📄 Sample article:")
            print(f"   Title: {sample_article['title'][:100]}...")
            print(f"   Date: {sample_article['published_date']}")
            print(f"   Sentiment: {sample_article.get('sentiment_score')} ({EODHDNewsAdapter.get_sentiment_label(sample_article.get('sentiment_score'))})")
            print(f"   Symbols: {sample_article['symbols']}")
        
        # Test historical coverage
        coverage = await adapter.get_historical_coverage(test_symbol, max_years=5)
        print(f"📊 Historical coverage: {coverage}")


if __name__ == "__main__":
    asyncio.run(test_eodhd_adapter())