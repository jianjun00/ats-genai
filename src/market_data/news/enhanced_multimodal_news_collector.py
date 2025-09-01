#!/usr/bin/env python3
"""
Enhanced Multi-Modal News Collector
Extends existing news infrastructure with additional sources and economic events detection.
Built for the multi-modal prediction system.
"""

import os
import asyncio
import aiohttp
import asyncpg
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import argparse
from dataclasses import dataclass
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class NewsSourceConfig:
    """Configuration for each news source"""
    name: str
    base_url: str
    api_key_env: str
    rate_limit_per_minute: int
    max_concurrent: int
    supports_historical: bool
    supports_realtime: bool
    cost_per_request: float  # USD

class EnhancedNewsCollector:
    """Enhanced news collector supporting multiple sources with economic events detection"""
    
    SOURCES = {
        'polygon': NewsSourceConfig(
            name='polygon',
            base_url='https://api.polygon.io/v2/reference/news',
            api_key_env='POLYGON_API_KEY',
            rate_limit_per_minute=500,
            max_concurrent=50,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.002
        ),
        'tiingo': NewsSourceConfig(
            name='tiingo', 
            base_url='https://api.tiingo.com/tiingo/news',
            api_key_env='TIINGO_API_KEY',
            rate_limit_per_minute=1000,
            max_concurrent=30,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.001
        ),
        'alpha_vantage': NewsSourceConfig(
            name='alpha_vantage',
            base_url='https://www.alphavantage.co/query',
            api_key_env='ALPHA_VANTAGE_API_KEY',
            rate_limit_per_minute=500,
            max_concurrent=25,
            supports_historical=True,
            supports_realtime=False,
            cost_per_request=0.003
        ),
        'fmp': NewsSourceConfig(
            name='fmp',
            base_url='https://financialmodelingprep.com/api/v3/stock_news',
            api_key_env='FMP_API_KEY', 
            rate_limit_per_minute=300,
            max_concurrent=20,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.004
        )
    }
    
    # Economic event classification keywords
    ECONOMIC_EVENT_KEYWORDS = {
        'earnings': ['earnings', 'quarterly results', 'profit', 'revenue', 'eps', 'guidance'],
        'fed': ['federal reserve', 'fed', 'interest rate', 'fomc', 'powell', 'monetary policy'],
        'employment': ['employment', 'unemployment', 'jobs report', 'nonfarm payrolls', 'jobless claims'],
        'inflation': ['inflation', 'cpi', 'pce', 'consumer price index', 'core inflation'],
        'growth': ['gdp', 'gross domestic product', 'economic growth', 'recession'],
        'corporate': ['merger', 'acquisition', 'ipo', 'buyback', 'dividend', 'split'],
        'macro': ['economic data', 'trade', 'tariff', 'fiscal policy', 'stimulus']
    }
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 20):
        self.db_config = db_config
        self.pool_size = pool_size
        self.pool = None
        
        # Initialize API keys
        self.api_keys = {}
        for source, config in self.SOURCES.items():
            api_key = os.getenv(config.api_key_env)
            if api_key:
                self.api_keys[source] = api_key
                logger.info(f"✅ {source.upper()} API key loaded")
            else:
                logger.warning(f"⚠️ {source.upper()} API key not found")
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=self.pool_size,
            max_size=self.pool_size * 2,
            server_settings={'jit': 'off'}
        )
        
        # Initialize HTTP session
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=100)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.pool:
            await self.pool.close()
        if hasattr(self, 'session'):
            await self.session.close()
    
    async def fetch_alpha_vantage_news(self, symbols: List[str] = None, 
                                      time_from: str = None, time_to: str = None,
                                      limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch news from Alpha Vantage API"""
        if 'alpha_vantage' not in self.api_keys:
            logger.warning("Alpha Vantage API key not available")
            return []
        
        semaphore = asyncio.Semaphore(self.SOURCES['alpha_vantage'].max_concurrent)
        
        async def fetch_batch():
            async with semaphore:
                params = {
                    'function': 'NEWS_SENTIMENT',
                    'apikey': self.api_keys['alpha_vantage'],
                    'limit': min(limit, 1000)
                }
                
                if symbols:
                    params['tickers'] = ','.join(symbols[:50])  # API limit
                if time_from:
                    params['time_from'] = time_from
                if time_to:
                    params['time_to'] = time_to
                
                try:
                    async with self.session.get(self.SOURCES['alpha_vantage'].base_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if 'feed' in data:
                                return [self._parse_alpha_vantage_article(article) for article in data['feed']]
                        
                        logger.warning(f"Alpha Vantage API error: {response.status}")
                        return []
                        
                except Exception as e:
                    logger.error(f"Alpha Vantage fetch error: {e}")
                    return []
        
        return await fetch_batch()
    
    def _parse_alpha_vantage_article(self, article: Dict) -> Dict[str, Any]:
        """Parse Alpha Vantage news article"""
        return {
            'alpha_vantage_id': article.get('url', '').split('/')[-1] or str(hash(article.get('title', ''))),
            'title': article.get('title', ''),
            'summary': article.get('summary', ''),
            'url': article.get('url'),
            'time_published': self._parse_alpha_vantage_time(article.get('time_published')),
            'authors': [author.strip() for author in article.get('authors', '').split(',') if author.strip()],
            'topics': [topic['topic'] for topic in article.get('topics', [])],
            'tickers': [ticker['ticker'] for ticker in article.get('ticker_sentiment', [])],
            'overall_sentiment_score': float(article.get('overall_sentiment_score', 0)),
            'overall_sentiment_label': article.get('overall_sentiment_label'),
            'ticker_sentiment': article.get('ticker_sentiment', []),
            'data': article  # Store full original data
        }
    
    def _parse_alpha_vantage_time(self, time_str: str) -> datetime:
        """Parse Alpha Vantage timestamp"""
        if not time_str:
            return datetime.now()
        try:
            return datetime.strptime(time_str, '%Y%m%dT%H%M%S')
        except:
            return datetime.now()
    
    async def fetch_fmp_news(self, symbols: List[str] = None, 
                            limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch news from Financial Modeling Prep API"""
        if 'fmp' not in self.api_keys:
            logger.warning("FMP API key not available")
            return []
        
        semaphore = asyncio.Semaphore(self.SOURCES['fmp'].max_concurrent)
        
        async def fetch_symbol_news(symbol: str):
            async with semaphore:
                params = {
                    'tickers': symbol,
                    'limit': min(limit // len(symbols) if symbols else limit, 100),
                    'apikey': self.api_keys['fmp']
                }
                
                try:
                    async with self.session.get(self.SOURCES['fmp'].base_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            return [self._parse_fmp_article(article) for article in data if isinstance(data, list)]
                        
                        logger.warning(f"FMP API error for {symbol}: {response.status}")
                        return []
                        
                except Exception as e:
                    logger.error(f"FMP fetch error for {symbol}: {e}")
                    return []
        
        if symbols:
            tasks = [fetch_symbol_news(symbol) for symbol in symbols[:50]]  # Limit concurrent requests
            results = await asyncio.gather(*tasks)
            return [article for result in results for article in result]
        else:
            # Fetch general news
            params = {
                'limit': min(limit, 1000),
                'apikey': self.api_keys['fmp']
            }
            
            try:
                async with self.session.get(self.SOURCES['fmp'].base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return [self._parse_fmp_article(article) for article in data if isinstance(data, list)]
            except Exception as e:
                logger.error(f"FMP general news fetch error: {e}")
            
            return []
    
    def _parse_fmp_article(self, article: Dict) -> Dict[str, Any]:
        """Parse FMP news article"""
        return {
            'fmp_id': str(article.get('publishedDate', '')) + '_' + str(hash(article.get('title', ''))),
            'title': article.get('title', ''),
            'content': article.get('text', ''),
            'url': article.get('url'),
            'publishedDate': self._parse_fmp_time(article.get('publishedDate')),
            'site': article.get('site'),
            'symbol': article.get('symbol'),
            'data': article  # Store full original data
        }
    
    def _parse_fmp_time(self, time_str: str) -> datetime:
        """Parse FMP timestamp"""
        if not time_str:
            return datetime.now()
        try:
            return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except:
            return datetime.now()
    
    async def detect_economic_events(self, articles: List[Dict]) -> List[Dict[str, Any]]:
        """Detect and classify economic events from news articles"""
        events = []
        
        for article in articles:
            title = article.get('title', '').lower()
            content = article.get('content', '').lower() or article.get('summary', '').lower()
            full_text = f"{title} {content}"
            
            # Classify event type
            event_categories = []
            severity = 1
            
            for category, keywords in self.ECONOMIC_EVENT_KEYWORDS.items():
                if any(keyword in full_text for keyword in keywords):
                    event_categories.append(category)
                    
                    # Assign severity based on keywords
                    if category == 'fed':
                        severity = max(severity, 8)  # Fed events are high impact
                    elif category == 'earnings':
                        severity = max(severity, 6)  # Earnings are medium-high impact
                    elif category in ['employment', 'inflation']:
                        severity = max(severity, 7)  # Economic data is high impact
                    else:
                        severity = max(severity, 4)  # Default medium impact
            
            if event_categories:
                # Extract affected symbols
                affected_symbols = article.get('tickers', []) or []
                if isinstance(affected_symbols, str):
                    affected_symbols = [affected_symbols]
                
                # Create economic event
                event = {
                    'event_type': event_categories[0],  # Primary category
                    'event_category': event_categories[0],
                    'severity': min(severity, 10),
                    'confidence_score': min(len(event_categories) * 0.3, 1.0),  # Higher confidence for multiple matches
                    'affected_symbols': affected_symbols,
                    'affected_sectors': self._extract_sectors_from_text(full_text),
                    'event_date': article.get('published_date') or article.get('time_published') or datetime.now(),
                    'announcement_date': article.get('published_date') or article.get('time_published') or datetime.now(),
                    'predicted_impact_score': self._calculate_predicted_impact(event_categories, severity),
                    'title': article.get('title', ''),
                    'description': (article.get('content') or article.get('summary', ''))[:1000],  # Limit length
                    'source_url': article.get('url'),
                    'data_vendor': article.get('source', 'unknown'),
                    'data': {
                        'categories': event_categories,
                        'keywords_found': [kw for cat in event_categories for kw in self.ECONOMIC_EVENT_KEYWORDS[cat] if kw in full_text],
                        'article_source': article.get('source')
                    }
                }
                events.append(event)
        
        return events
    
    def _extract_sectors_from_text(self, text: str) -> List[str]:
        """Extract affected sectors from news text"""
        sectors = []
        
        sector_keywords = {
            'technology': ['tech', 'software', 'apple', 'microsoft', 'google', 'amazon'],
            'financial': ['bank', 'financial', 'jpmorgan', 'goldman', 'credit'],
            'healthcare': ['health', 'pharma', 'medical', 'drug', 'pfizer', 'johnson'],
            'energy': ['oil', 'energy', 'gas', 'exxon', 'chevron', 'renewable'],
            'consumer': ['retail', 'consumer', 'walmart', 'target', 'nike'],
            'industrial': ['industrial', 'manufacturing', 'boeing', 'caterpillar']
        }
        
        for sector, keywords in sector_keywords.items():
            if any(keyword in text for keyword in keywords):
                sectors.append(sector)
        
        return sectors
    
    def _calculate_predicted_impact(self, categories: List[str], severity: int) -> float:
        """Calculate predicted market impact score"""
        base_impact = severity / 10.0  # Normalize to 0-1
        
        # Adjust based on category
        category_multipliers = {
            'fed': 1.5,
            'employment': 1.3,
            'inflation': 1.3,
            'earnings': 1.0,
            'corporate': 0.8,
            'macro': 1.2
        }
        
        max_multiplier = max([category_multipliers.get(cat, 1.0) for cat in categories])
        
        # Apply some randomness to simulate prediction uncertainty
        import random
        noise = random.uniform(-0.1, 0.1)
        
        return min(max(base_impact * max_multiplier + noise, -1.0), 1.0)
    
    async def bulk_insert_alpha_vantage_news(self, articles: List[Dict]) -> int:
        """Bulk insert Alpha Vantage news articles"""
        if not articles:
            return 0
        
        async with self.pool.acquire() as conn:
            try:
                records = [
                    (
                        article['alpha_vantage_id'],
                        article['title'],
                        article['summary'],
                        article['url'],
                        article['time_published'],
                        article['authors'],
                        article['topics'],
                        article['tickers'],
                        article['overall_sentiment_score'],
                        article['overall_sentiment_label'],
                        json.dumps(article['ticker_sentiment']),
                        json.dumps(article['data'])
                    )
                    for article in articles
                ]
                
                await conn.executemany("""
                    INSERT INTO dev_news_alpha_vantage (
                        alpha_vantage_id, title, summary, url, time_published,
                        authors, topics, tickers, overall_sentiment_score,
                        overall_sentiment_label, ticker_sentiment, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (alpha_vantage_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        updated_at = CURRENT_TIMESTAMP
                """, records)
                
                logger.info(f"✅ Inserted {len(articles)} Alpha Vantage news articles")
                return len(articles)
                
            except Exception as e:
                logger.error(f"❌ Alpha Vantage news insert error: {e}")
                return 0
    
    async def bulk_insert_fmp_news(self, articles: List[Dict]) -> int:
        """Bulk insert FMP news articles"""
        if not articles:
            return 0
        
        async with self.pool.acquire() as conn:
            try:
                records = [
                    (
                        article['fmp_id'],
                        article['title'],
                        article['content'],
                        article['url'],
                        article['publishedDate'],
                        article['site'],
                        article['symbol'],
                        json.dumps(article['data'])
                    )
                    for article in articles
                ]
                
                await conn.executemany("""
                    INSERT INTO dev_news_fmp (
                        fmp_id, title, content, url, publishedDate, site, symbol, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (fmp_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        content = EXCLUDED.content,
                        updated_at = CURRENT_TIMESTAMP
                """, records)
                
                logger.info(f"✅ Inserted {len(articles)} FMP news articles")
                return len(articles)
                
            except Exception as e:
                logger.error(f"❌ FMP news insert error: {e}")
                return 0
    
    async def bulk_insert_economic_events(self, events: List[Dict]) -> int:
        """Bulk insert economic events"""
        if not events:
            return 0
        
        async with self.pool.acquire() as conn:
            try:
                records = [
                    (
                        event['event_type'],
                        event.get('event_subtype'),
                        event['event_category'],
                        event['severity'],
                        event['confidence_score'],
                        event['affected_symbols'],
                        event['affected_sectors'],
                        [],  # affected_regions - empty for now
                        event['event_date'],
                        event['announcement_date'],
                        None,  # market_open_date - calculate later
                        event['predicted_impact_score'],
                        None,  # actual_impact_score - measure later
                        None,  # impact_duration_days - measure later
                        event['title'],
                        event.get('description'),
                        event.get('source_url'),
                        json.dumps(event['data']),
                        event['data_vendor']
                    )
                    for event in events
                ]
                
                await conn.executemany("""
                    INSERT INTO dev_economic_events (
                        event_type, event_subtype, event_category, severity, confidence_score,
                        affected_symbols, affected_sectors, affected_regions,
                        event_date, announcement_date, market_open_date,
                        predicted_impact_score, actual_impact_score, impact_duration_days,
                        title, description, source_url, data, data_vendor
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                    ON CONFLICT DO NOTHING  -- Avoid duplicates
                """, records)
                
                logger.info(f"✅ Inserted {len(events)} economic events")
                return len(events)
                
            except Exception as e:
                logger.error(f"❌ Economic events insert error: {e}")
                return 0
    
    async def collect_enhanced_news(self, symbols: List[str] = None, 
                                   hours_back: int = 24, limit: int = 1000) -> Dict[str, int]:
        """Main collection method for enhanced news from all sources"""
        logger.info(f"🚀 Starting enhanced news collection for {len(symbols) if symbols else 'all'} symbols")
        logger.info(f"📅 Time range: {hours_back} hours back, limit: {limit}")
        
        start_time = time.time()
        results = {'alpha_vantage': 0, 'fmp': 0, 'economic_events': 0}
        
        # Calculate time range
        time_to = datetime.now()
        time_from = time_to - timedelta(hours=hours_back)
        
        # Collect from Alpha Vantage
        if 'alpha_vantage' in self.api_keys:
            logger.info("📰 Fetching Alpha Vantage news...")
            av_articles = await self.fetch_alpha_vantage_news(
                symbols=symbols,
                time_from=time_from.strftime('%Y%m%dT%H%M'),
                time_to=time_to.strftime('%Y%m%dT%H%M'),
                limit=limit
            )
            results['alpha_vantage'] = await self.bulk_insert_alpha_vantage_news(av_articles)
        
        # Collect from FMP
        if 'fmp' in self.api_keys:
            logger.info("📰 Fetching FMP news...")
            fmp_articles = await self.fetch_fmp_news(symbols=symbols, limit=limit)
            results['fmp'] = await self.bulk_insert_fmp_news(fmp_articles)
        
        # Detect economic events from all collected articles
        all_articles = []
        if 'alpha_vantage' in locals():
            all_articles.extend([{**article, 'source': 'alpha_vantage'} for article in av_articles])
        if 'fmp_articles' in locals():
            all_articles.extend([{**article, 'source': 'fmp'} for article in fmp_articles])
        
        if all_articles:
            logger.info("🔍 Detecting economic events...")
            economic_events = await self.detect_economic_events(all_articles)
            results['economic_events'] = await self.bulk_insert_economic_events(economic_events)
        
        elapsed_time = time.time() - start_time
        total_items = sum(results.values())
        
        logger.info(f"🎉 Enhanced news collection completed!")
        logger.info(f"⏱️  Time: {elapsed_time:.1f} seconds")
        logger.info(f"📊 Results: {results}")
        logger.info(f"🔥 Rate: {total_items/elapsed_time:.1f} items/second")
        
        return results

async def main():
    """Main function for running enhanced news collection"""
    parser = argparse.ArgumentParser(description="Enhanced Multi-Modal News Collector")
    parser.add_argument('--symbols', nargs='+', help='Symbols to collect news for')
    parser.add_argument('--hours_back', type=int, default=24, help='Hours to look back')
    parser.add_argument('--limit', type=int, default=1000, help='Maximum articles per source')
    parser.add_argument('--mode', choices=['historical', 'realtime'], default='realtime')
    args = parser.parse_args()
    
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'postgres'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'dev_password'),
        'database': os.getenv('DB_NAME', 'dev_db')
    }
    
    # Get symbols if not provided
    symbols = args.symbols
    if not symbols:
        # Use top symbols by default
        symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA',
            'BRK.B', 'JPM', 'JNJ', 'V', 'PG', 'HD', 'MA', 'UNH'
        ]
    
    # Run collection
    async with EnhancedNewsCollector(db_config) as collector:
        results = await collector.collect_enhanced_news(
            symbols=symbols,
            hours_back=args.hours_back,
            limit=args.limit
        )
        
        print(f"\n📊 Final Results:")
        for source, count in results.items():
            print(f"  {source}: {count:,} items")

if __name__ == "__main__":
    asyncio.run(main())