#!/usr/bin/env python3
"""
Simple Polygon News Backfill

Direct approach using Polygon instruments without complex xref dependencies.
Designed to work with the current database structure and get news flowing quickly.

Environment Variables:
- POLYGON_API_KEY: Required Polygon API key
- NEWS_START_DATE: Start date (YYYY-MM-DD), defaults to 30 days ago
- NEWS_END_DATE: End date (YYYY-MM-DD), defaults to today
- NEWS_LIMIT: Max symbols to process, defaults to 100
- NEWS_SYMBOLS: Comma-separated symbols to process, overrides database lookup
"""

import os
import sys
import asyncio
import aiohttp
import asyncpg
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimplePolygonNewsCollector:
    """Simplified Polygon news collector with direct database integration"""
    
    def __init__(self, api_key: str, max_concurrent: int = 20):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_news_for_symbol(self, symbol: str, published_gte: str = None, published_lte: str = None) -> List[Dict[str, Any]]:
        """Fetch news for a specific symbol"""
        async with self.semaphore:
            url = "https://api.polygon.io/v2/reference/news"
            params = {
                'ticker': symbol,
                'limit': 1000,
                'apikey': self.api_key
            }
            
            if published_gte:
                params['published_utc.gte'] = published_gte
            if published_lte:
                params['published_utc.lte'] = published_lte
            
            for attempt in range(3):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = data.get('results', [])
                            logger.debug(f"✅ {symbol}: {len(results)} articles")
                            return results
                        elif response.status == 429:
                            logger.warning(f"⚠️ Rate limit hit for {symbol}, waiting...")
                            await asyncio.sleep(12)  # Polygon rate limit
                        else:
                            logger.warning(f"⚠️ {symbol}: HTTP {response.status}")
                            return []
                            
                except Exception as e:
                    logger.warning(f"⚠️ {symbol} attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
            
            return []

    async def fetch_news_batch(self, symbols: List[str], start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch news for a batch of symbols"""
        tasks = [
            self.fetch_news_for_symbol(symbol, start_date, end_date)
            for symbol in symbols
        ]
        
        all_news = []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for symbol, result in zip(symbols, results):
            if isinstance(result, list):
                all_news.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"❌ {symbol}: {result}")
        
        return all_news

async def get_polygon_symbols(pool: asyncpg.Pool, limit: int = 100) -> List[str]:
    """Get active Polygon instrument symbols from database"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol FROM dev_instrument_polygon 
            WHERE active = true 
            ORDER BY symbol 
            LIMIT $1
        """, limit)
        return [row['symbol'] for row in rows]

async def insert_news_articles(pool: asyncpg.Pool, articles: List[Dict[str, Any]]) -> int:
    """Insert news articles into database"""
    if not articles:
        return 0
    
    inserted = 0
    async with pool.acquire() as conn:
        for article in articles:
            try:
                # Extract key fields
                polygon_id = article.get('id', '')
                title = article.get('title', '')
                description = article.get('description', '')
                author = article.get('author', '')
                
                # Parse published date
                published_utc = None
                if article.get('published_utc'):
                    try:
                        published_utc = datetime.fromisoformat(
                            article['published_utc'].replace('Z', '+00:00')
                        )
                    except:
                        pass
                
                # Extract other fields
                article_url = article.get('article_url', '')
                image_url = article.get('image_url', '')
                
                # Publisher info
                publisher = article.get('publisher', {})
                publisher_name = publisher.get('name', '')
                publisher_homepage = publisher.get('homepage_url', '')
                publisher_logo = publisher.get('logo_url', '')
                publisher_favicon = publisher.get('favicon_url', '')
                
                # Arrays
                keywords = article.get('keywords', [])
                tickers = article.get('tickers', [])
                insights = article.get('insights', [])
                
                # Insert with conflict handling
                await conn.execute("""
                    INSERT INTO dev_news_polygon (
                        polygon_id, title, description, author, published_utc,
                        article_url, image_url, publisher_name, publisher_homepage_url,
                        publisher_logo_url, publisher_favicon_url, keywords, tickers,
                        insights, data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (polygon_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        updated_at = CURRENT_TIMESTAMP
                """, 
                    polygon_id, title, description, author, published_utc,
                    article_url, image_url, publisher_name, publisher_homepage,
                    publisher_logo, publisher_favicon, keywords, tickers,
                    json.dumps(insights), json.dumps(article)
                )
                inserted += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to insert article {article.get('id', 'unknown')}: {e}")
    
    return inserted

async def main():
    """Main execution function"""
    
    # Get configuration from environment
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("❌ POLYGON_API_KEY environment variable required")
        sys.exit(1)
    
    # Date configuration
    today = datetime.now().date()
    default_start = today - timedelta(days=30)
    
    start_date = os.getenv('NEWS_START_DATE', default_start.strftime('%Y-%m-%d'))
    end_date = os.getenv('NEWS_END_DATE', today.strftime('%Y-%m-%d'))
    limit = int(os.getenv('NEWS_LIMIT', '100'))
    
    # Check for specific symbols
    symbols_env = os.getenv('NEWS_SYMBOLS')
    if symbols_env:
        symbols = [s.strip().upper() for s in symbols_env.split(',')]
        logger.info(f"🎯 Using specified symbols: {symbols}")
    else:
        symbols = None
    
    logger.info(f"🚀 Starting Simple Polygon News Backfill")
    logger.info(f"📅 Date Range: {start_date} to {end_date}")
    logger.info(f"📊 Max Symbols: {limit:,}")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)
        
        # Get symbols if not specified
        if not symbols:
            symbols = await get_polygon_symbols(pool, limit)
            logger.info(f"📈 Found {len(symbols)} active Polygon symbols")
        
        if not symbols:
            logger.warning("⚠️ No symbols found to process")
            return
        
        # Process news in batches
        batch_size = 10
        total_articles = 0
        total_inserted = 0
        
        async with SimplePolygonNewsCollector(api_key) as collector:
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(symbols) + batch_size - 1) // batch_size
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches}: {batch_symbols}")
                
                # Fetch news for batch
                articles = await collector.fetch_news_batch(batch_symbols, start_date, end_date)
                total_articles += len(articles)
                
                # Insert into database
                inserted = await insert_news_articles(pool, articles)
                total_inserted += inserted
                
                logger.info(f"✅ Batch {batch_num}: {len(articles)} articles, {inserted} inserted")
                
                # Brief pause between batches
                await asyncio.sleep(1.0)
        
        await pool.close()
        
        logger.info("🎉 Simple Polygon News Backfill Complete!")
        logger.info(f"📰 Total Articles: {total_articles:,}")
        logger.info(f"💾 Total Inserted: {total_inserted:,}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())