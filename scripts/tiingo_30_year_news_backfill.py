#!/usr/bin/env python3
"""
Tiingo 30-Year Historical News Backfill

Comprehensive historical news collection from Tiingo API:
- Uses existing Tiingo news infrastructure from turbo_news_backfill.py
- Processes in yearly chunks with robust error handling
- Supports resumption and progress tracking
- Handles Tiingo's rate limits and API structure

Environment Variables:
- TIINGO_API_KEY: Required Tiingo API key
- NEWS_START_YEAR: Start year (YYYY), defaults to 1995 (30 years ago)
- NEWS_END_YEAR: End year (YYYY), defaults to current year
- NEWS_SYMBOL_LIMIT: Max symbols to process, defaults to 100
- NEWS_RESUME_FROM_YEAR: Resume from specific year if interrupted
- NEWS_PRIORITY_SYMBOLS: Comma-separated high-priority symbols
"""

import os
import sys
import asyncio
import aiohttp
import asyncpg
import json
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Any
import time

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TiingoNewsCollector:
    """30-year Tiingo news collector with strategic processing"""
    
    def __init__(self, api_key: str, max_concurrent: int = 10):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.start_time = datetime.now()
        
        # Progress tracking
        self.total_articles_collected = 0
        self.total_articles_inserted = 0
        self.years_completed = 0
        self.symbols_completed = 0
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=25)
        timeout = aiohttp.ClientTimeout(total=120)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_priority_symbols(self, pool: asyncpg.Pool, limit: int = 100) -> List[str]:
        """Get high-priority symbols from Tiingo instruments"""
        async with pool.acquire() as conn:
            # Check for specific priority symbols
            priority_symbols_env = os.getenv('NEWS_PRIORITY_SYMBOLS')
            if priority_symbols_env:
                priority_list = [s.strip().upper() for s in priority_symbols_env.split(',')]
                logger.info(f"🎯 Using specified priority symbols: {priority_list[:10]}...")
                return priority_list[:limit]
            
            # Get from Tiingo instruments table if available
            try:
                rows = await conn.fetch("""
                    SELECT symbol FROM dev_instrument_tiingo 
                    WHERE active = true 
                    ORDER BY 
                      CASE 
                        WHEN symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'V') THEN 1
                        WHEN LENGTH(symbol) <= 3 THEN 2
                        ELSE 3
                      END,
                      symbol 
                    LIMIT $1
                """, limit)
                if rows:
                    symbols = [row['symbol'] for row in rows]
                    logger.info(f"📈 Selected {len(symbols)} symbols from Tiingo instruments: {symbols[:10]}")
                    return symbols
            except:
                logger.info("📊 Tiingo instruments table not found, using default symbols")
            
            # Fallback to major symbols
            default_symbols = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'V',
                'BAC', 'WMT', 'PG', 'MA', 'HD', 'UNH', 'DIS', 'ADBE', 'NFLX', 'CRM',
                'PYPL', 'INTC', 'ABT', 'VZ', 'KO', 'NKE', 'MRK', 'T', 'PFE', 'CMCSA'
            ]
            return default_symbols[:limit]

    async def fetch_news_for_symbol_year(self, symbol: str, year: int) -> List[Dict[str, Any]]:
        """Fetch Tiingo news for a specific symbol and year"""
        async with self.semaphore:
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            
            url = "https://api.tiingo.com/tiingo/news"
            params = {
                'tickers': symbol,
                'startDate': start_date,
                'endDate': end_date,
                'token': self.api_key,
                'limit': 1000
            }
            
            all_articles = []
            
            for attempt in range(3):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Tiingo returns articles directly (not in 'results')
                            if isinstance(data, list):
                                articles = data
                            else:
                                articles = data.get('results', data.get('articles', []))
                            
                            # Convert to standardized format
                            for article in articles:
                                standardized = self.standardize_tiingo_article(article)
                                if standardized:
                                    all_articles.append(standardized)
                            
                            logger.debug(f"✅ {symbol} {year}: {len(all_articles)} articles")
                            return all_articles
                            
                        elif response.status == 429:
                            wait_time = 2 + (attempt * 3)  # Tiingo rate limiting
                            logger.warning(f"⚠️ Tiingo rate limit for {symbol} {year}, waiting {wait_time}s...")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.warning(f"⚠️ {symbol} {year}: HTTP {response.status}")
                            if attempt == 2:
                                return []
                            await asyncio.sleep(2)
                            
                except Exception as e:
                    logger.warning(f"⚠️ {symbol} {year} attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
            
            return all_articles

    def standardize_tiingo_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Tiingo article to standardized format"""
        try:
            # Parse published date
            published_utc = None
            if article.get('publishedDate'):
                try:
                    published_utc = datetime.fromisoformat(
                        article['publishedDate'].replace('Z', '+00:00')
                    )
                except:
                    pass
            
            return {
                'tiingo_id': str(article.get('id', '')),
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'author': article.get('author', ''),
                'published_utc': published_utc,
                'article_url': article.get('url', ''),
                'image_url': article.get('imageUrl', ''),
                'source': article.get('source', ''),
                'tickers': article.get('tickers', []),
                'tags': article.get('tags', []),
                'data': article  # Store full original data
            }
        except Exception as e:
            logger.warning(f"⚠️ Failed to standardize Tiingo article: {e}")
            return None

    async def process_symbol_year_batch(self, pool: asyncpg.Pool, symbols: List[str], year: int) -> Dict[str, int]:
        """Process a batch of symbols for a specific year"""
        logger.info(f"📦 Processing {len(symbols)} symbols for year {year}")
        
        results = {
            'total_articles': 0,
            'total_inserted': 0,
            'symbols_processed': 0,
            'errors': 0
        }
        
        for symbol in symbols:
            try:
                logger.info(f"  📰 {symbol} {year}...")
                articles = await self.fetch_news_for_symbol_year(symbol, year)
                results['total_articles'] += len(articles)
                
                if articles:
                    inserted = await self.insert_tiingo_news_articles(pool, articles)
                    results['total_inserted'] += inserted
                    logger.info(f"  ✅ {symbol} {year}: {len(articles)} articles, {inserted} inserted")
                else:
                    logger.info(f"  ➖ {symbol} {year}: No articles found")
                
                results['symbols_processed'] += 1
                
                # Tiingo rate limiting (more conservative than Polygon)
                await asyncio.sleep(2)  # 2-second delays for Tiingo
                
            except Exception as e:
                logger.error(f"  ❌ {symbol} {year}: {e}")
                results['errors'] += 1
        
        self.total_articles_collected += results['total_articles']
        self.total_articles_inserted += results['total_inserted']
        
        return results

    async def insert_tiingo_news_articles(self, pool: asyncpg.Pool, articles: List[Dict[str, Any]]) -> int:
        """Insert Tiingo news articles into dev_news_tiingo table"""
        if not articles:
            return 0
        
        # Ensure table exists
        await self.ensure_tiingo_news_table(pool)
        
        inserted = 0
        async with pool.acquire() as conn:
            for article in articles:
                try:
                    await conn.execute("""
                        INSERT INTO dev_news_tiingo (
                            tiingo_id, title, description, author, published_date,
                            article_url, image_url, source, tickers, tags, data
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (tiingo_id) DO UPDATE SET
                            title = EXCLUDED.title,
                            description = EXCLUDED.description,
                            updated_at = CURRENT_TIMESTAMP
                    """, 
                        article.get('tiingo_id'),
                        article.get('title', ''),
                        article.get('description', ''),
                        article.get('author', ''),
                        article.get('published_utc'),
                        article.get('article_url', ''),
                        article.get('image_url', ''),
                        article.get('source', ''),
                        article.get('tickers', []),
                        article.get('tags', []),
                        json.dumps(article.get('data', {}))
                    )
                    inserted += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to insert Tiingo article {article.get('tiingo_id', 'unknown')}: {e}")
        
        return inserted

    async def ensure_tiingo_news_table(self, pool: asyncpg.Pool):
        """Ensure dev_news_tiingo table exists"""
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_news_tiingo (
                    id SERIAL PRIMARY KEY,
                    tiingo_id VARCHAR(255) UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    author VARCHAR(500),
                    published_date TIMESTAMP WITH TIME ZONE,
                    article_url TEXT,
                    image_url TEXT,
                    source VARCHAR(255),
                    tickers TEXT[],
                    tags TEXT[],
                    data JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_news_tiingo_published_date ON dev_news_tiingo(published_date);
                CREATE INDEX IF NOT EXISTS idx_news_tiingo_tickers ON dev_news_tiingo USING GIN(tickers);
                CREATE INDEX IF NOT EXISTS idx_news_tiingo_tags ON dev_news_tiingo USING GIN(tags);
                CREATE INDEX IF NOT EXISTS idx_news_tiingo_tiingo_id ON dev_news_tiingo(tiingo_id);
                CREATE INDEX IF NOT EXISTS idx_news_tiingo_source ON dev_news_tiingo(source);
            """)

    def log_progress_summary(self, year: int, symbols_processed: int, total_symbols: int):
        """Log comprehensive progress summary"""
        elapsed = datetime.now() - self.start_time
        hours_elapsed = elapsed.total_seconds() / 3600
        
        logger.info("=" * 80)
        logger.info("📊 30-YEAR TIINGO NEWS BACKFILL PROGRESS")
        logger.info("=" * 80)
        logger.info(f"📅 Current Year: {year}")
        logger.info(f"📈 Symbols Processed This Year: {symbols_processed}/{total_symbols}")
        logger.info(f"📰 Total Articles Collected: {self.total_articles_collected:,}")
        logger.info(f"💾 Total Articles Inserted: {self.total_articles_inserted:,}")
        logger.info(f"⏱️  Elapsed Time: {elapsed}")
        
        if hours_elapsed > 0:
            articles_per_hour = self.total_articles_collected / hours_elapsed
            logger.info(f"🚀 Collection Rate: {articles_per_hour:.1f} articles/hour")
            
            # Estimate completion time
            remaining_symbols = total_symbols - symbols_processed
            if symbols_processed > 0:
                avg_time_per_symbol = elapsed.total_seconds() / symbols_processed
                estimated_remaining = timedelta(seconds=remaining_symbols * avg_time_per_symbol)
                logger.info(f"⏳ Estimated Year Completion: {estimated_remaining}")
        
        logger.info("=" * 80)

async def main():
    """Main execution function"""
    
    # Get configuration from environment
    api_key = os.getenv('TIINGO_API_KEY')
    if not api_key:
        logger.error("❌ TIINGO_API_KEY environment variable required")
        sys.exit(1)
    
    # Year configuration
    current_year = datetime.now().year
    start_year = int(os.getenv('NEWS_START_YEAR', str(current_year - 30)))  # 30 years ago
    end_year = int(os.getenv('NEWS_END_YEAR', str(current_year)))
    symbol_limit = int(os.getenv('NEWS_SYMBOL_LIMIT', '100'))
    resume_year = int(os.getenv('NEWS_RESUME_FROM_YEAR', str(start_year)))
    
    logger.info(f"🚀 Starting 30-Year Tiingo News Backfill")
    logger.info(f"📅 Year Range: {start_year} to {end_year} ({end_year - start_year + 1} years)")
    logger.info(f"📊 Max Symbols: {symbol_limit:,}")
    logger.info(f"🔄 Resume From Year: {resume_year}")
    logger.info(f"⚠️  This is a MASSIVE operation - estimated weeks of processing time")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with TiingoNewsCollector(api_key) as collector:
            # Get priority symbols
            symbols = await collector.get_priority_symbols(pool, symbol_limit)
            logger.info(f"📈 Processing {len(symbols)} priority symbols")
            
            # Process year by year, working backwards from recent to historical
            for year in range(end_year, start_year - 1, -1):
                if year < resume_year:
                    logger.info(f"⏭️  Skipping {year} (before resume year {resume_year})")
                    continue
                
                logger.info(f"📅 Starting Year {year} ({end_year - year + 1}/{end_year - start_year + 1})")
                
                # Process all symbols for this year
                year_results = await collector.process_symbol_year_batch(pool, symbols, year)
                
                logger.info(f"✅ Year {year} Complete:")
                logger.info(f"  📰 Articles: {year_results['total_articles']:,}")
                logger.info(f"  💾 Inserted: {year_results['total_inserted']:,}")
                logger.info(f"  📊 Symbols: {year_results['symbols_processed']}/{len(symbols)}")
                logger.info(f"  ❌ Errors: {year_results['errors']}")
                
                collector.years_completed += 1
                
                # Progress summary every year
                collector.log_progress_summary(year, year_results['symbols_processed'], len(symbols))
                
                # Brief pause between years
                await asyncio.sleep(5)
        
        await pool.close()
        
        logger.info("🎉 30-Year Tiingo News Backfill Complete!")
        logger.info(f"📰 Total Articles Collected: {collector.total_articles_collected:,}")
        logger.info(f"💾 Total Articles Inserted: {collector.total_articles_inserted:,}")
        logger.info(f"📅 Years Completed: {collector.years_completed}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())