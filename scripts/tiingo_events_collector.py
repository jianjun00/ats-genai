#!/usr/bin/env python3
"""
Tiingo Financial Events Collector

Professional-grade financial events collector using Tiingo's news and fundamentals APIs.
Collects financial news with sentiment analysis and fundamental events.

Features:
- Financial news with sentiment analysis
- Fundamental data events (earnings releases)
- News classification by importance and market impact
- Rate limiting compliance (Tiingo: 1000 requests/hour free, higher for paid)

Usage:
    python tiingo_events_collector.py --years 1 --limit 100
    python tiingo_events_collector.py --symbols AAPL,MSFT --news-only
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
import time
import json
import argparse
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tiingo_events_collector")

class TiingoEventsCollector:
    """
    Tiingo financial events collector focusing on news and fundamental events.
    
    Capabilities:
    - Financial news with sentiment classification
    - Market-moving news identification
    - Company-specific news filtering
    - News impact scoring
    - Rate limiting compliance
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tiingo.com/tiingo"
        
        # Rate limiting (Tiingo free: 1000 requests/hour, paid: higher)
        self.request_delay = 4  # 4 seconds = ~900 requests/hour (conservative)
        
        # Statistics tracking
        self.stats = {
            'total_symbols': 0,
            'processed_symbols': 0,
            'news_events': 0,
            'market_moving_news': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_symbols': 0
        }
        
        logger.info("📊 Tiingo Financial Events Collector initialized")
        logger.info(f"   Rate limit: {3600/self.request_delay:.1f} requests/hour")

    async def get_database_connection(self):
        """Get database connection (Docker-compatible)."""
        return await asyncpg.connect(
            host='postgres',  # Docker service name
            port=5432,        # Internal Docker port
            user='postgres',
            password='dev_password',
            database='dev_db'
        )

    async def get_symbols_for_collection(self, conn, limit=None, specific_symbols=None):
        """Get symbols for events collection."""
        if specific_symbols:
            symbols = [s.strip().upper() for s in specific_symbols if s.strip()]
            self.stats['total_symbols'] = len(symbols)
            logger.info(f"📊 Using specific symbols: {symbols}")
            return symbols
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # Get high-cap US stocks suitable for news collection
        instruments = await conn.fetch(f"""
            SELECT DISTINCT symbol 
            FROM dev_instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA')
              AND LENGTH(symbol) <= 5  -- Focus on major stocks
            ORDER BY symbol
            {limit_clause}
        """)
        
        symbols = [inst['symbol'] for inst in instruments]
        self.stats['total_symbols'] = len(symbols)
        logger.info(f"📊 Found {len(symbols)} symbols for Tiingo news collection")
        return symbols

    def make_api_request(self, endpoint: str, **kwargs) -> Optional[Any]:
        """Make Tiingo API request with error handling."""
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Token {self.api_key}'
        }
        
        params = {**kwargs}
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            self.stats['api_calls'] += 1
            
            if response.status_code == 429:
                logger.warning("Rate limit hit, waiting...")
                time.sleep(60)
                return self.make_api_request(endpoint, **kwargs)
            
            if response.status_code == 404:
                logger.debug(f"No data found for {endpoint}")
                return None
            
            if response.status_code != 200:
                logger.error(f"Tiingo API error: {response.status_code} - {response.text[:200]}")
                self.stats['errors'] += 1
                return None
            
            data = response.json()
            return data
            
        except Exception as e:
            logger.error(f"Error making API request to {endpoint}: {e}")
            self.stats['errors'] += 1
            return None

    def get_financial_news(self, symbol: str = None, start_date: str = None, 
                          end_date: str = None, limit: int = 1000) -> List[Dict]:
        """Get financial news data."""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        endpoint = "news"
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'limit': limit,
            'sortBy': 'publishedDate'
        }
        
        if symbol:
            params['tickers'] = symbol
        
        data = self.make_api_request(endpoint, **params)
        
        if not data or not isinstance(data, list):
            return []
        
        logger.debug(f"Retrieved {len(data)} news articles for {symbol or 'market'}")
        return data

    def calculate_news_sentiment_and_impact(self, news_item: Dict) -> tuple:
        """Calculate sentiment and impact score from news data."""
        sentiment = 'neutral'
        impact_score = 0.0
        
        # Get title and description for sentiment analysis
        title = news_item.get('title', '').lower()
        description = news_item.get('description', '').lower()
        text = f"{title} {description}"
        
        # Simple keyword-based sentiment analysis
        positive_keywords = ['beat', 'beats', 'exceed', 'strong', 'growth', 'profit', 
                           'gain', 'up', 'rise', 'upgrade', 'buy', 'bullish', 'surge']
        negative_keywords = ['miss', 'misses', 'weak', 'decline', 'loss', 'fall', 
                           'down', 'drop', 'downgrade', 'sell', 'bearish', 'plunge']
        
        positive_count = sum(1 for word in positive_keywords if word in text)
        negative_count = sum(1 for word in negative_keywords if word in text)
        
        if positive_count > negative_count:
            sentiment = 'positive'
            impact_score = min(0.3 + (positive_count - negative_count) * 0.1, 1.0)
        elif negative_count > positive_count:
            sentiment = 'negative'
            impact_score = max(-0.3 - (negative_count - positive_count) * 0.1, -1.0)
        
        # Boost impact for earnings-related news
        if any(word in text for word in ['earnings', 'quarterly', 'revenue', 'eps']):
            impact_score *= 1.5
            impact_score = min(max(impact_score, -1.0), 1.0)  # Keep in bounds
        
        return sentiment, impact_score

    def determine_importance_level(self, news_item: Dict) -> str:
        """Determine importance level based on news content."""
        title = news_item.get('title', '').lower()
        description = news_item.get('description', '').lower()
        text = f"{title} {description}"
        
        # High importance keywords
        high_importance = ['earnings', 'acquisition', 'merger', 'ceo', 'bankruptcy', 
                          'lawsuit', 'fda approval', 'regulation', 'guidance']
        
        # Medium importance keywords  
        medium_importance = ['revenue', 'profit', 'partnership', 'contract', 'product launch']
        
        if any(word in text for word in high_importance):
            return 'high'
        elif any(word in text for word in medium_importance):
            return 'medium'
        else:
            return 'low'

    def is_market_moving_news(self, news_item: Dict) -> bool:
        """Determine if news is potentially market-moving."""
        title = news_item.get('title', '').lower()
        description = news_item.get('description', '').lower()
        text = f"{title} {description}"
        
        market_moving_keywords = [
            'earnings', 'beats', 'misses', 'guidance', 'acquisition', 'merger',
            'fda', 'approval', 'lawsuit', 'settlement', 'ceo', 'resignation',
            'bankruptcy', 'dividend', 'split', 'upgrade', 'downgrade'
        ]
        
        return any(word in text for word in market_moving_keywords)

    async def insert_news_event(self, conn, news_item: Dict, symbols: List[str]) -> Optional[int]:
        """Insert news event with idempotent operations."""
        try:
            # Get news metadata
            article_id = news_item.get('id')
            if not article_id:
                return None
            
            published_date = news_item.get('publishedDate')
            if not published_date:
                return None
            
            # Parse published date
            event_datetime = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
            
            # Calculate sentiment and impact
            sentiment, impact_score = self.calculate_news_sentiment_and_impact(news_item)
            
            # Determine importance and market impact
            importance = self.determine_importance_level(news_item)
            market_moving = self.is_market_moving_news(news_item)
            
            # Create events for each ticker mentioned
            event_ids = []
            for symbol in symbols:
                # Create unique event ID
                event_id = f"tiingo_news_{article_id}_{symbol}"
                
                # Insert main financial event
                financial_event_id = await conn.fetchval("""
                    INSERT INTO dev_financial_events (
                        event_id,
                        symbol,
                        event_type,
                        event_datetime,
                        title,
                        description,
                        sentiment,
                        impact_score,
                        importance_level,
                        market_moving,
                        vendor,
                        source_url,
                        raw_data
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (event_id) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP,
                        raw_data = EXCLUDED.raw_data
                    RETURNING id
                """, 
                event_id,
                symbol,
                'announcement',
                event_datetime,
                news_item.get('title', '')[:255],  # Truncate if too long
                news_item.get('description', '')[:500],  # Truncate if too long
                sentiment,
                impact_score,
                importance,
                market_moving,
                'tiingo',
                news_item.get('url', ''),
                json.dumps(news_item)
                )
                
                if financial_event_id:
                    event_ids.append(financial_event_id)
                    logger.debug(f"💾 Inserted news event for {symbol} - {news_item.get('title', '')[:50]}")
            
            return len(event_ids)
            
        except Exception as e:
            logger.error(f"Error inserting news event: {e}")
            self.stats['errors'] += 1
            return None

    async def collect_market_news(self, conn, days_back: int = 30, limit: int = 1000):
        """Collect market-wide financial news."""
        try:
            logger.info("📰 Collecting market-wide financial news...")
            
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            # Get general financial news (no specific ticker)
            news_data = self.get_financial_news(
                symbol=None, 
                start_date=start_date, 
                end_date=end_date,
                limit=limit
            )
            
            if not news_data:
                logger.warning("⚠️ No market news data received")
                return 0
            
            events_inserted = 0
            
            for news_item in news_data:
                try:
                    # Get tickers mentioned in the news
                    tickers = news_item.get('tickers', [])
                    if not tickers:
                        continue
                    
                    # Filter to only include our tracked symbols
                    valid_tickers = []
                    for ticker in tickers:
                        # Remove common suffixes and clean up
                        clean_ticker = ticker.replace('.US', '').replace('.TO', '').upper()
                        if len(clean_ticker) <= 5 and clean_ticker.isalpha():
                            valid_tickers.append(clean_ticker)
                    
                    if not valid_tickers:
                        continue
                    
                    # Insert event for relevant tickers
                    inserted_count = await self.insert_news_event(conn, news_item, valid_tickers)
                    if inserted_count:
                        events_inserted += inserted_count
                        
                        if self.is_market_moving_news(news_item):
                            self.stats['market_moving_news'] += inserted_count
                
                except Exception as e:
                    logger.error(f"Error processing news item: {e}")
                    continue
            
            self.stats['news_events'] += events_inserted
            logger.info(f"✅ Processed {len(news_data)} news articles, inserted {events_inserted} events")
            
            return events_inserted
            
        except Exception as e:
            logger.error(f"❌ Failed to collect market news: {e}")
            self.stats['errors'] += 1
            return 0

    async def collect_symbol_news(self, conn, symbol: str, days_back: int = 30):
        """Collect news for a specific symbol."""
        try:
            logger.info(f"📰 Collecting news for {symbol}...")
            
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            # Get symbol-specific news
            news_data = self.get_financial_news(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                limit=100
            )
            
            if not news_data:
                logger.debug(f"⚠️ No news data for {symbol}")
                self.stats['skipped_symbols'] += 1
                return 0
            
            events_inserted = 0
            
            for news_item in news_data:
                try:
                    inserted_count = await self.insert_news_event(conn, news_item, [symbol])
                    if inserted_count:
                        events_inserted += inserted_count
                        
                        if self.is_market_moving_news(news_item):
                            self.stats['market_moving_news'] += inserted_count
                
                except Exception as e:
                    logger.error(f"Error processing news for {symbol}: {e}")
                    continue
            
            self.stats['processed_symbols'] += 1
            self.stats['news_events'] += events_inserted
            
            logger.info(f"✅ Processed {symbol}: {events_inserted} news events")
            
            # Rate limiting
            time.sleep(self.request_delay)
            
            return events_inserted
            
        except Exception as e:
            logger.error(f"❌ Failed to collect news for {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def run_collection(self, days_back: int = 30, limit: Optional[int] = None,
                           specific_symbols: Optional[List[str]] = None,
                           market_news_only: bool = False):
        """Run the complete news collection process."""
        logger.info("🚀 Starting Tiingo financial events collection...")
        logger.info(f"📅 Collecting news from last {days_back} days")
        
        conn = await self.get_database_connection()
        
        try:
            # First collect market-wide news (most comprehensive)
            await self.collect_market_news(conn, days_back=days_back, limit=2000)
            
            # If not market-news-only, also collect symbol-specific news
            if not market_news_only:
                symbols = await self.get_symbols_for_collection(conn, limit, specific_symbols)
                
                if not symbols:
                    logger.warning("❌ No symbols found for collection")
                    return
                
                logger.info(f"📊 Processing {len(symbols)} symbols for detailed news")
                
                # Process each symbol
                for i, symbol in enumerate(symbols, 1):
                    try:
                        await self.collect_symbol_news(conn, symbol, days_back)
                        
                        # Progress logging
                        if i % 100 == 0 or i == len(symbols):
                            progress = (i / len(symbols)) * 100
                            logger.info(f"📊 Progress: {i:,}/{len(symbols):,} ({progress:.1f}%) - "
                                      f"{self.stats['news_events']:,} news events collected")
                            
                    except Exception as e:
                        logger.error(f"❌ Critical error processing {symbol}: {e}")
                        continue
            
        finally:
            await conn.close()
    
    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 TIINGO FINANCIAL EVENTS COLLECTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Symbols: {self.stats['total_symbols']:,}")
        logger.info(f"  Processed Symbols: {self.stats['processed_symbols']:,}")
        logger.info(f"  Skipped Symbols: {self.stats['skipped_symbols']:,}")
        logger.info(f"  API Calls: {self.stats['api_calls']:,}")
        logger.info(f"  Errors: {self.stats['errors']:,}")
        logger.info("")
        logger.info(f"📈 EVENTS BREAKDOWN:")
        logger.info(f"  News Events: {self.stats['news_events']:,}")
        logger.info(f"  Market-Moving News: {self.stats['market_moving_news']:,}")
        
        if self.stats['news_events'] > 0:
            market_moving_rate = (self.stats['market_moving_news'] / self.stats['news_events'] * 100)
            logger.info(f"  Market-Moving Rate: {market_moving_rate:.1f}%")
        
        success_rate = (self.stats['processed_symbols'] / self.stats['total_symbols'] * 100) if self.stats['total_symbols'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="Tiingo financial events collector")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of symbols to process')
    parser.add_argument('--days', type=int, default=30, help='Days of news history to collect')
    parser.add_argument('--symbols', type=str, default=None, 
                       help='Comma-separated specific symbols to process')
    parser.add_argument('--market-news-only', action='store_true', 
                       help='Collect only market-wide news (skip individual symbols)')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Get Tiingo API key
        tiingo_api_key = os.environ.get("TIINGO_API_KEY")
        if not tiingo_api_key:
            logger.error("❌ TIINGO_API_KEY environment variable not set")
            sys.exit(1)
        
        logger.info("✅ Tiingo API key found")
        
        # Parse specific symbols if provided
        specific_symbols = None
        if args.symbols:
            specific_symbols = [s.strip() for s in args.symbols.split(',')]
            logger.info(f"📊 Processing specific symbols: {specific_symbols}")
        
        # Initialize collector
        collector = TiingoEventsCollector(tiingo_api_key)
        
        # Run collection
        await collector.run_collection(
            days_back=args.days,
            limit=args.limit,
            specific_symbols=specific_symbols,
            market_news_only=args.market_news_only
        )
        
        # Log final summary
        collector.log_final_summary()
        
        logger.info("✅ Tiingo financial events collection complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run Tiingo collection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())