#!/usr/bin/env python3
"""
EODHD Financial Events Collector

Professional-grade financial events collector using EODHD's comprehensive APIs.
Collects earnings calendar, corporate actions, insider transactions, and economic events.

Features:
- Earnings calendar with upcoming earnings and trends
- Corporate actions (splits, dividends, IPOs)
- Insider transactions (SEC Form 4)
- Economic events data
- Financial news with sentiment
- Rate limiting compliance (EODHD: 20 calls/minute free, higher for paid)

Usage:
    python eodhd_events_collector.py --years 5 --limit 100
    python eodhd_events_collector.py --symbols AAPL,MSFT --earnings-only
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
logger = logging.getLogger("eodhd_events_collector")

class EODHDEventsCollector:
    """
    EODHD financial events collector with comprehensive event coverage.
    
    Capabilities:
    - Earnings calendar and IPO tracking
    - Corporate actions (splits, dividends)
    - Insider transactions (SEC Form 4)
    - Economic events data
    - Financial news sentiment
    - Historical depth up to 30 years
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://eodhd.com/api"
        
        # Rate limiting (EODHD free: 20 calls/minute, paid: higher)
        self.request_delay = 3  # 3 seconds = 20 calls/minute (conservative)
        
        # Statistics tracking
        self.stats = {
            'total_symbols': 0,
            'processed_symbols': 0,
            'earnings_events': 0,
            'corporate_actions': 0,
            'insider_events': 0,
            'economic_events': 0,
            'news_events': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_symbols': 0
        }
        
        logger.info("📊 EODHD Financial Events Collector initialized")
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
        
        # Get US-listed stocks suitable for EODHD
        instruments = await conn.fetch(f"""
            SELECT DISTINCT symbol 
            FROM dev_instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'NYSE MKT')
              AND LENGTH(symbol) <= 6  -- EODHD works well with standard tickers
            ORDER BY symbol
            {limit_clause}
        """)
        
        symbols = [inst['symbol'] for inst in instruments]
        self.stats['total_symbols'] = len(symbols)
        logger.info(f"📊 Found {len(symbols)} symbols for EODHD collection")
        return symbols

    def make_api_request(self, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make EODHD API request with error handling."""
        params = {
            'api_token': self.api_key,
            'fmt': 'json',
            **kwargs
        }
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, params=params, timeout=30)
            self.stats['api_calls'] += 1
            
            if response.status_code == 429:
                logger.warning("Rate limit hit, waiting...")
                time.sleep(60)
                return self.make_api_request(endpoint, **kwargs)
            
            if response.status_code != 200:
                logger.error(f"EODHD API error: {response.status_code} - {response.text[:200]}")
                self.stats['errors'] += 1
                return None
            
            data = response.json()
            
            # Check for API error messages
            if isinstance(data, dict) and 'error' in data:
                logger.error(f"EODHD API Error: {data['error']}")
                self.stats['errors'] += 1
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"Error making API request to {endpoint}: {e}")
            self.stats['errors'] += 1
            return None

    def get_earnings_calendar(self, date_from: str = None, date_to: str = None) -> List[Dict]:
        """Get earnings calendar data."""
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        
        data = self.make_api_request('calendar/earnings', 
                                   from_date=date_from, 
                                   to_date=date_to)
        
        if not data or not isinstance(data, dict):
            return []
        
        earnings_data = data.get('earnings', [])
        logger.debug(f"Retrieved {len(earnings_data)} earnings calendar entries")
        return earnings_data

    def get_corporate_actions(self, symbol: str, date_from: str = None) -> List[Dict]:
        """Get corporate actions (splits, dividends)."""
        if not date_from:
            date_from = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')  # 5 years
        
        # Get dividends
        dividends = self.make_api_request(f'div/{symbol}.US', from_date=date_from)
        
        # Get splits  
        splits = self.make_api_request(f'splits/{symbol}.US', from_date=date_from)
        
        actions = []
        
        if dividends and isinstance(dividends, list):
            for div in dividends:
                actions.append({
                    'type': 'dividend',
                    'symbol': symbol,
                    'data': div
                })
        
        if splits and isinstance(splits, list):
            for split in splits:
                actions.append({
                    'type': 'split',
                    'symbol': symbol,
                    'data': split
                })
        
        return actions

    def get_insider_transactions(self, symbol: str, date_from: str = None) -> List[Dict]:
        """Get insider transactions (SEC Form 4)."""
        if not date_from:
            date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')  # 1 year
        
        data = self.make_api_request(f'insider-transactions', 
                                   code=f'{symbol}.US',
                                   from_date=date_from)
        
        if not data or not isinstance(data, list):
            return []
        
        return data

    def get_financial_news(self, symbol: str, date_from: str = None, limit: int = 50) -> List[Dict]:
        """Get financial news with sentiment."""
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')  # 30 days
        
        data = self.make_api_request('news', 
                                   s=f'{symbol}.US',
                                   from_date=date_from,
                                   limit=limit)
        
        if not data or not isinstance(data, list):
            return []
        
        return data

    def calculate_sentiment_and_impact(self, event_data: Dict, event_type: str) -> tuple:
        """Calculate sentiment and impact score based on event data."""
        sentiment = 'neutral'
        impact_score = 0.0
        
        if event_type == 'earnings':
            # For earnings, check if actual vs estimate data is available
            actual = event_data.get('actual_eps')
            estimate = event_data.get('estimate_eps')
            
            if actual is not None and estimate is not None:
                try:
                    actual_val = float(actual)
                    estimate_val = float(estimate)
                    if estimate_val != 0:
                        surprise_pct = ((actual_val - estimate_val) / abs(estimate_val)) * 100
                        if surprise_pct > 5.0:
                            sentiment = 'positive'
                            impact_score = min(surprise_pct / 50.0, 1.0)
                        elif surprise_pct < -5.0:
                            sentiment = 'negative'
                            impact_score = max(surprise_pct / 50.0, -1.0)
                except (ValueError, TypeError):
                    pass
        
        elif event_type == 'dividend':
            # Dividends are generally positive
            sentiment = 'positive'
            impact_score = 0.3
        
        elif event_type == 'split':
            # Stock splits are generally positive
            sentiment = 'positive' 
            impact_score = 0.5
        
        elif event_type == 'insider':
            # Insider buying is positive, selling could be neutral/negative
            transaction_type = event_data.get('transactionType', '').lower()
            if 'buy' in transaction_type or 'purchase' in transaction_type:
                sentiment = 'positive'
                impact_score = 0.2
            elif 'sell' in transaction_type or 'sale' in transaction_type:
                sentiment = 'negative'
                impact_score = -0.1
        
        return sentiment, impact_score

    async def insert_financial_event(self, conn, symbol: str, event_type: str, 
                                   event_data: Dict, event_datetime: datetime) -> Optional[int]:
        """Insert financial event with idempotent operations."""
        try:
            # Create unique event ID
            event_id = f"eodhd_{event_type}_{symbol}_{event_datetime.strftime('%Y%m%d')}"
            
            # Calculate sentiment and impact
            sentiment, impact_score = self.calculate_sentiment_and_impact(event_data, event_type)
            
            # Determine importance based on event type
            importance_mapping = {
                'earnings': 'high',
                'dividend': 'medium', 
                'split': 'high',
                'insider': 'medium',
                'news': 'low'
            }
            importance = importance_mapping.get(event_type, 'medium')
            
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
                    vendor,
                    raw_data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (event_id) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    raw_data = EXCLUDED.raw_data
                RETURNING id
            """, 
            event_id,
            symbol,
            event_type,
            event_datetime,
            f"{symbol} {event_type.title()} Event",
            f"EODHD {event_type} data for {symbol}",
            sentiment,
            impact_score,
            importance,
            'eodhd',
            json.dumps(event_data)
            )
            
            return financial_event_id
            
        except Exception as e:
            logger.error(f"Error inserting financial event for {symbol}: {e}")
            self.stats['errors'] += 1
            return None

    async def process_earnings_events(self, conn, earnings_data: List[Dict]):
        """Process earnings calendar events."""
        events_inserted = 0
        
        for earning in earnings_data:
            try:
                symbol = earning.get('code', '').replace('.US', '')
                if not symbol:
                    continue
                
                event_date = earning.get('report_date')
                if not event_date:
                    continue
                
                event_datetime = datetime.strptime(event_date, '%Y-%m-%d')
                
                event_id = await self.insert_financial_event(
                    conn, symbol, 'earnings', earning, event_datetime
                )
                
                if event_id:
                    # Insert earnings-specific data if we have the table
                    try:
                        await conn.execute("""
                            INSERT INTO dev_earnings_events (
                                financial_event_id,
                                symbol,
                                report_period,
                                report_type
                            ) VALUES ($1, $2, $3, $4)
                            ON CONFLICT (financial_event_id) DO UPDATE SET
                                updated_at = CURRENT_TIMESTAMP
                        """,
                        event_id,
                        symbol,
                        event_datetime.date(),
                        'preliminary'
                        )
                    except Exception as e:
                        logger.debug(f"Could not insert earnings details for {symbol}: {e}")
                    
                    events_inserted += 1
                    logger.debug(f"💾 Inserted earnings event for {symbol} ({event_date})")
                
            except Exception as e:
                logger.error(f"Error processing earnings event: {e}")
                continue
        
        self.stats['earnings_events'] += events_inserted
        return events_inserted

    async def collect_symbol_events(self, conn, symbol: str, collect_earnings: bool = True,
                                  collect_corporate_actions: bool = True,
                                  collect_insider: bool = True,
                                  collect_news: bool = False):
        """Collect all events for a specific symbol."""
        try:
            logger.info(f"📈 Collecting EODHD events for {symbol}...")
            events_collected = 0
            
            # Collect corporate actions (dividends, splits)
            if collect_corporate_actions:
                actions = self.get_corporate_actions(symbol)
                for action in actions:
                    try:
                        action_data = action['data']
                        action_type = action['type']
                        
                        # Get date from action data
                        date_field = 'date' if 'date' in action_data else 'ex_date'
                        if date_field not in action_data:
                            continue
                        
                        event_datetime = datetime.strptime(action_data[date_field], '%Y-%m-%d')
                        
                        event_id = await self.insert_financial_event(
                            conn, symbol, 'corporate_action', action_data, event_datetime
                        )
                        
                        if event_id:
                            events_collected += 1
                            logger.debug(f"💾 Inserted {action_type} event for {symbol}")
                    
                    except Exception as e:
                        logger.error(f"Error processing {action_type} for {symbol}: {e}")
                        continue
                
                time.sleep(self.request_delay)  # Rate limiting
            
            # Collect insider transactions
            if collect_insider:
                insider_data = self.get_insider_transactions(symbol)
                for transaction in insider_data:
                    try:
                        transaction_date = transaction.get('date')
                        if not transaction_date:
                            continue
                        
                        event_datetime = datetime.strptime(transaction_date, '%Y-%m-%d')
                        
                        event_id = await self.insert_financial_event(
                            conn, symbol, 'insider_trading', transaction, event_datetime
                        )
                        
                        if event_id:
                            events_collected += 1
                            logger.debug(f"💾 Inserted insider event for {symbol}")
                    
                    except Exception as e:
                        logger.error(f"Error processing insider transaction for {symbol}: {e}")
                        continue
                
                time.sleep(self.request_delay)  # Rate limiting
            
            # Collect financial news (optional, can be noisy)
            if collect_news:
                news_data = self.get_financial_news(symbol)
                for news_item in news_data[:10]:  # Limit to 10 recent news items
                    try:
                        news_date = news_item.get('date')
                        if not news_date:
                            continue
                        
                        event_datetime = datetime.strptime(news_date, '%Y-%m-%d %H:%M:%S')
                        
                        event_id = await self.insert_financial_event(
                            conn, symbol, 'announcement', news_item, event_datetime
                        )
                        
                        if event_id:
                            events_collected += 1
                            logger.debug(f"💾 Inserted news event for {symbol}")
                    
                    except Exception as e:
                        logger.error(f"Error processing news for {symbol}: {e}")
                        continue
                
                time.sleep(self.request_delay)  # Rate limiting
            
            self.stats['processed_symbols'] += 1
            logger.info(f"✅ Processed {symbol}: {events_collected} total events")
            
            return events_collected
            
        except Exception as e:
            logger.error(f"❌ Failed to collect events for {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def run_collection(self, years_back: int = 5, limit: Optional[int] = None,
                           specific_symbols: Optional[List[str]] = None,
                           earnings_only: bool = False):
        """Run the complete events collection process."""
        logger.info("🚀 Starting EODHD financial events collection...")
        logger.info(f"📅 Collecting data with {years_back} years historical depth")
        
        conn = await self.get_database_connection()
        
        try:
            # First collect earnings calendar (market-wide)
            logger.info("📊 Collecting earnings calendar...")
            earnings_data = self.get_earnings_calendar()
            if earnings_data:
                await self.process_earnings_events(conn, earnings_data)
                logger.info(f"✅ Processed {len(earnings_data)} earnings calendar events")
            
            # Then collect symbol-specific events
            symbols = await self.get_symbols_for_collection(conn, limit, specific_symbols)
            
            if not symbols:
                logger.warning("❌ No symbols found for collection")
                return
            
            logger.info(f"📊 Processing {len(symbols)} symbols for detailed events")
            
            # Process each symbol
            for i, symbol in enumerate(symbols, 1):
                try:
                    await self.collect_symbol_events(
                        conn, symbol,
                        collect_earnings=False,  # Already collected from calendar
                        collect_corporate_actions=not earnings_only,
                        collect_insider=not earnings_only,
                        collect_news=False  # Skip news for now (too noisy)
                    )
                    
                    # Progress logging
                    if i % 50 == 0 or i == len(symbols):
                        progress = (i / len(symbols)) * 100
                        total_events = (self.stats['earnings_events'] + 
                                      self.stats['corporate_actions'] + 
                                      self.stats['insider_events'])
                        logger.info(f"📊 Progress: {i:,}/{len(symbols):,} ({progress:.1f}%) - "
                                  f"{total_events:,} events collected")
                        
                except Exception as e:
                    logger.error(f"❌ Critical error processing {symbol}: {e}")
                    continue
            
        finally:
            await conn.close()
    
    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 EODHD FINANCIAL EVENTS COLLECTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Symbols: {self.stats['total_symbols']:,}")
        logger.info(f"  Processed Symbols: {self.stats['processed_symbols']:,}")
        logger.info(f"  Skipped Symbols: {self.stats['skipped_symbols']:,}")
        logger.info(f"  API Calls: {self.stats['api_calls']:,}")
        logger.info(f"  Errors: {self.stats['errors']:,}")
        logger.info("")
        logger.info(f"📈 EVENTS BREAKDOWN:")
        logger.info(f"  Earnings Events: {self.stats['earnings_events']:,}")
        logger.info(f"  Corporate Actions: {self.stats['corporate_actions']:,}")
        logger.info(f"  Insider Events: {self.stats['insider_events']:,}")
        logger.info(f"  Economic Events: {self.stats['economic_events']:,}")
        logger.info(f"  News Events: {self.stats['news_events']:,}")
        
        total_events = (self.stats['earnings_events'] + self.stats['corporate_actions'] + 
                       self.stats['insider_events'] + self.stats['economic_events'] + 
                       self.stats['news_events'])
        logger.info(f"  Total Events: {total_events:,}")
        
        success_rate = (self.stats['processed_symbols'] / self.stats['total_symbols'] * 100) if self.stats['total_symbols'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="EODHD financial events collector")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of symbols to process')
    parser.add_argument('--years', type=int, default=5, help='Years of historical data to collect')
    parser.add_argument('--symbols', type=str, default=None, 
                       help='Comma-separated specific symbols to process')
    parser.add_argument('--earnings-only', action='store_true', 
                       help='Collect only earnings data (skip corporate actions and insider)')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Get EODHD API key
        eodhd_api_key = os.environ.get("EODHD_API_KEY")
        if not eodhd_api_key:
            logger.error("❌ EODHD_API_KEY environment variable not set")
            sys.exit(1)
        
        logger.info("✅ EODHD API key found")
        
        # Parse specific symbols if provided
        specific_symbols = None
        if args.symbols:
            specific_symbols = [s.strip() for s in args.symbols.split(',')]
            logger.info(f"📊 Processing specific symbols: {specific_symbols}")
        
        # Initialize collector
        collector = EODHDEventsCollector(eodhd_api_key)
        
        # Run collection
        await collector.run_collection(
            years_back=args.years,
            limit=args.limit,
            specific_symbols=specific_symbols,
            earnings_only=args.earnings_only
        )
        
        # Log final summary
        collector.log_final_summary()
        
        logger.info("✅ EODHD financial events collection complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run EODHD collection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())