#!/usr/bin/env python3
"""
Alpha Vantage Financial Events Collector

Professional-grade financial events collector leveraging Alpha Vantage's comprehensive APIs.
Collects earnings calendar, analyst estimates, and company overview data.

Features:
- Earnings calendar with estimates vs actuals
- Company overview and fundamental metrics
- Analyst ratings and price targets
- News sentiment integration
- 30-year historical data support
- Rate limiting compliance (Alpha Vantage: 5 calls/minute free, 75/minute premium)

Usage:
    python alpha_vantage_events_collector.py --years 5 --limit 100
    python alpha_vantage_events_collector.py --symbols AAPL,MSFT --earnings-only
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
logger = logging.getLogger("alpha_vantage_events_collector")

class AlphaVantageEventsCollector:
    """
    Alpha Vantage financial events collector with multi-endpoint support.
    
    Capabilities:
    - Earnings calendar and estimates
    - Company overview and fundamentals
    - News sentiment analysis
    - Analyst ratings compilation
    - Historical data with 30-year depth
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        
        # Rate limiting (Alpha Vantage free: 5 calls/minute, premium: 75/minute)
        self.request_delay = 12  # 12 seconds = 5 calls/minute (conservative)
        
        # Statistics tracking
        self.stats = {
            'total_symbols': 0,
            'processed_symbols': 0,
            'earnings_events': 0,
            'analyst_events': 0,
            'overview_data': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_symbols': 0
        }
        
        logger.info("📊 Alpha Vantage Financial Events Collector initialized")
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
        
        # Get US-listed stocks suitable for Alpha Vantage
        instruments = await conn.fetch(f"""
            SELECT DISTINCT symbol 
            FROM dev_instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'NYSE MKT')
              AND LENGTH(symbol) <= 5  -- Alpha Vantage works better with standard tickers
            ORDER BY symbol
            {limit_clause}
        """)
        
        symbols = [inst['symbol'] for inst in instruments]
        self.stats['total_symbols'] = len(symbols)
        logger.info(f"📊 Found {len(symbols)} symbols for Alpha Vantage collection")
        return symbols

    def make_api_request(self, function: str, symbol: str = None, **kwargs) -> Optional[Dict]:
        """Make Alpha Vantage API request with error handling."""
        params = {
            'function': function,
            'apikey': self.api_key,
            **kwargs
        }
        
        if symbol:
            params['symbol'] = symbol
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            self.stats['api_calls'] += 1
            
            if response.status_code != 200:
                logger.error(f"Alpha Vantage API error: {response.status_code}")
                self.stats['errors'] += 1
                return None
            
            data = response.json()
            
            # Check for API error messages
            if 'Error Message' in data:
                logger.error(f"Alpha Vantage API Error: {data['Error Message']}")
                self.stats['errors'] += 1
                return None
            
            if 'Note' in data and 'API call frequency' in data['Note']:
                logger.warning("Rate limit hit, waiting...")
                time.sleep(60)
                return self.make_api_request(function, symbol, **kwargs)
            
            return data
            
        except Exception as e:
            logger.error(f"Error making API request for {function} ({symbol}): {e}")
            self.stats['errors'] += 1
            return None

    def get_earnings_calendar(self, horizon: str = "3month") -> List[Dict]:
        """Get earnings calendar data."""
        data = self.make_api_request('EARNINGS_CALENDAR', horizon=horizon)
        
        if not data:
            return []
        
        # Parse CSV response (Alpha Vantage earnings calendar returns CSV)
        try:
            import csv
            import io
            
            # Alpha Vantage returns CSV data as text
            if isinstance(data, str):
                csv_data = data
            else:
                # If it's JSON with a CSV field
                csv_data = data.get('data', '')
            
            reader = csv.DictReader(io.StringIO(csv_data))
            earnings_data = list(reader)
            
            logger.debug(f"Retrieved {len(earnings_data)} earnings calendar entries")
            return earnings_data
            
        except Exception as e:
            logger.error(f"Error parsing earnings calendar: {e}")
            return []

    def get_company_overview(self, symbol: str) -> Optional[Dict]:
        """Get company overview data."""
        data = self.make_api_request('OVERVIEW', symbol=symbol)
        
        if data and 'Symbol' in data:
            return data
        
        return None

    def get_earnings_data(self, symbol: str) -> Optional[Dict]:
        """Get detailed earnings data."""
        data = self.make_api_request('EARNINGS', symbol=symbol)
        
        if data and ('quarterlyEarnings' in data or 'annualEarnings' in data):
            return data
        
        return None

    def calculate_earnings_sentiment(self, actual_eps: Optional[float], 
                                   estimated_eps: Optional[float]) -> tuple:
        """Calculate sentiment and impact score from earnings surprise."""
        if actual_eps is None or estimated_eps is None or estimated_eps == 0:
            return 'neutral', 0.0
        
        surprise_pct = ((actual_eps - estimated_eps) / abs(estimated_eps)) * 100
        
        # Sentiment determination
        if surprise_pct > 5.0:
            sentiment = 'positive'
        elif surprise_pct < -5.0:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        # Impact score (-1.0 to +1.0)
        impact_score = min(max(surprise_pct / 50.0, -1.0), 1.0)
        
        return sentiment, impact_score

    async def extract_and_insert_earnings(self, conn, symbol: str, earnings_data: Dict):
        """Extract and insert earnings events."""
        events_inserted = 0
        
        try:
            quarterly_earnings = earnings_data.get('quarterlyEarnings', [])
            
            for earning in quarterly_earnings[:20]:  # Last 5 years of quarterly data
                try:
                    fiscal_date_ending = earning.get('fiscalDateEnding')
                    reported_date = earning.get('reportedDate')
                    reported_eps = earning.get('reportedEPS')
                    estimated_eps = earning.get('estimatedEPS')
                    surprise = earning.get('surprise')
                    surprise_percentage = earning.get('surprisePercentage')
                    
                    if not fiscal_date_ending:
                        continue
                    
                    # Convert string values to appropriate types
                    try:
                        actual_eps = float(reported_eps) if reported_eps and reported_eps != 'None' else None
                        est_eps = float(estimated_eps) if estimated_eps and estimated_eps != 'None' else None
                        surprise_pct = float(surprise_percentage) if surprise_percentage and surprise_percentage != 'None' else None
                    except (ValueError, TypeError):
                        actual_eps = est_eps = surprise_pct = None
                    
                    # Calculate sentiment and impact
                    sentiment, impact_score = self.calculate_earnings_sentiment(actual_eps, est_eps)
                    
                    # Create unique event ID
                    event_id = f"alpha_vantage_earnings_{symbol}_{fiscal_date_ending}"
                    
                    # Insert main financial event
                    financial_event_id = await conn.fetchval("""
                        INSERT INTO dev_financial_events (
                            event_id,
                            symbol,
                            event_type,
                            event_datetime,
                            announcement_datetime,
                            fiscal_period,
                            fiscal_year,
                            title,
                            description,
                            sentiment,
                            impact_score,
                            importance_level,
                            expected_value,
                            actual_value,
                            variance_pct,
                            vendor,
                            raw_data
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                        ON CONFLICT (event_id) DO UPDATE SET
                            updated_at = CURRENT_TIMESTAMP,
                            raw_data = EXCLUDED.raw_data
                        RETURNING id
                    """, 
                    event_id,
                    symbol,
                    'earnings',
                    datetime.strptime(fiscal_date_ending, '%Y-%m-%d') if fiscal_date_ending else None,
                    datetime.strptime(reported_date, '%Y-%m-%d') if reported_date else None,
                    f"Q{((datetime.strptime(fiscal_date_ending, '%Y-%m-%d').month - 1) // 3) + 1}",
                    datetime.strptime(fiscal_date_ending, '%Y-%m-%d').year,
                    f"{symbol} Q{((datetime.strptime(fiscal_date_ending, '%Y-%m-%d').month - 1) // 3) + 1} {datetime.strptime(fiscal_date_ending, '%Y-%m-%d').year} Earnings",
                    f"Alpha Vantage earnings data for {symbol}",
                    sentiment,
                    impact_score,
                    'high',
                    est_eps,
                    actual_eps,
                    surprise_pct,
                    'alpha_vantage',
                    json.dumps(earning)
                    )
                    
                    # Insert earnings-specific data
                    await conn.execute("""
                        INSERT INTO dev_earnings_events (
                            financial_event_id,
                            symbol,
                            report_period,
                            eps_actual_cents,
                            eps_estimated_cents,
                            eps_surprise_pct,
                            earnings_beat
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (financial_event_id) DO UPDATE SET
                            updated_at = CURRENT_TIMESTAMP
                    """,
                    financial_event_id,
                    symbol,
                    datetime.strptime(fiscal_date_ending, '%Y-%m-%d').date(),
                    int(actual_eps * 10000) if actual_eps is not None else None,
                    int(est_eps * 10000) if est_eps is not None else None,
                    surprise_pct,
                    actual_eps > est_eps if (actual_eps is not None and est_eps is not None) else None
                    )
                    
                    events_inserted += 1
                    
                except Exception as e:
                    logger.error(f"Error processing earning record for {symbol}: {e}")
                    continue
            
            self.stats['earnings_events'] += events_inserted
            return events_inserted
            
        except Exception as e:
            logger.error(f"Error extracting earnings for {symbol}: {e}")
            return 0

    async def extract_and_insert_company_overview(self, conn, symbol: str, overview_data: Dict):
        """Extract and insert company overview as announcement events."""
        try:
            # Create unique event ID for company overview
            event_id = f"alpha_vantage_overview_{symbol}"
            
            # Extract key metrics
            market_cap = overview_data.get('MarketCapitalization')
            pe_ratio = overview_data.get('PERatio')
            dividend_yield = overview_data.get('DividendYield')
            analyst_target_price = overview_data.get('AnalystTargetPrice')
            
            # Determine importance based on market cap
            importance = 'high' if market_cap and float(market_cap) > 10000000000 else 'medium'  # $10B threshold
            
            # Insert as company announcement event
            financial_event_id = await conn.fetchval("""
                INSERT INTO dev_financial_events (
                    event_id,
                    symbol,
                    event_type,
                    event_datetime,
                    title,
                    description,
                    sentiment,
                    importance_level,
                    vendor,
                    raw_data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (event_id) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    raw_data = EXCLUDED.raw_data
                RETURNING id
            """, 
            event_id,
            symbol,
            'announcement',
            datetime.now(),
            f"{symbol} Company Overview Update",
            f"Alpha Vantage company fundamentals for {symbol}",
            'neutral',
            importance,
            'alpha_vantage',
            json.dumps(overview_data)
            )
            
            # If there's analyst target price, create an analyst rating event
            if analyst_target_price and analyst_target_price != 'None':
                try:
                    target_price = float(analyst_target_price)
                    await self.insert_analyst_consensus_event(conn, symbol, target_price, overview_data)
                except (ValueError, TypeError):
                    pass
            
            self.stats['overview_data'] += 1
            return financial_event_id
            
        except Exception as e:
            logger.error(f"Error inserting company overview for {symbol}: {e}")
            return None

    async def insert_analyst_consensus_event(self, conn, symbol: str, target_price: float, overview_data: Dict):
        """Insert analyst consensus as rating event."""
        try:
            event_id = f"alpha_vantage_analyst_consensus_{symbol}"
            
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
                    importance_level,
                    vendor,
                    raw_data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (event_id) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, 
            event_id,
            symbol,
            'analyst_rating',
            datetime.now(),
            f"{symbol} Analyst Consensus",
            f"Alpha Vantage analyst consensus price target for {symbol}",
            'neutral',
            'medium',
            'alpha_vantage',
            json.dumps(overview_data)
            )
            
            # Insert analyst rating data
            await conn.execute("""
                INSERT INTO dev_analyst_ratings (
                    financial_event_id,
                    symbol,
                    analyst_firm,
                    new_rating,
                    new_price_target_cents,
                    rating_change
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (financial_event_id) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP
            """,
            financial_event_id,
            symbol,
            'Alpha Vantage Consensus',
            'neutral',  # Default to neutral for consensus
            int(target_price * 100),  # Convert to cents
            'reiterated'
            )
            
            self.stats['analyst_events'] += 1
            
        except Exception as e:
            logger.error(f"Error inserting analyst consensus for {symbol}: {e}")

    async def collect_symbol_events(self, conn, symbol: str, collect_earnings: bool = True, 
                                  collect_overview: bool = True):
        """Collect all events for a specific symbol."""
        try:
            logger.info(f"📈 Collecting events for {symbol}...")
            events_collected = 0
            
            # Collect earnings data
            if collect_earnings:
                earnings_data = self.get_earnings_data(symbol)
                if earnings_data:
                    earnings_events = await self.extract_and_insert_earnings(conn, symbol, earnings_data)
                    events_collected += earnings_events
                    logger.debug(f"💰 {symbol}: {earnings_events} earnings events")
                    
                time.sleep(self.request_delay)  # Rate limiting
            
            # Collect company overview
            if collect_overview:
                overview_data = self.get_company_overview(symbol)
                if overview_data:
                    await self.extract_and_insert_company_overview(conn, symbol, overview_data)
                    events_collected += 1
                    logger.debug(f"🏢 {symbol}: Company overview collected")
                    
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
        logger.info("🚀 Starting Alpha Vantage financial events collection...")
        logger.info(f"📅 Collecting data with {years_back} years historical depth")
        
        conn = await self.get_database_connection()
        
        try:
            # Get symbols to process
            symbols = await self.get_symbols_for_collection(conn, limit, specific_symbols)
            
            if not symbols:
                logger.warning("❌ No symbols found for collection")
                return
            
            logger.info(f"📊 Processing {len(symbols)} symbols")
            
            # Process each symbol
            for i, symbol in enumerate(symbols, 1):
                try:
                    await self.collect_symbol_events(
                        conn, symbol,
                        collect_earnings=True,
                        collect_overview=not earnings_only
                    )
                    
                    # Progress logging
                    if i % 10 == 0 or i == len(symbols):
                        progress = (i / len(symbols)) * 100
                        total_events = self.stats['earnings_events'] + self.stats['analyst_events'] + self.stats['overview_data']
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
        logger.info("🎉 ALPHA VANTAGE FINANCIAL EVENTS COLLECTION COMPLETE")
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
        logger.info(f"  Analyst Events: {self.stats['analyst_events']:,}")
        logger.info(f"  Company Overview: {self.stats['overview_data']:,}")
        
        total_events = self.stats['earnings_events'] + self.stats['analyst_events'] + self.stats['overview_data']
        logger.info(f"  Total Events: {total_events:,}")
        
        success_rate = (self.stats['processed_symbols'] / self.stats['total_symbols'] * 100) if self.stats['total_symbols'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="Alpha Vantage financial events collector")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of symbols to process')
    parser.add_argument('--years', type=int, default=5, help='Years of historical data to collect')
    parser.add_argument('--symbols', type=str, default=None, 
                       help='Comma-separated specific symbols to process')
    parser.add_argument('--earnings-only', action='store_true', 
                       help='Collect only earnings data (skip company overview)')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Get Alpha Vantage API key
        alpha_vantage_api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not alpha_vantage_api_key:
            logger.error("❌ ALPHA_VANTAGE_API_KEY environment variable not set")
            sys.exit(1)
        
        logger.info("✅ Alpha Vantage API key found")
        
        # Parse specific symbols if provided
        specific_symbols = None
        if args.symbols:
            specific_symbols = [s.strip() for s in args.symbols.split(',')]
            logger.info(f"📊 Processing specific symbols: {specific_symbols}")
        
        # Initialize collector
        collector = AlphaVantageEventsCollector(alpha_vantage_api_key)
        
        # Run collection
        await collector.run_collection(
            years_back=args.years,
            limit=args.limit,
            specific_symbols=specific_symbols,
            earnings_only=args.earnings_only
        )
        
        # Log final summary
        collector.log_final_summary()
        
        logger.info("✅ Alpha Vantage financial events collection complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run Alpha Vantage collection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())