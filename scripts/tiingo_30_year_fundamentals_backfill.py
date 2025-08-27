#!/usr/bin/env python3
"""
Tiingo 30-Year Fundamentals Backfill

Comprehensive collection of fundamental data from Tiingo API:
- Daily fundamentals (market cap, enterprise value, key metrics)
- Financial statements (balance sheet, income statement, cash flow)
- Both quarterly and annual data for 30 years
- Idempotent operations with proper error handling

Environment Variables:
- TIINGO_API_KEY: Required Tiingo API key
- LIMIT: Number of instruments to process (default: all)
- YEARS: Number of years to backfill (default: 30)
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
import json
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import time
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tiingo_30_year_fundamentals_backfill")

class TiingoFundamentalsCollector:
    """
    Tiingo 30-year fundamentals collector with comprehensive data processing.
    
    Features:
    - Daily fundamentals collection (market cap, enterprise value, etc.)
    - Financial statements collection (balance sheet, income, cash flow)
    - 30-year historical data coverage
    - Idempotent database operations
    - Rate limiting and error handling
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tiingo.com/tiingo/fundamentals"
        
        # Rate limiting (conservative for API stability)
        self.request_delay = 1.0  # 1 second between requests
        
        # Statistics
        self.stats = {
            'total_instruments': 0,
            'processed_instruments': 0,
            'daily_records': 0,
            'statement_records': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_instruments': 0
        }
        
        logger.info(f"📊 Tiingo 30-Year Fundamentals Collector initialized")
        logger.info(f"   Base URL: {self.base_url}")
        logger.info(f"   Rate limit: {1.0/self.request_delay:.0f} requests/second")

    async def get_database_connection(self):
        """Get database connection."""
        db_host = os.getenv('DB_HOST', 'postgres')
        db_port = int(os.getenv('DB_PORT', '5432'))
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'dev_password')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        return await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )

    async def ensure_fundamentals_tables(self, conn):
        """Create Tiingo fundamentals tables if they don't exist."""
        
        # Daily fundamentals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_tiingo_fundamentals_daily (
                date DATE NOT NULL,
                symbol TEXT NOT NULL,
                instrument_id INTEGER,
                market_cap DOUBLE PRECISION,
                enterprise_val DOUBLE PRECISION,
                pe_ratio DOUBLE PRECISION,
                pb_ratio DOUBLE PRECISION,
                trail_pe_ratio DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol)
            )
        """)
        
        # Financial statements table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_tiingo_fundamentals_statements (
                date DATE NOT NULL,
                symbol TEXT NOT NULL,
                instrument_id INTEGER,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                statement_type TEXT NOT NULL, -- 'balanceSheet', 'incomeStatement', 'cashFlow'
                data_code TEXT NOT NULL,
                value DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol, statement_type, data_code)
            )
        """)
        
        # Create indexes for performance
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tiingo_daily_symbol_date 
            ON dev_tiingo_fundamentals_daily (symbol, date DESC)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tiingo_statements_symbol_date 
            ON dev_tiingo_fundamentals_statements (symbol, date DESC)
        """)
        
        logger.info("✅ Tiingo fundamentals tables ready")

    async def get_instruments_for_backfill(self, conn, limit=None):
        """Get DOW 30 instruments only (Tiingo API restriction)."""
        # Tiingo fundamentals API is limited to DOW 30 companies for Free/Power plans
        dow_30_symbols = [
            'AAPL', 'MSFT', 'UNH', 'GS', 'HD', 'CAT', 'AMGN', 'MCD', 'CRM', 'V',
            'BA', 'JPM', 'JNJ', 'HON', 'AXP', 'PG', 'CVX', 'IBM', 'MRK', 'DIS',
            'WMT', 'MMM', 'TRV', 'NKE', 'KO', 'DOW', 'CSCO', 'INTC', 'WBA', 'VZ'
        ]
        
        # Apply limit if specified
        if limit and limit < len(dow_30_symbols):
            dow_30_symbols = dow_30_symbols[:limit]
        
        # Convert to instrument format matching existing dev_instruments schema
        instruments = []
        for i, symbol in enumerate(dow_30_symbols, 1):
            instruments.append({
                'id': i,  # Fake ID for compatibility
                'symbol': symbol,
                'name': f"{symbol} Corporation",  # Fake name
                'exchange': 'NYSE',  # Most DOW 30 are on NYSE
                'active': True
            })
        
        self.stats['total_instruments'] = len(instruments)
        logger.info(f"📊 Processing {len(instruments)} DOW 30 instruments (Tiingo API restriction)")
        logger.info(f"🔗 DOW 30 symbols: {', '.join([i['symbol'] for i in instruments])}")
        return instruments

    def fetch_daily_fundamentals(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Fetch daily fundamentals from Tiingo API."""
        url = f"{self.base_url}/{symbol}/daily"
        params = {
            'token': self.api_key,
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"✅ Daily fundamentals: {symbol} - {len(data)} records")
                return data
            elif response.status_code == 404:
                logger.debug(f"⚠️ No daily fundamentals for {symbol}")
                return []
            else:
                logger.warning(f"⚠️ Daily fundamentals API error for {symbol}: {response.status_code}")
                self.stats['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching daily fundamentals for {symbol}: {e}")
            self.stats['errors'] += 1
            return []

    def fetch_statements(self, symbol: str) -> List[Dict]:
        """Fetch financial statements from Tiingo API."""
        url = f"{self.base_url}/{symbol}/statements"
        params = {
            'token': self.api_key,
            'format': 'json'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"✅ Statements: {symbol} - {len(data)} periods")
                return data
            elif response.status_code == 404:
                logger.debug(f"⚠️ No statements for {symbol}")
                return []
            else:
                logger.warning(f"⚠️ Statements API error for {symbol}: {response.status_code}")
                self.stats['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ Error fetching statements for {symbol}: {e}")
            self.stats['errors'] += 1
            return []

    async def insert_daily_fundamentals(self, conn, symbol: str, instrument_id: int, daily_data: List[Dict]) -> int:
        """Insert daily fundamentals with idempotent operations."""
        if not daily_data:
            return 0
        
        rows = []
        for record in daily_data:
            try:
                # Parse date
                date_str = record['date'][:10]  # Extract YYYY-MM-DD
                date_val = datetime.strptime(date_str, '%Y-%m-%d').date()
                
                rows.append((
                    date_val,
                    symbol,
                    instrument_id,
                    record.get('marketCap'),
                    record.get('enterpriseVal'),
                    record.get('peRatio'),
                    record.get('pbRatio'),
                    record.get('trailingPE')
                ))
            except Exception as e:
                logger.warning(f"Error processing daily record for {symbol}: {e}")
                continue
        
        if not rows:
            return 0
        
        # Idempotent UPSERT
        try:
            await conn.executemany("""
                INSERT INTO dev_tiingo_fundamentals_daily 
                (date, symbol, instrument_id, market_cap, enterprise_val, pe_ratio, pb_ratio, trail_pe_ratio)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date, symbol) DO UPDATE SET
                    instrument_id = EXCLUDED.instrument_id,
                    market_cap = EXCLUDED.market_cap,
                    enterprise_val = EXCLUDED.enterprise_val,
                    pe_ratio = EXCLUDED.pe_ratio,
                    pb_ratio = EXCLUDED.pb_ratio,
                    trail_pe_ratio = EXCLUDED.trail_pe_ratio,
                    updated_at = CURRENT_TIMESTAMP
            """, rows)
            
            self.stats['daily_records'] += len(rows)
            logger.debug(f"💾 Inserted {len(rows)} daily fundamentals for {symbol}")
            return len(rows)
            
        except Exception as e:
            logger.error(f"❌ Database error inserting daily fundamentals for {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def insert_statements(self, conn, symbol: str, instrument_id: int, statements_data: List[Dict]) -> int:
        """Insert financial statements with idempotent operations."""
        if not statements_data:
            return 0
        
        rows = []
        for period in statements_data:
            try:
                # Parse period info
                date_val = datetime.strptime(period['date'], '%Y-%m-%d').date()
                year = period.get('year', date_val.year)
                quarter = period.get('quarter', 0)  # 0 = annual
                
                statement_data = period.get('statementData', {})
                
                # Process each statement type
                for statement_type, items in statement_data.items():
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and 'dataCode' in item and 'value' in item:
                                rows.append((
                                    date_val,
                                    symbol,
                                    instrument_id,
                                    year,
                                    quarter,
                                    statement_type,
                                    item['dataCode'],
                                    float(item['value']) if item['value'] is not None else None
                                ))
                                
            except Exception as e:
                logger.warning(f"Error processing statement for {symbol}: {e}")
                continue
        
        if not rows:
            return 0
        
        # Idempotent UPSERT
        try:
            await conn.executemany("""
                INSERT INTO dev_tiingo_fundamentals_statements 
                (date, symbol, instrument_id, year, quarter, statement_type, data_code, value)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date, symbol, statement_type, data_code) DO UPDATE SET
                    instrument_id = EXCLUDED.instrument_id,
                    year = EXCLUDED.year,
                    quarter = EXCLUDED.quarter,
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
            """, rows)
            
            self.stats['statement_records'] += len(rows)
            logger.debug(f"💾 Inserted {len(rows)} statement records for {symbol}")
            return len(rows)
            
        except Exception as e:
            logger.error(f"❌ Database error inserting statements for {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def check_existing_data(self, conn, symbol: str, start_date: date, end_date: date) -> bool:
        """Check if instrument already has fundamental data."""
        daily_count = await conn.fetchval("""
            SELECT COUNT(*) FROM dev_tiingo_fundamentals_daily
            WHERE symbol = $1 AND date BETWEEN $2 AND $3
        """, symbol, start_date, end_date)
        
        statements_count = await conn.fetchval("""
            SELECT COUNT(*) FROM dev_tiingo_fundamentals_statements
            WHERE symbol = $1 AND date BETWEEN $2 AND $3
        """, symbol, start_date, end_date)
        
        return daily_count > 0 or statements_count > 0

    async def backfill_instrument_fundamentals(self, conn, instrument, start_date: date, end_date: date, skip_existing=True):
        """Backfill fundamentals for a single instrument."""
        instrument_id = instrument['id']
        symbol = instrument['symbol']
        
        try:
            # Check if we should skip existing data
            if skip_existing:
                if await self.check_existing_data(conn, symbol, start_date, end_date):
                    logger.info(f"⏭️ Skipping {symbol} - already has fundamentals data")
                    self.stats['skipped_instruments'] += 1
                    return 0
            
            logger.info(f"📈 Processing {symbol} (ID: {instrument_id}) for fundamentals...")
            
            # Fetch daily fundamentals
            daily_data = self.fetch_daily_fundamentals(symbol, start_date, end_date)
            time.sleep(self.request_delay)
            
            # Fetch financial statements
            statements_data = self.fetch_statements(symbol)
            time.sleep(self.request_delay)
            
            total_inserted = 0
            
            # Insert daily fundamentals
            if daily_data:
                daily_inserted = await self.insert_daily_fundamentals(conn, symbol, instrument_id, daily_data)
                total_inserted += daily_inserted
            
            # Insert statements
            if statements_data:
                statements_inserted = await self.insert_statements(conn, symbol, instrument_id, statements_data)
                total_inserted += statements_inserted
            
            if total_inserted > 0:
                logger.info(f"✅ Completed {symbol}: {total_inserted} records inserted")
                self.stats['processed_instruments'] += 1
            else:
                logger.warning(f"⚠️ No fundamental data for {symbol}")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def run_backfill(self, start_date: date, end_date: date, limit=None, skip_existing=True):
        """Run the complete fundamentals backfill process."""
        logger.info("🚀 Starting Tiingo 30-year fundamentals backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        conn = await self.get_database_connection()
        
        try:
            # Ensure tables exist
            await self.ensure_fundamentals_tables(conn)
            
            # Get instruments to process
            instruments = await self.get_instruments_for_backfill(conn, limit)
            
            if not instruments:
                logger.warning("❌ No instruments found for backfill")
                return
            
            logger.info(f"📊 Processing {len(instruments)} instruments")
            
            # Process each instrument
            for i, instrument in enumerate(instruments, 1):
                try:
                    await self.backfill_instrument_fundamentals(conn, instrument, start_date, end_date, skip_existing)
                    
                    # Progress logging
                    if i % 50 == 0 or i == len(instruments):
                        progress = (i / len(instruments)) * 100
                        logger.info(f"📊 Progress: {i:,}/{len(instruments):,} ({progress:.1f}%) - "
                                  f"{self.stats['daily_records']:,} daily + {self.stats['statement_records']:,} statements")
                        
                except Exception as e:
                    logger.error(f"❌ Critical error processing instrument {instrument.get('symbol', 'unknown')}: {e}")
                    continue
            
        finally:
            await conn.close()
    
    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 TIINGO 30-YEAR FUNDAMENTALS BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Instruments: {self.stats['total_instruments']:,}")
        logger.info(f"  Processed Instruments: {self.stats['processed_instruments']:,}")
        logger.info(f"  Skipped Instruments: {self.stats['skipped_instruments']:,}")
        logger.info(f"  Daily Records: {self.stats['daily_records']:,}")
        logger.info(f"  Statement Records: {self.stats['statement_records']:,}")
        logger.info(f"  Total Records: {self.stats['daily_records'] + self.stats['statement_records']:,}")
        logger.info(f"  API Calls Made: {self.stats['api_calls']:,}")
        logger.info(f"  Errors: {self.stats['errors']:,}")
        logger.info("")
        
        success_rate = ((self.stats['processed_instruments']) / self.stats['total_instruments'] * 100) if self.stats['total_instruments'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="Tiingo 30-year fundamentals backfill")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '0')) or None, 
                       help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '30')), 
                       help='Number of years of historical data to fetch (default: 30)')
    parser.add_argument('--start_date', type=str, default=None, 
                       help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, 
                       help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--skip_existing', action='store_true', default=True, 
                       help='Skip instruments that already have fundamental data')
    
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
        
        # Calculate date range
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (datetime.now() - timedelta(days=365 * args.years)).date()
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()
        
        logger.info(f"📅 Backfilling Tiingo fundamentals from {start_date} to {end_date}")
        
        # Initialize collector
        collector = TiingoFundamentalsCollector(tiingo_api_key)
        
        # Run backfill
        await collector.run_backfill(
            start_date, end_date, 
            limit=args.limit, 
            skip_existing=args.skip_existing
        )
        
        # Log final summary
        collector.log_final_summary()
        
        logger.info("✅ Tiingo 30-year fundamentals backfill complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run Tiingo fundamentals backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())