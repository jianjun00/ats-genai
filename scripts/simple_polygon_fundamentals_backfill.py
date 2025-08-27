#!/usr/bin/env python3
"""
Simple Polygon Fundamentals Backfill

Simplified fundamental data collection from Polygon.io API without complex date filtering:
- Collects all available fundamental data for specified symbols
- Both quarterly and annual data 
- Processes in batches with robust error handling
- Handles Polygon's rate limits (5 calls/minute for fundamentals)

Environment Variables:
- POLYGON_API_KEY: Required Polygon API key
- FUNDAMENTALS_SYMBOL_LIMIT: Max symbols to process, defaults to 20
- FUNDAMENTALS_TIMEFRAME: "annual", "quarterly", or "both" (default: both)
"""

import os
import sys
import asyncio
import aiohttp
import asyncpg
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimplePolygonFundamentalsCollector:
    """Simple Polygon fundamentals collector without complex date filtering"""
    
    def __init__(self, api_key: str, max_concurrent: int = 3):
        self.api_key = api_key
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.start_time = datetime.now()
        
        # Progress tracking
        self.total_records_collected = 0
        self.total_records_inserted = 0
        self.symbols_completed = 0
        
        # Rate limiting - Polygon allows 5 calls/minute for fundamentals
        self.rate_limit_delay = 12  # 12 seconds between calls
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes for fundamentals
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_priority_symbols(self, pool: asyncpg.Pool, limit: int = 20) -> List[str]:
        """Get high-priority symbols from Polygon instruments"""
        async with pool.acquire() as conn:
            # Check for specific priority symbols
            priority_symbols_env = os.getenv('FUNDAMENTALS_PRIORITY_SYMBOLS')
            if priority_symbols_env:
                priority_list = [s.strip().upper() for s in priority_symbols_env.split(',')]
                logger.info(f"🎯 Using specified priority symbols: {priority_list[:10]}...")
                return priority_list[:limit]
            
            # Get from Polygon instruments table if available
            try:
                rows = await conn.fetch("""
                    SELECT symbol FROM dev_instrument_polygon 
                    WHERE active = true 
                    ORDER BY 
                      CASE 
                        WHEN symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'V') THEN 1
                        WHEN LENGTH(symbol) <= 4 AND symbol ~ '^[A-Z]+$' THEN 2  -- Standard tickers
                        ELSE 3
                      END,
                      symbol 
                    LIMIT $1
                """, limit)
                if rows:
                    symbols = [row['symbol'] for row in rows]
                    logger.info(f"📈 Selected {len(symbols)} symbols from Polygon instruments: {symbols[:10]}")
                    return symbols
            except Exception as e:
                logger.info(f"📊 Polygon instruments table not accessible: {e}")
            
            # Fallback to major S&P 500 symbols
            default_symbols = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'V',
                'BAC', 'WMT', 'PG', 'MA', 'HD', 'UNH', 'DIS', 'ADBE', 'NFLX', 'CRM'
            ]
            return default_symbols[:limit]

    async def fetch_financials_for_symbol(self, symbol: str, timeframe: str = "annual") -> List[Dict[str, Any]]:
        """Fetch all available Polygon financials for a symbol and timeframe"""
        async with self.semaphore:
            url = "https://api.polygon.io/vX/reference/financials"
            params = {
                'ticker': symbol,
                'timeframe': timeframe,
                'limit': 1000,  # Get all available data
                'apikey': self.api_key
            }
            
            all_records = []
            
            for attempt in range(3):
                try:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get('status') == 'OK':
                                results = data.get('results', [])
                                
                                # Convert to standardized format
                                for record in results:
                                    standardized = self.standardize_polygon_financial(record, symbol)
                                    if standardized:
                                        all_records.append(standardized)
                                
                                logger.info(f"✅ {symbol} {timeframe}: {len(all_records)} records")
                                
                                # Rate limiting - Polygon fundamentals are heavily limited
                                await asyncio.sleep(self.rate_limit_delay)
                                return all_records
                                
                        elif response.status == 429:
                            wait_time = 60 + (attempt * 30)  # Polygon rate limiting
                            logger.warning(f"⚠️ Polygon rate limit for {symbol} {timeframe}, waiting {wait_time}s...")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.warning(f"⚠️ {symbol} {timeframe}: HTTP {response.status}")
                            if attempt == 2:
                                return []
                            await asyncio.sleep(10)
                            
                except Exception as e:
                    logger.warning(f"⚠️ {symbol} {timeframe} attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(5 ** attempt)
            
            return all_records

    def standardize_polygon_financial(self, record: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Convert Polygon financial record to standardized format"""
        try:
            financials = record.get('financials', {})
            
            return {
                'symbol': symbol,
                'cik': record.get('cik'),
                'fiscal_period': record.get('fiscal_period'),
                'fiscal_year': record.get('fiscal_year'),
                'start_date': record.get('start_date'),
                'end_date': record.get('end_date'),
                'timeframe': record.get('timeframe'),
                'filing_date': record.get('filing_date'),
                'acceptance_datetime': record.get('acceptance_datetime'),
                'company_name': record.get('company_name'),
                'sic': record.get('sic'),
                
                # Financial statements as JSONB
                'balance_sheet': financials.get('balance_sheet', {}),
                'income_statement': financials.get('income_statement', {}),
                'cash_flow_statement': financials.get('cash_flow_statement', {}),
                'comprehensive_income': financials.get('comprehensive_income', {}),
                
                # Key metrics extracted for easy querying
                'total_assets': self.extract_financial_value(financials, 'balance_sheet', 'assets'),
                'total_liabilities': self.extract_financial_value(financials, 'balance_sheet', 'liabilities'),
                'total_equity': self.extract_financial_value(financials, 'balance_sheet', 'equity'),
                'total_revenue': self.extract_financial_value(financials, 'income_statement', 'revenues'),
                'net_income': self.extract_financial_value(financials, 'income_statement', 'net_income_loss'),
                'operating_income': self.extract_financial_value(financials, 'income_statement', 'operating_income_loss'),
                'gross_profit': self.extract_financial_value(financials, 'income_statement', 'gross_profit'),
                'operating_cash_flow': self.extract_financial_value(financials, 'cash_flow_statement', 'net_cash_flow_from_operating_activities'),
                
                # Store full original data
                'raw_data': record
            }
        except Exception as e:
            logger.warning(f"⚠️ Failed to standardize Polygon financial record for {symbol}: {e}")
            return None

    def extract_financial_value(self, financials: Dict, statement: str, field: str) -> Optional[float]:
        """Extract financial value from nested structure"""
        try:
            statement_data = financials.get(statement, {})
            field_data = statement_data.get(field, {})
            if isinstance(field_data, dict) and 'value' in field_data:
                return float(field_data['value'])
            return None
        except:
            return None

    async def process_symbols_batch(self, pool: asyncpg.Pool, symbols: List[str], 
                                   timeframes: List[str]) -> Dict[str, int]:
        """Process a batch of symbols for all timeframes"""
        logger.info(f"📦 Processing {len(symbols)} symbols for timeframes: {', '.join(timeframes)}")
        
        results = {
            'total_records': 0,
            'total_inserted': 0,
            'symbols_processed': 0,
            'errors': 0
        }
        
        for symbol in symbols:
            symbol_records = []
            
            try:
                for timeframe in timeframes:
                    logger.info(f"  📊 {symbol} {timeframe}...")
                    
                    records = await self.fetch_financials_for_symbol(symbol, timeframe)
                    symbol_records.extend(records)
                    results['total_records'] += len(records)
                
                if symbol_records:
                    inserted = await self.insert_polygon_financials(pool, symbol_records)
                    results['total_inserted'] += inserted
                    logger.info(f"  ✅ {symbol}: {len(symbol_records)} records, {inserted} inserted")
                else:
                    logger.info(f"  ➖ {symbol}: No financial data found")
                
                results['symbols_processed'] += 1
                
            except Exception as e:
                logger.error(f"  ❌ {symbol}: {e}")
                results['errors'] += 1
        
        self.total_records_collected += results['total_records']
        self.total_records_inserted += results['total_inserted']
        
        return results

    async def insert_polygon_financials(self, pool: asyncpg.Pool, records: List[Dict[str, Any]]) -> int:
        """Insert Polygon financial records into dev_fundamentals_polygon table"""
        if not records:
            return 0
        
        # Ensure table exists
        await self.ensure_fundamentals_table(pool)
        
        inserted = 0
        async with pool.acquire() as conn:
            for record in records:
                try:
                    await conn.execute("""
                        INSERT INTO dev_fundamentals_polygon (
                            symbol, cik, fiscal_period, fiscal_year, start_date, end_date,
                            timeframe, filing_date, acceptance_datetime, company_name, sic,
                            balance_sheet, income_statement, cash_flow_statement, comprehensive_income,
                            total_assets, total_liabilities, total_equity, total_revenue,
                            net_income, operating_income, gross_profit, operating_cash_flow,
                            raw_data
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
                                $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
                        ON CONFLICT (symbol, fiscal_period, fiscal_year, timeframe) DO UPDATE SET
                            balance_sheet = EXCLUDED.balance_sheet,
                            income_statement = EXCLUDED.income_statement,
                            cash_flow_statement = EXCLUDED.cash_flow_statement,
                            comprehensive_income = EXCLUDED.comprehensive_income,
                            total_assets = EXCLUDED.total_assets,
                            total_liabilities = EXCLUDED.total_liabilities,
                            total_equity = EXCLUDED.total_equity,
                            total_revenue = EXCLUDED.total_revenue,
                            net_income = EXCLUDED.net_income,
                            operating_income = EXCLUDED.operating_income,
                            gross_profit = EXCLUDED.gross_profit,
                            operating_cash_flow = EXCLUDED.operating_cash_flow,
                            raw_data = EXCLUDED.raw_data,
                            updated_at = CURRENT_TIMESTAMP
                    """, 
                        record.get('symbol'),
                        record.get('cik'),
                        record.get('fiscal_period'),
                        record.get('fiscal_year'),
                        record.get('start_date'),
                        record.get('end_date'),
                        record.get('timeframe'),
                        record.get('filing_date'),
                        record.get('acceptance_datetime'),
                        record.get('company_name'),
                        record.get('sic'),
                        json.dumps(record.get('balance_sheet', {})),
                        json.dumps(record.get('income_statement', {})),
                        json.dumps(record.get('cash_flow_statement', {})),
                        json.dumps(record.get('comprehensive_income', {})),
                        record.get('total_assets'),
                        record.get('total_liabilities'),
                        record.get('total_equity'),
                        record.get('total_revenue'),
                        record.get('net_income'),
                        record.get('operating_income'),
                        record.get('gross_profit'),
                        record.get('operating_cash_flow'),
                        json.dumps(record.get('raw_data', {}))
                    )
                    inserted += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to insert financial record for {record.get('symbol', 'unknown')}: {e}")
        
        return inserted

    async def ensure_fundamentals_table(self, pool: asyncpg.Pool):
        """Ensure dev_fundamentals_polygon table exists with proper schema"""
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_fundamentals_polygon (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    cik VARCHAR(20),
                    fiscal_period VARCHAR(10),
                    fiscal_year VARCHAR(10),
                    start_date DATE,
                    end_date DATE,
                    timeframe VARCHAR(20),
                    filing_date DATE,
                    acceptance_datetime TIMESTAMP WITH TIME ZONE,
                    company_name VARCHAR(500),
                    sic VARCHAR(10),
                    
                    -- Full financial statements as JSONB
                    balance_sheet JSONB,
                    income_statement JSONB,
                    cash_flow_statement JSONB,
                    comprehensive_income JSONB,
                    
                    -- Key extracted metrics for easy querying
                    total_assets BIGINT,
                    total_liabilities BIGINT,
                    total_equity BIGINT,
                    total_revenue BIGINT,
                    net_income BIGINT,
                    operating_income BIGINT,
                    gross_profit BIGINT,
                    operating_cash_flow BIGINT,
                    
                    -- Full raw data
                    raw_data JSONB,
                    
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE UNIQUE INDEX IF NOT EXISTS idx_fundamentals_polygon_unique 
                ON dev_fundamentals_polygon(symbol, fiscal_period, fiscal_year, timeframe);
                
                CREATE INDEX IF NOT EXISTS idx_fundamentals_polygon_symbol ON dev_fundamentals_polygon(symbol);
                CREATE INDEX IF NOT EXISTS idx_fundamentals_polygon_fiscal_year ON dev_fundamentals_polygon(fiscal_year);
                CREATE INDEX IF NOT EXISTS idx_fundamentals_polygon_filing_date ON dev_fundamentals_polygon(filing_date);
                CREATE INDEX IF NOT EXISTS idx_fundamentals_polygon_timeframe ON dev_fundamentals_polygon(timeframe);
                CREATE INDEX IF NOT EXISTS idx_fundamentals_polygon_total_revenue ON dev_fundamentals_polygon(total_revenue);
                CREATE INDEX IF NOT EXISTS idx_fundamentals_polygon_net_income ON dev_fundamentals_polygon(net_income);
            """)

    def log_progress_summary(self, symbols_processed: int, total_symbols: int):
        """Log comprehensive progress summary"""
        elapsed = datetime.now() - self.start_time
        hours_elapsed = elapsed.total_seconds() / 3600
        
        logger.info("=" * 80)
        logger.info("📊 SIMPLE POLYGON FUNDAMENTALS BACKFILL PROGRESS")
        logger.info("=" * 80)
        logger.info(f"📈 Symbols Processed: {symbols_processed}/{total_symbols}")
        logger.info(f"📊 Total Records Collected: {self.total_records_collected:,}")
        logger.info(f"💾 Total Records Inserted: {self.total_records_inserted:,}")
        logger.info(f"⏱️  Elapsed Time: {elapsed}")
        
        if symbols_processed > 0:
            avg_records_per_symbol = self.total_records_collected / symbols_processed
            logger.info(f"📈 Average Records per Symbol: {avg_records_per_symbol:.1f}")
            
        if hours_elapsed > 0:
            records_per_hour = self.total_records_collected / hours_elapsed
            logger.info(f"🚀 Collection Rate: {records_per_hour:.1f} records/hour")
        
        logger.info("=" * 80)

async def main():
    """Main execution function"""
    
    # Get configuration from environment
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("❌ POLYGON_API_KEY environment variable required")
        sys.exit(1)
    
    symbol_limit = int(os.getenv('FUNDAMENTALS_SYMBOL_LIMIT', '20'))
    
    # Timeframe configuration
    timeframe_setting = os.getenv('FUNDAMENTALS_TIMEFRAME', 'both').lower()
    if timeframe_setting == 'both':
        timeframes = ['annual', 'quarterly']
    elif timeframe_setting in ['annual', 'quarterly']:
        timeframes = [timeframe_setting]
    else:
        logger.error(f"❌ Invalid FUNDAMENTALS_TIMEFRAME: {timeframe_setting}. Use 'annual', 'quarterly', or 'both'")
        sys.exit(1)
    
    logger.info(f"🚀 Starting Simple Polygon Fundamentals Backfill")
    logger.info(f"📊 Max Symbols: {symbol_limit:,}")
    logger.info(f"📈 Timeframes: {', '.join(timeframes)}")
    logger.info(f"⏱️  Rate Limit: 12 seconds between calls")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with SimplePolygonFundamentalsCollector(api_key) as collector:
            # Get priority symbols
            symbols = await collector.get_priority_symbols(pool, symbol_limit)
            logger.info(f"📈 Processing {len(symbols)} priority symbols")
            
            # Process all symbols
            results = await collector.process_symbols_batch(pool, symbols, timeframes)
            
            logger.info(f"✅ All symbols processed:")
            logger.info(f"  📊 Total Records: {results['total_records']:,}")
            logger.info(f"  💾 Total Inserted: {results['total_inserted']:,}")
            logger.info(f"  📈 Symbols Processed: {results['symbols_processed']}/{len(symbols)}")
            logger.info(f"  ❌ Errors: {results['errors']}")
            
            # Final summary
            collector.log_progress_summary(results['symbols_processed'], len(symbols))
        
        await pool.close()
        
        logger.info("🎉 Simple Polygon Fundamentals Backfill Complete!")
        logger.info(f"📊 Total Records Collected: {collector.total_records_collected:,}")
        logger.info(f"💾 Total Records Inserted: {collector.total_records_inserted:,}")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())