#!/usr/bin/env python3
"""
Comprehensive Fundamental Data Population Script
Populates fundamental data for all instruments from multiple vendors over 30 years
with checkpoint support and automatic resume capability.

Vendors supported:
1. FMP (Financial Modeling Prep) - Financial statements, ratios, metrics
2. Polygon - Earnings, financials, cash flow 
3. Alpha Vantage - Balance sheet, income statement, cash flow

Usage:
PYTHONPATH=src python3 scripts/run_comprehensive_fundamentals.py --vendor all --years 30
PYTHONPATH=src python3 scripts/run_comprehensive_fundamentals.py --vendor fmp --resume
PYTHONPATH=src python3 scripts/run_comprehensive_fundamentals.py --vendor polygon --start-date 1995-01-01

Or via run_dev:
python3 scripts/run_dev.py run --script "scripts/run_comprehensive_fundamentals.py --vendor fmp --years 30"
"""

import asyncio
import asyncpg
import httpx
import logging
import argparse
import sys
import os
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass
import json

# Add src to path for imports
sys.path.append('/home/jianjun/ats-genai-data/src')

from config.environment import Environment
from frontfill.checkpoint_manager import CheckpointManager, CheckpointType, JobStatus, Checkpoint
from dao.fundamentals_dao import FundamentalsDAO
from core.logging.logger_config import get_logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = get_logger(__name__)

@dataclass
class FundamentalData:
    """Structured fundamental data record."""
    symbol: str
    date: date
    vendor: str
    
    # Financial Statement Data
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    ebitda: Optional[float] = None
    
    # Balance Sheet Data
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    shareholders_equity: Optional[float] = None
    current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    
    # Cash Flow Data
    operating_cash_flow: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    free_cash_flow: Optional[float] = None
    
    # Ratios and Metrics
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    
    # Raw data for reference
    raw_data: Optional[Dict[str, Any]] = None


class FMPClient:
    """Financial Modeling Prep API client for fundamental data."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.session = httpx.AsyncClient(timeout=30.0)
    
    async def get_financial_statements(self, symbol: str, statement_type: str, 
                                     limit: int = 120) -> List[Dict[str, Any]]:
        """
        Fetch financial statements for a symbol.
        statement_type: 'income-statement', 'balance-sheet-statement', 'cash-flow-statement'
        """
        url = f"{self.base_url}/{statement_type}/{symbol}"
        params = {"limit": limit, "apikey": self.api_key}
        
        try:
            response = await self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.warning(f"FMP API error for {symbol} {statement_type}: {e}")
            return []
    
    async def get_ratios(self, symbol: str, limit: int = 120) -> List[Dict[str, Any]]:
        """Fetch financial ratios."""
        url = f"{self.base_url}/ratios/{symbol}"
        params = {"limit": limit, "apikey": self.api_key}
        
        try:
            response = await self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.warning(f"FMP ratios API error for {symbol}: {e}")
            return []
    
    async def get_key_metrics(self, symbol: str, limit: int = 120) -> List[Dict[str, Any]]:
        """Fetch key financial metrics."""
        url = f"{self.base_url}/key-metrics/{symbol}"
        params = {"limit": limit, "apikey": self.api_key}
        
        try:
            response = await self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.warning(f"FMP key metrics API error for {symbol}: {e}")
            return []
    
    async def close(self):
        """Close the HTTP session."""
        await self.session.aclose()


class PolygonClient:
    """Polygon.io API client for fundamental data."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.session = httpx.AsyncClient(timeout=30.0)
    
    async def get_financials(self, symbol: str, filing_date_gte: str = None) -> List[Dict[str, Any]]:
        """Fetch financial data from Polygon."""
        url = f"{self.base_url}/vX/reference/financials"
        params = {
            "ticker": symbol,
            "apiKey": self.api_key,
            "limit": 100
        }
        
        if filing_date_gte:
            params["filing_date.gte"] = filing_date_gte
        
        try:
            response = await self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except httpx.HTTPError as e:
            logger.warning(f"Polygon financials API error for {symbol}: {e}")
            return []
    
    async def close(self):
        """Close the HTTP session."""
        await self.session.aclose()


class AlphaVantageClient:
    """Alpha Vantage API client for fundamental data."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.session = httpx.AsyncClient(timeout=30.0)
    
    async def get_financial_statement(self, symbol: str, function: str) -> Dict[str, Any]:
        """
        Fetch financial statements from Alpha Vantage.
        function: 'INCOME_STATEMENT', 'BALANCE_SHEET', 'CASH_FLOW'
        """
        params = {
            "function": function,
            "symbol": symbol,
            "apikey": self.api_key
        }
        
        try:
            response = await self.session.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.warning(f"Alpha Vantage API error for {symbol} {function}: {e}")
            return {}
    
    async def close(self):
        """Close the HTTP session."""
        await self.session.aclose()


class ComprehensiveFundamentalPopulator:
    """Main class for comprehensive fundamental data population."""
    
    def __init__(self, env: Environment):
        self.env = env
        self.connection_pool = None
        self.checkpoint_manager = None
        self.fundamentals_dao = None
        
        # Initialize API clients
        self.fmp_client = None
        self.polygon_client = None
        self.alphavantage_client = None
        
        self.stats = {
            'processed': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
    
    async def initialize(self):
        """Initialize database connections and API clients."""
        # Initialize database connection
        db_url = self.env.get_database_url()
        self.connection_pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
        
        # Initialize checkpoint manager
        self.checkpoint_manager = CheckpointManager(self.connection_pool, self.env)
        await self.checkpoint_manager.initialize_tables()
        
        # Initialize DAO
        self.fundamentals_dao = FundamentalsDAO(self.env)
        
        # Initialize API clients with environment variables
        fmp_key = os.getenv('FMP_API_KEY')
        polygon_key = os.getenv('POLYGON_API_KEY')
        av_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        
        if fmp_key:
            self.fmp_client = FMPClient(fmp_key)
            logger.info("FMP client initialized")
        
        if polygon_key:
            self.polygon_client = PolygonClient(polygon_key)
            logger.info("Polygon client initialized")
        
        if av_key:
            self.alphavantage_client = AlphaVantageClient(av_key)
            logger.info("Alpha Vantage client initialized")
        
        # Initialize extended fundamentals table
        await self.create_extended_fundamentals_table()
    
    async def create_extended_fundamentals_table(self):
        """Create comprehensive fundamentals table with all financial metrics."""
        table_name = self.env.get_table_name("fundamentals_comprehensive")
        
        async with self.connection_pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    vendor VARCHAR(50) NOT NULL,
                    fiscal_period VARCHAR(20), -- Q1, Q2, Q3, Q4, FY
                    
                    -- Income Statement
                    revenue BIGINT,
                    gross_profit BIGINT,
                    operating_income BIGINT,
                    net_income BIGINT,
                    ebitda BIGINT,
                    eps DECIMAL(10,4),
                    
                    -- Balance Sheet
                    total_assets BIGINT,
                    total_liabilities BIGINT,
                    shareholders_equity BIGINT,
                    current_assets BIGINT,
                    current_liabilities BIGINT,
                    total_debt BIGINT,
                    cash_and_equivalents BIGINT,
                    
                    -- Cash Flow
                    operating_cash_flow BIGINT,
                    investing_cash_flow BIGINT,
                    financing_cash_flow BIGINT,
                    free_cash_flow BIGINT,
                    
                    -- Ratios and Metrics
                    market_cap BIGINT,
                    pe_ratio DECIMAL(10,4),
                    pb_ratio DECIMAL(10,4),
                    debt_to_equity DECIMAL(10,4),
                    roe DECIMAL(10,4),
                    roa DECIMAL(10,4),
                    current_ratio DECIMAL(10,4),
                    quick_ratio DECIMAL(10,4),
                    
                    -- Metadata
                    raw_data JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(symbol, date, vendor, fiscal_period)
                )
            """)
            
            # Create indexes for performance
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_fundamentals_comprehensive_symbol_date 
                ON {table_name}(symbol, date DESC)
            """)
            
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_fundamentals_comprehensive_vendor_date 
                ON {table_name}(vendor, date DESC)
            """)
            
            logger.info("Extended fundamentals table initialized")
    
    async def get_all_instruments(self) -> List[str]:
        """Get all unique symbols from instrument tables."""
        symbols = set()
        
        # Get symbols from all instrument sources
        instrument_tables = [
            'dev_instrument_tiingo',
            'dev_instrument_eodhd', 
            'dev_instruments'
        ]
        
        async with self.connection_pool.acquire() as conn:
            for table in instrument_tables:
                try:
                    rows = await conn.fetch(f"SELECT DISTINCT symbol FROM {table} WHERE symbol IS NOT NULL")
                    table_symbols = {row['symbol'] for row in rows}
                    symbols.update(table_symbols)
                    logger.info(f"Found {len(table_symbols)} symbols in {table}")
                except Exception as e:
                    logger.warning(f"Could not query {table}: {e}")
        
        symbol_list = sorted(list(symbols))
        logger.info(f"Total unique symbols: {len(symbol_list)}")
        return symbol_list
    
    async def populate_fmp_fundamentals(self, symbols: List[str], 
                                       start_date: Optional[date] = None,
                                       resume: bool = True) -> None:
        """Populate fundamental data from FMP."""
        if not self.fmp_client:
            logger.error("FMP client not available - check FMP_API_KEY")
            return
        
        job_name = "fmp_fundamentals_30year"
        vendor = "fmp"
        
        # Check for existing checkpoint
        checkpoint = None
        start_symbol_index = 0
        
        if resume:
            checkpoint = await self.checkpoint_manager.get_checkpoint(job_name, vendor)
            if checkpoint:
                try:
                    start_symbol_index = int(checkpoint.checkpoint_value)
                    logger.info(f"Resuming FMP population from symbol index {start_symbol_index}")
                except ValueError:
                    logger.warning(f"Invalid checkpoint value: {checkpoint.checkpoint_value}")
        
        # Start job run
        run_id = await self.checkpoint_manager.start_job_run(
            job_name, "fundamentals", vendor, 
            checkpoint_start=str(start_symbol_index)
        )
        
        try:
            symbols_to_process = symbols[start_symbol_index:]
            logger.info(f"Processing {len(symbols_to_process)} symbols with FMP")
            
            for i, symbol in enumerate(symbols_to_process):
                current_index = start_symbol_index + i
                
                try:
                    # Get financial statements
                    income_data = await self.fmp_client.get_financial_statements(symbol, "income-statement")
                    balance_data = await self.fmp_client.get_financial_statements(symbol, "balance-sheet-statement")
                    cashflow_data = await self.fmp_client.get_financial_statements(symbol, "cash-flow-statement")
                    ratios_data = await self.fmp_client.get_ratios(symbol)
                    metrics_data = await self.fmp_client.get_key_metrics(symbol)
                    
                    # Process and combine data
                    records_inserted = await self.process_fmp_data(
                        symbol, income_data, balance_data, cashflow_data, 
                        ratios_data, metrics_data, start_date
                    )
                    
                    self.stats['processed'] += 1
                    self.stats['inserted'] += records_inserted
                    
                    # Update checkpoint every 50 symbols
                    if (current_index + 1) % 50 == 0:
                        checkpoint = Checkpoint(
                            job_name=job_name,
                            job_type="fundamentals",
                            vendor=vendor,
                            checkpoint_type=CheckpointType.OFFSET,
                            checkpoint_value=str(current_index + 1),
                            metadata={"last_symbol": symbol, "stats": self.stats.copy()},
                            created_at=datetime.now(),
                            updated_at=datetime.now(),
                            status=JobStatus.RUNNING
                        )
                        await self.checkpoint_manager.save_checkpoint(checkpoint)
                        
                        await self.checkpoint_manager.update_job_run(
                            run_id, 
                            records_processed=50,
                            records_inserted=records_inserted,
                            checkpoint_end=str(current_index + 1)
                        )
                        
                        logger.info(f"FMP: Processed {current_index + 1}/{len(symbols)} symbols. "
                                  f"Stats: {self.stats}")
                    
                    # Rate limiting - FMP allows 250 requests/minute
                    await asyncio.sleep(0.25)  # 4 requests per second
                    
                except Exception as e:
                    logger.error(f"Error processing {symbol} with FMP: {e}")
                    self.stats['errors'] += 1
                    continue
            
            # Mark job as completed
            final_checkpoint = Checkpoint(
                job_name=job_name,
                job_type="fundamentals", 
                vendor=vendor,
                checkpoint_type=CheckpointType.OFFSET,
                checkpoint_value=str(len(symbols)),
                metadata={"completed": True, "final_stats": self.stats.copy()},
                created_at=datetime.now(),
                updated_at=datetime.now(),
                status=JobStatus.COMPLETED
            )
            await self.checkpoint_manager.save_checkpoint(final_checkpoint)
            
            await self.checkpoint_manager.complete_job_run(
                run_id, JobStatus.COMPLETED, 
                checkpoint_end=str(len(symbols))
            )
            
            logger.info(f"FMP population completed. Final stats: {self.stats}")
            
        except Exception as e:
            logger.error(f"FMP population failed: {e}")
            await self.checkpoint_manager.complete_job_run(
                run_id, JobStatus.FAILED, error_message=str(e)
            )
            raise
    
    async def process_fmp_data(self, symbol: str, income_data: List[Dict], 
                              balance_data: List[Dict], cashflow_data: List[Dict],
                              ratios_data: List[Dict], metrics_data: List[Dict],
                              start_date: Optional[date] = None) -> int:
        """Process and insert FMP fundamental data."""
        table_name = self.env.get_table_name("fundamentals_comprehensive")
        records_inserted = 0
        
        # Create lookup dictionaries by date for efficient merging
        balance_by_date = {item['date']: item for item in balance_data if 'date' in item}
        cashflow_by_date = {item['date']: item for item in cashflow_data if 'date' in item}
        ratios_by_date = {item['date']: item for item in ratios_data if 'date' in item}
        metrics_by_date = {item['date']: item for item in metrics_data if 'date' in item}
        
        async with self.connection_pool.acquire() as conn:
            for income_item in income_data:
                if 'date' not in income_item:
                    continue
                    
                record_date = datetime.strptime(income_item['date'], '%Y-%m-%d').date()
                
                # Skip if before start date
                if start_date and record_date < start_date:
                    continue
                
                # Get corresponding data from other statements
                balance_item = balance_by_date.get(income_item['date'], {})
                cashflow_item = cashflow_by_date.get(income_item['date'], {})
                ratios_item = ratios_by_date.get(income_item['date'], {})
                metrics_item = metrics_by_date.get(income_item['date'], {})
                
                try:
                    await conn.execute(f"""
                        INSERT INTO {table_name} 
                        (symbol, date, vendor, fiscal_period, revenue, gross_profit, 
                         operating_income, net_income, ebitda, eps, total_assets, 
                         total_liabilities, shareholders_equity, current_assets, 
                         current_liabilities, total_debt, cash_and_equivalents,
                         operating_cash_flow, investing_cash_flow, financing_cash_flow,
                         free_cash_flow, market_cap, pe_ratio, pb_ratio, debt_to_equity,
                         roe, roa, current_ratio, quick_ratio, raw_data)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
                                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, 
                                $25, $26, $27, $28, $29, $30)
                        ON CONFLICT (symbol, date, vendor, fiscal_period) DO UPDATE SET
                            revenue = EXCLUDED.revenue,
                            gross_profit = EXCLUDED.gross_profit,
                            operating_income = EXCLUDED.operating_income,
                            net_income = EXCLUDED.net_income,
                            ebitda = EXCLUDED.ebitda,
                            eps = EXCLUDED.eps,
                            total_assets = EXCLUDED.total_assets,
                            total_liabilities = EXCLUDED.total_liabilities,
                            shareholders_equity = EXCLUDED.shareholders_equity,
                            current_assets = EXCLUDED.current_assets,
                            current_liabilities = EXCLUDED.current_liabilities,
                            total_debt = EXCLUDED.total_debt,
                            cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                            operating_cash_flow = EXCLUDED.operating_cash_flow,
                            investing_cash_flow = EXCLUDED.investing_cash_flow,
                            financing_cash_flow = EXCLUDED.financing_cash_flow,
                            free_cash_flow = EXCLUDED.free_cash_flow,
                            market_cap = EXCLUDED.market_cap,
                            pe_ratio = EXCLUDED.pe_ratio,
                            pb_ratio = EXCLUDED.pb_ratio,
                            debt_to_equity = EXCLUDED.debt_to_equity,
                            roe = EXCLUDED.roe,
                            roa = EXCLUDED.roa,
                            current_ratio = EXCLUDED.current_ratio,
                            quick_ratio = EXCLUDED.quick_ratio,
                            raw_data = EXCLUDED.raw_data,
                            updated_at = CURRENT_TIMESTAMP
                    """, 
                    symbol, record_date, 'fmp', income_item.get('period', 'FY'),
                    income_item.get('revenue'), income_item.get('grossProfit'),
                    income_item.get('operatingIncome'), income_item.get('netIncome'),
                    income_item.get('ebitda'), income_item.get('eps'),
                    balance_item.get('totalAssets'), balance_item.get('totalLiabilities'),
                    balance_item.get('totalStockholdersEquity'), balance_item.get('totalCurrentAssets'),
                    balance_item.get('totalCurrentLiabilities'), balance_item.get('totalDebt'),
                    balance_item.get('cashAndCashEquivalents'),
                    cashflow_item.get('operatingCashFlow'), cashflow_item.get('netCashUsedProvidedByInvestingActivities'),
                    cashflow_item.get('netCashUsedProvidedByFinancingActivities'), cashflow_item.get('freeCashFlow'),
                    metrics_item.get('marketCap'), ratios_item.get('priceEarningsRatio'),
                    ratios_item.get('priceToBookRatio'), ratios_item.get('debtEquityRatio'),
                    ratios_item.get('returnOnEquity'), ratios_item.get('returnOnAssets'),
                    ratios_item.get('currentRatio'), ratios_item.get('quickRatio'),
                    json.dumps({
                        'income': income_item,
                        'balance': balance_item,
                        'cashflow': cashflow_item,
                        'ratios': ratios_item,
                        'metrics': metrics_item
                    })
                    )
                    records_inserted += 1
                    
                except Exception as e:
                    logger.warning(f"Error inserting FMP data for {symbol} {record_date}: {e}")
                    self.stats['errors'] += 1
        
        return records_inserted
    
    async def populate_polygon_fundamentals(self, symbols: List[str], 
                                          start_date: Optional[date] = None,
                                          resume: bool = True) -> None:
        """Populate fundamental data from Polygon."""
        if not self.polygon_client:
            logger.error("Polygon client not available - check POLYGON_API_KEY")
            return
        
        job_name = "polygon_fundamentals_30year"
        vendor = "polygon"
        
        # Implementation similar to FMP but adapted for Polygon API structure
        # This would follow the same pattern as populate_fmp_fundamentals
        logger.info("Polygon fundamental population - implementation pending")
    
    async def populate_alphavantage_fundamentals(self, symbols: List[str],
                                               start_date: Optional[date] = None,
                                               resume: bool = True) -> None:
        """Populate fundamental data from Alpha Vantage."""
        if not self.alphavantage_client:
            logger.error("Alpha Vantage client not available - check ALPHA_VANTAGE_API_KEY")
            return
        
        job_name = "alphavantage_fundamentals_30year"
        vendor = "alphavantage"
        
        # Implementation similar to FMP but adapted for Alpha Vantage API structure
        # This would follow the same pattern as populate_fmp_fundamentals  
        logger.info("Alpha Vantage fundamental population - implementation pending")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.fmp_client:
            await self.fmp_client.close()
        if self.polygon_client:
            await self.polygon_client.close()
        if self.alphavantage_client:
            await self.alphavantage_client.close()
        if self.connection_pool:
            await self.connection_pool.close()


async def main():
    parser = argparse.ArgumentParser(description="Comprehensive Fundamental Data Population")
    parser.add_argument('--vendor', choices=['all', 'fmp', 'polygon', 'alphavantage'], 
                       default='all', help='Vendor to use for data population')
    parser.add_argument('--years', type=int, default=30, help='Number of years back to populate')
    parser.add_argument('--resume', action='store_true', help='Resume from last checkpoint')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Calculate start date
    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = date.today() - timedelta(days=args.years * 365)
    
    logger.info(f"Starting comprehensive fundamental data population")
    logger.info(f"Vendor: {args.vendor}, Start Date: {start_date}, Resume: {args.resume}")
    
    # Initialize environment
    env = Environment(environment='dev')
    
    # Initialize populator
    populator = ComprehensiveFundamentalPopulator(env)
    
    try:
        await populator.initialize()
        
        # Get all instruments
        symbols = await populator.get_all_instruments()
        logger.info(f"Found {len(symbols)} total symbols to process")
        
        # Execute population based on vendor selection
        if args.vendor in ['all', 'fmp']:
            logger.info("Starting FMP fundamental data population...")
            await populator.populate_fmp_fundamentals(symbols, start_date, args.resume)
        
        if args.vendor in ['all', 'polygon']:
            logger.info("Starting Polygon fundamental data population...")
            await populator.populate_polygon_fundamentals(symbols, start_date, args.resume)
        
        if args.vendor in ['all', 'alphavantage']:
            logger.info("Starting Alpha Vantage fundamental data population...")
            await populator.populate_alphavantage_fundamentals(symbols, start_date, args.resume)
        
        logger.info("Comprehensive fundamental data population completed!")
        logger.info(f"Final statistics: {populator.stats}")
        
    except Exception as e:
        logger.error(f"Population failed: {e}")
        raise
    finally:
        await populator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())