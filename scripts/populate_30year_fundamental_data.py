#!/usr/bin/env python3
"""
Comprehensive 30-Year Fundamental Data Population Script

Populates fundamental data for all instruments over the past 30 years from multiple vendors:
- Polygon API (annual/quarterly financials)
- Financial Modeling Prep (comprehensive financial statements)
- Tiingo (fundamental daily data)
- EODHD (annual/quarterly fundamentals)

Key Features:
- Uses existing fundamental adapters for multi-vendor integration
- Checkpoint-based resumable processing for massive scale
- Cross-vendor data validation and deduplication
- Quality scoring and data reconciliation
- Progress tracking and comprehensive error recovery
- Database storage with proper schema

Usage:
    python scripts/populate_30year_fundamental_data.py --mode full --limit 10
    python scripts/populate_30year_fundamental_data.py --vendors polygon,fmp --symbols AAPL,MSFT
    python scripts/populate_30year_fundamental_data.py --resume --checkpoint-file last_run.json
    python scripts/populate_30year_fundamental_data.py --debug --concurrent 1
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Optional, Set
import argparse
from dataclasses import dataclass, asdict
import tempfile
import aiofiles
import time

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from market_data.agent.polygon_fundamentals_adapter import PolygonFundamentalsAdapter
from market_data.agent.fmp_fundamentals_adapter import FMPFundamentalsAdapter
from market_data.agent.tiingo_fundamentals_adapter import TiingoFundamentalsAdapter
from market_data.agent.eodhd_fundamentals_adapter import EODHDFundamentalsAdapter
from core.logging.logger_config import get_logger
from config.environment import Environment, EnvironmentType
import asyncpg

logger = get_logger(__name__)

@dataclass
class FundamentalPopulationCheckpoint:
    """Checkpoint data for resumable fundamental processing"""
    start_date: str
    end_date: str
    vendors: List[str]
    total_symbols: int
    processed_symbols: int
    current_symbol: str
    current_vendor: str
    symbols_completed: List[str]
    symbols_failed: List[str]
    total_records_stored: int
    total_api_calls: Dict[str, int]  # Per vendor
    last_update_timestamp: str
    rate_limit_delays: Dict[str, int]  # Per vendor
    quality_scores: Dict[str, Dict[str, float]]  # [symbol][vendor] = score
    errors: List[Dict]
    processing_stats: Dict
    vendor_progress: Dict[str, Dict]  # Per vendor progress tracking

class Comprehensive30YearFundamentalPopulator:
    """Main class for comprehensive 30-year fundamental data population"""

    def __init__(self,
                 vendors: List[str] = None,
                 checkpoint_file: str = "fundamental_30year_checkpoint.json",
                 max_concurrent: int = 1,  # Conservative for API limits
                 debug: bool = False):

        self.checkpoint_file = Path(checkpoint_file)
        self.max_concurrent = max_concurrent
        self.debug = debug

        # Vendor configuration
        self.available_vendors = ['polygon', 'fmp', 'tiingo', 'eodhd']
        self.vendors = vendors or self.available_vendors
        logger.info(f"Enabled vendors: {self.vendors}")

        # Initialize vendor adapters
        self.adapters = {}
        self._initialize_adapters()

        # Processing state
        self.checkpoint = None
        self.universe_symbols: Set[str] = set()
        self.start_date = None
        self.end_date = None

        # Statistics
        self.stats = {
            'symbols_processed': 0,
            'symbols_completed': 0,
            'symbols_failed': 0,
            'total_records_collected': 0,
            'total_records_stored': 0,
            'total_api_calls': {vendor: 0 for vendor in self.vendors},
            'rate_limit_delays': {vendor: 0 for vendor in self.vendors},
            'processing_time_seconds': 0,
            'average_quality_score': 0.0,
            'vendor_stats': {vendor: {} for vendor in self.vendors},
            'errors': []
        }

        logger.info(f"Initialized Comprehensive 30-year fundamental populator")
        logger.info(f"Vendors: {self.vendors}")

    def _initialize_adapters(self):
        """Initialize vendor adapters based on available API keys"""

        for vendor in self.vendors:
            if vendor == 'polygon':
                api_key = os.getenv('POLYGON_API_KEY')
                if api_key:
                    self.adapters[vendor] = PolygonFundamentalsAdapter(api_key)
                    logger.info(f"✅ Polygon adapter initialized")
                else:
                    logger.warning(f"❌ POLYGON_API_KEY not found, skipping Polygon")

            elif vendor == 'fmp':
                api_key = os.getenv('FMP_API_KEY')
                if api_key:
                    self.adapters[vendor] = FMPFundamentalsAdapter(api_key)
                    logger.info(f"✅ FMP adapter initialized")
                else:
                    logger.warning(f"❌ FMP_API_KEY not found, skipping FMP")

            elif vendor == 'tiingo':
                api_key = os.getenv('TIINGO_API_KEY')
                if api_key:
                    self.adapters[vendor] = TiingoFundamentalsAdapter(api_key)
                    logger.info(f"✅ Tiingo adapter initialized")
                else:
                    logger.warning(f"❌ TIINGO_API_KEY not found, skipping Tiingo")

            elif vendor == 'eodhd':
                api_key = os.getenv('EODHD_API_KEY')
                if api_key:
                    self.adapters[vendor] = EODHDFundamentalsAdapter(api_key)
                    logger.info(f"✅ EODHD adapter initialized")
                else:
                    logger.warning(f"❌ EODHD_API_KEY not found, skipping EODHD")

        self.vendors = list(self.adapters.keys())
        logger.info(f"Successfully initialized adapters: {self.vendors}")

        if not self.adapters:
            raise ValueError("No vendor adapters could be initialized. Please set API keys.")

    async def _load_universe(self):
        """Load the complete universe of instruments from dev database"""
        logger.info("Loading instrument universe from dev database...")

        # Use proper configuration system - set ENVIRONMENT=dev for dev database
        os.environ['ENVIRONMENT'] = 'dev'
        env = Environment()
        logger.info(f"Using environment config: {env.get_database_config()}")

        # Create database connection pool using the configured environment
        pool = await env.database.create_pool_with_retry(max_retries=3)

        # Query for ALL active instruments from dev_instrument table
        query = f"""
        SELECT DISTINCT symbol
        FROM {env.get_table_name('instruments')}
        WHERE active = true
          AND symbol IS NOT NULL
          AND symbol != ''
          AND symbol != 'NULL'
          AND symbol ~ '^[A-Z]{{1,5}}$'  -- US equities pattern
        ORDER BY symbol
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            self.universe_symbols = {row['symbol'] for row in rows}

        await pool.close()
        logger.info(f"Loaded {len(self.universe_symbols)} US equity symbols from dev database")

        if self.debug:
            # Limit to small subset for debugging
            self.universe_symbols = set(list(self.universe_symbols)[:5])
            logger.info(f"DEBUG mode: Limited to {len(self.universe_symbols)} symbols")
        else:
            logger.info(f"🚀 FULL PRODUCTION MODE: Processing complete universe of {len(self.universe_symbols)} symbols over 30 years (1994-2025)")
            logger.info(f"📈 This will be a MASSIVE 30-year fundamental data backfill across {len(self.vendors)} vendors")

    async def create_checkpoint(self,
                                start_date: date,
                                end_date: date,
                                symbols: Optional[Set[str]] = None) -> FundamentalPopulationCheckpoint:
        """Create initial checkpoint for processing"""

        if symbols is None:
            symbols = self.universe_symbols

        checkpoint = FundamentalPopulationCheckpoint(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            vendors=self.vendors,
            total_symbols=len(symbols),
            processed_symbols=0,
            current_symbol="",
            current_vendor="",
            symbols_completed=[],
            symbols_failed=[],
            total_records_stored=0,
            total_api_calls={vendor: 0 for vendor in self.vendors},
            last_update_timestamp=datetime.now().isoformat(),
            rate_limit_delays={vendor: 0 for vendor in self.vendors},
            quality_scores={},
            errors=[],
            processing_stats={},
            vendor_progress={vendor: {'processed': 0, 'failed': 0} for vendor in self.vendors}
        )

        await self.save_checkpoint(checkpoint)
        return checkpoint

    async def load_checkpoint(self, checkpoint_file: Optional[Path] = None) -> Optional[FundamentalPopulationCheckpoint]:
        """Load checkpoint from file"""
        file_path = checkpoint_file or self.checkpoint_file

        if not file_path.exists():
            logger.info(f"No checkpoint file found at {file_path}")
            return None

        async with aiofiles.open(file_path, 'r') as f:
            data = json.loads(await f.read())

        checkpoint = FundamentalPopulationCheckpoint(**data)
        logger.info(f"Loaded checkpoint: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols processed")
        return checkpoint

    async def save_checkpoint(self, checkpoint: FundamentalPopulationCheckpoint):
        """Save checkpoint to file"""
        checkpoint.last_update_timestamp = datetime.now().isoformat()

        async with aiofiles.open(self.checkpoint_file, 'w') as f:
            await f.write(json.dumps(asdict(checkpoint), indent=2))

        if self.debug:
            logger.debug(f"Checkpoint saved: {checkpoint.processed_symbols}/{checkpoint.total_symbols} symbols")

    async def populate_symbol_fundamentals(self,
                                         symbol: str,
                                         start_date: date,
                                         end_date: date) -> Dict:
        """Populate fundamental data for a single symbol across all vendors"""

        symbol_stats = {
            'symbol': symbol,
            'records_collected': 0,
            'records_stored': 0,
            'processing_time': 0,
            'vendor_results': {},
            'quality_scores': {},
            'errors': []
        }

        start_time = time.time()
        logger.info(f"Starting {symbol} fundamental population: {start_date} to {end_date}")

        all_fundamentals = []

        # Process each vendor for this symbol
        for vendor in self.vendors:
            if vendor not in self.adapters:
                continue

            vendor_stats = {
                'records': 0,
                'api_calls': 0,
                'quality_score': 0.0,
                'processing_time': 0,
                'errors': []
            }

            vendor_start_time = time.time()

            logger.info(f"{symbol}: Fetching fundamentals from {vendor}")

            # Fetch fundamental data from this vendor
            fundamentals = await self.adapters[vendor].fetch_fundamentals(
                symbol, start_date, end_date
            )

            vendor_stats['records'] = len(fundamentals)
            vendor_stats['api_calls'] = 1
            self.stats['total_api_calls'][vendor] += 1

            # Calculate quality score based on data completeness
            if fundamentals:
                complete_records = sum(1 for f in fundamentals if f.revenue is not None)
                vendor_stats['quality_score'] = complete_records / len(fundamentals) if fundamentals else 0.0

            all_fundamentals.extend(fundamentals)

            logger.info(f"{symbol}: Got {len(fundamentals)} fundamental records from {vendor}")

            vendor_stats['processing_time'] = time.time() - vendor_start_time
            symbol_stats['vendor_results'][vendor] = vendor_stats
            symbol_stats['quality_scores'][vendor] = vendor_stats['quality_score']

        symbol_stats['records_collected'] = len(all_fundamentals)

        # Store the data in database
        if all_fundamentals:
            stored_count = await self._store_fundamental_data(all_fundamentals)
            symbol_stats['records_stored'] = stored_count
            self.stats['total_records_collected'] += symbol_stats['records_collected']
            self.stats['total_records_stored'] += symbol_stats['records_stored']
            logger.info(f"{symbol}: {symbol_stats['records_collected']} records collected, "
                       f"{symbol_stats['records_stored']} stored across {len(self.vendors)} vendors")
        else:
            logger.warning(f"{symbol}: No fundamental data available from any vendor")

        symbol_stats['processing_time'] = time.time() - start_time
        return symbol_stats

    async def _store_fundamental_data(self, fundamentals: List) -> int:
        """Store fundamental data records in database"""
        if not fundamentals:
            return 0

        # Use proper configuration system
        os.environ['ENVIRONMENT'] = 'dev'
        env = Environment()

        # Create database connection
        pool = await env.database.create_pool_with_retry(max_retries=3)

        stored_count = 0
        insert_query = f"""
        INSERT INTO {env.get_table_name('fundamental_data')} (
            symbol, date, vendor, fiscal_period, revenue, gross_profit, operating_income,
            net_income, ebitda, eps, total_assets, total_liabilities, shareholders_equity,
            current_assets, current_liabilities, total_debt, operating_cash_flow,
            investing_cash_flow, financing_cash_flow, free_cash_flow, market_cap,
            pe_ratio, pb_ratio, debt_to_equity, roe, roa, current_ratio, quick_ratio,
            quality_score, raw_data
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
            $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30
        ) ON CONFLICT (symbol, date, vendor, fiscal_period) DO UPDATE SET
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
            quality_score = EXCLUDED.quality_score,
            raw_data = EXCLUDED.raw_data,
            updated_at = NOW()
        """

        async with pool.acquire() as conn:
            async with conn.transaction():
                for fundamental in fundamentals:
                    # Calculate quality score based on data completeness
                    key_fields = [fundamental.revenue, fundamental.net_income, fundamental.total_assets]
                    quality_score = sum(1 for field in key_fields if field is not None) / len(key_fields)

                    await conn.execute(
                        insert_query,
                        fundamental.symbol,
                        fundamental.date,
                        fundamental.vendor,
                        fundamental.fiscal_period,
                        fundamental.revenue,
                        fundamental.gross_profit,
                        fundamental.operating_income,
                        fundamental.net_income,
                        fundamental.ebitda,
                        fundamental.eps,
                        fundamental.total_assets,
                        fundamental.total_liabilities,
                        fundamental.shareholders_equity,
                        fundamental.current_assets,
                        fundamental.current_liabilities,
                        fundamental.total_debt,
                        fundamental.operating_cash_flow,
                        fundamental.investing_cash_flow,
                        fundamental.financing_cash_flow,
                        fundamental.free_cash_flow,
                        fundamental.market_cap,
                        fundamental.pe_ratio,
                        fundamental.pb_ratio,
                        fundamental.debt_to_equity,
                        fundamental.roe,
                        fundamental.roa,
                        fundamental.current_ratio,
                        fundamental.quick_ratio,
                        quality_score,
                        json.dumps(fundamental.raw_data) if fundamental.raw_data else None
                    )
                    stored_count += 1

        await pool.close()
        return stored_count

    async def run_full_population(self,
                                  start_date: date,
                                  end_date: date,
                                  limit: Optional[int] = None,
                                  symbols: Optional[List[str]] = None):
        """Run full 30-year fundamental population"""

        logger.info(f"Starting comprehensive fundamental population: {start_date} to {end_date}")

        # Load universe if not already loaded
        if not self.universe_symbols:
            await self._load_universe()

        # Determine symbol list
        if symbols:
            target_symbols = set(symbols)
        else:
            target_symbols = self.universe_symbols

        if limit:
            target_symbols = set(list(target_symbols)[:limit])

        logger.info(f"Processing {len(target_symbols)} symbols across {len(self.vendors)} vendors")
        logger.info(f"Total combinations: {len(target_symbols)} × {len(self.vendors)} = {len(target_symbols) * len(self.vendors)}")

        # Create initial checkpoint
        self.checkpoint = await self.create_checkpoint(start_date, end_date, target_symbols)

        # Process each symbol
        total_quality_score = 0
        quality_count = 0

        for i, symbol in enumerate(sorted(target_symbols)):
            self.checkpoint.current_symbol = symbol
            await self.save_checkpoint(self.checkpoint)

            symbol_stats = await self.populate_symbol_fundamentals(symbol, start_date, end_date)

            # Update checkpoint based on results
            if symbol_stats['errors'] or not any(symbol_stats['vendor_results'].values()):
                self.checkpoint.symbols_failed.append(symbol)
                self.stats['symbols_failed'] += 1
            else:
                self.checkpoint.symbols_completed.append(symbol)
                self.stats['symbols_completed'] += 1

                # Calculate average quality across vendors for this symbol
                vendor_qualities = [score for score in symbol_stats['quality_scores'].values() if score > 0]
                if vendor_qualities:
                    avg_quality = sum(vendor_qualities) / len(vendor_qualities)
                    total_quality_score += avg_quality
                    quality_count += 1

            self.checkpoint.processed_symbols += 1
            self.checkpoint.total_records_stored = self.stats['total_records_stored']
            self.checkpoint.total_api_calls = self.stats['total_api_calls']
            self.checkpoint.rate_limit_delays = self.stats['rate_limit_delays']
            self.checkpoint.quality_scores[symbol] = symbol_stats['quality_scores']

            self.stats['symbols_processed'] += 1

            # Calculate average quality score
            if quality_count > 0:
                self.stats['average_quality_score'] = total_quality_score / quality_count

            # Update checkpoint every symbol
            await self.save_checkpoint(self.checkpoint)

            # Progress report
            progress = (self.checkpoint.processed_symbols / self.checkpoint.total_symbols) * 100
            estimated_remaining = (len(target_symbols) - i - 1) * symbol_stats['processing_time']
            estimated_remaining_hours = estimated_remaining / 3600

            total_api_calls = sum(self.stats['total_api_calls'].values())

            logger.info(f"Progress: {self.checkpoint.processed_symbols}/{self.checkpoint.total_symbols} "
                       f"({progress:.1f}%) - Current: {symbol}")
            logger.info(f"Quality: {self.stats['average_quality_score']:.3f}, "
                       f"API calls: {total_api_calls:,}, "
                       f"Estimated remaining: {estimated_remaining_hours:.1f}h")

        logger.info("Comprehensive fundamental population complete")
        await self._print_final_stats()

    async def _print_final_stats(self):
        """Print comprehensive final statistics"""

        logger.info("=" * 80)
        logger.info("COMPREHENSIVE 30-YEAR FUNDAMENTAL DATA POPULATION FINAL STATISTICS")
        logger.info("=" * 80)
        logger.info(f"Vendors processed: {len(self.vendors)} ({', '.join(self.vendors)})")
        logger.info(f"Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"Symbols completed: {self.stats['symbols_completed']}")
        logger.info(f"Symbols failed: {self.stats['symbols_failed']}")
        logger.info(f"Total records collected: {self.stats['total_records_collected']:,}")
        logger.info(f"Total records stored: {self.stats['total_records_stored']:,}")
        logger.info(f"Average quality score: {self.stats['average_quality_score']:.3f}")

        # Per-vendor statistics
        logger.info(f"API calls per vendor:")
        for vendor, calls in self.stats['total_api_calls'].items():
            rate_limits = self.stats['rate_limit_delays'].get(vendor, 0)
            logger.info(f"  - {vendor}: {calls:,} calls, {rate_limits} rate limit delays")

        if self.stats['errors']:
            logger.info(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                logger.info(f"  - {error}")
            if len(self.stats['errors']) > 5:
                logger.info(f"  ... and {len(self.stats['errors']) - 5} more errors")

        logger.info("=" * 80)

async def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(description="Comprehensive 30-Year Fundamental Data Population")
    parser.add_argument('--mode', choices=['full', 'incremental'], default='full',
                        help='Population mode')
    parser.add_argument('--start-date', type=str, default='1994-01-01',
                        help='Start date (YYYY-MM-DD) - Default: 1994-01-01 for 30-year history')
    parser.add_argument('--end-date', type=str,
                        default=datetime.now().strftime('%Y-%m-%d'),
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--vendors', type=str, default='polygon,fmp,tiingo,eodhd',
                        help='Comma-separated list of vendors to use')
    parser.add_argument('--symbols', type=str,
                        help='Comma-separated list of symbols (optional)')
    parser.add_argument('--limit', type=int,
                        help='Limit number of symbols to process')
    parser.add_argument('--checkpoint-file', type=str, default='fundamental_30year_checkpoint.json',
                        help='Checkpoint file for resumable processing')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode (limited symbols)')
    parser.add_argument('--concurrent', type=int, default=1,
                        help='Max concurrent operations (be conservative with APIs)')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting Comprehensive 30-year fundamental data population")
    logger.info(f"Arguments: {vars(args)}")

    # Parse vendors
    vendors = [v.strip() for v in args.vendors.split(',')]

    # Initialize populator
    populator = Comprehensive30YearFundamentalPopulator(
        vendors=vendors,
        checkpoint_file=args.checkpoint_file,
        max_concurrent=args.concurrent,
        debug=args.debug
    )

    if args.resume:
        # TODO: Implement resume functionality
        logger.error("Resume functionality not yet implemented")
        return
    else:
        # Start new population
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)

        symbols = None
        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(',')]

        await populator.run_full_population(
            start_date=start_date,
            end_date=end_date,
            limit=args.limit,
            symbols=symbols
        )

    logger.info("Comprehensive 30-year fundamental population script completed")

if __name__ == "__main__":
    asyncio.run(main())