#!/usr/bin/env python3
"""
Comprehensive Daily Price Population Script

This script analyzes data coverage for all instruments across Tiingo, EODHD, and Polygon
vendors and populates missing daily price data from listing date to now (or delisting date).

Usage:
    python3 scripts/run_dev.py run --script scripts/comprehensive_daily_price_population.py
"""

import asyncio
import asyncpg
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Set
import json
import time
from pathlib import Path

logger = logging.getLogger(__name__)

class DataCoverageAnalyzer:
    """Analyzes data coverage across all vendors and instruments."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.db_pool = None

    async def initialize(self):
        """Initialize database connection."""
        self.db_pool = await asyncpg.create_pool(
            self.db_url,
            min_size=2,
            max_size=10,
            command_timeout=300
        )
        logger.info("Database connection pool initialized")

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def analyze_coverage(self) -> Dict[str, any]:
        """Analyze data coverage across all vendors and instruments."""

        logger.info("Starting comprehensive data coverage analysis...")

        async with self.db_pool.acquire() as conn:
            # Get all instruments with their listing/delisting info
            instruments_query = """
            SELECT
                id,
                symbol,
                exchange,
                name,
                start_date as listing_date,
                end_date as delisting_date,
                CASE
                    WHEN end_date IS NULL THEN CURRENT_DATE
                    ELSE end_date
                END as effective_end_date
            FROM dev_instrument
            WHERE start_date IS NOT NULL
            ORDER BY symbol
            """

            instruments = await conn.fetch(instruments_query)
            logger.info(f"Found {len(instruments)} instruments with listing dates")

            # Analyze coverage for each vendor
            coverage_analysis = {}

            for vendor in ['tiingo', 'eodhd', 'polygon']:
                logger.info(f"Analyzing {vendor} coverage...")
                vendor_analysis = await self._analyze_vendor_coverage(conn, vendor, instruments)
                coverage_analysis[vendor] = vendor_analysis

        return {
            'total_instruments': len(instruments),
            'analysis_timestamp': datetime.now().isoformat(),
            'vendor_coverage': coverage_analysis
        }

    async def _analyze_vendor_coverage(self, conn, vendor: str, instruments) -> Dict[str, any]:
        """Analyze coverage for a specific vendor."""

        table_name = f"dev_daily_price_{vendor}"

        # Get current data coverage
        coverage_query = f"""
        SELECT
            i.id,
            i.symbol,
            i.start_date as listing_date,
            CASE
                WHEN i.end_date IS NULL THEN CURRENT_DATE
                ELSE i.end_date
            END as effective_end_date,
            MIN(p.date) as data_start_date,
            MAX(p.date) as data_end_date,
            COUNT(p.date) as total_records,
            CASE
                WHEN MIN(p.date) IS NULL THEN 'NO_DATA'
                WHEN MIN(p.date) > i.start_date + INTERVAL '30 days' THEN 'MISSING_EARLY_DATA'
                WHEN MAX(p.date) < CURRENT_DATE - INTERVAL '7 days' THEN 'MISSING_RECENT_DATA'
                ELSE 'GOOD_COVERAGE'
            END as coverage_status
        FROM dev_instrument i
        LEFT JOIN {table_name} p ON i.id = p.instrument_id
        WHERE i.start_date IS NOT NULL
        GROUP BY i.id, i.symbol, i.start_date, i.end_date
        ORDER BY coverage_status, i.symbol
        """

        coverage_data = await conn.fetch(coverage_query)

        # Categorize instruments
        no_data = [row for row in coverage_data if row['coverage_status'] == 'NO_DATA']
        missing_early = [row for row in coverage_data if row['coverage_status'] == 'MISSING_EARLY_DATA']
        missing_recent = [row for row in coverage_data if row['coverage_status'] == 'MISSING_RECENT_DATA']
        good_coverage = [row for row in coverage_data if row['coverage_status'] == 'GOOD_COVERAGE']

        # Calculate statistics
        total_expected_days = 0
        total_actual_days = 0

        for row in coverage_data:
            listing_date = row['listing_date']
            end_date = row['effective_end_date']

            # Calculate expected trading days (rough estimate: 252 days per year)
            years = (end_date - listing_date).days / 365.25
            expected_days = int(years * 252)
            total_expected_days += expected_days

            actual_days = row['total_records'] or 0
            total_actual_days += actual_days

        coverage_percentage = (total_actual_days / max(total_expected_days, 1)) * 100

        return {
            'vendor': vendor,
            'total_instruments': len(coverage_data),
            'instruments_with_no_data': len(no_data),
            'instruments_missing_early_data': len(missing_early),
            'instruments_missing_recent_data': len(missing_recent),
            'instruments_with_good_coverage': len(good_coverage),
            'overall_coverage_percentage': round(coverage_percentage, 2),
            'total_records': total_actual_days,
            'expected_records': total_expected_days,
            'no_data_symbols': [row['symbol'] for row in no_data[:100]], # Limit for readability
            'missing_early_symbols': [row['symbol'] for row in missing_early[:50]],
            'missing_recent_symbols': [row['symbol'] for row in missing_recent[:50]]
        }

class DailyPricePopulator:
    """Populates missing daily price data for specific vendors."""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.db_pool = None

    async def initialize(self):
        """Initialize database connection."""
        self.db_pool = await asyncpg.create_pool(
            self.db_url,
            min_size=2,
            max_size=10,
            command_timeout=300
        )
        logger.info("Daily price populator initialized")

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def populate_missing_data(self, vendor: str, symbols: List[str] = None, max_symbols: int = 100) -> Dict[str, any]:
        """Populate missing daily price data for specified vendor and symbols."""

        logger.info(f"Starting data population for {vendor} vendor...")

        async with self.db_pool.acquire() as conn:
            # Get instruments that need data population
            if symbols:
                symbol_filter = "AND i.symbol = ANY($1)"
                params = [symbols]
            else:
                symbol_filter = ""
                params = []

            instruments_query = f"""
            SELECT
                i.id,
                i.symbol,
                i.exchange,
                i.start_date as listing_date,
                CASE
                    WHEN i.end_date IS NULL THEN CURRENT_DATE
                    ELSE i.end_date
                END as effective_end_date,
                COALESCE(MAX(p.date), i.start_date - INTERVAL '1 day') as last_data_date
            FROM dev_instrument i
            LEFT JOIN dev_daily_price_{vendor} p ON i.id = p.instrument_id
            WHERE i.start_date IS NOT NULL {symbol_filter}
            GROUP BY i.id, i.symbol, i.exchange, i.start_date, i.end_date
            HAVING COALESCE(MAX(p.date), i.start_date - INTERVAL '1 day') < CURRENT_DATE - INTERVAL '7 days'
            ORDER BY i.symbol
            LIMIT ${"2" if symbols else "1"}
            """

            if symbols:
                instruments_to_populate = await conn.fetch(instruments_query, symbols, max_symbols)
            else:
                instruments_to_populate = await conn.fetch(instruments_query, max_symbols)

        logger.info(f"Found {len(instruments_to_populate)} instruments needing data population for {vendor}")

        # Generate population commands for each vendor
        population_commands = []

        for instrument in instruments_to_populate:
            symbol = instrument['symbol']
            start_date = max(instrument['last_data_date'] + timedelta(days=1), instrument['listing_date'])
            end_date = instrument['effective_end_date']

            if start_date <= end_date:
                if vendor == 'tiingo':
                    cmd = self._create_tiingo_population_command(symbol, start_date, end_date)
                elif vendor == 'eodhd':
                    cmd = self._create_eodhd_population_command(symbol, start_date, end_date)
                elif vendor == 'polygon':
                    cmd = self._create_polygon_population_command(symbol, start_date, end_date)
                else:
                    logger.warning(f"Unknown vendor: {vendor}")
                    continue

                population_commands.append({
                    'symbol': symbol,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'command': cmd,
                    'vendor': vendor
                })

        return {
            'vendor': vendor,
            'instruments_to_populate': len(instruments_to_populate),
            'population_commands': population_commands[:10],  # Show first 10 for review
            'total_commands': len(population_commands)
        }

    def _create_tiingo_population_command(self, symbol: str, start_date: date, end_date: date) -> str:
        """Create Tiingo population command."""
        return f"python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py --env '{{\"SYMBOLS_FILTER\": \"{symbol}\", \"START_DATE\": \"{start_date}\", \"END_DATE\": \"{end_date}\"}}'"

    def _create_eodhd_population_command(self, symbol: str, start_date: date, end_date: date) -> str:
        """Create EODHD population command."""
        return f"python3 scripts/run_dev.py run --script scripts/eodhd_30_year_daily_backfill.py --env '{{\"SYMBOLS_FILTER\": \"{symbol}\", \"START_DATE\": \"{start_date}\", \"END_DATE\": \"{end_date}\"}}'"

    def _create_polygon_population_command(self, symbol: str, start_date: date, end_date: date) -> str:
        """Create Polygon population command."""
        return f"python3 scripts/run_dev.py run --script scripts/run_polygon_backfill_direct.py --env '{{\"SYMBOLS_FILTER\": \"{symbol}\", \"START_DATE\": \"{start_date}\", \"END_DATE\": \"{end_date}\"}}'"

async def main():
    """Main function to run comprehensive daily price population."""

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("="*80)
    logger.info("COMPREHENSIVE DAILY PRICE POPULATION")
    logger.info("="*80)

    # Database connection - use Docker-compatible connection
    DATABASE_URL = "postgresql://postgres:dev_password@ats-dev-postgres:5432/dev_db"

    # Step 1: Analyze current coverage
    logger.info("\n🔍 STEP 1: Analyzing current data coverage...")
    analyzer = DataCoverageAnalyzer(DATABASE_URL)
    await analyzer.initialize()

    coverage_analysis = await analyzer.analyze_coverage()

    # Save coverage analysis
    output_dir = Path("/data/analysis")
    output_dir.mkdir(exist_ok=True)

    coverage_file = output_dir / f"coverage_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(coverage_file, 'w') as f:
        json.dump(coverage_analysis, f, indent=2, default=str)

    logger.info(f"Coverage analysis saved to: {coverage_file}")

    # Print summary
    logger.info("\n📊 COVERAGE SUMMARY:")
    logger.info(f"Total instruments: {coverage_analysis['total_instruments']:,}")

    for vendor, analysis in coverage_analysis['vendor_coverage'].items():
        logger.info(f"\n{vendor.upper()} Coverage:")
        logger.info(f"  Instruments with data: {analysis['total_instruments'] - analysis['instruments_with_no_data']:,}")
        logger.info(f"  No data: {analysis['instruments_with_no_data']:,}")
        logger.info(f"  Missing early data: {analysis['instruments_missing_early_data']:,}")
        logger.info(f"  Missing recent data: {analysis['instruments_missing_recent_data']:,}")
        logger.info(f"  Coverage: {analysis['overall_coverage_percentage']:.1f}%")

    await analyzer.close()

    # Step 2: Generate population commands
    logger.info("\n🔧 STEP 2: Generating population commands...")
    populator = DailyPricePopulator(DATABASE_URL)
    await populator.initialize()

    population_plans = {}
    for vendor in ['tiingo', 'eodhd', 'polygon']:
        logger.info(f"\nGenerating population plan for {vendor}...")
        plan = await populator.populate_missing_data(vendor, max_symbols=50)
        population_plans[vendor] = plan

        logger.info(f"{vendor}: {plan['instruments_to_populate']} instruments need population")
        if plan['population_commands']:
            logger.info(f"Sample command: {plan['population_commands'][0]['command']}")

    await populator.close()

    # Step 3: Save population plans
    plans_file = output_dir / f"population_plans_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(plans_file, 'w') as f:
        json.dump(population_plans, f, indent=2, default=str)

    logger.info(f"\nPopulation plans saved to: {plans_file}")

    # Step 4: Show execution summary
    logger.info("\n🚀 EXECUTION RECOMMENDATIONS:")
    logger.info("1. Review the coverage analysis and population plans")
    logger.info("2. Execute population commands for priority symbols (AAPL, TSLA, etc.)")
    logger.info("3. Run batch population for remaining symbols")
    logger.info("4. Monitor progress and validate data quality")

    total_commands = sum(plan['total_commands'] for plan in population_plans.values())
    logger.info(f"\nTotal population commands generated: {total_commands:,}")

    # Show specific commands for AAPL and TSLA if available
    priority_symbols = ['AAPL', 'TSLA']
    logger.info(f"\n🎯 PRIORITY SYMBOLS ({', '.join(priority_symbols)}):")

    for vendor, plan in population_plans.items():
        for cmd_info in plan['population_commands']:
            if cmd_info['symbol'] in priority_symbols:
                logger.info(f"{vendor.upper()} {cmd_info['symbol']}: {cmd_info['command']}")

    logger.info("\n✅ Comprehensive daily price population analysis completed!")

if __name__ == "__main__":
    asyncio.run(main())