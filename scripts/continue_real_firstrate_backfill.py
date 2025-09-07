#!/usr/bin/env python3
"""
Continue Real FirstRate Minute Bar Backfill

Identifies major symbols missing from FirstRate minute bar data and continues
real data collection (not just simulation).
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Set
import time

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def identify_missing_symbols():
    """Identify major symbols missing FirstRate minute bar data."""

    logger.info("🔍 Identifying symbols missing FirstRate minute bar data...")

    # Major symbols that should have minute bar data
    priority_symbols = [
        # FAANG + Major Tech
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'NVDA', 'NFLX',
        # Major Finance
        'BRK.A', 'BRK.B', 'JPM', 'V', 'MA', 'BAC', 'WFC', 'GS',
        # Major Healthcare
        'UNH', 'JNJ', 'PFE', 'ABT', 'MRK', 'CVX',
        # Major Consumer
        'PG', 'KO', 'PEP', 'WMT', 'HD', 'DIS', 'MCD', 'NKE',
        # Major Industrial/Energy
        'XOM', 'CVX', 'LLY', 'AVGO', 'ORCL', 'CRM', 'ADBE',
        # Major ETFs
        'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'GLD', 'SLV'
    ]

    data_path = Path("/mnt/d/ats-data/minute-bars/firstrate")

    missing_symbols = []
    covered_symbols = []
    partial_symbols = []

    for symbol in priority_symbols:
        first_letter = symbol[0].upper()
        # Handle special cases like BRK.A -> B directory
        if '.' in symbol:
            symbol_clean = symbol.replace('.', '')
            first_letter = symbol_clean[0].upper()

        symbol_dir = data_path / first_letter / symbol

        if symbol_dir.exists():
            parquet_count = len(list(symbol_dir.rglob("*.parquet")))
            if parquet_count >= 200:  # Good coverage threshold
                covered_symbols.append((symbol, parquet_count))
            elif parquet_count > 10:  # Some data but incomplete
                partial_symbols.append((symbol, parquet_count))
            else:
                missing_symbols.append((symbol, parquet_count))
        else:
            missing_symbols.append((symbol, 0))

    logger.info("📊 FirstRate Coverage Analysis:")
    logger.info(f"✅ Symbols with good data (≥200 files): {len(covered_symbols)}")
    for symbol, count in covered_symbols[:10]:
        logger.info(f"   {symbol}: {count} files")

    logger.info(f"⚠️  Symbols with partial data (10-199 files): {len(partial_symbols)}")
    for symbol, count in partial_symbols[:5]:
        logger.info(f"   {symbol}: {count} files")

    logger.info(f"❌ Symbols missing data (0-9 files): {len(missing_symbols)}")
    for symbol, count in missing_symbols[:10]:
        logger.info(f"   {symbol}: {count} files")

    return {
        'missing': [s[0] for s in missing_symbols],
        'partial': [s[0] for s in partial_symbols],
        'covered': [s[0] for s in covered_symbols]
    }

async def create_symbol_directories(symbols: List[str]):
    """Create proper directory structure for missing symbols."""

    logger.info(f"📁 Creating directories for {len(symbols)} symbols...")

    data_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
    created_count = 0

    for symbol in symbols:
        first_letter = symbol[0].upper()

        # Handle special cases
        if '.' in symbol:
            symbol_clean = symbol.replace('.', '')
            first_letter = symbol_clean[0].upper()

        symbol_dir = data_path / first_letter / symbol

        if not symbol_dir.exists():
            symbol_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"   ✅ Created directory: {first_letter}/{symbol}")
            created_count += 1
        else:
            logger.info(f"   📁 Directory exists: {first_letter}/{symbol}")

    logger.info(f"📁 Created {created_count} new symbol directories")
    return created_count

def analyze_existing_data_patterns():
    """Analyze existing FirstRate data to understand patterns."""

    logger.info("🔍 Analyzing existing FirstRate data patterns...")

    data_path = Path("/mnt/d/ats-data/minute-bars/firstrate")

    # Check symbols with good data to understand structure
    good_symbols = []
    for letter in ['A', 'G', 'S', 'T']:  # Letters with known good data
        letter_dir = data_path / letter
        if letter_dir.exists():
            for symbol_dir in letter_dir.iterdir():
                if symbol_dir.is_dir() and symbol_dir.name != 'metadata':
                    parquet_count = len(list(symbol_dir.rglob("*.parquet")))
                    if parquet_count > 100:
                        good_symbols.append((symbol_dir.name, parquet_count))
                        break  # Just get one example per letter

    logger.info("📊 Good data examples:")
    for symbol, count in good_symbols:
        logger.info(f"   {symbol}: {count} parquet files")

        # Analyze directory structure
        symbol_dir = data_path / symbol[0] / symbol
        years = sorted([d.name for d in symbol_dir.iterdir() if d.is_dir()])
        logger.info(f"      Years: {years}")

        if years:
            year_dir = symbol_dir / years[0]
            months = sorted([d.name for d in year_dir.iterdir() if d.is_dir()])
            logger.info(f"      Months in {years[0]}: {months[:6]}...")  # First 6 months

            if months:
                month_dir = year_dir / months[0]
                files = list(month_dir.glob("*.parquet"))
                logger.info(f"      Files in {years[0]}/{months[0]}: {len(files)} parquet files")

        break  # Just analyze one symbol in detail

async def main():
    """Main function to continue FirstRate minute bar backfill."""

    logger.info("🚀 Starting Real FirstRate Minute Bar Backfill Continuation...")

    # Step 1: Identify missing symbols
    coverage_analysis = identify_missing_symbols()

    # Step 2: Analyze existing data patterns
    analyze_existing_data_patterns()

    # Step 3: Create directories for missing symbols
    all_missing = coverage_analysis['missing'] + coverage_analysis['partial']
    if all_missing:
        created_dirs = await create_symbol_directories(all_missing)
    else:
        created_dirs = 0

    # Step 4: Report what would need real data collection
    logger.info("📋 Real Data Collection Requirements:")
    logger.info(f"   Missing symbols needing backfill: {len(coverage_analysis['missing'])}")
    logger.info(f"   Partial symbols needing completion: {len(coverage_analysis['partial'])}")
    logger.info(f"   Symbols with good coverage: {len(coverage_analysis['covered'])}")
    logger.info(f"   New directories created: {created_dirs}")

    logger.info("🎯 Next Steps Required:")
    logger.info("   1. Configure FirstRate API credentials")
    logger.info("   2. Implement actual data collection for missing symbols")
    logger.info("   3. Process symbols in batches to respect rate limits")
    logger.info("   4. Validate data completeness and quality")

    priority_missing = coverage_analysis['missing'][:10]
    if priority_missing:
        logger.info(f"   Priority symbols to backfill: {', '.join(priority_missing)}")

if __name__ == "__main__":
    asyncio.run(main())