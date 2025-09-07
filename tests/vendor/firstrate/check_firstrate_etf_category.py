#!/usr/bin/env python3
"""
Check FirstRate ETF data category for SPY, QQQ, DXY, TLT, USO, GLD, IWM
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from domains.market_data.services.agent.firstrate_adapter import FirstRateAdapter
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_etf_category():
    """Check ETF category in FirstRate data"""
    adapter = FirstRateAdapter("/data/firstrate-data")

    # Target ETFs/instruments
    target_symbols = ['SPY', 'QQQ', 'DXY', 'TLT', 'USO', 'GLD', 'IWM']

    # Check all available data types
    data_types = ['stock', 'etf', 'fx', 'index']

    found_symbols = {}

    for data_type in data_types:
        logger.info(f"🔍 Checking {data_type} category...")

        try:
            zip_files = adapter.get_available_zip_files(data_type)
            logger.info(f"   Found {len(zip_files)} {data_type} zip files")

            if not zip_files:
                logger.info(f"   No {data_type} zip files found")
                continue

            for zip_file in zip_files:
                logger.info(f"   Checking {zip_file.name}...")
                symbols = adapter.extract_symbols_from_zip(zip_file)
                logger.info(f"   Found {len(symbols)} symbols in {zip_file.name}")

                # Show a sample of symbols
                sample_symbols = list(symbols)[:10]
                logger.info(f"   Sample symbols: {sample_symbols}")

                for target in target_symbols:
                    if target in symbols:
                        min_date, max_date = adapter.get_date_range_for_symbol(zip_file, target)
                        found_symbols[target] = {
                            'category': data_type,
                            'zip_file': zip_file.name,
                            'min_date': min_date,
                            'max_date': max_date
                        }
                        logger.info(f"✅ {target} found in {data_type}/{zip_file.name}")
                        logger.info(f"   Date range: {min_date} to {max_date}")

        except Exception as e:
            logger.error(f"   Error checking {data_type}: {e}")

    # Final summary
    logger.info(f"\n📊 Final ETF/Instrument Availability Summary:")
    for symbol in target_symbols:
        if symbol in found_symbols:
            info = found_symbols[symbol]
            logger.info(f"✅ {symbol}: {info['category']} - {info['min_date']} to {info['max_date']} ({info['zip_file']})")
        else:
            logger.info(f"❌ {symbol}: Not found in any category")

    return found_symbols

if __name__ == "__main__":
    check_etf_category()