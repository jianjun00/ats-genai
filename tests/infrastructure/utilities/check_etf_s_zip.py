#!/usr/bin/env python3
"""
Check contents of ETF S zip file for SPY
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import zipfile
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_etf_s_zip():
    """Check ETF S zip file for SPY and other symbols"""
    zip_path = Path("/data/firstrate-data/etf/etf_S_full_1min_adjsplitdiv_1py2dog.zip")

    logger.info(f"🔍 Checking ETF S zip file: {zip_path.name}")

    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        # Get all file names in zip
        file_list = zip_file.namelist()

        # Extract symbol names from file paths
        symbols = set()
        for file_path in file_list:
            if file_path.endswith('.csv'):
                # Extract symbol from filename
                filename = Path(file_path).name
                if '_' in filename:
                    symbol = filename.split('_')[0]
                    symbols.add(symbol)

        logger.info(f"📊 Total files in zip: {len(file_list)}")
        logger.info(f"📊 Unique symbols found: {len(symbols)}")

        # Check for SPY specifically
        if 'SPY' in symbols:
            logger.info("🎉 ✅ SPY FOUND IN ETF S ZIP!")

            # Get SPY files
            spy_files = [f for f in file_list if f.startswith('SPY_')]
            logger.info(f"📁 SPY files: {len(spy_files)}")
            for spy_file in spy_files[:5]:  # Show first 5
                logger.info(f"   📄 {spy_file}")

            # Check date range by examining file names
            years = set()
            for spy_file in spy_files:
                parts = spy_file.split('_')
                if len(parts) >= 2:
                    date_part = parts[1]
                    if len(date_part) >= 4:
                        year = date_part[:4]
                        if year.isdigit():
                            years.add(year)
            if years:
                sorted_years = sorted(years)
                logger.info(f"📅 SPY data years: {sorted_years[0]} to {sorted_years[-1]} ({len(years)} years)")

        else:
            logger.info("❌ SPY not found in ETF S zip")

        # Show sample of symbols starting with S
        s_symbols = [s for s in symbols if s.startswith('S')]
        logger.info(f"📝 Sample S symbols (first 20): {sorted(s_symbols)[:20]}")

        return 'SPY' in symbols, sorted(symbols)

if __name__ == "__main__":
    spy_found, all_symbols = check_etf_s_zip()

    if spy_found:
        print("\n🎯 RESULT: SPY FOUND! Ready for backfill.")
    else:
        print(f"\n❌ RESULT: SPY not found. Available S symbols: {len([s for s in all_symbols if s.startswith('S')])}")