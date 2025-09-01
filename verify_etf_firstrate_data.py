#!/usr/bin/env python3
"""
Verify ETF FirstRate data availability: SPY, QQQ, DXY, TLT, USO, GLD, IWM
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_etf_data():
    """Verify ETF data availability in FirstRate zip files"""
    adapter = FirstRateAdapter("/data/firstrate-data")
    
    # Target ETFs/instruments
    target_symbols = ['SPY', 'QQQ', 'DXY', 'TLT', 'USO', 'GLD', 'IWM']
    
    # Get available zip files
    zip_files = adapter.get_available_zip_files('stock')
    logger.info(f"Found {len(zip_files)} stock zip files")
    
    # Check which symbols are found
    found_symbols = {}
    
    for zip_file in zip_files:
        logger.info(f"Checking {zip_file.name}...")
        symbols = adapter.extract_symbols_from_zip(zip_file)
        
        for target in target_symbols:
            if target in symbols:
                min_date, max_date = adapter.get_date_range_for_symbol(zip_file, target)
                found_symbols[target] = {
                    'zip_file': zip_file.name,
                    'min_date': min_date,
                    'max_date': max_date
                }
                logger.info(f"✅ {target} found in {zip_file.name}")
                logger.info(f"   Date range: {min_date} to {max_date}")
    
    # Report findings
    logger.info(f"\n📊 ETF/Instrument Availability Summary:")
    for symbol in target_symbols:
        if symbol in found_symbols:
            info = found_symbols[symbol]
            logger.info(f"✅ {symbol}: {info['min_date']} to {info['max_date']} ({info['zip_file']})")
        else:
            logger.info(f"❌ {symbol}: Not found in any zip files")
    
    # Test reading sample data for found symbols
    logger.info(f"\n🧪 Testing sample data retrieval:")
    for symbol in found_symbols:
        logger.info(f"Testing {symbol}...")
        info = found_symbols[symbol]
        
        # Find the zip file
        zip_file = None
        for zf in zip_files:
            if zf.name == info['zip_file']:
                zip_file = zf
                break
        
        if zip_file:
            from datetime import date
            test_year = 2020  # Test a recent year
            
            start_date = date(test_year, 1, 1)
            end_date = date(test_year, 1, 31)
            
            count = 0
            try:
                for tick in adapter.process_minute_data_from_zip(
                    zip_file, symbol, start_date, end_date
                ):
                    count += 1
                    if count >= 10:  # Just test first 10 records
                        break
                
                logger.info(f"   {symbol} Jan 2020: Found {count} sample records")
                
            except Exception as e:
                logger.error(f"   {symbol} Jan 2020: Error - {e}")
    
    # Check existing data
    logger.info(f"\n📁 Checking existing processed data:")
    for symbol in found_symbols:
        existing_path = Path(f"/data/minute-bars/firstrate/{symbol}")
        if existing_path.exists():
            existing_years = sorted([d.name for d in existing_path.iterdir() if d.is_dir() and d.name.isdigit()])
            logger.info(f"📁 {symbol}: Existing years {existing_years}")
        else:
            logger.info(f"📁 {symbol}: No existing data found")
    
    return found_symbols

if __name__ == "__main__":
    verify_etf_data()