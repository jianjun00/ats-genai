#!/usr/bin/env python3
"""
Verify TSLA FirstRate data availability and date range
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_tsla_data():
    """Verify TSLA data is available in FirstRate zip files"""
    adapter = FirstRateAdapter("/data/firstrate-data")
    
    # Get available zip files
    zip_files = adapter.get_available_zip_files('stock')
    logger.info(f"Found {len(zip_files)} stock zip files")
    
    # Check which zip contains TSLA
    tsla_found = False
    tsla_zip = None
    tsla_date_range = None
    
    for zip_file in zip_files:
        logger.info(f"Checking {zip_file.name}...")
        symbols = adapter.extract_symbols_from_zip(zip_file)
        
        if 'TSLA' in symbols:
            tsla_found = True
            tsla_zip = zip_file
            min_date, max_date = adapter.get_date_range_for_symbol(zip_file, 'TSLA')
            tsla_date_range = (min_date, max_date)
            logger.info(f"✅ TSLA found in {zip_file.name}")
            logger.info(f"   Date range: {min_date} to {max_date}")
            break
    
    if not tsla_found:
        logger.error("❌ TSLA not found in any zip files")
        return False
    
    # Test reading a few records for different years
    test_years = [2010, 2015, 2020, 2024, 2025]
    
    for year in test_years:
        logger.info(f"Testing {year} data...")
        from datetime import date
        
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31) if year < 2025 else date(2025, 8, 31)
        
        count = 0
        try:
            for tick in adapter.process_minute_data_from_zip(
                tsla_zip, 'TSLA', start_date, end_date
            ):
                count += 1
                if count >= 10:  # Just test first 10 records
                    break
            
            logger.info(f"   {year}: Found {count} sample records")
            
        except Exception as e:
            logger.error(f"   {year}: Error - {e}")
    
    # Check existing TSLA data
    existing_path = Path("/data/minute-bars/firstrate/TSLA")
    if existing_path.exists():
        existing_years = sorted([d.name for d in existing_path.iterdir() if d.is_dir() and d.name.isdigit()])
        logger.info(f"📁 Existing TSLA data years: {existing_years}")
    else:
        logger.info(f"📁 No existing TSLA data found")
    
    return True

if __name__ == "__main__":
    verify_tsla_data()