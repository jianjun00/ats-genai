#!/usr/bin/env python3
"""
Complete AAPL FirstRate missing years backfill: 2000-2004, 2025
Based on successful 2024 processing method
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

from market_data.agent.firstrate_adapter import FirstRateAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_aapl_missing_years():
    """Process all missing AAPL years: 2000-2004, 2025"""
    logger.info("🚀 Processing AAPL missing years: 2000-2004, 2025...")
    
    # Initialize components
    adapter = FirstRateAdapter("/data/firstrate-data")
    output_path = Path("/data/minute-bars/firstrate")
    minute_manager = FileBasedMinuteManager(str(output_path))
    
    # Find AAPL zip file (reuse from 2024 test)
    zip_files = adapter.get_available_zip_files('stock')
    aapl_zip = None
    
    for zip_file in zip_files:
        symbols = adapter.extract_symbols_from_zip(zip_file)
        if 'AAPL' in symbols:
            aapl_zip = zip_file
            logger.info(f"📦 Using {zip_file.name} for AAPL data")
            break
    
    if not aapl_zip:
        logger.error("❌ AAPL not found in any zip files")
        return
    
    # Define missing years to process
    missing_years = [2000, 2001, 2002, 2003, 2004, 2025]
    total_records = 0
    
    for year in missing_years:
        logger.info(f"📅 Processing year {year}...")
        year_records = 0
        
        # Determine month range for the year
        if year == 2025:
            months = range(1, 9)  # Jan-Aug 2025 (current)
        else:
            months = range(1, 13)  # All 12 months
        
        for month in months:
            month_str = f"{year}-{month:02d}"
            logger.info(f"🔄 Processing {month_str}...")
            
            # Define date range for the month
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1)
                from datetime import timedelta
                end_date = end_date - timedelta(days=1)  # Last day of December
            else:
                end_date = date(year, month + 1, 1)
                from datetime import timedelta
                end_date = end_date - timedelta(days=1)  # Last day of month
            
            # Get ticks for this month and collect into MinuteBar objects
            month_bars = []
            tick_generator = adapter.process_minute_data_from_zip(
                aapl_zip, 'AAPL', start_date, end_date
            )
            
            for tick in tick_generator:
                # Convert tick to MinuteBar
                bar = MinuteBar(
                    symbol=tick.symbol,
                    timestamp=tick.timestamp,
                    open=tick.open,
                    high=tick.high,
                    low=tick.low,
                    close=tick.close,
                    volume=tick.volume,
                    vendor="firstrate"
                )
                month_bars.append(bar)
                
                if len(month_bars) % 10000 == 0:
                    logger.info(f"   📈 Collected {len(month_bars):,} ticks for {month_str}")
            
            # Store all bars for this month
            if month_bars:
                result = await minute_manager.store_minute_data('AAPL', month_bars)
                month_records = len(month_bars)
                year_records += month_records
                logger.info(f"   ✅ {month_str}: {month_records:,} records stored")
            else:
                logger.info(f"   ⚠️ {month_str}: No data found")
        
        total_records += year_records
        logger.info(f"🎯 Year {year} complete: {year_records:,} records")
    
    logger.info(f"🎉 AAPL missing years processing complete!")
    logger.info(f"📊 Total records processed: {total_records:,}")
    
    # Final verification
    aapl_path = output_path / "AAPL"
    if aapl_path.exists():
        years = sorted([d.name for d in aapl_path.iterdir() if d.is_dir() and d.name.isdigit()])
        logger.info(f"📁 Final AAPL coverage: {years}")
        logger.info(f"🏆 Total years: {len(years)} (Target: 26 years from 2000-2025)")
        
        # Check if we achieved the complete dataset
        expected_years = set(str(y) for y in range(2000, 2026))
        actual_years = set(years)
        missing = expected_years - actual_years
        
        if not missing:
            logger.info("✅ COMPLETE: Full 26-year AAPL FirstRate dataset achieved!")
        else:
            logger.warning(f"⚠️ Still missing years: {sorted(missing)}")

if __name__ == "__main__":
    asyncio.run(process_aapl_missing_years())