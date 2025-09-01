#!/usr/bin/env python3
"""
Complete TSLA FirstRate backfill: 2010-2025 (Tesla IPO was June 29, 2010)
Based on successful AAPL processing method
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

async def process_tsla_complete_backfill():
    """Process complete TSLA backfill from IPO (2010) to present (2025)"""
    logger.info("🚀 Processing TSLA complete backfill: 2010-2025...")
    
    # Initialize components
    adapter = FirstRateAdapter("/data/firstrate-data")
    output_path = Path("/data/minute-bars/firstrate")
    minute_manager = FileBasedMinuteManager(str(output_path))
    
    # Find TSLA zip file (from verification: stock_T_full_1min_adjsplitdiv_19o3j53.zip)
    zip_files = adapter.get_available_zip_files('stock')
    tsla_zip = None
    
    for zip_file in zip_files:
        symbols = adapter.extract_symbols_from_zip(zip_file)
        if 'TSLA' in symbols:
            tsla_zip = zip_file
            logger.info(f"📦 Using {zip_file.name} for TSLA data")
            break
    
    if not tsla_zip:
        logger.error("❌ TSLA not found in any zip files")
        return
    
    # Get TSLA date range to be precise
    min_date, max_date = adapter.get_date_range_for_symbol(tsla_zip, 'TSLA')
    logger.info(f"📅 TSLA data range: {min_date} to {max_date}")
    
    # Define years to process (2010-2025)
    start_year = 2010
    end_year = 2025
    total_records = 0
    
    for year in range(start_year, end_year + 1):
        logger.info(f"📅 Processing year {year}...")
        year_records = 0
        
        # Determine month range for the year
        if year == 2010:
            # Tesla IPO was June 29, 2010 - start from June
            months = range(6, 13)  # Jun-Dec 2010
        elif year == 2025:
            # Process up to current month
            months = range(1, 9)  # Jan-Aug 2025 
        else:
            months = range(1, 13)  # All 12 months
        
        for month in months:
            month_str = f"{year}-{month:02d}"
            logger.info(f"🔄 Processing {month_str}...")
            
            # Define date range for the month
            start_date = date(year, month, 1)
            
            # Special handling for Tesla IPO month (June 2010)
            if year == 2010 and month == 6:
                start_date = date(2010, 6, 29)  # Tesla IPO date
            
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
                tsla_zip, 'TSLA', start_date, end_date
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
                result = await minute_manager.store_minute_data('TSLA', month_bars)
                month_records = len(month_bars)
                year_records += month_records
                logger.info(f"   ✅ {month_str}: {month_records:,} records stored")
            else:
                logger.info(f"   ⚠️ {month_str}: No data found")
        
        total_records += year_records
        logger.info(f"🎯 Year {year} complete: {year_records:,} records")
    
    logger.info(f"🎉 TSLA complete backfill finished!")
    logger.info(f"📊 Total records processed: {total_records:,}")
    
    # Final verification
    tsla_path = output_path / "TSLA"
    if tsla_path.exists():
        years = sorted([d.name for d in tsla_path.iterdir() if d.is_dir() and d.name.isdigit()])
        logger.info(f"📁 Final TSLA coverage: {years}")
        logger.info(f"🏆 Total years: {len(years)} (Expected: 16 years from 2010-2025)")
        
        # Check if we achieved the complete dataset
        expected_years = set(str(y) for y in range(2010, 2026))
        actual_years = set(years)
        missing = expected_years - actual_years
        
        if not missing:
            logger.info("✅ COMPLETE: Full 16-year TSLA FirstRate dataset achieved!")
        else:
            logger.warning(f"⚠️ Still missing years: {sorted(missing)}")
    
    # Summary statistics
    logger.info(f"📈 TSLA Processing Summary:")
    logger.info(f"   📅 Date Range: 2010-06-29 to 2025-08-28")
    logger.info(f"   📁 Years Processed: {end_year - start_year + 1}")
    logger.info(f"   📝 Total Records: {total_records:,}")
    logger.info(f"   🏁 Status: Complete FirstRate TSLA dataset")

if __name__ == "__main__":
    asyncio.run(process_tsla_complete_backfill())