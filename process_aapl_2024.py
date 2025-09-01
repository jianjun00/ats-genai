#!/usr/bin/env python3
"""
Process AAPL 2024 data from FirstRate - focused test
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

async def process_aapl_2024():
    """Process AAPL 2024 data specifically"""
    logger.info("🚀 Processing AAPL 2024 data...")
    
    # Initialize components
    adapter = FirstRateAdapter("/data/firstrate-data")
    output_path = Path("/data/minute-bars/firstrate")
    minute_manager = FileBasedMinuteManager(str(output_path))
    
    # Find AAPL zip file
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
    
    # Process 2024 data month by month
    year = 2024
    total_records = 0
    
    for month in range(1, 13):  # Jan-Dec 2024
        month_str = f"{year}-{month:02d}"
        logger.info(f"🔄 Processing {month_str}...")
        
        # Define date range for the month
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1).replace(day=1)
            from datetime import timedelta
            end_date = end_date - timedelta(days=1)  # Last day of December
        else:
            end_date = date(year, month + 1, 1).replace(day=1)
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
            logger.info(f"   💾 Storage result: {result}")
        else:
            month_records = 0
        
        total_records += month_records
        logger.info(f"✅ {month_str}: {month_records:,} records processed")
    
    logger.info(f"🎉 AAPL 2024 complete: {total_records:,} total records")
    
    # Verify the output
    aapl_2024_path = output_path / "AAPL" / "2024"
    if aapl_2024_path.exists():
        parquet_files = list(aapl_2024_path.glob("*.parquet"))
        logger.info(f"📁 Created {len(parquet_files)} parquet files in {aapl_2024_path}")
    else:
        logger.warning(f"⚠️ Expected directory not found: {aapl_2024_path}")

if __name__ == "__main__":
    asyncio.run(process_aapl_2024())