#!/usr/bin/env python3
"""
Test fixed FirstRate backfill with a few symbols
"""

import sys
import asyncio
import logging

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

# Import the fixed processor
sys.path.insert(0, '/home/jianjun/ats-genai-data/scripts')

import os
import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List

from market_data.agent.firstrate_adapter import FirstRateAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager

async def test_fixed_backfill():
    """Test the fixed backfill with just a couple symbols"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🧪 Testing fixed FirstRate backfill...")
    
    # Initialize components  
    adapter = FirstRateAdapter("/data/firstrate-data")
    output_path = Path("/data/minute-bars/firstrate")
    minute_manager = FileBasedMinuteManager(str(output_path))
    
    # Get a small inventory subset for testing
    logger.info("📊 Getting symbol inventory (first zip only)...")
    zip_files = adapter.get_available_zip_files('stock')
    
    if not zip_files:
        logger.error("❌ No zip files found!")
        return
    
    # Process just the first zip file for testing
    test_zip = zip_files[0]
    logger.info(f"📦 Testing with: {test_zip.name}")
    
    symbols = adapter.extract_symbols_from_zip(test_zip)
    logger.info(f"🔢 Found {len(symbols)} symbols in test zip")
    
    # Test with first 3 symbols
    test_symbols = symbols[:3]
    logger.info(f"🎯 Testing with symbols: {test_symbols}")
    
    total_records = 0
    
    for symbol in test_symbols:
        logger.info(f"🔄 Processing {symbol}...")
        
        try:
            # Get date range for symbol
            min_date, max_date = adapter.get_date_range_for_symbol(test_zip, symbol)
            
            if not min_date or not max_date:
                logger.warning(f"⚠️ {symbol}: No valid date range, skipping")
                continue
                
            logger.info(f"   📅 Date range: {min_date} to {max_date}")
            
            # Process first month only for test
            tick_count = 0
            month_start = min_date.replace(day=1)
            month_end = month_start.replace(day=28)  # Safe end of month
            
            # Get ticks for first month
            for tick in adapter.process_minute_data_from_zip(test_zip, symbol, month_start, month_end):
                await minute_manager.store_minute_bar(
                    symbol=tick.symbol,
                    timestamp=tick.timestamp,
                    open_price=tick.open,
                    high_price=tick.high,
                    low_price=tick.low,
                    close_price=tick.close,
                    volume=tick.volume,
                    vendor="firstrate"
                )
                
                tick_count += 1
                if tick_count >= 100:  # Limit for test
                    break
            
            logger.info(f"✅ {symbol}: Processed {tick_count} ticks")
            total_records += tick_count
            
        except Exception as e:
            logger.error(f"❌ {symbol} failed: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    logger.info(f"🎉 Test completed! Total records: {total_records}")
    return total_records

if __name__ == "__main__":
    result = asyncio.run(test_fixed_backfill())