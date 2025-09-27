#!/usr/bin/env python3
"""
Quick Priority FirstRate Backfill
Focus on just the most critical symbols to get immediate value
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from pathlib import Path
import logging

# Add src to path
sys.path.insert(0, 'src')

from infrastructure.vendor.firstrate.adapters.firstrate_minute_adapter import FirstRateMinuteAdapter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_quick_backfill():
    """Run backfill for highest priority symbols only"""
    
    # Top 10 most critical symbols - market movers and major ETFs
    critical_symbols = [
        'SPY',    # S&P 500 ETF - most important
        'QQQ',    # NASDAQ 100 ETF
        'AAPL',   # Apple
        'MSFT',   # Microsoft  
        'GOOGL',  # Google
        'AMZN',   # Amazon
        'TSLA',   # Tesla
        'NVDA',   # NVIDIA
        'META',   # Meta
        'IWM'     # Russell 2000 ETF
    ]
    
    start_time = datetime.now()
    logger.info("🚀 QUICK PRIORITY FIRSTRATE BACKFILL")
    logger.info("="*50)
    logger.info(f"🎯 Processing {len(critical_symbols)} critical symbols")
    
    async with FirstRateMinuteAdapter() as adapter:
        # Check available files
        files = adapter.get_recent_firstrate_files(30)
        logger.info(f"📁 Found {len(files)} FirstRate files for past 30 days")
        
        # Process critical symbols
        results = await adapter.incremental_backfill_to_files(
            symbols=critical_symbols,
            days_back=30,
            output_path='/mnt/d/ats-data/minute-bars/firstrate'
        )
        
        processed = len(results.get('symbols_processed', []))
        written = results.get('files_written', 0)
        skipped = results.get('files_skipped', 0)
        
        # Summary
        duration = datetime.now() - start_time
        logger.info("="*50)
        logger.info("🏁 QUICK PRIORITY BACKFILL COMPLETE")
        logger.info("="*50)
        logger.info(f"⏱️ Total duration: {duration}")
        logger.info(f"📊 Symbols processed: {processed}/{len(critical_symbols)}")
        logger.info(f"📄 Files written (updated): {written}")
        logger.info(f"⏭️ Files skipped (no changes): {skipped}")
        
        if written > 0:
            logger.info(f"🎯 SUCCESS: Updated {written} critical files!")
            
            # Show examples of updated files
            logger.info("\n📁 Updated files:")
            for symbol in critical_symbols:
                for month in ['08', '09']:
                    file_path = f"/mnt/d/ats-data/minute-bars/firstrate/{symbol[0]}/{symbol}/2025/{month}/{symbol}_2025_{month}.parquet"
                    if Path(file_path).exists():
                        mod_time = datetime.fromtimestamp(Path(file_path).stat().st_mtime)
                        # Show if modified in last hour (recently updated)
                        if (datetime.now() - mod_time).total_seconds() < 3600:
                            logger.info(f"  ✅ {symbol}: {symbol}_2025_{month}.parquet (updated: {mod_time})")
        else:
            logger.info("ℹ️ All critical files were already up to date")
            
if __name__ == "__main__":
    asyncio.run(run_quick_backfill())