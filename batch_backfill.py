#!/usr/bin/env python3
"""
Batch FirstRate Backfill
Process symbols in small batches to avoid timeouts
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

async def run_batch_backfill():
    """Run backfill in small batches"""
    
    # Define batches of symbols
    batches = [
        # Batch 1: Remaining mega caps
        ['META', 'NFLX', 'BABA'],
        
        # Batch 2: Major ETFs  
        ['VTI', 'XLF', 'XLK', 'XLE'],
        
        # Batch 3: Financial & Healthcare
        ['JPM', 'JNJ', 'UNH', 'V'],
        
        # Batch 4: Consumer & Tech
        ['PG', 'HD', 'MA', 'ADBE'],
        
        # Batch 5: Additional popular
        ['DIS', 'CRM', 'KO', 'PEP']
    ]
    
    start_time = datetime.now()
    logger.info("🚀 BATCH FIRSTRATE BACKFILL")
    logger.info("="*50)
    
    total_processed = 0
    total_files_written = 0
    total_files_skipped = 0
    
    async with FirstRateMinuteAdapter() as adapter:
        files = adapter.get_recent_firstrate_files(30)
        logger.info(f"📁 Found {len(files)} FirstRate files for past 30 days")
        
        for batch_num, batch_symbols in enumerate(batches, 1):
            logger.info(f"\n📦 BATCH {batch_num}: Processing {batch_symbols}")
            
            results = await adapter.incremental_backfill_to_files(
                symbols=batch_symbols,
                days_back=30,
                output_path='/mnt/d/ats-data/minute-bars/firstrate'
            )
            
            batch_processed = len(results.get('symbols_processed', []))
            batch_written = results.get('files_written', 0)
            batch_skipped = results.get('files_skipped', 0)
            
            total_processed += batch_processed
            total_files_written += batch_written
            total_files_skipped += batch_skipped
            
            logger.info(f"✅ Batch {batch_num}: {batch_processed}/{len(batch_symbols)} processed, {batch_written} written, {batch_skipped} skipped")
            
            # Show which files were updated
            if batch_written > 0:
                for symbol in batch_symbols:
                    for month in ['08', '09']:
                        file_path = f"/mnt/d/ats-data/minute-bars/firstrate/{symbol[0]}/{symbol}/2025/{month}/{symbol}_2025_{month}.parquet"
                        if Path(file_path).exists():
                            mod_time = datetime.fromtimestamp(Path(file_path).stat().st_mtime)
                            if (datetime.now() - mod_time).total_seconds() < 300:  # Updated in last 5 minutes
                                logger.info(f"  📄 Updated: {symbol}_2025_{month}.parquet")
            
    duration = datetime.now() - start_time
    logger.info("\n" + "="*50)
    logger.info("🏁 BATCH BACKFILL COMPLETE")
    logger.info("="*50)
    logger.info(f"⏱️ Total duration: {duration}")
    logger.info(f"📊 Total symbols processed: {total_processed}")
    logger.info(f"📄 Files written (updated): {total_files_written}")
    logger.info(f"⏭️ Files skipped (no changes): {total_files_skipped}")
    
    if total_files_written > 0:
        logger.info(f"🎯 SUCCESS: Updated {total_files_written} files with recent data!")

if __name__ == "__main__":
    asyncio.run(run_batch_backfill())