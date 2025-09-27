#!/usr/bin/env python3
"""
Comprehensive FirstRate 30-Day Backfill
Downloads fresh data and backfills all instruments for the most recent 30 days
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from pathlib import Path
import logging

# Add src to path
sys.path.insert(0, 'src')

from infrastructure.vendor.firstrate.adapters.firstrate_daily_downloader import FirstRateDownloader, DownloadJob
from infrastructure.vendor.firstrate.adapters.firstrate_minute_adapter import FirstRateMinuteAdapter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def download_30_days_data():
    """Download 30 days of fresh FirstRate data"""
    logger.info("🚀 STEP 1: Downloading 30 days of fresh FirstRate data")
    
    downloader = FirstRateDownloader()
    jobs = [DownloadJob(asset_type="stock")]
    
    success_count = 0
    today = date.today()
    
    for days_ago in range(30):
        download_date = today - timedelta(days=days_ago)
        # Skip weekends - FirstRate typically doesn't have weekend data
        if download_date.weekday() >= 5:  # Saturday=5, Sunday=6
            continue
            
        logger.info(f"📥 Downloading {download_date}")
        results = await downloader.download_daily_data(jobs, download_date)
        if results.get("stock", False):
            success_count += 1
            logger.info(f"✅ Downloaded {download_date}")
        else:
            logger.info(f"⏭️ No data for {download_date}")
    logger.info(f"📊 Downloaded data for {success_count} trading days")
    return success_count > 0

async def get_all_instruments():
    """Get all available instruments from FirstRate data"""
    logger.info("🔍 STEP 2: Scanning for all available instruments")
    
    async with FirstRateMinuteAdapter() as adapter:
        # Get recent files to sample instruments from
        files = adapter.get_recent_firstrate_files(30)
        logger.info(f"📁 Found {len(files)} FirstRate files to scan")
        
        all_instruments = set()
        
        # Sample a few files to get instrument list
        sample_files = files[:5] if len(files) > 5 else files
        
        for zip_file in sample_files:
            import zipfile
            with zipfile.ZipFile(zip_file, 'r') as zf:
                # Get all symbols from this file
                txt_files = [f for f in zf.namelist() if f.endswith('_day_1min_adjsplit.txt')]
                file_symbols = [f.replace('_day_1min_adjsplit.txt', '') for f in txt_files]
                all_instruments.update(file_symbols)
                
        instruments_list = sorted(list(all_instruments))
        logger.info(f"📊 Found {len(instruments_list)} unique instruments")
        
        # Show sample
        if instruments_list:
            sample = instruments_list[:20]
            logger.info(f"🔍 Sample instruments: {', '.join(sample)}")
            if len(instruments_list) > 20:
                logger.info(f"... and {len(instruments_list) - 20} more")
        
        return instruments_list

async def run_comprehensive_backfill(instruments):
    """Run backfill for all instruments"""
    logger.info(f"🚀 STEP 3: Running comprehensive backfill for {len(instruments)} instruments")
    
    async with FirstRateMinuteAdapter() as adapter:
        # Process in batches to avoid overwhelming the system
        batch_size = 100
        total_processed = 0
        total_files_written = 0
        total_files_skipped = 0
        
        for i in range(0, len(instruments), batch_size):
            batch = instruments[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(instruments) + batch_size - 1) // batch_size
            
            logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} instruments)")
            logger.info(f"🔄 Batch symbols: {', '.join(batch[:10])}{'...' if len(batch) > 10 else ''}")
            
            results = await adapter.incremental_backfill_to_files(
                symbols=batch,
                days_back=30,
                output_path='/mnt/d/ats-data/minute-bars/firstrate'
            )
            
            batch_processed = len(results.get('symbols_processed', []))
            batch_written = results.get('files_written', 0)
            batch_skipped = results.get('files_skipped', 0)
            
            total_processed += batch_processed
            total_files_written += batch_written
            total_files_skipped += batch_skipped
            
            logger.info(f"✅ Batch {batch_num} complete: {batch_processed}/{len(batch)} processed, "
                       f"{batch_written} files written, {batch_skipped} skipped")
            
        logger.info(f"🎉 COMPREHENSIVE BACKFILL COMPLETE!")
        logger.info(f"📊 Total instruments processed: {total_processed}/{len(instruments)}")
        logger.info(f"📄 Total files written: {total_files_written}")
        logger.info(f"⏭️ Total files skipped: {total_files_skipped}")
        
        return {
            'total_instruments': len(instruments),
            'processed': total_processed,
            'files_written': total_files_written,
            'files_skipped': total_files_skipped
        }

async def main():
    """Main execution"""
    start_time = datetime.now()
    
    logger.info("="*80)
    logger.info("🏗️ FIRSTRATE COMPREHENSIVE 30-DAY BACKFILL")
    logger.info("="*80)
    logger.info(f"🕐 Started: {start_time}")
    
    # Step 1: Download fresh data
    download_success = await download_30_days_data()
    if not download_success:
        logger.error("❌ Failed to download fresh data - aborting")
        return
    
    # Step 2: Get all instruments
    instruments = await get_all_instruments()
    if not instruments:
        logger.error("❌ No instruments found - aborting")
        return
    
    # Step 3: Run comprehensive backfill
    results = await run_comprehensive_backfill(instruments)
    
    # Final summary
    duration = datetime.now() - start_time
    logger.info("="*80)
    logger.info("🏁 FINAL RESULTS")
    logger.info("="*80)
    logger.info(f"⏱️ Total duration: {duration}")
    logger.info(f"📊 Instruments found: {results['total_instruments']:,}")
    logger.info(f"✅ Instruments processed: {results['processed']:,}")
    logger.info(f"📄 Files written (updated): {results['files_written']:,}")
    logger.info(f"⏭️ Files skipped (no changes): {results['files_skipped']:,}")
    
    success_rate = (results['processed'] / results['total_instruments'] * 100) if results['total_instruments'] > 0 else 0
    logger.info(f"📈 Success rate: {success_rate:.1f}%")
    
    if results['files_written'] > 0:
        logger.info("🎯 BACKFILL SUCCESSFUL - Files updated with recent data!")
    else:
        logger.info("ℹ️ No files needed updates - all data was already current")
        
if __name__ == "__main__":
    asyncio.run(main())