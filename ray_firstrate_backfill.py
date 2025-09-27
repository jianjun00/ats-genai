#!/usr/bin/env python3
"""
Ray-Parallelized FirstRate 30-Day Backfill
Uses Ray to process multiple symbols concurrently for massive speedup
"""

import ray
import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from pathlib import Path
import logging
from typing import List, Dict, Any
import time

# Add src to path
sys.path.insert(0, 'src')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

@ray.remote
class FirstRateWorker:
    """Ray remote worker for processing FirstRate symbols."""
    
    def __init__(self):
        import sys
        sys.path.insert(0, 'src')
        from infrastructure.vendor.firstrate.adapters.firstrate_minute_adapter import FirstRateMinuteAdapter
        self.adapter = FirstRateMinuteAdapter()
    
    async def process_symbol_batch(self, symbols: List[str], days_back: int = 30, output_path: str = '/mnt/d/ats-data/minute-bars/firstrate') -> Dict[str, Any]:
        """Process a batch of symbols."""
        results = await self.adapter.incremental_backfill_to_files(
            symbols=symbols,
            days_back=days_back,
            output_path=output_path
        )
        return {
            'success': True,
            'symbols_processed': results.get('symbols_processed', []),
            'files_written': results.get('files_written', 0),
            'files_skipped': results.get('files_skipped', 0),
            'worker_id': ray.get_runtime_context().get_worker_id()
        }
async def get_all_firstrate_instruments() -> List[str]:
    """Get all available FirstRate instruments."""
    from infrastructure.vendor.firstrate.adapters.firstrate_minute_adapter import FirstRateMinuteAdapter
    
    async with FirstRateMinuteAdapter() as adapter:
        # Get recent files to sample instruments from
        files = adapter.get_recent_firstrate_files(30)
        logger.info(f"📁 Found {len(files)} FirstRate files to scan")
        
        all_instruments = set()
        
        # Sample first 5 files to get comprehensive instrument list
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
        
        return instruments_list

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

async def run_ray_parallel_backfill():
    """Run parallel FirstRate backfill using Ray."""
    
    start_time = datetime.now()
    logger.info("🚀 RAY-PARALLELIZED FIRSTRATE 30-DAY BACKFILL")
    logger.info("="*70)
    
    # Initialize Ray
    if not ray.is_initialized():
        # Initialize with configuration optimized for I/O bound tasks
        ray.init(
            num_cpus=os.cpu_count(),
            object_store_memory=2000000000,  # 2GB object store
            ignore_reinit_error=True
        )
    
    logger.info(f"🎯 Ray initialized with {ray.cluster_resources()['CPU']} CPUs")
    
    # Step 1: Get all instruments
    logger.info("🔍 STEP 1: Getting all available instruments")
    instruments = await get_all_firstrate_instruments()
    
    if not instruments:
        logger.error("❌ No instruments found - aborting")
        return
    
    total_instruments = len(instruments)
    logger.info(f"📊 Processing {total_instruments} instruments with Ray parallelization")
    
    # Step 2: Configure parallel processing
    # Use smaller batches per worker but more workers for better parallelization
    symbols_per_worker = 10  # Smaller batches for better load balancing
    num_workers = min(16, os.cpu_count() * 2)  # More workers than CPUs for I/O bound tasks
    
    logger.info(f"⚡ Configuration: {num_workers} workers, {symbols_per_worker} symbols per batch")
    
    # Create workers
    workers = [FirstRateWorker.remote() for _ in range(num_workers)]
    logger.info(f"🔧 Created {len(workers)} Ray workers")
    
    # Step 3: Distribute work across workers
    symbol_chunks = chunk_list(instruments, symbols_per_worker)
    total_batches = len(symbol_chunks)
    
    logger.info(f"📦 Split {total_instruments} symbols into {total_batches} batches")
    logger.info(f"🚀 Starting parallel processing...")
    
    # Process batches in parallel with progress tracking
    futures = []
    for i, chunk in enumerate(symbol_chunks):
        worker = workers[i % len(workers)]  # Round-robin assignment
        future = worker.process_symbol_batch.remote(
            symbols=chunk,
            days_back=30,
            output_path='/mnt/d/ats-data/minute-bars/firstrate'
        )
        futures.append((future, i + 1, chunk))
    
    # Collect results with progress tracking
    completed_batches = 0
    total_processed = 0
    total_files_written = 0
    total_files_skipped = 0
    errors = []
    
    logger.info(f"⏳ Processing {len(futures)} batches in parallel...")
    
    # Process results as they complete
    remaining_futures = [(f, batch_num, chunk) for f, batch_num, chunk in futures]
    
    while remaining_futures:
        # Wait for at least one result
        ready_futures, remaining_futures_temp = ray.wait([f for f, _, _ in remaining_futures], num_returns=1, timeout=30)
        
        if not ready_futures:
            logger.info("⏳ Still processing...")
            continue
        
        # Update remaining futures
        completed_future = ready_futures[0]
        batch_info = None
        for i, (f, batch_num, chunk) in enumerate(remaining_futures):
            if f == completed_future:
                batch_info = (f, batch_num, chunk)
                remaining_futures.pop(i)
                break
        
        if batch_info:
            future, batch_num, chunk = batch_info
            result = ray.get(future)
            completed_batches += 1
            
            if result['success']:
                batch_processed = len(result['symbols_processed'])
                batch_written = result['files_written']
                batch_skipped = result['files_skipped']
                
                total_processed += batch_processed
                total_files_written += batch_written
                total_files_skipped += batch_skipped
                
                progress_pct = (completed_batches / total_batches) * 100
                logger.info(f"✅ Batch {batch_num}/{total_batches} ({progress_pct:.1f}%): "
                           f"{batch_processed}/{len(chunk)} processed, "
                           f"{batch_written} written, {batch_skipped} skipped")
            else:
                errors.append(f"Batch {batch_num}: {result['error']}")
                logger.error(f"❌ Batch {batch_num} failed: {result['error']}")
                
    duration = datetime.now() - start_time
    success_rate = (total_processed / total_instruments * 100) if total_instruments > 0 else 0
    
    logger.info("="*70)
    logger.info("🏁 RAY PARALLEL BACKFILL COMPLETE")
    logger.info("="*70)
    logger.info(f"⏱️ Total duration: {duration}")
    logger.info(f"📊 Total instruments: {total_instruments:,}")
    logger.info(f"✅ Instruments processed: {total_processed:,}")
    logger.info(f"📄 Files written (updated): {total_files_written:,}")
    logger.info(f"⏭️ Files skipped (no changes): {total_files_skipped:,}")
    logger.info(f"📈 Success rate: {success_rate:.1f}%")
    logger.info(f"⚡ Speedup: {num_workers}x parallel processing")
    
    if errors:
        logger.info(f"⚠️ Errors encountered: {len(errors)}")
        for error in errors[:5]:  # Show first 5 errors
            logger.info(f"  • {error}")
        if len(errors) > 5:
            logger.info(f"  ... and {len(errors) - 5} more errors")
    
    # Performance metrics
    throughput = total_processed / duration.total_seconds() if duration.total_seconds() > 0 else 0
    logger.info(f"🚀 Processing throughput: {throughput:.1f} symbols/second")
    
    if total_files_written > 0:
        logger.info(f"🎯 SUCCESS: Updated {total_files_written:,} files with recent data!")
    else:
        logger.info("ℹ️ All files were already up to date")
        
if __name__ == "__main__":
    asyncio.run(run_ray_parallel_backfill())