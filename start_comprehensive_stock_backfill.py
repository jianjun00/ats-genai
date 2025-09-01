#!/usr/bin/env python3
"""
Start Comprehensive FirstRate Stock Backfill
Launches processing of remaining 6,827 stock symbols from FirstRate data

This is a MASSIVE backfill operation:
- 6,827 stock symbols remaining
- 43.5 GB of ZIP data
- Estimated 227+ hours processing time
- Should be run continuously for ~9-10 days
"""

import os
import sys
import time
import json
import subprocess
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def load_remaining_symbols():
    """Load the list of remaining symbols to process"""
    
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    
    if not analysis_file.exists():
        logger.error("Analysis file not found. Run analyze_stock_universe.py first!")
        return []
    
    with open(analysis_file, 'r') as f:
        analysis = json.load(f)
        
    remaining = analysis.get('remaining_symbols', [])
    logger.info(f"Found {len(remaining)} symbols to process")
    
    return remaining


def create_prioritized_batches(symbols, batch_size=50):
    """Create prioritized processing batches"""
    
    # Prioritize by common/well-known symbols first
    priority_symbols = []
    regular_symbols = []
    
    # High-priority symbols (major stocks, common names)
    high_priority = {
        'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'PFE', 'JNJ', 'V',
        'WMT', 'JPM', 'UNH', 'HD', 'PG', 'MA', 'DIS', 'PYPL', 'BAC', 'NFLX',
        'CRM', 'XOM', 'CVX', 'PEP', 'KO', 'ABT', 'COST', 'TMUS', 'AVGO', 'ADBE'
    }
    
    for symbol in symbols:
        if symbol in high_priority:
            priority_symbols.append(symbol)
        else:
            regular_symbols.append(symbol)
    
    # Create batches: priority symbols first, then alphabetical
    all_ordered = priority_symbols + sorted(regular_symbols)
    
    batches = []
    for i in range(0, len(all_ordered), batch_size):
        batch = all_ordered[i:i + batch_size]
        batches.append(batch)
    
    logger.info(f"Created {len(batches)} batches of ~{batch_size} symbols each")
    logger.info(f"Priority symbols in first batches: {len(priority_symbols)}")
    
    return batches


def execute_stock_batch(symbols, batch_num, total_batches):
    """Execute a batch of stock symbols using the FirstRate processor"""
    
    symbols_str = ",".join(symbols)
    checkpoint_file = f"stock_batch_{batch_num:04d}_checkpoint.json"
    
    logger.info(f"🚀 Starting Batch {batch_num}/{total_batches}")
    logger.info(f"   Symbols: {len(symbols)} ({symbols_str[:100]}{'...' if len(symbols_str) > 100 else ''})")
    logger.info(f"   Checkpoint: {checkpoint_file}")
    
    cmd = [
        'python3', 
        'scripts/populate_firstrate_minute_bars.py',
        '--asset-type', 'stock',
        '--symbols', symbols_str,
        '--checkpoint-file', checkpoint_file,
        '--debug'
    ]
    
    start_time = time.time()
    
    try:
        # Execute via run_dev to use Docker container
        result = subprocess.run([
            'python3', 'scripts/run_dev.py', 'run', 
            '--script', 'scripts/populate_firstrate_minute_bars.py',
            '--asset-type', 'stock',
            '--symbols', symbols_str,
            '--checkpoint-file', checkpoint_file,
            '--debug'
        ], capture_output=True, text=True, cwd=os.getcwd())
        
        processing_time = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"✅ Batch {batch_num} completed successfully in {processing_time:.1f}s")
            logger.info(f"   Average: {processing_time/len(symbols):.1f}s per symbol")
            return True, processing_time
        else:
            error_output = result.stderr or result.stdout or "Unknown error"
            logger.error(f"❌ Batch {batch_num} failed after {processing_time:.1f}s")
            logger.error(f"   Error: {error_output[:300]}")
            return False, processing_time
            
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ Exception in batch {batch_num}: {e}")
        return False, processing_time


def run_comprehensive_backfill():
    """Run the comprehensive stock backfill process"""
    
    logger.info("🚀 Starting Comprehensive FirstRate Stock Backfill")
    logger.info("⚠️  WARNING: This is a MASSIVE operation (6,827 symbols, ~227 hours)")
    logger.info("📅 Expected to run continuously for 9-10 days")
    
    # Load remaining symbols
    symbols = load_remaining_symbols()
    
    if not symbols:
        logger.error("No symbols to process!")
        return
    
    if len(symbols) > 1000:
        logger.warning(f"⚠️  Processing {len(symbols)} symbols - this will take DAYS")
        logger.warning("   Consider running smaller batches first to test the system")
        
        response = input("Continue with full backfill? [y/N]: ")
        if response.lower() != 'y':
            logger.info("Backfill cancelled by user")
            return
    
    # Create processing batches
    batches = create_prioritized_batches(symbols, batch_size=25)  # Smaller batches for stability
    
    # Processing statistics
    stats = {
        "started_at": datetime.now().isoformat(),
        "total_symbols": len(symbols),
        "total_batches": len(batches),
        "completed_batches": 0,
        "failed_batches": 0,
        "total_processing_time": 0,
        "symbols_processed": 0,
        "symbols_failed": 0
    }
    
    logger.info(f"📊 Processing Plan:")
    logger.info(f"   Total symbols: {len(symbols)}")
    logger.info(f"   Total batches: {len(batches)}")
    logger.info(f"   Batch size: ~25 symbols")
    logger.info(f"   Est. time per batch: ~50 minutes")
    logger.info(f"   Est. total time: {len(batches) * 50 / 60:.1f} hours")
    
    # Process batches sequentially
    for i, batch in enumerate(batches, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"BATCH {i}/{len(batches)} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*80}")
        
        success, processing_time = execute_stock_batch(batch, i, len(batches))
        
        # Update statistics
        stats["total_processing_time"] += processing_time
        
        if success:
            stats["completed_batches"] += 1
            stats["symbols_processed"] += len(batch)
            logger.info(f"✅ Cumulative progress: {stats['symbols_processed']}/{len(symbols)} symbols")
        else:
            stats["failed_batches"] += 1
            stats["symbols_failed"] += len(batch)
            logger.error(f"❌ Batch failed - continuing with next batch")
        
        # Save progress checkpoint
        with open("comprehensive_backfill_progress.json", "w") as f:
            json.dump(stats, f, indent=2, default=str)
        
        # Progress report every 10 batches
        if i % 10 == 0:
            elapsed_hours = stats["total_processing_time"] / 3600
            completion_pct = stats["symbols_processed"] / len(symbols) * 100
            
            logger.info(f"\n📊 PROGRESS REPORT (Batch {i}/{len(batches)}):")
            logger.info(f"   Completed: {stats['symbols_processed']}/{len(symbols)} symbols ({completion_pct:.1f}%)")
            logger.info(f"   Successful batches: {stats['completed_batches']}")
            logger.info(f"   Failed batches: {stats['failed_batches']}")
            logger.info(f"   Processing time: {elapsed_hours:.1f} hours")
            
            if stats["symbols_processed"] > 0:
                avg_time_per_symbol = stats["total_processing_time"] / stats["symbols_processed"]
                remaining_symbols = len(symbols) - stats["symbols_processed"]
                eta_seconds = remaining_symbols * avg_time_per_symbol
                eta_hours = eta_seconds / 3600
                
                logger.info(f"   Avg time per symbol: {avg_time_per_symbol:.1f}s")
                logger.info(f"   ETA for completion: {eta_hours:.1f} hours ({eta_hours/24:.1f} days)")
        
        # Brief pause between batches to allow system breathing room
        if i < len(batches):
            logger.info("⏸️  Pausing 30 seconds between batches...")
            time.sleep(30)
    
    # Final summary
    logger.info(f"\n{'='*80}")
    logger.info("🏁 COMPREHENSIVE BACKFILL COMPLETED")
    logger.info(f"{'='*80}")
    logger.info(f"✅ Successful batches: {stats['completed_batches']}/{len(batches)}")
    logger.info(f"✅ Symbols processed: {stats['symbols_processed']}/{len(symbols)}")
    logger.info(f"❌ Failed batches: {stats['failed_batches']}")
    logger.info(f"❌ Symbols failed: {stats['symbols_failed']}")
    logger.info(f"⏱️  Total processing time: {stats['total_processing_time']/3600:.1f} hours")
    
    completion_pct = stats['symbols_processed'] / len(symbols) * 100
    logger.info(f"📊 Overall success rate: {completion_pct:.1f}%")
    
    # Save final statistics
    stats["completed_at"] = datetime.now().isoformat()
    with open("comprehensive_backfill_final_stats.json", "w") as f:
        json.dump(stats, f, indent=2, default=str)
    
    logger.info("💾 Final statistics saved to comprehensive_backfill_final_stats.json")


def main():
    """Main execution function"""
    
    # Setup comprehensive logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'comprehensive_stock_backfill_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    
    logger.info("🏗️  FirstRate Comprehensive Stock Backfill Starting...")
    logger.info(f"📍 Working directory: {os.getcwd()}")
    logger.info(f"📅 Started at: {datetime.now()}")
    
    try:
        run_comprehensive_backfill()
    except KeyboardInterrupt:
        logger.info("⚠️  Backfill interrupted by user (Ctrl+C)")
        logger.info("💾 Progress saved in comprehensive_backfill_progress.json")
    except Exception as e:
        logger.error(f"💥 Fatal error in backfill: {e}")
        raise


if __name__ == "__main__":
    main()