#!/usr/bin/env python3
"""
Test FirstRate Stock Backfill
Process a small sample of high-priority symbols to validate the system

This is a TEST RUN before the comprehensive backfill:
- Process ~50 high-priority symbols
- Validate system performance and stability
- Test checkpoint and error handling
- Estimate realistic processing times
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


def get_test_symbols():
    """Get a prioritized list of test symbols for validation"""

    # High-priority symbols for testing (major stocks)
    test_symbols = [
        # Technology giants
        'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'ADBE',

        # Financial sector
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'USB', 'TFC',

        # Healthcare & pharmaceuticals
        'JNJ', 'PFE', 'ABT', 'MRK', 'BMY', 'ABBV', 'LLY', 'UNH',

        # Consumer goods
        'PG', 'KO', 'PEP', 'WMT', 'HD', 'NKE', 'MCD', 'SBUX',

        # Industrial & energy
        'XOM', 'CVX', 'COP', 'GE', 'CAT', 'BA', 'MMM', 'HON',

        # Telecom & media
        'VZ', 'T', 'TMUS', 'DIS', 'NFLX', 'CMCSA', 'VEA'
    ]

    logger.info(f"Selected {len(test_symbols)} high-priority test symbols")
    return test_symbols


def execute_test_batch(symbols, batch_name="test"):
    """Execute a test batch to validate the processing system"""

    symbols_str = ",".join(symbols)
    checkpoint_file = f"test_stock_batch_{batch_name}_checkpoint.json"

    logger.info(f"🧪 Starting Test Batch: {batch_name}")
    logger.info(f"   Symbols: {len(symbols)}")
    logger.info(f"   Symbol list: {symbols_str}")
    logger.info(f"   Checkpoint: {checkpoint_file}")

    start_time = time.time()

    try:
        # Build the command for Docker execution
        cmd = [
            'python3', 'scripts/run_dev.py', 'run',
            '--script', 'restart_firstrate_etfs_correct.py'  # Using existing working script as template
        ]

        # Create a temporary script for stock processing
        temp_script_content = f'''#!/usr/bin/env python3
import subprocess
import sys

print("🚀 Test Stock Backfill Starting...")
print("   Symbols: {symbols_str}")

cmd = [
    'python3',
    'scripts/populate_firstrate_minute_bars.py',
    '--asset-type', 'stock',
    '--symbols', '{symbols_str}',
    '--checkpoint-file', '{checkpoint_file}',
    '--debug'
]

print(f"Executing: {{' '.join(cmd)}}")
result = subprocess.run(cmd, capture_output=False, text=True)
sys.exit(result.returncode)
'''

        # Write temporary script
        temp_script = Path('temp_test_stock_batch.py')
        temp_script.write_text(temp_script_content)

        # Execute the test batch
        result = subprocess.run([
            'python3', 'scripts/run_dev.py', 'run',
            '--script', 'temp_test_stock_batch.py'
        ], capture_output=True, text=True, cwd=os.getcwd())

        processing_time = time.time() - start_time

        # Clean up temp script
        if temp_script.exists():
            temp_script.unlink()

        if result.returncode == 0:
            logger.info(f"✅ Test batch completed successfully in {processing_time:.1f}s")
            logger.info(f"   Average: {processing_time/len(symbols):.1f}s per symbol")

            # Log output for analysis
            if result.stdout:
                logger.info(f"Output: {result.stdout[-500:]}")  # Last 500 chars

            return True, processing_time
        else:
            error_output = result.stderr or result.stdout or "Unknown error"
            logger.error(f"❌ Test batch failed after {processing_time:.1f}s")
            logger.error(f"   Error: {error_output[-500:]}")  # Last 500 chars
            return False, processing_time

    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ Exception in test batch: {e}")
        return False, processing_time


def run_test_backfill():
    """Run test backfill to validate system before comprehensive run"""

    logger.info("🧪 Starting FirstRate Stock Backfill Test")
    logger.info("📋 This is a validation run before the comprehensive backfill")

    # Get test symbols
    test_symbols = get_test_symbols()

    # Split into smaller batches for testing
    batch_size = 10  # Small batches for testing
    batches = []

    for i in range(0, len(test_symbols), batch_size):
        batch = test_symbols[i:i + batch_size]
        batches.append(batch)

    logger.info(f"📦 Created {len(batches)} test batches of {batch_size} symbols each")

    # Test statistics
    stats = {
        "test_started_at": datetime.now().isoformat(),
        "total_test_symbols": len(test_symbols),
        "total_test_batches": len(batches),
        "completed_batches": 0,
        "failed_batches": 0,
        "total_processing_time": 0,
        "symbols_processed": 0,
        "symbols_failed": 0
    }

    # Process test batches
    for i, batch in enumerate(batches, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"TEST BATCH {i}/{len(batches)}")
        logger.info(f"{'='*60}")

        success, processing_time = execute_test_batch(batch, f"batch_{i:02d}")

        # Update statistics
        stats["total_processing_time"] += processing_time

        if success:
            stats["completed_batches"] += 1
            stats["symbols_processed"] += len(batch)
            logger.info(f"✅ Test progress: {stats['symbols_processed']}/{len(test_symbols)} symbols")
        else:
            stats["failed_batches"] += 1
            stats["symbols_failed"] += len(batch)
            logger.error(f"❌ Test batch failed")

        # Save test progress
        with open("test_backfill_progress.json", "w") as f:
            json.dump(stats, f, indent=2, default=str)

        # Pause between test batches
        if i < len(batches):
            logger.info("⏸️  Pausing 10 seconds between test batches...")
            time.sleep(10)

    # Test summary and recommendations
    logger.info(f"\n{'='*60}")
    logger.info("🧪 TEST BACKFILL RESULTS")
    logger.info(f"{'='*60}")

    success_rate = stats["symbols_processed"] / len(test_symbols) * 100 if test_symbols else 0
    avg_time_per_symbol = stats["total_processing_time"] / stats["symbols_processed"] if stats["symbols_processed"] > 0 else 0

    logger.info(f"✅ Test symbols processed: {stats['symbols_processed']}/{len(test_symbols)}")
    logger.info(f"✅ Success rate: {success_rate:.1f}%")
    logger.info(f"❌ Failed batches: {stats['failed_batches']}/{len(batches)}")
    logger.info(f"⏱️  Total test time: {stats['total_processing_time']:.1f}s ({stats['total_processing_time']/60:.1f} min)")
    logger.info(f"⏱️  Average per symbol: {avg_time_per_symbol:.1f}s")

    # Extrapolate to full backfill
    if stats["symbols_processed"] > 0:
        full_backfill_symbols = 6827  # From analysis
        estimated_full_time = full_backfill_symbols * avg_time_per_symbol
        estimated_hours = estimated_full_time / 3600
        estimated_days = estimated_hours / 24

        logger.info(f"\n📊 COMPREHENSIVE BACKFILL PROJECTIONS:")
        logger.info(f"   Symbols remaining: {full_backfill_symbols}")
        logger.info(f"   Est. time per symbol: {avg_time_per_symbol:.1f}s")
        logger.info(f"   Est. total time: {estimated_hours:.1f} hours ({estimated_days:.1f} days)")
        logger.info(f"   Est. processing rate: {3600/avg_time_per_symbol:.1f} symbols/hour")

        if success_rate >= 80:
            logger.info("✅ System validation PASSED - ready for comprehensive backfill")
        else:
            logger.warning("⚠️  System validation concerns - review failures before comprehensive run")

    # Save final test results
    stats["test_completed_at"] = datetime.now().isoformat()
    stats["success_rate_percent"] = success_rate
    stats["avg_time_per_symbol"] = avg_time_per_symbol

    with open("test_backfill_final_results.json", "w") as f:
        json.dump(stats, f, indent=2, default=str)

    logger.info("💾 Test results saved to test_backfill_final_results.json")

    return success_rate >= 80  # Return True if test validation passed


def main():
    """Main test execution"""

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'test_stock_backfill_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )

    logger.info("🧪 FirstRate Stock Backfill Test Starting...")
    logger.info(f"📍 Working directory: {os.getcwd()}")
    logger.info(f"📅 Started at: {datetime.now()}")

    try:
        success = run_test_backfill()

        if success:
            logger.info("\n🎉 TEST VALIDATION PASSED!")
            logger.info("   System is ready for comprehensive stock backfill")
            logger.info("   Run: python3 scripts/run_dev.py run --script start_comprehensive_stock_backfill.py")
        else:
            logger.warning("\n⚠️  TEST VALIDATION FAILED!")
            logger.warning("   Review errors before running comprehensive backfill")

    except KeyboardInterrupt:
        logger.info("⚠️  Test interrupted by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"💥 Fatal error in test: {e}")
        raise


if __name__ == "__main__":
    main()