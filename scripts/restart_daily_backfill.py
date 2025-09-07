#!/usr/bin/env python3
"""
Restart Multi-Vendor Daily Price Backfill from Checkpoint

This script restarts the multi-vendor daily price backfill process from
the existing checkpoint file, allowing it to resume where it left off.

Usage:
    PYTHONPATH=src python3 scripts/restart_daily_backfill.py
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import asyncio
import logging

# Import the backfiller
from data_ingestion.legacy_backfill_scripts.multi_vendor_30year_daily_backfill import MultiVendorDailyBackfiller
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("restart_daily_backfill")

async def main():
    """Restart the multi-vendor daily price backfill from checkpoint."""

    logger.info("🚀 Restarting multi-vendor daily price backfill from checkpoint...")

    # API Keys (use environment variables or placeholders)
    polygon_api_key = os.getenv('POLYGON_API_KEY')
    tiingo_api_key = os.getenv('TIINGO_API_KEY')
    eodhd_api_key = os.getenv('EODHD_API_KEY')

    # Checkpoint file path (Docker container path)
    checkpoint_file = "/data/checkpoints/multi_vendor_daily_backfill_30year.json"

    if not Path(checkpoint_file).exists():
        logger.error(f"❌ Checkpoint file not found: {checkpoint_file}")
        return 1

    logger.info(f"📋 Using checkpoint file: {checkpoint_file}")

    # Date range (30 years)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * 30)

    try:
        # Initialize backfiller
        backfiller = MultiVendorDailyBackfiller(
            polygon_api_key=polygon_api_key,
            tiingo_api_key=tiingo_api_key,
            eodhd_api_key=eodhd_api_key,
            checkpoint_file=checkpoint_file
        )

        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info("🔄 Resuming from existing checkpoint...")

        # Run backfill with larger batch for production
        results = await backfiller.run_backfill(
            start_date=start_date,
            end_date=end_date,
            limit=2000  # Process next 2000 instruments
        )

        logger.info("📊 Final Results:")
        logger.info(f"  Total instruments: {results['total_instruments']:,}")
        logger.info(f"  Processed: {results['processed_instruments']:,}")
        logger.info(f"  Completed: {results['completed_instruments']:,}")
        logger.info(f"  Failed: {results['failed_instruments']:,}")
        logger.info(f"  Polygon records: {results['total_records_polygon']:,}")
        logger.info(f"  Tiingo records: {results['total_records_tiingo']:,}")
        logger.info(f"  EODHD records: {results['total_records_eodhd']:,}")

        logger.info("✅ Multi-vendor daily price backfill batch completed successfully!")
        return 0

    except KeyboardInterrupt:
        logger.info("⏸️ Backfill interrupted by user - progress saved to checkpoint")
        return 130

    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)