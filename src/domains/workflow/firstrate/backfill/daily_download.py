#!/usr/bin/env python3
"""
FirstRate Daily Download Job

Automated daily job to download 1-minute bar data from FirstRate API.
Designed to run daily via cron or systemd timer.

Usage:
    python scripts/firstrate_daily_download.py --all
    python scripts/firstrate_daily_download.py --asset-types stock etf
    python scripts/firstrate_daily_download.py --date 2024-08-29
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from domains.market_data.services.core.agent.core.firstrate_daily_downloader import FirstRateDownloader, DownloadJob
from core.shared.run_aware_logging import setup_run_aware_logging


async def run_daily_download(
    asset_types: list = None,
    download_date: date = None,
    cleanup_days: int = 7,
    debug: bool = False
) -> bool:
    """Run the daily download job."""

    # Default asset types
    if asset_types is None:
        asset_types = ["stock", "etf", "fx"]

    # Setup logging
    log_level = "DEBUG" if debug else "INFO"
    setup_run_aware_logging(log_level=log_level)

    logger = logging.getLogger(__name__)

    # Log job start
    run_date = download_date or date.today()
    logger.info(f"🚀 Starting FirstRate daily download job for {run_date}")
    logger.info(f"📊 Asset types: {asset_types}")

    # Create download jobs
    jobs = [DownloadJob(asset_type=asset_type) for asset_type in asset_types]

    # Create downloader
    downloader = FirstRateDownloader()

    # Run download
    results = await downloader.download_daily_data(jobs, download_date)

    # Cleanup old files
    total_cleaned = 0
    if cleanup_days > 0:
        logger.info(f"🧹 Cleaning up files older than {cleanup_days} days")
        for asset_type in asset_types:
            cleaned = downloader.cleanup_old_files(asset_type, cleanup_days)
            total_cleaned += cleaned

    # Summary
    successful = sum(1 for success in results.values() if success)
    total = len(results)

    if successful == total:
        logger.info(f"✅ Daily download completed successfully: {successful}/{total} asset types")
        logger.info(f"📁 Files stored in: /mnt/d/ats-data/firstrate-data/daily/")
        if total_cleaned > 0:
            logger.info(f"🧹 Cleaned up {total_cleaned} old files")
        return True
    else:
        logger.error(f"❌ Daily download partially failed: {successful}/{total} successful")
        logger.error(f"Failed asset types: {[k for k, v in results.items() if not v]}")
        return False

def main():
    """Main function with CLI argument parsing."""
    import argparse

    parser = argparse.ArgumentParser(
        description="FirstRate Daily Download Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all asset types for today
  python scripts/firstrate_daily_download.py --all

  # Download specific asset types
  python scripts/firstrate_daily_download.py --asset-types stock etf

  # Download for specific date
  python scripts/firstrate_daily_download.py --date 2024-08-29

  # Download with debug logging
  python scripts/firstrate_daily_download.py --all --debug

  # Download without cleanup
  python scripts/firstrate_daily_download.py --all --no-cleanup
        """
    )

    # Asset type options
    asset_group = parser.add_mutually_exclusive_group(required=True)
    asset_group.add_argument("--all", action="store_true",
                           help="Download all asset types (stock, etf, fx)")
    asset_group.add_argument("--asset-types", nargs="+",
                           choices=["stock", "etf", "fx"],
                           help="Specific asset types to download")

    # Other options
    parser.add_argument("--date", help="Download date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--cleanup-days", type=int, default=7,
                       help="Keep this many days of files, delete older ones (default: 7)")
    parser.add_argument("--no-cleanup", action="store_true",
                       help="Skip cleanup of old files")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Determine asset types
    if args.all:
        asset_types = ["stock", "etf", "fx"]
    else:
        asset_types = args.asset_types

    # Parse date if provided
    download_date = None
    if args.date:
        download_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    cleanup_days = 0 if args.no_cleanup else args.cleanup_days

    # Run the job
    success = asyncio.run(run_daily_download(
        asset_types=asset_types,
        download_date=download_date,
        cleanup_days=cleanup_days,
        debug=args.debug
    ))

    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)