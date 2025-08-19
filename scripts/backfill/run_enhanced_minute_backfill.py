#!/usr/bin/env python3
"""
Enhanced Minute Backfill Runner

Production script for running minute-level backfills with checkpoint support
for both Polygon and Tiingo data sources.

Usage:
    python scripts/backfill/run_enhanced_minute_backfill.py --mode sample --days 30
    python scripts/backfill/run_enhanced_minute_backfill.py --mode full --symbols AAPL,MSFT,GOOGL --days 365
    python scripts/backfill/run_enhanced_minute_backfill.py --resume --checkpoint-file /path/to/checkpoint.json
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import List, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import env
from market_data.backfill.enhanced_minute_backfill_orchestrator import (
    run_enhanced_minute_backfill,
    EnhancedMinuteBackfillOrchestrator,
    EnhancedBackfillConfig
)
import asyncpg


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('enhanced_minute_backfill.log')
    ]
)
logger = logging.getLogger(__name__)


def get_sample_symbols() -> List[str]:
    """Get a sample list of high-volume symbols for testing."""
    return [
        # Major ETFs
        'SPY', 'QQQ', 'IWM', 'VTI',
        
        # Tech giants
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA',
        
        # More tech
        'META', 'NFLX', 'CRM', 'ADBE',
        
        # Banks
        'JPM', 'BAC', 'WFC',
        
        # Consumer
        'JNJ', 'PG', 'WMT'
    ]


def get_sp500_symbols() -> List[str]:
    """Get S&P 500 symbols (simplified list for demo)."""
    # In production, this would fetch from a database or API
    return [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'BRK.B',
        'UNH', 'JNJ', 'XOM', 'JPM', 'PG', 'V', 'HD', 'CVX', 'MA', 'PFE',
        'ABBV', 'BAC', 'LLY', 'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'WMT',
        'DIS', 'ABT', 'CRM', 'VZ', 'ADBE', 'DHR', 'ACN', 'MRK', 'TXN',
        'NKE', 'NFLX', 'QCOM', 'WFC', 'RTX', 'NEE', 'AMD', 'UPS', 'T',
        'PM', 'SBUX', 'LOW', 'LIN', 'SPGI', 'HON', 'UNP', 'IBM', 'GS'
    ]


def parse_symbols(symbols_str: str) -> List[str]:
    """Parse comma-separated symbols string."""
    if not symbols_str:
        return []
    return [s.strip().upper() for s in symbols_str.split(',')]


async def validate_database_connection(db_url: str) -> bool:
    """Validate database connection before starting backfill."""
    try:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        await pool.close()
        logger.info("✅ Database connection validated")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False


def validate_api_keys(polygon_key: Optional[str], tiingo_key: Optional[str]) -> bool:
    """Validate that required API keys are present."""
    if not polygon_key:
        logger.error("❌ POLYGON_API_KEY is required")
        return False
    
    if not tiingo_key:
        logger.error("❌ TIINGO_API_KEY is required")
        return False
    
    logger.info("✅ API keys validated")
    return True


def calculate_storage_requirements(symbols: List[str], days: int) -> dict:
    """Calculate estimated storage requirements."""
    # Rough estimates for minute data
    bars_per_symbol_per_day = 390  # 6.5 hours * 60 minutes
    bytes_per_bar = 100  # Rough estimate including metadata
    
    total_bars = len(symbols) * days * bars_per_symbol_per_day
    total_bytes = total_bars * bytes_per_bar
    
    return {
        'total_bars': total_bars,
        'total_mb': total_bytes / (1024 * 1024),
        'total_gb': total_bytes / (1024 * 1024 * 1024)
    }


async def run_backfill_job(args):
    """Run the enhanced minute backfill job."""
    
    # Get configuration
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    tiingo_api_key = os.getenv("TIINGO_API_KEY")
    
    # Validate API keys
    if not validate_api_keys(polygon_api_key, tiingo_api_key):
        return 1
    
    # Get database URL
    db_url = env.get_database_url()
    logger.info(f"Database URL: {db_url}")
    
    # Validate database connection
    if not await validate_database_connection(db_url):
        return 1
    
    # Determine symbols
    if args.resume and args.checkpoint_file:
        logger.info(f"Resuming from checkpoint: {args.checkpoint_file}")
        symbols = []  # Will be loaded from checkpoint
    elif args.symbols:
        symbols = parse_symbols(args.symbols)
        logger.info(f"Using custom symbols: {symbols}")
    elif args.mode == "sample":
        symbols = get_sample_symbols()
        logger.info(f"Using sample symbols: {len(symbols)} symbols")
    elif args.mode == "sp500":
        symbols = get_sp500_symbols()
        logger.info(f"Using S&P 500 symbols: {len(symbols)} symbols")
    else:
        logger.error("Must specify symbols, mode, or use --resume")
        return 1
    
    # Calculate date range
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=args.days)
    
    logger.info(f"Date range: {start_date.date()} to {end_date.date()} ({args.days} days)")
    
    # Calculate storage requirements
    if symbols:
        storage_req = calculate_storage_requirements(symbols, args.days)
        logger.info(f"Estimated storage: {storage_req['total_gb']:.2f} GB "
                   f"({storage_req['total_bars']:,} bars)")
    
    # Setup checkpoint file
    checkpoint_file = args.checkpoint_file
    if not checkpoint_file:
        checkpoint_dir = Path(args.storage_path) / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = str(checkpoint_dir / f"enhanced_minute_backfill_{timestamp}.json")
    
    logger.info(f"Checkpoint file: {checkpoint_file}")
    
    # Configure parallel processing
    max_workers = args.max_workers
    if args.mode == "sample":
        max_workers = min(max_workers, 8)  # Conservative for sample
    
    logger.info(f"Parallel configuration: {max_workers} max workers")
    
    try:
        # Run the backfill
        logger.info("🚀 Starting enhanced minute backfill...")
        
        start_time = datetime.now()
        
        result = await run_enhanced_minute_backfill(
            db_url=db_url,
            symbols=symbols,
            polygon_api_key=polygon_api_key,
            tiingo_api_key=tiingo_api_key,
            start_date=start_date,
            end_date=end_date,
            storage_path=args.storage_path,
            checkpoint_file=checkpoint_file,
            max_workers=max_workers
        )
        
        end_time = datetime.now()
        
        # Report results
        logger.info("✅ Enhanced minute backfill completed!")
        logger.info(f"Total duration: {end_time - start_time}")
        
        # Print summary statistics
        print("\n" + "="*80)
        print("ENHANCED MINUTE BACKFILL SUMMARY")
        print("="*80)
        
        job_summary = result.get('job_summary', {})
        print(f"Job ID: {job_summary.get('job_id', 'N/A')}")
        print(f"Status: {job_summary.get('status', 'N/A')}")
        print(f"Duration: {job_summary.get('duration_hours', 0):.2f} hours")
        
        segment_stats = result.get('segment_statistics', {})
        print(f"\nSegment Statistics:")
        print(f"  Total Segments: {segment_stats.get('total_segments', 0):,}")
        print(f"  Completed: {segment_stats.get('segments_completed', 0):,}")
        print(f"  Failed: {segment_stats.get('segments_failed', 0):,}")
        print(f"  Success Rate: {segment_stats.get('success_rate', 0):.1%}")
        
        data_stats = result.get('data_statistics', {})
        print(f"\nData Statistics:")
        print(f"  Total Bars Fetched: {data_stats.get('total_bars_fetched', 0):,}")
        print(f"  Total Bars Reconciled: {data_stats.get('total_bars_reconciled', 0):,}")
        print(f"  Total Bars Stored: {data_stats.get('total_bars_stored', 0):,}")
        print(f"  Reconciliation Rate: {data_stats.get('reconciliation_rate', 0):.1%}")
        
        perf_stats = result.get('performance_metrics', {})
        print(f"\nPerformance Metrics:")
        print(f"  Bars per Hour: {perf_stats.get('bars_per_hour', 0):,.0f}")
        print(f"  Segments per Hour: {perf_stats.get('segments_per_hour', 0):.1f}")
        print(f"  Avg Time per Segment: {perf_stats.get('avg_processing_time_per_segment', 0):.2f}s")
        
        error_stats = result.get('error_analysis', {})
        print(f"\nError Analysis:")
        print(f"  Total Errors: {error_stats.get('total_errors', 0)}")
        print(f"  Failed Segments: {len(error_stats.get('failed_segments', []))}")
        print(f"  Error Rate: {error_stats.get('error_rate_per_segment', 0):.1%}")
        
        print("\n" + "="*80)
        
        # Save detailed results
        results_file = Path(args.storage_path) / "results" / f"backfill_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        results_file.parent.mkdir(parents=True, exist_ok=True)
        
        import json
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        logger.info(f"Detailed results saved to: {results_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enhanced Minute Backfill with Checkpoint Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sample backfill (20 symbols, 30 days)
  python scripts/backfill/run_enhanced_minute_backfill.py --mode sample --days 30

  # Custom symbols backfill
  python scripts/backfill/run_enhanced_minute_backfill.py --symbols AAPL,MSFT,GOOGL --days 365

  # Full S&P 500 backfill
  python scripts/backfill/run_enhanced_minute_backfill.py --mode sp500 --days 1825 --max-workers 20

  # Resume from checkpoint
  python scripts/backfill/run_enhanced_minute_backfill.py --resume --checkpoint-file /path/to/checkpoint.json

  # High-performance configuration
  python scripts/backfill/run_enhanced_minute_backfill.py --mode sp500 --days 365 --max-workers 30 --storage-path /fast/storage
        """
    )
    
    # Backfill mode
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--mode", 
        choices=["sample", "sp500"],
        help="Backfill mode: 'sample' for 20 high-volume symbols, 'sp500' for S&P 500"
    )
    mode_group.add_argument(
        "--symbols",
        help="Comma-separated list of symbols (e.g., AAPL,MSFT,GOOGL)"
    )
    mode_group.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing checkpoint"
    )
    
    # Time range
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)"
    )
    
    # Performance tuning
    parser.add_argument(
        "--max-workers",
        type=int,
        default=15,
        help="Maximum concurrent workers (default: 15)"
    )
    
    # Storage configuration
    parser.add_argument(
        "--storage-path",
        default="/home/jianjun/ats/data/STK/1min",
        help="Base storage path (default: /home/jianjun/ats/data/STK/1min)"
    )
    
    # Checkpoint configuration
    parser.add_argument(
        "--checkpoint-file",
        help="Specific checkpoint file path (auto-generated if not specified)"
    )
    
    # Debugging
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without running backfill"
    )
    
    args = parser.parse_args()
    
    # Configure debug logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Validate arguments
    if not args.resume and not args.mode and not args.symbols:
        parser.error("Must specify --mode, --symbols, or --resume")
    
    if args.resume and not args.checkpoint_file:
        parser.error("--resume requires --checkpoint-file")
    
    # Dry run validation
    if args.dry_run:
        print("DRY RUN - Configuration validation only")
        print(f"Mode: {args.mode}")
        print(f"Symbols: {args.symbols}")
        print(f"Days: {args.days}")
        print(f"Max workers: {args.max_workers}")
        print(f"Storage path: {args.storage_path}")
        print(f"Checkpoint file: {args.checkpoint_file}")
        
        # Validate API keys
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        tiingo_api_key = os.getenv("TIINGO_API_KEY")
        validate_api_keys(polygon_api_key, tiingo_api_key)
        
        print("✅ Dry run completed - configuration looks good")
        return 0
    
    # Run the backfill
    try:
        exit_code = asyncio.run(run_backfill_job(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("🛑 Backfill interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()