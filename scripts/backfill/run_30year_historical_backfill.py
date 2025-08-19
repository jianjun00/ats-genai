#!/usr/bin/env python3
"""
30-Year Historical Daily Price Backfill Runner (1995-2020)

Production script for running historical daily price backfill to populate
30 years of data from 1995-2020, since we already have data from 2020 onwards.

Usage:
    # Sample run with 50 symbols
    python scripts/backfill/run_30year_historical_backfill.py --mode sample --symbols 50

    # Full S&P 500 backfill
    python scripts/backfill/run_30year_historical_backfill.py --mode sp500

    # Custom symbol list
    python scripts/backfill/run_30year_historical_backfill.py --symbols AAPL,MSFT,GOOGL,AMZN,TSLA

    # Resume from checkpoint
    python scripts/backfill/run_30year_historical_backfill.py --resume --checkpoint-file /path/to/checkpoint.json

    # High-performance mode
    python scripts/backfill/run_30year_historical_backfill.py --mode sp500 --workers 30 --batch-size 200
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
import logging
from typing import List, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import env
from market_data.eod.historical_30year_backfill import (
    run_30_year_historical_backfill,
    Historical30YearBackfill,
    HistoricalBackfillConfig
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('historical_30year_backfill.log')
    ]
)
logger = logging.getLogger(__name__)


def get_sp500_sample_symbols(count: int = 50) -> List[str]:
    """Get a sample of high-volume S&P 500 symbols."""
    symbols = [
        # Mega-cap tech
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX',
        
        # Financial services  
        'BRK.B', 'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP',
        
        # Healthcare & Pharma
        'UNH', 'JNJ', 'PFE', 'ABBV', 'TMO', 'ABT', 'DHR', 'BMY', 'MRK', 'LLY',
        
        # Energy
        'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'MPC', 'PSX', 'VLO',
        
        # Consumer & Retail
        'WMT', 'HD', 'PG', 'KO', 'PEP', 'COST', 'TGT', 'LOW', 'SBUX', 'MCD',
        
        # Industrial
        'UPS', 'HON', 'RTX', 'LMT', 'BA', 'CAT', 'DE', 'MMM', 'GE', 'FDX',
        
        # Technology (non-mega cap)
        'CRM', 'ORCL', 'ADBE', 'IBM', 'QCOM', 'TXN', 'AVGO', 'AMD', 'INTC', 'CSCO',
        
        # Utilities & REITs
        'NEE', 'DUK', 'SO', 'D', 'EXC', 'PEG', 'XEL', 'AEP',
        
        # ETFs (major ones that existed in 1995+)
        'SPY', 'QQQ', 'IWM', 'VTI', 'DIA'
    ]
    
    return symbols[:count]


def get_full_sp500_symbols() -> List[str]:
    """Get comprehensive S&P 500 symbols (representative list)."""
    # In production, this would query from a financial data provider
    # For now, using a comprehensive representative list
    return [
        # Technology (17% of S&P 500)
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'CRM', 'ORCL', 'ADBE',
        'NFLX', 'QCOM', 'TXN', 'AVGO', 'AMD', 'INTC', 'CSCO', 'IBM', 'PYPL', 'NOW',
        'INTU', 'AMAT', 'ADI', 'LRCX', 'KLAC', 'MCHP', 'CDNS', 'SNPS', 'FTNT', 'ANSS',
        
        # Financial Services (13% of S&P 500)  
        'BRK.B', 'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'USB', 'TFC',
        'PNC', 'COF', 'SCHW', 'BLK', 'SPGI', 'CME', 'ICE', 'MCO', 'AON', 'MMC',
        'AJG', 'BRO', 'CB', 'TRV', 'ALL', 'PGR', 'MET', 'PRU', 'AFL', 'HIG',
        
        # Healthcare (12% of S&P 500)
        'UNH', 'JNJ', 'PFE', 'ABBV', 'TMO', 'ABT', 'DHR', 'BMY', 'MRK', 'LLY',
        'CVS', 'MDT', 'CI', 'AMGN', 'GILD', 'ANTM', 'ISRG', 'VRTX', 'ELV', 'ZTS',
        'REGN', 'BIIB', 'IQV', 'ILMN', 'MRNA', 'DXCM', 'SYK', 'BSX', 'EW', 'BDX',
        
        # Consumer Discretionary (11% of S&P 500)
        'HD', 'TGT', 'LOW', 'SBUX', 'MCD', 'NKE', 'TJX', 'BKNG', 'DIS', 'LULU',
        'CMG', 'ORLY', 'AZO', 'BBY', 'ROST', 'YUM', 'GM', 'F', 'ABNB', 'EBAY',
        'MAR', 'HLT', 'CCL', 'NCLH', 'RCL', 'MGM', 'LVS', 'WYNN', 'CZR', 'LEN',
        
        # Communication Services (9% of S&P 500)
        'GOOGL', 'META', 'NFLX', 'DIS', 'VZ', 'T', 'TMUS', 'CHTR', 'CMCSA', 'ATVI',
        'EA', 'TTWO', 'MTCH', 'TWTR', 'SNAP', 'PINS', 'ZM', 'ROKU', 'SPOT', 'DISH',
        
        # Industrials (8% of S&P 500)
        'UPS', 'HON', 'RTX', 'LMT', 'BA', 'CAT', 'DE', 'MMM', 'GE', 'FDX',
        'WM', 'EMR', 'ETN', 'ITW', 'PH', 'CSX', 'UNP', 'NSC', 'LUV', 'DAL',
        'UAL', 'AAL', 'JBHT', 'ODFL', 'CHRW', 'EXPD', 'XPO', 'J', 'IR', 'ROK',
        
        # Consumer Staples (6% of S&P 500)
        'WMT', 'PG', 'KO', 'PEP', 'COST', 'WBA', 'CVS', 'KMB', 'CL', 'GIS',
        'K', 'HSY', 'MKC', 'SJM', 'CAG', 'CPB', 'HRL', 'TSN', 'TAP', 'STZ',
        'BF.B', 'PM', 'MO', 'BTI', 'EL', 'CLX', 'CHD', 'COTY', 'KHC', 'MNST',
        
        # Energy (4% of S&P 500)
        'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'MPC', 'PSX', 'VLO', 'OXY', 'BKR',
        'HAL', 'DVN', 'FANG', 'MRO', 'APA', 'HES', 'NOV', 'KMI', 'OKE', 'WMB',
        
        # Utilities (3% of S&P 500)
        'NEE', 'DUK', 'SO', 'D', 'EXC', 'PEG', 'XEL', 'AEP', 'SRE', 'PCG',
        'ED', 'ETR', 'ES', 'FE', 'EIX', 'PPL', 'CMS', 'DTE', 'ATO', 'WEC',
        
        # Real Estate (3% of S&P 500)
        'AMT', 'PLD', 'CCI', 'EQIX', 'PSA', 'DLR', 'SBAC', 'WY', 'AVB', 'EQR',
        'WELL', 'MAA', 'ESS', 'UDR', 'CPT', 'REG', 'FRT', 'BXP', 'VTR', 'HST',
        
        # Materials (2% of S&P 500) 
        'LIN', 'APD', 'ECL', 'SHW', 'FCX', 'NEM', 'DOW', 'PPG', 'IFF', 'EMN',
        'CF', 'LYB', 'ALB', 'IP', 'PKG', 'AMCR', 'AVY', 'SEE', 'MLM', 'VMC',
        
        # Major ETFs (for completeness)
        'SPY', 'QQQ', 'IWM', 'VTI', 'DIA', 'VEA', 'VWO', 'BND', 'AGG', 'TLT'
    ]


def parse_symbols(symbols_str: str) -> List[str]:
    """Parse comma-separated symbols string."""
    if not symbols_str:
        return []
    return [s.strip().upper() for s in symbols_str.split(',')]


def estimate_processing_time(symbol_count: int, workers: int) -> dict:
    """Estimate processing time based on symbol count and workers."""
    # Rough estimates based on 25 years of daily data
    api_calls_per_symbol = 13  # ~2 years per call for 25 years
    seconds_per_api_call = 0.1  # With rate limiting
    db_operations_per_symbol = 1  # Batch insert
    seconds_per_db_operation = 0.05
    
    total_api_time = (symbol_count * api_calls_per_symbol * seconds_per_api_call) / workers
    total_db_time = (symbol_count * db_operations_per_symbol * seconds_per_db_operation) / min(workers, 5)
    
    total_minutes = (total_api_time + total_db_time) / 60
    
    return {
        'estimated_minutes': total_minutes,
        'estimated_hours': total_minutes / 60,
        'total_records_estimated': symbol_count * 6500  # ~6500 trading days in 25 years
    }


async def validate_prerequisites():
    """Validate that all prerequisites are met."""
    # Check API keys
    polygon_key = os.getenv("POLYGON_API_KEY")
    tiingo_key = os.getenv("TIINGO_API_KEY")
    
    if not polygon_key:
        logger.error("❌ POLYGON_API_KEY environment variable is required")
        return False
    
    if not tiingo_key:
        logger.error("❌ TIINGO_API_KEY environment variable is required")
        return False
    
    # Check database connectivity
    try:
        db_url = env.get_database_url()
        logger.info(f"Testing database connection: {db_url}")
        
        import asyncpg
        conn = await asyncpg.connect(db_url)
        await conn.fetchval("SELECT 1")
        await conn.close()
        
        logger.info("✅ Database connection successful")
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False
    
    # Check Ray availability
    try:
        import ray
        logger.info("✅ Ray is available for parallel processing")
    except ImportError:
        logger.error("❌ Ray is not installed. Install with: pip install ray")
        return False
    
    logger.info("✅ All prerequisites validated")
    return True


async def run_backfill_job(args):
    """Run the 30-year historical backfill job."""
    
    # Validate prerequisites
    if not await validate_prerequisites():
        return 1
    
    # Get configuration
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    tiingo_api_key = os.getenv("TIINGO_API_KEY")
    
    # Determine symbols
    if args.resume and args.checkpoint_file:
        logger.info(f"Resuming from checkpoint: {args.checkpoint_file}")
        symbols = []  # Will be loaded from checkpoint
    elif args.symbols:
        symbols = parse_symbols(args.symbols)
        logger.info(f"Using custom symbols: {symbols}")
    elif args.mode == "sample":
        symbol_count = args.symbol_count if hasattr(args, 'symbol_count') else 50
        symbols = get_sp500_sample_symbols(symbol_count)
        logger.info(f"Using sample mode: {len(symbols)} symbols")
    elif args.mode == "sp500":
        symbols = get_full_sp500_symbols()
        logger.info(f"Using full S&P 500 mode: {len(symbols)} symbols")
    else:
        logger.error("Must specify symbols, mode, or use --resume")
        return 1
    
    # Display estimated processing time
    if symbols:
        estimates = estimate_processing_time(len(symbols), args.workers)
        logger.info(f"Estimated processing time: {estimates['estimated_hours']:.1f} hours")
        logger.info(f"Estimated records: {estimates['total_records_estimated']:,}")
    
    # Setup checkpoint file
    checkpoint_file = args.checkpoint_file
    if not checkpoint_file:
        checkpoint_dir = Path("checkpoints") / "30year_historical"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = str(checkpoint_dir / f"historical_30year_{timestamp}.json")
    
    logger.info(f"Checkpoint file: {checkpoint_file}")
    
    # Setup database configuration
    db_config = {
        'db_host': args.db_host,
        'db_port': args.db_port,
        'db_user': args.db_user,
        'db_password': args.db_password,
        'db_name': args.db_name
    }
    
    # Confirm before starting
    if not args.auto_confirm:
        print(f"\n{'='*60}")
        print("30-YEAR HISTORICAL BACKFILL CONFIGURATION")
        print(f"{'='*60}")
        print(f"Date Range: 1995-01-01 to 2020-12-31 (25 years)")
        print(f"Symbols: {len(symbols) if symbols else 'From checkpoint'}")
        print(f"Workers: {args.workers}")
        print(f"Batch Size: {args.batch_size}")
        print(f"Database: {args.db_host}:{args.db_port}/{args.db_name}")
        print(f"Checkpoint: {checkpoint_file}")
        
        if symbols:
            print(f"Est. Processing Time: {estimates['estimated_hours']:.1f} hours")
            print(f"Est. Records: {estimates['total_records_estimated']:,}")
        
        print(f"{'='*60}")
        
        confirmation = input("Proceed with backfill? (y/N): ").strip().lower()
        if confirmation not in ['y', 'yes']:
            logger.info("Backfill cancelled by user")
            return 0
    
    try:
        # Run the backfill
        logger.info("🚀 Starting 30-year historical backfill...")
        
        start_time = datetime.now()
        
        result = await run_30_year_historical_backfill(
            polygon_api_key=polygon_api_key,
            tiingo_api_key=tiingo_api_key,
            symbols=symbols,
            checkpoint_file=checkpoint_file,
            max_workers=args.workers,
            **db_config
        )
        
        end_time = datetime.now()
        
        # Report results
        logger.info("✅ 30-year historical backfill completed!")
        logger.info(f"Total duration: {end_time - start_time}")
        
        # Print comprehensive summary
        print("\n" + "="*80)
        print("30-YEAR HISTORICAL BACKFILL SUMMARY")
        print("="*80)
        
        backfill_summary = result.get('backfill_summary', {})
        print(f"Date Range: {backfill_summary.get('date_range', 'N/A')}")
        print(f"Years Processed: {backfill_summary.get('years_processed', 'N/A')}")
        print(f"Duration: {backfill_summary.get('duration_hours', 0):.2f} hours")
        
        job_stats = result.get('job_statistics', {})
        print(f"\nSymbol Processing:")
        print(f"  Total Symbols: {job_stats.get('total_symbols', 0):,}")
        print(f"  Completed: {job_stats.get('symbols_completed', 0):,}")
        print(f"  Failed: {job_stats.get('symbols_failed', 0):,}")
        print(f"  Success Rate: {job_stats.get('success_rate', 0):.1%}")
        
        data_stats = result.get('data_statistics', {})
        print(f"\nData Statistics:")
        print(f"  Total Records Fetched: {data_stats.get('total_records_fetched', 0):,}")
        print(f"  Records Stored: {data_stats.get('total_records_stored', 0):,}")
        print(f"  Storage Efficiency: {data_stats.get('storage_efficiency', 0):.1%}")
        
        vendor_stats = data_stats.get('records_by_vendor', {})
        for vendor, count in vendor_stats.items():
            print(f"    {vendor.title()}: {count:,}")
        
        perf_stats = result.get('performance_metrics', {})
        print(f"\nPerformance Metrics:")
        print(f"  Symbols per Hour: {perf_stats.get('symbols_per_hour', 0):.1f}")
        print(f"  Records per Hour: {perf_stats.get('records_per_hour', 0):,.0f}")
        print(f"  Avg Records per Symbol: {perf_stats.get('avg_records_per_symbol', 0):,.0f}")
        
        error_stats = result.get('error_analysis', {})
        print(f"\nError Analysis:")
        print(f"  Failed Symbols: {len(error_stats.get('failed_symbols', []))}")
        print(f"  Error Rate: {error_stats.get('error_rate', 0):.1%}")
        
        if error_stats.get('failed_symbols'):
            print(f"  Failed Symbol Examples: {', '.join(error_stats['failed_symbols'][:5])}")
        
        print("\n" + "="*80)
        
        # Save detailed results
        results_dir = Path("results") / "30year_historical"
        results_dir.mkdir(parents=True, exist_ok=True)
        results_file = results_dir / f"backfill_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        logger.info(f"Detailed results saved to: {results_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ 30-year backfill failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="30-Year Historical Daily Price Backfill (1995-2020)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sample backfill (50 symbols)
  python scripts/backfill/run_30year_historical_backfill.py --mode sample

  # Sample with specific count
  python scripts/backfill/run_30year_historical_backfill.py --mode sample --symbol-count 100

  # Full S&P 500 backfill
  python scripts/backfill/run_30year_historical_backfill.py --mode sp500

  # Custom symbols
  python scripts/backfill/run_30year_historical_backfill.py --symbols AAPL,MSFT,GOOGL,AMZN,TSLA

  # Resume from checkpoint
  python scripts/backfill/run_30year_historical_backfill.py --resume --checkpoint-file checkpoints/30year_historical/historical_30year_20241218_140000.json

  # High-performance configuration
  python scripts/backfill/run_30year_historical_backfill.py --mode sp500 --workers 30 --batch-size 200 --auto-confirm
        """
    )
    
    # Backfill mode
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--mode", 
        choices=["sample", "sp500"],
        help="Backfill mode: 'sample' for limited symbols, 'sp500' for full S&P 500"
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
    
    # Symbol configuration
    parser.add_argument(
        "--symbol-count",
        type=int,
        default=50,
        help="Number of symbols for sample mode (default: 50)"
    )
    
    # Performance tuning
    parser.add_argument(
        "--workers",
        type=int,
        default=20,
        help="Maximum Ray workers (default: 20)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Symbols per batch (default: 100)"
    )
    
    # Database configuration
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5433, help="Database port")
    parser.add_argument("--db-user", default="postgres", help="Database user")
    parser.add_argument("--db-password", default="postgres", help="Database password")
    parser.add_argument("--db-name", default="dev_db", help="Database name")
    
    # Checkpoint configuration
    parser.add_argument(
        "--checkpoint-file",
        help="Specific checkpoint file path (auto-generated if not specified)"
    )
    
    # Control options
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        help="Skip confirmation prompt"
    )
    
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
        print(f"Workers: {args.workers}")
        print(f"Batch size: {args.batch_size}")
        print(f"Database: {args.db_host}:{args.db_port}/{args.db_name}")
        print(f"Checkpoint file: {args.checkpoint_file}")
        
        # Validate API keys
        polygon_key = os.getenv("POLYGON_API_KEY")
        tiingo_key = os.getenv("TIINGO_API_KEY")
        
        if polygon_key and tiingo_key:
            print("✅ API keys found")
        else:
            print("❌ Missing API keys")
        
        print("✅ Dry run completed")
        return 0
    
    # Run the backfill
    try:
        exit_code = asyncio.run(run_backfill_job(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("🛑 30-year backfill interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()