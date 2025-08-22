#!/usr/bin/env python3
"""
5-Year Unified Multi-Vendor Backfill Execution Script

Orchestrates the complete 5-year backfill of 1-minute data from both Polygon 
and Tiingo with cross-vendor reconciliation to reduce data errors.

Usage:
    python scripts/backfill/run_unified_5year_backfill.py --symbols AAPL,MSFT,GOOGL
    python scripts/backfill/run_unified_5year_backfill.py --portfolio spy500 --limit 50
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Optional
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from market_data.backfill.unified_backfill_orchestrator import (
    UnifiedBackfillOrchestrator,
    BackfillConfig,
    run_5_year_backfill
)
from storage.hybrid_minute_data_manager import StorageConfig
from config.environment import env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/unified_backfill.log')
    ]
)
logger = logging.getLogger(__name__)


def get_sp500_symbols(limit: Optional[int] = None) -> List[str]:
    """Get S&P 500 symbols for backfill."""
    # Major S&P 500 symbols for testing
    symbols = [
        # Tech Giants
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'NVDA', 'META',
        'NFLX', 'ADBE', 'CRM', 'ORCL', 'AVGO', 'CSCO', 'INTC', 'AMD',
        
        # Financial Services
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'BLK', 'SCHW',
        
        # Healthcare & Pharma
        'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK', 'TMO', 'ABT', 'CVS', 'DHR',
        
        # Consumer & Retail
        'WMT', 'HD', 'PG', 'KO', 'PEP', 'MCD', 'NKE', 'SBUX', 'TGT',
        
        # Industrial & Energy
        'BA', 'CAT', 'GE', 'MMM', 'HON', 'UPS', 'RTX', 'LMT', 'XOM', 'CVX',
        
        # Communication & Media
        'VZ', 'T', 'CMCSA', 'DIS', 'NFLX',
        
        # ETFs for broad market coverage
        'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'EFA', 'EEM'
    ]
    
    if limit:
        return symbols[:limit]
    return symbols


def get_database_url() -> str:
    """Get database connection URL from environment."""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5433')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def validate_api_keys() -> tuple[str, str]:
    """Validate and return API keys."""
    polygon_key = os.getenv('POLYGON_API_KEY')
    tiingo_key = os.getenv('TIINGO_API_KEY')
    
    if not polygon_key:
        raise ValueError("POLYGON_API_KEY environment variable is required")
    
    if not tiingo_key:
        raise ValueError("TIINGO_API_KEY environment variable is required")
    
    return polygon_key, tiingo_key


async def run_backfill_with_config(
    symbols: List[str],
    config_overrides: dict = None
) -> dict:
    """Run backfill with custom configuration."""
    
    # Validate environment
    polygon_key, tiingo_key = validate_api_keys()
    db_url = get_database_url()
    
    logger.info(f"Starting unified 5-year backfill for {len(symbols)} symbols")
    logger.info(f"Database: {db_url.split('@')[1]}")  # Hide credentials
    logger.info(f"Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")
    
    # Create configuration
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5 * 365)  # 5 years
    
    # Storage configuration
    storage_config = StorageConfig(
        base_data_path="/home/jianjun/ats/data/STK/1min",
        hot_data_days=30,      # 1 month in database
        warm_data_days=90,     # 3 months on disk uncompressed  
        cold_data_days=1825,   # 5 years compressed
        batch_size=5000,       # Larger batches for backfill
        max_concurrent_files=6
    )
    
    # Backfill configuration
    backfill_config = BackfillConfig(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        polygon_api_key=polygon_key,
        tiingo_api_key=tiingo_key,
        storage_base_path=storage_config.base_data_path,
        batch_size=10,                    # Symbols per batch
        chunk_size_days=7,                # Weekly chunks
        max_concurrent_symbols=3,         # Conservative concurrency
        max_retries=5,                    # High retry count
        retry_delay_seconds=300,          # 5 minute retry delay
        continue_on_error=True,           # Don't stop on symbol errors
        checkpoint_file=f"{storage_config.base_data_path}/unified_backfill_checkpoint.json"
    )
    
    # Apply any config overrides
    if config_overrides:
        for key, value in config_overrides.items():
            if hasattr(backfill_config, key):
                setattr(backfill_config, key, value)
                logger.info(f"Config override: {key} = {value}")
    
    # Run the backfill
    try:
        result = await run_5_year_backfill(
            db_url=db_url,
            symbols=symbols,
            polygon_api_key=polygon_key,
            tiingo_api_key=tiingo_key,
            storage_path=storage_config.base_data_path
        )
        
        logger.info("Backfill completed successfully!")
        
        # Log summary statistics
        if result:
            execution_summary = result.get('execution_summary', {})
            data_summary = result.get('data_summary', {})
            
            logger.info(f"Execution time: {execution_summary.get('duration_hours', 0):.2f} hours")
            logger.info(f"Symbols completed: {execution_summary.get('symbols_completed', 0)}")
            logger.info(f"Total bars reconciled: {data_summary.get('total_bars_reconciled', 0):,}")
            logger.info(f"Total bars stored: {data_summary.get('total_bars_stored', 0):,}")
        
        return result
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        raise


async def run_sample_backfill():
    """Run a small sample backfill for testing."""
    logger.info("Running sample backfill (5 symbols, 30 days)")
    
    # Small test set
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'SPY', 'QQQ']
    
    # Override config for testing
    config_overrides = {
        'start_date': datetime.now() - timedelta(days=30),
        'chunk_size_days': 7,
        'batch_size': 2,
        'max_concurrent_symbols': 2
    }
    
    return await run_backfill_with_config(symbols, config_overrides)


async def run_full_sp500_backfill(limit: Optional[int] = None):
    """Run full S&P 500 backfill."""
    symbols = get_sp500_symbols(limit)
    logger.info(f"Running full S&P 500 backfill ({len(symbols)} symbols, 5 years)")
    
    return await run_backfill_with_config(symbols)


async def resume_backfill():
    """Resume a previously interrupted backfill."""
    checkpoint_file = "/home/jianjun/ats/data/STK/1min/unified_backfill_checkpoint.json"
    
    if not Path(checkpoint_file).exists():
        logger.error(f"No checkpoint file found at {checkpoint_file}")
        return None
    
    logger.info(f"Resuming backfill from checkpoint: {checkpoint_file}")
    
    # Load checkpoint to see progress
    with open(checkpoint_file, 'r') as f:
        checkpoint = json.load(f)
    
    logger.info(f"Previous progress: {len(checkpoint.get('symbols_completed', []))} symbols completed")
    logger.info(f"Bars processed: {checkpoint.get('bars_processed', 0):,}")
    
    # Get original symbol list (would need to be saved in checkpoint or re-specified)
    symbols = get_sp500_symbols()  # Default to full list
    
    return await run_backfill_with_config(symbols)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='5-Year Unified Multi-Vendor Backfill')
    
    parser.add_argument(
        '--mode', 
        choices=['sample', 'full', 'custom', 'resume'], 
        default='sample',
        help='Backfill mode'
    )
    
    parser.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated list of symbols (for custom mode)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of symbols (for full mode)'
    )
    
    parser.add_argument(
        '--chunk-days',
        type=int,
        default=7,
        help='Days per processing chunk'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Symbols per batch'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=3,
        help='Maximum concurrent symbol processing'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test configuration without running backfill'
    )
    
    args = parser.parse_args()
    
    # Validate environment before starting
    try:
        validate_api_keys()
        db_url = get_database_url()
        logger.info(f"Environment validated successfully")
        logger.info(f"Database: {db_url.split('@')[1]}")
    except Exception as e:
        logger.error(f"Environment validation failed: {e}")
        sys.exit(1)
    
    if args.dry_run:
        logger.info("Dry run completed - configuration is valid")
        return
    
    # Configure event loop
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run backfill based on mode
    try:
        if args.mode == 'sample':
            result = asyncio.run(run_sample_backfill())
            
        elif args.mode == 'full':
            result = asyncio.run(run_full_sp500_backfill(args.limit))
            
        elif args.mode == 'custom':
            if not args.symbols:
                logger.error("--symbols required for custom mode")
                sys.exit(1)
            
            symbols = [s.strip().upper() for s in args.symbols.split(',')]
            config_overrides = {
                'chunk_size_days': args.chunk_days,
                'batch_size': args.batch_size,
                'max_concurrent_symbols': args.max_concurrent
            }
            result = asyncio.run(run_backfill_with_config(symbols, config_overrides))
            
        elif args.mode == 'resume':
            result = asyncio.run(resume_backfill())
            
        else:
            logger.error(f"Unknown mode: {args.mode}")
            sys.exit(1)
        
        # Save final results
        if result:
            results_file = f"/tmp/backfill_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(results_file, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Results saved to: {results_file}")
        
        logger.info("Backfill execution completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Backfill interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Backfill execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()