#!/usr/bin/env python3
"""
Ray-Parallelized Feature Extraction Runner
Distributes feature extraction across symbols using Ray framework for faster processing
"""

import ray
import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import traceback

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

logger = logging.getLogger(__name__)

@ray.remote
def extract_features_for_symbol(symbol: str, start_date: str, end_date: str, 
                               environment: str, gin_config: str, debug: bool = False) -> Dict[str, Any]:
    """Ray remote function to extract features for a single symbol"""
    try:
        # Set up logging for this worker
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
            
        logger.info(f"🚀 Starting feature extraction for {symbol} ({start_date} to {end_date})")
        
        # Import the main function from feature extraction runner
        from domains.services.training_data.feature_extraction_runner import main as feature_main
        
        # Simulate command line arguments for this symbol
        import sys
        original_argv = sys.argv.copy()
        
        try:
            sys.argv = [
                'feature_extraction_runner.py',
                '--symbols', symbol,
                '--start-date', start_date,
                '--end-date', end_date,
                '--environment', environment,
                '--gin-config', gin_config
            ]
            
            if debug:
                sys.argv.append('--debug')
            
            # Run feature extraction
            result = asyncio.run(feature_main())
            
            logger.info(f"✅ Completed feature extraction for {symbol}")
            
            return {
                'symbol': symbol,
                'status': 'success',
                'message': f"Feature extraction completed for {symbol}"
            }
            
        finally:
            # Restore original argv
            sys.argv = original_argv
            
    except Exception as e:
        error_msg = f"❌ Feature extraction failed for {symbol}: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        return {
            'symbol': symbol,
            'status': 'failed',
            'error': error_msg,
            'traceback': traceback.format_exc()
        }

def setup_ray(num_cpus: int = None):
    """Initialize Ray cluster"""
    try:
        # Check if Ray is already initialized
        if ray.is_initialized():
            logger.info("✅ Ray already initialized")
            return
            
        # Initialize Ray
        if num_cpus:
            ray.init(num_cpus=num_cpus)
        else:
            ray.init()
            
        logger.info(f"✅ Ray initialized with {ray.available_resources()} resources")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize Ray: {e}")
        raise

def run_parallel_feature_extraction(
    symbols: List[str],
    start_date: str,
    end_date: str,
    environment: str,
    gin_config: str,
    debug: bool = False,
    num_cpus: int = None
) -> List[Dict[str, Any]]:
    """Run feature extraction in parallel across symbols"""
    
    # Setup Ray
    setup_ray(num_cpus)
    
    logger.info(f"🔍 Starting parallel feature extraction:")
    logger.info(f"   Symbols: {symbols}")
    logger.info(f"   Date range: {start_date} to {end_date}")
    logger.info(f"   Environment: {environment}")
    logger.info(f"   Workers: {len(symbols)} (one per symbol)")
    
    try:
        # Start feature extraction tasks (one per symbol)
        futures = []
        for i, symbol in enumerate(symbols):
            future = extract_features_for_symbol.remote(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                environment=environment,
                gin_config=gin_config,
                debug=debug
            )
            futures.append(future)
            logger.info(f"🚀 Started extraction task {i+1}/{len(symbols)} for {symbol}")
        
        # Wait for all tasks to complete
        logger.info(f"⏳ Waiting for {len(futures)} tasks to complete...")
        results = ray.get(futures)
        
        # Report results
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        
        logger.info(f"\n📊 Parallel Feature Extraction Summary:")
        logger.info(f"   ✅ Successful: {len(successful)}/{len(symbols)} symbols")
        logger.info(f"   ❌ Failed: {len(failed)}/{len(symbols)} symbols")
        
        for result in successful:
            logger.info(f"   ✅ {result['symbol']}: {result.get('message', 'Success')}")
        
        for result in failed:
            logger.error(f"   ❌ {result['symbol']}: {result.get('error', 'Unknown error')}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Parallel feature extraction failed: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        # Shutdown Ray
        ray.shutdown()
        logger.info("✅ Ray shutdown complete")

def main():
    """Main entry point with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Ray-parallelized feature extraction across symbols",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract features for multiple symbols in parallel
  python3 scripts/feature_extraction_ray.py \\
    --symbols AAPL MSFT GOOGL NVDA TSLA \\
    --start-date 2024-01-01 \\
    --end-date 2025-11-14 \\
    --environment intg \\
    --gin-config config/feature_extraction.gin \\
    --debug
    
  # Limit number of parallel workers
  python3 scripts/feature_extraction_ray.py \\
    --symbols AAPL MSFT TSLA \\
    --start-date 2024-01-01 \\
    --end-date 2024-12-31 \\
    --environment intg \\
    --gin-config config/feature_extraction.gin \\
    --num-cpus 3
        """
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        required=True,
        help='Stock symbols to extract features for (e.g., AAPL MSFT GOOGL)'
    )
    
    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date for feature extraction (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--end-date',
        required=True,
        help='End date for feature extraction (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--environment',
        required=True,
        choices=['dev', 'test', 'intg', 'prod'],
        help='Environment configuration to use'
    )
    
    parser.add_argument(
        '--gin-config',
        required=True,
        help='Gin configuration file path'
    )
    
    parser.add_argument(
        '--num-cpus',
        type=int,
        help='Number of CPU cores to use for Ray (default: auto-detect)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        return 1
    
    if not os.path.exists(args.gin_config):
        print(f"❌ Gin config file not found: {args.gin_config}")
        return 1
    
    # Set up logging
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Set environment variables for database connection
    if args.environment == 'intg':
        os.environ['DATABASE_URL'] = 'postgresql://postgres:intg_password@localhost:4432/intg_db'
    elif args.environment == 'dev':
        os.environ['DATABASE_URL'] = 'postgresql://postgres:dev_password@localhost:3432/dev_db'
    
    os.environ['PYTHONPATH'] = 'src'
    
    try:
        # Run parallel feature extraction
        results = run_parallel_feature_extraction(
            symbols=args.symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            environment=args.environment,
            gin_config=args.gin_config,
            debug=args.debug,
            num_cpus=args.num_cpus
        )
        
        # Check if any tasks failed
        failed_tasks = [r for r in results if r['status'] == 'failed']
        if failed_tasks:
            print(f"⚠️ {len(failed_tasks)} symbols failed feature extraction")
            return 1
        
        print(f"🎯 All {len(results)} symbols completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Feature extraction failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())