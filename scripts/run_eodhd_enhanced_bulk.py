#!/usr/bin/env python3
"""
Enhanced EODHD bulk population with proper IPO date fetching

This script uses the fundamentals API to get complete instrument data
including IPO dates, unlike the previous version that only used 
exchange-symbol-list which doesn't include temporal data.
"""

import sys
sys.path.append('/workspace/src')

import subprocess
import os
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_eodhd_enhanced_bulk")

def get_sample_symbols_for_testing(limit=50):
    """Get a small sample of popular symbols for testing the new logic"""
    # Popular symbols that should have IPO dates
    test_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'TSLA', 'META', 'NVDA', 'AMZN', 'NFLX', 'AMD',
        'CRM', 'ORCL', 'ADBE', 'PYPL', 'INTC', 'CSCO', 'PEP', 'KO', 'COST', 'AVGO',
        'TXN', 'QCOM', 'HON', 'UNP', 'V', 'MA', 'HD', 'BA', 'DIS', 'CMCSA',
        'VZ', 'T', 'WMT', 'JNJ', 'PG', 'JPM', 'BAC', 'WFC', 'C', 'GS',
        'IBM', 'GE', 'F', 'GM', 'CAT', 'MMM', 'NKE', 'MCD', 'SBUX', 'BKNG'
    ]
    
    return test_symbols[:limit]

def run_eodhd_test_population(symbols, batch_size=10):
    """Run EODHD population test with sample symbols"""
    if not symbols:
        logger.error("❌ No symbols to process")
        return False
    
    logger.info(f"🧪 Starting EODHD test population with {len(symbols)} symbols")
    
    # Split symbols into batches to avoid command line limits
    total_batches = (len(symbols) + batch_size - 1) // batch_size
    successful_batches = 0
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"🧪 Processing test batch {batch_num}/{total_batches} ({len(batch)} symbols)")
        logger.info(f"   Symbols: {', '.join(batch)}")
        
        # Create comma-separated ticker list
        ticker_list = ','.join(batch)
        
        # Set up environment
        env = os.environ.copy()
        env['EODHD_API_KEY'] = '68aa0c7d2fe831.67386369'
        
        # Run the populate script with individual ticker mode
        cmd = [
            'python3', 'src/secmaster/populate_instrument_eodhd.py',
            '--ticker', ticker_list,
            '--environment', 'dev',
            '--debug'
        ]
        
        try:
            logger.info(f"   Executing: {' '.join(cmd[:4])} --ticker {ticker_list[:50]}{'...' if len(ticker_list) > 50 else ''}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                successful_batches += 1
                logger.info(f"✅ Test batch {batch_num} completed successfully")
                # Log any IPO dates found
                if 'IPO:' in result.stdout:
                    ipo_lines = [line.strip() for line in result.stdout.split('\n') if 'IPO:' in line]
                    for line in ipo_lines[-3:]:  # Show last 3 IPO date lines
                        logger.info(f"   {line}")
            else:
                logger.error(f"❌ Test batch {batch_num} failed: {result.stderr[:300]}")
                if result.stdout:
                    logger.error(f"❌ Stdout: {result.stdout[-200:]}")
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Test batch {batch_num} timed out (5 minutes)")
        except Exception as e:
            logger.error(f"❌ Test batch {batch_num} error: {e}")
            
        # Small delay between batches for rate limiting
        import time
        time.sleep(2)
    
    logger.info(f"🧪 EODHD test population completed: {successful_batches}/{total_batches} batches successful")
    return successful_batches == total_batches

def run_eodhd_full_bulk_population(exchange='US', start_ticker=''):
    """Run full bulk population using the new bulk mode"""
    logger.info(f"🌍 Starting EODHD full bulk population for {exchange} exchange")
    
    if start_ticker:
        logger.info(f"   Starting from ticker: {start_ticker}")
    
    # Set up environment
    env = os.environ.copy()
    env['EODHD_API_KEY'] = '68aa0c7d2fe831.67386369'
    
    # Run the populate script in bulk mode
    cmd = [
        'python3', 'src/secmaster/populate_instrument_eodhd.py',
        '--bulk',
        '--exchange', exchange,
        '--environment', 'dev'
    ]
    
    if start_ticker:
        cmd.extend(['--start_ticker', start_ticker])
    
    try:
        logger.info(f"🌍 Executing: {' '.join(cmd)}")
        logger.warning(f"⚠️  This will take several hours due to API rate limiting (3 seconds per symbol)")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=14400)  # 4 hours
        if result.returncode == 0:
            logger.info(f"✅ Full bulk population completed successfully")
            if result.stdout:
                logger.info(f"Output summary: {result.stdout[-500:]}")
            return True
        else:
            logger.error(f"❌ Full bulk population failed: {result.stderr[:1000]}")
            if result.stdout:
                logger.error(f"❌ Stdout: {result.stdout[-500:]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Full bulk population timed out (4 hours)")
        return False
    except Exception as e:
        logger.error(f"❌ Full bulk population error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="EODHD enhanced bulk population with IPO dates")
    parser.add_argument('--mode', choices=['test', 'full'], default='test', 
                       help='Run mode: test (sample symbols) or full (all symbols)')
    parser.add_argument('--exchange', default='US', help='Exchange to process (default: US)')
    parser.add_argument('--start-ticker', default='', help='Start from this ticker (for resuming)')
    parser.add_argument('--test-size', type=int, default=20, help='Number of test symbols (default: 20)')
    args = parser.parse_args()
    
    if args.mode == 'test':
        logger.info("🧪 Starting EODHD test population with sample symbols...")
        
        # Get sample symbols for testing
        symbols = get_sample_symbols_for_testing(args.test_size)
        logger.info(f"🧪 Testing with {len(symbols)} popular symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")
        
        # Run test population
        success = run_eodhd_test_population(symbols, batch_size=5)
        
        if success:
            logger.info("✅ EODHD test population completed successfully!")
            logger.info("   You can now check the database for IPO dates using:")
            logger.info("   PYTHONPATH=src python scripts/run_dev.py query --query \"SELECT symbol, name, ipo_date FROM dev_instrument_eodhd WHERE ipo_date IS NOT NULL LIMIT 10\"")
        else:
            logger.error("❌ EODHD test population had failures")
            sys.exit(1)
    
    elif args.mode == 'full':
        logger.info(f"🌍 Starting EODHD full bulk population for {args.exchange} exchange...")
        
        # Run full bulk population
        success = run_eodhd_full_bulk_population(args.exchange, args.start_ticker)
        
        if success:
            logger.info("✅ EODHD full bulk population completed successfully!")
        else:
            logger.error("❌ EODHD full bulk population failed")
            sys.exit(1)

if __name__ == "__main__":
    main()