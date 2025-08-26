#!/usr/bin/env python3
"""
Bulk populate Tiingo instruments using their native supported-tickers API
"""

import sys
sys.path.append('/workspace/src')

import subprocess
import os
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_tiingo_bulk")

def get_tiingo_supported_symbols():
    """Get all supported symbols from Tiingo's native API"""
    api_key = os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5')
    url = f"https://api.tiingo.com/tiingo/supported-tickers?token={api_key}"
    
    logger.info(f"🔵 Fetching supported tickers from Tiingo API...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        symbols = [item.get('ticker') for item in data if item.get('ticker')]
        logger.info(f"🔵 Found {len(symbols)} supported symbols from Tiingo")
        return symbols
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch Tiingo symbols: {e}")
        return []

def run_tiingo_population(symbols, batch_size=100):
    """Run Tiingo population with batches of symbols"""
    if not symbols:
        logger.error("❌ No symbols to process")
        return False
    
    logger.info(f"🔵 Starting Tiingo bulk population with {len(symbols)} symbols")
    
    # Split symbols into batches to avoid API rate limits and command line limits
    total_batches = (len(symbols) + batch_size - 1) // batch_size
    successful_batches = 0
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"🔵 Processing Tiingo batch {batch_num}/{total_batches} ({len(batch)} symbols)")
        
        # Create comma-separated ticker list
        ticker_list = ','.join(batch)
        
        # Set up environment
        env = os.environ.copy()
        env['TIINGO_API_KEY'] = '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5'
        
        # Run the populate script
        cmd = [
            'python', '/workspace/src/secmaster/populate_instrument_tiingo.py',
            '--ticker', ticker_list,
            '--environment', 'dev'
        ]
        
        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                successful_batches += 1
                logger.info(f"✅ Tiingo batch {batch_num} completed successfully")
            else:
                logger.error(f"❌ Tiingo batch {batch_num} failed: {result.stderr[:500]}")
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Tiingo batch {batch_num} timed out (10 minutes)")
        except Exception as e:
            logger.error(f"❌ Tiingo batch {batch_num} error: {e}")
            
        # Delay between batches to respect API rate limits
        import time
        time.sleep(2)
    
    logger.info(f"🔵 Tiingo bulk population completed: {successful_batches}/{total_batches} batches successful")
    return successful_batches == total_batches

def main():
    logger.info("🚀 Starting Tiingo bulk population using native supported-tickers API...")
    
    # Get symbols from Tiingo's bulk endpoint
    symbols = get_tiingo_supported_symbols()
    
    if not symbols:
        logger.error("❌ Failed to retrieve symbols from Tiingo API")
        sys.exit(1)
    
    # Run bulk population
    success = run_tiingo_population(symbols, batch_size=100)
    
    if success:
        logger.info("✅ Tiingo bulk population completed successfully!")
    else:
        logger.error("❌ Tiingo bulk population had failures")
        sys.exit(1)

if __name__ == "__main__":
    main()