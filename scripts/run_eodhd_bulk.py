#!/usr/bin/env python3
"""
Bulk populate EODHD instruments using their native exchange-symbol-list API
"""

import sys
sys.path.append('/workspace/src')

import subprocess
import os
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_eodhd_bulk")

def get_eodhd_supported_symbols():
    """Get all supported US symbols from EODHD's native API"""
    api_key = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')
    url = f"https://eodhd.com/api/exchange-symbol-list/US?api_token={api_key}&fmt=json"
    
    logger.info(f"🌍 Fetching US exchange symbols from EODHD API...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # EODHD returns format: {"Code": "A", "Name": "Agilent Technologies Inc", "Country": "USA", ...}
        symbols = []
        for item in data:
            code = item.get('Code')
            if code:
                # EODHD expects symbols in format "SYMBOL.US"
                symbols.append(f"{code}.US")
        
        logger.info(f"🌍 Found {len(symbols)} US exchange symbols from EODHD")
        return symbols
        
    except Exception as e:
        logger.error(f"❌ Failed to fetch EODHD symbols: {e}")
        return []

def run_eodhd_population(symbols, batch_size=100):
    """Run EODHD population with batches of symbols"""
    if not symbols:
        logger.error("❌ No symbols to process")
        return False
    
    logger.info(f"🌍 Starting EODHD bulk population with {len(symbols)} symbols")
    
    # Split symbols into batches to avoid API rate limits and command line limits
    total_batches = (len(symbols) + batch_size - 1) // batch_size
    successful_batches = 0
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"🌍 Processing EODHD batch {batch_num}/{total_batches} ({len(batch)} symbols)")
        
        # Create comma-separated ticker list
        ticker_list = ','.join(batch)
        
        # Set up environment
        env = os.environ.copy()
        env['EODHD_API_KEY'] = '68aa0c7d2fe831.67386369'
        
        # Run the populate script
        cmd = [
            'python', '/workspace/src/secmaster/populate_instrument_eodhd.py',
            '--ticker', ticker_list,
            '--environment', 'dev'
        ]
        
        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
            if result.returncode == 0:
                successful_batches += 1
                logger.info(f"✅ EODHD batch {batch_num} completed successfully")
            else:
                logger.error(f"❌ EODHD batch {batch_num} failed: {result.stderr[:500]}")
        except subprocess.TimeoutExpired:
            logger.error(f"❌ EODHD batch {batch_num} timed out (10 minutes)")
        except Exception as e:
            logger.error(f"❌ EODHD batch {batch_num} error: {e}")
            
        # Delay between batches to respect API rate limits
        import time
        time.sleep(2)
    
    logger.info(f"🌍 EODHD bulk population completed: {successful_batches}/{total_batches} batches successful")
    return successful_batches == total_batches

def main():
    logger.info("🚀 Starting EODHD bulk population using native exchange-symbol-list API...")
    
    # Get symbols from EODHD's bulk endpoint
    symbols = get_eodhd_supported_symbols()
    
    if not symbols:
        logger.error("❌ Failed to retrieve symbols from EODHD API")
        sys.exit(1)
    
    # Run bulk population
    success = run_eodhd_population(symbols, batch_size=100)
    
    if success:
        logger.info("✅ EODHD bulk population completed successfully!")
    else:
        logger.error("❌ EODHD bulk population had failures")
        sys.exit(1)

if __name__ == "__main__":
    main()