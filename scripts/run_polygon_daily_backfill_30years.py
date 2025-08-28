#!/usr/bin/env python3
"""
30-Year Daily Price Backfill for All Polygon Instruments

This script performs comprehensive historical price backfill for all Polygon instruments
covering 30 years of data (1995-2025) using existing infrastructure.
"""

import sys
sys.path.append('/workspace/src')

import subprocess
import os
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_polygon_daily_backfill_30years")

def main():
    """Start comprehensive 30-year daily price backfill for all Polygon instruments."""
    
    # Set Polygon API key from environment
    polygon_key = os.getenv('POLYGON_API_KEY')
    if not polygon_key:
        logger.error("❌ POLYGON_API_KEY environment variable not set")
        sys.exit(1)
    
    # Set environment variables for the subprocess
    env = os.environ.copy()
    env['POLYGON_API_KEY'] = polygon_key
    
    # Calculate 30-year date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * 30)  # 30 years back
    
    logger.info(f"🚀 Starting 30-year Polygon daily price backfill...")
    logger.info(f"📅 Date range: {start_date} to {end_date} (30 years)")
    logger.info(f"🔧 Using existing run_daily_price_backfill.py infrastructure")
    
    # Run the comprehensive backfill using existing infrastructure
    cmd = [
        'python3', '/workspace/src/secmaster/run_daily_price_backfill.py',
        '--environment', 'dev',
        '--start_date', start_date.strftime('%Y-%m-%d'),
        '--end_date', end_date.strftime('%Y-%m-%d'),
        '--debug'
    ]
    
    logger.info(f"🔄 Executing: {' '.join(cmd)}")
    logger.info(f"⏱️  Expected runtime: ~8-12 hours for all instruments")
    logger.info(f"📊 Will process all 11,598 Polygon instruments")
    
    try:
        # Start the backfill process
        result = subprocess.run(cmd, env=env, cwd='/workspace')
        
        if result.returncode == 0:
            logger.info("✅ 30-year Polygon daily price backfill completed successfully!")
            logger.info("📈 Historical price data now available for all instruments")
        else:
            logger.error(f"❌ Backfill failed with exit code: {result.returncode}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Exception during backfill: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()