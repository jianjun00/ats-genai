#!/usr/bin/env python3
"""
Script to run comprehensive multi-vendor news backfill.
Extracted for Kubernetes execution with proper error handling.
"""

import sys
import os
import asyncio
import logging

# Add src to Python path
sys.path.insert(0, '/app/src')

from market_data.news.comprehensive_news_backfill import ComprehensiveNewsBackfiller

async def main():
    """Execute comprehensive news backfill with error handling"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Database configuration from environment
        db_config = {
            'host': os.getenv("DB_HOST", "postgres"),
            'port': int(os.getenv("DB_PORT", "5432")),
            'user': os.getenv("DB_USER", "postgres"),
            'password': os.getenv("DB_PASSWORD", "dev_password"),
            'database': os.getenv("DB_NAME", "dev_db")
        }
        
        logger.info(f"Database config: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        
        # Test with major symbols for initial validation
        major_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
            'NVDA', 'META', 'BRK.A', 'V', 'JNJ'
        ]
        
        # Check for limit argument
        limit = None
        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            limit = int(sys.argv[1])
            logger.info(f"Limiting to {limit} symbols")
        
        if limit:
            major_symbols = major_symbols[:limit]
        
        logger.info(f"Processing {len(major_symbols)} symbols: {', '.join(major_symbols)}")
        
        # Execute comprehensive backfill
        async with ComprehensiveNewsBackfiller(db_config, major_symbols) as backfiller:
            results = await backfiller.run_comprehensive_backfill()
            
            # Log final results
            total_articles = sum(results.values())
            logger.info(f"🎉 BACKFILL COMPLETE: {total_articles:,} total articles")
            
            for vendor, count in results.items():
                logger.info(f"   {vendor.upper()}: {count:,} articles")
            
            # Return success
            return 0
            
    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)