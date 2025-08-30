#!/usr/bin/env python3
"""
Daily Price Collector Service - Continuously collects market data
"""

import os
import asyncio
import logging
import signal
import sys
from datetime import datetime, time
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/price_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DailyPriceCollector:
    def __init__(self):
        self.running = True
        self.environment = os.getenv('ENVIRONMENT', 'dev')
        logger.info(f"Starting Daily Price Collector for {self.environment} environment")
        
    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    async def collect_daily_prices(self):
        """Main collection logic - placeholder for actual implementation"""
        logger.info("Starting daily price collection cycle")
        
        # TODO: Implement actual price collection logic
        # This would integrate with existing scripts like:
        # - scripts/polygon_30_year_daily_backfill.py
        # - scripts/tiingo_30_year_daily_backfill.py  
        # - scripts/eodhd_30_year_daily_backfill.py
        
        await asyncio.sleep(10)  # Simulate work
        logger.info("Daily price collection cycle completed")
        
    async def run(self):
        """Main service loop"""
        logger.info("Daily Price Collector service started")
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        while self.running:
            try:
                current_time = datetime.now().time()
                
                # Run collection during market hours (6 AM to 6 PM)
                if time(6, 0) <= current_time <= time(18, 0):
                    await self.collect_daily_prices()
                    # Wait 1 hour between collections during market hours
                    await asyncio.sleep(3600)
                else:
                    # Wait 5 minutes during off-hours
                    logger.info("Outside market hours, sleeping...")
                    await asyncio.sleep(300)
                    
            except Exception as e:
                logger.error(f"Error in price collection: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
                
        logger.info("Daily Price Collector service stopped")

if __name__ == "__main__":
    collector = DailyPriceCollector()
    asyncio.run(collector.run())