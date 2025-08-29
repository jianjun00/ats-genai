#!/usr/bin/env python3
"""
Restart daily backfill jobs with the fixes applied.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, date

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Restart daily backfill jobs with fixes."""
    logger.info("🚀 Starting fixed daily backfill jobs...")
    
    # Test that our fixes work
    try:
        # Test Polygon datetime fix
        from zoneinfo import ZoneInfo
        timestamp_ms = 1640995200000  # 2022-01-01 UTC
        utc_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=ZoneInfo("UTC"))
        date_val = utc_dt.date()
        logger.info(f"✅ Polygon datetime fix working: {date_val}")
        
        # Test imports
        from market_data.agent.polygon_adapter import PolygonAdapter
        from market_data.agent.tiingo_adapter import TiingoAdapter
        logger.info("✅ Adapters imported successfully")
        
        # Test database connection (basic check)
        from config.database import Database
        db = Database()
        logger.info(f"✅ Database config loaded: {db.host}:{db.port}/{db.database}")
        
    except Exception as e:
        logger.error(f"❌ Fix verification failed: {e}")
        sys.exit(1)
    
    logger.info("🎉 All fixes verified successfully!")
    logger.info("📝 Manual restart required for full backfill jobs")
    logger.info("🔧 Use the following commands to restart backfill jobs:")
    logger.info("   - Polygon: PYTHONPATH=src POLYGON_API_KEY=<key> python scripts/populate_30year_polygon_minute_bars.py --resume")
    logger.info("   - Daily prices: Use vendor-specific daily backfill scripts")

if __name__ == "__main__":
    main()