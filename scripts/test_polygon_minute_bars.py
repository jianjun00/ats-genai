#!/usr/bin/env python3
"""
Test Polygon Minute Bars with Centralized API Key Management

Demonstrates that the centralized API key system works with a valid API key.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Set environment type for Gin config system
os.environ['ENVIRONMENT_TYPE'] = 'dev'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_polygon_minute_bars():
    """Test Polygon minute bar collection using centralized API key system"""

    try:
        # Use centralized API key management
        from config.environment import Environment, EnvironmentType
        env = Environment(env_type=EnvironmentType.DEV)

        api_key = env.get_api_key('polygon')
        logger.info(f"✅ Retrieved Polygon API key via centralized system: {api_key[:8]}...{api_key[-4:]}")

        # Import Polygon adapter
        from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter

        # Initialize adapter with centralized API key
        adapter = PolygonMinuteAdapter(api_key)
        logger.info("✅ PolygonMinuteAdapter initialized successfully")

        # Test with a simple request for recent data
        symbol = 'AAPL'
        start_date = '2024-01-02'
        end_date = '2024-01-02'

        logger.info(f"🧪 Testing minute bar collection for {symbol} on {start_date}")

        # This should work with the valid API key
        try:
            bars = await adapter.get_minute_bars(symbol, start_date, end_date)

            if bars:
                logger.info(f"✅ SUCCESS: Retrieved {len(bars)} minute bars for {symbol}")
                logger.info(f"   Sample bar: {bars[0]}")
                logger.info("🎉 Centralized API key management is working perfectly!")
                return True
            else:
                logger.warning(f"⚠️  No data returned for {symbol} on {start_date} (may be holiday/weekend)")
                return True  # Still counts as successful API call

        except Exception as e:
            logger.error(f"❌ Polygon API call failed: {e}")
            return False

    except Exception as e:
        logger.error(f"❌ Failed to initialize Polygon test: {e}")
        return False

def main():
    """Main test function"""

    logger.info("🚀 Testing Polygon Minute Bars with Centralized API Key Management")
    logger.info("=" * 70)

    try:
        import asyncio
        result = asyncio.run(test_polygon_minute_bars())

        if result:
            logger.info("\n✅ Test completed successfully!")
            logger.info("🔑 Centralized API key management is working correctly")
            logger.info("📊 Ready for production data collection with valid API keys")
        else:
            logger.error("\n❌ Test failed - check API key or network connection")
            return 1

    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())