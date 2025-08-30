#!/usr/bin/env python3
"""
Local test of the real-time collector orchestrator
Tests the core functionality without full deployment
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# Add src path
sys.path.append('src')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('test')

async def test_basic_functionality():
    """Test basic functionality without external APIs."""
    
    # Test MarketHours class
    try:
        from realtime_collector_orchestrator import MarketHours
        
        market_hours = MarketHours()
        is_open = market_hours.is_market_open()
        logger.info(f"✅ MarketHours works - Market open: {is_open}")
        
    except Exception as e:
        logger.error(f"❌ MarketHours test failed: {e}")
        return False
    
    # Test MinuteBar dataclass
    try:
        from realtime_collector_orchestrator import MinuteBar
        
        bar = MinuteBar(
            symbol='TEST',
            timestamp=datetime.now(timezone.utc),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            volume=1000,
            vendor='test'
        )
        
        logger.info(f"✅ MinuteBar works - Symbol: {bar.symbol}, Price: {bar.close_price}")
        
    except Exception as e:
        logger.error(f"❌ MinuteBar test failed: {e}")
        return False
    
    # Test API key detection
    api_keys = {
        'POLYGON_API_KEY': os.getenv('POLYGON_API_KEY', ''),
        'TIINGO_API_KEY': os.getenv('TIINGO_API_KEY', ''),
        'FMP_API_KEY': os.getenv('FMP_API_KEY', ''),
    }
    
    available = [k for k, v in api_keys.items() if v]
    logger.info(f"✅ Available API keys: {len(available)}/3 - {available}")
    
    return True

def load_env_file():
    """Load environment variables from .env.test."""
    env_file = ".env.test"
    if os.path.exists(env_file):
        logger.info("📁 Loading environment from .env.test")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
    else:
        logger.warning("⚠️ .env.test file not found")

async def main():
    """Main test function."""
    logger.info("🧪 Testing Real-Time Collector Components")
    logger.info("=" * 50)
    
    # Load environment
    load_env_file()
    
    # Test basic functionality
    if await test_basic_functionality():
        logger.info("🎉 All basic tests passed!")
        return True
    else:
        logger.error("💥 Some tests failed!")
        return False

if __name__ == "__main__":
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir + '/..')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)