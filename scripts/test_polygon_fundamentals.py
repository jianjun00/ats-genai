#!/usr/bin/env python3
"""
Simple test script for Polygon fundamentals API
"""

import os
import sys
import asyncio
import aiohttp
import json
import logging

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_fundamentals_api():
    """Test Polygon fundamentals API with simple calls"""
    
    api_key = os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD')
    
    async with aiohttp.ClientSession() as session:
        # Test 1: Basic call without filters
        url = "https://api.polygon.io/vX/reference/financials"
        params = {
            'ticker': 'AAPL',
            'timeframe': 'annual',
            'limit': 5,
            'apikey': api_key
        }
        
        logger.info("🧪 Test 1: Basic AAPL annual call...")
        
        try:
            async with session.get(url, params=params) as response:
                logger.info(f"Response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Success! Got {len(data.get('results', []))} records")
                    
                    # Show sample record
                    if data.get('results'):
                        sample = data['results'][0]
                        logger.info(f"Sample record: {sample.get('fiscal_year')} {sample.get('timeframe')}")
                else:
                    text = await response.text()
                    logger.error(f"❌ Error: {response.status} - {text}")
                    
        except Exception as e:
            logger.error(f"❌ Exception: {e}")

        # Test 2: Quarterly data
        params['timeframe'] = 'quarterly'
        logger.info("🧪 Test 2: AAPL quarterly call...")
        
        try:
            async with session.get(url, params=params) as response:
                logger.info(f"Response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Success! Got {len(data.get('results', []))} records")
                else:
                    text = await response.text()
                    logger.error(f"❌ Error: {response.status} - {text}")
                    
        except Exception as e:
            logger.error(f"❌ Exception: {e}")

        # Test 3: Different symbol
        params['ticker'] = 'MSFT'
        params['timeframe'] = 'annual'
        logger.info("🧪 Test 3: MSFT annual call...")
        
        try:
            async with session.get(url, params=params) as response:
                logger.info(f"Response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Success! Got {len(data.get('results', []))} records")
                else:
                    text = await response.text()
                    logger.error(f"❌ Error: {response.status} - {text}")
                    
        except Exception as e:
            logger.error(f"❌ Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_fundamentals_api())