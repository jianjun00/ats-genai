#!/usr/bin/env python3
"""
Test Tiingo News API

Test if Tiingo actually has a working news API and what data is available.
"""

import os
import requests
import json
import logging
from datetime import datetime, date, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_tiingo_news_api():
    """Test Tiingo news API endpoints."""
    
    api_key = os.environ.get("TIINGO_API_KEY")
    if not api_key:
        logger.error("❌ TIINGO_API_KEY not found")
        return
    
    # Test cases for different endpoints and parameters
    test_cases = [
        # Test if news endpoint exists at all
        {
            "name": "Basic News Endpoint",
            "url": "https://api.tiingo.com/tiingo/news",
            "params": {"token": api_key, "limit": 5}
        },
        
        # Test with specific ticker
        {
            "name": "News for AAPL",
            "url": "https://api.tiingo.com/tiingo/news", 
            "params": {
                "token": api_key,
                "tickers": "AAPL",
                "limit": 5
            }
        },
        
        # Test with date range (recent)
        {
            "name": "Recent News for AAPL",
            "url": "https://api.tiingo.com/tiingo/news",
            "params": {
                "token": api_key,
                "tickers": "AAPL",
                "startDate": (date.today() - timedelta(days=30)).strftime('%Y-%m-%d'),
                "endDate": date.today().strftime('%Y-%m-%d'),
                "limit": 10
            }
        },
        
        # Test alternative news endpoints
        {
            "name": "Alternative News Endpoint",
            "url": "https://api.tiingo.com/news",
            "params": {"token": api_key, "limit": 5}
        },
        
        # Test crypto news (Tiingo has crypto)
        {
            "name": "Crypto News",
            "url": "https://api.tiingo.com/tiingo/crypto/news",
            "params": {"token": api_key, "limit": 5}
        }
    ]
    
    logger.info("🧪 Testing Tiingo News API endpoints...")
    
    working_endpoints = []
    
    for test in test_cases:
        logger.info(f"🔍 Testing: {test['name']}")
        
        try:
            response = requests.get(test['url'], params=test['params'], timeout=10)
            
            logger.info(f"  📊 HTTP Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    if isinstance(data, list):
                        logger.info(f"  ✅ SUCCESS: Got {len(data)} items")
                        if data:
                            # Show sample keys
                            sample_keys = list(data[0].keys()) if data else []
                            logger.info(f"    Sample keys: {sample_keys}")
                            working_endpoints.append(test)
                    elif isinstance(data, dict):
                        logger.info(f"  ✅ SUCCESS: Got dict response")
                        logger.info(f"    Keys: {list(data.keys())}")
                        working_endpoints.append(test)
                    else:
                        logger.info(f"  ⚠️ Unexpected response type: {type(data)}")
                        
                except json.JSONDecodeError:
                    logger.warning(f"  ❌ Invalid JSON response")
                    logger.info(f"    Response: {response.text[:200]}...")
                    
            elif response.status_code == 404:
                logger.info(f"  ❌ Endpoint not found")
            elif response.status_code == 401:
                logger.warning(f"  🔑 Unauthorized - check API key")
            elif response.status_code == 403:
                logger.warning(f"  🚫 Forbidden - may require premium plan")
            else:
                logger.warning(f"  ⚠️ Error response")
                logger.info(f"    Response: {response.text[:200]}...")
                
        except requests.exceptions.Timeout:
            logger.warning(f"  ⏰ Request timeout")
        except requests.exceptions.RequestException as e:
            logger.error(f"  ❌ Request error: {e}")
        except Exception as e:
            logger.error(f"  💥 Unexpected error: {e}")
            
        logger.info("")
    
    # Summary
    logger.info("=" * 60)
    logger.info("📊 TIINGO NEWS API TEST SUMMARY")
    logger.info("=" * 60)
    
    if working_endpoints:
        logger.info(f"✅ Found {len(working_endpoints)} working endpoints:")
        for endpoint in working_endpoints:
            logger.info(f"  - {endpoint['name']}: {endpoint['url']}")
        
        logger.info("🚀 Tiingo news collection is possible!")
    else:
        logger.warning("❌ No working news endpoints found")
        logger.warning("💡 This indicates:")
        logger.warning("   1. Tiingo may not have a public news API")
        logger.warning("   2. News API may require premium plan")
        logger.warning("   3. Different endpoint structure needed")
        logger.warning("   4. API key lacks news permissions")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    test_tiingo_news_api()