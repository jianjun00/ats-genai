#!/usr/bin/env python3
"""
Debug Tiingo Fundamentals API Issues

Investigate and diagnose 400 errors from Tiingo fundamentals API.
"""

import os
import requests
import json
import logging
from datetime import datetime, date, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_tiingo_fundamentals_api():
    """Debug Tiingo fundamentals API issues."""
    
    api_key = os.environ.get("TIINGO_API_KEY")
    if not api_key:
        logger.error("❌ TIINGO_API_KEY not found")
        return
    
    base_url = "https://api.tiingo.com/tiingo/fundamentals"
    
    # Test various symbols and date ranges
    test_cases = [
        # Major symbols that should have data
        {"symbol": "AAPL", "days_back": 30},
        {"symbol": "MSFT", "days_back": 30}, 
        {"symbol": "GOOGL", "days_back": 30},
        
        # Try different date ranges
        {"symbol": "AAPL", "days_back": 365},  # 1 year
        {"symbol": "AAPL", "days_back": 1825}, # 5 years
    ]
    
    logger.info("🔍 Debugging Tiingo fundamentals API...")
    
    for test_case in test_cases:
        symbol = test_case["symbol"]
        days_back = test_case["days_back"]
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"🧪 Testing {symbol} from {start_date} to {end_date} ({days_back} days)")
        
        # Test daily fundamentals
        daily_url = f"{base_url}/{symbol}/daily"
        daily_params = {
            'token': api_key,
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json'
        }
        
        try:
            response = requests.get(daily_url, params=daily_params, timeout=10)
            logger.info(f"  📊 Daily API: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"    ✅ Success: {len(data)} records")
                if data:
                    sample = data[0]
                    logger.info(f"    Sample keys: {list(sample.keys())}")
            elif response.status_code == 400:
                logger.error(f"    ❌ 400 Bad Request: {response.text[:200]}")
            else:
                logger.warning(f"    ⚠️ {response.status_code}: {response.text[:200]}")
                
        except Exception as e:
            logger.error(f"    💥 Exception: {e}")
        
        # Test statements (without date range)
        statements_url = f"{base_url}/{symbol}/statements"
        statements_params = {
            'token': api_key,
            'format': 'json'
        }
        
        try:
            response = requests.get(statements_url, params=statements_params, timeout=10)
            logger.info(f"  📋 Statements API: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"    ✅ Success: {len(data)} periods")
                if data:
                    sample = data[0]
                    logger.info(f"    Sample keys: {list(sample.keys())}")
            elif response.status_code == 400:
                logger.error(f"    ❌ 400 Bad Request: {response.text[:200]}")
            else:
                logger.warning(f"    ⚠️ {response.status_code}: {response.text[:200]}")
                
        except Exception as e:
            logger.error(f"    💥 Exception: {e}")
        
        logger.info("")
    
    # Test without date parameters for daily
    logger.info("🔍 Testing daily API without date parameters...")
    no_date_url = f"{base_url}/AAPL/daily"
    no_date_params = {
        'token': api_key,
        'format': 'json'
    }
    
    try:
        response = requests.get(no_date_url, params=no_date_params, timeout=10)
        logger.info(f"Daily API (no dates): {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Success: {len(data)} records")
        else:
            logger.warning(f"⚠️ {response.status_code}: {response.text[:300]}")
            
    except Exception as e:
        logger.error(f"💥 Exception: {e}")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    debug_tiingo_fundamentals_api()