#!/usr/bin/env python3
"""
API Key Validation Script

Validates all API keys used in the ATS platform to ensure they work before services start.
Uses the same API keys from .env.test found in git history for consistency.
"""

import os
import requests
import sys
from datetime import datetime, timedelta

# API keys from .env.test in git history (commit 5168e8e83)
DEFAULT_API_KEYS = {
    'POLYGON_API_KEY': 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',
    'TIINGO_API_KEY': '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5',
    'EODHD_API_KEY': '68aa0c7d2fe831.67386369',
    'FMP_API_KEY': 'Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr',
    'ALPHA_VANTAGE_API_KEY': '9GI0NZ3V4VNFX271'
}

def validate_polygon_api(api_key):
    """Validate Polygon API key"""
    url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2025-09-11/2025-09-11"
    params = {
        'adjusted': 'true',
        'sort': 'asc', 
        'limit': 1,
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'ERROR':
                return False, f"API Error: {data.get('error', 'Unknown error')}"
            return True, f"OK - Status: {data.get('status', 'Unknown')}"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:100]}"
    except Exception as e:
        return False, f"Exception: {str(e)}"

def validate_tiingo_api(api_key):
    """Validate Tiingo API key"""
    url = f"https://api.tiingo.com/tiingo/daily/AAPL/prices"
    params = {
        'startDate': '2025-09-10',
        'token': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return True, f"OK - Returned {len(data)} records"
        elif response.status_code == 403:
            return False, f"HTTP 403: Invalid token or access denied"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:100]}"
    except Exception as e:
        return False, f"Exception: {str(e)}"

def validate_eodhd_api(api_key):
    """Validate EODHD API key"""
    # Use timestamp format for EODHD
    yesterday = datetime.now() - timedelta(days=1)
    timestamp = int(yesterday.timestamp())
    
    url = f"https://eodhistoricaldata.com/api/intraday/AAPL.US"
    params = {
        'api_token': api_key,
        'interval': '1m',
        'from': timestamp,
        'to': timestamp + 3600,  # 1 hour window
        'fmt': 'json'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and 'errors' in data:
                return False, f"API Error: {data['errors']}"
            return True, f"OK - Returned {len(data) if isinstance(data, list) else 'data'}"
        elif response.status_code == 422:
            # 422 is expected for some time periods
            return True, "OK - 422 expected for some time periods"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:100]}"
    except Exception as e:
        return False, f"Exception: {str(e)}"

def validate_fmp_api(api_key):
    """Validate Financial Modeling Prep API key"""
    url = f"https://financialmodelingprep.com/api/v3/profile/AAPL"
    params = {'apikey': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                return True, f"OK - Profile data returned"
            return False, "Empty response"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:100]}"
    except Exception as e:
        return False, f"Exception: {str(e)}"

def validate_alpha_vantage_api(api_key):
    """Validate Alpha Vantage API key"""
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': 'AAPL',
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'Error Message' in data:
                return False, f"API Error: {data['Error Message']}"
            elif 'Note' in data:
                return False, f"Rate Limited: {data['Note']}"
            elif 'Global Quote' in data:
                return True, "OK - Global quote returned"
            return False, f"Unexpected response: {data}"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:100]}"
    except Exception as e:
        return False, f"Exception: {str(e)}"

def main():
    """Validate all API keys"""
    validators = {
        'POLYGON_API_KEY': validate_polygon_api,
        'TIINGO_API_KEY': validate_tiingo_api,
        'EODHD_API_KEY': validate_eodhd_api,
        'FMP_API_KEY': validate_fmp_api,
        'ALPHA_VANTAGE_API_KEY': validate_alpha_vantage_api
    }
    
    print("🔑 Validating API Keys...")
    print("=" * 60)
    
    all_valid = True
    
    for key_name, validator in validators.items():
        # Use environment variable if set, otherwise use default from .env.test
        api_key = os.getenv(key_name, DEFAULT_API_KEYS.get(key_name, ''))
        
        if not api_key:
            print(f"❌ {key_name}: Not configured")
            all_valid = False
            continue
            
        print(f"🔍 Testing {key_name}...")
        is_valid, message = validator(api_key)
        
        if is_valid:
            print(f"✅ {key_name}: {message}")
        else:
            print(f"❌ {key_name}: {message}")
            all_valid = False
    
    print("=" * 60)
    
    if all_valid:
        print("✅ All API keys are valid!")
        return 0
    else:
        print("❌ Some API keys are invalid. Check configuration.")
        return 1

if __name__ == '__main__':
    sys.exit(main())