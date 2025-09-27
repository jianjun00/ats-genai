#!/usr/bin/env python3
"""
API Key Testing and Management Script

Tests all configured API keys and provides guidance on setting up valid keys.
"""

import os
import sys
import requests
import logging
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Set environment type for Gin config system
os.environ['ENVIRONMENT_TYPE'] = 'dev'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_eodhd_key(api_key):
    """Test EODHD API key"""
    url = f"https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json"
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        data = response.json()
        if isinstance(data, list) and len(data) > 0:
            logger.info(f"✅ EODHD key valid - found {len(data)} exchanges")
            return True
        else:
            logger.error("❌ EODHD key valid but returned empty data")
            return False
    else:
        logger.error(f"❌ EODHD key invalid - status: {response.status_code}, response: {response.text}")
        return False
def test_polygon_key(api_key):
    """Test Polygon API key"""
    url = f"https://api.polygon.io/v1/marketstatus/now?apikey={api_key}"
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        data = response.json()
        if data.get('status') == 'OK':
            logger.info("✅ Polygon key valid")
            return True
        else:
            logger.error(f"❌ Polygon key invalid - response: {data}")
            return False
    else:
        logger.error(f"❌ Polygon key invalid - status: {response.status_code}")
        return False
def test_tiingo_key(api_key):
    """Test Tiingo API key"""
    url = f"https://api.tiingo.com/api/test?token={api_key}"
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        data = response.json()
        if data.get('message') == 'You successfully sent a request':
            logger.info("✅ Tiingo key valid")
            return True
        else:
            logger.error(f"❌ Tiingo key invalid - response: {data}")
            return False
    else:
        logger.error(f"❌ Tiingo key invalid - status: {response.status_code}")
        return False
def main():
    """Test all API keys"""

    logger.info("🔑 Testing API Keys Configuration")
    logger.info("=" * 50)

    from config.environment import Environment, EnvironmentType
    env = Environment(env_type=EnvironmentType.DEV)
    logger.info(f"✅ Environment initialized: {env.env_type.value}")
    vendors = ['eodhd', 'polygon', 'tiingo']
    test_functions = {
        'eodhd': test_eodhd_key,
        'polygon': test_polygon_key,
        'tiingo': test_tiingo_key
    }

    results = {}

    for vendor in vendors:
        logger.info(f"\n🧪 Testing {vendor.upper()}...")

        api_key = env.get_api_key(vendor)
        if not api_key:
            logger.error(f"❌ No API key found for {vendor}")
            results[vendor] = False
            continue

        # Mask key for logging
        masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
        logger.info(f"   Using key: {masked_key}")

        test_func = test_functions.get(vendor)
        if test_func:
            results[vendor] = test_func(api_key)
        else:
            logger.warning(f"⚠️  No test function for {vendor}")
            results[vendor] = None

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("📊 API Key Test Summary")
    logger.info("=" * 50)

    valid_count = sum(1 for result in results.values() if result is True)
    total_count = len([r for r in results.values() if r is not None])

    for vendor, result in results.items():
        status = "✅ VALID" if result else "❌ INVALID" if result is False else "⚠️  UNKNOWN"
        env_var = f"{vendor.upper()}_API_KEY"
        logger.info(f"{vendor.upper():8}: {status}")

        if not result and result is not None:
            logger.info(f"         To fix: export {env_var}='your-valid-{vendor}-key'")

    logger.info(f"\nResult: {valid_count}/{total_count} API keys are valid")

    if valid_count == 0:
        logger.warning("\n⚠️  No valid API keys found. Data collection will fail.")
        logger.warning("   Get API keys from:")
        logger.warning("   - EODHD: https://eodhd.com/")
        logger.warning("   - Polygon: https://polygon.io/")
        logger.warning("   - Tiingo: https://tiingo.com/")
    elif valid_count < total_count:
        logger.info(f"\n✅ {valid_count} API keys working. Consider obtaining missing keys for full functionality.")
    else:
        logger.info("\n🎉 All API keys are valid! Ready for data collection.")

    return 0 if valid_count > 0 else 1

if __name__ == "__main__":
    exit(main())