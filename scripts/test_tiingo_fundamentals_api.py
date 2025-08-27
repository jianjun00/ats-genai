#!/usr/bin/env python3
"""
Test Tiingo Fundamentals API

Discover and test Tiingo's fundamentals API endpoints to understand the data structure
and build the collection infrastructure.
"""

import os
import requests
import json
import logging
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TiingoFundamentalsAPITester:
    """Test Tiingo fundamentals API endpoints and data structure."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tiingo.com"
        
        # Common test symbols
        self.test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        logger.info(f"🧪 Tiingo Fundamentals API Tester initialized")
        logger.info(f"   Base URL: {self.base_url}")
        logger.info(f"   Test symbols: {', '.join(self.test_symbols)}")

    def test_fundamentals_endpoints(self):
        """Test various potential fundamentals endpoints."""
        
        endpoints_to_test = [
            # Standard fundamentals endpoints
            "/tiingo/fundamentals/{symbol}",
            "/tiingo/fundamentals/{symbol}/statements",
            "/tiingo/fundamentals/{symbol}/daily",
            "/tiingo/fundamentals/{symbol}/meta",
            
            # Financial statements
            "/tiingo/fundamentals/{symbol}/statements/balanceSheet",
            "/tiingo/fundamentals/{symbol}/statements/incomeStatement", 
            "/tiingo/fundamentals/{symbol}/statements/cashFlow",
            
            # Alternative patterns
            "/fundamentals/{symbol}",
            "/fundamentals/{symbol}/statements",
            "/fundamentals/{symbol}/balance-sheet",
            "/fundamentals/{symbol}/income-statement",
            "/fundamentals/{symbol}/cash-flow",
            
            # Quarterly/Annual specific
            "/tiingo/fundamentals/{symbol}?frequency=quarterly",
            "/tiingo/fundamentals/{symbol}?frequency=annual",
        ]
        
        results = {}
        
        for endpoint_pattern in endpoints_to_test:
            logger.info(f"🔍 Testing endpoint pattern: {endpoint_pattern}")
            
            for symbol in self.test_symbols[:2]:  # Test with first 2 symbols
                endpoint = endpoint_pattern.format(symbol=symbol)
                full_url = f"{self.base_url}{endpoint}"
                
                try:
                    # Add common parameters
                    params = {
                        'token': self.api_key,
                        'format': 'json'
                    }
                    
                    response = requests.get(full_url, params=params, timeout=10)
                    
                    if response.status_code == 200:
                        logger.info(f"  ✅ SUCCESS: {endpoint} ({symbol})")
                        data = response.json()
                        
                        # Store successful response for analysis
                        results[endpoint_pattern] = {
                            'status': 200,
                            'symbol': symbol,
                            'data_keys': list(data.keys()) if isinstance(data, dict) else 'list',
                            'sample_data': str(data)[:200] + '...' if len(str(data)) > 200 else str(data)
                        }
                        
                        # Stop testing this pattern after first success
                        break
                        
                    elif response.status_code == 404:
                        logger.debug(f"  ❌ 404: {endpoint} ({symbol})")
                    elif response.status_code == 401:
                        logger.warning(f"  🔑 401 Unauthorized: {endpoint} ({symbol}) - check API key")
                    elif response.status_code == 403:
                        logger.warning(f"  🚫 403 Forbidden: {endpoint} ({symbol}) - may require premium plan")
                    else:
                        logger.warning(f"  ⚠️ {response.status_code}: {endpoint} ({symbol})")
                        
                except requests.exceptions.Timeout:
                    logger.warning(f"  ⏰ Timeout: {endpoint} ({symbol})")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"  ❌ Request error: {endpoint} ({symbol}) - {e}")
                except Exception as e:
                    logger.error(f"  💥 Unexpected error: {endpoint} ({symbol}) - {e}")
        
        return results

    def test_known_tiingo_patterns(self):
        """Test known Tiingo API patterns based on their documentation."""
        
        logger.info("🔍 Testing known Tiingo API patterns...")
        
        # Test daily prices endpoint (known to work) to verify API key
        test_url = f"{self.base_url}/tiingo/daily/AAPL/prices"
        params = {
            'token': self.api_key,
            'startDate': '2025-08-26',
            'endDate': '2025-08-26',
            'format': 'json'
        }
        
        try:
            response = requests.get(test_url, params=params, timeout=10)
            if response.status_code == 200:
                logger.info("✅ API key validation successful (daily prices work)")
            else:
                logger.error(f"❌ API key validation failed: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"❌ API key validation error: {e}")
            return {}
        
        # Now test fundamentals patterns
        fundamentals_patterns = [
            # Meta/definitions endpoint
            ("Meta/Definitions", f"{self.base_url}/tiingo/fundamentals/meta"),
            
            # Daily fundamentals
            ("Daily Fundamentals", f"{self.base_url}/tiingo/fundamentals/AAPL/daily"),
            
            # Statements (most likely endpoint)
            ("Statements", f"{self.base_url}/tiingo/fundamentals/AAPL/statements"),
            
            # Direct fundamentals
            ("Direct Fundamentals", f"{self.base_url}/tiingo/fundamentals/AAPL"),
        ]
        
        results = {}
        
        for name, url in fundamentals_patterns:
            logger.info(f"🔍 Testing {name}: {url}")
            
            params = {
                'token': self.api_key,
                'format': 'json'
            }
            
            try:
                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    logger.info(f"  ✅ SUCCESS: {name}")
                    data = response.json()
                    
                    results[name] = {
                        'url': url,
                        'status': 200,
                        'data_type': type(data).__name__,
                        'sample_keys': list(data.keys()) if isinstance(data, dict) else 'list',
                        'sample_data': str(data)[:300] + '...' if len(str(data)) > 300 else str(data)
                    }
                    
                elif response.status_code == 403:
                    logger.warning(f"  🚫 403 Forbidden: {name} - may require premium Tiingo plan")
                    results[name] = {'url': url, 'status': 403, 'error': 'Requires premium plan'}
                    
                elif response.status_code == 404:
                    logger.debug(f"  ❌ 404: {name}")
                    
                else:
                    logger.warning(f"  ⚠️ {response.status_code}: {name}")
                    results[name] = {'url': url, 'status': response.status_code}
                    
            except Exception as e:
                logger.error(f"  💥 Error testing {name}: {e}")
        
        return results

    def analyze_results(self, results):
        """Analyze and summarize test results."""
        
        logger.info("=" * 60)
        logger.info("📊 TIINGO FUNDAMENTALS API TEST RESULTS")
        logger.info("=" * 60)
        
        successful_endpoints = []
        forbidden_endpoints = []
        
        for name, result in results.items():
            if result.get('status') == 200:
                successful_endpoints.append((name, result))
                logger.info(f"✅ {name}:")
                logger.info(f"   URL: {result.get('url', 'N/A')}")
                logger.info(f"   Data type: {result.get('data_type', 'N/A')}")
                logger.info(f"   Keys: {result.get('sample_keys', 'N/A')}")
                logger.info(f"   Sample: {result.get('sample_data', 'N/A')[:100]}...")
                logger.info("")
                
            elif result.get('status') == 403:
                forbidden_endpoints.append((name, result))
        
        if successful_endpoints:
            logger.info(f"🎉 Found {len(successful_endpoints)} working fundamentals endpoints!")
        else:
            logger.warning("❌ No working fundamentals endpoints found")
            
        if forbidden_endpoints:
            logger.warning(f"🚫 {len(forbidden_endpoints)} endpoints require premium Tiingo plan:")
            for name, result in forbidden_endpoints:
                logger.warning(f"   {name}: {result.get('url', 'N/A')}")
        
        return successful_endpoints

def main():
    """Main execution function."""
    
    # Get Tiingo API key
    tiingo_api_key = os.environ.get("TIINGO_API_KEY")
    if not tiingo_api_key:
        logger.error("❌ TIINGO_API_KEY environment variable not set")
        return 1
    
    logger.info("✅ Tiingo API key found")
    
    # Initialize tester
    tester = TiingoFundamentalsAPITester(tiingo_api_key)
    
    # Test known patterns first (more efficient)
    logger.info("🚀 Testing known Tiingo API patterns...")
    known_results = tester.test_known_tiingo_patterns()
    
    # If no success with known patterns, do broader discovery
    if not any(r.get('status') == 200 for r in known_results.values()):
        logger.info("🔍 No success with known patterns, trying broader discovery...")
        discovery_results = tester.test_fundamentals_endpoints()
        all_results = {**known_results, **discovery_results}
    else:
        all_results = known_results
    
    # Analyze and summarize
    successful_endpoints = tester.analyze_results(all_results)
    
    if successful_endpoints:
        logger.info("✅ Tiingo fundamentals API discovery successful!")
        logger.info("🚀 Ready to build fundamentals collection infrastructure")
        return 0
    else:
        logger.warning("⚠️ No working fundamentals endpoints found")
        logger.warning("💡 This may indicate:")
        logger.warning("   1. Fundamentals require premium Tiingo plan")
        logger.warning("   2. Different API endpoint structure")
        logger.warning("   3. API key lacks fundamentals permissions")
        return 1

if __name__ == "__main__":
    exit(main())