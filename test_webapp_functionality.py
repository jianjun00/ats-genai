#!/usr/bin/env python3
"""
Test Cases for Real Data Analytics WebApp

Verify that the webapp is actually providing the functionality requested:
1. Portfolio analytics (not just instrument lists)
2. Training data investigation 
3. Model predictions
4. Real data (no mock data)
"""

import requests
import json
import sys
from datetime import datetime

BASE_URL = "http://localhost:3000"

def test_endpoint(url, test_name, expected_fields=None, should_not_contain=None):
    """Test a single endpoint and verify response"""
    print(f"\n🧪 Testing: {test_name}")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ FAIL: Expected 200, got {response.status_code}")
            return False
        
        data = response.json()
        print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        
        # Check for expected fields
        if expected_fields:
            for field in expected_fields:
                if field not in data:
                    print(f"❌ FAIL: Missing expected field '{field}'")
                    return False
                print(f"✅ Found expected field: {field}")
        
        # Check that certain values are NOT present (e.g., mock data)
        if should_not_contain:
            response_str = json.dumps(data).lower()
            for bad_value in should_not_contain:
                if bad_value.lower() in response_str:
                    print(f"❌ FAIL: Found unwanted value '{bad_value}'")
                    return False
                print(f"✅ Confirmed absence of: {bad_value}")
        
        # Check for mock_data flag
        if isinstance(data, dict) and 'mock_data' in data:
            if data['mock_data']:
                print(f"❌ FAIL: mock_data is True")
                return False
            print(f"✅ Confirmed: mock_data is False")
        
        print(f"✅ PASS: {test_name}")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: Exception - {e}")
        return False

def test_portfolio_analytics():
    """Test portfolio analytics functionality"""
    print(f"\n{'='*60}")
    print("TESTING PORTFOLIO ANALYTICS")
    print('='*60)
    
    # Test portfolio endpoint
    success = test_endpoint(
        f"{BASE_URL}/api/v1/portfolio",
        "Portfolio Data",
        expected_fields=["portfolio_data", "data_source"],
        should_not_contain=["mock", "sample", "fake"]
    )
    
    # Check if we actually have portfolio analytics or just instrument lists
    try:
        response = requests.get(f"{BASE_URL}/api/v1/portfolio")
        data = response.json()
        
        if "portfolio_data" in data and len(data["portfolio_data"]) > 0:
            sample_item = data["portfolio_data"][0]
            
            # Check if this is real portfolio analytics vs just instrument data
            analytics_fields = ["return", "sharpe_ratio", "allocation", "weight", "performance", "pnl"]
            has_analytics = any(field in sample_item for field in analytics_fields)
            
            if has_analytics:
                print("✅ PASS: Contains actual portfolio analytics data")
            else:
                print("❌ FAIL: Only contains instrument data, no portfolio analytics")
                print(f"Sample item fields: {list(sample_item.keys())}")
                return False
    except Exception as e:
        print(f"❌ FAIL: Could not analyze portfolio data - {e}")
        return False
    
    return success

def test_training_data():
    """Test training data functionality"""
    print(f"\n{'='*60}")
    print("TESTING TRAINING DATA")
    print('='*60)
    
    success = test_endpoint(
        f"{BASE_URL}/api/v1/training",
        "Training Data",
        expected_fields=["data_source"],
        should_not_contain=["mock"]
    )
    
    # Check if training data is actually accessible
    try:
        response = requests.get(f"{BASE_URL}/api/v1/training")
        data = response.json()
        
        if "training_datasets" in data:
            if len(data["training_datasets"]) == 0:
                print("❌ FAIL: No training datasets found")
                if "note" in data:
                    print(f"Note: {data['note']}")
                return False
            else:
                print(f"✅ PASS: Found {len(data['training_datasets'])} training datasets")
        else:
            print("❌ FAIL: No training_datasets field in response")
            return False
    except Exception as e:
        print(f"❌ FAIL: Could not analyze training data - {e}")
        return False
    
    return success

def test_model_predictions():
    """Test model predictions functionality"""
    print(f"\n{'='*60}")
    print("TESTING MODEL PREDICTIONS")
    print('='*60)
    
    success = test_endpoint(
        f"{BASE_URL}/api/v1/prices",
        "Price Data (Model Input)",
        expected_fields=["price_data", "data_source"],
        should_not_contain=["mock"]
    )
    
    # Check if we have actual predictions vs just price data
    try:
        response = requests.get(f"{BASE_URL}/api/v1/prices")
        data = response.json()
        
        if "price_data" in data and len(data["price_data"]) > 0:
            sample_item = data["price_data"][0]
            
            # Check if this contains actual model predictions
            prediction_fields = ["prediction", "support_levels", "resistance_levels", "signal", "forecast"]
            has_predictions = any(field in sample_item for field in prediction_fields)
            
            if has_predictions:
                print("✅ PASS: Contains actual model predictions")
            else:
                print("❌ FAIL: Only contains price data, no model predictions")
                print(f"Sample item fields: {list(sample_item.keys())}")
                return False
        else:
            print("❌ FAIL: No price data found")
            return False
    except Exception as e:
        print(f"❌ FAIL: Could not analyze price data - {e}")
        return False
    
    return success

def test_database_connectivity():
    """Test real database connectivity"""
    print(f"\n{'='*60}")
    print("TESTING DATABASE CONNECTIVITY")
    print('='*60)
    
    success = test_endpoint(
        f"{BASE_URL}/health",
        "Health Check",
        expected_fields=["status", "database_connected", "data_summary"],
        should_not_contain=["mock"]
    )
    
    # Verify database is actually connected
    try:
        response = requests.get(f"{BASE_URL}/health")
        data = response.json()
        
        if not data.get("database_connected", False):
            print("❌ FAIL: Database not connected")
            return False
        
        if data.get("status") != "healthy":
            print(f"❌ FAIL: Status is {data.get('status')}, not healthy")
            return False
        
        # Check data counts
        if "data_summary" in data:
            instruments = data["data_summary"].get("active_instruments", 0)
            prices = data["data_summary"].get("price_records", 0)
            
            if instruments == 0:
                print("❌ FAIL: No instruments in database")
                return False
            
            if prices == 0:
                print("❌ FAIL: No price records in database")
                return False
            
            print(f"✅ PASS: Database has {instruments} instruments and {prices} price records")
        
    except Exception as e:
        print(f"❌ FAIL: Could not verify database connectivity - {e}")
        return False
    
    return success

def test_ui_functionality():
    """Test UI accessibility"""
    print(f"\n{'='*60}")
    print("TESTING UI FUNCTIONALITY")
    print('='*60)
    
    try:
        response = requests.get(BASE_URL, timeout=10)
        print(f"UI Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ FAIL: UI not accessible")
            return False
        
        html = response.text
        
        # Check for key UI elements
        required_elements = [
            "Real Data Analytics Dashboard",
            "NO MOCK DATA",
            "Portfolio Analytics",
            "Training Data",
            "Model Predictions"
        ]
        
        for element in required_elements:
            if element not in html:
                print(f"❌ FAIL: Missing UI element: {element}")
                return False
            print(f"✅ Found UI element: {element}")
        
        print("✅ PASS: UI is accessible and contains required elements")
        return True
        
    except Exception as e:
        print(f"❌ FAIL: UI test failed - {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 WEBAPP FUNCTIONALITY TEST SUITE")
    print("="*60)
    print(f"Testing webapp at: {BASE_URL}")
    print(f"Test time: {datetime.now()}")
    
    tests = [
        ("Database Connectivity", test_database_connectivity),
        ("UI Functionality", test_ui_functionality),
        ("Portfolio Analytics", test_portfolio_analytics),
        ("Training Data", test_training_data),
        ("Model Predictions", test_model_predictions)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ FAIL: {test_name} crashed - {e}")
            results[test_name] = False
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST RESULTS SUMMARY")
    print('='*60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED - Webapp is fully functional!")
        return 0
    else:
        print("⚠️  SOME TESTS FAILED - Webapp needs fixes")
        return 1

if __name__ == "__main__":
    sys.exit(main())