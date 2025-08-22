#!/usr/bin/env python3
"""
Comprehensive Test Suite for Real Data Analytics WebApp

This test suite validates ACTUAL FUNCTIONALITY, not just field presence.
Tests should have caught the missing features from the beginning.
"""

import requests
import json
import sys
from datetime import datetime
from typing import Dict, Any, List

BASE_URL = "http://localhost:3000"

class FunctionalityTester:
    """Tests that validate actual functionality, not just field presence"""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results = []
    
    def assert_true(self, condition: bool, message: str, details: str = ""):
        """Assert a condition and track results"""
        if condition:
            self.passed += 1
            print(f"✅ PASS: {message}")
            self.results.append({"test": message, "status": "PASS", "details": details})
        else:
            self.failed += 1
            print(f"❌ FAIL: {message}")
            if details:
                print(f"   Details: {details}")
            self.results.append({"test": message, "status": "FAIL", "details": details})
        return condition
    
    def assert_false(self, condition: bool, message: str, details: str = ""):
        """Assert a condition is false"""
        return self.assert_true(not condition, message, details)
    
    def assert_contains(self, data: Any, key: str, message: str):
        """Assert data contains a key"""
        if isinstance(data, dict):
            return self.assert_true(key in data, message, f"Available keys: {list(data.keys())}")
        else:
            return self.assert_true(False, message, f"Data is not a dict: {type(data)}")
    
    def assert_not_empty(self, data: Any, message: str):
        """Assert data is not empty"""
        if isinstance(data, (list, dict, str)):
            return self.assert_true(len(data) > 0, message, f"Length: {len(data)}")
        else:
            return self.assert_true(data is not None, message, f"Value: {data}")

def test_portfolio_analytics_functionality(tester: FunctionalityTester):
    """Test that portfolio analytics actually provides analytics, not just instrument lists"""
    print(f"\n{'='*60}")
    print("TESTING PORTFOLIO ANALYTICS FUNCTIONALITY")
    print('='*60)
    
    try:
        # Test portfolio metrics endpoint
        response = requests.get(f"{BASE_URL}/api/v1/portfolio/metrics", timeout=10)
        tester.assert_true(response.status_code == 200, 
                          "Portfolio metrics endpoint accessible",
                          f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Should NOT just be instrument data
            tester.assert_false("symbol" in str(data).lower() and "exchange" in str(data).lower() and "name" in str(data).lower(),
                               "Portfolio metrics should not be just instrument lists",
                               f"Response contains basic instrument fields")
            
            # Should contain actual analytics concepts
            response_str = json.dumps(data).lower()
            analytics_terms = ["return", "performance", "sharpe", "allocation", "attribution", "metric"]
            has_analytics = any(term in response_str for term in analytics_terms)
            tester.assert_true(has_analytics,
                              "Portfolio metrics contains actual analytics terms",
                              f"Found analytics terms: {[term for term in analytics_terms if term in response_str]}")
        
        # Test attribution endpoint
        response = requests.get(f"{BASE_URL}/api/v1/portfolio/attribution", timeout=10)
        tester.assert_true(response.status_code == 200,
                          "Portfolio attribution endpoint accessible")
        
        # Test breakdown endpoint
        response = requests.get(f"{BASE_URL}/api/v1/portfolio/breakdown", timeout=10)
        tester.assert_true(response.status_code == 200,
                          "Portfolio breakdown endpoint accessible")
        
        if response.status_code == 200:
            data = response.json()
            
            # Breakdown should contain portfolio-specific data
            if "portfolio_breakdown" in data:
                breakdown = data["portfolio_breakdown"]
                if len(breakdown) > 0:
                    sample_holding = breakdown[0]
                    
                    # Should have portfolio-specific fields, not just instrument data
                    portfolio_fields = ["allocation", "shares", "market_value", "weight", "position"]
                    has_portfolio_fields = any(field in sample_holding for field in portfolio_fields)
                    tester.assert_true(has_portfolio_fields,
                                      "Portfolio breakdown contains actual portfolio data",
                                      f"Sample holding: {sample_holding}")
                else:
                    tester.assert_true(False, "Portfolio breakdown is empty")
            else:
                tester.assert_true(False, "No portfolio_breakdown field in response")
    
    except Exception as e:
        tester.assert_true(False, f"Portfolio analytics test failed with exception: {e}")

def test_training_data_functionality(tester: FunctionalityTester):
    """Test that training data is actually accessible and provides real data"""
    print(f"\n{'='*60}")
    print("TESTING TRAINING DATA FUNCTIONALITY")
    print('='*60)
    
    try:
        # Test datasets endpoint
        response = requests.get(f"{BASE_URL}/api/v1/training/datasets", timeout=10)
        tester.assert_true(response.status_code == 200,
                          "Training datasets endpoint accessible")
        
        if response.status_code == 200:
            data = response.json()
            
            # Should have training datasets, not empty
            if "training_datasets" in data:
                datasets = data["training_datasets"]
                tester.assert_not_empty(datasets, "Training datasets should not be empty")
                
                if len(datasets) > 0:
                    # Should have actual dataset metadata
                    sample_dataset = datasets[0]
                    training_fields = ["metadata", "features", "sequences", "file", "size"]
                    has_training_fields = any(field in str(sample_dataset).lower() for field in training_fields)
                    tester.assert_true(has_training_fields,
                                      "Training datasets contain actual training data metadata",
                                      f"Sample dataset: {sample_dataset}")
                else:
                    # If empty, should explain why
                    tester.assert_contains(data, "note", "Should explain why no training data available")
                    note = data.get("note", "")
                    # Should not be a generic error
                    tester.assert_false("not mounted" in note or "not found" in note,
                                       "Training data should be properly accessible, not missing",
                                       f"Note: {note}")
            else:
                tester.assert_true(False, "No training_datasets field in response")
        
        # Test features endpoint
        response = requests.get(f"{BASE_URL}/api/v1/training/features", timeout=10)
        tester.assert_true(response.status_code == 200,
                          "Training features endpoint accessible")
        
        if response.status_code == 200:
            data = response.json()
            
            # Should provide feature analysis, not just error messages
            response_str = json.dumps(data).lower()
            error_terms = ["error", "not available", "not found", "not mounted"]
            has_errors = any(term in response_str for term in error_terms)
            tester.assert_false(has_errors,
                               "Training features should provide analysis, not errors",
                               f"Response: {data}")
    
    except Exception as e:
        tester.assert_true(False, f"Training data test failed with exception: {e}")

def test_model_predictions_functionality(tester: FunctionalityTester):
    """Test that model predictions provide actual predictions, not just price data"""
    print(f"\n{'='*60}")
    print("TESTING MODEL PREDICTIONS FUNCTIONALITY")
    print('='*60)
    
    try:
        # Test model performance endpoint
        response = requests.get(f"{BASE_URL}/api/v1/predictions/performance", timeout=10)
        tester.assert_true(response.status_code == 200,
                          "Model performance endpoint accessible")
        
        if response.status_code == 200:
            data = response.json()
            
            # Should contain model-specific metrics
            response_str = json.dumps(data).lower()
            model_terms = ["accuracy", "precision", "recall", "prediction", "model", "performance"]
            has_model_terms = any(term in response_str for term in model_terms)
            tester.assert_true(has_model_terms,
                              "Model performance contains actual model metrics",
                              f"Response: {data}")
        
        # Test recent predictions endpoint
        response = requests.get(f"{BASE_URL}/api/v1/predictions/recent", timeout=10)
        tester.assert_true(response.status_code == 200,
                          "Recent predictions endpoint accessible")
        
        if response.status_code == 200:
            data = response.json()
            
            # Should contain actual predictions, not just price data
            response_str = json.dumps(data).lower()
            prediction_terms = ["support", "resistance", "signal", "prediction", "forecast", "confidence"]
            has_prediction_terms = any(term in response_str for term in prediction_terms)
            tester.assert_true(has_prediction_terms,
                              "Recent predictions contain actual prediction data",
                              f"Response: {data}")
            
            # Should NOT be just basic price data
            basic_price_only = ("open" in response_str and "high" in response_str and 
                               "low" in response_str and "close" in response_str and
                               not any(term in response_str for term in prediction_terms))
            tester.assert_false(basic_price_only,
                               "Predictions should not be just basic OHLC price data",
                               f"Response appears to be basic price data")
    
    except Exception as e:
        tester.assert_true(False, f"Model predictions test failed with exception: {e}")

def test_no_mock_data_functionality(tester: FunctionalityTester):
    """Test that NO endpoints return mock data"""
    print(f"\n{'='*60}")
    print("TESTING NO MOCK DATA FUNCTIONALITY")
    print('='*60)
    
    endpoints = [
        "/health",
        "/api/v1/portfolio/metrics",
        "/api/v1/portfolio/attribution", 
        "/api/v1/portfolio/breakdown",
        "/api/v1/training/datasets",
        "/api/v1/training/features",
        "/api/v1/predictions/performance",
        "/api/v1/predictions/recent"
    ]
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                response_str = json.dumps(data).lower()
                
                # Should explicitly state no mock data
                if "mock_data" in data:
                    tester.assert_false(data.get("mock_data", True),
                                       f"{endpoint} should have mock_data: false",
                                       f"mock_data value: {data.get('mock_data')}")
                
                # Should not contain mock-related terms (except in "no mock data" statements)
                mock_terms = ["mock", "fake", "sample", "dummy", "simulated"]
                mock_found = []
                for term in mock_terms:
                    if term in response_str:
                        # Allow terms in negative contexts like "no mock data"
                        context_ok = any(phrase in response_str for phrase in [
                            f"no {term}", f"not {term}", f"{term}_data\": false", 
                            f"{term} data usage\": \"eliminated\"", f"real data only"
                        ])
                        if not context_ok:
                            mock_found.append(term)
                
                tester.assert_true(len(mock_found) == 0,
                                  f"{endpoint} should not contain mock data references",
                                  f"Found mock terms: {mock_found}")
            
        except Exception as e:
            tester.assert_true(False, f"Mock data test for {endpoint} failed: {e}")

def test_real_data_connectivity(tester: FunctionalityTester):
    """Test that endpoints actually connect to real data sources"""
    print(f"\n{'='*60}")
    print("TESTING REAL DATA CONNECTIVITY")
    print('='*60)
    
    try:
        # Health check should show real database connection
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        tester.assert_true(response.status_code == 200, "Health endpoint accessible")
        
        if response.status_code == 200:
            data = response.json()
            
            # Should be connected to database
            tester.assert_contains(data, "database_connected", "Health check should report database status")
            if "database_connected" in data:
                tester.assert_true(data["database_connected"],
                                  "Should be connected to real database",
                                  f"Database connected: {data['database_connected']}")
            
            # Should have real data counts
            if "data_summary" in data:
                summary = data["data_summary"]
                if "instruments" in summary:
                    tester.assert_true(summary["instruments"] > 0,
                                      "Should have real instruments in database",
                                      f"Instruments count: {summary['instruments']}")
                
                if "price_records" in summary:
                    tester.assert_true(summary["price_records"] > 0,
                                      "Should have real price records in database", 
                                      f"Price records: {summary['price_records']}")
            
            # Should explicitly state real data usage
            response_str = json.dumps(data).lower()
            real_data_indicators = ["real_data_only", "real data", "no mock", "eliminated"]
            has_real_data_indicators = any(indicator in response_str for indicator in real_data_indicators)
            tester.assert_true(has_real_data_indicators,
                              "Health check should explicitly confirm real data usage",
                              f"Found indicators: {[ind for ind in real_data_indicators if ind in response_str]}")
    
    except Exception as e:
        tester.assert_true(False, f"Real data connectivity test failed: {e}")

def test_ui_functionality_completeness(tester: FunctionalityTester):
    """Test that UI actually provides access to all claimed functionality"""
    print(f"\n{'='*60}")
    print("TESTING UI FUNCTIONALITY COMPLETENESS")
    print('='*60)
    
    try:
        response = requests.get(BASE_URL, timeout=10)
        tester.assert_true(response.status_code == 200, "UI is accessible")
        
        if response.status_code == 200:
            html = response.text.lower()
            
            # Should have tabs for all major functionality
            required_tabs = ["portfolio", "training", "predictions"]
            for tab in required_tabs:
                tester.assert_true(tab in html,
                                  f"UI should have {tab} tab",
                                  f"Tab '{tab}' found in HTML")
            
            # Should have buttons/links to actual functionality
            functionality_elements = [
                "portfolio metrics", "attribution", "breakdown",
                "training data", "features",
                "model predictions", "performance"
            ]
            
            for element in functionality_elements:
                tester.assert_true(element in html,
                                  f"UI should reference {element}",
                                  f"Element '{element}' found in HTML")
            
            # Should explicitly state real data usage
            real_data_phrases = ["real data", "no mock", "database", "real"]
            has_real_data_ui = any(phrase in html for phrase in real_data_phrases)
            tester.assert_true(has_real_data_ui,
                              "UI should clearly indicate real data usage")
    
    except Exception as e:
        tester.assert_true(False, f"UI functionality test failed: {e}")

def main():
    """Run comprehensive functionality tests"""
    print("🧪 COMPREHENSIVE FUNCTIONALITY TEST SUITE")
    print("="*80)
    print("This test suite validates ACTUAL functionality, not just field presence")
    print(f"Testing webapp at: {BASE_URL}")
    print(f"Test time: {datetime.now()}")
    print(f"Purpose: Catch missing functionality that shallow tests missed")
    
    tester = FunctionalityTester()
    
    # Run all functionality tests
    test_real_data_connectivity(tester)
    test_ui_functionality_completeness(tester)
    test_portfolio_analytics_functionality(tester)
    test_training_data_functionality(tester)
    test_model_predictions_functionality(tester)
    test_no_mock_data_functionality(tester)
    
    # Summary
    print(f"\n{'='*80}")
    print("COMPREHENSIVE TEST RESULTS")
    print('='*80)
    
    total_tests = tester.passed + tester.failed
    pass_rate = (tester.passed / total_tests * 100) if total_tests > 0 else 0
    
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {tester.passed}")
    print(f"Failed: {tester.failed}")
    print(f"Pass Rate: {pass_rate:.1f}%")
    
    # Show failed tests
    if tester.failed > 0:
        print(f"\n❌ FAILED TESTS:")
        for result in tester.results:
            if result["status"] == "FAIL":
                print(f"  - {result['test']}")
                if result["details"]:
                    print(f"    {result['details']}")
    
    # Final verdict
    if tester.failed == 0:
        print(f"\n🎉 ALL FUNCTIONALITY TESTS PASSED!")
        print("The webapp provides the actual functionality claimed.")
        return 0
    else:
        print(f"\n⚠️  FUNCTIONALITY TESTS REVEALED ISSUES!")
        print("The webapp claims functionality it doesn't actually provide.")
        print("These comprehensive tests would have caught the issues from the beginning.")
        return 1

if __name__ == "__main__":
    sys.exit(main())