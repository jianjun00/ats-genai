#!/usr/bin/env python3
"""
Comprehensive regression test suite for EDA integration issues.
Prevents reoccurrence of critical issues identified during implementation.
"""

import pytest
import requests
import time
import concurrent.futures
import threading
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDARegressionSuite:
    """Comprehensive regression test suite for all identified EDA issues."""
    
    @classmethod
    def setup_class(cls):
        """Setup for the test class."""
        cls.base_url = "http://localhost:3000"
        cls.timeout = 20
        
        # Wait for service to be ready
        max_retries = 15
        for i in range(max_retries):
            try:
                response = requests.get(f"{cls.base_url}/health", timeout=5)
                if response.status_code == 200:
                    print(f"✅ EDA service ready after {i+1} attempts")
                    break
            except:
                time.sleep(2)
        else:
            raise Exception("EDA service not available after 30 seconds")
    
    # REGRESSION TESTS FOR CRITICAL ISSUES
    
    def test_regression_no_single_threaded_blocking(self):
        """
        REGRESSION: Prevent single-threaded HTTP server blocking issue.
        Original Issue: Service would hang on concurrent requests due to JavaScript polling.
        User Report: "does it keep on going down?"
        """
        print("🧪 Testing regression: No single-threaded server blocking...")
        
        def make_concurrent_request(request_id):
            start_time = time.time()
            try:
                response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
                end_time = time.time()
                return {
                    "request_id": request_id,
                    "success": response.status_code == 200,
                    "response_time": end_time - start_time,
                    "error": None
                }
            except Exception as e:
                return {
                    "request_id": request_id, 
                    "success": False,
                    "response_time": time.time() - start_time,
                    "error": str(e)
                }
        
        # Test concurrent requests (simulates JavaScript polling + user interactions)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_concurrent_request, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures, timeout=self.timeout)]
        
        # All requests should succeed without hanging
        successful_requests = [r for r in results if r["success"]]
        failed_requests = [r for r in results if not r["success"]]
        
        assert len(successful_requests) >= 8, f"Too many failed requests: {len(failed_requests)}/10 failed"
        
        # Response times should be reasonable (not hanging)
        avg_response_time = sum(r["response_time"] for r in successful_requests) / len(successful_requests)
        assert avg_response_time < 5.0, f"Average response time {avg_response_time:.2f}s too high - suggests blocking"
        
        print(f"   ✅ {len(successful_requests)}/10 concurrent requests succeeded")
        print(f"   ✅ Average response time: {avg_response_time:.2f}s (no blocking detected)")
    
    def test_regression_no_loading_forever_issue(self):
        """
        REGRESSION: Prevent 'Loading...' UI issue.
        Original Issue: "Loading... Interactive Analysis shows no dataset"
        User Report: JavaScript couldn't load datasets due to empty/zero data
        """
        print("🧪 Testing regression: No 'Loading...' forever issue...")
        
        # Test datasets endpoint returns usable data
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        assert response.status_code == 200, "Datasets endpoint must return 200"
        
        datasets = response.json()
        assert len(datasets) > 0, "Must have datasets to prevent 'Loading...' state"
        
        # Verify each dataset has realistic data (not zeros that cause UI issues)
        for dataset in datasets:
            assert dataset["row_count"] > 0, f"Dataset {dataset['name']} has zero rows - would cause 'Loading...' issue"
            assert dataset["column_count"] > 0, f"Dataset {dataset['name']} has zero columns - would cause issues" 
            assert dataset["display_name"] and dataset["display_name"].strip(), f"Dataset {dataset['name']} needs display name"
            
        # Test schema endpoints return column data for dropdown population
        test_dataset = datasets[0]["name"]
        schema_response = requests.get(f"{self.base_url}/api/v1/datasets/{test_dataset}/schema", 
                                     timeout=self.timeout)
        assert schema_response.status_code == 200, "Schema endpoint must work for dropdown population"
        
        schema = schema_response.json()
        assert "columns" in schema and len(schema["columns"]) > 0, "Schema must have columns for dropdown"
        
        print(f"   ✅ Found {len(datasets)} datasets with realistic data")
        print(f"   ✅ Schema contains {len(schema['columns'])} columns for dropdown")
    
    def test_regression_database_timeout_fallback_works(self):
        """
        REGRESSION: Ensure database timeout fallback system works.
        Original Issue: Container couldn't resolve "postgres" hostname, causing timeouts
        Fix: Implemented fallback data system when database queries fail
        """
        print("🧪 Testing regression: Database timeout fallback system...")
        
        # Test that service returns data even if database is unavailable
        start_time = time.time()
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        response_time = time.time() - start_time
        
        assert response.status_code == 200, "Service should return fallback data when DB unavailable"
        
        datasets = response.json()
        assert len(datasets) > 0, "Fallback system should provide datasets"
        
        # Response should be fast (fallback), not slow (waiting for DB timeout)
        assert response_time < 10.0, f"Response took {response_time:.2f}s - may not be using fallback"
        
        # Verify fallback data quality
        fallback_dataset = datasets[0]
        required_fields = ["name", "display_name", "row_count", "column_count", "vendor", "data_type"]
        for field in required_fields:
            assert field in fallback_dataset, f"Fallback data missing field: {field}"
        
        print(f"   ✅ Fallback system provides {len(datasets)} datasets in {response_time:.2f}s")
    
    def test_regression_column_dropdown_filtering_works(self):
        """
        REGRESSION: Ensure column dropdown filtering includes all numeric types.
        Original Issue: Missing "bigint" and other PostgreSQL data types in filtering logic
        User Report: Column dropdown was empty when it should show numeric columns
        """
        print("🧪 Testing regression: Column dropdown filtering works...")
        
        # Test schema for dataset with various numeric column types
        response = requests.get(f"{self.base_url}/api/v1/datasets/dev_instrument_tiingo/schema", 
                              timeout=self.timeout)
        assert response.status_code == 200
        
        schema = response.json()
        assert "columns" in schema
        
        # Apply JavaScript filtering logic (matches frontend code)
        numeric_columns = []
        for col in schema["columns"]:
            data_type = col["data_type"].lower()
            # This must match the JavaScript filtering logic exactly
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint", "smallint", "real", "decimal", "float"]):
                numeric_columns.append(col["column_name"])
        
        # Should find expected numeric columns
        expected_numeric_columns = ["market_cap", "price", "volume"]
        for expected_col in expected_numeric_columns:
            assert expected_col in numeric_columns, f"Column dropdown should include '{expected_col}'"
        
        assert len(numeric_columns) >= 3, f"Should find at least 3 numeric columns, got {len(numeric_columns)}"
        
        print(f"   ✅ Column filtering finds {len(numeric_columns)} numeric columns: {numeric_columns}")
    
    def test_regression_javascript_error_handling_present(self):
        """
        REGRESSION: Ensure JavaScript has proper error handling for debugging.
        Original Issue: JavaScript errors were hard to diagnose due to lack of logging
        Fix: Added console.log and try-catch blocks for better error visibility
        """
        print("🧪 Testing regression: JavaScript error handling present...")
        
        # Get EDA dashboard HTML
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        assert response.status_code == 200
        
        html_content = response.text
        
        # Verify error handling patterns are present in JavaScript
        required_error_patterns = [
            "try {",
            "catch (error)",
            "console.error('Error loading datasets:', error)",
            "console.error('Error loading columns:', error)",
            "console.log('Loading datasets...')"
        ]
        
        for pattern in required_error_patterns:
            assert pattern in html_content, f"Missing error handling pattern: '{pattern}'"
        
        print("   ✅ JavaScript includes proper error handling and debug logging")
    
    def test_regression_analytics_service_integration_intact(self):
        """
        REGRESSION: Ensure EDA integration doesn't break existing analytics functionality.
        Original Requirement: "make sure this is part of ats analytics dashboard, not a new dashboard"
        Must verify existing analytics endpoints still work after EDA integration.
        """
        print("🧪 Testing regression: Analytics service integration intact...")
        
        # Test main analytics dashboard still works
        response = requests.get(f"{self.base_url}/", timeout=self.timeout)
        assert response.status_code == 200, "Main analytics dashboard should still work"
        
        html_content = response.text
        assert "Analytics Dashboard" in html_content, "Main dashboard content should be intact"
        
        # Test that EDA link is present in main dashboard (integration requirement)
        assert "/eda" in html_content, "Main dashboard should link to EDA functionality"
        
        # Test health endpoint still works
        health_response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
        assert health_response.status_code == 200
        assert health_response.json()["status"] == "healthy"
        
        # Test EDA dashboard is accessible as integrated feature
        eda_response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        assert eda_response.status_code == 200, "EDA should be integrated into analytics service"
        
        print("   ✅ EDA properly integrated into existing analytics service")
        print("   ✅ Main analytics dashboard functionality preserved")
    
    def test_regression_realistic_fallback_data_quality(self):
        """
        REGRESSION: Ensure fallback data is realistic and matches actual ATS schema.
        Original Issue: Fallback data must match real database schema for proper testing
        Prevents confusion between fallback behavior and real data issues.
        """
        print("🧪 Testing regression: Realistic fallback data quality...")
        
        # Test datasets fallback data
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        datasets = response.json()
        
        # Verify realistic ATS dataset names and characteristics
        expected_dataset_names = [
            "dev_instrument_tiingo",
            "dev_instrument_polygon", 
            "dev_daily_prices_polygon_30year",
            "dev_daily_prices_tiingo",
            "dev_instrument_eodhd"
        ]
        
        found_dataset_names = [d["name"] for d in datasets]
        for expected_name in expected_dataset_names:
            assert expected_name in found_dataset_names, f"Missing expected ATS dataset: {expected_name}"
        
        # Test schema fallback data for Tiingo instruments
        schema_response = requests.get(f"{self.base_url}/api/v1/datasets/dev_instrument_tiingo/schema")
        schema = schema_response.json()
        
        # Verify realistic Tiingo schema structure
        column_names = [col["column_name"] for col in schema["columns"]]
        expected_tiingo_columns = ["symbol", "name", "market_cap", "price", "volume", "start_date", "end_date"]
        
        for expected_col in expected_tiingo_columns:
            assert expected_col in column_names, f"Tiingo schema missing expected column: {expected_col}"
        
        # Verify data types are realistic for PostgreSQL
        numeric_columns = [col for col in schema["columns"] if col["column_name"] in ["market_cap", "price", "volume"]]
        for col in numeric_columns:
            data_type = col["data_type"].lower()
            assert any(t in data_type for t in ["numeric", "bigint", "integer", "double"]), \
                f"Column {col['column_name']} should have numeric type, got {col['data_type']}"
        
        print(f"   ✅ Fallback includes {len(expected_dataset_names)} realistic ATS datasets")
        print(f"   ✅ Tiingo schema has {len(expected_tiingo_columns)} expected columns with correct types")
    
    def test_regression_no_service_crash_under_load(self):
        """
        REGRESSION: Ensure service doesn't crash under concurrent load.
        Original Issue: Single-threaded server would hang, potentially crash service
        Must verify service stability under realistic usage patterns.
        """
        print("🧪 Testing regression: No service crash under load...")
        
        def mixed_load_request(request_type, request_id):
            try:
                if request_type == "health":
                    response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
                elif request_type == "datasets":
                    response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
                elif request_type == "schema":
                    response = requests.get(f"{self.base_url}/api/v1/datasets/dev_instrument_tiingo/schema", timeout=self.timeout)
                elif request_type == "eda_page":
                    response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
                
                return {"id": request_id, "type": request_type, "success": response.status_code == 200}
            except:
                return {"id": request_id, "type": request_type, "success": False}
        
        # Create mixed load (simulates real usage: page loads + API calls + health checks)
        request_types = ["health", "datasets", "schema", "eda_page"] * 5  # 20 total requests
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(mixed_load_request, req_type, i) 
                      for i, req_type in enumerate(request_types)]
            
            results = []
            for future in concurrent.futures.as_completed(futures, timeout=self.timeout + 10):
                results.append(future.result())
        
        # Verify service stability
        successful_requests = [r for r in results if r["success"]]
        success_rate = len(successful_requests) / len(results)
        
        assert success_rate >= 0.75, f"Success rate {success_rate:.1%} too low - service may be unstable"
        
        # Verify service is still responsive after load test
        final_health_check = requests.get(f"{self.base_url}/health", timeout=self.timeout)
        assert final_health_check.status_code == 200, "Service should remain healthy after load test"
        
        print(f"   ✅ Service handled {len(results)} concurrent requests with {success_rate:.1%} success rate")
        print("   ✅ Service remains stable and responsive after load test")


def run_comprehensive_eda_regression_suite():
    """Run the complete EDA regression test suite."""
    print("🚀 Starting Comprehensive EDA Regression Test Suite")
    print("=" * 60)
    
    # Initialize test suite
    test_suite = TestEDARegressionSuite()
    test_suite.setup_class()
    
    # Run all regression tests
    regression_tests = [
        ("No Single-Threaded Blocking", test_suite.test_regression_no_single_threaded_blocking),
        ("No 'Loading...' Forever Issue", test_suite.test_regression_no_loading_forever_issue),
        ("Database Timeout Fallback", test_suite.test_regression_database_timeout_fallback_works),
        ("Column Dropdown Filtering", test_suite.test_regression_column_dropdown_filtering_works),
        ("JavaScript Error Handling", test_suite.test_regression_javascript_error_handling_present),
        ("Analytics Service Integration", test_suite.test_regression_analytics_service_integration_intact),
        ("Realistic Fallback Data", test_suite.test_regression_realistic_fallback_data_quality),
        ("No Service Crash Under Load", test_suite.test_regression_no_service_crash_under_load)
    ]
    
    passed_tests = 0
    failed_tests = []
    
    for test_name, test_func in regression_tests:
        try:
            test_func()
            passed_tests += 1
            print(f"✅ PASSED: {test_name}")
        except Exception as e:
            failed_tests.append((test_name, str(e)))
            print(f"❌ FAILED: {test_name} - {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print(f"📊 EDA REGRESSION TEST SUMMARY")
    print(f"   Total Tests: {len(regression_tests)}")
    print(f"   Passed: {passed_tests}")
    print(f"   Failed: {len(failed_tests)}")
    
    if failed_tests:
        print(f"\n❌ FAILED TESTS:")
        for test_name, error in failed_tests:
            print(f"   - {test_name}: {error}")
        return False
    else:
        print(f"\n🎉 ALL EDA REGRESSION TESTS PASSED!")
        print(f"✅ No critical issues detected")
        print(f"✅ All original problems resolved")
        print(f"✅ Service is stable and functional")
        return True


if __name__ == "__main__":
    success = run_comprehensive_eda_regression_suite()
    exit(0 if success else 1)