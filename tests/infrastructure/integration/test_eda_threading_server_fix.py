#!/usr/bin/env python3
"""
Integration tests for EDA threading HTTP server fix.
Tests concurrent request handling to prevent service timeouts.
"""

import pytest
import requests
import time
import concurrent.futures
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDAThreadingServerFix:
    """Test suite for threading HTTP server functionality."""

    @classmethod
    def setup_class(cls):
        """Setup for the test class."""
        cls.base_url = "http://localhost:3000"
        cls.timeout = 30  # Increased timeout for threading tests

        # Wait for service to be ready
        max_retries = 10
        for i in range(max_retries):
            response = requests.get(f"{cls.base_url}/health", timeout=5)
            if response.status_code == 200:
                break
            raise Exception("EDA service not available after 20 seconds")

    def test_single_request_baseline(self):
        """Baseline test: single request should work normally."""
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        assert response.status_code == 200
        datasets = response.json()
        assert len(datasets) > 0

    def test_concurrent_datasets_requests(self):
        """Test concurrent requests to datasets endpoint don't block."""
        def make_datasets_request():
            response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
            assert response.status_code == 200
            return response.json()

        # Run 5 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_datasets_request) for _ in range(5)]

            # All requests should complete successfully
            results = []
            for future in concurrent.futures.as_completed(futures, timeout=self.timeout):
                result = future.result()
                results.append(result)
                assert len(result) > 0  # Should return datasets

        assert len(results) == 5, "All concurrent requests should complete"

    def test_concurrent_schema_requests(self):
        """Test concurrent schema requests don't cause timeouts."""
        dataset_name = "dev_instrument_tiingo"

        def make_schema_request():
            response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema",
                                  timeout=self.timeout)
            assert response.status_code == 200
            return response.json()

        # Run 3 concurrent schema requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(make_schema_request) for _ in range(3)]

            results = []
            for future in concurrent.futures.as_completed(futures, timeout=self.timeout):
                result = future.result()
                results.append(result)
                assert "columns" in result

        assert len(results) == 3, "All concurrent schema requests should complete"

    def test_mixed_concurrent_requests(self):
        """Test mixed concurrent requests (datasets + schema + health) don't interfere."""
        dataset_name = "dev_instrument_tiingo"

        def make_datasets_request():
            response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
            return ("datasets", response.status_code, len(response.json()) if response.status_code == 200 else 0)

        def make_schema_request():
            response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema",
                                  timeout=self.timeout)
            return ("schema", response.status_code, "columns" in response.json() if response.status_code == 200 else False)

        def make_health_request():
            response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
            return ("health", response.status_code, response.json().get("status") if response.status_code == 200 else None)

        # Run mixed concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            futures.extend([executor.submit(make_datasets_request) for _ in range(2)])
            futures.extend([executor.submit(make_schema_request) for _ in range(2)])
            futures.extend([executor.submit(make_health_request) for _ in range(2)])

            results = []
            for future in concurrent.futures.as_completed(futures, timeout=self.timeout):
                result = future.result()
                results.append(result)

        # Verify all requests completed successfully
        assert len(results) == 6, "All mixed concurrent requests should complete"

        datasets_results = [r for r in results if r[0] == "datasets"]
        schema_results = [r for r in results if r[0] == "schema"]
        health_results = [r for r in results if r[0] == "health"]

        # All should be successful
        assert all(r[1] == 200 for r in datasets_results), "All datasets requests should succeed"
        assert all(r[1] == 200 for r in schema_results), "All schema requests should succeed"
        assert all(r[1] == 200 for r in health_results), "All health requests should succeed"

        # Verify response content
        assert all(r[2] > 0 for r in datasets_results), "Datasets should return data"
        assert all(r[2] for r in schema_results), "Schemas should have columns"
        assert all(r[2] == "healthy" for r in health_results), "Health should be healthy"

    def test_rapid_sequential_requests_no_blocking(self):
        """Test rapid sequential requests don't cause blocking (simulates JavaScript polling)."""
        # This simulates the JavaScript frontend making rapid requests
        request_count = 10
        results = []

        start_time = time.time()

        for i in range(request_count):
            response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
            results.append((response.status_code, time.time() - start_time))

            # Small delay between requests (simulates JavaScript polling)
            time.sleep(0.1)

        end_time = time.time()
        total_time = end_time - start_time

        # All requests should succeed
        assert all(status == 200 for status, _ in results), "All rapid requests should succeed"

        # Total time should be reasonable (not blocked by previous requests)
        expected_min_time = request_count * 0.1  # Just the sleep delays
        expected_max_time = expected_min_time + 5  # Allow 5 seconds for actual request processing

        assert total_time >= expected_min_time, f"Total time {total_time:.2f}s too fast (expected >= {expected_min_time:.2f}s)"
        assert total_time <= expected_max_time, f"Total time {total_time:.2f}s too slow (expected <= {expected_max_time:.2f}s), suggests blocking"

    def test_concurrent_analysis_requests(self):
        """Test concurrent analysis requests (the heavy operations) don't block."""
        def make_analysis_request():
            payload = {
                "dataset_name": "dev_instrument_tiingo",
                "column": "market_cap"
            }
            response = requests.post(f"{self.base_url}/api/v1/analysis/distribution",
                                   json=payload, timeout=self.timeout)
            return response.status_code

        # Run 3 concurrent analysis requests (these are heavier operations)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(make_analysis_request) for _ in range(3)]

            results = []
            for future in concurrent.futures.as_completed(futures, timeout=self.timeout):
                status_code = future.result()
                results.append(status_code)

        # All should complete (may return 200 or error status, but shouldn't timeout)
        assert len(results) == 3, "All concurrent analysis requests should complete"
        print(f"Analysis request status codes: {results}")

    def test_server_resilience_under_load(self):
        """Test server remains responsive under moderate concurrent load."""
        def make_mixed_request(request_type):
            if request_type == "health":
                response = requests.get(f"{self.base_url}/health", timeout=self.timeout)
                return response.status_code == 200
            elif request_type == "datasets":
                response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
                return response.status_code == 200 and len(response.json()) > 0
            elif request_type == "schema":
                response = requests.get(f"{self.base_url}/api/v1/datasets/dev_instrument_tiingo/schema",
                                      timeout=self.timeout)
                return response.status_code == 200

        # Create moderate load: 15 requests of mixed types
        request_types = ["health", "datasets", "schema"] * 5

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(make_mixed_request, req_type) for req_type in request_types]

            successful_requests = 0
            for future in concurrent.futures.as_completed(futures, timeout=self.timeout + 10):
                if future.result():
                    successful_requests += 1

        # At least 80% of requests should succeed under load
        success_rate = successful_requests / len(request_types)
        assert success_rate >= 0.8, f"Success rate {success_rate:.2f} too low, server may be blocking"
        print(f"Server resilience test: {successful_requests}/{len(request_types)} requests succeeded ({success_rate:.1%})")


def test_threading_server_regression():
    """Regression test to ensure threading server is properly configured."""
    # This is a static code check that can be run without the service running
    sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')
    # We can't directly import the service due to its structure, so we check the file
    with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
        content = f.read()

    # Verify that ThreadingHTTPServer is used instead of HTTPServer
    assert "ThreadingHTTPServer" in content, "Service should use ThreadingHTTPServer for concurrent requests"
    assert "from http.server import" in content and "ThreadingHTTPServer" in content, "ThreadingHTTPServer should be imported"

    # Verify the server creation line
    server_creation_lines = [line for line in content.split('\n') if 'ThreadingHTTPServer' in line and 'server =' in line]
    assert len(server_creation_lines) > 0, "ThreadingHTTPServer should be instantiated"

    print("✅ Threading server regression check passed")

if __name__ == "__main__":
    # Run threading server tests
    test_suite = TestEDAThreadingServerFix()
    test_suite.setup_class()

    print("🧪 Testing EDA Threading Server Fix...")

    test_suite.test_single_request_baseline()
    print("✅ Single request baseline test passed")

    test_suite.test_concurrent_datasets_requests()
    print("✅ Concurrent datasets requests test passed")

    test_suite.test_concurrent_schema_requests()
    print("✅ Concurrent schema requests test passed")

    test_suite.test_mixed_concurrent_requests()
    print("✅ Mixed concurrent requests test passed")

    test_suite.test_rapid_sequential_requests_no_blocking()
    print("✅ Rapid sequential requests test passed")

    test_suite.test_concurrent_analysis_requests()
    print("✅ Concurrent analysis requests test passed")

    test_suite.test_server_resilience_under_load()
    print("✅ Server resilience under load test passed")

    test_threading_server_regression()

    print("\n🎉 All threading server tests passed!")
    print("✅ Service can handle concurrent requests without blocking")
    print("✅ JavaScript polling won't cause service timeouts")

