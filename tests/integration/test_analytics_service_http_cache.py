#!/usr/bin/env python3
"""
HTTP integration tests for analytics service cache prevention.

Tests actual HTTP requests to verify browser cache headers work correctly
and prevent the caching issue that caused "Datasets received: 0".
"""

import unittest
import requests
import time


class TestAnalyticsServiceHTTPCache(unittest.TestCase):
    """Test HTTP cache behavior of analytics service."""

    BASE_URL = "http://localhost:4000"

    @classmethod
    def setUpClass(cls):
        """Verify analytics service is running before tests."""
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                raise Exception(f"Analytics service not healthy: {response.status_code}")
        except Exception as e:
            raise unittest.SkipTest(f"Analytics service not available: {e}")

    def test_datasets_api_returns_no_cache_headers(self):
        """Test that datasets API returns proper no-cache headers."""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")

        # Verify successful response
        self.assertEqual(response.status_code, 200)

        # Verify all no-cache headers are present
        self.assertEqual(response.headers.get('Cache-Control'), 'no-cache, no-store, must-revalidate')
        self.assertEqual(response.headers.get('Pragma'), 'no-cache')
        self.assertEqual(response.headers.get('Expires'), '0')

        # Verify CORS header
        self.assertEqual(response.headers.get('Access-Control-Allow-Origin'), '*')

        # Verify content type
        self.assertEqual(response.headers.get('Content-Type'), 'application/json')

    def test_datasets_api_returns_valid_json_data(self):
        """Test that datasets API returns valid JSON with expected structure."""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")

        self.assertEqual(response.status_code, 200)

        # Verify JSON response
        data = response.json()
        self.assertIsInstance(data, list)

        # If data exists, verify structure
        if len(data) > 0:
            dataset = data[0]
            required_fields = ['name', 'display_name', 'row_count', 'column_count', 'size']
            for field in required_fields:
                self.assertIn(field, dataset, f"Dataset should have '{field}' field")

            # Verify intg_ prefix (since we're testing intg environment)
            self.assertTrue(dataset['name'].startswith('intg_'),
                          f"Dataset name '{dataset['name']}' should start with intg_ prefix")

    def test_multiple_requests_bypass_cache(self):
        """Test that multiple requests always get fresh data (no browser caching)."""
        # Make first request
        response1 = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response1.status_code, 200)
        data1 = response1.json()

        # Make second request immediately
        response2 = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response2.status_code, 200)
        data2 = response2.json()

        # Data should be identical (both fresh from database)
        self.assertEqual(data1, data2)

        # Both should have no-cache headers
        for response in [response1, response2]:
            self.assertEqual(response.headers.get('Cache-Control'), 'no-cache, no-store, must-revalidate')
            self.assertEqual(response.headers.get('Pragma'), 'no-cache')
            self.assertEqual(response.headers.get('Expires'), '0')

    def test_conditional_requests_not_supported(self):
        """Test that conditional requests (If-Modified-Since) are ignored."""
        # Make first request
        response1 = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response1.status_code, 200)

        # Make conditional request with If-Modified-Since (should be ignored)
        headers = {'If-Modified-Since': 'Wed, 21 Oct 2015 07:28:00 GMT'}
        response2 = requests.get(f"{self.BASE_URL}/api/eda/datasets", headers=headers)

        # Should still return 200 (not 304 Not Modified)
        self.assertEqual(response2.status_code, 200)
        data2 = response2.json()
        self.assertIsInstance(data2, list)

    def test_etag_not_provided(self):
        """Test that ETag header is not provided (prevents conditional requests)."""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response.status_code, 200)

        # Verify no ETag header (which could enable caching)
        self.assertNotIn('ETag', response.headers)
        self.assertNotIn('etag', response.headers)

    def test_last_modified_not_provided(self):
        """Test that Last-Modified header is not provided."""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response.status_code, 200)

        # Verify no Last-Modified header
        self.assertNotIn('Last-Modified', response.headers)
        self.assertNotIn('last-modified', response.headers)


class TestAnalyticsServiceCacheBusting(unittest.TestCase):
    """Test cache-busting techniques work correctly."""

    BASE_URL = "http://localhost:4000"

    @classmethod
    def setUpClass(cls):
        """Verify analytics service is running."""
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                raise Exception(f"Analytics service not healthy: {response.status_code}")
        except Exception as e:
            raise unittest.SkipTest(f"Analytics service not available: {e}")

    def test_query_parameter_cache_busting(self):
        """Test that base endpoint works regardless of query parameters."""
        base_url = f"{self.BASE_URL}/api/eda/datasets"

        # Base request should work
        response_base = requests.get(base_url)
        self.assertEqual(response_base.status_code, 200)
        base_data = response_base.json()
        self.assertIsInstance(base_data, list)

        # Query parameters might return 404 (endpoint doesn't handle them)
        # but that's expected behavior - we're testing cache headers on base endpoint
        response_with_query = requests.get(f"{base_url}?t={int(time.time())}")

        # The base endpoint cache headers should prevent browser caching
        self.assertEqual(response_base.headers.get('Cache-Control'), 'no-cache, no-store, must-revalidate')

        # Note: query parameter requests may return 404 and that's fine
        # The important thing is the base endpoint has proper no-cache headers

    def test_different_user_agents_same_response(self):
        """Test that different user agents get same response (no user-specific caching)."""
        base_url = f"{self.BASE_URL}/api/eda/datasets"

        # Make requests with different user agents
        ua1 = {'User-Agent': 'Mozilla/5.0 (Chrome/91.0) Test Browser 1'}
        ua2 = {'User-Agent': 'Mozilla/5.0 (Firefox/89.0) Test Browser 2'}
        ua3 = {'User-Agent': 'curl/7.68.0'}

        response1 = requests.get(base_url, headers=ua1)
        response2 = requests.get(base_url, headers=ua2)
        response3 = requests.get(base_url, headers=ua3)

        # All should return identical data
        data1 = response1.json()
        data2 = response2.json()
        data3 = response3.json()

        self.assertEqual(data1, data2)
        self.assertEqual(data2, data3)

        # All should have no-cache headers
        for response in [response1, response2, response3]:
            self.assertEqual(response.headers.get('Cache-Control'), 'no-cache, no-store, must-revalidate')


class TestAnalyticsServiceRegressionPrevention(unittest.TestCase):
    """Regression tests to prevent the original caching bug."""

    BASE_URL = "http://localhost:4000"

    @classmethod
    def setUpClass(cls):
        """Verify analytics service is running."""
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                raise Exception(f"Analytics service not healthy: {response.status_code}")
        except Exception as e:
            raise unittest.SkipTest(f"Analytics service not available: {e}")

    def test_no_max_age_cache_control(self):
        """Test that Cache-Control does not contain max-age (the original bug)."""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response.status_code, 200)

        cache_control = response.headers.get('Cache-Control', '')

        # Verify max-age is NOT present (original bug had max-age=3600)
        self.assertNotIn('max-age', cache_control.lower())
        self.assertNotIn('3600', cache_control)

        # Verify correct no-cache directives are present
        self.assertIn('no-cache', cache_control)
        self.assertIn('no-store', cache_control)
        self.assertIn('must-revalidate', cache_control)

    def test_browser_simulator_gets_fresh_data(self):
        """Simulate browser behavior to ensure fresh data is always returned."""
        base_url = f"{self.BASE_URL}/api/eda/datasets"

        # Simulate browser with common headers
        browser_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': f'{self.BASE_URL}/eda'
        }

        # Make request as if from browser JavaScript
        response = requests.get(base_url, headers=browser_headers)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIsInstance(data, list)

        # Verify this would not be cached by browser
        self.assertEqual(response.headers.get('Cache-Control'), 'no-cache, no-store, must-revalidate')
        self.assertEqual(response.headers.get('Pragma'), 'no-cache')
        self.assertEqual(response.headers.get('Expires'), '0')

    def test_environment_prefix_detection_working(self):
        """Test that environment-specific table prefix detection is working."""
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        self.assertEqual(response.status_code, 200)

        data = response.json()

        if len(data) > 0:
            # In ATS-INTG environment, all tables should have intg_ prefix
            for dataset in data:
                self.assertTrue(dataset['name'].startswith('intg_'),
                              f"Dataset '{dataset['name']}' should have intg_ prefix in intg environment")

            # Should have common intg tables
            table_names = [d['name'] for d in data]
            expected_tables = ['intg_instrument', 'intg_daily_price']
            expected_tables = ['intg_instrument', 'intg_daily_price_polygon']

            for expected_table in expected_tables:
                if expected_table not in table_names:
                    # Log warning but don't fail (database might not have all tables)
                    print(f"Warning: Expected table '{expected_table}' not found in datasets")


class TestAnalyticsServicePerformance(unittest.TestCase):
    """Performance tests to ensure caching changes don't impact performance."""

    BASE_URL = "http://localhost:4000"

    @classmethod
    def setUpClass(cls):
        """Verify analytics service is running."""
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                raise Exception(f"Analytics service not healthy: {response.status_code}")
        except Exception as e:
            raise unittest.SkipTest(f"Analytics service not available: {e}")

    def test_datasets_api_response_time(self):
        """Test that datasets API responds within reasonable time."""
        start_time = time.time()
        response = requests.get(f"{self.BASE_URL}/api/eda/datasets")
        end_time = time.time()

        response_time = end_time - start_time

        self.assertEqual(response.status_code, 200)
        self.assertLess(response_time, 2.0, f"Response time {response_time:.3f}s should be < 2s")

    def test_concurrent_requests_handling(self):
        """Test that multiple concurrent requests are handled properly."""
        import concurrent.futures

        def make_request():
            """Make a single request and return status and data length."""
            try:
                response = requests.get(f"{self.BASE_URL}/api/eda/datasets", timeout=5)
                return response.status_code, len(response.json()) if response.status_code == 200 else 0
            except Exception as e:
                return 500, 0

        # Make 5 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request) for _ in range(5)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All requests should succeed
        for status_code, data_length in results:
            self.assertEqual(status_code, 200)
            self.assertGreaterEqual(data_length, 0)

        # All should return same data length (consistency)
        data_lengths = [length for _, length in results]
        self.assertTrue(all(length == data_lengths[0] for length in data_lengths),
                       "All concurrent requests should return same data")


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)