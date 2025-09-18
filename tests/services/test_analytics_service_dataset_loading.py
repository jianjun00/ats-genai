#!/usr/bin/env python3
"""
Comprehensive test coverage for analytics service dataset loading issues.

Tests cover the two main issues that were discovered:
1. Hardcoded table prefix (dev_) not working in intg environment
2. Browser caching preventing updated responses from reaching frontend

This ensures these critical issues never happen again.
"""

import unittest
import os
import json
import time
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from domains.analytics.services.analytics_service import AnalyticsJobManager, get_cached_datasets, DATASET_CACHE

class TestAnalyticsServiceDatasetLoading(unittest.TestCase):
    """Test dataset loading functionality in analytics service."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear dataset cache before each test
        global DATASET_CACHE
        DATASET_CACHE['data'] = None
        DATASET_CACHE['timestamp'] = 0

        # Mock database connection
        self.mock_conn = Mock()
        self.mock_cursor = Mock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    def tearDown(self):
        """Clean up after tests."""
        # Reset environment variables
        if 'ENVIRONMENT' in os.environ:
            del os.environ['ENVIRONMENT']

class TestTablePrefixEnvironmentDetection(TestAnalyticsServiceDatasetLoading):
    """Test environment-aware table prefix detection."""

    @patch('services.analytics_service.get_raw_connection')
    def test_dev_environment_uses_dev_prefix(self, mock_get_connection):
        """Test that dev environment looks for dev_ tables."""
        os.environ['ENVIRONMENT'] = 'dev'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        # Mock database response for dev tables
        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'dev_instrument', 'size': '1 MB', 'schemaname': 'public'},
            {'tablename': 'dev_daily_price', 'size': '100 MB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 1000}

        job_manager = AnalyticsJobManager()
        datasets = job_manager.get_datasets()

        # Verify the query used dev_ prefix
        called_queries = [call[0][0] for call in self.mock_cursor.execute.call_args_list]
        table_query = next((q for q in called_queries if 'tablename LIKE' in q), None)
        self.assertIsNotNone(table_query, "Should have executed table prefix query")

        # Verify dev_ prefix was used in parameterized query
        table_query_params = [call[0][1] for call in self.mock_cursor.execute.call_args_list
                             if len(call[0]) > 1 and 'tablename LIKE' in call[0][0]]
        self.assertTrue(len(table_query_params) > 0, "Should have table prefix parameters")
        self.assertEqual(table_query_params[0][0], 'dev_%', "Should use dev_ prefix for dev environment")

        # Verify datasets were returned
        self.assertEqual(len(datasets), 2)
        self.assertEqual(datasets[0]['name'], 'dev_instrument')

    @patch('services.analytics_service.get_raw_connection')
    def test_intg_environment_uses_intg_prefix(self, mock_get_connection):
        """Test that intg environment looks for intg_ tables."""
        os.environ['ENVIRONMENT'] = 'intg'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        # Mock database response for intg tables
        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'intg_instrument', 'size': '2 MB', 'schemaname': 'public'},
            {'tablename': 'intg_daily_price', 'size': '200 MB', 'schemaname': 'public'},
            {'tablename': 'intg_daily_price_polygon', 'size': '200 MB', 'schemaname': 'public'},
            {'tablename': 'intg_comprehensive_backtest_runs', 'size': '50 KB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 5000}

        job_manager = AnalyticsJobManager()
        datasets = job_manager.get_datasets()

        # Verify the query used intg_ prefix
        table_query_params = [call[0][1] for call in self.mock_cursor.execute.call_args_list
                             if len(call[0]) > 1 and 'tablename LIKE' in call[0][0]]
        self.assertTrue(len(table_query_params) > 0, "Should have table prefix parameters")
        self.assertEqual(table_query_params[0][0], 'intg_%', "Should use intg_ prefix for intg environment")

        # Verify datasets were returned
        self.assertEqual(len(datasets), 3)
        self.assertEqual(datasets[0]['name'], 'intg_instrument')
        self.assertEqual(datasets[1]['name'], 'intg_daily_price')
        self.assertEqual(datasets[1]['name'], 'intg_daily_price_polygon')
        self.assertEqual(datasets[2]['name'], 'intg_comprehensive_backtest_runs')

    @patch('services.analytics_service.get_raw_connection')
    def test_missing_environment_defaults_to_dev(self, mock_get_connection):
        """Test that missing ENVIRONMENT variable defaults to dev."""
        # Ensure ENVIRONMENT is not set
        if 'ENVIRONMENT' in os.environ:
            del os.environ['ENVIRONMENT']

        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        self.mock_cursor.fetchall.return_value = []
        self.mock_cursor.fetchone.return_value = {'count': 0}

        job_manager = AnalyticsJobManager()
        job_manager.get_datasets()

        # Verify default dev_ prefix was used
        table_query_params = [call[0][1] for call in self.mock_cursor.execute.call_args_list
                             if len(call[0]) > 1 and 'tablename LIKE' in call[0][0]]
        self.assertTrue(len(table_query_params) > 0, "Should have table prefix parameters")
        self.assertEqual(table_query_params[0][0], 'dev_%', "Should default to dev_ prefix")

    @patch('services.analytics_service.get_raw_connection')
    def test_custom_environment_uses_custom_prefix(self, mock_get_connection):
        """Test that custom environment uses custom table prefix."""
        os.environ['ENVIRONMENT'] = 'test'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'test_sample_data', 'size': '10 KB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 100}

        job_manager = AnalyticsJobManager()
        datasets = job_manager.get_datasets()

        # Verify custom test_ prefix was used
        table_query_params = [call[0][1] for call in self.mock_cursor.execute.call_args_list
                             if len(call[0]) > 1 and 'tablename LIKE' in call[0][0]]
        self.assertTrue(len(table_query_params) > 0, "Should have table prefix parameters")
        self.assertEqual(table_query_params[0][0], 'test_%', "Should use test_ prefix for test environment")

        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0]['name'], 'test_sample_data')

class TestDatasetCaching(TestAnalyticsServiceDatasetLoading):
    """Test dataset caching functionality."""

    @patch('services.analytics_service.get_raw_connection')
    def test_cache_returns_fresh_data_on_first_call(self, mock_get_connection):
        """Test that first call fetches fresh data and caches it."""
        os.environ['ENVIRONMENT'] = 'dev'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'dev_test_table', 'size': '1 MB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 500}

        job_manager = AnalyticsJobManager()

        # First call should fetch from database
        datasets1 = get_cached_datasets(job_manager)
        self.assertEqual(len(datasets1), 1)

        # Verify data was cached
        self.assertIsNotNone(DATASET_CACHE['data'])
        self.assertEqual(len(DATASET_CACHE['data']), 1)
        self.assertGreater(DATASET_CACHE['timestamp'], 0)

    @patch('services.analytics_service.get_raw_connection')
    def test_cache_returns_cached_data_within_ttl(self, mock_get_connection):
        """Test that second call within TTL returns cached data."""
        os.environ['ENVIRONMENT'] = 'dev'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'dev_cached_table', 'size': '2 MB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 1000}

        job_manager = AnalyticsJobManager()

        # First call
        datasets1 = get_cached_datasets(job_manager)
        original_call_count = self.mock_cursor.execute.call_count

        # Second call should use cache
        datasets2 = get_cached_datasets(job_manager)

        # Verify no additional database calls were made
        self.assertEqual(self.mock_cursor.execute.call_count, original_call_count)

        # Verify same data returned
        self.assertEqual(datasets1, datasets2)
        self.assertEqual(len(datasets2), 1)

    @patch('services.analytics_service.get_raw_connection')
    def test_cache_refreshes_after_ttl_expires(self, mock_get_connection):
        """Test that cache refreshes after TTL expires."""
        os.environ['ENVIRONMENT'] = 'dev'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'dev_expired_table', 'size': '3 MB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 1500}

        job_manager = AnalyticsJobManager()

        # First call
        datasets1 = get_cached_datasets(job_manager)
        original_call_count = self.mock_cursor.execute.call_count

        # Artificially expire cache
        DATASET_CACHE['timestamp'] = time.time() - (DATASET_CACHE['ttl'] + 1)

        # Second call should refresh cache
        datasets2 = get_cached_datasets(job_manager)

        # Verify additional database calls were made
        self.assertGreater(self.mock_cursor.execute.call_count, original_call_count)

        # Verify data returned
        self.assertEqual(len(datasets2), 1)

class TestBrowserCacheHeaders(unittest.TestCase):
    """Test HTTP cache headers for preventing browser caching issues."""

    def setUp(self):
        """Set up test fixtures."""
        from domains.analytics.services.analytics_service import AnalyticsHandler

        # Mock HTTP components
        self.handler = AnalyticsHandler()
        self.handler.send_response = Mock()
        self.handler.send_header = Mock()
        self.handler.end_headers = Mock()
        self.handler.wfile = Mock()
        self.handler.path = '/api/eda/datasets'

        # Mock job manager
        mock_job_manager = Mock()
        mock_job_manager.get_datasets.return_value = [
            {'name': 'test_table', 'display_name': 'Test Table', 'row_count': 100, 'column_count': 5, 'size': '1 MB'}
        ]

        # Patch the job manager
        self.job_manager_patcher = patch('services.analytics_service.job_manager', mock_job_manager)
        self.job_manager_patcher.start()

        # Patch get_cached_datasets to return test data
        self.cache_patcher = patch('services.analytics_service.get_cached_datasets')
        self.mock_get_cached = self.cache_patcher.start()
        self.mock_get_cached.return_value = [
            {'name': 'test_table', 'display_name': 'Test Table', 'row_count': 100, 'column_count': 5, 'size': '1 MB'}
        ]

    def tearDown(self):
        """Clean up patches."""
        self.job_manager_patcher.stop()
        self.cache_patcher.stop()

    def test_datasets_api_sends_no_cache_headers(self):
        """Test that datasets API sends proper no-cache headers."""
        # Execute the GET request handler
        self.handler.do_GET()

        # Verify response status
        self.handler.send_response.assert_called_with(200)

        # Verify all required no-cache headers are sent
        expected_headers = [
            ('Content-type', 'application/json'),
            ('Access-Control-Allow-Origin', '*'),
            ('Cache-Control', 'no-cache, no-store, must-revalidate'),
            ('Pragma', 'no-cache'),
            ('Expires', '0')
        ]

        for header_name, header_value in expected_headers:
            self.handler.send_header.assert_any_call(header_name, header_value)

        # Verify end_headers was called
        self.handler.end_headers.assert_called_once()

    def test_datasets_api_prevents_browser_caching(self):
        """Test that the combination of headers prevents browser caching."""
        self.handler.do_GET()

        # Extract all cache-related headers
        cache_headers = {}
        for call in self.handler.send_header.call_args_list:
            header_name, header_value = call[0]
            if header_name.lower() in ['cache-control', 'pragma', 'expires']:
                cache_headers[header_name.lower()] = header_value

        # Verify comprehensive no-cache policy
        self.assertEqual(cache_headers.get('cache-control'), 'no-cache, no-store, must-revalidate')
        self.assertEqual(cache_headers.get('pragma'), 'no-cache')
        self.assertEqual(cache_headers.get('expires'), '0')

        # Verify no max-age is set (previous bug)
        self.assertNotIn('max-age', cache_headers.get('cache-control', ''))

    def test_datasets_api_returns_json_response(self):
        """Test that datasets API returns proper JSON response."""
        self.handler.do_GET()

        # Verify JSON data was written
        self.handler.wfile.write.assert_called_once()
        written_data = self.handler.wfile.write.call_args[0][0]

        # Verify it's valid JSON
        response_data = json.loads(written_data.decode('utf-8'))
        self.assertIsInstance(response_data, list)
        self.assertEqual(len(response_data), 1)
        self.assertEqual(response_data[0]['name'], 'test_table')

class TestDatabaseConnectionErrors(TestAnalyticsServiceDatasetLoading):
    """Test database connection error handling."""

    @patch('services.analytics_service.get_raw_connection')
    def test_database_connection_failure_handling(self, mock_get_connection):
        """Test proper handling of database connection failures."""
        # Simulate database connection failure
        mock_get_connection.side_effect = Exception("Database connection failed")

        job_manager = AnalyticsJobManager()

        with self.assertRaises(Exception) as context:
            job_manager.get_datasets()

        self.assertIn("Database connection failed", str(context.exception))

    @patch('services.analytics_service.get_raw_connection')
    def test_sql_execution_error_handling(self, mock_get_connection):
        """Test proper handling of SQL execution errors."""
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        # Simulate SQL execution error
        self.mock_cursor.execute.side_effect = Exception("SQL execution failed")

        job_manager = AnalyticsJobManager()

        with self.assertRaises(Exception) as context:
            job_manager.get_datasets()

        self.assertIn("SQL execution failed", str(context.exception))

class TestIntegrationScenarios(TestAnalyticsServiceDatasetLoading):
    """Integration tests for real-world scenarios."""

    @patch('services.analytics_service.get_raw_connection')
    def test_dev_to_intg_environment_switch_scenario(self, mock_get_connection):
        """Test switching from dev to intg environment (the actual bug scenario)."""
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        job_manager = AnalyticsJobManager()

        # Scenario 1: Start with dev environment (has dev_ tables)
        os.environ['ENVIRONMENT'] = 'dev'
        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'dev_instrument', 'size': '1 MB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 1000}

        dev_datasets = job_manager.get_datasets()
        self.assertEqual(len(dev_datasets), 1)
        self.assertEqual(dev_datasets[0]['name'], 'dev_instrument')

        # Reset mock calls
        self.mock_cursor.execute.reset_mock()

        # Scenario 2: Switch to intg environment (has intg_ tables)
        os.environ['ENVIRONMENT'] = 'intg'
        self.mock_cursor.fetchall.return_value = [
            {'tablename': 'intg_instrument', 'size': '2 MB', 'schemaname': 'public'},
            {'tablename': 'intg_daily_price', 'size': '200 MB', 'schemaname': 'public'}
            {'tablename': 'intg_daily_price_polygon', 'size': '200 MB', 'schemaname': 'public'}
        ]
        self.mock_cursor.fetchone.return_value = {'count': 5000}

        # Clear cache to force fresh query
        global DATASET_CACHE
        DATASET_CACHE['data'] = None
        DATASET_CACHE['timestamp'] = 0

        intg_datasets = job_manager.get_datasets()
        self.assertEqual(len(intg_datasets), 2)
        self.assertEqual(intg_datasets[0]['name'], 'intg_instrument')
        self.assertEqual(intg_datasets[1]['name'], 'intg_daily_price')
        self.assertEqual(intg_datasets[1]['name'], 'intg_daily_price_polygon')

        # Verify different prefixes were used
        table_query_params = [call[0][1] for call in self.mock_cursor.execute.call_args_list
                             if len(call[0]) > 1 and 'tablename LIKE' in call[0][0]]
        self.assertEqual(table_query_params[0][0], 'intg_%', "Should use intg_ prefix in intg environment")

    @patch('services.analytics_service.get_raw_connection')
    def test_empty_database_scenario(self, mock_get_connection):
        """Test behavior when database has no matching tables."""
        os.environ['ENVIRONMENT'] = 'prod'
        mock_get_connection.return_value.__enter__ = Mock(return_value=self.mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        # Empty database scenario
        self.mock_cursor.fetchall.return_value = []

        job_manager = AnalyticsJobManager()
        datasets = job_manager.get_datasets()

        self.assertEqual(len(datasets), 0)

        # Verify prod_ prefix was used
        table_query_params = [call[0][1] for call in self.mock_cursor.execute.call_args_list
                             if len(call[0]) > 1 and 'tablename LIKE' in call[0][0]]
        self.assertEqual(table_query_params[0][0], 'prod_%', "Should use prod_ prefix for prod environment")

if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)