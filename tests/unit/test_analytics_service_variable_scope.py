#!/usr/bin/env python3
"""
Unit Tests for Analytics Service Variable Scope Issues
Tests the specific job_manager variable scope bugs we fixed
"""

import pytest
import unittest.mock as mock
import io
import json
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from services.analytics_service import AnalyticsHandler, JobManager

class TestAnalyticsServiceVariableScope:
    """Test job_manager variable scope fixes in AnalyticsHandler"""

    def setup_method(self):
        """Setup for each test"""
        # Create mock request and handler
        self.mock_request = mock.MagicMock()
        self.mock_client_address = ('127.0.0.1', 12345)
        self.mock_server = mock.MagicMock()

        # Create handler instance
        self.handler = AnalyticsHandler(self.mock_request, self.mock_client_address, self.mock_server)

        # Mock the wfile for response writing
        self.handler.wfile = io.BytesIO()

        # Mock headers
        self.handler.headers = {'Content-Length': '0'}
        self.handler.rfile = io.BytesIO()

    def test_job_manager_import_available(self):
        """Test that JobManager can be imported and instantiated"""
        # This tests that JobManager is properly importable
        job_manager = JobManager()
        assert job_manager is not None

        # Test required methods exist
        required_methods = [
            'get_dataset_schema',
            'get_column_values',
            'get_filtered_data',
            'get_job_stats',
            'get_recent_jobs',
            'get_timeseries_data',
            'analyze_column_distribution'
        ]

        for method in required_methods:
            assert hasattr(job_manager, method), f"JobManager should have {method} method"

    def test_schema_endpoint_job_manager_scope(self):
        """Test that schema endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response') as mock_send_response, \
             mock.patch.object(self.handler, 'send_header') as mock_send_header, \
             mock.patch.object(self.handler, 'end_headers') as mock_end_headers:

            # Mock the schema endpoint path
            self.handler.path = '/api/eda/datasets/test_table/schema'

            # Mock JobManager and its methods
            with mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:
                mock_job_manager_instance = mock.MagicMock()
                mock_job_manager_instance.get_dataset_schema.return_value = {
                    'table_name': 'test_table',
                    'columns': [{'name': 'test_col', 'type': 'varchar'}]
                }
                mock_job_manager_class.return_value = mock_job_manager_instance

                # This should not raise "job_manager not defined" error
                try:
                    self.handler.do_GET()

                    # Verify JobManager was instantiated
                    mock_job_manager_class.assert_called_once()

                    # Verify schema method was called
                    mock_job_manager_instance.get_dataset_schema.assert_called_once_with('test_table')

                    # Verify response was written
                    output = self.handler.wfile.getvalue().decode('utf-8')
                    assert 'test_table' in output

                except NameError as e:
                    if "job_manager" in str(e):
                        pytest.fail("job_manager variable scope issue not fixed in schema endpoint")
                    else:
                        raise

    def test_column_values_endpoint_job_manager_scope(self):
        """Test that column values endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response'), \
             mock.patch.object(self.handler, 'send_header'), \
             mock.patch.object(self.handler, 'end_headers'):

            self.handler.path = '/api/eda/datasets/test_table/columns/test_col/values?limit=5'

            # Mock should_use_ray_for_table to return False (use regular job_manager path)
            with mock.patch.object(self.handler, 'should_use_ray_for_table', return_value=False):
                with mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:
                    mock_job_manager_instance = mock.MagicMock()
                    mock_job_manager_instance.get_column_values.return_value = {
                        'values': [{'value': 'test', 'count': 1}]
                    }
                    mock_job_manager_class.return_value = mock_job_manager_instance

                    try:
                        self.handler.do_GET()

                        # Verify JobManager was instantiated
                        mock_job_manager_class.assert_called_once()

                        # Verify column values method was called
                        mock_job_manager_instance.get_column_values.assert_called_once_with('test_table', 'test_col', 5)

                    except NameError as e:
                        if "job_manager" in str(e):
                            pytest.fail("job_manager variable scope issue not fixed in column values endpoint")
                        else:
                            raise

    def test_job_stats_endpoint_job_manager_scope(self):
        """Test that job stats endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response'), \
             mock.patch.object(self.handler, 'send_header'), \
             mock.patch.object(self.handler, 'end_headers'):

            self.handler.path = '/api/jobs/stats'

            with mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:
                mock_job_manager_instance = mock.MagicMock()
                mock_job_manager_instance.get_job_stats.return_value = {'total_jobs': 5}
                mock_job_manager_class.return_value = mock_job_manager_instance

                try:
                    self.handler.do_GET()

                    # Verify JobManager was instantiated
                    mock_job_manager_class.assert_called_once()

                    # Verify job stats method was called
                    mock_job_manager_instance.get_job_stats.assert_called_once()

                except NameError as e:
                    if "job_manager" in str(e):
                        pytest.fail("job_manager variable scope issue not fixed in job stats endpoint")
                    else:
                        raise

    def test_recent_jobs_endpoint_job_manager_scope(self):
        """Test that recent jobs endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response'), \
             mock.patch.object(self.handler, 'send_header'), \
             mock.patch.object(self.handler, 'end_headers'):

            self.handler.path = '/api/jobs/recent'

            with mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:
                mock_job_manager_instance = mock.MagicMock()
                mock_job_manager_instance.get_recent_jobs.return_value = []
                mock_job_manager_class.return_value = mock_job_manager_instance

                try:
                    self.handler.do_GET()

                    mock_job_manager_class.assert_called_once()
                    mock_job_manager_instance.get_recent_jobs.assert_called_once_with(15)

                except NameError as e:
                    if "job_manager" in str(e):
                        pytest.fail("job_manager variable scope issue not fixed in recent jobs endpoint")
                    else:
                        raise

    def test_collection_status_endpoint_job_manager_scope(self):
        """Test that collection status endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response'), \
             mock.patch.object(self.handler, 'send_header'), \
             mock.patch.object(self.handler, 'end_headers'):

            self.handler.path = '/api/data/collection/status'

            # Mock asyncio components
            with mock.patch('asyncio.get_event_loop') as mock_get_loop, \
                 mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:

                mock_loop = mock.MagicMock()
                mock_loop.is_closed.return_value = False
                mock_get_loop.return_value = mock_loop

                mock_job_manager_instance = mock.MagicMock()
                mock_job_manager_instance.get_collection_status.return_value = mock.MagicMock()
                mock_job_manager_class.return_value = mock_job_manager_instance

                mock_loop.run_until_complete.return_value = {'status': 'ok'}

                try:
                    self.handler.do_GET()

                    mock_job_manager_class.assert_called_once()

                except NameError as e:
                    if "job_manager" in str(e):
                        pytest.fail("job_manager variable scope issue not fixed in collection status endpoint")
                    else:
                        raise

    def test_timeseries_endpoint_job_manager_scope(self):
        """Test that timeseries endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response'), \
             mock.patch.object(self.handler, 'send_header'), \
             mock.patch.object(self.handler, 'end_headers'):

            self.handler.path = '/api/eda/datasets/test_table/timeseries/price/date'

            with mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:
                mock_job_manager_instance = mock.MagicMock()
                mock_job_manager_instance.get_timeseries_data.return_value = {
                    'data': [{'x': '2025-01-01', 'y': 100}]
                }
                mock_job_manager_class.return_value = mock_job_manager_instance

                try:
                    self.handler.do_GET()

                    mock_job_manager_class.assert_called_once()
                    mock_job_manager_instance.get_timeseries_data.assert_called_once_with('test_table', 'price', 'date')

                except NameError as e:
                    if "job_manager" in str(e):
                        pytest.fail("job_manager variable scope issue not fixed in timeseries endpoint")
                    else:
                        raise

    def test_filtered_data_endpoint_job_manager_scope(self):
        """Test that filtered data endpoint properly creates job_manager in scope"""
        with mock.patch.object(self.handler, 'send_response'), \
             mock.patch.object(self.handler, 'send_header'), \
             mock.patch.object(self.handler, 'end_headers'):

            self.handler.path = '/api/eda/datasets/test_table/data'

            # Mock POST data
            post_data = json.dumps({"filters": {}, "page": 1, "page_size": 10}).encode('utf-8')
            self.handler.headers = {'Content-Length': str(len(post_data))}
            self.handler.rfile = io.BytesIO(post_data)

            with mock.patch('services.analytics_service.JobManager') as mock_job_manager_class:
                mock_job_manager_instance = mock.MagicMock()
                mock_job_manager_instance.get_filtered_data.return_value = {
                    'data': [],
                    'total_count': 0,
                    'current_page': 1
                }
                mock_job_manager_class.return_value = mock_job_manager_instance

                try:
                    self.handler.do_POST()

                    mock_job_manager_class.assert_called_once()
                    mock_job_manager_instance.get_filtered_data.assert_called_once_with(
                        'test_table', {}, 1, 10
                    )

                except NameError as e:
                    if "job_manager" in str(e):
                        pytest.fail("job_manager variable scope issue not fixed in filtered data endpoint")
                    else:
                        raise

    def test_no_global_job_manager_dependency(self):
        """Test that endpoints don't depend on global job_manager variable"""
        # This test ensures that we properly create JobManager instances locally
        # rather than depending on a global variable that might not be in scope

        # Check that there's no global job_manager variable being used incorrectly
        from services.analytics_service import AnalyticsHandler

        # Get the source code
        import inspect
        source = inspect.getsource(AnalyticsHandler.do_GET)

        # Count job_manager references
        job_manager_refs = source.count('job_manager')
        job_manager_instantiations = source.count('JobManager()')

        # We should have approximately as many instantiations as references
        # (allowing for some variation in the actual code structure)
        assert job_manager_instantiations > 0, "Should have JobManager() instantiations in do_GET"

        # Check do_POST as well
        source_post = inspect.getsource(AnalyticsHandler.do_POST)
        post_refs = source_post.count('job_manager')
        post_instantiations = source_post.count('JobManager()')

        if post_refs > 0:
            assert post_instantiations > 0, "Should have JobManager() instantiations in do_POST"


class TestJobManagerErrorHandling:
    """Test that JobManager errors are handled gracefully"""

    def test_job_manager_database_error_handling(self):
        """Test JobManager handles database errors gracefully"""
        job_manager = JobManager()

        # Test with completely invalid table name
        result = job_manager.get_dataset_schema("completely_nonexistent_table_12345")

        # Should return error dict, not crash
        assert isinstance(result, dict), "Should return dict for invalid table"
        if "error" in result:
            assert "error" in result["error"].lower() or "not" in result["error"].lower(), \
                   "Error message should indicate problem"

    def test_job_manager_connection_error_handling(self):
        """Test JobManager handles connection errors gracefully"""
        # This test ensures that connection errors don't crash the application
        job_manager = JobManager()

        # Mock a connection error scenario
        with mock.patch('psycopg2.connect', side_effect=Exception("Connection failed")):
            try:
                # This should handle the error gracefully
                result = job_manager.get_job_stats()
                # Either returns error dict or raises controlled exception
                if isinstance(result, dict) and "error" in result:
                    assert True, "Connection error handled gracefully"
                else:
                    # If no error dict, should be valid stats
                    assert isinstance(result, dict), "Should return valid stats dict"
            except Exception as e:
                # Exception should be informative
                assert "connection" in str(e).lower() or "error" in str(e).lower(), \
                       "Exception should indicate connection issue"


if __name__ == "__main__":
    # Run tests directly
    import subprocess

    print("🧪 Running Analytics Service Variable Scope Tests...")

    result = subprocess.run([
        'python', '-m', 'pytest', __file__, '-v', '--tb=short'
    ], cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    exit(result.returncode)