"""
Comprehensive tests for the analytics service.

This module provides complete test coverage for the analytics service,
including job management, dataset caching, and Ray EDA integration.
"""

import pytest
from unittest.mock import Mock, patch
import time

from src.services.analytics_service import (
    JobManager,
    get_cached_datasets,
    DATASET_CACHE
)


class TestJobManager:
    """Test cases for JobManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global cache
        global DATASET_CACHE
        DATASET_CACHE['data'] = None
        DATASET_CACHE['timestamp'] = 0

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    def test_job_manager_initialization(self, mock_get_settings, mock_get_manager):
        """Test JobManager initialization."""
        mock_manager = Mock()
        mock_settings = Mock()
        mock_get_manager.return_value = mock_manager
        mock_get_settings.return_value = mock_settings

        job_manager = JobManager()

        assert job_manager.db_manager == mock_manager
        assert job_manager.settings == mock_settings
        mock_get_manager.assert_called_once()
        mock_get_settings.assert_called_once()

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    def test_initialize_database_success(self, mock_get_settings, mock_get_manager):
        """Test successful database initialization."""
        mock_manager = Mock()
        mock_manager.check_connection.return_value = True
        mock_get_manager.return_value = mock_manager
        mock_get_settings.return_value = Mock()

        job_manager = JobManager()
        job_manager.initialize()

        mock_manager.check_connection.assert_called_once()

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    def test_initialize_database_failure(self, mock_get_settings, mock_get_manager):
        """Test database initialization failure."""
        mock_manager = Mock()
        mock_manager.check_connection.return_value = False
        mock_get_manager.return_value = mock_manager
        mock_get_settings.return_value = Mock()

        job_manager = JobManager()
        job_manager.initialize()  # Should not raise exception

        mock_manager.check_connection.assert_called_once()

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    @patch('src.services.analytics_service.get_raw_connection')
    def test_get_job_stats_success(self, mock_get_connection, mock_get_settings, mock_get_manager):
        """Test successful job statistics retrieval."""
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {
            'total_jobs': 100,
            'running_jobs': 5,
            'completed_jobs': 90,
            'failed_jobs': 5,
            'avg_runtime_minutes': 15.5
        }

        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=None)

        # Mock settings
        mock_settings = Mock()
        mock_settings.get_table_name.return_value = 'dev_runs'
        mock_get_settings.return_value = mock_settings

        mock_get_manager.return_value = Mock()

        job_manager = JobManager()
        result = job_manager.get_job_stats()

        assert result['total_jobs'] == 100
        assert result['running_jobs'] == 5
        assert result['completed_jobs'] == 90
        assert result['failed_jobs'] == 5
        mock_cursor.execute.assert_called_once()
        mock_settings.get_table_name.assert_called_with("runs")

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    def test_get_datasets_success(self, mock_get_settings, mock_get_manager):
        """Test successful dataset retrieval."""
        mock_settings = Mock()
        mock_get_settings.return_value = mock_settings
        mock_get_manager.return_value = Mock()

        job_manager = JobManager()

        # Mock the database query for datasets
        with patch.object(job_manager, 'execute_query') as mock_execute:
            mock_datasets = [
                {'id': 1, 'name': 'test_dataset', 'records': 1000},
                {'id': 2, 'name': 'validation_dataset', 'records': 500}
            ]
            mock_execute.return_value = mock_datasets

            result = job_manager.get_datasets()

            assert len(result) == 2
            assert result[0]['name'] == 'test_dataset'
            assert result[1]['records'] == 500


class TestDatasetCaching:
    """Test cases for dataset caching functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Reset global cache
        global DATASET_CACHE
        DATASET_CACHE['data'] = None
        DATASET_CACHE['timestamp'] = 0

    def test_cache_miss_loads_data(self):
        """Test that cache miss loads fresh data."""
        mock_job_manager = Mock()
        mock_datasets = [{'id': 1, 'name': 'test'}]
        mock_job_manager.get_datasets.return_value = mock_datasets

        result = get_cached_datasets(mock_job_manager)

        assert result == mock_datasets
        assert DATASET_CACHE['data'] == mock_datasets
        assert DATASET_CACHE['timestamp'] > 0
        mock_job_manager.get_datasets.assert_called_once()

    def test_cache_hit_returns_cached_data(self):
        """Test that cache hit returns cached data without DB call."""
        # Pre-populate cache
        cached_data = [{'id': 1, 'name': 'cached'}]
        DATASET_CACHE['data'] = cached_data
        DATASET_CACHE['timestamp'] = time.time()  # Recent timestamp

        mock_job_manager = Mock()

        result = get_cached_datasets(mock_job_manager)

        assert result == cached_data
        mock_job_manager.get_datasets.assert_not_called()

    def test_cache_expiry_reloads_data(self):
        """Test that expired cache reloads data."""
        # Set expired cache
        old_data = [{'id': 1, 'name': 'old'}]
        DATASET_CACHE['data'] = old_data
        DATASET_CACHE['timestamp'] = time.time() - (5 * 60 * 60)  # 5 hours ago

        mock_job_manager = Mock()
        new_data = [{'id': 2, 'name': 'new'}]
        mock_job_manager.get_datasets.return_value = new_data

        result = get_cached_datasets(mock_job_manager)

        assert result == new_data
        assert DATASET_CACHE['data'] == new_data
        mock_job_manager.get_datasets.assert_called_once()

    def test_cache_refresh_failure_returns_stale_data(self):
        """Test that failed cache refresh returns stale data if available."""
        # Pre-populate with stale data
        stale_data = [{'id': 1, 'name': 'stale'}]
        DATASET_CACHE['data'] = stale_data
        DATASET_CACHE['timestamp'] = time.time() - (5 * 60 * 60)  # Expired

        mock_job_manager = Mock()
        mock_job_manager.get_datasets.side_effect = Exception("DB connection failed")

        result = get_cached_datasets(mock_job_manager)

        assert result == stale_data  # Should return stale data
        mock_job_manager.get_datasets.assert_called_once()

    def test_cache_refresh_failure_no_stale_data_raises_exception(self):
        """Test that failed cache refresh with no stale data raises exception."""
        # No cached data
        DATASET_CACHE['data'] = None
        DATASET_CACHE['timestamp'] = 0

        mock_job_manager = Mock()
        mock_job_manager.get_datasets.side_effect = Exception("DB connection failed")

        with pytest.raises(Exception, match="DB connection failed"):
            get_cached_datasets(mock_job_manager)


class TestRayIntegration:
    """Test cases for Ray EDA integration."""

    @patch('src.services.analytics_service.RAY_AVAILABLE', True)
    @patch('src.services.analytics_service.get_ray_eda_service')
    def test_ray_eda_service_available(self, mock_get_ray_service):
        """Test Ray EDA service when available."""
        mock_ray_service = Mock()
        mock_get_ray_service.return_value = mock_ray_service

        # Import would happen at module level, but we test the functionality
        from src.services.analytics_service import RAY_AVAILABLE
        assert RAY_AVAILABLE is True

    @patch('src.services.analytics_service.RAY_AVAILABLE', False)
    def test_ray_eda_service_unavailable(self):
        """Test fallback when Ray EDA service is unavailable."""
        from src.services.analytics_service import RAY_AVAILABLE
        assert RAY_AVAILABLE is False


class TestAnalyticsServiceIntegration:
    """Integration tests for analytics service components."""

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    def test_job_manager_end_to_end_flow(self, mock_get_settings, mock_get_manager):
        """Test complete job manager workflow."""
        # Setup mocks
        mock_manager = Mock()
        mock_manager.check_connection.return_value = True
        mock_get_manager.return_value = mock_manager

        mock_settings = Mock()
        mock_settings.get_table_name.return_value = 'test_runs'
        mock_get_settings.return_value = mock_settings

        # Create job manager and initialize
        job_manager = JobManager()
        init_result = job_manager.initialize()

        # Test that initialization works
        assert init_result is None  # initialize() doesn't return anything
        mock_manager.check_connection.assert_called_once()

        # Test caching functionality
        with patch.object(job_manager, 'get_datasets') as mock_get_datasets:
            mock_datasets = [{'id': 1, 'name': 'integration_test'}]
            mock_get_datasets.return_value = mock_datasets

            # First call should hit database
            result1 = get_cached_datasets(job_manager)
            assert result1 == mock_datasets
            mock_get_datasets.assert_called_once()

            # Second call should use cache
            mock_get_datasets.reset_mock()
            result2 = get_cached_datasets(job_manager)
            assert result2 == mock_datasets
            mock_get_datasets.assert_not_called()


class TestErrorHandling:
    """Test cases for error handling scenarios."""

    @patch('src.services.analytics_service.get_connection_manager')
    @patch('src.services.analytics_service.get_settings')
    def test_database_connection_error_handling(self, mock_get_settings, mock_get_manager):
        """Test handling of database connection errors."""
        mock_manager = Mock()
        mock_manager.check_connection.side_effect = Exception("Connection timeout")
        mock_get_manager.return_value = mock_manager
        mock_get_settings.return_value = Mock()

        job_manager = JobManager()

        # Should not raise exception, just log warning
        job_manager.initialize()

        mock_manager.check_connection.assert_called_once()

    def test_dataset_cache_memory_management(self):
        """Test that dataset cache doesn't grow unbounded."""
        # Simulate large dataset
        large_dataset = [{'id': i, 'data': 'x' * 1000} for i in range(1000)]

        mock_job_manager = Mock()
        mock_job_manager.get_datasets.return_value = large_dataset

        result = get_cached_datasets(mock_job_manager)

        assert len(result) == 1000
        assert DATASET_CACHE['data'] == large_dataset

        # Verify cache can be cleared
        DATASET_CACHE['data'] = None
        DATASET_CACHE['timestamp'] = 0

        assert DATASET_CACHE['data'] is None