#!/usr/bin/env python3
"""
Comprehensive tests for DatasetService - core metadata management functionality
Tests database integration, file discovery, quality validation, and error handling.
"""

import unittest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime
import numpy as np

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.services.dataset_service import (
    DatasetService, DatasetMetadata, DatasetFileIterator
)

class TestDatasetService(unittest.TestCase):
    """Test suite for DatasetService core functionality."""

    def setUp(self):
        """Set up test environment with mock database."""
        # Mock database configuration
        self.mock_db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }

        # Create temporary directory for test files
        self.test_dir = tempfile.mkdtemp()

        # Sample dataset metadata for testing
        self.sample_metadata = DatasetMetadata(
            dataset_id=1,
            dataset_name='test_aapl_dataset',
            dataset_type='training',
            symbols=['AAPL'],
            total_sequences=1000,
            total_records=50000,
            feature_count=15,
            label_count=1,
            sequence_length=100,
            file_format='npy',
            base_directory=self.test_dir,
            file_paths=[os.path.join(self.test_dir, 'test_data.npy')],
            file_size_mb=25.5,
            data_quality_score=0.95,
            feature_completeness=0.98,
            label_completeness=0.99,
            technical_indicators=['RSI', 'MACD', 'BB'],
            timeframes=['1h', '1d'],
            date_range_start='2025-07-01',
            date_range_end='2025-07-31',
            creation_timestamp=datetime.now(),
            last_updated=datetime.now(),
            processing_config={'batch_size': 64, 'features': 'all'}
        )

    def tearDown(self):
        """Clean up test environment."""
        # Clean up temporary files
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    @patch('psycopg2.connect')
    def test_service_initialization_success(self, mock_connect):
        """Test successful service initialization with database connection."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        service = DatasetService(self.mock_db_config)

        self.assertIsNotNone(service)
        mock_connect.assert_called_once()
        self.assertEqual(service.db_config, self.mock_db_config)

    @patch('psycopg2.connect')
    def test_service_initialization_db_failure(self, mock_connect):
        """Test service initialization with database connection failure."""
        mock_connect.side_effect = Exception("Database connection failed")

        service = DatasetService(self.mock_db_config)

        # Service should initialize but mark database as unavailable
        self.assertIsNotNone(service)
        self.assertFalse(hasattr(service, 'connection') and service.connection)

    @patch('psycopg2.connect')
    def test_get_dataset_metadata_success(self, mock_connect):
        """Test retrieving dataset metadata from database."""
        # Mock database response
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Sample database row
        mock_cursor.fetchone.return_value = (
            1, 'test_aapl_dataset', 'training', ['AAPL'], 1000, 50000,
            15, 1, 100, 'npy', self.test_dir, 25.5, 0.95, 0.98, 0.99,
            ['RSI', 'MACD'], ['1h', '1d'], '2025-07-01', '2025-07-31',
            datetime.now(), {'batch_size': 64}
        )

        service = DatasetService(self.mock_db_config)
        metadata = service.get_dataset_metadata(1)

        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.dataset_id, 1)
        self.assertEqual(metadata.dataset_name, 'test_aapl_dataset')
        self.assertEqual(metadata.symbols, ['AAPL'])
        self.assertEqual(metadata.total_sequences, 1000)
        self.assertEqual(metadata.data_quality_score, 0.95)

    @patch('psycopg2.connect')
    def test_get_dataset_metadata_not_found(self, mock_connect):
        """Test retrieving non-existent dataset metadata."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_cursor.fetchone.return_value = None

        service = DatasetService(self.mock_db_config)
        metadata = service.get_dataset_metadata(999)

        self.assertIsNone(metadata)

    @patch('psycopg2.connect')
    def test_list_datasets_with_filters(self, mock_connect):
        """Test listing datasets with symbol and limit filters."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Mock multiple dataset results
        mock_cursor.fetchall.return_value = [
            (1, 'aapl_dataset_1', 'training', ['AAPL'], 1000, 50000, 15, 1, 100, 'npy', self.test_dir, 25.5, 0.95, 0.98, 0.99, ['RSI'], ['1h'], '2025-07-01', '2025-07-31', datetime.now(), {}),
            (2, 'aapl_dataset_2', 'training', ['AAPL'], 2000, 100000, 15, 1, 100, 'npy', self.test_dir, 50.0, 0.92, 0.97, 0.98, ['MACD'], ['1d'], '2025-06-01', '2025-06-30', datetime.now(), {})
        ]

        service = DatasetService(self.mock_db_config)
        datasets = service.list_datasets(symbols=['AAPL'], limit=10)

        self.assertEqual(len(datasets), 2)
        self.assertTrue(all(d.dataset_name.startswith('aapl_dataset_') for d in datasets))
        self.assertTrue(all('AAPL' in d.symbols for d in datasets))

    def test_create_file_iterator_with_memory_estimation(self):
        """Test creating file iterators with memory estimation."""
        # Create test file
        test_file = os.path.join(self.test_dir, 'test_data.npy')
        test_data = np.random.randn(1000, 10)
        np.save(test_file, test_data)

        service = DatasetService()
        iterator = service._create_file_iterator(test_file, record_count=1000)

        self.assertIsNotNone(iterator)
        self.assertEqual(iterator.file_path, test_file)
        self.assertEqual(iterator.record_count, 1000)
        self.assertGreater(iterator.estimated_memory_mb, 0)
        self.assertGreater(iterator.batch_size_recommendation, 0)

    @patch('os.path.exists')
    def test_validate_dataset_availability_all_accessible(self, mock_exists):
        """Test dataset validation when all files are accessible."""
        mock_exists.return_value = True

        service = DatasetService()
        file_paths = ['/path/to/file1.npy', '/path/to/file2.npy']

        validation = service._validate_file_accessibility(file_paths)

        self.assertTrue(validation['valid'])
        self.assertEqual(validation['accessible_files'], 2)
        self.assertEqual(validation['total_files'], 2)
        self.assertEqual(len(validation['missing_files']), 0)

    @patch('os.path.exists')
    def test_validate_dataset_availability_partial_accessible(self, mock_exists):
        """Test dataset validation when some files are missing."""
        def exists_side_effect(path):
            return 'file1' in path  # Only file1 exists

        mock_exists.side_effect = exists_side_effect

        service = DatasetService()
        file_paths = ['/path/to/file1.npy', '/path/to/file2.npy']

        validation = service._validate_file_accessibility(file_paths)

        self.assertFalse(validation['valid'])  # Not all files accessible
        self.assertEqual(validation['accessible_files'], 1)
        self.assertEqual(validation['total_files'], 2)
        self.assertEqual(len(validation['missing_files']), 1)
        self.assertIn('/path/to/file2.npy', validation['missing_files'])

    @patch('psycopg2.connect')
    def test_get_file_iterators_success(self, mock_connect):
        """Test creating file iterators from dataset."""
        # Setup mock database
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Create test files
        test_file1 = os.path.join(self.test_dir, 'data1.npy')
        test_file2 = os.path.join(self.test_dir, 'data2.npy')
        np.save(test_file1, np.random.randn(1000, 5))
        np.save(test_file2, np.random.randn(500, 5))

        # Mock file paths query
        mock_cursor.fetchall.return_value = [
            (test_file1,), (test_file2,)
        ]

        service = DatasetService(self.mock_db_config)
        iterators = service.get_file_iterators(1)

        self.assertEqual(len(iterators), 2)
        self.assertTrue(all(isinstance(it, DatasetFileIterator) for it in iterators))
        self.assertTrue(all(os.path.exists(it.file_path) for it in iterators))

    @patch('psycopg2.connect')
    def test_get_dataset_statistics_comprehensive(self, mock_connect):
        """Test retrieving comprehensive dataset statistics."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Mock dataset metadata
        mock_cursor.fetchone.return_value = (
            1, 'test_dataset', 'training', ['AAPL', 'TSLA'], 5000, 250000,
            20, 3, 100, 'npy', self.test_dir, 125.0, 0.93, 0.96, 0.98,
            ['RSI', 'MACD', 'BB'], ['5m', '1h', '1d'], '2025-07-01', '2025-07-31',
            datetime.now(), {'indicators': 'all', 'normalization': True}
        )

        # Mock file count
        mock_cursor.fetchall.return_value = [(3,)]  # 3 files

        service = DatasetService(self.mock_db_config)
        stats = service.get_dataset_statistics(1)

        self.assertIsNotNone(stats)

        # Verify structure
        self.assertIn('dataset_info', stats)
        self.assertIn('data_volume', stats)
        self.assertIn('data_quality', stats)
        self.assertIn('data_characteristics', stats)
        self.assertIn('processing_info', stats)

        # Verify content
        self.assertEqual(stats['dataset_info']['name'], 'test_dataset')
        self.assertEqual(stats['data_volume']['total_sequences'], 5000)
        self.assertEqual(stats['data_quality']['quality_score'], 0.93)
        self.assertIn('AAPL', stats['dataset_info']['symbols'])

    def test_file_iterator_config_generation(self):
        """Test DatasetFileIterator configuration generation."""
        test_file = os.path.join(self.test_dir, 'test.npy')
        test_data = np.random.randn(2000, 8)
        np.save(test_file, test_data)

        iterator = DatasetFileIterator(
            file_path=test_file,
            record_count=2000,
            file_size_bytes=15500000,  # ~15.5MB
            estimated_memory_mb=15.5,
            batch_size_recommendation=64
        )

        config = iterator.get_iterator_config()

        self.assertEqual(config['file_path'], test_file)
        self.assertEqual(config['record_count'], 2000)
        self.assertEqual(config['batch_size_recommendation'], 64)
        self.assertEqual(config['estimated_memory_mb'], 15.5)
        self.assertIn('processing_recommendations', config)

    @patch('psycopg2.connect')
    def test_search_datasets_by_name(self, mock_connect):
        """Test searching datasets by name pattern."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_cursor.fetchall.return_value = [
            (1, 'aapl_2025_training', 'training', ['AAPL'], 1000, 50000, 15, 1, 100, 'npy', self.test_dir, 25.0, 0.95, 0.98, 0.99, ['RSI'], ['1h'], '2025-07-01', '2025-07-31', datetime.now(), {})
        ]

        service = DatasetService(self.mock_db_config)
        results = service.search_datasets('aapl_2025')

        self.assertEqual(len(results), 1)
        self.assertIn('aapl_2025', results[0].dataset_name.lower())

    @patch('psycopg2.connect')
    def test_database_error_handling(self, mock_connect):
        """Test graceful handling of database errors."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Simulate database error
        mock_cursor.execute.side_effect = Exception("Database query failed")

        service = DatasetService(self.mock_db_config)
        metadata = service.get_dataset_metadata(1)

        # Should return None instead of crashing
        self.assertIsNone(metadata)

    def test_memory_estimation_accuracy(self):
        """Test memory estimation for different data types."""
        service = DatasetService()

        # Test different data sizes
        test_cases = [
            (1000, 10, np.float32),    # 1K records, 10 features, float32
            (10000, 50, np.float64),   # 10K records, 50 features, float64
            (100000, 5, np.int32),     # 100K records, 5 features, int32
        ]

        for records, features, dtype in test_cases:
            estimated_mb = service._estimate_memory_usage(records, features, dtype)

            # Memory estimation should be positive and reasonable
            self.assertGreater(estimated_mb, 0)
            self.assertLess(estimated_mb, 10000)  # Less than 10GB

            # Verify rough accuracy (within order of magnitude)
            expected_bytes = records * features * np.dtype(dtype).itemsize
            expected_mb = expected_bytes / (1024 * 1024)

            # Allow for 50% variance due to overhead
            self.assertLess(abs(estimated_mb - expected_mb), expected_mb * 0.5)

class TestDatasetFileIterator(unittest.TestCase):
    """Test suite for DatasetFileIterator functionality."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    def test_iterator_config_generation(self):
        """Test iterator configuration with processing recommendations."""
        test_file = os.path.join(self.test_dir, 'large_dataset.npy')
        large_data = np.random.randn(50000, 20)
        np.save(test_file, large_data)

        iterator = DatasetFileIterator(
            file_path=test_file,
            record_count=50000,
            file_size_bytes=400000000,  # ~400MB
            estimated_memory_mb=400.0,
            batch_size_recommendation=32
        )

        config = iterator.get_iterator_config()

        # Verify basic config
        self.assertEqual(config['file_path'], test_file)
        self.assertEqual(config['record_count'], 50000)
        self.assertEqual(config['batch_size_recommendation'], 32)

        # Verify processing recommendations for large dataset
        recommendations = config['processing_recommendations']
        self.assertTrue(recommendations['use_batch_loading'])
        self.assertFalse(recommendations['load_full_into_memory'])
        self.assertGreater(recommendations['recommended_sample_size'], 0)

    def test_small_dataset_recommendations(self):
        """Test processing recommendations for small datasets."""
        test_file = os.path.join(self.test_dir, 'small_dataset.npy')
        small_data = np.random.randn(100, 5)
        np.save(test_file, small_data)

        iterator = DatasetFileIterator(
            file_path=test_file,
            record_count=100,
            file_size_bytes=100000,  # ~100KB
            estimated_memory_mb=0.1,
            batch_size_recommendation=100
        )

        config = iterator.get_iterator_config()
        recommendations = config['processing_recommendations']

        # Small dataset should recommend full loading
        self.assertFalse(recommendations['use_batch_loading'])
        self.assertTrue(recommendations['load_full_into_memory'])

if __name__ == '__main__':
    unittest.main()