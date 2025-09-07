#!/usr/bin/env python3
"""
Comprehensive tests for DatasetClient - high-level interface for training and EDA
Tests client integration, data loading, validation, and error handling.
"""

import unittest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import numpy as np
import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.clients.dataset_client import DatasetClient, DatasetLoader
from src.services.dataset_service import DatasetMetadata, DatasetFileIterator

class TestDatasetClient(unittest.TestCase):
    """Test suite for DatasetClient functionality."""

    def setUp(self):
        """Set up test environment with mock service."""
        self.test_dir = tempfile.mkdtemp()

        # Mock dataset metadata
        self.sample_metadata = DatasetMetadata(
            dataset_id=1,
            dataset_name='test_aapl_training',
            dataset_type='training',
            symbols=['AAPL'],
            total_sequences=5000,
            total_records=100000,
            feature_count=12,
            label_count=1,
            sequence_length=100,
            file_format='npy',
            base_directory=self.test_dir,
            file_paths=[os.path.join(self.test_dir, 'aapl_training.npy')],
            file_size_mb=50.0,
            data_quality_score=0.92,
            feature_completeness=0.95,
            label_completeness=0.98,
            technical_indicators=['RSI', 'MACD', 'BB'],
            timeframes=['1h', '1d'],
            date_range_start='2025-07-01',
            date_range_end='2025-07-31',
            creation_timestamp=datetime.now(),
            processing_config={'batch_size': 64}
        )

        # Create test data file
        self.test_data_file = os.path.join(self.test_dir, 'aapl_training.npy')
        test_data = np.random.randn(1000, 13)  # 12 features + 1 label
        np.save(self.test_data_file, test_data)

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    @patch('src.services.dataset_service.DatasetService')
    def test_client_initialization_success(self, mock_service_class):
        """Test successful client initialization."""
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        client = DatasetClient()

        self.assertIsNotNone(client)
        self.assertIsNotNone(client.service)
        mock_service_class.assert_called_once_with(None)

    @patch('src.services.dataset_service.DatasetService')
    def test_find_dataset_by_name(self, mock_service_class):
        """Test finding dataset by exact name."""
        mock_service = Mock()
        mock_service.get_dataset_by_name.return_value = self.sample_metadata
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        dataset = client.find_dataset(name='test_aapl_training')

        self.assertIsNotNone(dataset)
        self.assertEqual(dataset.dataset_name, 'test_aapl_training')
        self.assertEqual(dataset.symbols, ['AAPL'])
        mock_service.get_dataset_by_name.assert_called_once_with('test_aapl_training')

    @patch('src.services.dataset_service.DatasetService')
    def test_find_dataset_by_symbols_with_filters(self, mock_service_class):
        """Test finding dataset by symbols with quality and sequence filters."""
        mock_service = Mock()

        # Create multiple candidate datasets with different qualities
        candidate1 = self.sample_metadata  # Quality 0.92, 5000 sequences

        candidate2 = DatasetMetadata(
            dataset_id=2, dataset_name='low_quality_dataset', dataset_type='training',
            symbols=['AAPL'], total_sequences=8000, total_records=200000, feature_count=12, label_count=1,
            sequence_length=100, file_format='npy', base_directory=self.test_dir, file_paths=[],
            file_size_mb=80.0, data_quality_score=0.65, feature_completeness=0.80, label_completeness=0.85,
            technical_indicators=['RSI'], timeframes=['1h'], date_range_start='2025-06-01', date_range_end='2025-06-30',
            creation_timestamp=datetime.now(), processing_config={}
        )

        mock_service.list_datasets.return_value = [candidate1, candidate2]
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        dataset = client.find_dataset(symbols=['AAPL'], min_sequences=3000, min_quality=0.8)

        # Should return the high-quality dataset that meets criteria
        self.assertIsNotNone(dataset)
        self.assertEqual(dataset.dataset_id, 1)  # candidate1
        self.assertGreaterEqual(dataset.data_quality_score, 0.8)
        self.assertGreaterEqual(dataset.total_sequences, 3000)

    @patch('src.services.dataset_service.DatasetService')
    def test_find_dataset_no_matches(self, mock_service_class):
        """Test finding dataset when no matches meet criteria."""
        mock_service = Mock()

        # Return dataset that doesn't meet quality threshold
        low_quality_dataset = DatasetMetadata(
            dataset_id=3, dataset_name='poor_dataset', dataset_type='training',
            symbols=['AAPL'], total_sequences=2000, total_records=50000, feature_count=5, label_count=1,
            sequence_length=50, file_format='npy', base_directory=self.test_dir, file_paths=[],
            file_size_mb=10.0, data_quality_score=0.60, feature_completeness=0.70, label_completeness=0.75,
            technical_indicators=['RSI'], timeframes=['1h'], date_range_start='2025-05-01', date_range_end='2025-05-31',
            creation_timestamp=datetime.now(), processing_config={}
        )

        mock_service.list_datasets.return_value = [low_quality_dataset]
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        dataset = client.find_dataset(symbols=['AAPL'], min_sequences=5000, min_quality=0.9)

        self.assertIsNone(dataset)

    @patch('src.services.dataset_service.DatasetService')
    def test_get_training_data_config_success(self, mock_service_class):
        """Test getting training data configuration."""
        mock_service = Mock()
        mock_service.list_datasets.return_value = [self.sample_metadata]

        # Mock file iterators
        file_iterator = DatasetFileIterator(
            file_path=self.test_data_file,
            record_count=1000,
            batch_size_recommendation=64,
            estimated_memory_mb=10.0
        )
        mock_service.get_file_iterators.return_value = [file_iterator]

        # Mock validation
        mock_service.validate_dataset_availability.return_value = {
            'valid': True,
            'accessible_files': 1,
            'total_files': 1,
            'missing_files': []
        }

        mock_service_class.return_value = mock_service

        client = DatasetClient()
        config = client.get_training_data_config(symbols=['AAPL'], min_sequences=1000)

        self.assertIsNotNone(config)
        self.assertEqual(config['dataset_id'], 1)
        self.assertEqual(config['dataset_name'], 'test_aapl_training')
        self.assertEqual(config['symbols'], ['AAPL'])
        self.assertEqual(config['total_sequences'], 5000)
        self.assertEqual(config['feature_count'], 12)
        self.assertEqual(config['sequence_length'], 100)
        self.assertEqual(config['batch_size_recommendation'], 64)
        self.assertEqual(config['file_format'], 'npy')
        self.assertIn('date_range', config)
        self.assertIn('technical_indicators', config)

    @patch('src.services.dataset_service.DatasetService')
    def test_get_training_data_config_no_dataset(self, mock_service_class):
        """Test getting training config when no suitable dataset exists."""
        mock_service = Mock()
        mock_service.list_datasets.return_value = []
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        config = client.get_training_data_config(symbols=['NONEXISTENT'], min_sequences=1000)

        self.assertIsNone(config)

    @patch('src.services.dataset_service.DatasetService')
    def test_get_training_data_config_validation_failed(self, mock_service_class):
        """Test getting training config when file validation fails."""
        mock_service = Mock()
        mock_service.list_datasets.return_value = [self.sample_metadata]
        mock_service.get_file_iterators.return_value = [
            DatasetFileIterator(
                file_path='/nonexistent/file.npy',
                record_count=1000,
                batch_size_recommendation=64,
                estimated_memory_mb=10.0
            )
        ]

        # Mock failed validation
        mock_service.validate_dataset_availability.return_value = {
            'valid': False,
            'accessible_files': 0,
            'total_files': 1,
            'missing_files': ['/nonexistent/file.npy']
        }

        mock_service_class.return_value = mock_service

        client = DatasetClient()
        config = client.get_training_data_config(symbols=['AAPL'], min_sequences=1000)

        self.assertIsNone(config)

    @patch('src.services.dataset_service.DatasetService')
    def test_validate_dataset_for_training_success(self, mock_service_class):
        """Test dataset validation for training requirements."""
        mock_service = Mock()
        mock_service.get_dataset_metadata.return_value = self.sample_metadata
        mock_service.validate_dataset_availability.return_value = {
            'valid': True,
            'accessible_files': 1,
            'total_files': 1,
            'missing_files': []
        }
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        validation = client.validate_dataset_for_training(
            dataset_id=1,
            required_features=10,
            min_sequences=1000
        )

        self.assertTrue(validation['valid'])
        self.assertEqual(validation['dataset_name'], 'test_aapl_training')

        checks = validation['checks']
        self.assertTrue(checks['files_accessible'])
        self.assertTrue(checks['sufficient_sequences'])
        self.assertTrue(checks['sufficient_features'])
        self.assertTrue(checks['good_quality'])
        self.assertTrue(checks['has_labels'])

    @patch('src.services.dataset_service.DatasetService')
    def test_validate_dataset_for_training_insufficient_data(self, mock_service_class):
        """Test dataset validation when requirements not met."""
        # Create metadata with insufficient sequences and features
        insufficient_metadata = DatasetMetadata(
            dataset_id=2, dataset_name='insufficient_dataset', dataset_type='training',
            symbols=['AAPL'], total_sequences=500, total_records=10000, feature_count=3, label_count=0,  # No labels!
            sequence_length=50, file_format='npy', base_directory=self.test_dir, file_paths=[],
            file_size_mb=5.0, data_quality_score=0.60, feature_completeness=0.70, label_completeness=0.0,
            technical_indicators=['RSI'], timeframes=['1h'], date_range_start='2025-01-01', date_range_end='2025-01-31',
            creation_timestamp=datetime.now(), processing_config={}
        )

        mock_service = Mock()
        mock_service.get_dataset_metadata.return_value = insufficient_metadata
        mock_service.validate_dataset_availability.return_value = {
            'valid': True,
            'accessible_files': 1,
            'total_files': 1,
            'missing_files': []
        }
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        validation = client.validate_dataset_for_training(
            dataset_id=2,
            required_features=10,
            min_sequences=1000
        )

        self.assertFalse(validation['valid'])

        checks = validation['checks']
        self.assertTrue(checks['files_accessible'])
        self.assertFalse(checks['sufficient_sequences'])  # Only 500 < 1000
        self.assertFalse(checks['sufficient_features'])   # Only 3 < 10
        self.assertFalse(checks['good_quality'])          # 0.6 < 0.7
        self.assertFalse(checks['has_labels'])            # No labels

    @patch('src.services.dataset_service.DatasetService')
    def test_list_available_datasets(self, mock_service_class):
        """Test listing available datasets with summaries."""
        # Create multiple datasets
        dataset2 = DatasetMetadata(
            dataset_id=2, dataset_name='tsla_dataset', dataset_type='training',
            symbols=['TSLA'], total_sequences=3000, total_records=75000, feature_count=15, label_count=1,
            sequence_length=100, file_format='parquet', base_directory=self.test_dir, file_paths=[],
            file_size_mb=35.0, data_quality_score=0.88, feature_completeness=0.92, label_completeness=0.96,
            technical_indicators=['RSI', 'MACD'], timeframes=['1h', '4h', '1d'],
            date_range_start='2025-07-01', date_range_end='2025-07-31',
            creation_timestamp=datetime.now(), processing_config={}
        )

        mock_service = Mock()
        mock_service.list_datasets.return_value = [self.sample_metadata, dataset2]

        # Mock statistics for summaries
        def mock_get_statistics(dataset_id):
            if dataset_id == 1:
                return {
                    'dataset_info': {'name': 'test_aapl_training', 'symbols': ['AAPL'], 'creation_date': '2025-07-01T10:00:00'},
                    'data_volume': {'total_sequences': 5000, 'estimated_memory_mb': 50.0, 'file_count': 1},
                    'data_quality': {'quality_score': 0.92},
                    'data_characteristics': {'timeframes': ['1h', '1d'], 'technical_indicators': ['RSI', 'MACD', 'BB']},
                    'processing_info': {'date_range': '2025-07-01 to 2025-07-31'}
                }
            elif dataset_id == 2:
                return {
                    'dataset_info': {'name': 'tsla_dataset', 'symbols': ['TSLA'], 'creation_date': '2025-07-01T11:00:00'},
                    'data_volume': {'total_sequences': 3000, 'estimated_memory_mb': 35.0, 'file_count': 2},
                    'data_quality': {'quality_score': 0.88},
                    'data_characteristics': {'timeframes': ['1h', '4h', '1d'], 'technical_indicators': ['RSI', 'MACD']},
                    'processing_info': {'date_range': '2025-07-01 to 2025-07-31'}
                }

        mock_service.get_dataset_statistics.side_effect = mock_get_statistics
        mock_service_class.return_value = mock_service

        client = DatasetClient()
        summaries = client.list_available_datasets(symbols=['AAPL', 'TSLA'])

        self.assertEqual(len(summaries), 2)

        # Check first summary (AAPL)
        aapl_summary = next(s for s in summaries if s['id'] == 1)
        self.assertEqual(aapl_summary['name'], 'test_aapl_training')
        self.assertEqual(aapl_summary['symbols'], 'AAPL')
        self.assertEqual(aapl_summary['size'], '5,000 sequences')
        self.assertEqual(aapl_summary['quality'], '92.0%')
        self.assertEqual(aapl_summary['memory'], '50 MB')

        # Check second summary (TSLA)
        tsla_summary = next(s for s in summaries if s['id'] == 2)
        self.assertEqual(tsla_summary['name'], 'tsla_dataset')
        self.assertEqual(tsla_summary['symbols'], 'TSLA')
        self.assertEqual(tsla_summary['size'], '3,000 sequences')

class TestDatasetLoader(unittest.TestCase):
    """Test suite for DatasetLoader functionality."""

    def setUp(self):
        """Set up test environment with sample data files."""
        self.test_dir = tempfile.mkdtemp()

        # Create numpy test file
        self.npy_file = os.path.join(self.test_dir, 'test_data.npy')
        self.npy_data = np.random.randn(1000, 6)  # 5 features + 1 label
        np.save(self.npy_file, self.npy_data)

        # Create parquet test file
        self.parquet_file = os.path.join(self.test_dir, 'test_data.parquet')
        self.parquet_data = pd.DataFrame(np.random.randn(500, 8))  # 7 features + 1 label
        self.parquet_data.to_parquet(self.parquet_file)

        # Create sample config
        self.sample_config = {
            'dataset_id': 1,
            'dataset_name': 'test_loader_dataset',
            'symbols': ['AAPL'],
            'file_paths': [self.npy_file, self.parquet_file],
            'total_sequences': 1500,
            'batch_size_recommendation': 32,
            'estimated_memory_mb': 15.0,
            'iterator_configs': [
                {
                    'file_path': self.npy_file,
                    'record_count': 1000,
                    'batch_size_recommendation': 32,
                    'estimated_memory_mb': 10.0,
                    'processing_recommendations': {
                        'use_batch_loading': False,
                        'load_full_into_memory': True
                    }
                },
                {
                    'file_path': self.parquet_file,
                    'record_count': 500,
                    'batch_size_recommendation': 32,
                    'estimated_memory_mb': 5.0,
                    'processing_recommendations': {
                        'use_batch_loading': False,
                        'load_full_into_memory': True
                    }
                }
            ]
        }

        self.mock_service = Mock()

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    def test_loader_initialization(self):
        """Test DatasetLoader initialization."""
        loader = DatasetLoader(self.sample_config, self.mock_service)

        self.assertEqual(loader.config['dataset_name'], 'test_loader_dataset')
        self.assertEqual(len(loader.config['iterator_configs']), 2)
        self.assertIsNotNone(loader.service)

    def test_batch_iterator_numpy_file(self):
        """Test batch iterator with numpy files."""
        loader = DatasetLoader(self.sample_config, self.mock_service)

        batches = list(loader.get_batch_iterator(batch_size=100))

        # Should get batches from both files
        self.assertGreater(len(batches), 0)

        # Check batch shapes
        for X_batch, y_batch in batches:
            self.assertEqual(len(X_batch.shape), 2)  # 2D features
            self.assertEqual(len(y_batch.shape), 1)  # 1D labels
            self.assertLessEqual(len(X_batch), 100)  # Respects batch size
            self.assertEqual(len(X_batch), len(y_batch))  # Matching lengths

    def test_batch_iterator_parquet_file(self):
        """Test batch iterator handles parquet files correctly."""
        # Config with only parquet file
        parquet_config = {
            'dataset_name': 'parquet_test',
            'batch_size_recommendation': 50,
            'iterator_configs': [
                {
                    'file_path': self.parquet_file,
                    'record_count': 500
                }
            ]
        }

        loader = DatasetLoader(parquet_config, self.mock_service)
        batches = list(loader.get_batch_iterator(batch_size=50))

        self.assertGreater(len(batches), 0)

        # Verify data from parquet
        for X_batch, y_batch in batches:
            self.assertLessEqual(len(X_batch), 50)
            self.assertEqual(X_batch.shape[1], 7)  # 7 features (8 total - 1 label)

    def test_get_full_dataset(self):
        """Test loading full dataset into memory."""
        loader = DatasetLoader(self.sample_config, self.mock_service)

        X_full, y_full = loader.get_full_dataset()

        # Should load all data
        self.assertGreater(len(X_full), 0)
        self.assertGreater(len(y_full), 0)
        self.assertEqual(len(X_full), len(y_full))

        # Should combine data from both files
        # Note: exact count may vary due to different feature counts
        self.assertGreater(len(X_full), 1000)  # At least the numpy file size

    def test_get_full_dataset_memory_warning(self):
        """Test memory warning for large datasets."""
        # Create config with large estimated memory
        large_config = self.sample_config.copy()
        large_config['estimated_memory_mb'] = 3000.0  # 3GB

        loader = DatasetLoader(large_config, self.mock_service)

        with patch('src.clients.dataset_client.logger') as mock_logger:
            X_full, y_full = loader.get_full_dataset()

            # Should warn about large dataset
            mock_logger.warning.assert_called()
            warning_msg = mock_logger.warning.call_args[0][0]
            self.assertIn('Large dataset', warning_msg)

    def test_get_sample(self):
        """Test getting random sample from dataset."""
        loader = DatasetLoader(self.sample_config, self.mock_service)

        X_sample, y_sample = loader.get_sample(sample_size=200)

        # Should get requested sample size or less (if dataset smaller)
        self.assertLessEqual(len(X_sample), 200)
        self.assertEqual(len(X_sample), len(y_sample))
        self.assertGreater(len(X_sample), 0)

    def test_get_sample_larger_than_dataset(self):
        """Test sampling when request is larger than dataset."""
        loader = DatasetLoader(self.sample_config, self.mock_service)

        X_sample, y_sample = loader.get_sample(sample_size=10000)  # Larger than dataset

        # Should return entire dataset
        self.assertGreater(len(X_sample), 0)
        self.assertEqual(len(X_sample), len(y_sample))

    def test_get_metadata(self):
        """Test retrieving loader metadata."""
        loader = DatasetLoader(self.sample_config, self.mock_service)

        metadata = loader.get_metadata()

        self.assertEqual(metadata['dataset_name'], 'test_loader_dataset')
        self.assertEqual(metadata['symbols'], ['AAPL'])
        self.assertEqual(metadata['total_sequences'], 1500)
        self.assertIn('iterator_configs', metadata)

    def test_unsupported_file_format(self):
        """Test handling of unsupported file formats."""
        # Create unsupported file
        unsupported_file = os.path.join(self.test_dir, 'data.txt')
        with open(unsupported_file, 'w') as f:
            f.write("unsupported data format")

        unsupported_config = {
            'dataset_name': 'unsupported_test',
            'batch_size_recommendation': 32,
            'iterator_configs': [
                {
                    'file_path': unsupported_file,
                    'record_count': 100
                }
            ]
        }

        loader = DatasetLoader(unsupported_config, self.mock_service)
        batches = list(loader.get_batch_iterator())

        # Should handle gracefully (return empty)
        self.assertEqual(len(batches), 0)

    def test_file_load_error_handling(self):
        """Test graceful handling of file loading errors."""
        # Config with non-existent file
        error_config = {
            'dataset_name': 'error_test',
            'batch_size_recommendation': 32,
            'iterator_configs': [
                {
                    'file_path': '/nonexistent/file.npy',
                    'record_count': 100
                }
            ]
        }

        loader = DatasetLoader(error_config, self.mock_service)

        with patch('src.clients.dataset_client.logger') as mock_logger:
            batches = list(loader.get_batch_iterator())

            # Should log error and continue gracefully
            self.assertEqual(len(batches), 0)
            mock_logger.error.assert_called()

if __name__ == '__main__':
    unittest.main()