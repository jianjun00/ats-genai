#!/usr/bin/env python3
"""
Integration tests for Dataset Service and Client - end-to-end functionality
Tests real database integration, training pipeline integration, and EDA integration.
"""

import unittest
import tempfile
import os
import json
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from infrastructure.services_legacy.dataset_service import DatasetService
from core.shared.clients.dataset_client import DatasetClient
from scripts.eda_with_dataset_service import EDAAnalyzer

class TestDatasetServiceIntegration(unittest.TestCase):
    """Integration tests for dataset service with real database."""

    @classmethod
    def setUpClass(cls):
        """Set up test database connection."""
        cls.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'dev_db',
            'user': 'postgres',
            'password': 'dev_password'
        }

        # Test database connectivity
        conn = psycopg2.connect(**cls.db_config)
        conn.close()
        cls.db_available = True
    def setUp(self):
        """Set up test environment."""
        if not self.db_available:
            self.skipTest("Database not available for integration testing")

        self.test_dir = tempfile.mkdtemp()

        # Create test data files
        self.create_test_data_files()

        # Initialize service and client
        self.service = DatasetService(self.db_config)
        self.client = DatasetClient(self.db_config)

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    def create_test_data_files(self):
        """Create realistic test data files."""
        # Create numpy training data (sequences of OHLCV + indicators)
        self.training_file1 = os.path.join(self.test_dir, 'aapl_training_1.npy')

        # Simulate 3D training data: (sequences, timesteps, features)
        # Features: [open, high, low, close, volume, rsi, macd, bb_upper, bb_lower, returns]
        sequences = 500
        timesteps = 100
        features = 10

        training_data = np.random.randn(sequences, timesteps, features)

        # Make realistic OHLCV data
        training_data[:, :, 0] = np.abs(training_data[:, :, 0]) * 10 + 150  # Open prices around 150
        training_data[:, :, 1] = training_data[:, :, 0] + np.abs(training_data[:, :, 1]) * 2  # High > Open
        training_data[:, :, 2] = training_data[:, :, 0] - np.abs(training_data[:, :, 2]) * 2  # Low < Open
        training_data[:, :, 3] = training_data[:, :, 0] + (training_data[:, :, 3]) * 1  # Close around Open
        training_data[:, :, 4] = np.abs(training_data[:, :, 4]) * 1000000  # Volume

        # Technical indicators
        training_data[:, :, 5] = np.clip(training_data[:, :, 5] * 20 + 50, 0, 100)  # RSI 0-100
        training_data[:, :, 6] = training_data[:, :, 6] * 0.5  # MACD
        training_data[:, :, 7] = training_data[:, :, 1] + 2  # BB Upper
        training_data[:, :, 8] = training_data[:, :, 2] - 2  # BB Lower
        training_data[:, :, 9] = training_data[:, :, 9] * 0.02  # Returns -2% to +2%

        np.save(self.training_file1, training_data)

        # Create parquet file with minute data
        self.parquet_file = os.path.join(self.test_dir, 'aapl_minute_data.parquet')
        minute_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-07-01', periods=1000, freq='1min'),
            'open': np.random.randn(1000) * 2 + 150,
            'high': np.random.randn(1000) * 2 + 152,
            'low': np.random.randn(1000) * 2 + 148,
            'close': np.random.randn(1000) * 2 + 150,
            'volume': np.random.randint(10000, 1000000, 1000),
            'returns': np.random.randn(1000) * 0.01
        })
        minute_data.to_parquet(self.parquet_file)

    def test_end_to_end_training_pipeline_integration(self):
        """Test complete training pipeline using dataset service."""

        # Step 1: Create a test dataset entry in database (simulate existing dataset)
        # In real scenario, this would be created by data generation pipeline
        test_dataset_metadata = {
            'dataset_name': 'integration_test_aapl',
            'dataset_type': 'training',
            'symbols': ['AAPL'],
            'total_sequences': 500,
            'total_records': 50000,
            'feature_count': 10,
            'label_count': 1,
            'sequence_length': 100,
            'file_format': 'npy',
            'base_directory': self.test_dir,
            'file_size_mb': 10.0,
            'data_quality_score': 0.92,
            'feature_completeness': 0.95,
            'label_completeness': 0.98,
            'technical_indicators': ['RSI', 'MACD', 'BB'],
            'timeframes': ['1h', '1d'],
            'date_range_start': '2025-07-01',
            'date_range_end': '2025-07-31',
            'processing_config': json.dumps({'batch_size': 32})
        }

        # Register dataset (simulate what data generation pipeline would do)
        dataset_id = self.service.register_dataset(
            metadata=test_dataset_metadata,
            file_paths=[self.training_file1]
        )

        self.assertIsNotNone(dataset_id)

        # Step 2: Training job uses client to discover dataset
        config = self.client.get_training_data_config(
            symbols=['AAPL'],
            min_sequences=100
        )

        self.assertIsNotNone(config)
        self.assertEqual(config['dataset_name'], 'integration_test_aapl')
        self.assertEqual(config['symbols'], ['AAPL'])
        self.assertGreater(config['total_sequences'], 0)
        self.assertIn(self.training_file1, config['file_paths'])

        # Step 3: Validate dataset for training
        validation = self.client.validate_dataset_for_training(
            dataset_id=config['dataset_id'],
            required_features=5,
            min_sequences=100
        )

        self.assertTrue(validation['valid'])
        self.assertTrue(validation['checks']['files_accessible'])
        self.assertTrue(validation['checks']['sufficient_sequences'])
        self.assertTrue(validation['checks']['sufficient_features'])

        # Step 4: Create data loader and test data loading
        loader = self.client.create_data_loader(config)

        # Test batch iterator
        batch_count = 0
        for X_batch, y_batch in loader.get_batch_iterator(batch_size=32):
            self.assertGreater(len(X_batch), 0)
            self.assertEqual(len(X_batch), len(y_batch))
            batch_count += 1
            if batch_count >= 5:  # Test first few batches
                break

        self.assertGreater(batch_count, 0)

        # Test sample generation
        X_sample, y_sample = loader.get_sample(sample_size=100)
        self.assertEqual(len(X_sample), 100)
        self.assertEqual(len(y_sample), 100)

    def test_eda_integration_with_dataset_service(self):
        """Test EDA integration using dataset service."""

        # Create test dataset
        test_dataset_metadata = {
            'dataset_name': 'eda_integration_test',
            'dataset_type': 'training',
            'symbols': ['AAPL', 'TSLA'],
            'total_sequences': 200,
            'total_records': 20000,
            'feature_count': 10,
            'label_count': 1,
            'sequence_length': 100,
            'file_format': 'npy',
            'base_directory': self.test_dir,
            'file_size_mb': 5.0,
            'data_quality_score': 0.89,
            'feature_completeness': 0.92,
            'label_completeness': 0.96,
            'technical_indicators': ['RSI', 'MACD'],
            'timeframes': ['1h'],
            'date_range_start': '2025-07-01',
            'date_range_end': '2025-07-15',
            'processing_config': json.dumps({'features': 'technical'})
        }

        dataset_id = self.service.register_dataset(
            metadata=test_dataset_metadata,
            file_paths=[self.training_file1]
        )

        # Initialize EDA analyzer with dataset client
        analyzer = EDAAnalyzer(self.client)

        # Test dataset exploration
        available_datasets = analyzer.explore_available_datasets(['AAPL'])
        self.assertGreater(len(available_datasets), 0)

        # Test specific dataset analysis
        analysis = analyzer.analyze_dataset(dataset_id)

        self.assertIsNotNone(analysis)
        self.assertIn('dataset_overview', analysis)
        self.assertIn('data_quality_metrics', analysis)
        self.assertIn('statistical_summary', analysis)

        # Verify dataset overview
        overview = analysis['dataset_overview']
        self.assertEqual(overview['dataset_name'], 'eda_integration_test')
        self.assertIn('AAPL', overview['symbols'])
        self.assertEqual(overview['total_sequences'], 200)

        # Verify quality metrics
        quality = analysis['data_quality_metrics']
        self.assertAlmostEqual(quality['overall_score'], 0.89, places=2)
        self.assertAlmostEqual(quality['feature_completeness'], 0.92, places=2)

        # Test EDA report generation
        report = analyzer.generate_eda_report(dataset_id)

        self.assertIn('executive_summary', report)
        self.assertIn('detailed_analysis', report)
        self.assertIn('recommendations', report)

        # Verify executive summary
        summary = report['executive_summary']
        self.assertEqual(summary['dataset_name'], 'eda_integration_test')
        self.assertEqual(summary['status'], 'analysis_complete')

    def test_multiple_datasets_discovery_and_ranking(self):
        """Test client's ability to discover and rank multiple datasets."""

        # Create multiple test datasets with different qualities
        datasets_to_create = [
            {
                'name': 'high_quality_aapl',
                'symbols': ['AAPL'],
                'quality': 0.95,
                'sequences': 5000,
                'file_path': self.training_file1
            },
            {
                'name': 'medium_quality_aapl',
                'symbols': ['AAPL'],
                'quality': 0.80,
                'sequences': 3000,
                'file_path': self.training_file1
            },
            {
                'name': 'low_quality_aapl',
                'symbols': ['AAPL'],
                'quality': 0.65,
                'sequences': 1000,
                'file_path': self.training_file1
            }
        ]

        dataset_ids = []
        for ds_info in datasets_to_create:
            metadata = {
                'dataset_name': ds_info['name'],
                'dataset_type': 'training',
                'symbols': ds_info['symbols'],
                'total_sequences': ds_info['sequences'],
                'total_records': ds_info['sequences'] * 100,
                'feature_count': 10,
                'label_count': 1,
                'sequence_length': 100,
                'file_format': 'npy',
                'base_directory': self.test_dir,
                'file_size_mb': ds_info['sequences'] * 0.01,
                'data_quality_score': ds_info['quality'],
                'feature_completeness': ds_info['quality'],
                'label_completeness': ds_info['quality'] + 0.02,
                'technical_indicators': ['RSI'],
                'timeframes': ['1h'],
                'date_range_start': '2025-07-01',
                'date_range_end': '2025-07-31',
                'processing_config': json.dumps({})
            }

            dataset_id = self.service.register_dataset(
                metadata=metadata,
                file_paths=[ds_info['file_path']]
            )
            dataset_ids.append(dataset_id)

        # Test client selects best quality dataset
        best_dataset = self.client.find_dataset(
            symbols=['AAPL'],
            min_sequences=1000,
            min_quality=0.7
        )

        self.assertIsNotNone(best_dataset)
        self.assertEqual(best_dataset.dataset_name, 'high_quality_aapl')
        self.assertEqual(best_dataset.data_quality_score, 0.95)

        # Test filtering by minimum sequences
        filtered_dataset = self.client.find_dataset(
            symbols=['AAPL'],
            min_sequences=4000,  # Only high quality dataset meets this
            min_quality=0.7
        )

        self.assertIsNotNone(filtered_dataset)
        self.assertEqual(filtered_dataset.dataset_name, 'high_quality_aapl')

        # Test filtering by high quality threshold
        high_quality_dataset = self.client.find_dataset(
            symbols=['AAPL'],
            min_sequences=1000,
            min_quality=0.90  # Only high quality dataset meets this
        )

        self.assertIsNotNone(high_quality_dataset)
        self.assertEqual(high_quality_dataset.dataset_name, 'high_quality_aapl')

        # Test no matches for very strict criteria
        no_match = self.client.find_dataset(
            symbols=['AAPL'],
            min_sequences=10000,  # Too high
            min_quality=0.99      # Too high
        )

        self.assertIsNone(no_match)

    def test_file_format_support_integration(self):
        """Test integration with multiple file formats."""

        # Create dataset with multiple file formats
        mixed_format_metadata = {
            'dataset_name': 'mixed_format_dataset',
            'dataset_type': 'training',
            'symbols': ['AAPL'],
            'total_sequences': 700,  # Combined from both files
            'total_records': 51000,   # 500 sequences + 1000 minute records
            'feature_count': 7,
            'label_count': 1,
            'sequence_length': 100,
            'file_format': 'mixed',
            'base_directory': self.test_dir,
            'file_size_mb': 15.0,
            'data_quality_score': 0.88,
            'feature_completeness': 0.90,
            'label_completeness': 0.93,
            'technical_indicators': ['RSI', 'MACD'],
            'timeframes': ['1min', '1h'],
            'date_range_start': '2025-07-01',
            'date_range_end': '2025-07-31',
            'processing_config': json.dumps({'formats': ['npy', 'parquet']})
        }

        dataset_id = self.service.register_dataset(
            metadata=mixed_format_metadata,
            file_paths=[self.training_file1, self.parquet_file]
        )

        # Test loading data from mixed formats
        config = self.client.get_training_data_config(
            symbols=['AAPL'],
            min_sequences=500
        )

        self.assertIsNotNone(config)
        self.assertEqual(len(config['file_paths']), 2)
        self.assertIn(self.training_file1, config['file_paths'])
        self.assertIn(self.parquet_file, config['file_paths'])

        # Create loader and test mixed format loading
        loader = self.client.create_data_loader(config)

        batch_count = 0
        total_samples = 0

        for X_batch, y_batch in loader.get_batch_iterator(batch_size=50):
            self.assertGreater(len(X_batch), 0)
            self.assertEqual(len(X_batch), len(y_batch))
            batch_count += 1
            total_samples += len(X_batch)

            if batch_count >= 10:  # Test multiple batches
                break

        self.assertGreater(batch_count, 0)
        self.assertGreater(total_samples, 0)

    def test_error_handling_integration(self):
        """Test error handling in integration scenarios."""

        # Test with non-existent dataset
        config = self.client.get_training_data_config(
            symbols=['NONEXISTENT'],
            min_sequences=1000
        )
        self.assertIsNone(config)

        # Test validation with non-existent dataset ID
        validation = self.client.validate_dataset_for_training(
            dataset_id=99999,
            required_features=5,
            min_sequences=1000
        )
        self.assertFalse(validation['valid'])
        self.assertIn('error', validation)

        # Test dataset with missing files
        broken_metadata = {
            'dataset_name': 'broken_dataset',
            'dataset_type': 'training',
            'symbols': ['TEST'],
            'total_sequences': 1000,
            'total_records': 50000,
            'feature_count': 5,
            'label_count': 1,
            'sequence_length': 100,
            'file_format': 'npy',
            'base_directory': '/nonexistent',
            'file_size_mb': 10.0,
            'data_quality_score': 0.85,
            'feature_completeness': 0.90,
            'label_completeness': 0.95,
            'technical_indicators': ['RSI'],
            'timeframes': ['1h'],
            'date_range_start': '2025-07-01',
            'date_range_end': '2025-07-31',
            'processing_config': json.dumps({})
        }

        dataset_id = self.service.register_dataset(
            metadata=broken_metadata,
            file_paths=['/nonexistent/file.npy']
        )

        # Should register but validation should fail
        validation = self.client.validate_dataset_for_training(
            dataset_id=dataset_id,
            required_features=5,
            min_sequences=100
        )

        self.assertFalse(validation['valid'])
        self.assertFalse(validation['checks']['files_accessible'])

if __name__ == '__main__':
    unittest.main()