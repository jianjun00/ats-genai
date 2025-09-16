#!/usr/bin/env python3
"""
Integration Tests for Dataset Service Feature Metadata API
"""

import pytest
import psycopg2
import psycopg2.extras
import sys
import os
from datetime import datetime
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from services.dataset_service import DatasetService

class TestDatasetServiceFeatureMetadata:
    """Test dataset service feature metadata functionality."""

    @pytest.fixture
    def mock_db_config(self):
        """Mock database configuration."""
        return {
            'host': 'localhost',
            'port': 3432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

    @pytest.fixture
    def dataset_service(self, mock_db_config):
        """Create dataset service with mock config."""
        return DatasetService(mock_db_config)

    @pytest.fixture
    def sample_feature_metadata(self):
        """Sample feature metadata for testing."""
        return {
            "features": [
                {
                    "name": "open",
                    "feature_type": "OHLC",
                    "data_type": "float64",
                    "shape": [50, 1],
                    "description": "Opening price",
                    "visualization_hints": {
                        "visualization_type": "CANDLESTICK",
                        "color_scheme": "green_red",
                        "is_primary_indicator": True
                    },
                    "statistics": {
                        "min_value": 150.0,
                        "max_value": 200.0,
                        "mean_value": 175.0,
                        "std_value": 12.5,
                        "outlier_count": 2
                    }
                },
                {
                    "name": "envelope_top",
                    "feature_type": "PRICE_INDICATOR",
                    "data_type": "float64",
                    "shape": [50, 1],
                    "description": "Price envelope top indicator",
                    "technical_indicator_params": {
                        "indicator_type": "price_envelope",
                        "percentage": 2.5
                    },
                    "visualization_hints": {
                        "visualization_type": "LINE_CHART",
                        "color_scheme": "blue",
                        "is_primary_indicator": False
                    },
                    "statistics": {
                        "min_value": 155.0,
                        "max_value": 205.0,
                        "mean_value": 180.0,
                        "std_value": 13.0,
                        "outlier_count": 1
                    }
                },
                {
                    "name": "volume",
                    "feature_type": "VOLUME_INDICATOR",
                    "data_type": "float64",
                    "shape": [50, 1],
                    "description": "Trading volume",
                    "visualization_hints": {
                        "visualization_type": "BAR_CHART",
                        "color_scheme": "orange",
                        "is_primary_indicator": False
                    },
                    "statistics": {
                        "min_value": 10000.0,
                        "max_value": 50000.0,
                        "mean_value": 25000.0,
                        "std_value": 8000.0,
                        "outlier_count": 0
                    }
                }
            ],
            "labels": [
                {
                    "name": "return_1h",
                    "label_type": "return",
                    "data_type": "float64",
                    "shape": [1],
                    "description": "1-hour return prediction",
                    "statistics": {
                        "min_value": -0.05,
                        "max_value": 0.06,
                        "mean_value": 0.001,
                        "std_value": 0.015
                    }
                }
            ],
            "metadata_version": "1.0",
            "creation_timestamp": "2024-09-06T20:00:00Z",
            "total_features": 3,
            "total_labels": 1,
            "data_quality_metrics": {
                "feature_completeness": 0.98,
                "label_completeness": 0.95,
                "overall_quality_score": 0.96
            }
        }

    @pytest.fixture
    def sample_db_row(self):
        """Sample database row for testing."""
        return {
            'id': 1,
            'dataset_name': 'test_dataset',
            'technical_indicators': 'envelope_top,envelope_bot,RSI_14',
            'symbols': 'AAPL,TSLA',
            'total_sequences': 1000,
            'feature_count': 10,
            'label_count': 2,
            'sequence_length': 50,
            'data_quality_score': 0.95,
            'feature_completeness': 0.98,
            'label_completeness': 0.92,
            'file_size_mb': 125.5,
            'date_range_start': datetime(2024, 1, 1).date(),
            'date_range_end': datetime(2024, 12, 31).date(),
            'creation_timestamp': datetime(2024, 9, 6, 20, 0, 0),
            'run_id': 42,
            'processing_config': {'batch_size': 32}
        }

    def test_get_feature_metadata_success(self, dataset_service, sample_feature_metadata):
        """Test successful feature metadata retrieval."""

        # Mock database response
        mock_result = {
            'feature_metadata': sample_feature_metadata,
            'dataset_name': 'test_dataset',
            'technical_indicators': 'envelope_top,envelope_bot'
        }

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = mock_result

            result = dataset_service.get_feature_metadata(1)

            assert result['metadata_version'] == '1.0'
            assert len(result['features']) == 3
            assert len(result['labels']) == 1
            assert result['total_features'] == 3
            assert result['total_labels'] == 1

            # Verify feature details
            open_feature = next(f for f in result['features'] if f['name'] == 'open')
            assert open_feature['feature_type'] == 'OHLC'
            assert open_feature['shape'] == [50, 1]
            assert open_feature['statistics']['min_value'] == 150.0
            assert open_feature['visualization_hints']['color_scheme'] == 'green_red'

            envelope_feature = next(f for f in result['features'] if f['name'] == 'envelope_top')
            assert envelope_feature['technical_indicator_params']['indicator_type'] == 'price_envelope'
            assert envelope_feature['technical_indicator_params']['percentage'] == 2.5

    def test_get_feature_metadata_dataset_not_found(self, dataset_service):
        """Test feature metadata retrieval for non-existent dataset."""

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = None

            result = dataset_service.get_feature_metadata(999)

            assert 'error' in result
            assert 'Dataset 999 not found' in result['error']
            assert result['features'] == []
            assert result['labels'] == []

    def test_get_feature_metadata_empty_metadata(self, dataset_service):
        """Test feature metadata retrieval with empty metadata."""

        mock_result = {
            'feature_metadata': None,
            'dataset_name': 'test_dataset',
            'technical_indicators': 'SMA_20,EMA_14,RSI_14'
        }

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = mock_result

            result = dataset_service.get_feature_metadata(1)

            # Should generate basic metadata
            assert result['metadata_version'] == '1.0'
            assert len(result['features']) > 0  # OHLCV + indicators
            assert 'note' in result
            assert 'Basic metadata generated' in result['note']

            # Should contain OHLCV features
            feature_names = [f['name'] for f in result['features']]
            assert 'open' in feature_names
            assert 'high' in feature_names
            assert 'low' in feature_names
            assert 'close' in feature_names
            assert 'volume' in feature_names
            assert 'SMA_20' in feature_names
            assert 'EMA_14' in feature_names
            assert 'RSI_14' in feature_names

    def test_find_datasets_by_features(self, dataset_service, sample_db_row, sample_feature_metadata):
        """Test finding datasets by feature names."""

        # Mock database response
        sample_db_row['feature_metadata'] = sample_feature_metadata

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchall.return_value = [sample_db_row]

            # Test finding datasets by feature names
            result = dataset_service.find_datasets_by_features(['open', 'close'])

            assert len(result) == 1
            assert result[0].dataset_name == 'test_dataset'
            assert result[0].dataset_id == 1
            assert 'AAPL' in result[0].symbols
            assert 'TSLA' in result[0].symbols

            # Verify SQL query construction
            mock_cursor.execute.assert_called_once()
            call_args = mock_cursor.execute.call_args
            query, params = call_args[0]

            assert 'feature_metadata IS NOT NULL' in query
            assert 'ILIKE' in query
            assert '%open%' in params
            assert '%close%' in params

    def test_find_datasets_by_feature_types(self, dataset_service, sample_db_row):
        """Test finding datasets by feature types."""

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchall.return_value = [sample_db_row]

            result = dataset_service.find_datasets_by_features(
                required_features=[],
                feature_types=['PRICE_INDICATOR', 'VOLUME_INDICATOR']
            )

            assert len(result) == 1

            # Verify SQL query includes feature type search
            call_args = mock_cursor.execute.call_args
            query, params = call_args[0]

            assert 'PRICE_INDICATOR' in params
            assert 'VOLUME_INDICATOR' in params

    def test_find_datasets_by_features_empty_result(self, dataset_service):
        """Test finding datasets with no matches."""

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchall.return_value = []

            result = dataset_service.find_datasets_by_features(['nonexistent_feature'])

            assert len(result) == 0

    def test_compare_feature_schemas_compatible(self, dataset_service, sample_feature_metadata):
        """Test comparing compatible feature schemas."""

        # Both datasets have same metadata
        with patch.object(dataset_service, 'get_feature_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = sample_feature_metadata

            result = dataset_service.compare_feature_schemas(1, 2)

            assert result['compatible'] == True
            assert len(result['common_features']) == 3  # open, envelope_top, volume
            assert len(result['missing_in_dataset_1']) == 0
            assert len(result['missing_in_dataset_2']) == 0
            assert len(result['type_mismatches']) == 0
            assert len(result['shape_mismatches']) == 0
            assert result['compatibility_score'] == 1.0

            # Verify common features
            assert 'open' in result['common_features']
            assert 'envelope_top' in result['common_features']
            assert 'volume' in result['common_features']

    def test_compare_feature_schemas_incompatible(self, dataset_service, sample_feature_metadata):
        """Test comparing incompatible feature schemas."""

        # Create different metadata for second dataset
        different_metadata = sample_feature_metadata.copy()
        different_metadata['features'] = [
            {
                "name": "open",
                "feature_type": "OHLC",
                "data_type": "float32",  # Different data type
                "shape": [100, 1],       # Different shape
            },
            {
                "name": "close",  # Different feature
                "feature_type": "OHLC",
                "data_type": "float64",
                "shape": [50, 1],
            },
            {
                "name": "new_indicator",  # Unique to dataset 2
                "feature_type": "PRICE_INDICATOR",
                "data_type": "float64",
                "shape": [50, 1],
            }
        ]

        with patch.object(dataset_service, 'get_feature_metadata') as mock_get_metadata:
            mock_get_metadata.side_effect = [sample_feature_metadata, different_metadata]

            result = dataset_service.compare_feature_schemas(1, 2)

            assert result['compatible'] == False
            assert len(result['common_features']) == 1  # Only 'open'
            assert len(result['missing_in_dataset_1']) == 2  # close, new_indicator
            assert len(result['missing_in_dataset_2']) == 2  # envelope_top, volume
            assert len(result['type_mismatches']) == 1  # open has different data type
            assert len(result['shape_mismatches']) == 1  # open has different shape
            assert result['compatibility_score'] < 1.0

            # Verify mismatch details
            type_mismatch = result['type_mismatches'][0]
            assert type_mismatch['feature'] == 'open'
            assert type_mismatch['dataset_1_type'] == 'float64'
            assert type_mismatch['dataset_2_type'] == 'float32'

            shape_mismatch = result['shape_mismatches'][0]
            assert shape_mismatch['feature'] == 'open'
            assert shape_mismatch['dataset_1_shape'] == [50, 1]
            assert shape_mismatch['dataset_2_shape'] == [100, 1]

    def test_compare_feature_schemas_with_errors(self, dataset_service):
        """Test schema comparison with metadata retrieval errors."""

        error_metadata = {'error': 'Dataset not found'}
        valid_metadata = {'features': [], 'labels': []}

        with patch.object(dataset_service, 'get_feature_metadata') as mock_get_metadata:
            mock_get_metadata.side_effect = [error_metadata, valid_metadata]

            result = dataset_service.compare_feature_schemas(1, 2)

            assert result['compatible'] == False
            assert 'error' in result
            assert result['metadata_1_error'] == 'Dataset not found'

    def test_generate_basic_feature_metadata(self, dataset_service):
        """Test basic feature metadata generation."""

        technical_indicators = "SMA_20,EMA_14,RSI_14,envelope_top,envelope_bot,BXTrenderBasic_14"

        result = dataset_service._generate_basic_feature_metadata(1, technical_indicators)

        assert result['metadata_version'] == '1.0'
        assert result['total_features'] == 11  # 5 OHLCV + 6 indicators
        assert result['total_labels'] == 0
        assert 'creation_timestamp' in result
        assert 'note' in result

        feature_names = [f['name'] for f in result['features']]

        # Check OHLCV features
        assert 'open' in feature_names
        assert 'high' in feature_names
        assert 'low' in feature_names
        assert 'close' in feature_names
        assert 'volume' in feature_names

        # Check technical indicators
        assert 'SMA_20' in feature_names
        assert 'EMA_14' in feature_names
        assert 'RSI_14' in feature_names
        assert 'envelope_top' in feature_names
        assert 'envelope_bot' in feature_names
        assert 'BXTrenderBasic_14' in feature_names

        # Verify feature types
        open_feature = next(f for f in result['features'] if f['name'] == 'open')
        assert open_feature['feature_type'] == 'OHLC'
        assert open_feature['data_type'] == 'float64'

        volume_feature = next(f for f in result['features'] if f['name'] == 'volume')
        assert volume_feature['feature_type'] == 'VOLUME_INDICATOR'

        sma_feature = next(f for f in result['features'] if f['name'] == 'SMA_20')
        assert sma_feature['feature_type'] == 'PRICE_INDICATOR'

    def test_row_to_dataset_metadata(self, dataset_service, sample_db_row):
        """Test database row to DatasetMetadata conversion."""

        file_paths = ['/path/to/file1.riegeli', '/path/to/file2.riegeli']

        result = dataset_service._row_to_dataset_metadata(sample_db_row, file_paths)

        assert result is not None
        assert result.dataset_id == 1
        assert result.dataset_name == 'test_dataset'
        assert result.dataset_type == 'training'
        assert result.symbols == ['AAPL', 'TSLA']
        assert result.total_sequences == 1000
        assert result.feature_count == 10
        assert result.label_count == 2
        assert result.file_paths == file_paths
        assert result.data_quality_score == 0.95
        assert result.technical_indicators == ['envelope_top', 'envelope_bot', 'RSI_14']
        assert result.run_id == 42

    def test_database_error_handling(self, dataset_service):
        """Test database error handling in feature metadata operations."""

        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.DatabaseError("Connection failed")

            # Test get_feature_metadata error handling
            result = dataset_service.get_feature_metadata(1)
            assert 'error' in result
            assert 'Connection failed' in result['error']

            # Test find_datasets_by_features error handling
            result = dataset_service.find_datasets_by_features(['test'])
            assert len(result) == 0

            # Test compare_feature_schemas error handling
            result = dataset_service.compare_feature_schemas(1, 2)
            assert result['compatible'] == False
            assert 'error' in result

class TestFeatureMetadataPerformance:
    """Test performance aspects of feature metadata operations."""

    @pytest.fixture
    def dataset_service(self):
        """Create dataset service for performance testing."""
        return DatasetService({
            'host': 'localhost',
            'port': 3432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        })

    def test_large_metadata_handling(self, dataset_service):
        """Test handling of large feature metadata."""

        # Create large metadata with many features
        large_metadata = {
            "features": [
                {
                    "name": f"feature_{i}",
                    "feature_type": "FLOAT",
                    "data_type": "float64",
                    "shape": [100, 1],
                    "description": f"Feature {i}",
                    "statistics": {
                        "min_value": float(i),
                        "max_value": float(i + 100),
                        "mean_value": float(i + 50),
                        "std_value": float(i + 10)
                    }
                }
                for i in range(1000)  # 1000 features
            ],
            "labels": [],
            "metadata_version": "1.0",
            "total_features": 1000,
            "total_labels": 0
        }

        mock_result = {
            'feature_metadata': large_metadata,
            'dataset_name': 'large_dataset',
            'technical_indicators': ''
        }

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = mock_result

            import time
            start_time = time.time()
            result = dataset_service.get_feature_metadata(1)
            end_time = time.time()

            # Should handle large metadata efficiently
            assert len(result['features']) == 1000
            assert end_time - start_time < 1.0  # Should complete within 1 second

    def test_basic_metadata_generation_performance(self, dataset_service):
        """Test performance of basic metadata generation."""

        # Large technical indicators list
        large_indicators = ",".join([f"indicator_{i}" for i in range(500)])

        import time
        start_time = time.time()
        result = dataset_service._generate_basic_feature_metadata(1, large_indicators)
        end_time = time.time()

        assert len(result['features']) == 505  # 5 OHLCV + 500 indicators
        assert end_time - start_time < 0.5  # Should be very fast

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])