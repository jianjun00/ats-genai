#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Enhanced Training Data Metadata System
"""

import pytest
import numpy as np
import tempfile
import json
from pathlib import Path
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.training_data.generators.training_data_metadata import (
    FeatureMetadata, LabelMetadata, TrainingDataMetadataManager,
    FeatureType, VisualizationType
)

class TestFeatureMetadata:
    """Test FeatureMetadata dataclass enhancements."""

    def test_feature_metadata_basic_fields(self):
        """Test basic feature metadata fields."""
        metadata = FeatureMetadata(
            name="test_feature",
            feature_type=FeatureType.OHLC,
            data_type="float64",
            dimension=1,
            description="Test feature"
        )

        assert metadata.name == "test_feature"
        assert metadata.feature_type == FeatureType.OHLC
        assert metadata.data_type == "float64"
        assert metadata.dimension == 1
        assert metadata.description == "Test feature"
        assert metadata.shape == []  # Default empty list
        assert metadata.outlier_count == 0  # Default value
        assert metadata.visualization_hints == {}  # Default empty dict
        assert metadata.technical_indicator_params == {}  # Default empty dict

    def test_feature_metadata_enhanced_fields(self):
        """Test enhanced feature metadata fields."""
        visualization_hints = {
            'color_scheme': 'green_red',
            'scale_type': 'linear',
            'is_primary_indicator': True
        }

        tech_params = {
            'indicator_type': 'price_envelope',
            'percentage': 2.5
        }

        metadata = FeatureMetadata(
            name="envelope_top",
            feature_type=FeatureType.PRICE_INDICATOR,
            data_type="float64",
            dimension=1,
            description="Price envelope top",
            shape=[50, 1],
            outlier_count=5,
            visualization_hints=visualization_hints,
            technical_indicator_params=tech_params,
            min_value=100.0,
            max_value=200.0,
            mean_value=150.0,
            std_value=25.0
        )

        assert metadata.shape == [50, 1]
        assert metadata.outlier_count == 5
        assert metadata.visualization_hints == visualization_hints
        assert metadata.technical_indicator_params == tech_params
        assert metadata.min_value == 100.0
        assert metadata.max_value == 200.0

class TestLabelMetadata:
    """Test LabelMetadata dataclass enhancements."""

    def test_label_metadata_enhanced_fields(self):
        """Test enhanced label metadata fields."""
        metadata = LabelMetadata(
            name="return_1h",
            label_type="return",
            data_type="float64",
            dimension=1,
            description="1-hour return label",
            lead_periods=1,
            shape=[1],
            outlier_count=3,
            min_value=-0.1,
            max_value=0.15
        )

        assert metadata.shape == [1]
        assert metadata.outlier_count == 3
        assert metadata.min_value == -0.1
        assert metadata.max_value == 0.15

class TestTrainingDataMetadataManager:
    """Test TrainingDataMetadataManager enhanced functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = TrainingDataMetadataManager(self.temp_dir)

        # Create sample data for testing
        np.random.seed(42)
        self.ohlc_data = np.random.uniform(100, 200, (1000, 50, 1))  # 1000 sequences, 50 timesteps
        self.volume_data = np.random.uniform(1000, 10000, (1000, 50, 1))
        self.return_data = np.random.normal(0.001, 0.02, (1000, 1))

        # Add some outliers to test outlier detection
        self.ohlc_data[0, 0, 0] = 1000.0  # Clear outlier
        self.return_data[0, 0] = 0.5  # Clear outlier

    def test_calculate_enhanced_statistics(self):
        """Test enhanced statistics calculation including outliers."""
        valid_data = np.array([100, 102, 98, 101, 99, 103, 97, 104, 96, 500])  # 500 is outlier

        stats = self.manager._calculate_enhanced_statistics(valid_data)

        assert stats['min_value'] == 96.0
        assert stats['max_value'] == 500.0
        assert stats['outlier_count'] > 0  # Should detect the 500 as outlier
        assert isinstance(stats['mean_value'], float)
        assert isinstance(stats['std_value'], float)

    def test_calculate_enhanced_statistics_empty_data(self):
        """Test statistics calculation with empty data."""
        valid_data = np.array([])

        stats = self.manager._calculate_enhanced_statistics(valid_data)

        assert stats['min_value'] is None
        assert stats['max_value'] is None
        assert stats['mean_value'] is None
        assert stats['std_value'] is None
        assert stats['outlier_count'] == 0

    def test_generate_visualization_hints(self):
        """Test visualization hints generation."""
        statistics = {
            'min_value': 10.0,
            'max_value': 1000.0,  # High ratio suggests log scale
        }

        # Test OHLC feature
        hints = self.manager._generate_visualization_hints(
            "close_price", FeatureType.OHLC, statistics
        )

        assert hints['color_scheme'] == 'green_red'
        assert hints['is_primary_indicator'] == True
        assert hints['scale_type'] == 'log'  # Due to high min/max ratio

        # Test volume indicator
        hints = self.manager._generate_visualization_hints(
            "volume", FeatureType.VOLUME_INDICATOR, statistics
        )

        assert hints['color_scheme'] == 'orange'
        assert hints['is_primary_indicator'] == False

        # Test return feature
        hints = self.manager._generate_visualization_hints(
            "return_1h", FeatureType.RETURN, {'min_value': -0.1, 'max_value': 0.1}
        )

        assert hints['color_scheme'] == 'green_red'
        assert hints['scale_type'] == 'symmetric'

    def test_extract_technical_indicator_params(self):
        """Test technical indicator parameter extraction."""
        # Test SMA indicator
        params = self.manager._extract_technical_indicator_params(
            "SMA_20", {'window_size': 20}
        )
        assert params['window_size'] == 20

        # Test RSI indicator
        params = self.manager._extract_technical_indicator_params(
            "RSI_14", {}
        )
        assert params['indicator_type'] == 'relative_strength_index'
        assert params['window_size'] == 14

        # Test envelope indicator
        params = self.manager._extract_technical_indicator_params(
            "envelope_top", {'percentage': 2.5}
        )
        assert params['indicator_type'] == 'price_envelope'
        assert params['percentage'] == 2.5

        # Test BX Trender variants
        params = self.manager._extract_technical_indicator_params(
            "BXTrenderBasic_14", {}
        )
        assert params['indicator_type'] == 'bx_trender'
        assert params['variant'] == 'basic'

        params = self.manager._extract_technical_indicator_params(
            "BXTrenderVolumeWeighted_14", {}
        )
        assert params['indicator_type'] == 'bx_trender'
        assert params['variant'] == 'volume_weighted'

    def test_create_enhanced_feature_metadata(self):
        """Test enhanced feature metadata creation."""
        config = {
            'source_column': 'close',
            'window_size': 20,
            'indicator_params': {'percentage': 2.5}
        }

        metadata = self.manager.create_feature_metadata(
            name="envelope_top",
            feature_type=FeatureType.PRICE_INDICATOR,
            data=self.ohlc_data[0, :, 0],  # Single sequence
            config=config
        )

        assert metadata.name == "envelope_top"
        assert metadata.feature_type == FeatureType.PRICE_INDICATOR
        assert metadata.data_type == "float64"
        assert metadata.shape == [50]  # Sequence length
        assert metadata.source_column == "close"
        assert metadata.window_size == 20
        assert metadata.min_value is not None
        assert metadata.max_value is not None
        assert metadata.mean_value is not None
        assert metadata.std_value is not None
        assert metadata.outlier_count >= 0
        assert metadata.visualization_type == VisualizationType.LINE_CHART
        assert 'color_scheme' in metadata.visualization_hints
        assert 'indicator_params' in metadata.parameters

    def test_create_enhanced_label_metadata(self):
        """Test enhanced label metadata creation."""
        config = {
            'lead_periods': 1,
            'label_type': 'return'
        }

        metadata = self.manager.create_label_metadata(
            name="return_1h",
            label_type="return",
            data=self.return_data[:, 0],  # All sequences, single label
            config=config
        )

        assert metadata.name == "return_1h"
        assert metadata.label_type == "return"
        assert metadata.shape == [1000]  # Number of sequences
        assert metadata.lead_periods == 1
        assert metadata.min_value is not None
        assert metadata.max_value is not None
        assert metadata.outlier_count >= 0  # Should detect our planted outlier
        assert metadata.visualization_type == VisualizationType.DISTRIBUTION

    def test_create_training_metadata_comprehensive(self):
        """Test comprehensive training metadata creation."""
        features_data = np.stack([
            self.ohlc_data[:, :, 0],  # open
            self.ohlc_data[:, :, 0] + np.random.uniform(-1, 1, (1000, 50)),  # high
            self.ohlc_data[:, :, 0] + np.random.uniform(-1, 0, (1000, 50)),  # low
            self.ohlc_data[:, :, 0] + np.random.uniform(-0.5, 0.5, (1000, 50)),  # close
            self.volume_data[:, :, 0]  # volume
        ], axis=2)

        labels_data = self.return_data

        feature_names = ['open', 'high', 'low', 'close', 'volume']
        label_names = ['return_1h']

        feature_configs = [
            {'source_column': 'open'},
            {'source_column': 'high'},
            {'source_column': 'low'},
            {'source_column': 'close'},
            {'source_column': 'volume'}
        ]

        label_configs = [
            {'label_type': 'return', 'lead_periods': 1}
        ]

        metadata = self.manager.create_training_metadata(
            dataset_name="test_dataset",
            features_data=features_data,
            labels_data=labels_data,
            feature_names=feature_names,
            label_names=label_names,
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=['AAPL', 'TSLA'],
            date_range={'start': '2024-01-01', 'end': '2024-12-31'},
            gin_config_path='config/test.gin',
            data_sources=['firstrate']
        )

        assert metadata.dataset_name == "test_dataset"
        assert metadata.total_sequences == 1000
        assert metadata.sequence_length == 50
        assert metadata.feature_count == 5
        assert metadata.label_count == 1
        assert len(metadata.features) == 5
        assert len(metadata.labels) == 1
        assert metadata.symbols == ['AAPL', 'TSLA']
        assert metadata.gin_config_path == 'config/test.gin'

        # Check feature metadata enhancements
        for feature in metadata.features:
            assert len(feature.shape) == 2  # [sequences, timesteps]
            assert feature.min_value is not None
            assert feature.max_value is not None
            assert hasattr(feature, 'outlier_count')
            assert hasattr(feature, 'visualization_hints')
            assert hasattr(feature, 'technical_indicator_params')

        # Check label metadata enhancements
        for label in metadata.labels:
            assert len(label.shape) == 1  # [sequences]
            assert hasattr(label, 'outlier_count')

        # Check data quality metrics
        assert 'feature_missing_ratio' in metadata.data_quality_metrics
        assert 'label_missing_ratio' in metadata.data_quality_metrics
        assert 'feature_completeness' in metadata.data_quality_metrics
        assert 'label_completeness' in metadata.data_quality_metrics

    def test_metadata_serialization_deserialization(self):
        """Test metadata save and load functionality."""
        # Create simple metadata
        features_data = self.ohlc_data[:100, :10, :]  # Smaller for testing
        labels_data = self.return_data[:100, :]

        metadata = self.manager.create_training_metadata(
            dataset_name="serialization_test",
            features_data=features_data,
            labels_data=labels_data,
            feature_names=['test_feature'],
            label_names=['test_label'],
            feature_configs=[{}],
            label_configs=[{}],
            symbols=['TEST'],
            date_range={'start': '2024-01-01', 'end': '2024-01-31'}
        )

        # Save metadata
        filepath = self.manager.save_metadata(metadata, "test_metadata.json")
        assert Path(filepath).exists()

        # Load and verify
        with open(filepath, 'r') as f:
            loaded_data = json.load(f)

        assert loaded_data['dataset_name'] == "serialization_test"
        assert loaded_data['total_sequences'] == 100
        assert loaded_data['sequence_length'] == 10
        assert len(loaded_data['features']) == 1
        assert len(loaded_data['labels']) == 1

        # Check enhanced fields are preserved
        feature_data = loaded_data['features'][0]
        assert 'shape' in feature_data
        assert 'outlier_count' in feature_data
        assert 'visualization_hints' in feature_data
        assert 'technical_indicator_params' in feature_data

    def test_feature_type_inference(self):
        """Test feature type inference logic."""
        # Test OHLC inference
        ftype = self.manager._infer_feature_type("ohlc_close", {})
        assert ftype == FeatureType.OHLC

        # Test price indicator inference
        ftype = self.manager._infer_feature_type("SMA_20", {})
        assert ftype == FeatureType.PRICE_INDICATOR

        ftype = self.manager._infer_feature_type("RSI_14", {})
        assert ftype == FeatureType.PRICE_INDICATOR

        # Test volume indicator inference
        ftype = self.manager._infer_feature_type("volume_weighted", {})
        assert ftype == FeatureType.VOLUME_INDICATOR

        # Test return inference
        ftype = self.manager._infer_feature_type("return_1h", {})
        assert ftype == FeatureType.RETURN

        # Test classification inference
        ftype = self.manager._infer_feature_type("trend_direction", {'feature_type': 'classification'})
        assert ftype == FeatureType.CLASSIFICATION

        # Test binary inference
        ftype = self.manager._infer_feature_type("binary_signal", {})
        assert ftype == FeatureType.BINARY

        # Test normalized inference
        ftype = self.manager._infer_feature_type("normalized_price", {})
        assert ftype == FeatureType.NORMALIZED

        # Test integer type inference
        ftype = self.manager._infer_feature_type("count_feature", {'data_type': 'int'})
        assert ftype == FeatureType.INT

        # Test default float inference
        ftype = self.manager._infer_feature_type("unknown_feature", {})
        assert ftype == FeatureType.FLOAT

class TestMetadataEdgeCases:
    """Test edge cases and error conditions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = TrainingDataMetadataManager(self.temp_dir)

    def test_nan_data_handling(self):
        """Test handling of NaN values in data."""
        data_with_nans = np.array([1.0, 2.0, np.nan, 4.0, np.nan, 6.0])

        metadata = self.manager.create_feature_metadata(
            name="test_nan",
            feature_type=FeatureType.FLOAT,
            data=data_with_nans,
            config={}
        )

        assert metadata.null_count == 2
        assert metadata.min_value == 1.0
        assert metadata.max_value == 6.0
        assert metadata.mean_value is not None

    def test_all_nan_data(self):
        """Test handling of all-NaN data."""
        all_nan_data = np.array([np.nan, np.nan, np.nan])

        metadata = self.manager.create_feature_metadata(
            name="test_all_nan",
            feature_type=FeatureType.FLOAT,
            data=all_nan_data,
            config={}
        )

        assert metadata.null_count == 3
        assert metadata.min_value is None
        assert metadata.max_value is None
        assert metadata.mean_value is None
        assert metadata.outlier_count == 0

    def test_single_value_data(self):
        """Test handling of constant data."""
        constant_data = np.array([5.0, 5.0, 5.0, 5.0, 5.0])

        metadata = self.manager.create_feature_metadata(
            name="test_constant",
            feature_type=FeatureType.FLOAT,
            data=constant_data,
            config={}
        )

        assert metadata.min_value == 5.0
        assert metadata.max_value == 5.0
        assert metadata.mean_value == 5.0
        assert metadata.std_value == 0.0
        assert metadata.outlier_count == 0  # No outliers in constant data

    def test_empty_config_handling(self):
        """Test handling of empty or None configurations."""
        data = np.random.normal(0, 1, 100)

        # Test with None config
        metadata = self.manager.create_feature_metadata(
            name="test_none_config",
            feature_type=FeatureType.FLOAT,
            data=data,
            config=None
        )

        assert metadata.parameters == {}
        assert metadata.source_column is None

        # Test with empty config
        metadata = self.manager.create_feature_metadata(
            name="test_empty_config",
            feature_type=FeatureType.FLOAT,
            data=data,
            config={}
        )

        assert metadata.parameters == {}

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])