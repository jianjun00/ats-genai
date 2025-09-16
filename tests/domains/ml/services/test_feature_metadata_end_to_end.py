#!/usr/bin/env python3
"""
End-to-End Tests for Feature Metadata Workflow
"""

import pytest
import tempfile
import shutil
import numpy as np
import sys
import os
import json
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.training_data.generators.training_data_metadata import (
    TrainingDataMetadataManager, FeatureType, VisualizationType
)
from services.dataset_service import DatasetService
from clients.dataset_client import DatasetClient

class TestFeatureMetadataEndToEnd:
    """Test complete feature metadata workflow from generation to API access."""

    @pytest.fixture
    def temp_workspace(self):
        """Create temporary workspace for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_training_data(self):
        """Generate sample training data for testing."""
        np.random.seed(42)

        # Generate OHLCV data
        n_sequences = 100
        sequence_length = 50

        base_prices = np.random.uniform(100, 200, (n_sequences, 1))
        price_changes = np.random.normal(0, 0.02, (n_sequences, sequence_length))
        prices = base_prices * np.exp(np.cumsum(price_changes, axis=1))

        # Create OHLC from prices with realistic relationships
        opens = prices
        highs = prices * (1 + np.random.uniform(0, 0.02, prices.shape))
        lows = prices * (1 - np.random.uniform(0, 0.02, prices.shape))
        closes = opens + np.random.normal(0, 1, prices.shape)

        # Ensure OHLC consistency
        highs = np.maximum(highs, np.maximum(opens, closes))
        lows = np.minimum(lows, np.minimum(opens, closes))

        # Generate volume
        volumes = np.random.uniform(10000, 100000, (n_sequences, sequence_length))

        # Generate technical indicators
        envelope_top = highs * 1.025
        envelope_bot = lows * 0.975
        pldot = np.where(np.random.random(prices.shape) < 0.1, lows * 0.99, 0)

        # BX Trender indicators
        bx_basic = np.random.uniform(-1, 1, prices.shape)
        bx_directional = np.random.uniform(-1, 1, prices.shape)
        bx_volume_weighted = bx_basic * (volumes / np.mean(volumes, axis=1, keepdims=True))

        # Stack all features
        features_data = np.stack([
            opens, highs, lows, closes, volumes,
            envelope_top, envelope_bot, pldot,
            bx_basic, bx_directional, bx_volume_weighted
        ], axis=2)

        # Generate labels (future returns)
        returns_1h = np.random.normal(0.001, 0.02, (n_sequences, 1))
        returns_1d = np.random.normal(0.005, 0.05, (n_sequences, 1))
        labels_data = np.stack([returns_1h, returns_1d], axis=2)

        return {
            'features_data': features_data,
            'labels_data': labels_data,
            'feature_names': [
                'open', 'high', 'low', 'close', 'volume',
                'envelope_top', 'envelope_bot', 'pldot',
                'BXTrenderBasic_14', 'BXTrenderDirectional_14', 'BXTrenderVolumeWeighted_14'
            ],
            'label_names': ['return_1h', 'return_1d'],
            'symbols': ['AAPL', 'TSLA'],
            'date_range': {'start': '2024-01-01', 'end': '2024-12-31'}
        }

    def test_complete_metadata_generation_workflow(self, temp_workspace, sample_training_data):
        """Test complete metadata generation from training data to JSON storage."""

        # Step 1: Initialize metadata manager
        metadata_manager = TrainingDataMetadataManager(temp_workspace)

        # Step 2: Create feature configurations
        feature_configs = [
            {'source_column': 'open'},
            {'source_column': 'high'},
            {'source_column': 'low'},
            {'source_column': 'close'},
            {'source_column': 'volume'},
            {'indicator_type': 'price_envelope', 'percentage': 2.5},
            {'indicator_type': 'price_envelope', 'percentage': 2.5},
            {'indicator_type': 'pivot_low_dots'},
            {'indicator_type': 'bx_trender', 'variant': 'basic', 'window_size': 14},
            {'indicator_type': 'bx_trender', 'variant': 'directional', 'window_size': 14},
            {'indicator_type': 'bx_trender', 'variant': 'volume_weighted', 'window_size': 14}
        ]

        label_configs = [
            {'label_type': 'return', 'lead_periods': 1},
            {'label_type': 'return', 'lead_periods': 24}
        ]

        # Step 3: Generate comprehensive metadata
        metadata = metadata_manager.create_training_metadata(
            dataset_name="e2e_test_dataset",
            features_data=sample_training_data['features_data'],
            labels_data=sample_training_data['labels_data'],
            feature_names=sample_training_data['feature_names'],
            label_names=sample_training_data['label_names'],
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=sample_training_data['symbols'],
            date_range=sample_training_data['date_range'],
            gin_config_path='config/test.gin',
            data_sources=['firstrate']
        )

        # Step 4: Validate metadata structure
        assert metadata.dataset_name == "e2e_test_dataset"
        assert metadata.total_sequences == 100
        assert metadata.sequence_length == 50
        assert metadata.feature_count == 11
        assert metadata.label_count == 2
        assert len(metadata.features) == 11
        assert len(metadata.labels) == 2

        # Step 5: Validate feature metadata details
        ohlcv_features = ['open', 'high', 'low', 'close', 'volume']
        indicator_features = ['envelope_top', 'envelope_bot', 'pldot']
        bx_features = ['BXTrenderBasic_14', 'BXTrenderDirectional_14', 'BXTrenderVolumeWeighted_14']

        for feature in metadata.features:
            # Check enhanced fields are present
            assert len(feature.shape) == 3  # [sequences, timesteps, features]
            assert feature.min_value is not None
            assert feature.max_value is not None
            assert feature.mean_value is not None
            assert feature.std_value is not None
            assert hasattr(feature, 'outlier_count')
            assert hasattr(feature, 'visualization_hints')
            assert hasattr(feature, 'technical_indicator_params')

            # Check feature type inference
            if feature.name in ohlcv_features:
                if feature.name == 'volume':
                    assert feature.feature_type == FeatureType.VOLUME_INDICATOR
                else:
                    assert feature.feature_type == FeatureType.OHLC
            elif feature.name in indicator_features or feature.name in bx_features:
                assert feature.feature_type == FeatureType.PRICE_INDICATOR

            # Check visualization hints
            assert 'color_scheme' in feature.visualization_hints
            assert 'scale_type' in feature.visualization_hints
            assert 'is_primary_indicator' in feature.visualization_hints

        # Step 6: Validate BX Trender parameter extraction
        bx_basic = next(f for f in metadata.features if f.name == 'BXTrenderBasic_14')
        assert bx_basic.technical_indicator_params['indicator_type'] == 'bx_trender'
        assert bx_basic.technical_indicator_params['variant'] == 'basic'

        bx_volume = next(f for f in metadata.features if f.name == 'BXTrenderVolumeWeighted_14')
        assert bx_volume.technical_indicator_params['variant'] == 'volume_weighted'

        # Step 7: Validate label metadata
        for label in metadata.labels:
            assert len(label.shape) == 2  # [sequences, predictions]
            assert label.label_type == 'return'
            assert hasattr(label, 'outlier_count')

        return_1h = next(l for l in metadata.labels if l.name == 'return_1h')
        assert return_1h.lead_periods == 1

        return_1d = next(l for l in metadata.labels if l.name == 'return_1d')
        assert return_1d.lead_periods == 24

        # Step 8: Save and reload metadata
        metadata_file = metadata_manager.save_metadata(metadata, "e2e_test_metadata.json")
        assert Path(metadata_file).exists()

        with open(metadata_file, 'r') as f:
            saved_data = json.load(f)

        # Validate saved structure
        assert saved_data['dataset_name'] == "e2e_test_dataset"
        assert len(saved_data['features']) == 11
        assert len(saved_data['labels']) == 2
        assert 'data_quality_metrics' in saved_data

        # Check that enhanced fields are serialized
        saved_feature = saved_data['features'][0]
        assert 'shape' in saved_feature
        assert 'outlier_count' in saved_feature
        assert 'visualization_hints' in saved_feature
        assert 'technical_indicator_params' in saved_feature

        print("✅ Complete metadata generation workflow validated")

    def test_dataset_service_integration_workflow(self, sample_training_data):
        """Test integration between metadata generation and dataset service."""

        # Step 1: Create mock dataset service with feature metadata
        dataset_service = DatasetService({
            'host': 'localhost', 'port': 3432, 'database': 'test_db',
            'user': 'test_user', 'password': 'test_password'
        })

        # Step 2: Create realistic feature metadata
        feature_metadata = {
            "features": [],
            "labels": [],
            "metadata_version": "1.0",
            "creation_timestamp": datetime.now().isoformat(),
            "total_features": len(sample_training_data['feature_names']),
            "total_labels": len(sample_training_data['label_names']),
            "data_quality_metrics": {
                "feature_completeness": 0.98,
                "label_completeness": 0.95,
                "overall_quality_score": 0.96
            }
        }

        # Add feature metadata
        for i, feature_name in enumerate(sample_training_data['feature_names']):
            feature_data = sample_training_data['features_data'][:, :, i]

            # Determine feature type
            if feature_name in ['open', 'high', 'low', 'close']:
                feature_type = "OHLC"
                viz_type = "CANDLESTICK"
                color_scheme = "green_red"
                is_primary = True
            elif feature_name == 'volume':
                feature_type = "VOLUME_INDICATOR"
                viz_type = "BAR_CHART"
                color_scheme = "orange"
                is_primary = False
            else:
                feature_type = "PRICE_INDICATOR"
                viz_type = "LINE_CHART"
                color_scheme = "blue"
                is_primary = False

            feature_metadata["features"].append({
                "name": feature_name,
                "feature_type": feature_type,
                "data_type": "float64",
                "shape": list(feature_data.shape),
                "description": f"{feature_name} indicator",
                "statistics": {
                    "min_value": float(np.nanmin(feature_data)),
                    "max_value": float(np.nanmax(feature_data)),
                    "mean_value": float(np.nanmean(feature_data)),
                    "std_value": float(np.nanstd(feature_data)),
                    "outlier_count": int(np.sum(np.abs(feature_data - np.nanmean(feature_data)) > 3 * np.nanstd(feature_data)))
                },
                "visualization_hints": {
                    "visualization_type": viz_type,
                    "color_scheme": color_scheme,
                    "is_primary_indicator": is_primary
                },
                "technical_indicator_params": {
                    "indicator_type": feature_name.lower().replace('_', '_')
                }
            })

        # Add label metadata
        for i, label_name in enumerate(sample_training_data['label_names']):
            label_data = sample_training_data['labels_data'][:, :, i]

            feature_metadata["labels"].append({
                "name": label_name,
                "label_type": "return",
                "data_type": "float64",
                "shape": list(label_data.shape),
                "description": f"{label_name} prediction",
                "lead_periods": 1 if '1h' in label_name else 24,
                "statistics": {
                    "min_value": float(np.nanmin(label_data)),
                    "max_value": float(np.nanmax(label_data)),
                    "mean_value": float(np.nanmean(label_data)),
                    "std_value": float(np.nanstd(label_data))
                }
            })

        # Step 3: Test dataset service API methods
        mock_db_result = {
            'feature_metadata': feature_metadata,
            'dataset_name': 'e2e_test_dataset',
            'technical_indicators': ','.join(sample_training_data['feature_names'][5:])
        }

        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = mock_db_result

            # Test feature metadata retrieval
            result = dataset_service.get_feature_metadata(1)

            assert result['metadata_version'] == '1.0'
            assert len(result['features']) == 11
            assert len(result['labels']) == 2

            # Validate feature details
            ohlc_features = [f for f in result['features'] if f['feature_type'] == 'OHLC']
            assert len(ohlc_features) == 4  # open, high, low, close

            volume_features = [f for f in result['features'] if f['feature_type'] == 'VOLUME_INDICATOR']
            assert len(volume_features) == 1  # volume

            indicator_features = [f for f in result['features'] if f['feature_type'] == 'PRICE_INDICATOR']
            assert len(indicator_features) == 6  # envelope_top, envelope_bot, pldot, BX indicators

            # Validate BX Trender features
            bx_features = [f for f in result['features'] if 'BXTrender' in f['name']]
            assert len(bx_features) == 3

            for bx_feature in bx_features:
                assert bx_feature['feature_type'] == 'PRICE_INDICATOR'
                assert bx_feature['visualization_hints']['visualization_type'] == 'LINE_CHART'

        print("✅ Dataset service integration workflow validated")

    def test_feature_search_and_compatibility_workflow(self):
        """Test feature-based dataset search and compatibility checking."""

        # Step 1: Create dataset service
        dataset_service = DatasetService({
            'host': 'localhost', 'port': 3432, 'database': 'test_db',
            'user': 'test_user', 'password': 'test_password'
        })

        # Step 2: Create mock datasets with different feature sets
        dataset1_features = {
            "features": [
                {"name": "open", "feature_type": "OHLC", "data_type": "float64", "shape": [50, 1]},
                {"name": "close", "feature_type": "OHLC", "data_type": "float64", "shape": [50, 1]},
                {"name": "volume", "feature_type": "VOLUME_INDICATOR", "data_type": "float64", "shape": [50, 1]},
                {"name": "envelope_top", "feature_type": "PRICE_INDICATOR", "data_type": "float64", "shape": [50, 1]},
                {"name": "BXTrenderBasic_14", "feature_type": "PRICE_INDICATOR", "data_type": "float64", "shape": [50, 1]}
            ],
            "labels": [],
            "metadata_version": "1.0"
        }

        dataset2_features = {
            "features": [
                {"name": "open", "feature_type": "OHLC", "data_type": "float64", "shape": [50, 1]},
                {"name": "close", "feature_type": "OHLC", "data_type": "float64", "shape": [50, 1]},
                {"name": "volume", "feature_type": "VOLUME_INDICATOR", "data_type": "float64", "shape": [50, 1]},
                {"name": "RSI_14", "feature_type": "PRICE_INDICATOR", "data_type": "float64", "shape": [50, 1]},
                {"name": "BXTrenderDirectional_14", "feature_type": "PRICE_INDICATOR", "data_type": "float64", "shape": [50, 1]}
            ],
            "labels": [],
            "metadata_version": "1.0"
        }

        dataset3_features = {
            "features": [
                {"name": "open", "feature_type": "OHLC", "data_type": "float32", "shape": [100, 1]},  # Different type/shape
                {"name": "close", "feature_type": "OHLC", "data_type": "float64", "shape": [50, 1]},
                {"name": "high", "feature_type": "OHLC", "data_type": "float64", "shape": [50, 1]},  # Additional feature
                {"name": "SMA_20", "feature_type": "PRICE_INDICATOR", "data_type": "float64", "shape": [50, 1]}
            ],
            "labels": [],
            "metadata_version": "1.0"
        }

        # Mock database rows
        mock_rows = [
            {
                'id': 1, 'dataset_name': 'dataset_1', 'symbols': 'AAPL',
                'total_sequences': 1000, 'feature_count': 5, 'label_count': 0,
                'data_quality_score': 0.95, 'feature_completeness': 0.98,
                'label_completeness': 0.95, 'file_size_mb': 100.0,
                'technical_indicators': 'envelope_top,BXTrenderBasic_14',
                'sequence_length': 50, 'date_range_start': datetime(2024, 1, 1).date(),
                'date_range_end': datetime(2024, 12, 31).date(),
                'creation_timestamp': datetime.now(), 'run_id': 1
            },
            {
                'id': 2, 'dataset_name': 'dataset_2', 'symbols': 'TSLA',
                'total_sequences': 800, 'feature_count': 5, 'label_count': 0,
                'data_quality_score': 0.92, 'feature_completeness': 0.96,
                'label_completeness': 0.94, 'file_size_mb': 80.0,
                'technical_indicators': 'RSI_14,BXTrenderDirectional_14',
                'sequence_length': 50, 'date_range_start': datetime(2024, 1, 1).date(),
                'date_range_end': datetime(2024, 12, 31).date(),
                'creation_timestamp': datetime.now(), 'run_id': 2
            }
        ]

        # Step 3: Test feature-based search
        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchall.return_value = mock_rows

            # Search for datasets with OHLCV features
            datasets = dataset_service.find_datasets_by_features(['open', 'close', 'volume'])

            assert len(datasets) == 2
            assert datasets[0].dataset_name == 'dataset_1'
            assert datasets[1].dataset_name == 'dataset_2'

            # Verify SQL construction
            call_args = mock_cursor.execute.call_args
            query, params = call_args[0]
            assert 'feature_metadata IS NOT NULL' in query
            assert '%open%' in params
            assert '%close%' in params
            assert '%volume%' in params

        # Step 4: Test schema compatibility checking
        with patch.object(dataset_service, 'get_feature_metadata') as mock_get_metadata:
            # Test compatible datasets
            mock_get_metadata.side_effect = [dataset1_features, dataset2_features]

            compatibility = dataset_service.compare_feature_schemas(1, 2)

            assert compatibility['compatible'] == True
            assert len(compatibility['common_features']) == 3  # open, close, volume
            assert len(compatibility['missing_in_dataset_1']) == 2  # RSI_14, BXTrenderDirectional_14
            assert len(compatibility['missing_in_dataset_2']) == 2  # envelope_top, BXTrenderBasic_14
            assert len(compatibility['type_mismatches']) == 0
            assert len(compatibility['shape_mismatches']) == 0
            assert compatibility['compatibility_score'] == 0.6  # 3 common / 5 total

            # Test incompatible datasets (type/shape mismatches)
            mock_get_metadata.side_effect = [dataset1_features, dataset3_features]

            compatibility = dataset_service.compare_feature_schemas(1, 3)

            assert compatibility['compatible'] == False
            assert len(compatibility['common_features']) == 2  # open, close
            assert len(compatibility['type_mismatches']) == 1  # open: float64 vs float32
            assert len(compatibility['shape_mismatches']) == 1  # open: [50,1] vs [100,1]

            type_mismatch = compatibility['type_mismatches'][0]
            assert type_mismatch['feature'] == 'open'
            assert type_mismatch['dataset_1_type'] == 'float64'
            assert type_mismatch['dataset_2_type'] == 'float32'

            shape_mismatch = compatibility['shape_mismatches'][0]
            assert shape_mismatch['feature'] == 'open'
            assert shape_mismatch['dataset_1_shape'] == [50, 1]
            assert shape_mismatch['dataset_2_shape'] == [100, 1]

        print("✅ Feature search and compatibility workflow validated")

    def test_client_integration_workflow(self):
        """Test dataset client integration with feature metadata."""

        # Step 1: Create mock dataset client
        dataset_client = DatasetClient()

        # Step 2: Mock dataset service responses
        mock_datasets = [
            Mock(
                dataset_id=1,
                dataset_name='high_quality_dataset',
                symbols=['AAPL', 'TSLA'],
                data_quality_score=0.95,
                total_sequences=2000,
                feature_count=15,
                technical_indicators=['envelope_top', 'envelope_bot', 'BXTrenderBasic_14'],
                file_paths=['/path/to/data.riegeli']
            ),
            Mock(
                dataset_id=2,
                dataset_name='medium_quality_dataset',
                symbols=['MSFT'],
                data_quality_score=0.85,
                total_sequences=1500,
                feature_count=12,
                technical_indicators=['SMA_20', 'EMA_14'],
                file_paths=['/path/to/data2.riegeli']
            )
        ]

        # Mock feature metadata for dataset ranking
        feature_metadata_1 = {
            "features": [
                {"name": "open", "feature_type": "OHLC"},
                {"name": "close", "feature_type": "OHLC"},
                {"name": "volume", "feature_type": "VOLUME_INDICATOR"},
                {"name": "envelope_top", "feature_type": "PRICE_INDICATOR"},
                {"name": "envelope_bot", "feature_type": "PRICE_INDICATOR"},
                {"name": "BXTrenderBasic_14", "feature_type": "PRICE_INDICATOR"}
            ]
        }

        feature_metadata_2 = {
            "features": [
                {"name": "open", "feature_type": "OHLC"},
                {"name": "close", "feature_type": "OHLC"},
                {"name": "SMA_20", "feature_type": "PRICE_INDICATOR"},
                {"name": "EMA_14", "feature_type": "PRICE_INDICATOR"}
            ]
        }

        # Step 3: Test intelligent dataset discovery with feature requirements
        with patch.object(dataset_client.service, 'list_datasets') as mock_list:
            with patch.object(dataset_client.service, 'get_feature_metadata') as mock_get_metadata:
                mock_list.return_value = mock_datasets
                mock_get_metadata.side_effect = [feature_metadata_1, feature_metadata_2]

                # Find dataset with specific feature requirements
                dataset = dataset_client.find_dataset(
                    symbols=['AAPL'],
                    min_sequences=1000,
                    min_quality=0.9
                )

                # Should return the high-quality dataset
                assert dataset is not None
                assert dataset.dataset_id == 1
                assert dataset.dataset_name == 'high_quality_dataset'
                assert dataset.data_quality_score == 0.95

                # Test configuration generation with feature metadata
                config = dataset_client.get_training_data_config(
                    symbols=['AAPL'],
                    min_sequences=1000
                )

                assert config is not None
                assert config['dataset_id'] == 1
                assert config['dataset_name'] == 'high_quality_dataset'
                assert 'feature_metadata' in config or 'technical_indicators' in config

        print("✅ Client integration workflow validated")

    def test_visualization_metadata_workflow(self, sample_training_data):
        """Test visualization metadata generation and usage."""

        # Step 1: Generate metadata with visualization hints
        temp_dir = tempfile.mkdtemp()
        try:
            metadata_manager = TrainingDataMetadataManager(temp_dir)

            # Create metadata with diverse feature types
            feature_configs = [
                {'source_column': 'open', 'visualization_priority': 'high'},
                {'source_column': 'high', 'visualization_priority': 'high'},
                {'source_column': 'low', 'visualization_priority': 'high'},
                {'source_column': 'close', 'visualization_priority': 'high'},
                {'source_column': 'volume', 'visualization_priority': 'medium'},
                {'indicator_type': 'price_envelope', 'percentage': 2.5},
                {'indicator_type': 'price_envelope', 'percentage': 2.5},
                {'indicator_type': 'pivot_low_dots'},
                {'indicator_type': 'bx_trender', 'variant': 'basic'},
                {'indicator_type': 'bx_trender', 'variant': 'directional'},
                {'indicator_type': 'bx_trender', 'variant': 'volume_weighted'}
            ]

            metadata = metadata_manager.create_training_metadata(
                dataset_name="viz_test_dataset",
                features_data=sample_training_data['features_data'],
                labels_data=sample_training_data['labels_data'],
                feature_names=sample_training_data['feature_names'],
                label_names=sample_training_data['label_names'],
                feature_configs=feature_configs,
                label_configs=[{}, {}],
                symbols=['AAPL'],
                date_range={'start': '2024-01-01', 'end': '2024-12-31'}
            )

            # Step 2: Validate visualization hints generation
            for feature in metadata.features:
                assert 'color_scheme' in feature.visualization_hints
                assert 'scale_type' in feature.visualization_hints
                assert 'is_primary_indicator' in feature.visualization_hints

                # Check specific visualization recommendations
                if feature.name in ['open', 'high', 'low', 'close']:
                    assert feature.visualization_hints['color_scheme'] == 'green_red'
                    assert feature.visualization_hints['is_primary_indicator'] == True
                    assert feature.visualization_type == VisualizationType.CANDLESTICK
                elif feature.name == 'volume':
                    assert feature.visualization_hints['color_scheme'] == 'orange'
                    assert feature.visualization_type == VisualizationType.BAR_CHART
                else:  # Technical indicators
                    assert feature.visualization_type == VisualizationType.LINE_CHART
                    assert feature.visualization_hints['is_primary_indicator'] == False

            # Step 3: Test multi-panel visualization configuration
            # Group features by visualization priority
            primary_features = [f for f in metadata.features if f.visualization_hints.get('is_primary_indicator')]
            secondary_features = [f for f in metadata.features if not f.visualization_hints.get('is_primary_indicator')]

            assert len(primary_features) == 4  # OHLC features
            assert len(secondary_features) == 7  # Volume + indicators

            # Validate chart layout recommendations
            candlestick_features = [f for f in metadata.features if f.visualization_type == VisualizationType.CANDLESTICK]
            line_features = [f for f in metadata.features if f.visualization_type == VisualizationType.LINE_CHART]
            bar_features = [f for f in metadata.features if f.visualization_type == VisualizationType.BAR_CHART]

            assert len(candlestick_features) == 4  # OHLC
            assert len(line_features) == 6  # Technical indicators
            assert len(bar_features) == 1  # Volume

            print("✅ Visualization metadata workflow validated")

        finally:
            shutil.rmtree(temp_dir)

    def test_error_handling_and_recovery_workflow(self):
        """Test error handling and recovery mechanisms in the metadata workflow."""

        # Step 1: Test metadata generation with corrupted data
        temp_dir = tempfile.mkdtemp()
        try:
            metadata_manager = TrainingDataMetadataManager(temp_dir)

            # Create data with various issues
            corrupted_features = np.array([
                [1.0, 2.0, np.nan],  # NaN values
                [np.inf, 4.0, 5.0],  # Infinite values
                [6.0, 7.0, 8.0]
            ])

            # Test feature metadata creation with problematic data
            metadata = metadata_manager.create_feature_metadata(
                name="corrupted_feature",
                feature_type=FeatureType.FLOAT,
                data=corrupted_features,
                config={}
            )

            # Should handle corrupted data gracefully
            assert metadata.name == "corrupted_feature"
            assert metadata.null_count > 0  # Should detect NaN
            assert metadata.min_value is not None  # Should work with finite values
            assert metadata.max_value is not None

        finally:
            shutil.rmtree(temp_dir)

        # Step 2: Test dataset service error recovery
        dataset_service = DatasetService({
            'host': 'localhost', 'port': 3432, 'database': 'test_db',
            'user': 'test_user', 'password': 'test_password'
        })

        # Test database connection failure
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = Exception("Database connection failed")

            result = dataset_service.get_feature_metadata(1)
            assert 'error' in result
            assert 'Database connection failed' in result['error']

            # Should return empty metadata structure
            assert result['features'] == []
            assert result['labels'] == []
            assert result['metadata_version'] == '1.0'

        # Step 3: Test missing metadata recovery
        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_cursor.fetchone.return_value = {
                'feature_metadata': None,  # No metadata available
                'dataset_name': 'legacy_dataset',
                'technical_indicators': 'SMA_20,EMA_14,RSI_14'
            }

            result = dataset_service.get_feature_metadata(1)

            # Should generate basic metadata as fallback
            assert len(result['features']) > 0
            assert 'note' in result
            assert 'Basic metadata generated' in result['note']

            feature_names = [f['name'] for f in result['features']]
            assert 'open' in feature_names
            assert 'SMA_20' in feature_names
            assert 'EMA_14' in feature_names
            assert 'RSI_14' in feature_names

        print("✅ Error handling and recovery workflow validated")

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])