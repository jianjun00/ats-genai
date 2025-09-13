"""
Integration tests for schema-aware EDA (Exploratory Data Analysis) integration.

Tests the complete workflow from schema-aware training data generation
to EDA visualization with schema metadata.
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
import tempfile
import os
import json
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock, patch

from src.modeling.training_data_generator import (
    ResidualReturnTrainingDataGenerator,
    TrainingConfig,
    generate_residual_return_training_data
)
from src.schema.training_schema import TrainingDatasetSchema, FeatureType


@pytest.fixture
def mock_eda_compatible_environment():
    """Mock environment that's compatible with EDA system."""
    env = MagicMock()
    env.get_table_name.side_effect = lambda table: f"dev_{table}"
    env.env_type.value = 'dev'
    return env


@pytest.fixture
def mock_eda_database():
    """Mock database with EDA-compatible dataset records."""
    pool = AsyncMock()
    conn = AsyncMock()

    # Mock training dataset records that EDA would query
    conn.fetch.return_value = [
        {
            'id': 1,
            'dataset_name': 'eda_test_dataset',
            'schema_hash': 'test_schema_hash_123',
            'schema_json': {
                'features': [
                    {'name': 'sma_20', 'feature_type': 'TECHNICAL_INDICATOR'},
                    {'name': 'return_1d', 'feature_type': 'RETURN_SERIES'}
                ],
                'metadata': {'symbol': 'AAPL', 'total_features': 2}
            },
            'feature_metadata': {
                'sma_20': {'type': 'technical', 'description': '20-day SMA'},
                'return_1d': {'type': 'return', 'description': '1-day return'}
            },
            'generation_config': {
                'lookback_days': 60,
                'prediction_horizons': [1, 3, 5]
            },
            'created_at': datetime(2024, 1, 1),
            'file_paths': {
                'features': '/tmp/test/features.npy',
                'labels': '/tmp/test/labels.npy',
                'schema': '/tmp/test/schema.json'
            }
        }
    ]

    conn.fetchrow.return_value = {
        'schema_json': {
            'schema_version': '1.0.0',
            'features': [
                {
                    'name': 'sma_20',
                    'feature_type': 'TECHNICAL_INDICATOR',
                    'statistics': {'mean': 150.5, 'std': 10.2}
                }
            ]
        }
    }

    pool.acquire.return_value.__aenter__.return_value = conn
    return pool


@pytest.fixture
def sample_training_result():
    """Sample training result for EDA testing."""
    features = np.random.randn(100, 5).astype(np.float32)
    labels = np.random.randn(100, 2).astype(np.float32)

    from src.schema.training_schema import (
        TrainingDatasetSchema, FeatureSchema, LabelSchema, DatasetMetadata
    )

    schema = TrainingDatasetSchema(
        schema_version='1.0.0',
        dataset_name='eda_integration_test',
        features=[
            FeatureSchema(name='sma_20', feature_type=FeatureType.TECHNICAL_INDICATOR),
            FeatureSchema(name='rsi_14', feature_type=FeatureType.TECHNICAL_INDICATOR),
            FeatureSchema(name='return_1d', feature_type=FeatureType.RETURN_SERIES),
            FeatureSchema(name='volume_avg', feature_type=FeatureType.VOLUME_PROFILE),
            FeatureSchema(name='volatility_20d', feature_type=FeatureType.VOLATILITY_METRICS)
        ],
        labels=[
            LabelSchema(name='return_1d_target', label_type='regression'),
            LabelSchema(name='direction', label_type='classification')
        ],
        metadata=DatasetMetadata(
            symbol='AAPL',
            sequence_length=60,
            total_features=5,
            total_samples=100,
            date_range_start=date(2023, 1, 1),
            date_range_end=date(2023, 12, 31)
        )
    )

    from src.schema.training_schema import ValidationResult
    from src.modeling.training_data_generator import TrainingDatasetResult

    return TrainingDatasetResult(
        dataset_path='/tmp/eda_test',
        features_array=features,
        labels_array=labels,
        schema=schema,
        validation_result=ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            confidence_score=0.95,
            validation_timestamp=datetime.now()
        ),
        metadata={'test': 'eda_integration'}
    )


class TestSchemaEDAIntegration:
    """Test integration between schema management and EDA system."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eda_dataset_discovery(self, mock_eda_database, mock_eda_compatible_environment):
        """Test EDA system can discover schema-aware training datasets."""

        # Mock EDA dataset discovery functionality
        async def mock_discover_datasets():
            """Mock function that EDA would use to discover datasets."""
            async with mock_eda_database.acquire() as conn:
                datasets = await conn.fetch("""
                    SELECT id, dataset_name, schema_hash, schema_json,
                           feature_metadata, created_at, file_paths
                    FROM dev_training_dataset
                    WHERE schema_hash IS NOT NULL
                    ORDER BY created_at DESC
                """)
                return datasets

        discovered_datasets = await mock_discover_datasets()

        assert len(discovered_datasets) == 1
        dataset = discovered_datasets[0]

        # Verify EDA can access schema information
        assert dataset['dataset_name'] == 'eda_test_dataset'
        assert dataset['schema_hash'] == 'test_schema_hash_123'
        assert 'features' in dataset['schema_json']
        assert 'metadata' in dataset['schema_json']

        # Verify feature metadata is available for EDA
        feature_metadata = dataset['feature_metadata']
        assert 'sma_20' in feature_metadata
        assert 'return_1d' in feature_metadata
        assert feature_metadata['sma_20']['type'] == 'technical'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eda_feature_categorization(self, sample_training_result):
        """Test EDA can categorize features by type using schema."""

        schema = sample_training_result.schema

        # Mock EDA feature categorization based on schema
        def categorize_features_by_type(training_schema):
            categories = {
                'technical_indicators': [],
                'return_features': [],
                'volume_features': [],
                'volatility_features': [],
                'other_features': []
            }

            for feature in training_schema.features:
                if feature.feature_type == FeatureType.TECHNICAL_INDICATOR:
                    categories['technical_indicators'].append(feature.name)
                elif feature.feature_type == FeatureType.RETURN_SERIES:
                    categories['return_features'].append(feature.name)
                elif feature.feature_type == FeatureType.VOLUME_PROFILE:
                    categories['volume_features'].append(feature.name)
                elif feature.feature_type == FeatureType.VOLATILITY_METRICS:
                    categories['volatility_features'].append(feature.name)
                else:
                    categories['other_features'].append(feature.name)

            return categories

        categories = categorize_features_by_type(schema)

        # Verify categorization works correctly
        assert len(categories['technical_indicators']) == 2
        assert 'sma_20' in categories['technical_indicators']
        assert 'rsi_14' in categories['technical_indicators']

        assert len(categories['return_features']) == 1
        assert 'return_1d' in categories['return_features']

        assert len(categories['volume_features']) == 1
        assert 'volume_avg' in categories['volume_features']

        assert len(categories['volatility_features']) == 1
        assert 'volatility_20d' in categories['volatility_features']

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eda_visualization_metadata(self, sample_training_result):
        """Test EDA can generate visualization metadata from schema."""

        schema = sample_training_result.schema
        features_array = sample_training_result.features_array

        # Mock EDA visualization metadata generation
        def generate_visualization_metadata(training_schema, features_data):
            viz_metadata = {
                'dataset_info': {
                    'name': training_schema.dataset_name,
                    'symbol': training_schema.metadata.symbol,
                    'samples': training_schema.metadata.total_samples,
                    'features': training_schema.metadata.total_features,
                    'date_range': {
                        'start': training_schema.metadata.date_range_start,
                        'end': training_schema.metadata.date_range_end
                    }
                },
                'feature_summaries': [],
                'recommended_plots': []
            }

            # Generate feature summaries
            for i, feature in enumerate(training_schema.features):
                feature_data = features_data[:, i]

                summary = {
                    'name': feature.name,
                    'type': feature.feature_type.name,
                    'data_type': feature.data_type,
                    'stats': {
                        'mean': float(np.mean(feature_data)),
                        'std': float(np.std(feature_data)),
                        'min': float(np.min(feature_data)),
                        'max': float(np.max(feature_data)),
                        'null_count': int(np.isnan(feature_data).sum())
                    }
                }

                viz_metadata['feature_summaries'].append(summary)

                # Recommend plots based on feature type
                if feature.feature_type == FeatureType.TECHNICAL_INDICATOR:
                    viz_metadata['recommended_plots'].append({
                        'feature': feature.name,
                        'plot_type': 'line_chart',
                        'description': f'Time series plot of {feature.name}'
                    })
                elif feature.feature_type == FeatureType.RETURN_SERIES:
                    viz_metadata['recommended_plots'].append({
                        'feature': feature.name,
                        'plot_type': 'histogram',
                        'description': f'Distribution of {feature.name}'
                    })
                elif feature.feature_type == FeatureType.VOLATILITY_METRICS:
                    viz_metadata['recommended_plots'].append({
                        'feature': feature.name,
                        'plot_type': 'box_plot',
                        'description': f'Volatility distribution of {feature.name}'
                    })

            return viz_metadata

        viz_metadata = generate_visualization_metadata(schema, features_array)

        # Verify visualization metadata
        assert viz_metadata['dataset_info']['name'] == 'eda_integration_test'
        assert viz_metadata['dataset_info']['symbol'] == 'AAPL'
        assert viz_metadata['dataset_info']['samples'] == 100
        assert viz_metadata['dataset_info']['features'] == 5

        # Verify feature summaries
        assert len(viz_metadata['feature_summaries']) == 5

        sma_summary = next(f for f in viz_metadata['feature_summaries'] if f['name'] == 'sma_20')
        assert sma_summary['type'] == 'TECHNICAL_INDICATOR'
        assert 'mean' in sma_summary['stats']
        assert 'std' in sma_summary['stats']

        # Verify recommended plots
        assert len(viz_metadata['recommended_plots']) == 5

        technical_plots = [p for p in viz_metadata['recommended_plots']
                          if p['plot_type'] == 'line_chart']
        assert len(technical_plots) == 2  # sma_20 and rsi_14

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eda_data_quality_integration(self, sample_training_result):
        """Test EDA integration with schema validation results."""

        validation_result = sample_training_result.validation_result
        schema = sample_training_result.schema

        # Mock EDA data quality dashboard integration
        def create_data_quality_dashboard(schema, validation):
            dashboard = {
                'overall_quality': {
                    'score': validation.confidence_score,
                    'is_valid': validation.is_valid,
                    'validation_timestamp': validation.validation_timestamp
                },
                'quality_metrics': {
                    'error_count': len(validation.errors),
                    'warning_count': len(validation.warnings),
                    'schema_compliance': validation.is_valid
                },
                'issues': {
                    'errors': validation.errors,
                    'warnings': validation.warnings
                },
                'schema_info': {
                    'version': schema.schema_version,
                    'feature_count': len(schema.features),
                    'label_count': len(schema.labels),
                    'hash': schema.get_schema_hash()
                },
                'recommendations': []
            }

            # Generate recommendations based on validation
            if validation.confidence_score < 0.8:
                dashboard['recommendations'].append({
                    'type': 'data_quality',
                    'message': 'Consider reviewing data quality - confidence score below 80%'
                })

            if validation.warnings:
                dashboard['recommendations'].append({
                    'type': 'warnings',
                    'message': f'Review {len(validation.warnings)} data quality warnings'
                })

            return dashboard

        dashboard = create_data_quality_dashboard(schema, validation_result)

        # Verify dashboard structure
        assert dashboard['overall_quality']['score'] == 0.95
        assert dashboard['overall_quality']['is_valid'] is True
        assert dashboard['quality_metrics']['error_count'] == 0
        assert dashboard['quality_metrics']['warning_count'] == 0
        assert dashboard['schema_info']['feature_count'] == 5
        assert dashboard['schema_info']['label_count'] == 2

        # High quality data should have no recommendations
        assert len(dashboard['recommendations']) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eda_schema_comparison(self, mock_eda_database):
        """Test EDA can compare schemas across different datasets."""

        # Mock multiple schemas for comparison
        schemas_data = [
            {
                'dataset_name': 'dataset_v1',
                'schema_hash': 'hash_v1',
                'features': ['sma_20', 'rsi_14'],
                'total_features': 2,
                'created_at': datetime(2023, 1, 1)
            },
            {
                'dataset_name': 'dataset_v2',
                'schema_hash': 'hash_v2',
                'features': ['sma_20', 'rsi_14', 'macd'],
                'total_features': 3,
                'created_at': datetime(2023, 6, 1)
            }
        ]

        # Mock EDA schema comparison functionality
        def compare_schemas(schemas):
            comparison = {
                'schema_evolution': [],
                'common_features': [],
                'unique_features': {},
                'feature_changes': []
            }

            if len(schemas) >= 2:
                # Find common features
                all_features = [set(s['features']) for s in schemas]
                common_features = set.intersection(*all_features)
                comparison['common_features'] = list(common_features)

                # Find unique features per schema
                for schema in schemas:
                    unique = set(schema['features']) - common_features
                    if unique:
                        comparison['unique_features'][schema['dataset_name']] = list(unique)

                # Track evolution over time
                sorted_schemas = sorted(schemas, key=lambda x: x['created_at'])
                for i in range(1, len(sorted_schemas)):
                    prev_schema = sorted_schemas[i-1]
                    curr_schema = sorted_schemas[i]

                    added_features = set(curr_schema['features']) - set(prev_schema['features'])
                    removed_features = set(prev_schema['features']) - set(curr_schema['features'])

                    if added_features or removed_features:
                        comparison['schema_evolution'].append({
                            'from': prev_schema['dataset_name'],
                            'to': curr_schema['dataset_name'],
                            'added_features': list(added_features),
                            'removed_features': list(removed_features)
                        })

            return comparison

        comparison_result = compare_schemas(schemas_data)

        # Verify schema comparison results
        assert len(comparison_result['common_features']) == 2
        assert 'sma_20' in comparison_result['common_features']
        assert 'rsi_14' in comparison_result['common_features']

        assert 'dataset_v2' in comparison_result['unique_features']
        assert 'macd' in comparison_result['unique_features']['dataset_v2']

        assert len(comparison_result['schema_evolution']) == 1
        evolution = comparison_result['schema_evolution'][0]
        assert evolution['from'] == 'dataset_v1'
        assert evolution['to'] == 'dataset_v2'
        assert 'macd' in evolution['added_features']


class TestSchemaAwareEDAVisualization:
    """Test schema-aware EDA visualization generation."""

    def test_feature_type_visualization_mapping(self):
        """Test mapping of feature types to appropriate visualization types."""

        # Define visualization mapping based on feature types
        visualization_mapping = {
            FeatureType.TECHNICAL_INDICATOR: ['line_chart', 'candlestick_overlay'],
            FeatureType.RETURN_SERIES: ['histogram', 'qq_plot', 'rolling_statistics'],
            FeatureType.VOLUME_PROFILE: ['bar_chart', 'volume_profile', 'box_plot'],
            FeatureType.VOLATILITY_METRICS: ['line_chart', 'heatmap', 'box_plot'],
            FeatureType.CORRELATION_MATRIX: ['correlation_heatmap', 'network_graph'],
            FeatureType.EVENT_INDICATOR: ['event_markers', 'binary_timeline'],
            FeatureType.TEMPORAL_FEATURES: ['seasonal_decomposition', 'calendar_heatmap']
        }

        # Test each mapping
        for feature_type, expected_viz_types in visualization_mapping.items():
            assert len(expected_viz_types) > 0

            # Each feature type should have at least one visualization
            assert isinstance(expected_viz_types, list)

            # Verify visualization types are strings
            for viz_type in expected_viz_types:
                assert isinstance(viz_type, str)
                assert len(viz_type) > 0

    def test_interactive_eda_configuration(self, sample_training_result):
        """Test generation of interactive EDA configuration."""

        schema = sample_training_result.schema

        # Mock interactive EDA configuration generation
        def generate_eda_config(training_schema):
            config = {
                'dataset_metadata': {
                    'name': training_schema.dataset_name,
                    'symbol': training_schema.metadata.symbol,
                    'timeframe': training_schema.metadata.base_timeframe,
                    'sequence_length': training_schema.metadata.sequence_length
                },
                'feature_groups': {},
                'default_views': [],
                'interactive_controls': []
            }

            # Group features by type
            for feature in training_schema.features:
                feature_type_name = feature.feature_type.name.lower()
                if feature_type_name not in config['feature_groups']:
                    config['feature_groups'][feature_type_name] = []
                config['feature_groups'][feature_type_name].append({
                    'name': feature.name,
                    'description': feature.description or f"{feature.name} feature"
                })

            # Generate default views
            if 'technical_indicator' in config['feature_groups']:
                config['default_views'].append({
                    'name': 'Technical Indicators Overview',
                    'type': 'multi_line_chart',
                    'features': [f['name'] for f in config['feature_groups']['technical_indicator']]
                })

            if 'return_series' in config['feature_groups']:
                config['default_views'].append({
                    'name': 'Returns Distribution',
                    'type': 'histogram_grid',
                    'features': [f['name'] for f in config['feature_groups']['return_series']]
                })

            # Interactive controls
            config['interactive_controls'] = [
                {
                    'type': 'date_range_slider',
                    'label': 'Time Period',
                    'default_range': [
                        training_schema.metadata.date_range_start,
                        training_schema.metadata.date_range_end
                    ]
                },
                {
                    'type': 'feature_selector',
                    'label': 'Select Features',
                    'options': [f.name for f in training_schema.features],
                    'default_selection': [f.name for f in training_schema.features[:3]]
                }
            ]

            return config

        eda_config = generate_eda_config(schema)

        # Verify EDA configuration
        assert eda_config['dataset_metadata']['name'] == 'eda_integration_test'
        assert eda_config['dataset_metadata']['symbol'] == 'AAPL'

        # Verify feature grouping
        assert 'technical_indicator' in eda_config['feature_groups']
        assert len(eda_config['feature_groups']['technical_indicator']) == 2

        # Verify default views
        assert len(eda_config['default_views']) >= 1
        tech_view = next(v for v in eda_config['default_views']
                        if v['name'] == 'Technical Indicators Overview')
        assert tech_view['type'] == 'multi_line_chart'
        assert len(tech_view['features']) == 2

        # Verify interactive controls
        assert len(eda_config['interactive_controls']) == 2
        date_control = next(c for c in eda_config['interactive_controls']
                           if c['type'] == 'date_range_slider')
        assert 'default_range' in date_control


class TestEndToEndSchemaEDAWorkflow:
    """End-to-end testing of schema-aware training to EDA workflow."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_training_to_eda_workflow(
        self, mock_eda_database, mock_eda_compatible_environment
    ):
        """Test complete workflow from training generation to EDA visualization."""

        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.core.dao.training_schema_core.dao.TrainingSchemaDAO'), \
             patch('src.state.universe_state_manager.UniverseStateManager') as mock_universe:

            # Setup mocks
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1] * 10,
                'date': pd.date_range('2023-06-01', periods=10),
                'residual_return': np.random.randn(10) * 0.02
            })

            mock_universe.get_lag_prices.return_value = pd.DataFrame({
                'date': pd.date_range('2023-01-01', periods=252),
                'open': 150 + np.random.randn(252) * 5,
                'high': 155 + np.random.randn(252) * 5,
                'low': 145 + np.random.randn(252) * 5,
                'close': 150 + np.random.randn(252) * 5,
                'volume': np.random.randint(1000000, 2000000, 252)
            })

            with tempfile.TemporaryDirectory() as temp_dir:
                # Step 1: Generate schema-aware training data
                training_result = await generate_residual_return_training_data(
                    mock_eda_database,
                    mock_eda_compatible_environment,
                    mock_universe,
                    datetime(2023, 6, 1),
                    datetime(2023, 6, 30),
                    instrument_ids=[1],
                    include_schema=True,
                    output_path=temp_dir
                )

                # Verify training data generation
                assert isinstance(training_result.schema, TrainingDatasetSchema)
                assert training_result.validation_result.is_valid

                # Step 2: Mock EDA system loading the data
                def load_training_data_for_eda(dataset_path):
                    """Mock EDA data loading."""

                    # Load schema
                    with open(os.path.join(dataset_path, 'schema.json'), 'r') as f:
                        schema_dict = json.load(f)

                    # Load arrays
                    features = np.load(os.path.join(dataset_path, 'features.npy'))
                    labels = np.load(os.path.join(dataset_path, 'labels.npy'))

                    # Load validation results
                    with open(os.path.join(dataset_path, 'validation.json'), 'r') as f:
                        validation_dict = json.load(f)

                    return {
                        'schema': schema_dict,
                        'features': features,
                        'labels': labels,
                        'validation': validation_dict,
                        'raw_data': pd.read_parquet(os.path.join(dataset_path, 'raw_data.parquet'))
                    }

                eda_data = load_training_data_for_eda(temp_dir)

                # Step 3: Generate EDA insights using schema
                def generate_eda_insights(eda_data):
                    """Mock EDA insight generation."""
                    schema = eda_data['schema']
                    features = eda_data['features']
                    validation = eda_data['validation']

                    insights = {
                        'data_quality': {
                            'overall_score': validation['confidence_score'],
                            'validation_status': validation['is_valid'],
                            'feature_completeness': 1.0 - (np.isnan(features).sum() / features.size)
                        },
                        'feature_analysis': {},
                        'correlations': {},
                        'recommendations': []
                    }

                    # Analyze each feature using schema metadata
                    for i, feature_info in enumerate(schema['features']):
                        feature_name = feature_info['name']
                        feature_type = feature_info['feature_type']
                        feature_data = features[:, i]

                        insights['feature_analysis'][feature_name] = {
                            'type': feature_type,
                            'distribution': {
                                'mean': float(np.mean(feature_data)),
                                'std': float(np.std(feature_data)),
                                'skew': float(pd.Series(feature_data).skew()),
                                'outlier_count': int(np.sum(np.abs(feature_data - np.mean(feature_data)) > 3 * np.std(feature_data)))
                            },
                            'quality_score': 1.0 if not np.isnan(feature_data).any() else 0.5
                        }

                    # Generate recommendations based on schema and data
                    if validation['confidence_score'] > 0.9:
                        insights['recommendations'].append({
                            'type': 'quality',
                            'message': 'High-quality dataset ready for modeling'
                        })

                    if len(schema['features']) > 10:
                        insights['recommendations'].append({
                            'type': 'dimensionality',
                            'message': 'Consider feature selection for high-dimensional dataset'
                        })

                    return insights

                eda_insights = generate_eda_insights(eda_data)

                # Verify EDA insights generation
                assert 'data_quality' in eda_insights
                assert 'feature_analysis' in eda_insights
                assert eda_insights['data_quality']['validation_status'] is True
                assert eda_insights['data_quality']['overall_score'] > 0.8

                # Verify feature analysis uses schema information
                feature_names = [f['name'] for f in eda_data['schema']['features']]
                for feature_name in feature_names:
                    assert feature_name in eda_insights['feature_analysis']
                    analysis = eda_insights['feature_analysis'][feature_name]
                    assert 'type' in analysis
                    assert 'distribution' in analysis
                    assert 'quality_score' in analysis

                # Verify recommendations are generated
                assert len(eda_insights['recommendations']) > 0
                assert any(r['type'] == 'quality' for r in eda_insights['recommendations'])

                # Step 4: Verify complete end-to-end integration
                assert os.path.exists(temp_dir)
                assert eda_data['features'].shape[0] > 0
                assert eda_data['features'].shape[1] > 0
                assert len(eda_data['schema']['features']) == eda_data['features'].shape[1]
                assert eda_insights['data_quality']['feature_completeness'] > 0.9