"""
Integration tests for schema-aware training data generation.

Tests the complete pipeline from training data generation to schema creation,
validation, and database persistence.
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from src.ml.training_data.generators.training_data_generator import (
    ResidualReturnTrainingDataGenerator,
    TrainingConfig,
    TrainingDatasetResult,
    generate_residual_return_training_data
)
from src.schema.training_schema import TrainingDatasetSchema, FeatureType, ValidationResult
from domains.ml.repositories.training_schema_dao import TrainingSchemaDAO


@pytest.fixture
def mock_environment():
    """Mock environment configuration."""
    env = MagicMock()
    env.get_table_name.return_value = "dev_instruments"
    env.env_type.value = 'dev'
    return env


@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = AsyncMock()
    conn = AsyncMock()
    
    # Mock instrument data
    conn.fetch.return_value = [
        {'id': 1, 'symbol': 'AAPL'},
        {'id': 2, 'symbol': 'MSFT'},
        {'id': 3, 'symbol': 'GOOGL'}
    ]
    
    conn.fetchrow.return_value = {
        'sector': 'Technology',
        'industry': 'Software'
    }
    
    pool.acquire.return_value.__aenter__.return_value = conn
    return pool


@pytest.fixture
def mock_universe_state_manager():
    """Mock universe state manager with realistic price data."""
    manager = MagicMock()
    
    # Create realistic OHLCV data
    dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
    price_data = pd.DataFrame({
        'date': dates,
        'open': 150 + np.random.randn(len(dates)) * 5,
        'high': 155 + np.random.randn(len(dates)) * 5,
        'low': 145 + np.random.randn(len(dates)) * 5,
        'close': 150 + np.random.randn(len(dates)) * 5,
        'volume': 1000000 + np.random.randint(0, 500000, len(dates))
    })
    price_data['close'] = price_data['close'].clip(lower=1.0)  # Ensure positive prices
    
    manager.get_lag_prices.return_value = price_data.tail(252)  # Return last 252 days
    return manager


@pytest.fixture
def training_config():
    """Standard training configuration for tests."""
    return TrainingConfig(
        lookback_days=60,
        prediction_horizons=[1, 3, 5],
        min_history_days=50,
        include_technical_indicators=True,
        include_event_features=False,  # Disable for simpler testing
        include_sector_features=False,
        exclude_weekends=True,
        min_price=1.0,
        min_volume=1000
    )


class TestSchemaAwareTrainingGeneration:
    """Test suite for schema-aware training data generation."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_basic_training_generation_with_schema(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager, training_config
    ):
        """Test basic training data generation with schema creation."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar') as mock_calendar, \
             patch('src.modeling.event_features.EventSequenceExtractor') as mock_extractor, \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO') as mock_dao:
            
            # Setup mocks
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1, 1, 1],
                'date': pd.date_range('2023-06-01', periods=3),
                'residual_return': [0.01, -0.005, 0.02],
                'r_squared': [0.8, 0.85, 0.75]
            })
            
            mock_dao_instance = AsyncMock()
            mock_dao.return_value = mock_dao_instance
            mock_dao_instance.register_schema.return_value = "schema_hash_123"
            
            # Create generator
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, 
                mock_universe_state_manager, training_config
            )
            
            # Generate training data
            start_date = datetime(2023, 6, 1)
            end_date = datetime(2023, 6, 30)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                result = await generator.generate_training_dataset(
                    start_date, end_date, 
                    instrument_ids=[1], 
                    include_schema=True,
                    output_path=temp_dir
                )
                
                # Verify result structure
                assert isinstance(result, TrainingDatasetResult)
                assert isinstance(result.schema, TrainingDatasetSchema)
                assert isinstance(result.validation_result, ValidationResult)
                assert result.features_array.shape[0] > 0  # Should have samples
                assert result.features_array.shape[1] > 0  # Should have features
                
                # Verify schema properties
                assert result.schema.dataset_name.startswith('residual_return_AAPL')
                assert result.schema.metadata.symbol == 'AAPL'
                assert result.schema.metadata.sequence_length == 60
                assert len(result.schema.features) > 0
                assert len(result.schema.labels) > 0
                
                # Verify files are created
                assert os.path.exists(os.path.join(temp_dir, 'features.npy'))
                assert os.path.exists(os.path.join(temp_dir, 'labels.npy'))
                assert os.path.exists(os.path.join(temp_dir, 'schema.json'))
                assert os.path.exists(os.path.join(temp_dir, 'validation.json'))

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_feature_type_inference(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test automatic feature type inference from column names."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1],
                'date': [datetime(2023, 6, 1)],
                'residual_return': [0.01]
            })
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            # Test feature type inference
            test_cases = [
                ('return_1d', FeatureType.RETURN_SERIES),
                ('sma_20', FeatureType.TECHNICAL_INDICATOR),
                ('rsi_14', FeatureType.TECHNICAL_INDICATOR),
                ('volume_avg', FeatureType.VOLUME_PROFILE),
                ('volatility_20d', FeatureType.VOLATILITY_METRICS),
                ('sector_momentum', FeatureType.MARKET_REGIME),
                ('earnings_proximity', FeatureType.EVENT_INDICATOR),
                ('day_of_week', FeatureType.TEMPORAL_FEATURES),
                ('custom_feature', FeatureType.CUSTOM_INDICATOR)
            ]
            
            for column_name, expected_type in test_cases:
                inferred_type = generator._infer_feature_type(column_name)
                assert inferred_type == expected_type, f"Failed for {column_name}: expected {expected_type}, got {inferred_type}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_schema_validation(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test schema validation with various data quality issues."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1],
                'date': [datetime(2023, 6, 1)],
                'residual_return': [0.01]
            })
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            # Create test schema
            from src.schema.training_schema import FeatureSchema, DatasetMetadata
            
            schema = TrainingDatasetSchema(
                schema_version='1.0.0',
                dataset_name='test_schema',
                features=[
                    FeatureSchema(name='feature1', feature_type=FeatureType.RETURN_SERIES),
                    FeatureSchema(name='feature2', feature_type=FeatureType.TECHNICAL_INDICATOR)
                ],
                labels=[],
                metadata=DatasetMetadata(symbol='TEST')
            )
            
            # Test valid data
            valid_features = np.array([[1.0, 2.0], [3.0, 4.0]])
            valid_labels = np.array([])
            
            validation_result = generator._validate_training_data(schema, valid_features, valid_labels)
            assert validation_result.is_valid
            assert len(validation_result.errors) == 0
            assert validation_result.confidence_score > 0.8
            
            # Test invalid data - wrong feature count
            invalid_features = np.array([[1.0], [3.0]])  # Only 1 feature instead of 2
            
            validation_result = generator._validate_training_data(schema, invalid_features, valid_labels)
            assert not validation_result.is_valid
            assert len(validation_result.errors) > 0
            assert "Feature count mismatch" in validation_result.errors[0]
            
            # Test data with infinite values
            inf_features = np.array([[1.0, np.inf], [3.0, 4.0]])
            
            validation_result = generator._validate_training_data(schema, inf_features, valid_labels)
            assert not validation_result.is_valid
            assert any("infinite values" in error for error in validation_result.errors)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backwards_compatibility(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test that legacy DataFrame-based API still works."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1, 1, 1],
                'date': pd.date_range('2023-06-01', periods=3),
                'residual_return': [0.01, -0.005, 0.02]
            })
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            # Test legacy mode (include_schema=False)
            start_date = datetime(2023, 6, 1)
            end_date = datetime(2023, 6, 30)
            
            result = await generator.generate_training_dataset(
                start_date, end_date,
                instrument_ids=[1],
                include_schema=False
            )
            
            # Should return TrainingDatasetResult but with DataFrame in metadata
            assert isinstance(result, TrainingDatasetResult)
            assert 'dataframe' in result.metadata
            assert isinstance(result.metadata['dataframe'], pd.DataFrame)
            assert result.schema is None
            assert result.validation_result is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_convenience_function(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test the convenience function for training data generation."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1],
                'date': [datetime(2023, 6, 1)],
                'residual_return': [0.01]
            })
            
            # Test convenience function
            result = await generate_residual_return_training_data(
                mock_connection_pool,
                mock_environment,
                mock_universe_state_manager,
                datetime(2023, 6, 1),
                datetime(2023, 6, 30),
                instrument_ids=[1],
                include_schema=True
            )
            
            assert isinstance(result, TrainingDatasetResult)
            assert isinstance(result.schema, TrainingDatasetSchema)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_multi_instrument_schema_generation(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test schema generation for multiple instruments."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            # Mock multiple instruments
            conn = mock_connection_pool.acquire.return_value.__aenter__.return_value
            conn.fetch.return_value = [
                {'symbol': 'AAPL'}, {'symbol': 'MSFT'}, {'symbol': 'GOOGL'}
            ]
            
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1, 2, 3] * 3,
                'date': pd.date_range('2023-06-01', periods=3).tolist() * 3,
                'residual_return': [0.01, -0.005, 0.02] * 3
            })
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            result = await generator.generate_training_dataset(
                datetime(2023, 6, 1), datetime(2023, 6, 30),
                instrument_ids=[1, 2, 3],
                include_schema=True
            )
            
            # Verify multi-instrument schema
            assert result.schema.metadata.symbol == 'MULTI'
            assert result.schema.metadata.additional_symbols == ['MSFT', 'GOOGL']
            assert result.schema.dataset_name.startswith('residual_return_MULTI')

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_schema_registration(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test that schemas are properly registered in database."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO') as mock_dao_class:
            
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1],
                'date': [datetime(2023, 6, 1)],
                'residual_return': [0.01]
            })
            
            # Mock DAO registration
            mock_dao_instance = AsyncMock()
            mock_dao_class.return_value = mock_dao_instance
            mock_dao_instance.register_schema.return_value = "test_schema_hash_456"
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            with tempfile.TemporaryDirectory() as temp_dir:
                result = await generator.generate_training_dataset(
                    datetime(2023, 6, 1), datetime(2023, 6, 30),
                    instrument_ids=[1],
                    include_schema=True,
                    output_path=temp_dir
                )
                
                # Verify schema registration was called
                mock_dao_instance.register_schema.assert_called_once()
                call_args = mock_dao_instance.register_schema.call_args
                assert call_args[0][0] == result.schema  # First arg should be the schema
                assert 'created_by' in call_args[1]
                assert 'tags' in call_args[1]
                assert 'description' in call_args[1]
                
                # Verify schema hash file was created
                schema_hash_file = os.path.join(temp_dir, 'schema_hash.txt')
                assert os.path.exists(schema_hash_file)
                with open(schema_hash_file, 'r') as f:
                    assert f.read().strip() == "test_schema_hash_456"


class TestSchemaValidationEdgeCases:
    """Test edge cases in schema validation and generation."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_empty_training_data(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test handling of empty training datasets."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            # Return empty residual returns
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame()
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            result = await generator.generate_training_dataset(
                datetime(2023, 6, 1), datetime(2023, 6, 30),
                instrument_ids=[1],
                include_schema=True
            )
            
            # Should handle empty data gracefully
            assert result.features_array.shape[0] == 0
            assert result.labels_array.shape[0] == 0
            assert result.schema.metadata.total_samples == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_quality_scoring(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test data quality scoring functionality."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO'):
            
            generator = ResidualReturnTrainingDataGenerator(
                mock_connection_pool, mock_environment, mock_universe_state_manager
            )
            
            # Test high quality data
            high_quality_data = pd.DataFrame({
                'high': [150, 151, 152],
                'low': [148, 149, 150],
                'close': [149, 150, 151],
                'volume': [1000000, 1100000, 950000]
            })
            
            quality_score = generator._calculate_data_quality_score(high_quality_data)
            assert quality_score > 0.8  # Should be high quality
            
            # Test low quality data with missing values and extreme volatility
            low_quality_data = pd.DataFrame({
                'high': [150, None, 200],  # Missing value
                'low': [148, 149, 50],     # Extreme price drop
                'close': [149, 150, 75],   # Extreme volatility
                'volume': [1000000, 1100000, 950000]
            })
            
            quality_score = generator._calculate_data_quality_score(low_quality_data)
            assert quality_score < 0.8  # Should be lower quality

    @pytest.mark.asyncio 
    @pytest.mark.asyncio
    async def test_feature_description_generation(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test automatic feature description generation."""
        
        generator = ResidualReturnTrainingDataGenerator(
            mock_connection_pool, mock_environment, mock_universe_state_manager
        )
        
        test_cases = [
            ('return_5d', FeatureType.RETURN_SERIES, 'Return-based feature'),
            ('sma_20', FeatureType.TECHNICAL_INDICATOR, 'Technical indicator'),
            ('sector_momentum', FeatureType.MARKET_REGIME, 'Market/sector feature'),
            ('earnings_event', FeatureType.EVENT_INDICATOR, 'Event-driven feature')
        ]
        
        for feature_name, feature_type, expected_prefix in test_cases:
            description = generator._generate_feature_description(feature_name, feature_type)
            assert expected_prefix in description
            assert feature_name in description


@pytest.mark.integration
class TestEndToEndSchemaWorkflow:
    """End-to-end integration tests for complete schema workflow."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_schema_workflow(
        self, mock_connection_pool, mock_environment, mock_universe_state_manager
    ):
        """Test complete workflow from generation to file output."""
        
        with patch('src.modeling.factor_models.ResidualReturnCalculator') as mock_calc, \
             patch('src.modeling.event_features.EventCalendar'), \
             patch('src.modeling.event_features.EventSequenceExtractor'), \
             patch('src.dao.training_schema_dao.TrainingSchemaDAO') as mock_dao_class:
            
            # Setup realistic residual returns
            mock_calc.return_value.calculate_residual_returns.return_value = pd.DataFrame({
                'instrument_id': [1] * 30,
                'date': pd.date_range('2023-06-01', periods=30),
                'residual_return': np.random.randn(30) * 0.02,
                'market_loading': [0.8] * 30,
                'r_squared': [0.75] * 30
            })
            
            mock_dao_instance = AsyncMock()
            mock_dao_class.return_value = mock_dao_instance
            mock_dao_instance.register_schema.return_value = "complete_workflow_hash"
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Generate training data with complete schema workflow
                result = await generate_residual_return_training_data(
                    mock_connection_pool,
                    mock_environment,
                    mock_universe_state_manager,
                    datetime(2023, 6, 1),
                    datetime(2023, 6, 30),
                    instrument_ids=[1],
                    include_schema=True,
                    output_path=temp_dir
                )
                
                # Verify complete result structure
                assert isinstance(result, TrainingDatasetResult)
                assert result.dataset_path == temp_dir
                assert result.features_array.shape[0] > 0
                assert result.features_array.shape[1] > 0
                assert isinstance(result.schema, TrainingDatasetSchema)
                assert result.validation_result.is_valid
                
                # Verify all files exist
                required_files = ['features.npy', 'labels.npy', 'schema.json', 
                                'validation.json', 'raw_data.parquet', 'schema_hash.txt']
                
                for filename in required_files:
                    file_path = os.path.join(temp_dir, filename)
                    assert os.path.exists(file_path), f"Missing file: {filename}"
                
                # Verify file contents
                import json
                
                # Check schema JSON
                with open(os.path.join(temp_dir, 'schema.json'), 'r') as f:
                    schema_data = json.load(f)
                    assert 'schema_version' in schema_data
                    assert 'features' in schema_data
                    assert 'labels' in schema_data
                    assert 'metadata' in schema_data
                
                # Check validation JSON
                with open(os.path.join(temp_dir, 'validation.json'), 'r') as f:
                    validation_data = json.load(f)
                    assert 'is_valid' in validation_data
                    assert 'confidence_score' in validation_data
                
                # Verify numpy arrays can be loaded
                features_loaded = np.load(os.path.join(temp_dir, 'features.npy'))
                labels_loaded = np.load(os.path.join(temp_dir, 'labels.npy'))
                
                assert features_loaded.shape == result.features_array.shape
                assert labels_loaded.shape == result.labels_array.shape
                
                # Verify parquet file
                import pandas as pd
                raw_data = pd.read_parquet(os.path.join(temp_dir, 'raw_data.parquet'))
                assert len(raw_data) > 0
                assert 'instrument_id' in raw_data.columns
                assert 'date' in raw_data.columns