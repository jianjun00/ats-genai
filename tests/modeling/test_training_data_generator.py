"""
Tests for training data generator for residual return prediction.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock
import asyncpg

from domains.ml.services.training_data.generators.training_data_generator import (
    TrainingConfig,
    TrainingSample,
    ResidualReturnTrainingDataGenerator,
    generate_residual_return_training_data
)
from domains.ml.services.factor_models import ResidualReturnCalculator
from domains.ml.services.event_features import EventSequenceExtractor, EventCalendar
from state.universe_state_manager import UniverseStateManager


@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = Mock(spec=asyncpg.Pool)
    conn = Mock(spec=asyncpg.Connection)
    pool.acquire.return_value.__aenter__.return_value = conn
    pool.acquire.return_value.__aexit__.return_value = None
    return pool, conn


@pytest.fixture
def mock_env():
    """Mock environment configuration."""
    env = Mock()
    env.get_table_name.side_effect = lambda x: f"test_{x}"
    return env


@pytest.fixture
def mock_universe_state_manager():
    """Mock universe state manager."""
    manager = Mock(spec=UniverseStateManager)

    # Default price data
    default_prices = pd.DataFrame({
        'high': [102, 104, 106, 108, 110],
        'low': [98, 100, 102, 104, 106],
        'close': [100, 102, 104, 106, 108],
        'volume': [1000, 1500, 1200, 1800, 2000]
    })

    manager.get_lag_prices.return_value = default_prices

    return manager


@pytest.fixture
def sample_training_config():
    """Sample training configuration."""
    return TrainingConfig(
        lookback_days=100,
        prediction_horizons=[1, 2, 3],
        min_history_days=30,
        factor_model_type='market_model',
        include_technical_indicators=True,
        include_event_features=True,
        include_sector_features=True,
        exclude_weekends=True,
        min_price=1.0,
        min_volume=1000
    )


@pytest.fixture
def sample_residual_returns():
    """Sample residual returns data."""
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    return pd.DataFrame({
        'instrument_id': [1] * len(dates),
        'date': dates,
        'residual_return': np.random.normal(0, 0.02, len(dates)),
        'market_return': np.random.normal(0.001, 0.015, len(dates)),
        'r_squared': np.random.uniform(0.3, 0.8, len(dates))
    })


class TestTrainingConfig:
    """Test TrainingConfig dataclass."""

    def test_training_config_defaults(self):
        """Test TrainingConfig with default values."""
        config = TrainingConfig()

        assert config.lookback_days == 252
        assert config.prediction_horizons == [1, 2, 3, 4, 5]
        assert config.min_history_days == 50
        assert config.factor_model_type == 'multi_factor'
        assert config.include_technical_indicators is True
        assert config.include_event_features is True
        assert config.min_price == 1.0
        assert config.min_volume == 1000

    def test_training_config_custom(self):
        """Test TrainingConfig with custom values."""
        config = TrainingConfig(
            lookback_days=100,
            prediction_horizons=[1, 3, 5],
            factor_model_type='market_model',
            min_price=5.0
        )

        assert config.lookback_days == 100
        assert config.prediction_horizons == [1, 3, 5]
        assert config.factor_model_type == 'market_model'
        assert config.min_price == 5.0

    def test_training_config_post_init(self):
        """Test TrainingConfig post_init processing."""
        config = TrainingConfig(prediction_horizons=None)

        # Should set default prediction horizons
        assert config.prediction_horizons == [1, 2, 3, 4, 5]


class TestTrainingSample:
    """Test TrainingSample dataclass."""

    def test_training_sample_creation(self):
        """Test TrainingSample creation."""
        features = {'momentum': 0.05, 'volatility': 0.02}
        targets = {'residual_return_1d': 0.01, 'positive_return_1d': 1.0}
        metadata = {'current_price': 100.0, 'data_quality_score': 0.9}

        sample = TrainingSample(
            instrument_id=123,
            date=datetime(2024, 1, 15),
            features=features,
            targets=targets,
            metadata=metadata
        )

        assert sample.instrument_id == 123
        assert sample.date == datetime(2024, 1, 15)
        assert sample.features == features
        assert sample.targets == targets
        assert sample.metadata == metadata


class TestResidualReturnTrainingDataGenerator:
    """Test ResidualReturnTrainingDataGenerator functionality."""

    def test_generator_initialization(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_training_config):
        """Test generator initialization."""
        pool, conn = mock_connection_pool

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager, sample_training_config
        )

        assert generator.pool == pool
        assert generator.env == mock_env
        assert generator.universe_state_manager == mock_universe_state_manager
        assert generator.config == sample_training_config
        assert isinstance(generator.residual_calculator, ResidualReturnCalculator)
        assert isinstance(generator.event_calendar, EventCalendar)
        assert isinstance(generator.event_extractor, EventSequenceExtractor)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_active_instruments(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test getting active instruments."""
        pool, conn = mock_connection_pool

        # Mock database response
        conn.fetch.return_value = [
            {'id': 1}, {'id': 2}, {'id': 3}
        ]

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        instruments = await generator._get_active_instruments()

        assert instruments == [1, 2, 3]
        conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_training_dataset_basic(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_residual_returns):
        """Test basic training dataset generation."""
        pool, conn = mock_connection_pool

        # Mock active instruments
        conn.fetch.return_value = [{'id': 1}]

        # Mock residual calculator
        with patch.object(ResidualReturnCalculator, 'calculate_residual_returns') as mock_calc:
            mock_calc.return_value = sample_residual_returns

            generator = ResidualReturnTrainingDataGenerator(
                pool, mock_env, mock_universe_state_manager
            )

            # Mock other methods
            with patch.object(generator, '_generate_batch_samples') as mock_batch:
                mock_batch.return_value = [
                    TrainingSample(
                        instrument_id=1,
                        date=datetime(2024, 1, 15),
                        features={'momentum': 0.05},
                        targets={'residual_return_1d': 0.01},
                        metadata={'current_price': 100.0}
                    )
                ]

                dataset = await generator.generate_training_dataset(
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 31),
                    instrument_ids=[1]
                )

                assert isinstance(dataset, pd.DataFrame)
                assert not dataset.empty
                assert 'instrument_id' in dataset.columns
                assert 'date' in dataset.columns

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_batch_samples(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_residual_returns):
        """Test batch sample generation."""
        pool, conn = mock_connection_pool

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        # Mock residual calculator
        with patch.object(generator.residual_calculator, 'calculate_residual_returns') as mock_calc:
            mock_calc.return_value = sample_residual_returns

            with patch.object(generator, '_generate_instrument_samples') as mock_inst:
                mock_inst.return_value = [
                    TrainingSample(
                        instrument_id=1,
                        date=datetime(2024, 1, 15),
                        features={'test': 1.0},
                        targets={'target': 0.01},
                        metadata={}
                    )
                ]

                samples = await generator._generate_batch_samples(
                    [1], datetime(2024, 1, 1), datetime(2024, 1, 31)
                )

                assert len(samples) == 1
                assert isinstance(samples[0], TrainingSample)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_instrument_samples(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_residual_returns):
        """Test instrument sample generation."""
        pool, conn = mock_connection_pool

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        with patch.object(generator, '_create_training_sample') as mock_create:
            mock_create.return_value = TrainingSample(
                instrument_id=1,
                date=datetime(2024, 1, 2),
                features={'test': 1.0},
                targets={'target': 0.01},
                metadata={}
            )

            samples = await generator._generate_instrument_samples(
                1, datetime(2024, 1, 1), datetime(2024, 1, 3), sample_residual_returns
            )

            assert len(samples) > 0
            assert all(isinstance(s, TrainingSample) for s in samples)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_training_sample(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_residual_returns):
        """Test training sample creation."""
        pool, conn = mock_connection_pool

        # Setup price data
        price_data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108],
            'high': [102, 104, 106, 108, 110],
            'low': [98, 100, 102, 104, 106],
            'volume': [1000, 1500, 1200, 1800, 2000]
        })
        mock_universe_state_manager.get_lag_prices.return_value = price_data

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        # Set up residual returns with correct index
        residuals = sample_residual_returns.set_index('date')

        with patch.object(generator, '_extract_technical_features') as mock_tech, \
             patch.object(generator, '_extract_event_features') as mock_event, \
             patch.object(generator, '_extract_sector_features') as mock_sector, \
             patch.object(generator, '_extract_market_features') as mock_market, \
             patch.object(generator, '_extract_factor_features') as mock_factor:

            mock_tech.return_value = {'rsi': 60.0, 'momentum': 0.05}
            mock_event.return_value = {'event_score': 0.8}
            mock_sector.return_value = {'sector': 'Technology', 'sector_return': 0.01}
            mock_market.return_value = {'day_of_week': 1}
            mock_factor.return_value = {'factor_loading': 1.2}

            sample = await generator._create_training_sample(
                1, datetime(2024, 1, 5), residuals
            )

            assert sample is not None
            assert isinstance(sample, TrainingSample)
            assert sample.instrument_id == 1
            assert len(sample.features) > 0
            assert len(sample.targets) > 0

    def test_passes_basic_filters(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test basic data quality filters."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Good data
        good_data = pd.DataFrame({
            'close': [100, 102, 104],
            'high': [102, 104, 106],
            'low': [98, 100, 102],
            'volume': [10000, 15000, 12000]
        })

        assert generator._passes_basic_filters(good_data) is True

        # Low price data
        low_price_data = pd.DataFrame({
            'close': [0.5, 0.6, 0.7],
            'high': [0.6, 0.7, 0.8],
            'low': [0.4, 0.5, 0.6]
        })

        assert generator._passes_basic_filters(low_price_data) is False

        # Low volume data
        low_volume_data = pd.DataFrame({
            'close': [100, 102, 104],
            'high': [102, 104, 106],
            'low': [98, 100, 102],
            'volume': [100, 150, 120]  # Below min_volume
        })

        assert generator._passes_basic_filters(low_volume_data) is False

        # Missing data
        missing_data = pd.DataFrame({
            'close': [100, np.nan, 104],
            'high': [102, np.nan, 106],
            'low': [98, np.nan, 102]
        })

        assert generator._passes_basic_filters(missing_data) is False

    def test_get_future_residuals(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test future residual return extraction."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Create residual returns data
        dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
        residuals = pd.DataFrame({
            'residual_return': np.random.normal(0, 0.02, len(dates))
        }, index=dates.date)

        current_date = datetime(2024, 1, 5)
        horizons = [1, 2, 3]

        future_residuals = generator._get_future_residuals(
            current_date, residuals, horizons
        )

        assert isinstance(future_residuals, dict)
        # Should have some future data
        assert len(future_residuals) <= len(horizons)

    def test_extract_technical_features(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test technical feature extraction."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        price_data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108],
            'high': [102, 104, 106, 108, 110],
            'low': [98, 100, 102, 104, 106],
            'volume': [1000, 1500, 1200, 1800, 2000]
        })

        with patch('modeling.training_data_generator.calculate_all_technical_indicators') as mock_calc:
            mock_calc.return_value = {
                'rsi_14': 60.0,
                'ema_20': 105.0,
                'atr_14': 2.5
            }

            features = generator._extract_technical_features(price_data)

            assert isinstance(features, dict)
            assert 'rsi_14' in features
            assert 'return_1d' in features  # Should add basic features

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_event_features(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test event feature extraction."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Mock event extractor
        with patch.object(generator.event_extractor, 'extract_event_features') as mock_extract:
            from domains.ml.services.event_features import EventFeatures

            mock_extract.return_value = EventFeatures(
                instrument_id=123,
                date=datetime(2024, 1, 15),
                upcoming_events=[],
                historical_patterns={},
                pre_event_sequences={},
                event_proximity_score=0.8,
                event_importance_weighted_score=0.6
            )

            with patch('modeling.training_data_generator.flatten_event_features_for_model') as mock_flatten:
                mock_flatten.return_value = {
                    'event_proximity_score': 0.8,
                    'event_importance_weighted_score': 0.6
                }

                features = await generator._extract_event_features(123, datetime(2024, 1, 15))

                assert isinstance(features, dict)
                assert 'event_proximity_score' in features

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_sector_features(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test sector feature extraction."""
        pool, conn = mock_connection_pool

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        with patch.object(generator, '_get_instrument_sector') as mock_sector, \
             patch.object(generator, '_get_sector_return') as mock_return:

            mock_sector.return_value = 'Technology'
            mock_return.return_value = 0.015

            features = await generator._extract_sector_features(123, datetime(2024, 1, 15))

            assert isinstance(features, dict)
            assert 'sector' in features
            assert 'sector_return_1d' in features
            assert features['sector'] == 'Technology'

    def test_extract_market_features(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test market feature extraction."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        price_data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108] * 50,  # 250 days for 52w calculations
            'high': [102, 104, 106, 108, 110] * 50,
            'low': [98, 100, 102, 104, 106] * 50
        })

        current_date = datetime(2024, 1, 15)  # Monday

        features = generator._extract_market_features(price_data, current_date)

        assert isinstance(features, dict)
        assert 'day_of_week' in features
        assert 'day_of_month' in features
        assert 'day_of_year' in features
        assert 'is_month_end' in features
        assert 'is_quarter_end' in features

        assert features['day_of_week'] == 0  # Monday
        assert features['day_of_month'] == 15

    def test_extract_factor_features(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test factor feature extraction."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Create mock residual data with factor loadings
        residuals = pd.DataFrame({
            'residual_return': np.random.normal(0, 0.02, 20),
            'market_loading': [1.2] * 20,
            'size_loading': [0.5] * 20,
            'r_squared': [0.7] * 20
        })

        features = generator._extract_factor_features(datetime(2024, 1, 15), residuals)

        assert isinstance(features, dict)
        if 'recent_market_loading' in features:
            assert isinstance(features['recent_market_loading'], (int, float))

    def test_create_targets(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test target creation."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        future_residuals = {
            1: 0.025,  # Strong positive
            2: -0.015, # Negative
            3: 0.005   # Small positive
        }

        targets = generator._create_targets(future_residuals)

        assert isinstance(targets, dict)

        # Should have direct returns
        assert 'residual_return_1d' in targets
        assert 'residual_return_2d' in targets
        assert 'residual_return_3d' in targets

        # Should have binary targets
        assert 'positive_return_1d' in targets
        assert 'strong_positive_1d' in targets
        assert 'strong_negative_2d' in targets

        # Check values
        assert targets['positive_return_1d'] == 1.0  # Positive return
        assert targets['positive_return_2d'] == 0.0  # Negative return
        assert targets['strong_positive_1d'] == 1.0  # > 2%
        assert targets['strong_negative_2d'] == 0.0  # Not < -2%

    def test_calculate_data_quality_score(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test data quality score calculation."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # High quality data
        good_data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108],
            'high': [102, 104, 106, 108, 110],
            'low': [98, 100, 102, 104, 106]
        })

        score = generator._calculate_data_quality_score(good_data)
        assert 0.1 <= score <= 1.0
        assert score > 0.8  # Should be high quality

        # Poor quality data (missing values, extreme volatility)
        poor_data = pd.DataFrame({
            'close': [100, np.nan, 200, 50, np.nan],  # Missing + extreme moves
            'high': [102, np.nan, 220, 60, np.nan],
            'low': [98, np.nan, 180, 40, np.nan]
        })

        score = generator._calculate_data_quality_score(poor_data)
        assert 0.1 <= score <= 1.0
        assert score < 0.5  # Should be low quality

    def test_samples_to_dataframe(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test conversion of samples to DataFrame."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        samples = [
            TrainingSample(
                instrument_id=1,
                date=datetime(2024, 1, 15),
                features={'momentum': 0.05, 'volatility': 0.02},
                targets={'residual_return_1d': 0.01, 'positive_return_1d': 1.0},
                metadata={'current_price': 100.0, 'data_quality_score': 0.9}
            ),
            TrainingSample(
                instrument_id=2,
                date=datetime(2024, 1, 15),
                features={'momentum': -0.02, 'volatility': 0.03},
                targets={'residual_return_1d': -0.005, 'positive_return_1d': 0.0},
                metadata={'current_price': 50.0, 'data_quality_score': 0.8}
            )
        ]

        df = generator._samples_to_dataframe(samples)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'instrument_id' in df.columns
        assert 'date' in df.columns
        assert 'momentum' in df.columns
        assert 'residual_return_1d' in df.columns
        assert 'meta_current_price' in df.columns

    def test_clean_training_data(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test training data cleaning."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Create messy data
        messy_data = pd.DataFrame({
            'instrument_id': [1, 2, 1, 3],  # Duplicate (1 appears twice)
            'date': [datetime(2024, 1, 15), datetime(2024, 1, 16),
                    datetime(2024, 1, 15), datetime(2024, 1, 17)],
            'momentum': [0.05, np.inf, -0.02, np.nan],  # Infinite and NaN
            'residual_return_1d': [0.01, -0.005, np.nan, 0.008],  # Missing target
            'positive_return_1d': [1.0, 0.0, np.nan, 1.0]
        })

        cleaned = generator._clean_training_data(messy_data)

        assert isinstance(cleaned, pd.DataFrame)
        assert len(cleaned) <= len(messy_data)  # Should remove some rows

        # Check for removed infinite values
        numeric_cols = cleaned.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if not col.startswith('residual_return_') and not col.startswith('positive_return_'):
                assert not cleaned[col].isin([np.inf, -np.inf]).any()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_instrument_sector(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test instrument sector retrieval."""
        pool, conn = mock_connection_pool

        conn.fetchrow.return_value = {'sector': 'Technology', 'industry': 'Software'}

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        sector = await generator._get_instrument_sector(123)

        assert sector == 'Technology'
        conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_instrument_sector_cached(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test instrument sector caching."""
        pool, conn = mock_connection_pool

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        # Set cache manually
        generator._sector_cache[123] = 'Healthcare'

        sector = await generator._get_instrument_sector(123)

        assert sector == 'Healthcare'
        # Should not call database
        conn.fetchrow.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_sector_return(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test sector return calculation."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Currently returns 0.0 (simplified implementation)
        sector_return = await generator._get_sector_return('Technology', datetime(2024, 1, 15))

        assert isinstance(sector_return, float)
        assert sector_return == 0.0


class TestConvenienceFunction:
    """Test convenience function."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_residual_return_training_data(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test convenience function for training data generation."""
        pool, conn = mock_connection_pool

        with patch('modeling.training_data_generator.ResidualReturnTrainingDataGenerator') as mock_class:
            mock_generator = Mock()
            mock_generator.generate_training_dataset = AsyncMock(return_value=pd.DataFrame({
                'instrument_id': [1, 2],
                'date': [datetime(2024, 1, 15), datetime(2024, 1, 15)],
                'momentum': [0.05, -0.02],
                'residual_return_1d': [0.01, -0.005]
            }))
            mock_class.return_value = mock_generator

            result = await generate_residual_return_training_data(
                pool, mock_env, mock_universe_state_manager,
                datetime(2024, 1, 1), datetime(2024, 1, 31)
            )

            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            mock_generator.generate_training_dataset.assert_called_once()


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_dataset_no_instruments(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test dataset generation with no instruments."""
        pool, conn = mock_connection_pool

        # No instruments returned
        conn.fetch.return_value = []

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        dataset = await generator.generate_training_dataset(
            datetime(2024, 1, 1), datetime(2024, 1, 31)
        )

        assert isinstance(dataset, pd.DataFrame)
        assert dataset.empty

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_batch_samples_error(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test batch sample generation with errors."""
        pool, conn = mock_connection_pool

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager
        )

        # Mock residual calculator to raise error
        with patch.object(generator.residual_calculator, 'calculate_residual_returns') as mock_calc:
            mock_calc.side_effect = Exception("Database error")

            samples = await generator._generate_batch_samples(
                [1], datetime(2024, 1, 1), datetime(2024, 1, 31)
            )

            # Should handle gracefully
            assert isinstance(samples, list)
            assert len(samples) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_training_sample_insufficient_data(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test training sample creation with insufficient data."""
        # Very little price data
        mock_universe_state_manager.get_lag_prices.return_value = pd.DataFrame({
            'close': [100, 101]  # Only 2 days
        })

        generator = ResidualReturnTrainingDataGenerator(
            None, None, mock_universe_state_manager,
            TrainingConfig(min_history_days=10)
        )

        residuals = pd.DataFrame({'residual_return': [0.01]}, index=[datetime(2024, 1, 15).date()])

        sample = await generator._create_training_sample(
            1, datetime(2024, 1, 15), residuals
        )

        assert sample is None  # Should return None for insufficient data

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_event_features_error(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test event feature extraction with errors."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Mock event extractor to raise error
        with patch.object(generator.event_extractor, 'extract_event_features') as mock_extract:
            mock_extract.side_effect = Exception("Event extraction failed")

            features = await generator._extract_event_features(123, datetime(2024, 1, 15))

            # Should return default features
            assert isinstance(features, dict)
            assert 'event_proximity_score' in features
            assert features['event_proximity_score'] == 0.0

    def test_extract_technical_features_error(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test technical feature extraction with errors."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        with patch('modeling.training_data_generator.calculate_all_technical_indicators') as mock_calc:
            mock_calc.side_effect = Exception("Technical calculation failed")

            features = generator._extract_technical_features(pd.DataFrame())

            # Should return empty dict
            assert isinstance(features, dict)
            assert len(features) == 0

    def test_get_future_residuals_no_data(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test future residual extraction with no data."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        empty_residuals = pd.DataFrame()

        future_residuals = generator._get_future_residuals(
            datetime(2024, 1, 15), empty_residuals, [1, 2, 3]
        )

        assert isinstance(future_residuals, dict)
        assert len(future_residuals) == 0

    def test_passes_basic_filters_empty_data(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test basic filters with empty data."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        empty_data = pd.DataFrame()

        assert generator._passes_basic_filters(empty_data) is False

    def test_days_since_high_low_edge_cases(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test days since high/low with edge cases."""
        generator = ResidualReturnTrainingDataGenerator(
            None, None, None
        )

        # Insufficient data
        short_prices = pd.Series([100, 101])

        days_high = generator._days_since_high(short_prices, window=10)
        days_low = generator._days_since_low(short_prices, window=10)

        assert days_high == 10  # Should return window size
        assert days_low == 10

        # Normal case
        normal_prices = pd.Series([100, 105, 102, 108, 103])  # High at index 3

        days_high = generator._days_since_high(normal_prices, window=5)
        assert days_high == 1  # 1 day since highest (108)


@pytest.mark.integration
class TestIntegrationScenarios:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_full_training_data_generation_workflow(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_residual_returns):
        """Test complete training data generation workflow."""
        pool, conn = mock_connection_pool

        # Setup comprehensive mocks
        conn.fetch.return_value = [{'id': 1}, {'id': 2}]  # Active instruments
        conn.fetchrow.return_value = {'sector': 'Technology', 'industry': 'Software'}

        # Setup price data
        price_data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108] * 10,  # 50 days
            'high': [102, 104, 106, 108, 110] * 10,
            'low': [98, 100, 102, 104, 106] * 10,
            'volume': [10000, 15000, 12000, 18000, 20000] * 10
        })
        mock_universe_state_manager.get_lag_prices.return_value = price_data

        generator = ResidualReturnTrainingDataGenerator(
            pool, mock_env, mock_universe_state_manager,
            TrainingConfig(
                lookback_days=30,
                prediction_horizons=[1, 2],
                min_history_days=10
            )
        )

        # Mock residual calculator
        with patch.object(generator.residual_calculator, 'calculate_residual_returns') as mock_calc:
            mock_calc.return_value = sample_residual_returns

            # Mock event features
            with patch.object(generator.event_extractor, 'extract_event_features') as mock_event:
                from domains.ml.services.event_features import EventFeatures
                mock_event.return_value = EventFeatures(
                    instrument_id=1, date=datetime(2024, 1, 15),
                    upcoming_events=[], historical_patterns={}, pre_event_sequences={},
                    event_proximity_score=0.5, event_importance_weighted_score=0.3
                )

                with patch('modeling.training_data_generator.flatten_event_features_for_model') as mock_flatten:
                    mock_flatten.return_value = {'event_score': 0.5}

                    with patch('modeling.training_data_generator.calculate_all_technical_indicators') as mock_tech:
                        mock_tech.return_value = {'rsi_14': 60.0, 'ema_20': 105.0}

                        # Generate dataset
                        dataset = await generator.generate_training_dataset(
                            datetime(2024, 1, 5), datetime(2024, 1, 10),
                            instrument_ids=[1, 2], batch_size=1
                        )

                        # Validate results
                        assert isinstance(dataset, pd.DataFrame)
                        assert not dataset.empty

                        # Check required columns
                        assert 'instrument_id' in dataset.columns
                        assert 'date' in dataset.columns

                        # Check features
                        feature_cols = [col for col in dataset.columns
                                      if not col.startswith(('residual_return_', 'positive_return_', 'meta_'))]
                        assert len(feature_cols) > 5  # Should have multiple features

                        # Check targets
                        target_cols = [col for col in dataset.columns if col.startswith('residual_return_')]
                        assert len(target_cols) >= 2  # Should have 1d, 2d targets

                        # All values should be finite
                        numeric_cols = dataset.select_dtypes(include=[np.number]).columns
                        for col in numeric_cols:
                            assert dataset[col].apply(lambda x: np.isfinite(x) if pd.notnull(x) else True).all(), f"Non-finite values in {col}"


if __name__ == "__main__":
    pytest.main([__file__])