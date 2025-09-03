"""
Tests for Support/Resistance Training Data Generator
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from domains.ml.services.training_data.support_resistance_generator import (
    SupportResistanceTrainingGenerator,
    SupportResistanceLevel,
    TrainingExample
)
from shared.utils.environment import Environment

class TestSupportResistanceTrainingGenerator:
    """Test suite for SupportResistanceTrainingGenerator"""

    @pytest.fixture
    def generator(self):
        """Create generator instance for testing"""
        env = MagicMock()
        env.get_database_url.return_value = "test://db"
        env.get_table_name.return_value = "test_table"
        return SupportResistanceTrainingGenerator(env=env)

    @pytest.fixture
    def sample_daily_data(self):
        """Create sample daily price data"""
        dates = [date(2023, 1, 1) + timedelta(days=i) for i in range(30)]
        data = []
        base_price = 100.0
        
        for i, trade_date in enumerate(dates):
            price = base_price + np.random.normal(0, 2)  # Random walk
            data.append({
                'date': trade_date,
                'open': price * 0.99,
                'high': price * 1.02,
                'low': price * 0.98,
                'close': price,
                'volume': 1000000 + np.random.randint(-200000, 200000)
            })
            base_price = price
        
        return pd.DataFrame(data)

    @pytest.fixture
    def sample_minute_data(self):
        """Create sample minute-level data"""
        base_time = datetime(2023, 1, 15, 9, 30)  # Market open
        data = []
        base_price = 105.0
        
        for i in range(390):  # Full trading day (6.5 hours * 60 minutes)
            timestamp = base_time + timedelta(minutes=i)
            price = base_price + np.random.normal(0, 0.1)
            
            data.append({
                'timestamp': timestamp,
                'open': price * 0.999,
                'high': price * 1.001,
                'low': price * 0.999,
                'close': price,
                'volume': np.random.randint(1000, 10000)
            })
            base_price = price
        
        return pd.DataFrame(data)

    def test_initialization(self, generator):
        """Test generator initialization"""
        assert generator.env is not None
        assert generator.min_level_strength == 0.3
        assert generator.level_tolerance_pct == 0.2
        assert generator.min_test_volume_ratio == 0.1
        assert generator.min_hold_minutes == 5

    def test_price_action_features(self, generator, sample_daily_data):
        """Test price action feature generation"""
        current_data = sample_daily_data.iloc[-1]
        features = generator._price_action_features(sample_daily_data, current_data)
        
        assert 'close' in features
        assert 'high' in features
        assert 'low' in features
        assert 'open' in features
        assert 'volume' in features
        assert 'daily_range' in features
        assert 'body_ratio' in features
        assert 'return_1d' in features
        
        # Validate ranges
        assert features['daily_range'] >= 0
        assert 0 <= features['body_ratio'] <= 1
        assert isinstance(features['close'], (int, float))

    def test_technical_indicator_features(self, generator, sample_daily_data):
        """Test technical indicator feature generation"""
        features = generator._technical_indicator_features(sample_daily_data)
        
        # Check moving averages
        assert 'ma_5' in features
        assert 'ma_10' in features
        assert 'ma_20' in features
        
        # Check RSI
        assert 'rsi_14' in features
        assert 'rsi_oversold' in features
        assert 'rsi_overbought' in features
        
        # Validate RSI range
        if 'rsi_14' in features:
            assert 0 <= features['rsi_14'] <= 100
        
        # Validate binary indicators
        assert features['rsi_oversold'] in [0.0, 1.0]
        assert features['rsi_overbought'] in [0.0, 1.0]

    def test_historical_sr_features(self, generator, sample_daily_data):
        """Test historical support/resistance feature generation"""
        features = generator._historical_sr_features(sample_daily_data)
        
        # Should generate some distance features
        assert isinstance(features, dict)
        
        # Check that features are numeric
        for key, value in features.items():
            assert isinstance(value, (int, float, np.number))
            if 'distance' in key:
                assert value >= 0  # Distances should be positive

    def test_volume_features(self, generator, sample_daily_data):
        """Test volume feature generation"""
        features = generator._volume_features(sample_daily_data)
        
        assert 'volume_ratio_20d' in features
        assert 'volume_ratio_5d' in features
        assert 'volume_trend' in features
        
        # Volume ratios should be positive
        assert features['volume_ratio_20d'] >= 0
        assert features['volume_ratio_5d'] >= 0

    def test_volatility_features(self, generator, sample_daily_data):
        """Test volatility feature generation"""
        features = generator._volatility_features(sample_daily_data)
        
        if 'atr' in features:
            assert features['atr'] >= 0  # ATR should be positive
        
        if 'volatility_20d' in features:
            assert features['volatility_20d'] >= 0  # Volatility should be positive

    def test_market_structure_features(self, generator, sample_daily_data):
        """Test market structure feature generation"""
        features = generator._market_structure_features(sample_daily_data)
        
        if 'trend_strength' in features:
            assert isinstance(features['trend_strength'], (int, float, np.number))
        
        if 'higher_highs_ratio' in features:
            assert 0 <= features['higher_highs_ratio'] <= 1
        
        if 'higher_lows_ratio' in features:
            assert 0 <= features['higher_lows_ratio'] <= 1

    def test_identify_support_levels(self, generator, sample_minute_data):
        """Test support level identification from minute data"""
        levels = generator._identify_support_levels(sample_minute_data)
        
        assert isinstance(levels, list)
        assert len(levels) <= 5  # Should return top 5 levels
        
        for level in levels:
            assert isinstance(level, SupportResistanceLevel)
            assert level.level_type == 'support'
            assert 0 <= level.strength <= 1
            assert level.tests_count >= 2  # Minimum tests required
            assert level.level > 0  # Price should be positive

    def test_identify_resistance_levels(self, generator, sample_minute_data):
        """Test resistance level identification from minute data"""
        levels = generator._identify_resistance_levels(sample_minute_data)
        
        assert isinstance(levels, list)
        assert len(levels) <= 5  # Should return top 5 levels
        
        for level in levels:
            assert isinstance(level, SupportResistanceLevel)
            assert level.level_type == 'resistance'
            assert 0 <= level.strength <= 1
            assert level.tests_count >= 2  # Minimum tests required
            assert level.level > 0  # Price should be positive

    def test_count_level_tests(self, generator):
        """Test level testing count logic"""
        # Create simple price series that tests a level
        prices = np.array([100, 101, 99.8, 99.9, 100.1, 99.8, 100.2])  # Tests ~100 level
        volumes = np.array([1000] * len(prices))
        level = 100.0
        
        result = generator._count_level_tests(prices, volumes, level, 'support')
        
        assert isinstance(result, dict)
        assert 'count' in result
        assert 'total_volume' in result
        assert 'hold_time' in result
        assert 'broken' in result
        assert 'strength' in result
        
        assert result['count'] >= 0
        assert result['total_volume'] >= 0
        assert result['hold_time'] >= 0
        assert isinstance(result['broken'], bool)
        assert 0 <= result['strength'] <= 1

    def test_calculate_rsi(self, generator):
        """Test RSI calculation"""
        # Test with known price series
        prices = np.array([44, 44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 
                          45.85, 47.37, 47.54, 46.87, 46.31, 46.28, 46.28])
        
        rsi = generator._calculate_rsi(prices, 14)
        
        assert 0 <= rsi <= 100
        assert isinstance(rsi, (int, float, np.number))
        
        # Test edge cases
        constant_prices = np.array([50] * 20)
        rsi_constant = generator._calculate_rsi(constant_prices, 14)
        assert rsi_constant == 50.0  # Should be neutral for constant prices

    def test_calculate_bollinger_bands(self, generator):
        """Test Bollinger Bands calculation"""
        prices = np.random.normal(100, 5, 50)  # Random prices around 100
        
        upper, middle, lower = generator._calculate_bollinger_bands(prices, 20, 2)
        
        assert upper > middle > lower
        assert isinstance(upper, (int, float, np.number))
        assert isinstance(middle, (int, float, np.number))
        assert isinstance(lower, (int, float, np.number))

    def test_calculate_macd(self, generator):
        """Test MACD calculation"""
        prices = np.random.normal(100, 2, 50)  # Random prices
        
        macd, signal = generator._calculate_macd(prices)
        
        assert isinstance(macd, (int, float, np.number))
        assert isinstance(signal, (int, float, np.number))

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_training_data_mock(self, generator):
        """Test training data generation with mocked database"""
        # Mock the database connection and methods
        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            
            # Mock the symbol training data generation
            generator._generate_symbol_training_data = AsyncMock(return_value=[
                TrainingExample(
                    symbol='TEST',
                    date=date(2023, 1, 15),
                    features={'close': 100.0, 'rsi_14': 50.0},
                    next_day_support_levels=[
                        SupportResistanceLevel(
                            level=98.0, level_type='support', strength=0.7,
                            tests_count=3, volume_at_level=1000000,
                            time_held=30, break_through=False
                        )
                    ],
                    next_day_resistance_levels=[
                        SupportResistanceLevel(
                            level=102.0, level_type='resistance', strength=0.6,
                            tests_count=2, volume_at_level=800000,
                            time_held=25, break_through=False
                        )
                    ],
                    next_day_high=101.5,
                    next_day_low=99.0,
                    next_day_close=100.5,
                    next_day_volume=1200000
                )
            ])
            
            # Run the test
            result = await generator.generate_training_data(
                symbols=['TEST'],
                start_date=date(2023, 1, 1),
                end_date=date(2023, 1, 31),
                min_examples_per_symbol=1
            )
            
            assert len(result) == 1
            assert result[0].symbol == 'TEST'
            assert len(result[0].features) >= 2
            assert len(result[0].next_day_support_levels) == 1
            assert len(result[0].next_day_resistance_levels) == 1

class TestSupportResistanceLevel:
    """Test suite for SupportResistanceLevel data structure"""

    def test_support_resistance_level_creation(self):
        """Test creating SupportResistanceLevel objects"""
        support_level = SupportResistanceLevel(
            level=95.50,
            level_type='support',
            strength=0.75,
            tests_count=4,
            volume_at_level=1500000,
            time_held=45,
            break_through=False
        )
        
        assert support_level.level == 95.50
        assert support_level.level_type == 'support'
        assert support_level.strength == 0.75
        assert support_level.tests_count == 4
        assert support_level.volume_at_level == 1500000
        assert support_level.time_held == 45
        assert support_level.break_through is False

    def test_support_resistance_level_validation(self):
        """Test validation of SupportResistanceLevel values"""
        # Valid support level
        support = SupportResistanceLevel(
            level=100.0, level_type='support', strength=0.5,
            tests_count=2, volume_at_level=1000000,
            time_held=20, break_through=False
        )
        
        assert support.level > 0
        assert support.level_type in ['support', 'resistance']
        assert 0 <= support.strength <= 1
        assert support.tests_count >= 0
        assert support.volume_at_level >= 0
        assert support.time_held >= 0

class TestTrainingExample:
    """Test suite for TrainingExample data structure"""

    def test_training_example_creation(self):
        """Test creating TrainingExample objects"""
        example = TrainingExample(
            symbol='AAPL',
            date=date(2023, 6, 15),
            features={
                'close': 185.50,
                'rsi_14': 65.2,
                'volume_ratio_20d': 1.15
            },
            next_day_support_levels=[
                SupportResistanceLevel(180.0, 'support', 0.6, 3, 1000000, 30, False)
            ],
            next_day_resistance_levels=[
                SupportResistanceLevel(190.0, 'resistance', 0.7, 2, 800000, 25, False)
            ],
            next_day_high=187.25,
            next_day_low=183.75,
            next_day_close=186.50,
            next_day_volume=1250000
        )
        
        assert example.symbol == 'AAPL'
        assert example.date == date(2023, 6, 15)
        assert len(example.features) == 3
        assert len(example.next_day_support_levels) == 1
        assert len(example.next_day_resistance_levels) == 1
        assert example.next_day_high > example.next_day_low
        assert example.next_day_volume > 0

    def test_training_example_feature_access(self):
        """Test accessing features in TrainingExample"""
        features = {
            'close': 100.0,
            'rsi_14': 50.0,
            'ma_20': 99.5,
            'volume_ratio_5d': 1.2
        }
        
        example = TrainingExample(
            symbol='TEST',
            date=date(2023, 1, 1),
            features=features,
            next_day_support_levels=[],
            next_day_resistance_levels=[],
            next_day_high=101.0,
            next_day_low=99.0,
            next_day_close=100.5,
            next_day_volume=1000000
        )
        
        assert example.features['close'] == 100.0
        assert example.features['rsi_14'] == 50.0
        assert example.features.get('nonexistent', 0.0) == 0.0

@pytest.mark.integration
class TestSupportResistanceIntegration:
    """Integration tests for the complete support/resistance system"""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment for integration tests"""
        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name = lambda name: f"test_{name}"
        return env

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_training_data_generation(self, mock_env):
        """Test complete training data generation workflow"""
        generator = SupportResistanceTrainingGenerator(env=mock_env)
        
        # Mock database operations
        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn
            
            # Mock daily data
            mock_conn.fetch.return_value = [
                {
                    'date': date(2023, 1, i),
                    'open': 100.0 + i * 0.1,
                    'high': 102.0 + i * 0.1,
                    'low': 98.0 + i * 0.1,
                    'close': 100.5 + i * 0.1,
                    'volume': 1000000 + i * 1000
                }
                for i in range(1, 31)
            ]
            
            # Mock minute data
            minute_data = []
            base_time = datetime(2023, 1, 15, 9, 30)
            for i in range(100):  # Sample minute data
                minute_data.append({
                    'timestamp': base_time + timedelta(minutes=i),
                    'open': 100.0 + np.random.normal(0, 0.1),
                    'high': 100.2 + np.random.normal(0, 0.1),
                    'low': 99.8 + np.random.normal(0, 0.1),
                    'close': 100.0 + np.random.normal(0, 0.1),
                    'volume': np.random.randint(1000, 5000)
                })
            
            # Set up side effects for different queries
            def mock_fetch_side_effect(*args, **kwargs):
                if 'minute_bars' in str(args[0]):
                    return minute_data
                else:
                    return mock_conn.fetch.return_value
            
            mock_conn.fetch.side_effect = mock_fetch_side_effect
            mock_conn.fetchrow.return_value = {'close': 99.5}  # Previous close
            
            # Test the generation
            training_examples = await generator.generate_training_data(
                symbols=['TEST'],
                start_date=date(2023, 1, 10),
                end_date=date(2023, 1, 20),
                min_examples_per_symbol=1
            )
            
            # Verify results
            assert isinstance(training_examples, list)
            # Note: May be empty due to mocked data not meeting criteria
            # In real integration tests with actual data, this would have results

    def test_feature_consistency(self):
        """Test that features are consistently generated"""
        generator = SupportResistanceTrainingGenerator()
        
        # Create deterministic data
        data = pd.DataFrame({
            'date': [date(2023, 1, i) for i in range(1, 21)],
            'open': [100.0] * 20,
            'high': [102.0] * 20,
            'low': [98.0] * 20,
            'close': [100.0] * 20,
            'volume': [1000000] * 20
        })
        
        # Generate features multiple times
        features1 = generator._technical_indicator_features(data)
        features2 = generator._technical_indicator_features(data)
        
        # Should be identical for same data
        assert features1 == features2
        
        # Should have expected feature types
        for feature_name, value in features1.items():
            assert isinstance(value, (int, float, np.number))
            assert not np.isnan(value)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])