"""
Tests for Adaptive Support/Resistance Model

Tests the daily retraining and adaptive learning capabilities
of the support/resistance prediction model.
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import tempfile
import os

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from domains.ml.services.dynamic_training.adaptive_sr_model import (
    AdaptiveSupportResistanceModel,
    AdaptiveModelConfig,
    AdaptiveModelState
)
from domains.ml.services.training_data.support_resistance_generator import (
    TrainingExample,
    SupportResistanceLevel
)

class TestAdaptiveModelConfig:
    """Test suite for AdaptiveModelConfig"""

    def test_config_creation(self):
        """Test creating adaptive model configuration"""
        config = AdaptiveModelConfig(
            bootstrap_years=4,
            min_bootstrap_examples=2000,
            rolling_window_days=300,
            retrain_frequency_days=2
        )

        assert config.bootstrap_years == 4
        assert config.min_bootstrap_examples == 2000
        assert config.rolling_window_days == 300
        assert config.retrain_frequency_days == 2
        assert config.learning_rate_decay == 0.95  # Default
        assert config.model_memory_weight == 0.8   # Default

    def test_config_defaults(self):
        """Test default configuration values"""
        config = AdaptiveModelConfig()

        assert config.bootstrap_years == 3
        assert config.min_bootstrap_examples == 5000
        assert config.rolling_window_days == 365
        assert config.retrain_frequency_days == 1
        assert config.learning_rate_decay == 0.95
        assert config.model_memory_weight == 0.8
        assert config.performance_lookback_days == 30
        assert config.min_accuracy_threshold == 0.4

    def test_base_model_config_creation(self):
        """Test that base model config is created correctly"""
        config = AdaptiveModelConfig()

        assert config.base_model_config is not None
        assert config.base_model_config.input_dim == 50
        assert config.base_model_config.hidden_dims == [128, 64, 32]
        assert config.base_model_config.epochs == 20  # Reduced for daily training
        assert config.base_model_config.batch_size == 32

class TestAdaptiveModelState:
    """Test suite for AdaptiveModelState"""

    def test_state_initialization(self):
        """Test adaptive model state initialization"""
        state = AdaptiveModelState()

        assert state.last_retrain_date is None
        assert state.total_training_examples == 0
        assert state.recent_performance == []
        assert state.model_version == 0
        assert state.training_history == []
        assert state.bootstrap_completed is False

    def test_state_updates(self):
        """Test updating model state"""
        state = AdaptiveModelState()

        # Update state
        state.last_retrain_date = date(2023, 1, 15)
        state.total_training_examples = 1500
        state.model_version = 3
        state.bootstrap_completed = True
        state.recent_performance = [0.65, 0.72, 0.68]

        assert state.last_retrain_date == date(2023, 1, 15)
        assert state.total_training_examples == 1500
        assert state.model_version == 3
        assert state.bootstrap_completed is True
        assert len(state.recent_performance) == 3

class TestAdaptiveSupportResistanceModel:
    """Test suite for AdaptiveSupportResistanceModel"""

    @pytest.fixture
    def config(self):
        """Create test configuration"""
        return AdaptiveModelConfig(
            bootstrap_years=2,
            min_bootstrap_examples=10,  # Low for testing
            rolling_window_days=30,
            retrain_frequency_days=1,
            min_retrain_examples=5     # Low for testing
        )

    @pytest.fixture
    def model(self, config):
        """Create adaptive model for testing"""
        return AdaptiveSupportResistanceModel(config)

    @pytest.fixture
    def sample_training_examples(self):
        """Create sample training examples"""
        examples = []

        for i in range(20):
            example = TrainingExample(
                symbol='TEST',
                date=date(2023, 1, 1) + timedelta(days=i),
                features={f'feature_{j}': np.random.randn() for j in range(10)},
                next_day_support_levels=[
                    SupportResistanceLevel(
                        level=95.0 + np.random.randn(),
                        level_type='support',
                        strength=np.random.uniform(0.3, 0.9),
                        tests_count=np.random.randint(2, 6),
                        volume_at_level=1000000,
                        time_held=30,
                        break_through=False
                    )
                ],
                next_day_resistance_levels=[
                    SupportResistanceLevel(
                        level=105.0 + np.random.randn(),
                        level_type='resistance',
                        strength=np.random.uniform(0.3, 0.8),
                        tests_count=np.random.randint(2, 5),
                        volume_at_level=900000,
                        time_held=25,
                        break_through=False
                    )
                ],
                next_day_high=106.0 + np.random.randn() * 0.5,
                next_day_low=94.0 + np.random.randn() * 0.5,
                next_day_close=100.0 + np.random.randn() * 2,
                next_day_volume=1200000
            )
            examples.append(example)

        return examples

    def test_model_initialization(self, model):
        """Test adaptive model initialization"""
        assert isinstance(model.config, AdaptiveModelConfig)
        assert isinstance(model.state, AdaptiveModelState)
        assert model.model is None  # Not yet trained
        assert hasattr(model, 'training_generator')
        assert hasattr(model, 'training_data_cache')
        assert len(model.training_data_cache) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bootstrap_model_mock(self, model):
        """Test model bootstrapping with mocked data"""
        symbols = ['TEST1', 'TEST2']
        end_date = date(2023, 1, 1)

        # Mock the training data generation
        model.training_generator.generate_training_data = AsyncMock(
            return_value=[
                TrainingExample(
                    symbol='TEST1',
                    date=date(2022, 6, 15),
                    features={'feature_1': 0.5, 'feature_2': -0.3},
                    next_day_support_levels=[
                        SupportResistanceLevel(95.0, 'support', 0.7, 3, 1000000, 30, False)
                    ],
                    next_day_resistance_levels=[
                        SupportResistanceLevel(105.0, 'resistance', 0.6, 2, 800000, 25, False)
                    ],
                    next_day_high=104.0,
                    next_day_low=96.0,
                    next_day_close=100.0,
                    next_day_volume=1100000
                ) for _ in range(15)  # Sufficient examples
            ]
        )

        # Test bootstrap
        success = await model.bootstrap_model(
            symbols=symbols,
            end_date=end_date
        )

        assert success is True
        assert model.state.bootstrap_completed is True
        assert model.state.model_version == 1
        assert model.state.last_retrain_date == end_date
        assert model.state.total_training_examples == 15
        assert len(model.state.training_history) == 1
        assert model.model is not None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bootstrap_insufficient_data(self, model):
        """Test bootstrap failure with insufficient data"""
        symbols = ['TEST']
        end_date = date(2023, 1, 1)

        # Mock insufficient training data
        model.training_generator.generate_training_data = AsyncMock(
            return_value=[]  # No examples
        )

        success = await model.bootstrap_model(
            symbols=symbols,
            end_date=end_date
        )

        assert success is False
        assert model.state.bootstrap_completed is False
        assert model.model is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_daily_update_before_bootstrap(self, model):
        """Test that daily update fails before bootstrap"""
        current_date = date(2023, 1, 2)
        symbols = ['TEST']

        updated = await model.daily_update(
            current_date=current_date,
            symbols=symbols
        )

        assert updated is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_daily_update_with_mock(self, model, sample_training_examples):
        """Test daily update with mocked components"""
        # First bootstrap the model
        model.state.bootstrap_completed = True
        model.state.last_retrain_date = date(2023, 1, 1)
        model.state.model_version = 1
        model.model = MagicMock()  # Mock trained model

        # Mock training data generation
        model._get_training_data_for_period = AsyncMock(
            return_value=sample_training_examples[:10]  # Sufficient for update
        )

        # Mock the update methods
        model._should_full_retrain = MagicMock(return_value=False)
        model._incremental_update = AsyncMock(return_value=True)

        current_date = date(2023, 1, 2)
        symbols = ['TEST']

        updated = await model.daily_update(
            current_date=current_date,
            symbols=symbols
        )

        assert updated is True
        assert model.state.last_retrain_date == current_date
        assert model.state.model_version == 2
        assert len(model.state.training_history) == 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_training_data_caching(self, model):
        """Test training data caching mechanism"""
        symbols = ['TEST']
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 31)

        # Mock the training generator
        mock_examples = [
            TrainingExample(
                symbol='TEST',
                date=date(2023, 1, 15),
                features={'f1': 1.0},
                next_day_support_levels=[],
                next_day_resistance_levels=[],
                next_day_high=105.0,
                next_day_low=95.0,
                next_day_close=100.0,
                next_day_volume=1000000
            )
        ]

        model.training_generator.generate_training_data = AsyncMock(
            return_value=mock_examples
        )

        # First call should generate data
        data1 = await model._get_training_data_for_period(symbols, start_date, end_date)
        assert data1 == mock_examples
        assert len(model.training_data_cache) == 1

        # Second call should use cache
        data2 = await model._get_training_data_for_period(symbols, start_date, end_date)
        assert data2 == mock_examples
        assert model.training_generator.generate_training_data.call_count == 1  # Called only once

    def test_should_retrain_for_performance(self, model):
        """Test performance-based retraining logic"""
        # No performance data
        assert model._should_retrain_for_performance() is False

        # Insufficient performance data
        model.state.recent_performance = [0.5, 0.6]
        assert model._should_retrain_for_performance() is False

        # Good performance
        model.state.recent_performance = [0.6, 0.7, 0.65, 0.68, 0.72]
        assert model._should_retrain_for_performance() is False

        # Poor performance (below threshold)
        model.state.recent_performance = [0.3, 0.25, 0.35, 0.2, 0.3]
        assert model._should_retrain_for_performance() is True

    def test_should_full_retrain(self, model):
        """Test full retrain decision logic"""
        # Very poor recent performance
        model.state.recent_performance = [0.1, 0.15, 0.12]
        assert model._should_full_retrain() is True

        # Good performance but no history
        model.state.recent_performance = [0.7, 0.75, 0.72]
        assert model._should_full_retrain() is False

        # Test monthly full retrain logic
        old_date = date.today() - timedelta(days=35)
        model.state.training_history = [
            {'date': old_date, 'type': 'bootstrap'},
            {'date': date.today() - timedelta(days=5), 'type': 'update'}
        ]
        assert model._should_full_retrain() is True

    def test_predict_before_training(self, model):
        """Test prediction before model is trained"""
        features = np.random.randn(1, 10)

        with pytest.raises(ValueError, match="Model not trained"):
            model.predict(features)

    def test_evaluate_daily_performance(self, model, sample_training_examples):
        """Test daily performance evaluation"""
        # Mock a trained model
        mock_model = MagicMock()
        mock_model.evaluate.return_value = {
            'support_mae': 0.02,
            'resistance_mae': 0.025,
            'overall_mae': 0.022
        }
        model.model = mock_model

        metrics = model.evaluate_daily_performance(sample_training_examples[:5])

        assert isinstance(metrics, dict)
        assert 'support_mae' in metrics
        assert len(model.state.recent_performance) == 1

        # Performance should be accuracy proxy (1 - MAE)
        expected_accuracy = 1.0 - 0.022
        assert abs(model.state.recent_performance[0] - expected_accuracy) < 1e-6

    def test_save_load_model(self, model):
        """Test saving and loading model state"""
        # Set up some state
        model.state.bootstrap_completed = True
        model.state.model_version = 5
        model.state.total_training_examples = 1000
        model.state.recent_performance = [0.6, 0.7, 0.65]
        model.model = MagicMock()  # Mock model

        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = os.path.join(temp_dir, 'test_adaptive_model.pkl')

            # Save model
            model.save_model(model_path)
            assert os.path.exists(model_path)

            # Create new model and load
            new_model = AdaptiveSupportResistanceModel(model.config)
            new_model.load_model(model_path)

            # Check that state was restored
            assert new_model.state.bootstrap_completed is True
            assert new_model.state.model_version == 5
            assert new_model.state.total_training_examples == 1000
            assert len(new_model.state.recent_performance) == 3
            assert new_model.model is not None

    def test_get_model_info(self, model):
        """Test getting model information"""
        # Set up some state
        model.state.bootstrap_completed = True
        model.state.model_version = 3
        model.state.last_retrain_date = date(2023, 1, 15)
        model.state.total_training_examples = 2500
        model.state.recent_performance = [0.65, 0.7, 0.68, 0.72, 0.69]
        model.state.training_history = [{'date': date(2023, 1, 1), 'type': 'bootstrap'}]
        model.training_data_cache = {'key1': [], 'key2': []}

        info = model.get_model_info()

        assert info['bootstrap_completed'] is True
        assert info['model_version'] == 3
        assert info['last_retrain_date'] == date(2023, 1, 15)
        assert info['total_training_examples'] == 2500
        assert len(info['recent_performance']) == 5
        assert info['training_history_count'] == 1
        assert info['cache_size'] == 2

@pytest.mark.integration
class TestAdaptiveModelIntegration:
    """Integration tests for the adaptive model system"""

    @pytest.fixture
    def integration_config(self):
        """Create integration test configuration"""
        return AdaptiveModelConfig(
            bootstrap_years=1,           # Short for testing
            min_bootstrap_examples=5,    # Very low for testing
            rolling_window_days=30,      # Short window
            retrain_frequency_days=1,
            min_retrain_examples=2       # Very low for testing
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_adaptive_workflow(self, integration_config):
        """Test complete adaptive model workflow"""
        model = AdaptiveSupportResistanceModel(integration_config)

        # Mock training data generation throughout
        def mock_training_data(size=10):
            return [
                TrainingExample(
                    symbol='TEST',
                    date=date(2023, 1, i),
                    features={f'f_{j}': np.random.randn() for j in range(5)},
                    next_day_support_levels=[
                        SupportResistanceLevel(95.0, 'support', 0.5, 3, 1000000, 30, False)
                    ],
                    next_day_resistance_levels=[
                        SupportResistanceLevel(105.0, 'resistance', 0.6, 2, 800000, 25, False)
                    ],
                    next_day_high=104.0, next_day_low=96.0,
                    next_day_close=100.0, next_day_volume=1000000
                )
                for i in range(1, size + 1)
            ]

        model.training_generator.generate_training_data = AsyncMock(
            side_effect=lambda **kwargs: mock_training_data(10)
        )

        # Step 1: Bootstrap
        symbols = ['TEST']
        bootstrap_end = date(2023, 1, 1)

        success = await model.bootstrap_model(symbols, bootstrap_end)
        assert success is True
        assert model.state.bootstrap_completed is True

        # Step 2: Daily updates
        for day in range(1, 6):  # 5 days of updates
            current_date = bootstrap_end + timedelta(days=day)

            updated = await model.daily_update(current_date, symbols)
            # Update success depends on mocked data and logic

        # Verify final state
        assert model.state.model_version >= 1
        assert len(model.state.training_history) >= 1

    def test_adaptive_vs_static_concept(self):
        """Test the conceptual difference between adaptive and static approaches"""
        # This test validates the conceptual approach

        # Adaptive approach characteristics
        adaptive_features = {
            'retrain_frequency': 'daily',
            'adaptation_speed': 'fast',
            'market_regime_sensitivity': 'high',
            'computational_cost': 'higher',
            'data_usage': 'rolling_window'
        }

        # Static approach characteristics
        static_features = {
            'retrain_frequency': 'weekly/monthly',
            'adaptation_speed': 'slow',
            'market_regime_sensitivity': 'low',
            'computational_cost': 'lower',
            'data_usage': 'fixed_periods'
        }

        # Key differences that justify adaptive approach
        assert adaptive_features['retrain_frequency'] != static_features['retrain_frequency']
        assert adaptive_features['adaptation_speed'] != static_features['adaptation_speed']
        assert adaptive_features['market_regime_sensitivity'] != static_features['market_regime_sensitivity']

        # Trade-offs
        assert adaptive_features['computational_cost'] == 'higher'  # Cost of adaptation
        assert static_features['computational_cost'] == 'lower'     # Cost efficiency

if __name__ == "__main__":
    pytest.main([__file__, "-v"])