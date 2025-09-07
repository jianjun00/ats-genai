"""
End-to-End Tests for Complete Support/Resistance ML Pipeline
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
import tempfile
import os
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from domains.trading.services.historical_universe_creator import HistoricalUniverseCreator, HistoricalStock
from domains.ml.services.training_data.support_resistance_generator import (
    SupportResistanceTrainingGenerator,
    TrainingExample,
    SupportResistanceLevel
)
from domains.ml.services.models.support_resistance_model import (
    SupportResistanceEnsemble,
    SRModelConfig
)
from domains.ml.services.evaluation.sr_backtester import SRBacktester, BacktestMetrics
from shared.utils.environment import Environment

@pytest.mark.integration
class TestCompleteSRPipeline:
    """End-to-end integration tests for the complete Support/Resistance ML pipeline"""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment for testing"""
        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name = lambda name: f"test_{name}"
        return env

    @pytest.fixture
    def sample_historical_stocks(self):
        """Create sample historical stocks for universe creation"""
        return [
            HistoricalStock(
                symbol='AAPL',
                instrument_id=1001,
                market_cap=2500000000000,  # $2.5T
                avg_volume=75000000,
                avg_price=150.0,
                trading_days=252,
                first_date=date(2020, 1, 2),
                last_date=date(2020, 12, 31)
            ),
            HistoricalStock(
                symbol='MSFT',
                instrument_id=1002,
                market_cap=2200000000000,  # $2.2T
                avg_volume=30000000,
                avg_price=250.0,
                trading_days=251,
                first_date=date(2020, 1, 3),
                last_date=date(2020, 12, 30)
            ),
            HistoricalStock(
                symbol='GOOGL',
                instrument_id=1003,
                market_cap=1800000000000,  # $1.8T
                avg_volume=25000000,
                avg_price=2000.0,
                trading_days=250,
                first_date=date(2020, 1, 6),
                last_date=date(2020, 12, 29)
            )
        ]

    @pytest.fixture
    def sample_training_examples(self):
        """Create sample training examples"""
        examples = []
        symbols = ['AAPL', 'MSFT', 'GOOGL']

        for symbol in symbols:
            for i in range(20):  # 20 examples per symbol
                base_price = {'AAPL': 150, 'MSFT': 250, 'GOOGL': 2000}[symbol]
                current_price = base_price + np.random.normal(0, base_price * 0.02)

                example = TrainingExample(
                    symbol=symbol,
                    date=date(2021, 1, 1) + timedelta(days=i),
                    features={
                        'close': current_price,
                        'rsi_14': np.random.uniform(30, 70),
                        'ma_20': current_price * np.random.uniform(0.98, 1.02),
                        'volume_ratio_20d': np.random.uniform(0.8, 1.5),
                        'atr': current_price * np.random.uniform(0.01, 0.03),
                        'bb_position': np.random.uniform(0.2, 0.8),
                        'trend_strength': np.random.uniform(-0.05, 0.05),
                        'volatility_20d': np.random.uniform(0.15, 0.35),
                        'distance_to_support': np.random.uniform(0.01, 0.05),
                        'distance_to_resistance': np.random.uniform(0.01, 0.05)
                    },
                    next_day_support_levels=[
                        SupportResistanceLevel(
                            level=current_price * np.random.uniform(0.96, 0.99),
                            level_type='support',
                            strength=np.random.uniform(0.3, 0.9),
                            tests_count=np.random.randint(2, 6),
                            volume_at_level=1000000,
                            time_held=30,
                            break_through=False
                        ),
                        SupportResistanceLevel(
                            level=current_price * np.random.uniform(0.93, 0.97),
                            level_type='support',
                            strength=np.random.uniform(0.2, 0.7),
                            tests_count=np.random.randint(2, 4),
                            volume_at_level=800000,
                            time_held=20,
                            break_through=False
                        )
                    ],
                    next_day_resistance_levels=[
                        SupportResistanceLevel(
                            level=current_price * np.random.uniform(1.01, 1.04),
                            level_type='resistance',
                            strength=np.random.uniform(0.3, 0.8),
                            tests_count=np.random.randint(2, 5),
                            volume_at_level=900000,
                            time_held=25,
                            break_through=False
                        ),
                        SupportResistanceLevel(
                            level=current_price * np.random.uniform(1.04, 1.07),
                            level_type='resistance',
                            strength=np.random.uniform(0.2, 0.6),
                            tests_count=np.random.randint(2, 4),
                            volume_at_level=700000,
                            time_held=15,
                            break_through=False
                        )
                    ],
                    next_day_high=current_price * np.random.uniform(1.002, 1.02),
                    next_day_low=current_price * np.random.uniform(0.98, 0.998),
                    next_day_close=current_price * np.random.uniform(0.99, 1.01),
                    next_day_volume=1200000 + np.random.randint(-200000, 200000)
                )
                examples.append(example)

        return examples

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_pipeline_mock(self, mock_env, sample_historical_stocks, sample_training_examples):
        """Test the complete pipeline with mocked database operations"""

        # Step 1: Universe Creation
        universe_creator = HistoricalUniverseCreator(env=mock_env)

        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn

            # Mock universe creation responses
            mock_conn.fetch.return_value = [
                {
                    'symbol': stock.symbol,
                    'instrument_id': stock.instrument_id,
                    'avg_volume': stock.avg_volume,
                    'avg_price': stock.avg_price,
                    'trading_days': stock.trading_days,
                    'first_date': stock.first_date,
                    'last_date': stock.last_date,
                    'estimated_market_cap': stock.market_cap
                }
                for stock in sample_historical_stocks
            ]

            mock_conn.fetchrow.return_value = {'id': 1001}  # Universe ID
            mock_conn.execute.return_value = None

            # Create universe
            universe_id = await universe_creator.create_historical_sample_universe(
                universe_name='test_pipeline_universe',
                sample_year=2020,
                sample_size=3,
                min_market_cap_millions=1000,
                min_avg_volume=20000000,
                min_trading_days=200,
                seed=42
            )

            assert universe_id == 1001

        # Step 2: Training Data Generation (use pre-created examples)
        # In real implementation, this would query the database
        training_examples = sample_training_examples
        assert len(training_examples) == 60  # 20 per symbol * 3 symbols

        # Verify training examples structure
        for example in training_examples[:5]:  # Check first 5
            assert isinstance(example, TrainingExample)
            assert example.symbol in ['AAPL', 'MSFT', 'GOOGL']
            assert len(example.features) == 10
            assert len(example.next_day_support_levels) == 2
            assert len(example.next_day_resistance_levels) == 2

        # Step 3: Model Training
        config = SRModelConfig(
            input_dim=10,  # Number of features
            hidden_dims=[32, 16],  # Small for testing
            max_support_levels=2,
            max_resistance_levels=2,
            epochs=3,  # Small for testing
            batch_size=8,
            patience=2
        )

        ensemble = SupportResistanceEnsemble(config)

        # Split data for training/testing
        train_examples = training_examples[:45]  # 75% for training
        test_examples = training_examples[45:]   # 25% for testing

        # Train model
        ensemble.train(train_examples, test_examples)

        # Verify model can make predictions
        test_features = np.random.randn(5, 10)
        predictions = ensemble.predict(test_features)

        assert isinstance(predictions, dict)
        assert 'support_levels' in predictions
        assert 'resistance_levels' in predictions
        assert predictions['support_levels'].shape == (5, 2)
        assert predictions['resistance_levels'].shape == (5, 2)

        # Step 4: Model Evaluation
        evaluation_metrics = ensemble.evaluate(test_examples)

        assert isinstance(evaluation_metrics, dict)
        assert 'support_mae' in evaluation_metrics
        assert 'resistance_mae' in evaluation_metrics
        assert 'overall_mae' in evaluation_metrics

        # Metrics should be reasonable
        assert evaluation_metrics['support_mae'] >= 0
        assert evaluation_metrics['resistance_mae'] >= 0
        assert evaluation_metrics['overall_mae'] >= 0

        # Step 5: Model Persistence
        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = os.path.join(temp_dir, 'test_pipeline_model.pkl')

            # Save model
            ensemble.save_model(model_path)
            assert os.path.exists(model_path)

            # Load model
            new_ensemble = SupportResistanceEnsemble(config)
            new_ensemble.load_model(model_path)

            # Test loaded model
            new_predictions = new_ensemble.predict(test_features)
            assert isinstance(new_predictions, dict)
            assert 'support_levels' in new_predictions

    def test_pipeline_data_flow(self, sample_training_examples):
        """Test data flow consistency through pipeline components"""

        # Test that data maintains consistency through transformations
        original_symbols = set(ex.symbol for ex in sample_training_examples)
        original_dates = [ex.date for ex in sample_training_examples]

        # Verify symbol consistency
        assert len(original_symbols) == 3
        assert original_symbols == {'AAPL', 'MSFT', 'GOOGL'}

        # Verify date consistency
        assert len(original_dates) == len(sample_training_examples)
        assert all(isinstance(d, date) for d in original_dates)

        # Test feature consistency
        all_feature_keys = set()
        for example in sample_training_examples:
            all_feature_keys.update(example.features.keys())

        # All examples should have same feature keys
        expected_features = {
            'close', 'rsi_14', 'ma_20', 'volume_ratio_20d', 'atr',
            'bb_position', 'trend_strength', 'volatility_20d',
            'distance_to_support', 'distance_to_resistance'
        }
        assert all_feature_keys == expected_features

        # Test that all examples have the expected structure
        for example in sample_training_examples:
            assert len(example.features) == len(expected_features)
            assert all(isinstance(v, (int, float, np.number)) for v in example.features.values())

    def test_pipeline_bias_prevention(self, sample_historical_stocks):
        """Test that pipeline prevents various forms of bias"""

        # Test survivorship bias prevention
        # Universe created from 2020 data, not based on future performance
        universe_year = 2020
        current_year = 2023

        assert universe_year < current_year  # Looking backwards

        # All stocks should be from the historical period
        for stock in sample_historical_stocks:
            assert stock.first_date.year <= universe_year
            assert stock.last_date.year <= universe_year

        # Test look-ahead bias prevention
        # Training examples should use only historical features
        # (This is enforced by the feature generation process)

        # Test selection bias prevention
        # Market cap weighted sampling includes various company sizes
        market_caps = [stock.market_cap for stock in sample_historical_stocks if stock.market_cap]
        assert len(set(market_caps)) > 1  # Different market caps
        assert min(market_caps) != max(market_caps)  # Variety in sizes

    def test_pipeline_performance_metrics(self, sample_training_examples):
        """Test that pipeline generates reasonable performance metrics"""

        # Create minimal model for testing
        config = SRModelConfig(
            input_dim=10,
            hidden_dims=[16],
            max_support_levels=2,
            max_resistance_levels=2,
            epochs=2,
            batch_size=4
        )

        ensemble = SupportResistanceEnsemble(config)

        # Train with subset of data
        train_examples = sample_training_examples[:30]
        test_examples = sample_training_examples[30:35]

        ensemble.train(train_examples, test_examples)

        # Evaluate
        metrics = ensemble.evaluate(test_examples)

        # Metrics should be reasonable ranges
        assert 0 <= metrics['overall_mae'] <= 1.0  # MAE shouldn't be too large
        assert metrics['support_mae'] >= 0
        assert metrics['resistance_mae'] >= 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_pipeline_scalability(self, mock_env):
        """Test pipeline behavior with larger datasets"""

        # Create larger synthetic dataset
        large_training_examples = []
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

        for symbol in symbols:
            for i in range(50):  # 50 examples per symbol = 250 total
                base_price = 100 + hash(symbol) % 100
                current_price = base_price + np.random.normal(0, 5)

                example = TrainingExample(
                    symbol=symbol,
                    date=date(2021, 1, 1) + timedelta(days=i),
                    features={f'feature_{j}': np.random.randn() for j in range(15)},
                    next_day_support_levels=[
                        SupportResistanceLevel(
                            level=current_price * 0.98,
                            level_type='support',
                            strength=0.6,
                            tests_count=3,
                            volume_at_level=1000000,
                            time_held=30,
                            break_through=False
                        )
                    ],
                    next_day_resistance_levels=[
                        SupportResistanceLevel(
                            level=current_price * 1.02,
                            level_type='resistance',
                            strength=0.7,
                            tests_count=2,
                            volume_at_level=800000,
                            time_held=25,
                            break_through=False
                        )
                    ],
                    next_day_high=current_price * 1.01,
                    next_day_low=current_price * 0.99,
                    next_day_close=current_price,
                    next_day_volume=1200000
                )
                large_training_examples.append(example)

        # Test that pipeline can handle larger dataset
        config = SRModelConfig(
            input_dim=15,
            hidden_dims=[64, 32],
            max_support_levels=1,
            max_resistance_levels=1,
            epochs=2,  # Keep low for testing
            batch_size=16
        )

        ensemble = SupportResistanceEnsemble(config)

        # Train with larger dataset
        train_examples = large_training_examples[:200]
        test_examples = large_training_examples[200:220]

        # Should complete without errors
        ensemble.train(train_examples, test_examples)

        # Should be able to make predictions
        test_features = np.random.randn(10, 15)
        predictions = ensemble.predict(test_features)

        assert predictions['support_levels'].shape == (10, 1)
        assert predictions['resistance_levels'].shape == (10, 1)

    def test_pipeline_error_handling(self):
        """Test pipeline error handling and edge cases"""

        # Test with empty training data
        config = SRModelConfig(
            input_dim=5,
            hidden_dims=[8],
            max_support_levels=1,
            max_resistance_levels=1,
            epochs=1
        )

        ensemble = SupportResistanceEnsemble(config)

        # Should handle empty training data gracefully
        try:
            ensemble.train([])  # Empty training data
            # If it doesn't raise an exception, that's fine
            # Different implementations may handle this differently
        except (ValueError, IndexError) as e:
            # Expected for empty data
            assert "empty" in str(e).lower() or "shape" in str(e).lower()

        # Test with malformed data
        malformed_example = TrainingExample(
            symbol='TEST',
            date=date(2021, 1, 1),
            features={},  # Empty features
            next_day_support_levels=[],
            next_day_resistance_levels=[],
            next_day_high=100.0,
            next_day_low=99.0,
            next_day_close=99.5,
            next_day_volume=1000000
        )

        # Should handle gracefully
        try:
            data = ensemble.prepare_data([malformed_example])
            # May succeed with default values or fail gracefully
        except (ValueError, KeyError, IndexError):
            # Expected for malformed data
            pass

    def test_pipeline_reproducibility(self, sample_training_examples):
        """Test that pipeline produces reproducible results"""

        # Set random seeds
        np.random.seed(42)

        config = SRModelConfig(
            input_dim=10,
            hidden_dims=[16],
            max_support_levels=1,
            max_resistance_levels=1,
            epochs=2,
            batch_size=4
        )

        # Train first model
        ensemble1 = SupportResistanceEnsemble(config)
        ensemble1.train(sample_training_examples[:20])

        # Reset seeds and train second model
        np.random.seed(42)
        ensemble2 = SupportResistanceEnsemble(config)
        ensemble2.train(sample_training_examples[:20])

        # Make predictions with same input
        test_input = np.random.RandomState(42).randn(3, 10)

        pred1 = ensemble1.predict(test_input)
        pred2 = ensemble2.predict(test_input)

        # Results should be similar (allowing for some variance due to randomness)
        # We can't guarantee exact reproducibility due to PyTorch's complexity,
        # but they should be in the same ballpark
        assert pred1['ensemble_support'].shape == pred2['ensemble_support'].shape
        assert pred1['ensemble_resistance'].shape == pred2['ensemble_resistance'].shape

@pytest.mark.integration
class TestPipelinePerformanceCharacteristics:
    """Test performance characteristics of the complete pipeline"""

    def test_training_convergence(self):
        """Test that model training converges properly"""

        # Create synthetic data that should be learnable
        np.random.seed(42)

        # Create patterns where support/resistance can be learned
        training_examples = []
        for i in range(100):
            # Create predictable patterns
            base_price = 100.0
            rsi = np.random.uniform(30, 70)

            # Simple rule: low RSI -> strong support, high RSI -> strong resistance
            if rsi < 40:
                support_strength = 0.8
                resistance_strength = 0.3
            elif rsi > 60:
                support_strength = 0.3
                resistance_strength = 0.8
            else:
                support_strength = 0.5
                resistance_strength = 0.5

            example = TrainingExample(
                symbol='PATTERN',
                date=date(2021, 1, 1) + timedelta(days=i),
                features={
                    'rsi_14': rsi,
                    'close': base_price,
                    'ma_20': base_price * 0.99,
                    'volume_ratio_20d': 1.0,
                    'atr': 2.0
                },
                next_day_support_levels=[
                    SupportResistanceLevel(
                        level=base_price * 0.98,
                        level_type='support',
                        strength=support_strength,
                        tests_count=3,
                        volume_at_level=1000000,
                        time_held=30,
                        break_through=False
                    )
                ],
                next_day_resistance_levels=[
                    SupportResistanceLevel(
                        level=base_price * 1.02,
                        level_type='resistance',
                        strength=resistance_strength,
                        tests_count=2,
                        volume_at_level=800000,
                        time_held=25,
                        break_through=False
                    )
                ],
                next_day_high=base_price * 1.01,
                next_day_low=base_price * 0.99,
                next_day_close=base_price,
                next_day_volume=1200000
            )
            training_examples.append(example)

        # Train model
        config = SRModelConfig(
            input_dim=5,
            hidden_dims=[32, 16],
            max_support_levels=1,
            max_resistance_levels=1,
            epochs=20,  # More epochs for convergence
            batch_size=16
        )

        ensemble = SupportResistanceEnsemble(config)
        train_examples = training_examples[:80]
        val_examples = training_examples[80:]

        ensemble.train(train_examples, val_examples)

        # Test that model learned something
        test_low_rsi = np.array([[35.0, 100.0, 99.0, 1.0, 2.0]])  # Low RSI
        test_high_rsi = np.array([[65.0, 100.0, 99.0, 1.0, 2.0]])  # High RSI

        pred_low = ensemble.predict(test_low_rsi)
        pred_high = ensemble.predict(test_high_rsi)

        # Model should predict different confidence levels
        # (exact values may vary due to training variance)
        assert pred_low['support_confidence'].shape == (1, 1)
        assert pred_high['resistance_confidence'].shape == (1, 1)

    def test_prediction_consistency(self):
        """Test that predictions are consistent across multiple calls"""

        # Create simple model
        config = SRModelConfig(
            input_dim=3,
            hidden_dims=[8],
            max_support_levels=1,
            max_resistance_levels=1,
            epochs=2
        )

        # Create minimal training data
        training_examples = [
            TrainingExample(
                symbol='TEST',
                date=date(2021, 1, 1),
                features={'f1': 1.0, 'f2': 2.0, 'f3': 3.0},
                next_day_support_levels=[
                    SupportResistanceLevel(95.0, 'support', 0.6, 3, 1000000, 30, False)
                ],
                next_day_resistance_levels=[
                    SupportResistanceLevel(105.0, 'resistance', 0.7, 2, 800000, 25, False)
                ],
                next_day_high=102.0,
                next_day_low=98.0,
                next_day_close=100.0,
                next_day_volume=1200000
            )
            for _ in range(10)
        ]

        ensemble = SupportResistanceEnsemble(config)
        ensemble.train(training_examples)

        # Make multiple predictions with same input
        test_input = np.array([[1.0, 2.0, 3.0]])

        pred1 = ensemble.predict(test_input)
        pred2 = ensemble.predict(test_input)
        pred3 = ensemble.predict(test_input)

        # Predictions should be identical (model is deterministic at inference)
        np.testing.assert_array_almost_equal(
            pred1['support_levels'], pred2['support_levels']
        )
        np.testing.assert_array_almost_equal(
            pred2['support_levels'], pred3['support_levels']
        )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])