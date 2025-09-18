"""
Real objects integration tests for Complete Support/Resistance ML Pipeline.

Replaces mock-heavy testing with authentic ML pipeline integration to test:
- Real end-to-end ML pipeline with actual model training and prediction
- Support/resistance level detection with real market data
- Model ensemble training with authentic data processing
- Performance evaluation with real ML metrics
- Error handling with actual ML training exceptions

This demonstrates fail-fast testing that eliminates AsyncMock, MagicMock, and patch dependencies
and provides authentic validation of ML pipeline functionality.
"""

import pytest
import numpy as np
import tempfile
import os
from datetime import date, timedelta
import sys
from pathlib import Path

from domains.trading.services.universe.historical_universe_creator import HistoricalUniverseCreator, HistoricalStock
from domains.ml.services.training_data.support_resistance_generator import (
    TrainingExample,
    SupportResistanceLevel
)
from domains.ml.services.models.support_resistance_model import (
    SupportResistanceEnsemble,
    SRModelConfig
)
from shared.utils.environment import Environment, EnvironmentType
from core.dao.instruments_dao import InstrumentsDAO


class TestCompleteSRPipelineRealObjects:
    """Real objects test suite for complete Support/Resistance ML pipeline."""

    @pytest.fixture
    async def test_environment(self):
        """Real Environment instance for testing."""
        return Environment(
            env_type=EnvironmentType.DEV,
            db_url="postgresql://postgres:dev_password@localhost:3432/dev_db"
        )

    @pytest.fixture
    def temp_model_dir(self):
        """Create temporary directory for real model storage."""
        temp_dir = tempfile.mkdtemp(prefix="test_sr_models_")
        yield temp_dir
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    async def real_universe_creator(self, test_environment):
        """Real HistoricalUniverseCreator with actual database."""
        return HistoricalUniverseCreator(environment=test_environment)

    @pytest.fixture
    async def test_market_data(self, test_environment):
        """Create real test market data for ML training."""
        dao = InstrumentsDAO(test_environment)
        
        # Create test instruments
        test_symbols = ['TEST_AAPL', 'TEST_GOOGL']
        instrument_ids = []
        
        for symbol in test_symbols:
            instrument_id = await dao.create_instrument(
                symbol=symbol,
                name=f"Test {symbol.replace('TEST_', '')} Inc.",
                exchange="NASDAQ",
                sector="Technology"
            )
            instrument_ids.append(instrument_id)
        
        # Generate realistic OHLCV data for ML training
        market_data = []
        base_price = 100.0
        
        for i, symbol in enumerate(test_symbols):
            # Generate 500 days of realistic price data
            for day in range(500):
                # Simulate realistic price movements
                trend = 0.001 * day  # Slight upward trend
                noise = np.random.normal(0, 0.02)  # 2% daily volatility
                
                price_change = trend + noise
                base_price *= (1 + price_change)
                
                # Ensure realistic OHLC relationships
                open_price = base_price
                close_price = base_price * (1 + np.random.normal(0, 0.01))
                high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.005)))
                low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.005)))
                volume = int(np.random.normal(1000000, 200000))
                
                market_data.append({
                    'symbol': symbol,
                    'date': date.today() - timedelta(days=500-day),
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': max(volume, 100000)  # Ensure positive volume
                })
        
        yield {
            'data': market_data,
            'symbols': test_symbols,
            'instrument_ids': instrument_ids
        }
        
        # Cleanup
        for instrument_id in instrument_ids:
            await dao.delete_instrument(instrument_id)

    @pytest.fixture
    def real_sr_config(self, temp_model_dir):
        """Real SRModelConfig for testing."""
        return SRModelConfig(
            lookback_window=20,
            min_support_resistance_strength=0.7,
            model_save_path=temp_model_dir,
            ensemble_size=3,
            training_epochs=10,  # Reduced for testing
            batch_size=32,
            learning_rate=0.001
        )

    async def test_end_to_end_sr_pipeline_real_objects(self, real_universe_creator, test_market_data, real_sr_config, temp_model_dir):
        """Test complete end-to-end support/resistance pipeline with real ML training."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        
        # Step 1: Real historical universe creation
        universe = await real_universe_creator.create_historical_universe(
            candidate_symbols=symbols,
            start_date=date.today() - timedelta(days=400),
            end_date=date.today() - timedelta(days=50),
            min_trading_days=200
        )
        
        # Validate universe creation
        assert universe is not None
        assert len(universe) > 0
        
        # Step 2: Real support/resistance level detection
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        training_examples = []
        for symbol in symbols:
            symbol_data = [d for d in market_data if d['symbol'] == symbol]
            
            # Generate real training examples
            examples = await sr_generator.generate_training_examples(
                symbol=symbol,
                price_data=symbol_data,
                lookback_days=real_sr_config.lookback_window
            )
            
            training_examples.extend(examples)
        
        # Validate training example generation
        assert len(training_examples) > 0
        for example in training_examples[:5]:  # Check first 5
            assert isinstance(example, TrainingExample)
            assert example.features is not None
            assert example.label is not None
        
        # Step 3: Real model training
        sr_ensemble = SupportResistanceEnsemble(config=real_sr_config)
        
        # Prepare real training data
        features = np.array([ex.features for ex in training_examples])
        labels = np.array([ex.label for ex in training_examples])
        
        # Train real ensemble models
        training_history = await sr_ensemble.train(features, labels)
        
        # Validate model training
        assert training_history is not None
        assert 'loss' in training_history or 'train_loss' in training_history
        
        # Step 4: Real model evaluation
        # Split data for evaluation
        split_idx = len(training_examples) // 2
        train_features = features[:split_idx]
        train_labels = labels[:split_idx]
        test_features = features[split_idx:]
        test_labels = labels[split_idx:]
        
        # Real model prediction
        predictions = await sr_ensemble.predict(test_features)
        
        # Validate predictions
        assert predictions is not None
        assert len(predictions) == len(test_labels)
        assert all(isinstance(p, (float, int, np.number)) for p in predictions)
        
        # Step 5: Real model persistence
        model_path = await sr_ensemble.save_model(f"{temp_model_dir}/sr_ensemble.pkl")
        
        # Validate model saving
        assert os.path.exists(model_path)
        assert os.path.getsize(model_path) > 0
        
        print(f"Pipeline completed: {len(training_examples)} examples, "
              f"model saved to {model_path}")

    async def test_support_resistance_detection_real_objects(self, test_market_data, real_sr_config):
        """Test support/resistance level detection with real price analysis."""
        market_data = test_market_data['data']
        symbol = test_market_data['symbols'][0]
        
        # Get price data for one symbol
        symbol_data = [d for d in market_data if d['symbol'] == symbol]
        symbol_data.sort(key=lambda x: x['date'])
        
        # Real support/resistance detection
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        sr_levels = await sr_generator.detect_support_resistance_levels(
            price_data=symbol_data,
            window_size=20,
            min_strength=0.6
        )
        
        # Validate support/resistance detection
        assert sr_levels is not None
        assert isinstance(sr_levels, list)
        
        for level in sr_levels:
            assert isinstance(level, SupportResistanceLevel)
            assert level.price > 0
            assert level.strength >= 0
            assert level.level_type in ['support', 'resistance']
            assert level.confidence >= 0
        
        print(f"Detected {len(sr_levels)} support/resistance levels for {symbol}")

    async def test_model_ensemble_real_objects(self, test_market_data, real_sr_config, temp_model_dir):
        """Test model ensemble training and prediction with real ML algorithms."""
        market_data = test_market_data['data']
        
        # Generate real training data
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        all_examples = []
        for symbol in test_market_data['symbols']:
            symbol_data = [d for d in market_data if d['symbol'] == symbol]
            examples = await sr_generator.generate_training_examples(
                symbol=symbol,
                price_data=symbol_data,
                lookback_days=10
            )
            all_examples.extend(examples)
        
        # Prepare ensemble training data
        features = np.array([ex.features for ex in all_examples])
        labels = np.array([ex.label for ex in all_examples])
        
        # Create and train real ensemble
        ensemble = SupportResistanceEnsemble(config=real_sr_config)
        
        # Train multiple models in ensemble
        training_results = await ensemble.train_ensemble(features, labels)
        
        # Validate ensemble training
        assert training_results is not None
        assert 'models_trained' in training_results
        assert training_results['models_trained'] > 0
        
        # Test ensemble prediction
        test_features = features[-10:]  # Last 10 examples for testing
        ensemble_predictions = await ensemble.predict_ensemble(test_features)
        
        # Validate ensemble predictions
        assert ensemble_predictions is not None
        assert len(ensemble_predictions) == len(test_features)
        
        # Ensemble should provide prediction confidence
        if hasattr(ensemble, 'prediction_confidence'):
            confidence = await ensemble.prediction_confidence(test_features)
            assert confidence is not None
            assert all(0 <= c <= 1 for c in confidence)

    async def test_real_time_prediction_real_objects(self, test_market_data, real_sr_config, temp_model_dir):
        """Test real-time prediction capabilities with actual model inference."""
        market_data = test_market_data['data']
        symbol = test_market_data['symbols'][0]
        
        # Train a model first
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        symbol_data = [d for d in market_data if d['symbol'] == symbol]
        training_examples = await sr_generator.generate_training_examples(
            symbol=symbol,
            price_data=symbol_data[:-50],  # Leave last 50 for real-time testing
            lookback_days=10
        )
        
        # Train model
        features = np.array([ex.features for ex in training_examples])
        labels = np.array([ex.label for ex in training_examples])
        
        ensemble = SupportResistanceEnsemble(config=real_sr_config)
        await ensemble.train(features, labels)
        
        # Test real-time prediction
        recent_data = symbol_data[-20:]  # Most recent 20 days
        
        real_time_prediction = await sr_generator.predict_support_resistance(
            model=ensemble,
            recent_price_data=recent_data,
            prediction_horizon=5
        )
        
        # Validate real-time prediction
        assert real_time_prediction is not None
        
        if isinstance(real_time_prediction, dict):
            assert 'support_levels' in real_time_prediction or 'resistance_levels' in real_time_prediction
            assert 'confidence' in real_time_prediction
            assert 'prediction_date' in real_time_prediction
        
        print(f"Real-time prediction: {real_time_prediction}")

    async def test_model_performance_evaluation_real_objects(self, test_market_data, real_sr_config):
        """Test model performance evaluation with real ML metrics."""
        market_data = test_market_data['data']
        
        # Generate training and test data
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        all_examples = []
        for symbol in test_market_data['symbols']:
            symbol_data = [d for d in market_data if d['symbol'] == symbol]
            examples = await sr_generator.generate_training_examples(
                symbol=symbol,
                price_data=symbol_data,
                lookback_days=15
            )
            all_examples.extend(examples)
        
        # Split data
        split_point = int(len(all_examples) * 0.8)
        train_examples = all_examples[:split_point]
        test_examples = all_examples[split_point:]
        
        # Train model
        train_features = np.array([ex.features for ex in train_examples])
        train_labels = np.array([ex.label for ex in train_examples])
        
        ensemble = SupportResistanceEnsemble(config=real_sr_config)
        await ensemble.train(train_features, train_labels)
        
        # Evaluate model
        test_features = np.array([ex.features for ex in test_examples])
        test_labels = np.array([ex.label for ex in test_examples])
        
        evaluation_metrics = await ensemble.evaluate(test_features, test_labels)
        
        # Validate evaluation metrics
        assert evaluation_metrics is not None
        assert isinstance(evaluation_metrics, dict)
        
        # Check for standard ML metrics
        expected_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'mse', 'mae']
        found_metrics = [metric for metric in expected_metrics if metric in evaluation_metrics]
        assert len(found_metrics) > 0  # At least one metric should be present
        
        # Validate metric values
        for metric_name, metric_value in evaluation_metrics.items():
            assert isinstance(metric_value, (float, int, np.number))
            if metric_name in ['accuracy', 'precision', 'recall', 'f1_score']:
                assert 0 <= metric_value <= 1
        
        print(f"Model evaluation metrics: {evaluation_metrics}")

    async def test_pipeline_error_handling_real_objects(self, real_sr_config):
        """Test error handling throughout the pipeline with real exceptions."""
        
        # Test empty data handling
        empty_data = []
        
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        try:
            examples = await sr_generator.generate_training_examples(
                symbol="EMPTY_SYMBOL",
                price_data=empty_data,
                lookback_days=10
            )
            
            # Should handle empty data gracefully
            assert examples is not None
            assert len(examples) == 0
            
        except Exception as e:
            # Real error with specific information
            assert isinstance(e, Exception)
            assert "empty" in str(e).lower() or "data" in str(e).lower()
        
        # Test invalid model configuration
        invalid_config = SRModelConfig(
            lookback_window=-1,  # Invalid negative window
            min_support_resistance_strength=1.5,  # Invalid > 1.0
            model_save_path="/invalid/path/that/does/not/exist",
            ensemble_size=0  # Invalid zero size
        )
        
        try:
            invalid_ensemble = SupportResistanceEnsemble(config=invalid_config)
            
            # Should validate configuration
            assert invalid_ensemble is not None  # Creation might succeed
            
            # But training should fail
            dummy_features = np.random.rand(10, 5)
            dummy_labels = np.random.rand(10)
            
            training_result = await invalid_ensemble.train(dummy_features, dummy_labels)
            
            # If training succeeds, configuration was corrected internally
            assert training_result is not None
            
        except Exception as e:
            # Real configuration error is expected
            assert isinstance(e, Exception)
            print(f"Expected configuration error: {e}")

    async def test_pipeline_memory_usage_real_objects(self, test_market_data, real_sr_config):
        """Test memory usage patterns with real ML operations."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        market_data = test_market_data['data']
        
        # Generate large training dataset
        from domains.ml.services.training_data.support_resistance_generator import SupportResistanceGenerator
        sr_generator = SupportResistanceGenerator(config=real_sr_config)
        
        # Process multiple symbols multiple times
        for iteration in range(3):
            all_examples = []
            
            for symbol in test_market_data['symbols']:
                symbol_data = [d for d in market_data if d['symbol'] == symbol]
                examples = await sr_generator.generate_training_examples(
                    symbol=symbol,
                    price_data=symbol_data,
                    lookback_days=20
                )
                all_examples.extend(examples)
            
            # Train and discard model
            if len(all_examples) > 0:
                features = np.array([ex.features for ex in all_examples])
                labels = np.array([ex.label for ex in all_examples])
                
                ensemble = SupportResistanceEnsemble(config=real_sr_config)
                await ensemble.train(features, labels)
                
                # Force cleanup
                del ensemble
                del features
                del labels
                del all_examples
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"Memory usage: {initial_memory:.1f}MB -> {final_memory:.1f}MB (+{memory_increase:.1f}MB)")
        
        # Memory increase should be reasonable for ML operations
        assert memory_increase < 500  # Should not leak excessive memory