#!/usr/bin/env python3
"""
Comprehensive tests for Configurable Training Data Generator.

Tests edge cases, data quality scenarios, performance, and integration.
"""

import pytest
import pandas as pd
import numpy as np
import torch
import sys
import os
import tempfile
import pickle
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.ml.services.configurable_train_data_generator import (
    ConfigurableTrainingDataGenerator,
    ConfigurableTrainingDataConfig,
    ConfigurableTrainDataCallback
)
from domains.trading.services.feature_registry import FeatureRegistry, FeatureConfig
from domains.trading.services.label_registry import LabelRegistry, LabelConfig

class TestConfigurableGeneratorEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_data(self):
        """Test with completely empty data."""
        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=10,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)
        empty_data = pd.DataFrame()

        with pytest.raises(ValueError, match="must have a date index"):
            generator.generate_training_data(empty_data)

    def test_insufficient_data_for_sequences(self):
        """Test with insufficient data to create sequences."""
        # Create very small dataset
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 5,
            'close': [100, 101, 102, 103, 104],
            'volume': [1000000] * 5
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=20,  # More than available data
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)

        with pytest.raises(ValueError, match="No training sequences generated"):
            generator.generate_training_data(data, symbols=['AAPL'])

    def test_missing_required_columns(self):
        """Test with missing required OHLCV columns."""
        dates = pd.date_range('2023-01-01', periods=30, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 30,
            'close': np.random.uniform(100, 110, 30),
            # Missing 'open', 'high', 'low' columns
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)

        with pytest.raises(ValueError, match="Missing required columns"):
            generator.generate_training_data(data, symbols=['AAPL'])

    def test_all_nan_values(self):
        """Test with all NaN values in price data."""
        dates = pd.date_range('2023-01-01', periods=30, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 30,
            'open': [np.nan] * 30,
            'high': [np.nan] * 30,
            'low': [np.nan] * 30,
            'close': [np.nan] * 30,
            'volume': [np.nan] * 30
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.1  # Very low threshold
        )

        generator = ConfigurableTrainingDataGenerator(config)

        # Should fail due to insufficient valid data
        with pytest.raises(ValueError, match="No training sequences generated"):
            generator.generate_training_data(data, symbols=['AAPL'])

    def test_single_symbol_single_sequence(self):
        """Test minimal viable dataset (single symbol, minimal sequences)."""
        dates = pd.date_range('2023-01-01', periods=10, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 10,
            'open': np.random.uniform(100, 110, 10),
            'high': np.random.uniform(110, 120, 10),
            'low': np.random.uniform(90, 100, 10),
            'close': np.random.uniform(95, 115, 10),
            'volume': np.random.uniform(1000000, 5000000, 10)
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=3,
            prediction_horizon=2,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.5
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should produce at least one sequence
        assert result['features'].shape[0] >= 1
        assert result['features'].shape[1] == 3  # sequence_length
        assert result['labels'].shape[1] == 2   # prediction_horizon

class TestConfigurableGeneratorDataQuality:
    """Test data quality handling and validation."""

    def test_outlier_detection_and_removal(self):
        """Test outlier detection and removal."""
        # Create data with clear outliers
        np.random.seed(42)
        normal_data = np.random.normal(100, 2, 48)  # Normal price data
        outlier_indices = [10, 25, 40]

        data_with_outliers = normal_data.copy()
        data_with_outliers[outlier_indices] = [200, 50, 180]  # Clear outliers

        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 50,
            'open': data_with_outliers,
            'high': data_with_outliers * 1.02,
            'low': data_with_outliers * 0.98,
            'close': data_with_outliers,
            'volume': np.random.uniform(1000000, 5000000, 50)
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        # Test with outlier removal enabled
        config_with_removal = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            remove_outliers=True,
            outlier_threshold=2.0
        )

        # Test without outlier removal
        config_without_removal = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            remove_outliers=False
        )

        generator_with = ConfigurableTrainingDataGenerator(config_with_removal)
        generator_without = ConfigurableTrainingDataGenerator(config_without_removal)

        result_with = generator_with.generate_training_data(data, symbols=['AAPL'])
        result_without = generator_without.generate_training_data(data, symbols=['AAPL'])

        # Results with outlier removal should have less extreme values
        features_with = result_with['features'].numpy() if hasattr(result_with['features'], 'numpy') else result_with['features']
        features_without = result_without['features'].numpy() if hasattr(result_without['features'], 'numpy') else result_without['features']

        # With outlier removal, feature variance should be lower
        var_with = np.nanvar(features_with)
        var_without = np.nanvar(features_without)
        assert var_with <= var_without

    def test_missing_value_handling(self):
        """Test missing value filling strategies."""
        # Create data with systematic missing values
        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        close_prices = np.random.uniform(100, 110, 50)

        # Insert missing values at specific indices
        missing_indices = [5, 15, 25, 35, 45]
        close_prices[missing_indices] = np.nan

        data = pd.DataFrame({
            'symbol': ['AAPL'] * 50,
            'open': close_prices * 0.995,
            'high': close_prices * 1.02,
            'low': close_prices * 0.98,
            'close': close_prices,
            'volume': np.random.uniform(1000000, 5000000, 50)
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.7
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle missing values without crashing
        assert result['features'].shape[0] > 0

        # Check that the final data has fewer NaN values than original
        features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']
        nan_ratio = np.isnan(features_array).sum() / features_array.size
        assert nan_ratio < 0.3  # Should have filled most missing values

    def test_scaling_edge_cases(self):
        """Test scaling with edge cases (constant values, zero variance)."""
        # Create data with constant values (zero variance)
        dates = pd.date_range('2023-01-01', periods=30, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 30,
            'open': [100.0] * 30,  # Constant values
            'high': [100.5] * 30,
            'low': [99.5] * 30,
            'close': [100.0] * 30,  # Constant values
            'volume': [1000000] * 30  # Constant values
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        # Test different scaling methods
        for scaling_method in ['standard', 'robust', 'minmax']:
            config = ConfigurableTrainingDataConfig(
                sequence_length=5,
                prediction_horizon=3,
                feature_registry=feature_registry,
                label_registry=label_registry,
                normalize_features=True,
                feature_scaling_method=scaling_method
            )

            generator = ConfigurableTrainingDataGenerator(config)

            # Should handle constant values gracefully (no division by zero)
            try:
                result = generator.generate_training_data(data, symbols=['AAPL'])
                features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']

                # Scaled features should not contain infinite values
                assert not np.isinf(features_array).any()

            except Exception as e:
                pytest.fail(f"Scaling method {scaling_method} failed with constant data: {e}")

    def test_min_valid_ratio_filtering(self):
        """Test minimum valid ratio filtering."""
        # Create data with varying amounts of missing values per sequence
        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        close_prices = np.random.uniform(100, 110, 50)

        # Create systematic missing patterns
        for i in range(10, 20):  # High missing value period
            if i % 2 == 0:
                close_prices[i] = np.nan

        data = pd.DataFrame({
            'symbol': ['AAPL'] * 50,
            'open': close_prices * 0.995,
            'high': close_prices * 1.02,
            'low': close_prices * 0.98,
            'close': close_prices,
            'volume': np.random.uniform(1000000, 5000000, 50)
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        # Test with high valid ratio requirement
        config_strict = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.9  # Very strict
        )

        # Test with low valid ratio requirement
        config_lenient = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.3  # Very lenient
        )

        generator_strict = ConfigurableTrainingDataGenerator(config_strict)
        generator_lenient = ConfigurableTrainingDataGenerator(config_lenient)

        result_strict = generator_strict.generate_training_data(data, symbols=['AAPL'])
        result_lenient = generator_lenient.generate_training_data(data, symbols=['AAPL'])

        # Lenient config should produce more sequences than strict config
        assert result_lenient['features'].shape[0] >= result_strict['features'].shape[0]

class TestConfigurableGeneratorPerformance:
    """Test performance and memory usage."""

    def test_large_dataset_performance(self):
        """Test performance with large dataset."""
        # Create large dataset (3 years of daily data, multiple symbols)
        np.random.seed(42)
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        dates = pd.date_range('2020-01-01', '2022-12-31', freq='D')

        all_data = []
        for symbol in symbols:
            n_days = len(dates)
            returns = np.random.normal(0.0005, 0.02, n_days)
            prices = [100]
            for ret in returns:
                prices.append(prices[-1] * (1 + ret))

            symbol_data = pd.DataFrame({
                'symbol': [symbol] * n_days,
                'open': [p * np.random.uniform(0.99, 1.01) for p in prices[1:]],
                'high': [p * np.random.uniform(1.01, 1.03) for p in prices[1:]],
                'low': [p * np.random.uniform(0.97, 0.99) for p in prices[1:]],
                'close': prices[1:],
                'volume': np.random.uniform(1000000, 10000000, n_days)
            }, index=dates)
            all_data.append(symbol_data)

        data = pd.concat(all_data)

        # Create moderate number of features and labels
        feature_registry = FeatureRegistry([
            FeatureConfig("returns_1d", "transform", {'transform_type': 'pct_change', 'column': 'close', 'periods': 1}),
            FeatureConfig("volatility", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 20}),
            FeatureConfig("volume_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 20})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return_1d", "return", {'return_type': 'simple', 'column': 'close'}, 1),
            LabelConfig("future_return_5d", "return", {'return_type': 'simple', 'column': 'close'}, 5),
            LabelConfig("direction", "classification", {'class_type': 'direction', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=30,
            prediction_horizon=10,
            feature_registry=feature_registry,
            label_registry=label_registry,
            window_stride=5  # Skip some windows for performance
        )

        generator = ConfigurableTrainingDataGenerator(config)

        import time
        import psutil
        import os

        # Monitor memory usage
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB

        start_time = time.time()
        result = generator.generate_training_data(data, symbols=symbols)
        end_time = time.time()

        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = memory_after - memory_before

        print(f"Performance test results:")
        print(f"  Processing time: {end_time - start_time:.2f} seconds")
        print(f"  Memory used: {memory_used:.2f} MB")
        print(f"  Data shape: {data.shape}")
        print(f"  Output sequences: {result['features'].shape[0]}")

        # Performance assertions (adjust thresholds as needed)
        assert end_time - start_time < 60.0  # Should complete in 60 seconds
        assert memory_used < 2000  # Should use less than 2GB additional memory
        assert result['features'].shape[0] > 100  # Should generate substantial sequences

    def test_memory_efficiency_with_streaming(self):
        """Test memory efficiency by processing symbols sequentially."""
        # Create moderate dataset per symbol
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        dates = pd.date_range('2023-01-01', periods=200, freq='D')

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=20,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)

        import psutil
        import os

        process = psutil.Process(os.getpid())
        memory_usages = []

        for symbol in symbols:
            # Create data for single symbol
            returns = np.random.normal(0.001, 0.02, len(dates))
            prices = [100]
            for ret in returns:
                prices.append(prices[-1] * (1 + ret))

            symbol_data = pd.DataFrame({
                'symbol': [symbol] * len(dates),
                'open': prices[1:],
                'high': [p * 1.02 for p in prices[1:]],
                'low': [p * 0.98 for p in prices[1:]],
                'close': prices[1:],
                'volume': np.random.uniform(1000000, 5000000, len(dates))
            }, index=dates)

            memory_before = process.memory_info().rss / 1024 / 1024
            result = generator.generate_training_data(symbol_data, symbols=[symbol])
            memory_after = process.memory_info().rss / 1024 / 1024

            memory_usages.append(memory_after - memory_before)

        # Memory usage per symbol should be relatively consistent
        memory_variance = np.var(memory_usages)
        print(f"Memory usage per symbol: {memory_usages}")
        print(f"Memory variance: {memory_variance}")

        # Memory usage should not grow significantly between symbols
        assert memory_variance < 100  # Low variance in memory usage

class TestConfigurableGeneratorOutputFormats:
    """Test different output formats."""

    def create_test_data(self):
        """Helper to create test data."""
        dates = pd.date_range('2023-01-01', periods=30, freq='D')
        return pd.DataFrame({
            'symbol': ['AAPL'] * 30,
            'open': np.random.uniform(100, 110, 30),
            'high': np.random.uniform(110, 120, 30),
            'low': np.random.uniform(90, 100, 30),
            'close': np.random.uniform(95, 115, 30),
            'volume': np.random.uniform(1000000, 5000000, 30)
        }, index=dates)

    def create_test_config(self, output_format='pytorch'):
        """Helper to create test configuration."""
        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        return ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            output_format=output_format
        )

    def test_pytorch_output_format(self):
        """Test PyTorch tensor output format."""
        data = self.create_test_data()
        config = self.create_test_config('pytorch')

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Check output types
        assert isinstance(result['features'], torch.Tensor)
        assert isinstance(result['labels'], torch.Tensor)
        assert isinstance(result['feature_masks'], torch.Tensor)
        assert isinstance(result['label_masks'], torch.Tensor)

        # Check tensor properties
        assert result['features'].dtype == torch.float32
        assert result['labels'].dtype == torch.float32

    def test_numpy_output_format(self):
        """Test NumPy array output format."""
        data = self.create_test_data()
        config = self.create_test_config('numpy')

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Check output types
        assert isinstance(result['features'], np.ndarray)
        assert isinstance(result['labels'], np.ndarray)
        assert isinstance(result['feature_masks'], np.ndarray)
        assert isinstance(result['label_masks'], np.ndarray)

        # Check array properties
        assert result['features'].dtype == np.float32
        assert result['labels'].dtype == np.float32

    def test_pandas_output_format(self):
        """Test Pandas DataFrame output format."""
        data = self.create_test_data()
        config = self.create_test_config('pandas')

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Check output types
        assert isinstance(result['features'], pd.DataFrame)
        assert isinstance(result['labels'], pd.DataFrame)

        # Check DataFrame properties
        assert len(result['features'].columns) == len(result['feature_names'])
        assert len(result['labels'].columns) == len(result['label_names'])

class TestConfigurableGeneratorReproducibility:
    """Test reproducibility and deterministic behavior."""

    def test_deterministic_output(self):
        """Test that identical inputs produce identical outputs."""
        # Create identical datasets
        np.random.seed(42)
        dates = pd.date_range('2023-01-01', periods=50, freq='D')

        def create_data():
            return pd.DataFrame({
                'symbol': ['AAPL'] * 50,
                'open': np.random.uniform(100, 110, 50),
                'high': np.random.uniform(110, 120, 50),
                'low': np.random.uniform(90, 100, 50),
                'close': np.random.uniform(95, 115, 50),
                'volume': np.random.uniform(1000000, 5000000, 50)
            }, index=dates)

        # Reset seed and create first dataset
        np.random.seed(42)
        data1 = create_data()

        # Reset seed and create second dataset (should be identical)
        np.random.seed(42)
        data2 = create_data()

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])
        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=10,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry,
            output_format='numpy'
        )

        generator1 = ConfigurableTrainingDataGenerator(config)
        generator2 = ConfigurableTrainingDataGenerator(config)

        result1 = generator1.generate_training_data(data1, symbols=['AAPL'])
        result2 = generator2.generate_training_data(data2, symbols=['AAPL'])

        # Results should be identical
        np.testing.assert_array_equal(result1['features'], result2['features'])
        np.testing.assert_array_equal(result1['labels'], result2['labels'])

class TestConfigurableGeneratorRealWorldScenarios:
    """Test realistic market data scenarios."""

    def test_market_crash_scenario(self):
        """Test handling of market crash scenario (extreme volatility)."""
        # Create data simulating market crash
        dates = pd.date_range('2023-01-01', periods=100, freq='D')

        # Normal period
        normal_returns = np.random.normal(0.001, 0.015, 50)

        # Crash period (higher volatility, negative trend)
        crash_returns = np.random.normal(-0.02, 0.08, 20)  # Severe drops

        # Recovery period
        recovery_returns = np.random.normal(0.005, 0.03, 30)

        all_returns = np.concatenate([normal_returns, crash_returns, recovery_returns])

        prices = [100]
        for ret in all_returns:
            prices.append(max(prices[-1] * (1 + ret), 1.0))  # Prevent negative prices

        data = pd.DataFrame({
            'symbol': ['AAPL'] * 100,
            'open': [p * np.random.uniform(0.99, 1.01) for p in prices[1:]],
            'high': [p * np.random.uniform(1.0, 1.05) for p in prices[1:]],
            'low': [p * np.random.uniform(0.95, 1.0) for p in prices[1:]],
            'close': prices[1:],
            'volume': [v * np.random.uniform(0.5, 3.0) for v in np.random.uniform(1000000, 5000000, 100)]  # Volume spikes during crash
        }, index=dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volatility", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 10}),
            FeatureConfig("volume_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 10})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1),
            LabelConfig("direction", "classification", {'class_type': 'direction', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=10,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry,
            remove_outliers=True,  # Should handle extreme values
            outlier_threshold=3.0
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle extreme market conditions without crashing
        assert result['features'].shape[0] > 0

        # Check that volatility feature captured the crash period
        features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']
        volatility_feature = features_array[:, :, 1]  # Assuming volatility is second feature

        # Should have periods of high volatility
        assert np.nanmax(volatility_feature) > np.nanmean(volatility_feature) * 2

    def test_missing_trading_days_scenario(self):
        """Test handling of missing trading days (weekends, holidays)."""
        # Create business days with some holidays removed
        all_dates = pd.bdate_range('2023-01-01', '2023-12-31')

        # Remove some random "holidays"
        np.random.seed(42)
        holiday_indices = np.random.choice(len(all_dates), size=20, replace=False)
        trading_dates = all_dates.delete(holiday_indices)

        returns = np.random.normal(0.001, 0.02, len(trading_dates))
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))

        data = pd.DataFrame({
            'symbol': ['AAPL'] * len(trading_dates),
            'open': prices[1:],
            'high': [p * 1.02 for p in prices[1:]],
            'low': [p * 0.98 for p in prices[1:]],
            'close': prices[1:],
            'volume': np.random.uniform(1000000, 5000000, len(trading_dates))
        }, index=trading_dates)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volatility", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 20})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=15,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle irregular trading calendar
        assert result['features'].shape[0] > 0

        # Features should be calculated correctly despite missing days
        features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']
        assert not np.isinf(features_array).any()

    def test_different_data_availability_across_symbols(self):
        """Test handling symbols with different data availability."""
        # Create data with different start dates for different symbols
        base_date = pd.date_range('2023-01-01', periods=100, freq='D')

        symbols_data = []

        # AAPL: Full data
        aapl_data = self._create_symbol_data('AAPL', base_date, 0)
        symbols_data.append(aapl_data)

        # MSFT: Missing first 20 days
        msft_dates = base_date[20:]
        msft_data = self._create_symbol_data('MSFT', msft_dates, 20)
        symbols_data.append(msft_data)

        # GOOGL: Missing random days
        googl_dates = base_date.delete(np.random.choice(100, 15, replace=False))
        googl_data = self._create_symbol_data('GOOGL', googl_dates, 0)
        symbols_data.append(googl_data)

        data = pd.concat(symbols_data)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=10,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL', 'MSFT', 'GOOGL'])

        # Should handle different data availability gracefully
        assert result['features'].shape[0] > 0

        # AAPL should contribute more sequences than MSFT or GOOGL
        # (This is hard to test directly, but the generator should complete successfully)

    def _create_symbol_data(self, symbol, dates, start_idx):
        """Helper to create data for a specific symbol."""
        np.random.seed(hash(symbol) % 2**32)
        returns = np.random.normal(0.001, 0.02, len(dates))
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))

        return pd.DataFrame({
            'symbol': [symbol] * len(dates),
            'open': prices[1:],
            'high': [p * 1.02 for p in prices[1:]],
            'low': [p * 0.98 for p in prices[1:]],
            'close': prices[1:],
            'volume': np.random.uniform(1000000, 5000000, len(dates))
        }, index=dates)

def run_comprehensive_tests():
    """Run all comprehensive tests."""
    import pytest

    # Run with verbose output and capture stdout
    return pytest.main([__file__, '-v', '--tb=short', '-s'])

if __name__ == "__main__":
    run_comprehensive_tests()