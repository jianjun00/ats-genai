#!/usr/bin/env python3
"""
Comprehensive tests for Feature Registry.

Tests edge cases, error handling, mathematical correctness, and performance.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from signals.feature_registry import (
    FeatureRegistry, 
    FeatureConfig, 
    IndicatorFeatureGenerator,
    TransformFeatureGenerator,
    CustomFeatureGenerator
)

class TestFeatureRegistryEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_data(self):
        """Test with empty DataFrame."""
        registry = FeatureRegistry()
        empty_data = pd.DataFrame()
        
        config = FeatureConfig(
            name="test_sma",
            feature_type="indicator", 
            parameters={'indicator_type': 'sma', 'period': 10}
        )
        registry.add_feature(config)
        
        result = registry.generate_features(empty_data)
        assert result.empty
    
    def test_insufficient_data(self):
        """Test with insufficient data for indicators."""
        # Create data with only 5 rows but request 20-period SMA
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104]
        }, index=dates)
        
        config = FeatureConfig(
            name="test_sma",
            feature_type="indicator",
            parameters={'indicator_type': 'sma', 'period': 20}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Should have the feature column but with NaN values
        assert 'test_sma_sma_20' in result.columns
        assert result['test_sma_sma_20'].isna().all()
    
    def test_missing_required_columns(self):
        """Test with missing required columns."""
        dates = pd.date_range('2023-01-01', periods=10, freq='D')
        data = pd.DataFrame({
            'open': np.random.random(10),
            # Missing 'close' column
        }, index=dates)
        
        config = FeatureConfig(
            name="test_sma",
            feature_type="indicator",
            parameters={'indicator_type': 'sma', 'period': 5}
        )
        
        registry = FeatureRegistry([config])
        
        # Should handle gracefully and return NaN or error placeholder
        result = registry.generate_features(data)
        assert 'test_sma_sma_5' in result.columns
    
    def test_all_nan_data(self):
        """Test with all NaN values."""
        dates = pd.date_range('2023-01-01', periods=20, freq='D')
        data = pd.DataFrame({
            'close': [np.nan] * 20,
            'volume': [np.nan] * 20
        }, index=dates)
        
        configs = [
            FeatureConfig(
                name="returns",
                feature_type="transform",
                parameters={'transform_type': 'pct_change', 'column': 'close'}
            ),
            FeatureConfig(
                name="vol_ratio", 
                feature_type="transform",
                parameters={'transform_type': 'volume_ratio', 'window': 10}
            )
        ]
        
        registry = FeatureRegistry(configs)
        result = registry.generate_features(data)
        
        # All results should be NaN
        assert result.isna().all().all()
    
    def test_single_data_point(self):
        """Test with single data point."""
        data = pd.DataFrame({
            'close': [100.0],
            'volume': [1000000]
        }, index=[datetime.now()])
        
        config = FeatureConfig(
            name="returns",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close'}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        assert len(result) == 1
        assert result.iloc[0, 0] is np.nan or pd.isna(result.iloc[0, 0])
    
    def test_irregular_timestamps(self):
        """Test with irregular timestamp spacing."""
        dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 3),  # Skip day 2
            datetime(2023, 1, 4), 
            datetime(2023, 1, 7),  # Skip weekend
            datetime(2023, 1, 15)  # Large gap
        ]
        
        data = pd.DataFrame({
            'close': [100, 102, 101, 105, 103],
            'volume': [1000000, 1200000, 950000, 1500000, 800000]
        }, index=dates)
        
        config = FeatureConfig(
            name="volatility",
            feature_type="transform",
            parameters={'transform_type': 'volatility', 'column': 'close', 'window': 3}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Should handle irregular spacing gracefully
        assert len(result) == len(data)
        assert not result.isna().all().all()  # Should have some valid values

class TestFeatureRegistryMathematical:
    """Test mathematical correctness of feature calculations."""
    
    def test_sma_calculation(self):
        """Test Simple Moving Average calculation."""
        # Create known data
        data = pd.DataFrame({
            'close': [10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
        })
        
        config = FeatureConfig(
            name="sma_3",
            feature_type="indicator",
            parameters={'indicator_type': 'sma', 'period': 3}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Calculate expected SMA manually
        expected_sma = data['close'].rolling(window=3).mean()
        calculated_sma = result['sma_3_sma_3']
        
        # Compare (allowing for small floating point differences)
        valid_mask = ~expected_sma.isna()
        np.testing.assert_array_almost_equal(
            calculated_sma[valid_mask].values,
            expected_sma[valid_mask].values,
            decimal=6
        )
    
    def test_pct_change_calculation(self):
        """Test percentage change calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]
        })
        
        config = FeatureConfig(
            name="returns",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Calculate expected returns manually
        expected_returns = data['close'].pct_change()
        calculated_returns = result['returns_pct_change']
        
        # Compare valid values
        valid_mask = ~expected_returns.isna()
        np.testing.assert_array_almost_equal(
            calculated_returns[valid_mask].values,
            expected_returns[valid_mask].values,
            decimal=6
        )
    
    def test_log_return_calculation(self):
        """Test log return calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]
        })
        
        config = FeatureConfig(
            name="log_returns",
            feature_type="transform",
            parameters={'transform_type': 'log_return', 'column': 'close'}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Calculate expected log returns manually
        expected_log_returns = np.log(data['close'] / data['close'].shift(1))
        calculated_log_returns = result['log_returns_log_return']
        
        # Compare valid values
        valid_mask = ~expected_log_returns.isna()
        np.testing.assert_array_almost_equal(
            calculated_log_returns[valid_mask].values,
            expected_log_returns[valid_mask].values,
            decimal=6
        )
    
    def test_volatility_calculation(self):
        """Test volatility calculation."""
        # Create data with known volatility
        np.random.seed(42)
        returns = np.random.normal(0, 0.02, 50)  # 2% daily volatility
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))
        
        data = pd.DataFrame({'close': prices[1:]})  # Skip initial price
        
        config = FeatureConfig(
            name="vol_10",
            feature_type="transform",
            parameters={'transform_type': 'volatility', 'column': 'close', 'window': 10}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Calculate expected volatility manually
        price_returns = data['close'].pct_change()
        expected_vol = price_returns.rolling(window=10).std()
        calculated_vol = result['vol_10_volatility']
        
        # Compare valid values
        valid_mask = ~expected_vol.isna()
        np.testing.assert_array_almost_equal(
            calculated_vol[valid_mask].values,
            expected_vol[valid_mask].values,
            decimal=6
        )
    
    def test_volume_ratio_calculation(self):
        """Test volume ratio calculation."""
        volumes = [1000000, 1200000, 900000, 1500000, 800000, 1100000, 1300000, 950000]
        data = pd.DataFrame({'volume': volumes})
        
        config = FeatureConfig(
            name="vol_ratio",
            feature_type="transform",
            parameters={'transform_type': 'volume_ratio', 'window': 5}
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Calculate expected volume ratio manually
        vol_ma = data['volume'].rolling(window=5).mean()
        expected_ratio = data['volume'] / vol_ma
        calculated_ratio = result['vol_ratio_volume_ratio']
        
        # Compare valid values
        valid_mask = ~expected_ratio.isna()
        np.testing.assert_array_almost_equal(
            calculated_ratio[valid_mask].values,
            expected_ratio[valid_mask].values,
            decimal=6
        )

class TestFeatureRegistryLag:
    """Test lag functionality."""
    
    def test_lag_periods_basic(self):
        """Test basic lag functionality."""
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
        })
        
        config = FeatureConfig(
            name="returns_lag2",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1},
            lag_periods=2
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # Calculate expected: returns then lag by 2
        returns = data['close'].pct_change()
        expected_lagged = returns.shift(2)
        calculated_lagged = result['returns_lag2_pct_change_lag2']
        
        # Compare valid values
        valid_mask = ~expected_lagged.isna()
        np.testing.assert_array_almost_equal(
            calculated_lagged[valid_mask].values,
            expected_lagged[valid_mask].values,
            decimal=6
        )
    
    def test_lag_periods_edge_cases(self):
        """Test lag with edge cases."""
        data = pd.DataFrame({
            'close': [100, 101, 102]  # Only 3 data points
        })
        
        config = FeatureConfig(
            name="returns_lag5",
            feature_type="transform", 
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1},
            lag_periods=5  # Lag longer than data
        )
        
        registry = FeatureRegistry([config])
        result = registry.generate_features(data)
        
        # All values should be NaN due to excessive lag
        assert result['returns_lag5_pct_change_lag5'].isna().all()

class TestFeatureRegistryConfiguration:
    """Test configuration validation and error handling."""
    
    def test_invalid_feature_type(self):
        """Test with invalid feature type."""
        config = FeatureConfig(
            name="invalid",
            feature_type="nonexistent_type",
            parameters={}
        )
        
        registry = FeatureRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        # Should handle gracefully without crashing
        result = registry.generate_features(data)
        assert 'invalid' in result.columns  # Should create placeholder
    
    def test_invalid_indicator_type(self):
        """Test with invalid indicator type."""
        config = FeatureConfig(
            name="invalid_indicator",
            feature_type="indicator",
            parameters={'indicator_type': 'nonexistent_indicator', 'period': 10}
        )
        
        registry = FeatureRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102, 103, 104]})
        
        # Should handle gracefully
        result = registry.generate_features(data)
        assert 'invalid_indicator' in result.columns
    
    def test_missing_parameters(self):
        """Test with missing required parameters."""
        config = FeatureConfig(
            name="sma_no_period",
            feature_type="indicator",
            parameters={'indicator_type': 'sma'}  # Missing 'period'
        )
        
        registry = FeatureRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102, 103, 104]})
        
        # Should handle gracefully (might use default or error gracefully)
        result = registry.generate_features(data)
        assert 'sma_no_period' in result.columns
    
    def test_invalid_transform_type(self):
        """Test with invalid transform type."""
        config = FeatureConfig(
            name="invalid_transform",
            feature_type="transform",
            parameters={'transform_type': 'nonexistent_transform', 'column': 'close'}
        )
        
        registry = FeatureRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        # Should handle gracefully
        result = registry.generate_features(data)
        assert 'invalid_transform' in result.columns

class TestFeatureRegistryCustomFunctions:
    """Test custom function registration and usage."""
    
    def test_custom_function_registration(self):
        """Test registering and using custom functions."""
        def simple_average(data: pd.DataFrame, window: int = 5, **kwargs) -> pd.Series:
            return data['close'].rolling(window=window).mean()
        
        config = FeatureConfig(
            name="custom_avg",
            feature_type="custom",
            parameters={'function_name': 'simple_average', 'window': 3}
        )
        
        registry = FeatureRegistry([config])
        registry.register_custom_function('simple_average', simple_average)
        
        data = pd.DataFrame({
            'close': [10, 12, 14, 16, 18, 20]
        })
        
        result = registry.generate_features(data)
        
        # Verify custom function was called correctly
        expected = data['close'].rolling(window=3).mean()
        calculated = result['custom_avg']
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            calculated[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_custom_function_with_error(self):
        """Test custom function that raises an error."""
        def broken_function(data: pd.DataFrame, **kwargs) -> pd.Series:
            raise ValueError("Intentional error for testing")
        
        config = FeatureConfig(
            name="broken_custom",
            feature_type="custom",
            parameters={'function_name': 'broken_function'}
        )
        
        registry = FeatureRegistry([config])
        registry.register_custom_function('broken_function', broken_function)
        
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        # Should handle error gracefully
        result = registry.generate_features(data)
        assert 'broken_custom' in result.columns
        # Should contain NaN or error placeholder values
        assert result['broken_custom'].isna().all()

class TestFeatureRegistryPerformance:
    """Test performance and scalability."""
    
    def test_large_dataset_performance(self):
        """Test with large dataset."""
        # Create large dataset (1 year of daily data)
        dates = pd.date_range('2020-01-01', '2020-12-31', freq='D')
        n_points = len(dates)
        
        np.random.seed(42)
        data = pd.DataFrame({
            'close': 100 + np.cumsum(np.random.normal(0, 1, n_points)),
            'volume': np.random.uniform(1000000, 5000000, n_points),
            'high': 100 + np.cumsum(np.random.normal(0.5, 1, n_points)),
            'low': 100 + np.cumsum(np.random.normal(-0.5, 1, n_points))
        }, index=dates)
        
        # Create multiple features
        configs = [
            FeatureConfig(
                name=f"sma_{period}",
                feature_type="indicator",
                parameters={'indicator_type': 'sma', 'period': period}
            ) for period in [5, 10, 20, 50]
        ] + [
            FeatureConfig(
                name=f"returns_{lag}",
                feature_type="transform",
                parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1},
                lag_periods=lag
            ) for lag in [1, 2, 3, 5]
        ]
        
        registry = FeatureRegistry(configs)
        
        import time
        start_time = time.time()
        result = registry.generate_features(data)
        end_time = time.time()
        
        # Should complete in reasonable time (adjust threshold as needed)
        assert end_time - start_time < 10.0  # 10 seconds max
        assert len(result) == len(data)
        assert len(result.columns) == len(configs)
    
    def test_many_features_performance(self):
        """Test with many features."""
        data = pd.DataFrame({
            'close': 100 + np.cumsum(np.random.normal(0, 1, 100)),
            'volume': np.random.uniform(1000000, 5000000, 100)
        })
        
        # Create many features (100 features)
        configs = []
        for i in range(25):
            configs.extend([
                FeatureConfig(
                    name=f"sma_{i}_{period}",
                    feature_type="indicator",
                    parameters={'indicator_type': 'sma', 'period': period}
                ) for period in [5, 10, 15, 20]
            ])
        
        registry = FeatureRegistry(configs)
        
        import time
        start_time = time.time()
        result = registry.generate_features(data)
        end_time = time.time()
        
        # Should handle many features efficiently
        assert end_time - start_time < 30.0  # 30 seconds max for 100 features
        assert len(result.columns) == 100

class TestFeatureRegistryIntegration:
    """Test integration scenarios."""
    
    def test_realistic_market_data_scenario(self):
        """Test with realistic market data patterns."""
        # Create realistic market data with trends, volatility clustering, gaps
        np.random.seed(42)
        n_days = 252  # 1 trading year
        
        # Create price series with realistic patterns
        returns = []
        volatility = 0.02  # Base volatility
        
        for i in range(n_days):
            # Volatility clustering
            if i > 0:
                volatility = 0.8 * volatility + 0.2 * abs(returns[-1]) + 0.001 * np.random.normal()
                volatility = max(0.005, min(0.1, volatility))  # Bound volatility
            
            # Fat-tailed returns
            if np.random.random() < 0.05:  # 5% chance of extreme move
                ret = np.random.normal(0, volatility * 3)
            else:
                ret = np.random.normal(0.0002, volatility)  # Small positive drift
            
            returns.append(ret)
        
        # Build price series
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))
        
        # Add missing data (market holidays)
        business_days = pd.bdate_range('2020-01-01', periods=n_days)
        data = pd.DataFrame({
            'close': prices[1:],
            'volume': np.random.lognormal(14, 0.5, n_days),  # Log-normal volume
            'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices[1:]],
            'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices[1:]]
        }, index=business_days)
        
        # Add some random missing days
        missing_indices = np.random.choice(data.index, size=10, replace=False)
        data = data.drop(missing_indices)
        
        # Create comprehensive feature set
        configs = [
            # Trend features
            FeatureConfig("sma_20", "indicator", {'indicator_type': 'sma', 'period': 20}),
            FeatureConfig("sma_50", "indicator", {'indicator_type': 'sma', 'period': 50}),
            
            # Momentum features  
            FeatureConfig("rsi", "indicator", {'indicator_type': 'rsi', 'period': 14}),
            FeatureConfig("returns_1d", "transform", {'transform_type': 'pct_change', 'column': 'close', 'periods': 1}, lag_periods=1),
            FeatureConfig("returns_5d", "transform", {'transform_type': 'pct_change', 'column': 'close', 'periods': 5}, lag_periods=1),
            
            # Volatility features
            FeatureConfig("vol_20", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 20}),
            FeatureConfig("vol_5", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 5}),
            
            # Volume features
            FeatureConfig("vol_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 20})
        ]
        
        registry = FeatureRegistry(configs)
        result = registry.generate_features(data)
        
        # Verify results make sense
        assert len(result) == len(data)
        assert len(result.columns) == len(configs)
        
        # Check that features have reasonable ranges
        assert result['returns_1d_pct_change_lag1'].abs().max() < 0.5  # No 50%+ daily moves
        assert result['vol_20_volatility'].min() >= 0  # Volatility non-negative
        assert result['vol_ratio_volume_ratio'].min() >= 0  # Volume ratio non-negative
        
        # Check that we have reasonable amount of valid data
        valid_ratio = (~result.isna()).sum().sum() / (len(result) * len(result.columns))
        assert valid_ratio > 0.7  # At least 70% valid data
    
    def test_multiple_symbols_consistency(self):
        """Test that features are calculated consistently across symbols."""
        # Create identical data for two symbols
        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        base_data = {
            'close': 100 + np.cumsum(np.random.normal(0, 1, 50)),
            'volume': np.random.uniform(1000000, 5000000, 50)
        }
        
        symbol_a_data = pd.DataFrame(base_data, index=dates)
        symbol_b_data = pd.DataFrame(base_data, index=dates)  # Identical data
        
        config = FeatureConfig(
            name="returns",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1}
        )
        
        registry = FeatureRegistry([config])
        
        result_a = registry.generate_features(symbol_a_data)
        result_b = registry.generate_features(symbol_b_data)
        
        # Results should be identical for identical input data
        valid_mask = ~result_a['returns_pct_change'].isna()
        np.testing.assert_array_almost_equal(
            result_a['returns_pct_change'][valid_mask].values,
            result_b['returns_pct_change'][valid_mask].values,
            decimal=10
        )

def run_comprehensive_tests():
    """Run all comprehensive tests."""
    import pytest
    
    # Run with verbose output
    return pytest.main([__file__, '-v', '--tb=short'])

if __name__ == "__main__":
    run_comprehensive_tests()