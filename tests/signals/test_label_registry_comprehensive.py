#!/usr/bin/env python3
"""
Comprehensive tests for Label Registry.

Tests edge cases, mathematical correctness, and various label types.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from signals.label_registry import (
    LabelRegistry,
    LabelConfig,
    PriceLabelGenerator,
    ReturnLabelGenerator,
    ClassificationLabelGenerator,
    CustomLabelGenerator
)

class TestLabelRegistryEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_data(self):
        """Test with empty DataFrame."""
        registry = LabelRegistry()
        empty_data = pd.DataFrame()
        
        config = LabelConfig(
            name="test_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=1
        )
        registry.add_label(config)
        
        result = registry.generate_labels(empty_data)
        assert result.empty
    
    def test_insufficient_future_data(self):
        """Test with insufficient future data for labels."""
        # Create data with only 5 rows but request 10-period ahead labels
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104]
        }, index=dates)
        
        config = LabelConfig(
            name="future_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=10  # More than available data
        )
        
        registry = LabelRegistry([config])
        result = registry.generate_labels(data)
        
        # Should have the label column but mostly NaN values
        assert 'future_return_simple_lead10' in result.columns
        # Most values should be NaN due to insufficient future data
        assert result['future_return_simple_lead10'].isna().sum() >= 4
    
    def test_lead_periods_at_boundary(self):
        """Test lead periods at data boundary."""
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104]
        })
        
        # Test lead period equal to data length
        config = LabelConfig(
            name="future_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=5
        )
        
        registry = LabelRegistry([config])
        result = registry.generate_labels(data)
        
        # All values should be NaN since we need 5 periods ahead
        assert result['future_return_simple_lead5'].isna().all()

class TestPriceLabelGenerator:
    """Test price-based label generation."""
    
    def test_future_price_calculation(self):
        """Test future price label calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104, 110, 106]
        })
        
        config = LabelConfig(
            name="future_price",
            label_type="price",
            parameters={'price_type': 'future_price', 'column': 'close'},
            lead_periods=2
        )
        
        generator = PriceLabelGenerator()
        result = generator.generate(data, config)
        
        # Manually calculate expected future prices
        expected = data['close'].shift(-2)
        
        # Compare valid values
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_price_change_calculation(self):
        """Test price change label calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]
        })
        
        config = LabelConfig(
            name="price_change",
            label_type="price",
            parameters={'price_type': 'price_change', 'column': 'close'},
            lead_periods=1
        )
        
        generator = PriceLabelGenerator()
        result = generator.generate(data, config)
        
        # Calculate expected price changes
        future_price = data['close'].shift(-1)
        expected = future_price - data['close']
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_price_ratio_calculation(self):
        """Test price ratio label calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]
        })
        
        config = LabelConfig(
            name="price_ratio",
            label_type="price",
            parameters={'price_type': 'price_ratio', 'column': 'close'},
            lead_periods=1
        )
        
        generator = PriceLabelGenerator()
        result = generator.generate(data, config)
        
        # Calculate expected price ratios
        future_price = data['close'].shift(-1)
        expected = future_price / data['close']
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_high_low_range_calculation(self):
        """Test high-low range label calculation."""
        data = pd.DataFrame({
            'high': [105, 110, 107, 113, 109],
            'low': [95, 100, 97, 103, 99],
            'close': [100, 105, 102, 108, 104]
        })
        
        config = LabelConfig(
            name="hl_range",
            label_type="price",
            parameters={'price_type': 'high_low_range'},
            lead_periods=1
        )
        
        generator = PriceLabelGenerator()
        result = generator.generate(data, config)
        
        # Calculate expected high-low ranges
        future_high = data['high'].shift(-1)
        future_low = data['low'].shift(-1)
        expected = future_high - future_low
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )

class TestReturnLabelGenerator:
    """Test return-based label generation."""
    
    def test_simple_return_calculation(self):
        """Test simple return calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]
        })
        
        config = LabelConfig(
            name="simple_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=1
        )
        
        generator = ReturnLabelGenerator()
        result = generator.generate(data, config)
        
        # Calculate expected simple returns
        current_price = data['close']
        future_price = data['close'].shift(-1)
        expected = (future_price - current_price) / current_price
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_log_return_calculation(self):
        """Test log return calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]
        })
        
        config = LabelConfig(
            name="log_return",
            label_type="return",
            parameters={'return_type': 'log', 'column': 'close'},
            lead_periods=1
        )
        
        generator = ReturnLabelGenerator()
        result = generator.generate(data, config)
        
        # Calculate expected log returns
        current_price = data['close']
        future_price = data['close'].shift(-1)
        expected = np.log(future_price / current_price)
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_cumulative_return_calculation(self):
        """Test cumulative return calculation."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104, 110, 106, 112]
        })
        
        config = LabelConfig(
            name="cum_return",
            label_type="return",
            parameters={'return_type': 'cumulative', 'column': 'close'},
            lead_periods=3
        )
        
        generator = ReturnLabelGenerator()
        result = generator.generate(data, config)
        
        # Manually calculate expected cumulative returns
        returns = data['close'].pct_change()
        expected = pd.Series(index=data.index, dtype=float)
        for i in range(len(data) - 3):
            cum_return = (1 + returns.iloc[i+1:i+1+3]).prod() - 1
            expected.iloc[i] = cum_return
        
        # Compare non-NaN values
        valid_mask = ~expected.isna() & ~result.isna()
        np.testing.assert_array_almost_equal(
            result[valid_mask].values,
            expected[valid_mask].values,
            decimal=6
        )
    
    def test_volatility_return_calculation(self):
        """Test volatility return calculation."""
        # Create data with known volatility pattern
        np.random.seed(42)
        prices = [100]
        for _ in range(20):
            ret = np.random.normal(0, 0.02)
            prices.append(prices[-1] * (1 + ret))
        
        data = pd.DataFrame({'close': prices})
        
        config = LabelConfig(
            name="vol_return",
            label_type="return",
            parameters={'return_type': 'volatility', 'column': 'close'},
            lead_periods=5
        )
        
        generator = ReturnLabelGenerator()
        result = generator.generate(data, config)
        
        # Manually calculate expected volatility
        returns = data['close'].pct_change()
        expected = pd.Series(index=data.index, dtype=float)
        for i in range(len(data) - 5):
            vol = returns.iloc[i+1:i+1+5].std()
            expected.iloc[i] = vol
        
        # Check that volatility values are non-negative and reasonable
        valid_values = result.dropna()
        assert (valid_values >= 0).all()
        assert valid_values.max() < 1.0  # Volatility shouldn't be > 100%
    
    def test_max_min_return_calculation(self):
        """Test max and min return calculations."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104, 110, 106]
        })
        
        # Test max return
        max_config = LabelConfig(
            name="max_return",
            label_type="return",
            parameters={'return_type': 'max_return', 'column': 'close'},
            lead_periods=3
        )
        
        min_config = LabelConfig(
            name="min_return", 
            label_type="return",
            parameters={'return_type': 'min_return', 'column': 'close'},
            lead_periods=3
        )
        
        generator = ReturnLabelGenerator()
        max_result = generator.generate(data, max_config)
        min_result = generator.generate(data, min_config)
        
        # Max return should be >= min return for each valid observation
        valid_mask = ~max_result.isna() & ~min_result.isna()
        assert (max_result[valid_mask] >= min_result[valid_mask]).all()

class TestClassificationLabelGenerator:
    """Test classification-based label generation."""
    
    def test_direction_classification(self):
        """Test direction classification."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104]  # up, down, up, down
        })
        
        config = LabelConfig(
            name="direction",
            label_type="classification",
            parameters={'class_type': 'direction', 'column': 'close'},
            lead_periods=1
        )
        
        generator = ClassificationLabelGenerator()
        result = generator.generate(data, config)
        
        # Calculate expected directions
        current_price = data['close']
        future_price = data['close'].shift(-1)
        expected = (future_price > current_price).astype(int)
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_equal(
            result[valid_mask].values,
            expected[valid_mask].values
        )
    
    def test_direction_threshold_classification(self):
        """Test direction classification with threshold."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 100.5]  # 5% up, -2.86% down, 5.88% up, -6.94% down
        })
        
        config = LabelConfig(
            name="direction_threshold",
            label_type="classification",
            parameters={
                'class_type': 'direction_threshold',
                'column': 'close',
                'threshold': 0.03  # 3% threshold
            },
            lead_periods=1
        )
        
        generator = ClassificationLabelGenerator()
        result = generator.generate(data, config)
        
        # Manually calculate expected classifications
        current_price = data['close']
        future_price = data['close'].shift(-1)
        return_pct = (future_price - current_price) / current_price
        
        expected = pd.Series(index=data.index, dtype=int)
        expected[return_pct > 0.03] = 1   # Up
        expected[return_pct < -0.03] = -1  # Down
        expected[abs(return_pct) <= 0.03] = 0  # Neutral
        
        valid_mask = ~expected.isna()
        np.testing.assert_array_equal(
            result[valid_mask].values,
            expected[valid_mask].values
        )
    
    def test_quantile_classification(self):
        """Test quantile-based classification."""
        # Create data with known distribution
        np.random.seed(42)
        returns = np.random.normal(0, 0.02, 120)  # 120 days of returns
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))
        
        data = pd.DataFrame({'close': prices})
        
        config = LabelConfig(
            name="quantile",
            label_type="classification",
            parameters={
                'class_type': 'quantile',
                'column': 'close',
                'n_quantiles': 5,
                'window': 50
            },
            lead_periods=1
        )
        
        generator = ClassificationLabelGenerator()
        result = generator.generate(data, config)
        
        # Check that quantile labels are in expected range
        valid_values = result.dropna()
        assert valid_values.min() >= 0
        assert valid_values.max() <= 4  # 5 quantiles (0-4)
        
        # Check that we have reasonable distribution
        value_counts = valid_values.value_counts()
        assert len(value_counts) > 1  # Should have multiple quantiles represented
    
    def test_volatility_regime_classification(self):
        """Test volatility regime classification."""
        # Create data with changing volatility
        low_vol_data = np.random.normal(0, 0.01, 50)  # Low volatility period
        high_vol_data = np.random.normal(0, 0.05, 50)  # High volatility period
        all_returns = np.concatenate([low_vol_data, high_vol_data])
        
        prices = [100]
        for ret in all_returns:
            prices.append(prices[-1] * (1 + ret))
        
        data = pd.DataFrame({'close': prices})
        
        config = LabelConfig(
            name="vol_regime",
            label_type="classification",
            parameters={
                'class_type': 'volatility_regime',
                'column': 'close',
                'window': 20
            },
            lead_periods=1
        )
        
        generator = ClassificationLabelGenerator()
        result = generator.generate(data, config)
        
        # Check that we get binary classification (0 and 1)
        valid_values = result.dropna()
        unique_values = set(valid_values.unique())
        assert unique_values.issubset({0, 1})

class TestLabelRegistryConfiguration:
    """Test configuration validation and error handling."""
    
    def test_invalid_label_type(self):
        """Test with invalid label type."""
        config = LabelConfig(
            name="invalid",
            label_type="nonexistent_type",
            parameters={},
            lead_periods=1
        )
        
        registry = LabelRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        # Should handle gracefully without crashing
        result = registry.generate_labels(data)
        assert 'invalid' in result.columns
    
    def test_invalid_return_type(self):
        """Test with invalid return type."""
        config = LabelConfig(
            name="invalid_return",
            label_type="return",
            parameters={'return_type': 'nonexistent_return', 'column': 'close'},
            lead_periods=1
        )
        
        registry = LabelRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        # Should handle gracefully
        result = registry.generate_labels(data)
        assert 'invalid_return' in result.columns
    
    def test_missing_column(self):
        """Test with missing required column."""
        config = LabelConfig(
            name="missing_col",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'nonexistent'},
            lead_periods=1
        )
        
        registry = LabelRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        # Should handle gracefully
        result = registry.generate_labels(data)
        assert 'missing_col' in result.columns
        # Should contain NaN values due to missing column
        assert result['missing_col'].isna().all()
    
    def test_zero_lead_periods(self):
        """Test with zero lead periods."""
        config = LabelConfig(
            name="zero_lead",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=0
        )
        
        registry = LabelRegistry([config])
        data = pd.DataFrame({'close': [100, 101, 102]})
        
        result = registry.generate_labels(data)
        
        # Zero lead should give current values (identity transformation)
        assert 'zero_lead' in result.columns

class TestLabelRegistryCustomFunctions:
    """Test custom label functions."""
    
    def test_custom_label_registration(self):
        """Test registering and using custom label functions."""
        def future_max_min_diff(data: pd.DataFrame, lead_periods: int, **kwargs) -> pd.Series:
            """Custom label: difference between future max and min."""
            result = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - lead_periods):
                future_prices = data['close'].iloc[i+1:i+1+lead_periods]
                if len(future_prices) > 0:
                    result.iloc[i] = future_prices.max() - future_prices.min()
            return result
        
        config = LabelConfig(
            name="custom_max_min",
            label_type="custom",
            parameters={'function_name': 'future_max_min_diff'},
            lead_periods=3
        )
        
        registry = LabelRegistry([config])
        registry.register_custom_function('future_max_min_diff', future_max_min_diff)
        
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104, 110, 106]
        })
        
        result = registry.generate_labels(data)
        
        # Verify custom function was called and produced reasonable results
        assert 'custom_max_min' in result.columns
        valid_values = result['custom_max_min'].dropna()
        assert len(valid_values) > 0
        assert (valid_values >= 0).all()  # Max-min difference should be non-negative

class TestLabelRegistryPerformance:
    """Test performance with large datasets."""
    
    def test_large_dataset_performance(self):
        """Test with large dataset."""
        # Create large dataset (2 years of daily data)
        np.random.seed(42)
        n_days = 500
        returns = np.random.normal(0.0005, 0.02, n_days)
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))
        
        data = pd.DataFrame({
            'close': prices[1:],
            'high': [p * 1.02 for p in prices[1:]],
            'low': [p * 0.98 for p in prices[1:]]
        })
        
        # Create multiple labels
        configs = [
            LabelConfig(f"return_{h}d", "return", {'return_type': 'simple', 'column': 'close'}, h)
            for h in [1, 3, 5, 10, 20]
        ] + [
            LabelConfig(f"direction_{h}d", "classification", {'class_type': 'direction', 'column': 'close'}, h)
            for h in [1, 3, 5]
        ]
        
        registry = LabelRegistry(configs)
        
        import time
        start_time = time.time()
        result = registry.generate_labels(data)
        end_time = time.time()
        
        # Should complete in reasonable time
        assert end_time - start_time < 5.0  # 5 seconds max
        assert len(result) == len(data)
        assert len(result.columns) == len(configs)

class TestLabelRegistryIntegration:
    """Test integration scenarios."""
    
    def test_all_label_types_together(self):
        """Test using all label types together."""
        data = pd.DataFrame({
            'close': [100, 105, 102, 108, 104, 110, 106, 112, 109, 115],
            'high': [102, 107, 104, 110, 106, 112, 108, 114, 111, 117],
            'low': [98, 103, 100, 106, 102, 108, 104, 110, 107, 113]
        })
        
        configs = [
            # Price labels
            LabelConfig("future_price", "price", {'price_type': 'future_price', 'column': 'close'}, 1),
            LabelConfig("price_change", "price", {'price_type': 'price_change', 'column': 'close'}, 1),
            
            # Return labels
            LabelConfig("simple_return", "return", {'return_type': 'simple', 'column': 'close'}, 1),
            LabelConfig("log_return", "return", {'return_type': 'log', 'column': 'close'}, 1),
            LabelConfig("max_return", "return", {'return_type': 'max_return', 'column': 'close'}, 3),
            
            # Classification labels
            LabelConfig("direction", "classification", {'class_type': 'direction', 'column': 'close'}, 1),
            LabelConfig("direction_thresh", "classification", {
                'class_type': 'direction_threshold', 'column': 'close', 'threshold': 0.02
            }, 1)
        ]
        
        registry = LabelRegistry(configs)
        result = registry.generate_labels(data)
        
        # Verify all labels are generated
        assert len(result.columns) == len(configs)
        
        # Verify mathematical relationships
        valid_mask = ~result['simple_return_simple_lead1'].isna()
        
        # Simple return and log return should be approximately equal for small changes
        simple_returns = result['simple_return_simple_lead1'][valid_mask]
        log_returns = result['log_return_log_lead1'][valid_mask]
        
        # For small returns, log(1+r) ≈ r
        small_return_mask = abs(simple_returns) < 0.1
        if small_return_mask.any():
            np.testing.assert_array_almost_equal(
                simple_returns[small_return_mask].values,
                log_returns[small_return_mask].values,
                decimal=2
            )

def run_comprehensive_tests():
    """Run all comprehensive tests."""
    import pytest
    
    # Run with verbose output
    return pytest.main([__file__, '-v', '--tb=short'])

if __name__ == "__main__":
    run_comprehensive_tests()