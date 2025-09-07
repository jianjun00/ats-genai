"""
Unit tests for BX Trender indicator configuration and parameter validation.
"""
import unittest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from types import SimpleNamespace

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from signals.enhanced_indicators import BXTrenderIndicator
from signals.indicator import BXTrenderBasic, BXTrenderDirectional, BXTrenderVolumeWeighted


class TestBXTrenderConfiguration(unittest.TestCase):
    """Test cases for BX Trender indicator configuration."""

    def test_period_validation(self):
        """Test period parameter validation."""
        # Valid periods
        valid_periods = [7, 14, 21, 30, 50, 100]
        for period in valid_periods:
            indicator = BXTrenderIndicator(period=period, variant='basic')
            self.assertEqual(indicator.period, period)

        # Invalid periods
        invalid_periods = [0, -1, 1, 2, 3, 4, 5, None, 'invalid', 1000]
        for period in invalid_periods:
            with self.assertRaises((ValueError, TypeError)):
                BXTrenderIndicator(period=period, variant='basic')

    def test_variant_validation(self):
        """Test variant parameter validation."""
        # Valid variants
        valid_variants = ['basic', 'directional', 'volume_weighted']
        for variant in valid_variants:
            indicator = BXTrenderIndicator(period=14, variant=variant)
            self.assertEqual(indicator.variant, variant)

        # Invalid variants
        invalid_variants = [None, '', 'invalid', 'Basic', 'BASIC', 123, []]
        for variant in invalid_variants:
            with self.assertRaises((ValueError, TypeError)):
                BXTrenderIndicator(period=14, variant=variant)

    def test_parameter_combinations(self):
        """Test various parameter combinations."""
        test_cases = [
            {'period': 7, 'variant': 'basic'},
            {'period': 14, 'variant': 'directional'},
            {'period': 21, 'variant': 'volume_weighted'},
            {'period': 50, 'variant': 'basic'},
        ]

        for params in test_cases:
            indicator = BXTrenderIndicator(**params)
            self.assertEqual(indicator.period, params['period'])
            self.assertEqual(indicator.variant, params['variant'])

    def test_default_parameters(self):
        """Test default parameter handling."""
        # Test with only required parameters
        indicator = BXTrenderIndicator(period=14, variant='basic')
        self.assertEqual(indicator.period, 14)
        self.assertEqual(indicator.variant, 'basic')

    def test_framework_indicator_configuration(self):
        """Test configuration of framework indicators."""
        # Test BXTrenderBasic
        basic = BXTrenderBasic(period=21)
        self.assertEqual(basic.period, 21)
        self.assertIsNone(basic.latest_bx_trender)

        # Test BXTrenderDirectional
        directional = BXTrenderDirectional(period=14)
        self.assertEqual(directional.period, 14)
        self.assertIsNone(directional.latest_bx_trender)

        # Test BXTrenderVolumeWeighted
        volume_weighted = BXTrenderVolumeWeighted(period=30)
        self.assertEqual(volume_weighted.period, 30)
        self.assertIsNone(volume_weighted.latest_bx_trender)

    def test_period_boundary_values(self):
        """Test period boundary values."""
        # Minimum valid period
        indicator = BXTrenderIndicator(period=6, variant='basic')
        self.assertEqual(indicator.period, 6)

        # Maximum reasonable period
        indicator = BXTrenderIndicator(period=200, variant='basic')
        self.assertEqual(indicator.period, 200)

        # Test calculation with boundary periods
        data = self.create_test_data(250)

        # Test small period
        small_period = BXTrenderIndicator(period=6, variant='basic')
        result = small_period.calculate(data)
        self.assertEqual(result['status'], 'valid')

        # Test large period
        large_period = BXTrenderIndicator(period=200, variant='basic')
        result = large_period.calculate(data)
        self.assertEqual(result['status'], 'valid')

    def test_configuration_immutability(self):
        """Test that configuration parameters cannot be changed after initialization."""
        indicator = BXTrenderIndicator(period=14, variant='basic')

        # Try to modify configuration
        with self.assertRaises(AttributeError):
            indicator.period = 21

        with self.assertRaises(AttributeError):
            indicator.variant = 'directional'

    def test_configuration_serialization(self):
        """Test configuration can be serialized/deserialized."""
        original = BXTrenderIndicator(period=21, variant='directional')

        # Create configuration dict
        config = {
            'period': original.period,
            'variant': original.variant
        }

        # Recreate from config
        recreated = BXTrenderIndicator(**config)

        self.assertEqual(original.period, recreated.period)
        self.assertEqual(original.variant, recreated.variant)

    def test_multiple_indicator_instances(self):
        """Test creating multiple indicator instances with different configurations."""
        indicators = [
            BXTrenderIndicator(period=7, variant='basic'),
            BXTrenderIndicator(period=14, variant='directional'),
            BXTrenderIndicator(period=21, variant='volume_weighted'),
            BXTrenderIndicator(period=30, variant='basic'),
        ]

        # Verify each has correct configuration
        expected_configs = [
            (7, 'basic'),
            (14, 'directional'),
            (21, 'volume_weighted'),
            (30, 'basic')
        ]

        for indicator, (period, variant) in zip(indicators, expected_configs):
            self.assertEqual(indicator.period, period)
            self.assertEqual(indicator.variant, variant)

    def test_configuration_with_calculation(self):
        """Test that different configurations produce different results."""
        data = self.create_test_data(50)

        # Create indicators with different periods
        indicator_7 = BXTrenderIndicator(period=7, variant='basic')
        indicator_21 = BXTrenderIndicator(period=21, variant='basic')

        result_7 = indicator_7.calculate(data)
        result_21 = indicator_21.calculate(data)

        # Both should be valid
        self.assertEqual(result_7['status'], 'valid')
        self.assertEqual(result_21['status'], 'valid')

        # Values should be different due to different periods
        self.assertNotEqual(result_7['value'], result_21['value'])

    def test_variant_specific_configuration(self):
        """Test variant-specific configuration behavior."""
        data = self.create_test_data_with_volume(50)

        # Create indicators with different variants
        basic = BXTrenderIndicator(period=14, variant='basic')
        directional = BXTrenderIndicator(period=14, variant='directional')
        volume_weighted = BXTrenderIndicator(period=14, variant='volume_weighted')

        result_basic = basic.calculate(data)
        result_directional = directional.calculate(data)
        result_volume = volume_weighted.calculate(data)

        # All should be valid
        self.assertEqual(result_basic['status'], 'valid')
        self.assertEqual(result_directional['status'], 'valid')
        self.assertEqual(result_volume['status'], 'valid')

        # Values should be different due to different calculations
        self.assertNotEqual(result_basic['value'], result_directional['value'])
        self.assertNotEqual(result_basic['value'], result_volume['value'])
        self.assertNotEqual(result_directional['value'], result_volume['value'])

    def create_test_data(self, periods=50):
        """Create test DataFrame with OHLC data."""
        np.random.seed(42)
        data = []
        base_price = 100

        for i in range(periods):
            price = base_price + np.random.uniform(-2, 2)
            data.append({
                'open': price * 0.99,
                'high': price * 1.01,
                'low': price * 0.98,
                'close': price,
                'volume': 100000 + np.random.randint(-10000, 10000),
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })

        return pd.DataFrame(data)

    def create_test_data_with_volume(self, periods=50):
        """Create test DataFrame with varying volume patterns."""
        np.random.seed(42)
        data = []
        base_price = 100
        base_volume = 100000

        for i in range(periods):
            price_change = np.random.uniform(-1, 1)
            price = base_price + price_change

            # Volume varies with price movement
            if abs(price_change) > 0.5:
                volume = base_volume * (1.5 + np.random.uniform(0, 0.5))
            else:
                volume = base_volume * (0.8 + np.random.uniform(0, 0.4))

            data.append({
                'open': price * 0.995,
                'high': price * 1.005,
                'low': price * 0.995,
                'close': price,
                'volume': int(volume),
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })

        return pd.DataFrame(data)


class TestBXTrenderConfigurationValidation(unittest.TestCase):
    """Test configuration validation edge cases."""

    def test_extreme_period_values(self):
        """Test extreme period values."""
        # Very small periods (edge case)
        with self.assertRaises(ValueError):
            BXTrenderIndicator(period=5, variant='basic')  # Too small

        # Very large periods (should work but might be impractical)
        indicator = BXTrenderIndicator(period=500, variant='basic')
        self.assertEqual(indicator.period, 500)

    def test_string_period_conversion(self):
        """Test string period conversion."""
        # Valid string numbers should be converted
        indicator = BXTrenderIndicator(period='14', variant='basic')
        self.assertEqual(indicator.period, 14)
        self.assertIsInstance(indicator.period, int)

        # Invalid strings should raise error
        with self.assertRaises((ValueError, TypeError)):
            BXTrenderIndicator(period='invalid', variant='basic')

    def test_float_period_conversion(self):
        """Test float period conversion."""
        # Float periods should be converted to int
        indicator = BXTrenderIndicator(period=14.0, variant='basic')
        self.assertEqual(indicator.period, 14)
        self.assertIsInstance(indicator.period, int)

        # Non-integer floats should be rounded or raise error
        indicator = BXTrenderIndicator(period=14.7, variant='basic')
        self.assertEqual(indicator.period, 14)  # Should be truncated

    def test_variant_case_sensitivity(self):
        """Test variant parameter case sensitivity."""
        # Should be case sensitive
        with self.assertRaises(ValueError):
            BXTrenderIndicator(period=14, variant='Basic')

        with self.assertRaises(ValueError):
            BXTrenderIndicator(period=14, variant='BASIC')

        with self.assertRaises(ValueError):
            BXTrenderIndicator(period=14, variant='Volume_Weighted')

    def test_none_parameters(self):
        """Test None parameter handling."""
        with self.assertRaises((ValueError, TypeError)):
            BXTrenderIndicator(period=None, variant='basic')

        with self.assertRaises((ValueError, TypeError)):
            BXTrenderIndicator(period=14, variant=None)

    def test_empty_parameters(self):
        """Test empty parameter handling."""
        with self.assertRaises((ValueError, TypeError)):
            BXTrenderIndicator(period='', variant='basic')

        with self.assertRaises(ValueError):
            BXTrenderIndicator(period=14, variant='')

    def test_configuration_consistency(self):
        """Test configuration consistency across framework and enhanced implementations."""
        # Create matching configurations
        period = 14

        # Framework implementations
        basic_framework = BXTrenderBasic(period=period)
        directional_framework = BXTrenderDirectional(period=period)
        volume_framework = BXTrenderVolumeWeighted(period=period)

        # Enhanced implementations
        basic_enhanced = BXTrenderIndicator(period=period, variant='basic')
        directional_enhanced = BXTrenderIndicator(period=period, variant='directional')
        volume_enhanced = BXTrenderIndicator(period=period, variant='volume_weighted')

        # All should have consistent period configuration
        framework_periods = [basic_framework.period, directional_framework.period, volume_framework.period]
        enhanced_periods = [basic_enhanced.period, directional_enhanced.period, volume_enhanced.period]

        for fw_period, en_period in zip(framework_periods, enhanced_periods):
            self.assertEqual(fw_period, en_period)


if __name__ == '__main__':
    unittest.main()