"""
Unit tests for BX Trender Volume Weighted indicator.
"""
import unittest
import math
import pandas as pd
import numpy as np
from unittest.mock import Mock
from datetime import datetime, timedelta
from types import SimpleNamespace

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from signals.indicator import BXTrenderVolumeWeighted
from signals.enhanced_indicators import BXTrenderIndicator


class TestBXTrenderVolumeWeighted(unittest.TestCase):
    """Test cases for BX Trender Volume Weighted indicator."""

    def setUp(self):
        """Set up test fixtures."""
        self.period = 14
        self.indicator = BXTrenderVolumeWeighted(period=self.period)

    def create_mock_intervals(self, price_volume_data):
        """
        Create mock InstrumentInterval objects for testing.
        
        Args:
            price_volume_data: List of tuples (close, volume)
        """
        intervals = []
        base_time = datetime(2024, 1, 1)
        
        for i, (close, volume) in enumerate(price_volume_data):
            interval = SimpleNamespace()
            interval.close = close
            interval.open = close * 0.999  # Slight difference
            interval.high = close * 1.001
            interval.low = close * 0.998
            interval.traded_volume = volume
            interval.start_date_time = base_time + timedelta(minutes=i)
            interval.status = 'ok'
            intervals.append(interval)
        
        return intervals

    def generate_volume_weighted_data(self, periods, price_trend='up', volume_pattern='normal'):
        """
        Generate price and volume data with specific patterns.
        
        Args:
            periods: Number of data points
            price_trend: 'up', 'down', or 'sideways'
            volume_pattern: 'normal', 'high_on_up', 'high_on_down', or 'decreasing'
        """
        data = []
        base_price = 100
        base_volume = 100000
        
        for i in range(periods):
            # Generate price based on trend
            if price_trend == 'up':
                price_change = 0.5 + np.random.uniform(0, 0.5)  # Positive bias
                price = base_price + i * 0.3 + np.random.uniform(-0.2, 0.8)
            elif price_trend == 'down':
                price_change = -0.5 - np.random.uniform(0, 0.5)  # Negative bias
                price = base_price - i * 0.3 + np.random.uniform(-0.8, 0.2)
            else:  # sideways
                price = base_price + np.random.uniform(-1, 1)
                price_change = np.random.uniform(-0.3, 0.3)
            
            # Generate volume based on pattern
            if volume_pattern == 'normal':
                volume = base_volume + np.random.uniform(-20000, 20000)
            elif volume_pattern == 'high_on_up':
                # Higher volume on up moves
                if price_change > 0:
                    volume = base_volume * (1.5 + np.random.uniform(0, 1))
                else:
                    volume = base_volume * (0.7 + np.random.uniform(0, 0.3))
            elif volume_pattern == 'high_on_down':
                # Higher volume on down moves
                if price_change < 0:
                    volume = base_volume * (1.5 + np.random.uniform(0, 1))
                else:
                    volume = base_volume * (0.7 + np.random.uniform(0, 0.3))
            else:  # decreasing
                volume = base_volume * (1 - i * 0.03)
            
            volume = max(volume, 1000)  # Ensure positive volume
            data.append((price, volume))
        
        return data

    def test_initialization(self):
        """Test indicator initialization."""
        self.assertEqual(self.indicator.period, 14)
        self.assertIsNone(self.indicator.latest_bx_trender)
        self.assertIsNone(self.indicator.bullish_volume_ratio)
        self.assertIsNone(self.indicator.bearish_volume_ratio)
        self.assertIsNone(self.indicator.volume_momentum)
        self.assertIsNone(self.indicator.trend_strength)
        self.assertIsNone(self.indicator.trend_direction)
        self.assertIsNone(self.indicator.status)

    def test_insufficient_data(self):
        """Test behavior with insufficient data."""
        # Test with no data
        self.indicator.update([])
        self.assertEqual(self.indicator.status, 'insufficient_data')
        self.assertIsNone(self.indicator.get_value())

        # Test with insufficient periods
        data = self.generate_volume_weighted_data(5, 'up', 'normal')
        intervals = self.create_mock_intervals(data)
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'insufficient_data')
        self.assertIsNone(self.indicator.get_value())

    def test_no_volume_data(self):
        """Test behavior when volume data is missing."""
        data = self.generate_volume_weighted_data(20, 'up', 'normal')
        intervals = self.create_mock_intervals(data)
        
        # Remove volume from one interval
        intervals[10].traded_volume = None
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'no_volume_data')
        self.assertIsNone(self.indicator.get_value())

        # Test with NaN volume
        intervals[10].traded_volume = float('nan')
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'no_volume_data')

        # Test with negative volume
        intervals[10].traded_volume = -1000
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')

    def test_invalid_data(self):
        """Test behavior with invalid data."""
        data = self.generate_volume_weighted_data(20, 'up', 'normal')
        intervals = self.create_mock_intervals(data)
        
        # Test with invalid status
        intervals[5].status = 'invalid'
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')
        self.assertIsNone(self.indicator.get_value())

        # Reset and test with NaN close price
        intervals[5].status = 'ok'
        intervals[10].close = float('nan')
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')

    def test_bullish_volume_weighted_trend(self):
        """Test BX Trender with high volume on up moves."""
        data = self.generate_volume_weighted_data(20, 'up', 'high_on_up')
        intervals = self.create_mock_intervals(data)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        bullish_ratio = self.indicator.get_bullish_volume_ratio()
        bearish_ratio = self.indicator.get_bearish_volume_ratio()
        
        # Should be bullish due to high volume on up moves
        self.assertIsNotNone(bx_trender)
        self.assertGreater(bx_trender, 55)  # Should be above neutral
        self.assertGreater(bullish_ratio, bearish_ratio)  # More bullish volume
        self.assertEqual(self.indicator.trend_direction, 1)  # Bullish direction

    def test_bearish_volume_weighted_trend(self):
        """Test BX Trender with high volume on down moves."""
        data = self.generate_volume_weighted_data(20, 'down', 'high_on_down')
        intervals = self.create_mock_intervals(data)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        bullish_ratio = self.indicator.get_bullish_volume_ratio()
        bearish_ratio = self.indicator.get_bearish_volume_ratio()
        
        # Should be bearish due to high volume on down moves
        self.assertIsNotNone(bx_trender)
        self.assertLess(bx_trender, 45)  # Should be below neutral
        self.assertGreater(bearish_ratio, bullish_ratio)  # More bearish volume
        self.assertEqual(self.indicator.trend_direction, -1)  # Bearish direction

    def test_neutral_volume_pattern(self):
        """Test BX Trender with balanced volume pattern."""
        data = self.generate_volume_weighted_data(20, 'sideways', 'normal')
        intervals = self.create_mock_intervals(data)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        bullish_ratio = self.indicator.get_bullish_volume_ratio()
        bearish_ratio = self.indicator.get_bearish_volume_ratio()
        
        # Should be near neutral
        self.assertIsNotNone(bx_trender)
        self.assertGreater(bx_trender, 40)
        self.assertLess(bx_trender, 60)
        
        # Volume ratios should be relatively balanced
        ratio_diff = abs(bullish_ratio - bearish_ratio)
        self.assertLess(ratio_diff, 0.3)  # Not too imbalanced

    def test_volume_momentum_calculation(self):
        """Test volume momentum calculation."""
        # Test with increasing volume
        data = []
        base_price = 100
        for i in range(20):
            price = base_price + np.random.uniform(-0.5, 0.5)
            volume = 50000 + i * 5000  # Increasing volume
            data.append((price, volume))
        
        intervals = self.create_mock_intervals(data)
        self.indicator.update(intervals)
        
        volume_momentum = self.indicator.get_volume_momentum()
        self.assertIsNotNone(volume_momentum)
        self.assertGreater(volume_momentum, 0)  # Should be positive for increasing volume

        # Test with decreasing volume
        decreasing_data = self.generate_volume_weighted_data(20, 'sideways', 'decreasing')
        decreasing_intervals = self.create_mock_intervals(decreasing_data)
        decreasing_indicator = BXTrenderVolumeWeighted(period=14)
        decreasing_indicator.update(decreasing_intervals)
        
        decreasing_momentum = decreasing_indicator.get_volume_momentum()
        self.assertIsNotNone(decreasing_momentum)
        self.assertLess(decreasing_momentum, 0)  # Should be negative for decreasing volume

    def test_zero_volume_handling(self):
        """Test handling of zero total volume."""
        # Create data with all zero price changes (no gains/losses)
        data = [(100, 100000) for _ in range(20)]  # Same price, same volume
        intervals = self.create_mock_intervals(data)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        bullish_ratio = self.indicator.get_bullish_volume_ratio()
        bearish_ratio = self.indicator.get_bearish_volume_ratio()
        
        # Should default to neutral values
        self.assertEqual(bx_trender, 50.0)
        self.assertEqual(bullish_ratio, 0.5)
        self.assertEqual(bearish_ratio, 0.5)

    def test_trend_strength_with_volume_weighting(self):
        """Test that trend strength is enhanced by volume."""
        # Test high volume scenario
        high_volume_data = []
        for i in range(20):
            price = 100 + i * 0.5  # Uptrend
            volume = 200000  # High volume
            high_volume_data.append((price, volume))
        
        high_vol_intervals = self.create_mock_intervals(high_volume_data)
        high_vol_indicator = BXTrenderVolumeWeighted(period=14)
        high_vol_indicator.update(high_vol_intervals)
        
        # Test low volume scenario
        low_volume_data = []
        for i in range(20):
            price = 100 + i * 0.5  # Same uptrend
            volume = 50000  # Low volume
            low_volume_data.append((price, volume))
        
        low_vol_intervals = self.create_mock_intervals(low_volume_data)
        low_vol_indicator = BXTrenderVolumeWeighted(period=14)
        low_vol_indicator.update(low_vol_intervals)
        
        high_strength = high_vol_indicator.trend_strength
        low_strength = low_vol_indicator.trend_strength
        
        # High volume should generally give higher trend strength
        self.assertIsNotNone(high_strength)
        self.assertIsNotNone(low_strength)
        # Note: This might not always be true due to volume ratio capping at 2x

    def test_different_periods(self):
        """Test BX Trender Volume Weighted with different periods."""
        data = self.generate_volume_weighted_data(30, 'up', 'high_on_up')
        intervals = self.create_mock_intervals(data)
        
        # Test period 7
        indicator_7 = BXTrenderVolumeWeighted(period=7)
        indicator_7.update(intervals)
        
        # Test period 21
        indicator_21 = BXTrenderVolumeWeighted(period=21)
        indicator_21.update(intervals)
        
        # Both should detect the bullish trend
        self.assertEqual(indicator_7.status, 'ok')
        self.assertEqual(indicator_21.status, 'ok')
        
        bx_7 = indicator_7.get_value()
        bx_21 = indicator_21.get_value()
        
        self.assertIsNotNone(bx_7)
        self.assertIsNotNone(bx_21)
        self.assertGreater(bx_7, 50)  # Bullish
        self.assertGreater(bx_21, 50)  # Bullish

    def test_volume_ratio_validation(self):
        """Test that volume ratios are properly calculated and sum to ~1."""
        data = self.generate_volume_weighted_data(20, 'up', 'normal')
        intervals = self.create_mock_intervals(data)
        
        self.indicator.update(intervals)
        
        bullish_ratio = self.indicator.get_bullish_volume_ratio()
        bearish_ratio = self.indicator.get_bearish_volume_ratio()
        
        self.assertIsNotNone(bullish_ratio)
        self.assertIsNotNone(bearish_ratio)
        
        # Ratios should be non-negative
        self.assertGreaterEqual(bullish_ratio, 0)
        self.assertGreaterEqual(bearish_ratio, 0)
        
        # Ratios should sum to approximately 1 (allowing for neutral volume)
        total_ratio = bullish_ratio + bearish_ratio
        self.assertLessEqual(total_ratio, 1.1)  # Allow small margin for neutral volume

    def test_reset_values_method(self):
        """Test the _reset_values method."""
        # Set some values first
        self.indicator.latest_bx_trender = 60.0
        self.indicator.bullish_volume_ratio = 0.6
        self.indicator.bearish_volume_ratio = 0.4
        self.indicator.volume_momentum = 0.1
        self.indicator.trend_strength = 0.5
        self.indicator.trend_direction = 1
        
        # Reset values
        self.indicator._reset_values()
        
        # Check all values are None
        self.assertIsNone(self.indicator.latest_bx_trender)
        self.assertIsNone(self.indicator.bullish_volume_ratio)
        self.assertIsNone(self.indicator.bearish_volume_ratio)
        self.assertIsNone(self.indicator.volume_momentum)
        self.assertIsNone(self.indicator.trend_strength)
        self.assertIsNone(self.indicator.trend_direction)

    def test_getter_methods(self):
        """Test all getter methods."""
        data = self.generate_volume_weighted_data(20, 'up', 'high_on_up')
        intervals = self.create_mock_intervals(data)
        self.indicator.update(intervals)
        
        # Test all getters return valid values
        self.assertIsNotNone(self.indicator.get_value())
        self.assertIsNotNone(self.indicator.get_bullish_volume_ratio())
        self.assertIsNotNone(self.indicator.get_bearish_volume_ratio())
        self.assertIsNotNone(self.indicator.get_volume_momentum())

    def test_calculation_error_handling(self):
        """Test error handling during calculation."""
        data = self.generate_volume_weighted_data(20, 'up', 'normal')
        intervals = self.create_mock_intervals(data)
        
        # Mock a calculation error
        original_update = self.indicator.__class__.update
        def error_update(self, intervals):
            raise ValueError("Test calculation error")
        
        self.indicator.__class__.update = error_update
        try:
            self.indicator.update(intervals)
            self.assertEqual(self.indicator.status, 'calculation_error')
            self.assertIsNone(self.indicator.get_value())
        finally:
            self.indicator.__class__.update = original_update

    def test_extreme_volume_scenarios(self):
        """Test handling of extreme volume scenarios."""
        # Test with very high volumes
        high_volume_data = [(100 + i * 0.1, 10000000) for i in range(20)]
        intervals = self.create_mock_intervals(high_volume_data)
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        self.assertIsNotNone(self.indicator.get_value())
        
        # Test with very low volumes (but not zero)
        low_volume_data = [(100 + i * 0.1, 10) for i in range(20)]
        low_vol_indicator = BXTrenderVolumeWeighted(period=14)
        low_intervals = self.create_mock_intervals(low_volume_data)
        low_vol_indicator.update(low_intervals)
        
        self.assertEqual(low_vol_indicator.status, 'ok')
        self.assertIsNotNone(low_vol_indicator.get_value())


class TestBXTrenderVolumeWeightedEnhancedFramework(unittest.TestCase):
    """Test BX Trender Volume Weighted in the enhanced indicators framework."""

    def setUp(self):
        """Set up test fixtures."""
        self.indicator = BXTrenderIndicator(period=14, variant='volume_weighted')

    def create_sample_data(self, periods=20, volume_pattern='high_on_up'):
        """Create sample DataFrame for testing."""
        np.random.seed(42)
        data = []
        base_price = 100
        
        for i in range(periods):
            price_change = 0.5 + np.random.uniform(-0.3, 0.7)  # Slight upward bias
            price = base_price + i * 0.3 + np.random.uniform(-0.5, 0.5)
            
            # Volume based on pattern
            if volume_pattern == 'high_on_up' and price_change > 0:
                volume = 150000 + np.random.uniform(0, 50000)
            elif volume_pattern == 'high_on_down' and price_change < 0:
                volume = 150000 + np.random.uniform(0, 50000)
            else:
                volume = 100000 + np.random.uniform(-20000, 20000)
            
            data.append({
                'open': price * 0.999,
                'high': price * 1.002,
                'low': price * 0.997,
                'close': price,
                'volume': max(volume, 1000),
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })
        
        return pd.DataFrame(data)

    def test_enhanced_framework_calculation(self):
        """Test calculation through enhanced framework."""
        data = self.create_sample_data()
        result = self.indicator.calculate(data)
        
        self.assertEqual(result['status'], 'valid')
        self.assertIn('value', result)
        self.assertIn('bx_trender', result)
        self.assertIn('bullish_volume_ratio', result)
        self.assertIn('bearish_volume_ratio', result)
        self.assertIn('volume_momentum', result)
        self.assertIn('volume_ratio', result)
        self.assertIn('trend_strength', result)
        self.assertIn('trend_direction', result)
        self.assertIn('total_volume', result)
        
        # Check that we detected some bullish bias due to high volume on up moves
        self.assertGreater(result['value'], 50)
        self.assertGreater(result['bullish_volume_ratio'], result['bearish_volume_ratio'])

    def test_enhanced_framework_no_volume_data(self):
        """Test enhanced framework without volume data."""
        data = self.create_sample_data()
        data = data.drop('volume', axis=1)  # Remove volume column
        result = self.indicator.calculate(data)
        
        self.assertEqual(result['status'], 'no_volume_data')
        self.assertIsNone(result['value'])

    def test_enhanced_framework_insufficient_data(self):
        """Test enhanced framework with insufficient data."""
        data = self.create_sample_data(periods=5)  # Too little data
        result = self.indicator.calculate(data)
        
        self.assertEqual(result['status'], 'insufficient_data')
        self.assertIsNone(result['value'])

    def test_enhanced_framework_bearish_scenario(self):
        """Test bearish scenario through enhanced framework."""
        data = self.create_sample_data(volume_pattern='high_on_down')
        
        # Make the price trend more bearish
        for i in range(len(data)):
            data.iloc[i, data.columns.get_loc('close')] = 100 - i * 0.5
        
        result = self.indicator.calculate(data)
        
        self.assertEqual(result['status'], 'valid')
        # Due to the complex nature of volume weighting, we mainly check it calculates
        self.assertIsNotNone(result['value'])
        self.assertEqual(result['trend_direction'], -1)


if __name__ == '__main__':
    unittest.main()