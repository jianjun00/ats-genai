"""
Unit tests for BX Trender Basic indicator.
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

from signals.indicator import BXTrenderBasic
from signals.enhanced_indicators import BXTrenderIndicator


class TestBXTrenderBasic(unittest.TestCase):
    """Test cases for BX Trender Basic indicator."""

    def setUp(self):
        """Set up test fixtures."""
        self.period = 14
        self.indicator = BXTrenderBasic(period=self.period)

    def create_mock_intervals(self, close_prices, volumes=None):
        """Create mock InstrumentInterval objects for testing."""
        intervals = []
        base_time = datetime(2024, 1, 1)
        
        for i, close in enumerate(close_prices):
            interval = SimpleNamespace()
            interval.close = close
            interval.open = close * 0.99  # Slight difference for realism
            interval.high = close * 1.01
            interval.low = close * 0.98
            interval.traded_volume = volumes[i] if volumes else 100000
            interval.start_date_time = base_time + timedelta(minutes=i)
            interval.status = 'ok'
            intervals.append(interval)
        
        return intervals

    def test_initialization(self):
        """Test indicator initialization."""
        self.assertEqual(self.indicator.period, 14)
        self.assertIsNone(self.indicator.latest_bx_trender)
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
        intervals = self.create_mock_intervals([100, 101, 102])  # Only 3 intervals
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'insufficient_data')
        self.assertIsNone(self.indicator.get_value())

    def test_invalid_data(self):
        """Test behavior with invalid data."""
        # Test with invalid status
        intervals = self.create_mock_intervals([100] * 20)
        intervals[5].status = 'invalid'
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')
        self.assertIsNone(self.indicator.get_value())

        # Test with NaN close price
        intervals = self.create_mock_intervals([100] * 20)
        intervals[10].close = float('nan')
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')
        self.assertIsNone(self.indicator.get_value())

        # Test with negative close price
        intervals = self.create_mock_intervals([100] * 20)
        intervals[10].close = -50
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')
        self.assertIsNone(self.indicator.get_value())

    def test_uptrend_calculation(self):
        """Test BX Trender calculation for uptrend."""
        # Create strong uptrend data
        close_prices = [100 + i * 2 for i in range(20)]  # Consistent uptrend
        intervals = self.create_mock_intervals(close_prices)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        
        # Should be well above 50 for strong uptrend
        self.assertIsNotNone(bx_trender)
        self.assertGreater(bx_trender, 70)  # Strong bullish signal
        self.assertEqual(self.indicator.get_trend_direction(), 1)
        self.assertGreater(self.indicator.get_trend_strength(), 0.4)

    def test_downtrend_calculation(self):
        """Test BX Trender calculation for downtrend."""
        # Create strong downtrend data
        close_prices = [100 - i * 2 for i in range(20)]  # Consistent downtrend
        intervals = self.create_mock_intervals(close_prices)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        
        # Should be well below 50 for strong downtrend
        self.assertIsNotNone(bx_trender)
        self.assertLess(bx_trender, 30)  # Strong bearish signal
        self.assertEqual(self.indicator.get_trend_direction(), -1)
        self.assertGreater(self.indicator.get_trend_strength(), 0.4)

    def test_sideways_calculation(self):
        """Test BX Trender calculation for sideways movement."""
        # Create sideways/neutral data
        np.random.seed(42)
        close_prices = [100 + np.random.uniform(-0.5, 0.5) for _ in range(20)]
        intervals = self.create_mock_intervals(close_prices)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        
        # Should be near 50 for sideways movement
        self.assertIsNotNone(bx_trender)
        self.assertGreater(bx_trender, 40)
        self.assertLess(bx_trender, 60)
        self.assertLess(self.indicator.get_trend_strength(), 0.3)

    def test_no_movement_calculation(self):
        """Test BX Trender calculation when there's no price movement."""
        # All same price
        close_prices = [100] * 20
        intervals = self.create_mock_intervals(close_prices)
        
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        
        # Should be exactly 50 when no movement
        self.assertEqual(bx_trender, 50.0)
        self.assertEqual(self.indicator.get_trend_direction(), 0)
        self.assertEqual(self.indicator.get_trend_strength(), 0.0)

    def test_mixed_trend_calculation(self):
        """Test BX Trender calculation for mixed up/down movements."""
        # Alternating up/down pattern
        close_prices = []
        base = 100
        for i in range(20):
            if i % 2 == 0:
                close_prices.append(base + 1)
            else:
                close_prices.append(base - 1)
        
        intervals = self.create_mock_intervals(close_prices)
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        
        # Should be close to 50 due to equal gains and losses
        self.assertIsNotNone(bx_trender)
        self.assertGreater(bx_trender, 45)
        self.assertLess(bx_trender, 55)

    def test_different_periods(self):
        """Test BX Trender with different periods."""
        close_prices = [100 + i * 0.5 for i in range(30)]  # Mild uptrend
        intervals = self.create_mock_intervals(close_prices)
        
        # Test period 7
        indicator_7 = BXTrenderBasic(period=7)
        indicator_7.update(intervals)
        bx_7 = indicator_7.get_value()
        
        # Test period 21
        indicator_21 = BXTrenderBasic(period=21)
        indicator_21.update(intervals)
        bx_21 = indicator_21.get_value()
        
        # Both should work and give reasonable values
        self.assertIsNotNone(bx_7)
        self.assertIsNotNone(bx_21)
        self.assertGreater(bx_7, 50)  # Uptrend
        self.assertGreater(bx_21, 50)  # Uptrend
        
        # Shorter period might be more sensitive
        self.assertEqual(indicator_7.status, 'ok')
        self.assertEqual(indicator_21.status, 'ok')

    def test_getter_methods(self):
        """Test all getter methods."""
        close_prices = [100 + i for i in range(20)]  # Clear uptrend
        intervals = self.create_mock_intervals(close_prices)
        self.indicator.update(intervals)
        
        # Test all getters return valid values
        self.assertIsNotNone(self.indicator.get_value())
        self.assertIsNotNone(self.indicator.get_trend_strength())
        self.assertIsNotNone(self.indicator.get_trend_direction())
        
        # Test value consistency
        self.assertEqual(self.indicator.get_value(), self.indicator.latest_bx_trender)
        self.assertEqual(self.indicator.get_trend_strength(), self.indicator.trend_strength)
        self.assertEqual(self.indicator.get_trend_direction(), self.indicator.trend_direction)

    def test_calculation_error_handling(self):
        """Test error handling during calculation."""
        # Create intervals that might cause calculation errors
        intervals = self.create_mock_intervals([100] * 20)
        
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

    def test_boundary_values(self):
        """Test BX Trender with boundary values."""
        # Test with very small price changes
        close_prices = [100 + i * 0.0001 for i in range(20)]
        intervals = self.create_mock_intervals(close_prices)
        self.indicator.update(intervals)
        
        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        self.assertIsNotNone(bx_trender)
        self.assertGreaterEqual(bx_trender, 0)
        self.assertLessEqual(bx_trender, 100)

    def test_trend_strength_scaling(self):
        """Test that trend strength is properly scaled to 0-1."""
        # Test various trend scenarios
        test_cases = [
            ([100] * 20, 0.0),  # No movement = 0 strength
            ([100 + i * 5 for i in range(20)], 0.8),  # Strong uptrend > 0.8
            ([100 - i * 5 for i in range(20)], 0.8),  # Strong downtrend > 0.8
        ]
        
        for close_prices, expected_min_strength in test_cases:
            indicator = BXTrenderBasic(period=14)
            intervals = self.create_mock_intervals(close_prices)
            indicator.update(intervals)
            
            strength = indicator.get_trend_strength()
            self.assertIsNotNone(strength)
            self.assertGreaterEqual(strength, 0.0)
            self.assertLessEqual(strength, 1.0)
            
            if expected_min_strength > 0:
                self.assertGreater(strength, expected_min_strength)


class TestBXTrenderBasicEnhancedFramework(unittest.TestCase):
    """Test BX Trender Basic in the enhanced indicators framework."""

    def setUp(self):
        """Set up test fixtures."""
        self.indicator = BXTrenderIndicator(period=14, variant='basic')

    def create_sample_data(self, periods=20):
        """Create sample DataFrame for testing."""
        np.random.seed(42)
        close_prices = [100 + i * 0.5 for i in range(periods)]  # Mild uptrend
        
        data = []
        for i, close in enumerate(close_prices):
            data.append({
                'open': close * 0.99,
                'high': close * 1.01,
                'low': close * 0.98,
                'close': close,
                'volume': 100000 + i * 1000,
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
        self.assertIn('trend_strength', result)
        self.assertIn('trend_direction', result)
        self.assertIn('avg_gains', result)
        self.assertIn('avg_losses', result)

    def test_enhanced_framework_insufficient_data(self):
        """Test enhanced framework with insufficient data."""
        data = self.create_sample_data(periods=5)  # Too little data
        result = self.indicator.calculate(data)
        
        self.assertEqual(result['status'], 'insufficient_data')
        self.assertIsNone(result['value'])

    def test_enhanced_framework_invalid_variant(self):
        """Test enhanced framework with invalid variant."""
        indicator = BXTrenderIndicator(period=14, variant='invalid')
        data = self.create_sample_data()
        result = indicator.calculate(data)
        
        self.assertEqual(result['status'], 'invalid_variant')
        self.assertIsNone(result['value'])


if __name__ == '__main__':
    unittest.main()