"""
Unit tests for BX Trender Directional indicator.
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from types import SimpleNamespace

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from domains.trading.signals.indicator import BXTrenderDirectional
from domains.trading.signals.enhanced_indicators import BXTrenderIndicator

class TestBXTrenderDirectional(unittest.TestCase):
    """Test cases for BX Trender Directional indicator."""

    def setUp(self):
        """Set up test fixtures."""
        self.period = 14
        self.indicator = BXTrenderDirectional(period=self.period)

    def create_mock_intervals(self, ohlc_data):
        """
        Create mock InstrumentInterval objects for testing.

        Args:
            ohlc_data: List of tuples (open, high, low, close)
        """
        intervals = []
        base_time = datetime(2024, 1, 1)

        for i, (open_price, high, low, close) in enumerate(ohlc_data):
            interval = SimpleNamespace()
            interval.open = open_price
            interval.high = high
            interval.low = low
            interval.close = close
            interval.traded_volume = 100000 + i * 1000
            interval.start_date_time = base_time + timedelta(minutes=i)
            interval.status = 'ok'
            intervals.append(interval)

        return intervals

    def generate_trending_ohlc(self, periods, trend='up'):
        """Generate OHLC data with specific trend characteristics."""
        ohlc_data = []
        base_price = 100

        for i in range(periods):
            if trend == 'up':
                # Uptrend: higher highs and higher lows
                open_price = base_price + i * 0.5
                close = open_price + 0.3
                high = close + 0.2
                low = open_price - 0.1
            elif trend == 'down':
                # Downtrend: lower highs and lower lows
                open_price = base_price - i * 0.5
                close = open_price - 0.3
                high = open_price + 0.1
                low = close - 0.2
            else:  # sideways
                # Sideways: random but bounded movement
                np.random.seed(42 + i)
                open_price = base_price + np.random.uniform(-0.5, 0.5)
                close = open_price + np.random.uniform(-0.3, 0.3)
                high = max(open_price, close) + np.random.uniform(0, 0.2)
                low = min(open_price, close) - np.random.uniform(0, 0.2)

            ohlc_data.append((open_price, high, low, close))

        return ohlc_data

    def test_initialization(self):
        """Test indicator initialization."""
        self.assertEqual(self.indicator.period, 14)
        self.assertIsNone(self.indicator.latest_bx_trender)
        self.assertIsNone(self.indicator.di_plus)
        self.assertIsNone(self.indicator.di_minus)
        self.assertIsNone(self.indicator.adx)
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
        ohlc_data = self.generate_trending_ohlc(5, 'up')
        intervals = self.create_mock_intervals(ohlc_data)
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'insufficient_data')
        self.assertIsNone(self.indicator.get_value())

    def test_invalid_data(self):
        """Test behavior with invalid data."""
        ohlc_data = self.generate_trending_ohlc(20, 'up')
        intervals = self.create_mock_intervals(ohlc_data)

        # Test with invalid status
        intervals[5].status = 'invalid'
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')
        self.assertIsNone(self.indicator.get_value())

        # Reset and test with NaN high
        intervals[5].status = 'ok'
        intervals[10].high = float('nan')
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')

        # Reset and test with negative low
        intervals[10].high = 105
        intervals[10].low = -50
        self.indicator.update(intervals)
        self.assertEqual(self.indicator.status, 'invalid_data')

    def test_uptrend_calculation(self):
        """Test BX Trender Directional calculation for strong uptrend."""
        ohlc_data = self.generate_trending_ohlc(20, 'up')
        intervals = self.create_mock_intervals(ohlc_data)

        self.indicator.update(intervals)

        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        di_plus = self.indicator.get_di_plus()
        di_minus = self.indicator.get_di_minus()
        adx = self.indicator.get_adx()

        # For uptrend, DI+ should be > DI-, BX Trender should be > 50
        self.assertIsNotNone(bx_trender)
        self.assertIsNotNone(di_plus)
        self.assertIsNotNone(di_minus)
        self.assertIsNotNone(adx)

        self.assertGreater(di_plus, di_minus)  # DI+ > DI- for uptrend
        self.assertGreater(bx_trender, 50)  # Normalized BX Trender > 50
        self.assertGreater(adx, 10)  # Should have some trend strength

        self.assertEqual(self.indicator.trend_direction, 1)  # Bullish direction

    def test_downtrend_calculation(self):
        """Test BX Trender Directional calculation for strong downtrend."""
        ohlc_data = self.generate_trending_ohlc(20, 'down')
        intervals = self.create_mock_intervals(ohlc_data)

        self.indicator.update(intervals)

        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        di_plus = self.indicator.get_di_plus()
        di_minus = self.indicator.get_di_minus()
        adx = self.indicator.get_adx()

        # For downtrend, DI- should be > DI+, BX Trender should be < 50
        self.assertIsNotNone(bx_trender)
        self.assertIsNotNone(di_plus)
        self.assertIsNotNone(di_minus)
        self.assertIsNotNone(adx)

        self.assertGreater(di_minus, di_plus)  # DI- > DI+ for downtrend
        self.assertLess(bx_trender, 50)  # Normalized BX Trender < 50
        self.assertGreater(adx, 10)  # Should have some trend strength

        self.assertEqual(self.indicator.trend_direction, -1)  # Bearish direction

    def test_sideways_calculation(self):
        """Test BX Trender Directional calculation for sideways movement."""
        ohlc_data = self.generate_trending_ohlc(20, 'sideways')
        intervals = self.create_mock_intervals(ohlc_data)

        self.indicator.update(intervals)

        self.assertEqual(self.indicator.status, 'ok')
        bx_trender = self.indicator.get_value()
        di_plus = self.indicator.get_di_plus()
        di_minus = self.indicator.get_di_minus()
        adx = self.indicator.get_adx()

        # For sideways movement, values should be more balanced
        self.assertIsNotNone(bx_trender)
        self.assertIsNotNone(di_plus)
        self.assertIsNotNone(di_minus)
        self.assertIsNotNone(adx)

        # BX Trender should be closer to 50 (neutral)
        self.assertGreater(bx_trender, 35)
        self.assertLess(bx_trender, 65)

        # ADX should be lower for sideways movement
        self.assertLessEqual(adx, 30)

    def test_normalization_bounds(self):
        """Test that BX Trender is properly normalized to 0-100 range."""
        # Test with extreme uptrend
        ohlc_data = []
        for i in range(20):
            open_price = 100 + i * 5
            close = open_price + 4
            high = close + 1
            low = open_price - 0.5
            ohlc_data.append((open_price, high, low, close))

        intervals = self.create_mock_intervals(ohlc_data)
        self.indicator.update(intervals)

        bx_trender = self.indicator.get_value()
        self.assertIsNotNone(bx_trender)
        self.assertGreaterEqual(bx_trender, 0)
        self.assertLessEqual(bx_trender, 100)

    def test_true_range_calculation(self):
        """Test True Range calculation accuracy."""
        # Create specific OHLC pattern to test True Range
        ohlc_data = [
            (100, 105, 95, 102),   # Initial bar
            (102, 108, 101, 106),  # TR = max(108-101, |108-102|, |101-102|) = 7
            (106, 107, 99, 101),   # TR = max(107-99, |107-106|, |99-106|) = 8
        ]

        # Add more bars to meet minimum requirement
        for i in range(3, 20):
            prev_close = ohlc_data[-1][3]
            open_price = prev_close + np.random.uniform(-1, 1)
            close = open_price + np.random.uniform(-2, 2)
            high = max(open_price, close) + np.random.uniform(0, 2)
            low = min(open_price, close) - np.random.uniform(0, 2)
            ohlc_data.append((open_price, high, low, close))

        intervals = self.create_mock_intervals(ohlc_data)
        self.indicator.update(intervals)

        self.assertEqual(self.indicator.status, 'ok')
        self.assertIsNotNone(self.indicator.get_value())

    def test_directional_movement_calculation(self):
        """Test directional movement calculation accuracy."""
        # Create pattern with clear directional movement
        ohlc_data = [
            (100, 105, 95, 102),   # Base
            (102, 110, 100, 108),  # Up move: +DM = 110-105 = 5, -DM = 0
            (108, 112, 105, 110),  # Up move: +DM = 112-110 = 2, -DM = 0
            (110, 111, 104, 106),  # Down move: +DM = 0, -DM = 105-104 = 1
        ]

        # Add more bars for sufficient data
        base_price = 106
        for i in range(4, 20):
            if i % 2 == 0:  # Alternate up/down for mixed signals
                open_price = base_price
                close = base_price + 2
                high = close + 1
                low = base_price - 0.5
            else:
                open_price = base_price
                close = base_price - 1
                high = base_price + 0.5
                low = close - 1

            ohlc_data.append((open_price, high, low, close))
            base_price = close

        intervals = self.create_mock_intervals(ohlc_data)
        self.indicator.update(intervals)

        self.assertEqual(self.indicator.status, 'ok')
        di_plus = self.indicator.get_di_plus()
        di_minus = self.indicator.get_di_minus()

        # Both DI+ and DI- should be positive (movement in both directions)
        self.assertIsNotNone(di_plus)
        self.assertIsNotNone(di_minus)
        self.assertGreaterEqual(di_plus, 0)
        self.assertGreaterEqual(di_minus, 0)

    def test_adx_calculation(self):
        """Test ADX (trend strength) calculation."""
        # Test strong trend should give high ADX
        strong_trend_ohlc = self.generate_trending_ohlc(20, 'up')
        intervals = self.create_mock_intervals(strong_trend_ohlc)
        self.indicator.update(intervals)

        strong_adx = self.indicator.get_adx()

        # Test weak trend should give lower ADX
        weak_trend_ohlc = self.generate_trending_ohlc(20, 'sideways')
        weak_indicator = BXTrenderDirectional(period=14)
        weak_intervals = self.create_mock_intervals(weak_trend_ohlc)
        weak_indicator.update(weak_intervals)

        weak_adx = weak_indicator.get_adx()

        # Strong trend should have higher ADX than weak trend
        self.assertIsNotNone(strong_adx)
        self.assertIsNotNone(weak_adx)
        self.assertGreater(strong_adx, weak_adx)

    def test_different_periods(self):
        """Test BX Trender Directional with different periods."""
        ohlc_data = self.generate_trending_ohlc(30, 'up')
        intervals = self.create_mock_intervals(ohlc_data)

        # Test period 7
        indicator_7 = BXTrenderDirectional(period=7)
        indicator_7.update(intervals)

        # Test period 21
        indicator_21 = BXTrenderDirectional(period=21)
        indicator_21.update(intervals)

        # Both should work and detect the uptrend
        self.assertEqual(indicator_7.status, 'ok')
        self.assertEqual(indicator_21.status, 'ok')

        bx_7 = indicator_7.get_value()
        bx_21 = indicator_21.get_value()

        self.assertIsNotNone(bx_7)
        self.assertIsNotNone(bx_21)
        self.assertGreater(bx_7, 50)  # Uptrend
        self.assertGreater(bx_21, 50)  # Uptrend

    def test_reset_values_method(self):
        """Test the _reset_values method."""
        # Set some values first
        self.indicator.latest_bx_trender = 60.0
        self.indicator.di_plus = 30.0
        self.indicator.di_minus = 20.0
        self.indicator.adx = 25.0
        self.indicator.trend_strength = 0.5
        self.indicator.trend_direction = 1

        # Reset values
        self.indicator._reset_values()

        # Check all values are None
        self.assertIsNone(self.indicator.latest_bx_trender)
        self.assertIsNone(self.indicator.di_plus)
        self.assertIsNone(self.indicator.di_minus)
        self.assertIsNone(self.indicator.adx)
        self.assertIsNone(self.indicator.trend_strength)
        self.assertIsNone(self.indicator.trend_direction)

    def test_getter_methods(self):
        """Test all getter methods."""
        ohlc_data = self.generate_trending_ohlc(20, 'up')
        intervals = self.create_mock_intervals(ohlc_data)
        self.indicator.update(intervals)

        # Test all getters return valid values
        self.assertIsNotNone(self.indicator.get_value())
        self.assertIsNotNone(self.indicator.get_di_plus())
        self.assertIsNotNone(self.indicator.get_di_minus())
        self.assertIsNotNone(self.indicator.get_adx())

    def test_calculation_error_handling(self):
        """Test error handling during calculation."""
        ohlc_data = self.generate_trending_ohlc(20, 'up')
        intervals = self.create_mock_intervals(ohlc_data)

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

class TestBXTrenderDirectionalEnhancedFramework(unittest.TestCase):
    """Test BX Trender Directional in the enhanced indicators framework."""

    def setUp(self):
        """Set up test fixtures."""
        self.indicator = BXTrenderIndicator(period=14, variant='directional')

    def create_sample_data(self, periods=20, trend='up'):
        """Create sample DataFrame for testing."""
        np.random.seed(42)
        data = []
        base_price = 100

        for i in range(periods):
            if trend == 'up':
                open_price = base_price + i * 0.5
                close = open_price + 0.3
                high = close + 0.2
                low = open_price - 0.1
            else:  # down
                open_price = base_price - i * 0.5
                close = open_price - 0.3
                high = open_price + 0.1
                low = close - 0.2

            data.append({
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': 100000 + i * 1000,
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })

        return pd.DataFrame(data)

    def test_enhanced_framework_calculation(self):
        """Test calculation through enhanced framework."""
        data = self.create_sample_data(trend='up')
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIn('value', result)
        self.assertIn('bx_trender', result)
        self.assertIn('bx_trender_normalized', result)
        self.assertIn('di_plus', result)
        self.assertIn('di_minus', result)
        self.assertIn('adx', result)
        self.assertIn('trend_strength', result)
        self.assertIn('trend_direction', result)

        # Check uptrend detection
        self.assertGreater(result['di_plus'], result['di_minus'])
        self.assertGreater(result['value'], 50)
        self.assertEqual(result['trend_direction'], 1)

    def test_enhanced_framework_downtrend(self):
        """Test downtrend detection through enhanced framework."""
        data = self.create_sample_data(trend='down')
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertGreater(result['di_minus'], result['di_plus'])
        self.assertLess(result['value'], 50)
        self.assertEqual(result['trend_direction'], -1)

    def test_enhanced_framework_insufficient_data(self):
        """Test enhanced framework with insufficient data."""
        data = self.create_sample_data(periods=5)  # Too little data
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'insufficient_data')
        self.assertIsNone(result['value'])

if __name__ == '__main__':
    unittest.main()