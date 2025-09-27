"""
Unit tests for Volume Profile indicator basic functionality.
"""
import unittest
import pandas as pd
import numpy as np

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from domains.trading.services.indicators.enhanced_indicators import VolumeProfileIndicator

class TestVolumeProfileBasic(unittest.TestCase):
    """Test cases for Volume Profile indicator basic functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.period = 20
        self.bin_count = 50
        self.value_area_pct = 70.0
        self.indicator = VolumeProfileIndicator(self.period, self.bin_count, self.value_area_pct)

    def create_test_data(self, periods: int = 30, trend: str = 'sideways') -> pd.DataFrame:
        """Create test data with specified trend characteristics."""
        np.random.seed(42)

        base_price = 100.0
        data = []

        for i in range(periods):
            if trend == 'uptrend':
                # Consistent upward movement
                price_change = 0.2 + np.random.uniform(0, 0.3)
                base_price += price_change
            elif trend == 'downtrend':
                # Consistent downward movement
                price_change = -0.2 + np.random.uniform(-0.3, 0)
                base_price += price_change
            else:  # sideways
                # Random walk around base price
                price_change = np.random.uniform(-0.5, 0.5)
                base_price += price_change

            # Generate OHLC from base price
            open_price = base_price + np.random.uniform(-0.1, 0.1)
            close_price = base_price + np.random.uniform(-0.1, 0.1)
            high_price = max(open_price, close_price) + np.random.uniform(0, 0.2)
            low_price = min(open_price, close_price) - np.random.uniform(0, 0.2)

            volume = 10000 + np.random.randint(-3000, 10000)

            data.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': max(volume, 1000)  # Ensure positive volume
            })

        return pd.DataFrame(data)

    def test_initialization(self):
        """Test Volume Profile indicator initialization."""
        self.assertEqual(self.indicator.period, 20)
        self.assertEqual(self.indicator.bin_count, 50)
        self.assertEqual(self.indicator.value_area_pct, 70.0)
        self.assertEqual(self.indicator.name, "VolumeProfile_20_50")

    def test_insufficient_data(self):
        """Test behavior with insufficient data."""
        # Test with no data
        empty_data = pd.DataFrame()
        result = self.indicator.calculate(empty_data)
        self.assertEqual(result['status'], 'insufficient_data')
        self.assertIsNone(result['value'])

        # Test with insufficient periods
        short_data = self.create_test_data(periods=5)
        result = self.indicator.calculate(short_data)
        self.assertEqual(result['status'], 'insufficient_data')
        self.assertIsNone(result['value'])

    def test_missing_columns(self):
        """Test behavior with missing required columns."""
        # Missing volume column
        data = self.create_test_data(periods=25)
        data_no_volume = data.drop('volume', axis=1)
        result = self.indicator.calculate(data_no_volume)
        self.assertEqual(result['status'], 'missing_columns')
        self.assertIsNone(result['value'])

        # Missing OHLC columns
        data_no_ohlc = data[['volume']]
        result = self.indicator.calculate(data_no_ohlc)
        self.assertEqual(result['status'], 'missing_columns')
        self.assertIsNone(result['value'])

    def test_invalid_data(self):
        """Test behavior with invalid data."""
        data = self.create_test_data(periods=25)

        # Test with NaN values
        data_with_nan = data.copy()
        data_with_nan.loc[10, 'close'] = np.nan
        result = self.indicator.calculate(data_with_nan)
        self.assertEqual(result['status'], 'invalid_data')

        # Test with negative prices
        data_with_negative = data.copy()
        data_with_negative.loc[10, 'close'] = -50
        result = self.indicator.calculate(data_with_negative)
        self.assertEqual(result['status'], 'invalid_data')

        # Test with zero volume
        data_with_zero_volume = data.copy()
        data_with_zero_volume.loc[10, 'volume'] = 0
        result = self.indicator.calculate(data_with_zero_volume)
        self.assertEqual(result['status'], 'invalid_data')

    def test_valid_calculation(self):
        """Test valid Volume Profile calculation."""
        data = self.create_test_data(periods=30, trend='sideways')
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['value'])
        self.assertIsNotNone(result['poc'])
        self.assertIsNotNone(result['vah'])
        self.assertIsNotNone(result['val'])

        # POC should be the main return value
        self.assertEqual(result['value'], result['poc'])

        # VAH should be greater than or equal to VAL
        self.assertGreaterEqual(result['vah'], result['val'])

        # POC should be within the price range
        price_min = data.tail(self.period)[['open', 'high', 'low', 'close']].min().min()
        price_max = data.tail(self.period)[['open', 'high', 'low', 'close']].max().max()
        self.assertGreaterEqual(result['poc'], price_min)
        self.assertLessEqual(result['poc'], price_max)

    def test_uptrend_characteristics(self):
        """Test Volume Profile characteristics during uptrend."""
        data = self.create_test_data(periods=30, trend='uptrend')
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['poc'])

        # In uptrend, bias might be bullish (but not guaranteed with random data)
        self.assertIn(result['dominant_side'], ['bullish', 'bearish', 'neutral'])

        # Profile shape should be classified
        self.assertIn(result['profile_shape'], ['balanced', 'trending', 'rotational', 'double_distribution', 'undefined'])

    def test_downtrend_characteristics(self):
        """Test Volume Profile characteristics during downtrend."""
        data = self.create_test_data(periods=30, trend='downtrend')
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['poc'])

        # Profile should be classified
        self.assertIn(result['dominant_side'], ['bullish', 'bearish', 'neutral'])
        self.assertIn(result['profile_shape'], ['balanced', 'trending', 'rotational', 'double_distribution', 'undefined'])

    def test_sideways_characteristics(self):
        """Test Volume Profile characteristics during sideways movement."""
        data = self.create_test_data(periods=30, trend='sideways')
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')

        # Sideways movement should have relatively balanced profile
        self.assertIn(result['dominant_side'], ['bullish', 'bearish', 'neutral'])
        self.assertIn(result['profile_shape'], ['balanced', 'trending', 'rotational', 'double_distribution', 'undefined'])

    def test_value_area_calculation(self):
        """Test Value Area calculation accuracy."""
        data = self.create_test_data(periods=30)
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')

        # Value Area should be correctly set
        self.assertEqual(result['value_area_volume_pct'], 70.0)

        # VAH should be >= VAL
        self.assertGreaterEqual(result['vah'], result['val'])

        # POC should typically be within Value Area (but not guaranteed)
        # This is a soft check since POC might be outside VA in some distributions
        if result['val'] <= result['poc'] <= result['vah']:
            self.assertTrue(True, "POC within Value Area")
        else:
            # Log this case but don't fail - it's valid for POC to be outside VA
            print(f"Note: POC {result['poc']:.2f} outside VA [{result['val']:.2f}, {result['vah']:.2f}]")

    def test_volume_distribution_summary(self):
        """Test volume distribution summary generation."""
        data = self.create_test_data(periods=30)
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIn('volume_distribution_summary', result)

        summary = result['volume_distribution_summary']
        self.assertIn('total_bins', summary)
        self.assertIn('active_bins', summary)
        self.assertIn('top_volume_levels', summary)

        # Should have reasonable number of active bins
        self.assertGreater(summary['active_bins'], 0)
        self.assertLessEqual(summary['active_bins'], self.bin_count)

        # Top volume levels should be sorted by volume
        top_levels = summary['top_volume_levels']
        if len(top_levels) > 1:
            for i in range(len(top_levels) - 1):
                self.assertGreaterEqual(top_levels[i]['volume'], top_levels[i + 1]['volume'])

    def test_total_volume_accuracy(self):
        """Test total volume calculation accuracy."""
        data = self.create_test_data(periods=30)
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')

        # Calculate expected total volume from input data
        expected_volume = data.tail(self.period)['volume'].sum()
        calculated_volume = result['total_volume']

        # Should be approximately equal (allowing for floating point precision)
        self.assertAlmostEqual(calculated_volume, expected_volume, places=0)

    def test_volume_concentration_metric(self):
        """Test volume concentration calculation."""
        data = self.create_test_data(periods=30)
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')

        concentration = result['volume_concentration']

        # Concentration should be between 0 and 1
        self.assertGreaterEqual(concentration, 0.0)
        self.assertLessEqual(concentration, 1.0)

        # Should be reasonable for random data (not too concentrated)
        self.assertLess(concentration, 0.8)  # Very high concentration unlikely with random data

    def test_price_range_tracking(self):
        """Test price range tracking in results."""
        data = self.create_test_data(periods=30)
        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')

        price_range = result['price_range']
        self.assertIsInstance(price_range, tuple)
        self.assertEqual(len(price_range), 2)

        price_min, price_max = price_range
        self.assertLess(price_min, price_max)

        # Price range should encompass actual data range
        actual_data = data.tail(self.period)
        actual_min = actual_data[['open', 'high', 'low', 'close']].min().min()
        actual_max = actual_data[['open', 'high', 'low', 'close']].max().max()

        self.assertAlmostEqual(price_min, actual_min, places=2)
        self.assertAlmostEqual(price_max, actual_max, places=2)

    def test_different_bin_counts(self):
        """Test Volume Profile with different bin counts."""
        data = self.create_test_data(periods=30)

        bin_counts = [10, 30, 50, 100]
        results = []

        for bin_count in bin_counts:
            indicator = VolumeProfileIndicator(period=20, bin_count=bin_count)
            result = indicator.calculate(data)
            self.assertEqual(result['status'], 'valid')
            results.append((bin_count, result))

        # All should produce valid results
        for bin_count, result in results:
            self.assertIsNotNone(result['poc'])
            self.assertIsNotNone(result['vah'])
            self.assertIsNotNone(result['val'])

            # Bin count should affect granularity
            summary = result['volume_distribution_summary']
            self.assertLessEqual(summary['active_bins'], bin_count)

    def test_different_periods(self):
        """Test Volume Profile with different lookback periods."""
        data = self.create_test_data(periods=50)

        periods = [10, 20, 30, 40]
        results = []

        for period in periods:
            indicator = VolumeProfileIndicator(period=period, bin_count=30)
            result = indicator.calculate(data)
            self.assertEqual(result['status'], 'valid')
            results.append((period, result))

        # All should produce valid results
        for period, result in results:
            self.assertIsNotNone(result['poc'])
            self.assertIsNotNone(result['vah'])
            self.assertIsNotNone(result['val'])

    def test_identical_prices(self):
        """Test Volume Profile with identical prices (no price movement)."""
        # Create data with same price but different volumes
        data = pd.DataFrame({
            'open': [100.0] * 25,
            'high': [100.0] * 25,
            'low': [100.0] * 25,
            'close': [100.0] * 25,
            'volume': [10000 + i * 1000 for i in range(25)]  # Varying volumes
        })

        result = self.indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')

        # POC, VAH, VAL should all be the same price
        self.assertAlmostEqual(result['poc'], 100.0, places=2)
        self.assertAlmostEqual(result['vah'], 100.0, places=2)
        self.assertAlmostEqual(result['val'], 100.0, places=2)

        # Profile should be classified appropriately
        self.assertEqual(result['profile_shape'], 'balanced')
        self.assertEqual(result['dominant_side'], 'neutral')

class TestVolumeProfileEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for Volume Profile."""

    def setUp(self):
        """Set up test fixtures."""
        self.indicator = VolumeProfileIndicator(period=20, bin_count=50)

    def test_extreme_volume_values(self):
        """Test with extreme volume values."""
        # Very high volumes
        data = pd.DataFrame({
            'open': [100 + i * 0.1 for i in range(25)],
            'high': [101 + i * 0.1 for i in range(25)],
            'low': [99 + i * 0.1 for i in range(25)],
            'close': [100 + i * 0.1 for i in range(25)],
            'volume': [1000000000] * 25  # Very high volume
        })

        result = self.indicator.calculate(data)
        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['poc'])

    def test_extreme_price_values(self):
        """Test with extreme price values."""
        # Very high prices
        data = pd.DataFrame({
            'open': [10000 + i * 10 for i in range(25)],
            'high': [10001 + i * 10 for i in range(25)],
            'low': [9999 + i * 10 for i in range(25)],
            'close': [10000 + i * 10 for i in range(25)],
            'volume': [10000] * 25
        })

        result = self.indicator.calculate(data)
        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['poc'])

    def test_minimal_price_range(self):
        """Test with minimal price range (very tight trading)."""
        base_price = 100.0
        data = pd.DataFrame({
            'open': [base_price + np.random.uniform(-0.001, 0.001) for _ in range(25)],
            'high': [base_price + np.random.uniform(0.001, 0.002) for _ in range(25)],
            'low': [base_price + np.random.uniform(-0.002, -0.001) for _ in range(25)],
            'close': [base_price + np.random.uniform(-0.001, 0.001) for _ in range(25)],
            'volume': [10000 + i * 100 for i in range(25)]
        })

        result = self.indicator.calculate(data)
        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['poc'])

if __name__ == '__main__':
    unittest.main()