"""
Edge case and error handling tests for BX Trender indicators.
"""
import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch
from datetime import datetime, timedelta
from types import SimpleNamespace

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from domains.trading.services.indicators.enhanced_indicators import BXTrenderIndicator
from domains.trading.signals.indicator import BXTrenderBasic

class TestBXTrenderEdgeCases(unittest.TestCase):
    """Test edge cases and error handling for BX Trender indicators."""

    def test_extreme_price_values(self):
        """Test with extreme price values."""
        # Very large prices
        large_prices = [1000000 + i * 10000 for i in range(20)]
        data = self.create_dataframe_from_prices(large_prices)

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['value'])
        self.assertGreaterEqual(result['value'], 0)
        self.assertLessEqual(result['value'], 100)

        # Very small prices (micro pennies)
        small_prices = [0.0001 + i * 0.00001 for i in range(20)]
        data = self.create_dataframe_from_prices(small_prices)

        result = indicator.calculate(data)
        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['value'])

    def test_nan_and_inf_values(self):
        """Test handling of NaN and Inf values."""
        prices = [100 + i for i in range(20)]
        data = self.create_dataframe_from_prices(prices)

        # Insert NaN values
        data.loc[5, 'close'] = np.nan
        data.loc[10, 'high'] = np.nan

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

        # Test with Inf values
        data = self.create_dataframe_from_prices(prices)
        data.loc[5, 'close'] = np.inf

        result = indicator.calculate(data)
        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_negative_prices(self):
        """Test handling of negative prices."""
        # Mixed positive and negative prices
        prices = [100 - i * 15 for i in range(20)]  # Goes negative after index 6
        data = self.create_dataframe_from_prices(prices)

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_zero_prices(self):
        """Test handling of zero prices."""
        prices = [100] * 10 + [0] * 10  # Half zeros
        data = self.create_dataframe_from_prices(prices)

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_negative_volume(self):
        """Test handling of negative volume."""
        prices = [100 + i for i in range(20)]
        data = self.create_dataframe_from_prices(prices)
        data.loc[5, 'volume'] = -1000  # Negative volume

        # Volume weighted variant should handle this
        indicator = BXTrenderIndicator(period=14, variant='volume_weighted')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_zero_volume(self):
        """Test handling of zero volume."""
        prices = [100 + i for i in range(20)]
        data = self.create_dataframe_from_prices(prices)
        data['volume'] = 0  # All zero volume

        # Volume weighted variant should handle this
        indicator = BXTrenderIndicator(period=14, variant='volume_weighted')
        result = indicator.calculate(data)

        # Should still work with zero volume (might default to basic calculation)
        self.assertIn(result['status'], ['valid', 'invalid_data'])

    def test_missing_ohlc_columns(self):
        """Test handling of missing OHLC columns."""
        data = pd.DataFrame({
            'close': [100 + i for i in range(20)],
            'volume': [10000] * 20,
            'timestamp': [pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i) for i in range(20)]
        })
        # Missing open, high, low columns

        indicator = BXTrenderIndicator(period=14, variant='directional')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_missing_volume_column(self):
        """Test handling of missing volume column for volume-weighted variant."""
        data = pd.DataFrame({
            'open': [100 + i * 0.5 for i in range(20)],
            'high': [101 + i * 0.5 for i in range(20)],
            'low': [99 + i * 0.5 for i in range(20)],
            'close': [100 + i for i in range(20)],
            'timestamp': [pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i) for i in range(20)]
        })
        # Missing volume column

        indicator = BXTrenderIndicator(period=14, variant='volume_weighted')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_inconsistent_ohlc_relationships(self):
        """Test handling of inconsistent OHLC relationships."""
        # Create data where high < low or close outside high/low range
        data = []
        for i in range(20):
            data.append({
                'open': 100 + i,
                'high': 95 + i,  # High less than open/close (invalid)
                'low': 105 + i,  # Low greater than open/close (invalid)
                'close': 100 + i,
                'volume': 10000,
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })

        data = pd.DataFrame(data)

        indicator = BXTrenderIndicator(period=14, variant='directional')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'invalid_data')
        self.assertIsNone(result['value'])

    def test_single_data_point(self):
        """Test handling of single data point."""
        data = pd.DataFrame({
            'open': [100],
            'high': [101],
            'low': [99],
            'close': [100],
            'volume': [10000],
            'timestamp': [pd.Timestamp('2024-01-01')]
        })

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'insufficient_data')
        self.assertIsNone(result['value'])

    def test_exact_minimum_data(self):
        """Test with exact minimum required data points."""
        # Create data with exactly the required period
        prices = [100 + i * 0.5 for i in range(14)]  # Exactly 14 points for period=14
        data = self.create_dataframe_from_prices(prices)

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['value'])

    def test_duplicate_timestamps(self):
        """Test handling of duplicate timestamps."""
        data = []
        base_time = pd.Timestamp('2024-01-01')

        for i in range(20):
            timestamp = base_time if i < 10 else base_time + pd.Timedelta(minutes=1)  # Duplicates
            data.append({
                'open': 100 + i * 0.5,
                'high': 101 + i * 0.5,
                'low': 99 + i * 0.5,
                'close': 100 + i,
                'volume': 10000,
                'timestamp': timestamp
            })

        data = pd.DataFrame(data)

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        # Should handle duplicates gracefully or report invalid data
        self.assertIn(result['status'], ['valid', 'invalid_data'])

    def test_memory_intensive_calculation(self):
        """Test with large datasets to check memory handling."""
        # Create large dataset
        large_size = 10000
        prices = [100 + np.sin(i * 0.1) * 10 for i in range(large_size)]
        data = self.create_dataframe_from_prices(prices)

        indicator = BXTrenderIndicator(period=50, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        self.assertIsNotNone(result['value'])

    def test_calculation_exceptions(self):
        """Test handling of calculation exceptions."""
        prices = [100 + i for i in range(20)]
        data = self.create_dataframe_from_prices(prices)

        # Mock to raise exception during calculation
        with patch('signals.enhanced_indicators.BXTrenderIndicator.calculate') as mock_calc:
            mock_calc.side_effect = Exception("Calculation error")

            indicator = BXTrenderIndicator(period=14, variant='basic')
            result = indicator.calculate(data)
            # If exception is caught, should return error status
            self.assertEqual(result['status'], 'calculation_error')
    def test_framework_indicator_edge_cases(self):
        """Test edge cases in framework indicators."""
        # Test with empty intervals
        basic = BXTrenderBasic(period=14)
        basic.update([])
        self.assertEqual(basic.status, 'insufficient_data')

        # Test with invalid interval status
        intervals = self.create_mock_intervals([100] * 20)
        intervals[5].status = 'invalid'

        basic.update(intervals)
        self.assertEqual(basic.status, 'invalid_data')

    def test_floating_point_precision(self):
        """Test floating point precision issues."""
        # Create prices with very small differences
        base_price = 100.0
        prices = [base_price + i * 1e-10 for i in range(20)]  # Tiny differences
        data = self.create_dataframe_from_prices(prices)

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        self.assertEqual(result['status'], 'valid')
        # Value should be around 50 (no significant movement)
        self.assertGreater(result['value'], 45)
        self.assertLess(result['value'], 55)

    def test_timestamp_ordering(self):
        """Test handling of unordered timestamps."""
        # Create data with mixed timestamp order
        prices = [100 + i for i in range(10)]
        data = []

        for i, price in enumerate(prices):
            # Reverse every other timestamp
            timestamp_offset = -i if i % 2 == 0 else i
            data.append({
                'open': price * 0.99,
                'high': price * 1.01,
                'low': price * 0.98,
                'close': price,
                'volume': 10000,
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=timestamp_offset)
            })

        data = pd.DataFrame(data)

        indicator = BXTrenderIndicator(period=8, variant='basic')
        result = indicator.calculate(data)

        # Should handle or detect ordering issues
        self.assertIn(result['status'], ['valid', 'invalid_data'])

    def create_dataframe_from_prices(self, close_prices):
        """Create DataFrame from close prices."""
        data = []
        for i, close in enumerate(close_prices):
            data.append({
                'open': close * 0.99,
                'high': close * 1.01,
                'low': close * 0.98,
                'close': close,
                'volume': 10000 + i * 100,
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })
        return pd.DataFrame(data)

    def create_mock_intervals(self, close_prices, volumes=None):
        """Create mock InstrumentInterval objects."""
        intervals = []
        base_time = datetime(2024, 1, 1)

        for i, close in enumerate(close_prices):
            interval = SimpleNamespace()
            interval.close = close
            interval.open = close * 0.99
            interval.high = close * 1.01
            interval.low = close * 0.98
            interval.traded_volume = volumes[i] if volumes else 10000
            interval.start_date_time = base_time + timedelta(minutes=i)
            interval.status = 'ok'
            intervals.append(interval)

        return intervals

class TestBXTrenderErrorRecovery(unittest.TestCase):
    """Test error recovery and resilience."""

    def test_partial_data_corruption(self):
        """Test recovery from partial data corruption."""
        # Create good data with some corrupted entries
        prices = [100 + i for i in range(50)]
        data = self.create_dataframe_from_prices(prices)

        # Corrupt some entries
        data.loc[10, 'close'] = np.nan
        data.loc[15, 'volume'] = -1000
        data.loc[20, 'high'] = np.inf

        indicator = BXTrenderIndicator(period=14, variant='basic')
        result = indicator.calculate(data)

        # Should detect corruption
        self.assertEqual(result['status'], 'invalid_data')

    def test_data_type_conversion(self):
        """Test automatic data type conversion."""
        # Create data with string numbers
        data = pd.DataFrame({
            'open': ['99.5'] * 20,
            'high': ['101.0'] * 20,
            'low': ['98.5'] * 20,
            'close': ['100.0'] * 20,
            'volume': ['10000'] * 20,
            'timestamp': [pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i) for i in range(20)]
        })

        indicator = BXTrenderIndicator(period=14, variant='basic')

        # Should either convert or report invalid data
        result = indicator.calculate(data)
        self.assertIn(result['status'], ['valid', 'invalid_data'])
    def test_concurrent_calculation(self):
        """Test thread safety of calculations."""
        import threading

        prices = [100 + i * 0.1 for i in range(100)]
        data = self.create_dataframe_from_prices(prices)

        results = []
        errors = []

        def calculate_indicator():
            indicator = BXTrenderIndicator(period=14, variant='basic')
            result = indicator.calculate(data)
            results.append(result)
        threads = [threading.Thread(target=calculate_indicator) for _ in range(10)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # All calculations should succeed or fail consistently
        if results:
            first_result = results[0]
            for result in results[1:]:
                if first_result['status'] == 'valid' and result['status'] == 'valid':
                    # Results should be identical
                    self.assertAlmostEqual(first_result['value'], result['value'], places=5)

    def create_dataframe_from_prices(self, close_prices):
        """Create DataFrame from close prices."""
        data = []
        for i, close in enumerate(close_prices):
            data.append({
                'open': close * 0.99,
                'high': close * 1.01,
                'low': close * 0.98,
                'close': close,
                'volume': 10000 + i * 100,
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })
        return pd.DataFrame(data)

if __name__ == '__main__':
    unittest.main()