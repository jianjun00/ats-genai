#!/usr/bin/env python3
"""
Simple test of hourly aggregation without Gin configuration dependencies.

Tests the core aggregation logic directly without complex configuration.
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

# Import directly without Gin configuration
from domains.ml.services.training_data.runners.training_data_callback_runner import TrainingDataJobRunner


class SimpleTrainingDataJobConfig:
    """Simple config class without Gin dependencies."""
    def __init__(self):
        self.job_name = "test_simple"
        self.symbols = ['AAPL']
        self.start_date = datetime(2025, 8, 4).date()
        self.end_date = datetime(2025, 8, 4).date()
        self.base_interval_minutes = 1
        self.training_interval_minutes = 60
        self.output_structure = "hourly_rows"
        self.use_universe_state_indicators = True
        self.normalize_features = False


class TestSimpleHourlyAggregation(unittest.TestCase):
    """Simple test of hourly aggregation logic."""

    def setUp(self):
        """Set up without Gin configuration."""
        # Create runner manually
        self.runner = TrainingDataJobRunner.__new__(TrainingDataJobRunner)
        self.runner.config = SimpleTrainingDataJobConfig()
        self.runner.run_id = 123
        self.runner.env = None

    def create_test_minute_data(self):
        """Create realistic test minute data."""
        start_time = datetime(2025, 8, 4, 9, 30)
        minute_data = []

        base_price = 200.0

        for minute in range(120):  # 2 hours of data
            timestamp = start_time + timedelta(minutes=minute)

            price_change = np.random.normal(0, 0.2)
            current_price = base_price + price_change

            minute_bar = {
                'datetime': timestamp,
                'open': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                'high': round(current_price + np.random.uniform(0.1, 0.4), 2),
                'low': round(current_price - np.random.uniform(0.1, 0.4), 2),
                'close': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                'volume': np.random.randint(100, 1000)
            }

            # Ensure OHLC logic
            minute_bar['high'] = max(minute_bar['high'], minute_bar['open'], minute_bar['close'])
            minute_bar['low'] = min(minute_bar['low'], minute_bar['open'], minute_bar['close'])

            minute_data.append(minute_bar)
            base_price = minute_bar['close']

        return pd.DataFrame(minute_data)

    def test_basic_hourly_aggregation(self):
        """Test basic hourly aggregation functionality."""
        minute_data = self.create_test_minute_data()

        # Test aggregation without universe state manager
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )

        # Should create 2 hourly rows
        self.assertEqual(len(hourly_rows), 2)

        # Check first hour structure
        first_hour = hourly_rows[0]
        required_fields = [
            'datetime', 'symbol', 'hour_open', 'hour_high', 'hour_low',
            'hour_close', 'hour_volume', 'market_period', 'day_progress'
        ]

        for field in required_fields:
            self.assertIn(field, first_hour, f"Missing field: {field}")

        # Verify OHLC aggregation logic
        first_hour_data = minute_data[minute_data['datetime'].dt.hour == 9]

        expected_open = first_hour_data['open'].iloc[0]
        expected_high = first_hour_data['high'].max()
        expected_low = first_hour_data['low'].min()
        expected_close = first_hour_data['close'].iloc[-1]
        expected_volume = first_hour_data['volume'].sum()

        self.assertEqual(first_hour['hour_open'], expected_open)
        self.assertEqual(first_hour['hour_high'], expected_high)
        self.assertEqual(first_hour['hour_low'], expected_low)
        self.assertEqual(first_hour['hour_close'], expected_close)
        self.assertEqual(first_hour['hour_volume'], expected_volume)

        print(f"✅ Basic aggregation test passed")
        print(f"   Generated {len(hourly_rows)} hourly rows from {len(minute_data)} minute bars")
        print(f"   First hour OHLCV: {expected_open}/{expected_high}/{expected_low}/{expected_close}")

    def test_aggregation_with_universe_indicators(self):
        """Test aggregation with mocked universe state indicators."""
        minute_data = self.create_test_minute_data()

        # Mock universe state manager
        from unittest.mock import Mock
        mock_universe_manager = Mock()
        mock_universe_manager.get_indicators_for_hour.return_value = {
            'envelope_top': 205.5,
            'envelope_bot': 194.8,
            'pldot': -0.0032,
            'oneone_high': 204.2,
            'oneone_low': 195.8,
            'z1b': 2.8,
            'z2b': 5.2,
            'z5t': 3.1,
            'z6t': 4.4
        }

        # Test aggregation with universe state indicators
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=mock_universe_manager
        )

        self.assertEqual(len(hourly_rows), 2)

        # Check that universe state indicators are present
        first_hour = hourly_rows[0]
        universe_indicators = [
            'hour_envelope_top', 'hour_envelope_bot', 'hour_pldot',
            'hour_oneone_high', 'hour_oneone_low',
            'hour_z1b', 'hour_z2b', 'hour_z5t', 'hour_z6t'
        ]

        for indicator in universe_indicators:
            self.assertIn(indicator, first_hour, f"Missing universe indicator: {indicator}")

        # Verify indicator values
        self.assertEqual(first_hour['hour_envelope_top'], 205.5)
        self.assertEqual(first_hour['hour_envelope_bot'], 194.8)
        self.assertEqual(first_hour['hour_pldot'], -0.0032)

        print(f"✅ Universe state indicators test passed")
        print(f"   Envelope range: {first_hour['hour_envelope_bot']:.2f} - {first_hour['hour_envelope_top']:.2f}")
        print(f"   PL Dot: {first_hour['hour_pldot']:.4f}")

    def test_data_quality_validation(self):
        """Test data quality and validation."""
        minute_data = self.create_test_minute_data()

        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )

        for i, hour_row in enumerate(hourly_rows):
            # Test OHLC logic
            self.assertLessEqual(
                hour_row['hour_low'], hour_row['hour_open'],
                f"Hour {i}: Low should be <= Open"
            )
            self.assertLessEqual(
                hour_row['hour_low'], hour_row['hour_close'],
                f"Hour {i}: Low should be <= Close"
            )
            self.assertGreaterEqual(
                hour_row['hour_high'], hour_row['hour_open'],
                f"Hour {i}: High should be >= Open"
            )
            self.assertGreaterEqual(
                hour_row['hour_high'], hour_row['hour_close'],
                f"Hour {i}: High should be >= Close"
            )

            # Test volume is positive
            self.assertGreater(
                hour_row['hour_volume'], 0,
                f"Hour {i}: Volume should be positive"
            )

            # Test day progress is valid
            self.assertGreaterEqual(
                hour_row['day_progress'], 0.0,
                f"Hour {i}: Day progress should be >= 0.0"
            )
            self.assertLessEqual(
                hour_row['day_progress'], 1.0,
                f"Hour {i}: Day progress should be <= 1.0"
            )

            # Test symbol consistency
            self.assertEqual(
                hour_row['symbol'], 'AAPL',
                f"Hour {i}: Symbol should be AAPL"
            )

        print(f"✅ Data quality validation passed")
        print(f"   All {len(hourly_rows)} hourly rows have valid OHLC, volume, and metadata")

    def test_empty_data_handling(self):
        """Test handling of empty minute data."""
        empty_data = pd.DataFrame()

        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            empty_data, 'AAPL', universe_manager=None
        )

        self.assertEqual(len(hourly_rows), 0, "Empty data should produce no hourly rows")
        print(f"✅ Empty data handling test passed")


def main():
    """Run simple aggregation tests."""
    print("🧪 Simple Hourly Aggregation Tests (No Gin Configuration)")
    print("=" * 60)

    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSimpleHourlyAggregation)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("🎉 ALL SIMPLE TESTS PASSED!")
        print("\n✅ Core hourly aggregation functionality is working correctly:")
        print("   • Basic OHLCV aggregation from minute data ✓")
        print("   • Universe state builder indicator integration ✓")
        print("   • Data quality validation ✓")
        print("   • Edge case handling (empty data) ✓")
        print("\n🚀 Hourly training data generation core logic is functional!")
    else:
        print("❌ SOME TESTS FAILED!")
        print("   Please review failed tests before proceeding.")

    return result.wasSuccessful()


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)