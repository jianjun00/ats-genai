#!/usr/bin/env python3
"""
End-to-end test of hourly training data generation with real test data.

This test creates actual minute-level data files and tests the complete pipeline.
"""

import unittest
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os
import tempfile
import shutil
from unittest.mock import Mock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

# TrainingDataJobRunner class does not exist in feature_extraction_runner, TrainingDataJobConfig
from domains.trading.services.core.minute.file_based_minute_service import FileBasedMinuteManager


class TestHourlyGenerationWithRealData(unittest.TestCase):
    """Test complete hourly generation pipeline with real test data."""

    @classmethod
    def setUpClass(cls):
        """Create temporary directory and test data files."""
        cls.test_data_dir = tempfile.mkdtemp(prefix='hourly_real_test_')
        cls.minute_data_path = Path(cls.test_data_dir) / 'minute-files'
        cls.addClassCleanup(shutil.rmtree, cls.test_data_dir)

        # Create comprehensive test data
        cls._create_comprehensive_test_data()

    @classmethod
    def _create_comprehensive_test_data(cls):
        """Create realistic minute-level test data files."""
        print(f"Creating test data in: {cls.minute_data_path}")

        symbols = ['AAPL', 'MSFT']
        start_date = datetime(2025, 8, 4)  # Monday
        end_date = datetime(2025, 8, 8)    # Friday

        for symbol in symbols:
            current_date = start_date
            base_price = 200.0 if symbol == 'AAPL' else 300.0

            while current_date <= end_date:
                # Skip weekends
                if current_date.weekday() >= 5:
                    current_date += timedelta(days=1)
                    continue

                year = current_date.year
                month = current_date.month

                # Create directory structure: symbol/year/month/
                symbol_dir = cls.minute_data_path / symbol / str(year) / f"{month:02d}"
                symbol_dir.mkdir(parents=True, exist_ok=True)

                # Generate realistic trading day data
                minute_data = []
                market_open = current_date.replace(hour=9, minute=30, second=0, microsecond=0)

                daily_trend = np.random.normal(0, 0.5)  # Daily trend

                for minute in range(390):  # 6.5 hours = 390 minutes
                    timestamp = market_open + timedelta(minutes=minute)

                    # Create realistic intraday price movement
                    time_factor = minute / 390.0  # 0.0 to 1.0 through the day

                    # Higher volatility at open and close
                    if time_factor < 0.1 or time_factor > 0.9:
                        volatility_factor = 1.5
                    else:
                        volatility_factor = 1.0

                    price_change = (
                        daily_trend * time_factor +  # Daily trend
                        np.random.normal(0, 0.3 * volatility_factor)  # Random movement
                    )

                    current_price = base_price + price_change

                    # Generate OHLCV
                    minute_volatility = np.random.uniform(0.1, 0.8) * volatility_factor

                    minute_open = current_price + np.random.uniform(-0.2, 0.2)
                    minute_high = current_price + minute_volatility * np.random.uniform(0.1, 0.6)
                    minute_low = current_price - minute_volatility * np.random.uniform(0.1, 0.6)
                    minute_close = current_price + np.random.uniform(-0.2, 0.2)

                    # Ensure OHLC logic
                    minute_high = max(minute_high, minute_open, minute_close)
                    minute_low = min(minute_low, minute_open, minute_close)

                    # Generate realistic volume (higher at open/close)
                    base_volume = np.random.randint(200, 1500)
                    if time_factor < 0.1:  # Opening hour
                        volume = int(base_volume * np.random.uniform(2.0, 4.0))
                    elif time_factor > 0.85:  # Closing period
                        volume = int(base_volume * np.random.uniform(1.5, 2.5))
                    else:  # Regular hours
                        volume = int(base_volume * np.random.uniform(0.7, 1.3))

                    minute_bar = {
                        'timestamp': timestamp,
                        'open': round(minute_open, 2),
                        'high': round(minute_high, 2),
                        'low': round(minute_low, 2),
                        'close': round(minute_close, 2),
                        'volume': volume,
                        'vwap': round((minute_high + minute_low + minute_close) / 3, 2),
                        'trade_count': np.random.randint(20, 150),
                        'vendor': 'test_comprehensive',
                        'quality_score': 1.0
                    }

                    minute_data.append(minute_bar)
                    base_price = minute_close  # Carry price forward

                # Save daily data to monthly file (append if exists)
                if minute_data:
                    df = pd.DataFrame(minute_data)
                    df.set_index('timestamp', inplace=True)

                    file_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"

                    if file_path.exists():
                        # Append to existing file
                        existing_df = pd.read_parquet(file_path)
                        combined_df = pd.concat([existing_df, df]).sort_index()
                        combined_df.to_parquet(file_path, engine='pyarrow')
                    else:
                        # Create new file
                        df.to_parquet(file_path, engine='pyarrow')

                current_date += timedelta(days=1)

        print(f"Created test data files:")
        for parquet_file in sorted(cls.minute_data_path.rglob("*.parquet")):
            relative_path = parquet_file.relative_to(cls.minute_data_path)
            df = pd.read_parquet(parquet_file)
            print(f"  {relative_path}: {len(df)} minute bars")

    def setUp(self):
        """Set up each test."""
        self.config = TrainingDataJobConfig(
            job_name="real_data_test",
            symbols=['AAPL'],
            start_date=datetime(2025, 8, 4).date(),
            end_date=datetime(2025, 8, 6).date(),
            base_interval_minutes=1,
            training_interval_minutes=60,
            output_structure="hourly_rows",
            use_universe_state_indicators=True,
            normalize_features=False
        )

        # Mock environment
        self.mock_env = Mock()
        self.mock_env.get_database_url.return_value = "postgresql://test"

        self.runner = TrainingDataJobRunner(self.mock_env)
        self.runner.config = self.config
        self.runner.run_id = 555

    @pytest.mark.asyncio

    async def test_end_to_end_with_real_minute_data(self):
        """Test complete pipeline with real minute data files."""

        # Create FileBasedMinuteManager with our test data
        minute_manager = FileBasedMinuteManager(base_path=str(self.minute_data_path))

        # Retrieve minute data (this tests the FileBasedMinuteManager)
        minute_data = await minute_manager.get_minute_data(
            symbol='AAPL',
            start_date=self.config.start_date,
            end_date=self.config.end_date
        )

        # Verify we got data
        self.assertIsNotNone(minute_data, "Should retrieve minute data from test files")
        self.assertGreater(len(minute_data), 0, "Should have minute data records")

        print(f"Retrieved {len(minute_data)} minute data points for AAPL")
        print(f"Date range: {minute_data.index.min()} to {minute_data.index.max()}")

        # Test hourly aggregation directly
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )

        self.assertGreater(len(hourly_rows), 0, "Should generate hourly rows from minute data")

        print(f"Generated {len(hourly_rows)} hourly rows")

        # Verify hourly row structure
        first_hour = hourly_rows[0]
        required_fields = [
            'datetime', 'symbol', 'hour_open', 'hour_high', 'hour_low',
            'hour_close', 'hour_volume', 'market_period', 'day_progress'
        ]

        for field in required_fields:
            self.assertIn(field, first_hour, f"Missing required field: {field}")

        # Verify data quality
        for hour_row in hourly_rows:
            # Test OHLC logic
            self.assertLessEqual(hour_row['hour_low'], hour_row['hour_open'])
            self.assertLessEqual(hour_row['hour_low'], hour_row['hour_close'])
            self.assertGreaterEqual(hour_row['hour_high'], hour_row['hour_open'])
            self.assertGreaterEqual(hour_row['hour_high'], hour_row['hour_close'])

            # Test volume is positive
            self.assertGreater(hour_row['hour_volume'], 0)

            # Test symbol consistency
            self.assertEqual(hour_row['symbol'], 'AAPL')

    @pytest.mark.asyncio

    async def test_multiple_symbols_real_data(self):
        """Test with multiple symbols using real data."""

        self.config.symbols = ['AAPL', 'MSFT']

        minute_manager = FileBasedMinuteManager(base_path=str(self.minute_data_path))

        for symbol in self.config.symbols:
            minute_data = await minute_manager.get_minute_data(
                symbol=symbol,
                start_date=self.config.start_date,
                end_date=self.config.end_date
            )

            if minute_data is not None and not minute_data.empty:
                hourly_rows = self.runner._aggregate_minutes_to_hourly(
                    minute_data, symbol, universe_manager=None
                )

                self.assertGreater(len(hourly_rows), 0, f"Should generate hourly rows for {symbol}")

                # Verify all rows have correct symbol
                for row in hourly_rows:
                    self.assertEqual(row['symbol'], symbol)

                print(f"{symbol}: {len(minute_data)} minutes -> {len(hourly_rows)} hours")

    @pytest.mark.asyncio

    async def test_hourly_aggregation_accuracy(self):
        """Test accuracy of hourly OHLCV aggregation from minute data."""

        minute_manager = FileBasedMinuteManager(base_path=str(self.minute_data_path))

        minute_data = await minute_manager.get_minute_data(
            symbol='AAPL',
            start_date=datetime(2025, 8, 4).date(),
            end_date=datetime(2025, 8, 4).date()  # Single day for precise testing
        )

        if minute_data is not None and not minute_data.empty:
            # Add datetime column from index for aggregation
            minute_data_with_dt = minute_data.reset_index()
            minute_data_with_dt.rename(columns={'timestamp': 'datetime'}, inplace=True)

            hourly_rows = self.runner._aggregate_minutes_to_hourly(
                minute_data_with_dt, 'AAPL', universe_manager=None
            )

            if len(hourly_rows) > 0:
                # Verify first hour aggregation manually
                first_hour_dt = hourly_rows[0]['datetime']
                first_hour_start = first_hour_dt.replace(minute=0, second=0, microsecond=0)
                first_hour_end = first_hour_start + timedelta(hours=1)

                # Get corresponding minute data
                hour_minute_data = minute_data_with_dt[
                    (minute_data_with_dt['datetime'] >= first_hour_start) &
                    (minute_data_with_dt['datetime'] < first_hour_end)
                ]

                if len(hour_minute_data) > 0:
                    # Calculate expected OHLCV
                    expected_open = hour_minute_data['open'].iloc[0]
                    expected_high = hour_minute_data['high'].max()
                    expected_low = hour_minute_data['low'].min()
                    expected_close = hour_minute_data['close'].iloc[-1]
                    expected_volume = hour_minute_data['volume'].sum()

                    # Compare with aggregated values
                    first_hour = hourly_rows[0]
                    self.assertEqual(first_hour['hour_open'], expected_open)
                    self.assertEqual(first_hour['hour_high'], expected_high)
                    self.assertEqual(first_hour['hour_low'], expected_low)
                    self.assertEqual(first_hour['hour_close'], expected_close)
                    self.assertEqual(first_hour['hour_volume'], expected_volume)

                    print(f"✅ Hourly aggregation accuracy verified")
                    print(f"   Minutes: {len(hour_minute_data)} -> Hour OHLCV: {expected_open}/{expected_high}/{expected_low}/{expected_close}")

    def test_real_data_file_structure(self):
        """Test that our test data files match expected structure."""

        # Verify file structure exists
        expected_files = [
            'AAPL/2025/08/AAPL_2025_08.parquet',
            'MSFT/2025/08/MSFT_2025_08.parquet'
        ]

        for expected_file in expected_files:
            file_path = self.minute_data_path / expected_file
            self.assertTrue(file_path.exists(), f"Expected test data file: {expected_file}")

            # Verify file contains data
            df = pd.read_parquet(file_path)
            self.assertGreater(len(df), 0, f"File should contain data: {expected_file}")

            # Verify required columns
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                self.assertIn(col, df.columns, f"Missing column {col} in {expected_file}")


def run_async_tests():
    """Run async tests using asyncio."""

    class AsyncTestRunner:
        def __init__(self):
            self.test_instance = TestHourlyGenerationWithRealData()

        async def run_all_tests(self):
            """Run all async tests."""

            print("🧪 Running End-to-End Hourly Training Data Tests with Real Data")
            print("=" * 70)

            # Set up test class
            TestHourlyGenerationWithRealData.setUpClass()

            # Run tests
            test_methods = [
                'test_end_to_end_with_real_minute_data',
                'test_multiple_symbols_real_data',
                'test_hourly_aggregation_accuracy'
            ]

            for test_method in test_methods:
                print(f"\n📋 Running {test_method}...")
                self.test_instance.setUp()

                await getattr(self.test_instance, test_method)()
                print(f"✅ {test_method} PASSED")
            print(f"\n📋 Running test_real_data_file_structure...")
            self.test_instance.setUp()
            self.test_instance.test_real_data_file_structure()
            print(f"✅ test_real_data_file_structure PASSED")

            print(f"\n🎉 All tests PASSED!")

    runner = AsyncTestRunner()
    asyncio.run(runner.run_all_tests())


if __name__ == '__main__':
    run_async_tests()