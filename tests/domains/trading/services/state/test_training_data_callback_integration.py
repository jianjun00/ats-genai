"""
Integration tests for DateBasedTrainingDataCallback with real test environment.

Tests the pure callback implementation with actual test data setup,
following the indicator_runner test pattern.
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from datetime import datetime

from tests.fixtures.insert_test_daily_prices import insert_test_daily_prices
from tests.fixtures.insert_test_daily_price_polygon import insert_test_daily_price_polygon
from tests.fixtures.setup_test_universe_data import setup_test_universe_data

from state.training_data_callback import DateBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from core.shared.utils.environment import Environment, EnvironmentType
from state.universe_state_manager import UniverseStateManager

class TestTrainingDataCallback:
    """Test DateBasedTrainingDataCallback with real test data."""

    def test_callback_file_lifecycle_with_test_data(self, unit_test_db, setup_test_universe_data):
        """
        Test the exact file lifecycle as requested:
        - handleStartOfDay tells current date, opens dated file
        - handleInterval appends records to dated file
        - handleEndOfDay closes dated file

        Uses real test data setup from fixtures.
        """
        # Insert test daily prices for AAPL
        insert_test_daily_price_polygon(unit_test_db, ['AAPL'], '2024-01-15', '2024-01-17')

        config = TrainingDataConfig(
            base_interval_minutes=1,
            training_interval_minutes=60,
            sequence_lengths={'5m': 2, '15m': 2, '1h': 2, '1d': 2},  # Small for testing
            prediction_horizons={'1h': 1, '1d': 1}
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create callback with test environment
            callback = DateBasedTrainingDataCallback(
                symbols=['AAPL'],
                config=config,
                output_dir=tmp_dir,
                save_format="pickle"
            )

            # Mock runner with test environment
            class TestRunner:
                def get_environment(self):
                    return Environment(unit_test_db, env_type=EnvironmentType.TEST)

                def get_universe_state_manager(self):
                    return UniverseStateManager(env=self.get_environment())

            runner = TestRunner()

            # Test Day 1: January 15, 2024
            print("\n🌅 Testing Day 1: 2024-01-15")

            # Initialize callback
            start_time = datetime(2024, 1, 15, 8, 0, 0)
            callback.handleStart(runner, start_time)
            assert callback.training_generator is not None

            # 1. handleStartOfDay - tells current date, opens dated file
            sod_time = datetime(2024, 1, 15, 9, 0, 0)
            callback.handleStartOfDay(runner, sod_time)

            assert callback.current_date == sod_time.date()
            assert len(callback.daily_examples) == 0
            assert callback.daily_stats['date'] == '2024-01-15'
            print(f"   ✅ handleStartOfDay: Current date = {callback.current_date}")
            print(f"   ✅ Opened dated file collection for: {callback.current_date}")

            # 2. handleInterval - append records to dated file (multiple intervals)
            interval_times = [
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 11, 0, 0),
                datetime(2024, 1, 15, 12, 0, 0),
            ]

            for i, interval_time in enumerate(interval_times, 1):
                examples_before = len(callback.daily_examples)

                asyncio.run(callback.handleInterval(runner, interval_time))

                examples_after = len(callback.daily_examples)
                intervals_processed = callback.daily_stats.get('intervals_processed', 0)

                print(f"   ✅ Interval {i}: {examples_after - examples_before} records appended")
                print(f"      Total in dated file: {examples_after} examples")
                print(f"      Intervals processed: {intervals_processed}")

                assert intervals_processed == i

            # 3. handleEndOfDay - close dated file
            eod_time = datetime(2024, 1, 15, 16, 0, 0)
            asyncio.run(callback.handleEndOfDay(runner, eod_time))

            assert callback.current_date is None  # Cleared after EOD
            assert len(callback.daily_examples) == 0  # Cleared after EOD
            assert callback.total_days == 1
            print(f"   ✅ handleEndOfDay: Closed and saved dated file")
            print(f"   ✅ Daily data cleared: current_date={callback.current_date}")

            # Verify Day 1 files were created
            output_path = Path(tmp_dir)
            daily_20240115 = output_path / "daily" / "20240115"
            metadata_20240115 = output_path / "metadata" / "daily_stats_20240115.json"

            assert (output_path / "daily").exists()
            assert (output_path / "metadata").exists()
            assert metadata_20240115.exists()
            print(f"   ✅ Files created: daily/{metadata_20240115.parent.name}/, metadata/{metadata_20240115.name}")

            # Test Day 2: January 16, 2024 (NEW dated file)
            print("\n🌅 Testing Day 2: 2024-01-16 (NEW dated file)")

            # 1. handleStartOfDay for NEW date - opens NEW dated file
            sod_time_day2 = datetime(2024, 1, 16, 9, 0, 0)
            callback.handleStartOfDay(runner, sod_time_day2)

            assert callback.current_date == sod_time_day2.date()
            assert len(callback.daily_examples) == 0  # Reset for new day
            print(f"   ✅ handleStartOfDay: NEW current date = {callback.current_date}")
            print(f"   ✅ Opened NEW dated file collection")

            # 2. handleInterval for NEW date - append to NEW dated file
            interval_day2 = datetime(2024, 1, 16, 10, 0, 0)
            asyncio.run(callback.handleInterval(runner, interval_day2))

            intervals_processed_day2 = callback.daily_stats.get('intervals_processed', 0)
            print(f"   ✅ handleInterval: Appended to NEW dated file")
            print(f"      Examples in NEW file: {len(callback.daily_examples)}")
            print(f"      Intervals processed: {intervals_processed_day2}")

            # 3. handleEndOfDay for NEW date - close NEW dated file
            eod_time_day2 = datetime(2024, 1, 16, 16, 0, 0)
            asyncio.run(callback.handleEndOfDay(runner, eod_time_day2))

            assert callback.total_days == 2  # Both days processed
            print(f"   ✅ handleEndOfDay: Closed NEW dated file")
            print(f"   ✅ Total days processed: {callback.total_days}")

            # Verify Day 2 files were created
            daily_20240116 = output_path / "daily" / "20240116"
            metadata_20240116 = output_path / "metadata" / "daily_stats_20240116.json"

            assert metadata_20240116.exists()
            print(f"   ✅ Files created: daily/20240116/, metadata/{metadata_20240116.name}")

            # Final verification: Both days exist independently
            print(f"\n📂 Final Verification: Both days exist independently")
            print(f"   Day 1 metadata: {metadata_20240115.exists()}")
            print(f"   Day 2 metadata: {metadata_20240116.exists()}")
            print(f"   Total days processed: {callback.total_days}")
            print(f"   Total examples: {callback.total_examples}")

            assert metadata_20240115.exists()
            assert metadata_20240116.exists()
            assert callback.total_days == 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_callback_with_real_runner_framework(self, unit_test_db, setup_test_universe_data):
        """
        Test pure callback approach with actual Runner framework.

        This is the CORRECT way - no TrainingDataRunner class,
        just DateBasedTrainingDataCallback with existing Runner.
        """
        from app.runner import Runner

        # Insert test data
        insert_test_daily_price_polygon(unit_test_db, ['AAPL'], '2024-01-15', '2024-01-15')

        config = TrainingDataConfig(
            sequence_lengths={'5m': 1, '15m': 1, '1h': 1, '1d': 1},
            prediction_horizons={'1h': 1, '1d': 1}
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            # ✅ Create ONLY the callback - no TrainingDataRunner class
            training_callback = DateBasedTrainingDataCallback(
                symbols=['AAPL'],
                config=config,
                output_dir=tmp_dir,
                save_format="pickle"
            )

            # ✅ Use existing Runner framework with callback
            runner = Runner(
                start_date="2024-01-15",
                end_date="2024-01-15",
                environment=Environment(unit_test_db, env_type=EnvironmentType.TEST),
                universe_id=1,
                callbacks=[training_callback],  # ✅ ONLY the callback
                base_duration="1d"
            )

            print(f"\n🚀 Testing pure callback with real Runner framework")
            print(f"   Callback: {type(training_callback).__name__}")
            print(f"   ❌ NO TrainingDataRunner class created")
            print(f"   ✅ Using existing Runner + callback")

            # ✅ Run using existing framework
            await runner.run()

            print(f"   ✅ Pure callback execution completed")

            # Verify callback was used and files created
            output_path = Path(tmp_dir)
            daily_dir = output_path / "daily"
            metadata_dir = output_path / "metadata"

            assert daily_dir.exists()
            assert metadata_dir.exists()
            assert training_callback.total_days >= 1

            metadata_files = list(metadata_dir.glob("*.json"))
            print(f"   ✅ Files created: {len(metadata_files)} metadata files")

            # Verify this was pure callback (no runner class)
            assert isinstance(training_callback, DateBasedTrainingDataCallback)
            assert not hasattr(training_callback, 'generate_training_data')  # Not a runner class
            assert hasattr(training_callback, 'handleInterval')  # Is a callback

            print(f"   ✅ Verified: Pure callback implementation")
            print(f"   ✅ Total days processed: {training_callback.total_days}")
            print(f"   ✅ Total examples: {training_callback.total_examples}")

    def test_multi_day_file_separation(self, unit_test_db, setup_test_universe_data):
        """
        Test that each day gets its own file as requested.
        """
        # Insert test data for multiple days
        insert_test_daily_price_polygon(unit_test_db, ['AAPL'], '2024-01-15', '2024-01-17')

        config = TrainingDataConfig(
            sequence_lengths={'5m': 1, '15m': 1, '1h': 1, '1d': 1},
            prediction_horizons={'1h': 1, '1d': 1}
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            callback = DateBasedTrainingDataCallback(
                symbols=['AAPL'],
                config=config,
                output_dir=tmp_dir,
                save_format="pickle"
            )

            class TestRunner:
                def get_environment(self):
                    return Environment(unit_test_db, env_type=EnvironmentType.TEST)

                def get_universe_state_manager(self):
                    return UniverseStateManager(env=self.get_environment())

            runner = TestRunner()
            callback.handleStart(runner, datetime(2024, 1, 15, 8, 0, 0))

            # Process multiple days
            test_days = [
                (datetime(2024, 1, 15, 9, 0, 0), datetime(2024, 1, 15, 16, 0, 0)),
                (datetime(2024, 1, 16, 9, 0, 0), datetime(2024, 1, 16, 16, 0, 0)),
                (datetime(2024, 1, 17, 9, 0, 0), datetime(2024, 1, 17, 16, 0, 0)),
            ]

            for sod_time, eod_time in test_days:
                # Start of day
                callback.handleStartOfDay(runner, sod_time)
                current_date = callback.current_date

                # Some intervals
                asyncio.run(callback.handleInterval(runner, sod_time.replace(hour=10)))
                asyncio.run(callback.handleInterval(runner, sod_time.replace(hour=11)))

                # End of day
                asyncio.run(callback.handleEndOfDay(runner, eod_time))

                # Verify each day gets its own file
                date_str = current_date.strftime('%Y%m%d')
                metadata_file = Path(tmp_dir) / "metadata" / f"daily_stats_{date_str}.json"

                assert metadata_file.exists(), f"Metadata file missing for {date_str}"
                print(f"   ✅ Day {date_str}: File created {metadata_file.name}")

            # Verify all days were processed separately
            output_path = Path(tmp_dir)
            metadata_files = list(output_path.glob("metadata/daily_stats_*.json"))

            assert len(metadata_files) == 3, f"Expected 3 daily files, got {len(metadata_files)}"

            expected_dates = ['20240115', '20240116', '20240117']
            for date_str in expected_dates:
                metadata_file = output_path / "metadata" / f"daily_stats_{date_str}.json"
                assert metadata_file.exists(), f"Missing file for {date_str}"

            print(f"   ✅ All {len(expected_dates)} days have separate files")
            print(f"   ✅ File lifecycle working correctly")
            assert callback.total_days == 3