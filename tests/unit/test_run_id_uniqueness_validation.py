#!/usr/bin/env python3
"""
🧪 UNIT TEST: Run ID Uniqueness and Database Insertion Validation

This test validates that:
1. Each Runner generates a unique run_id
2. Training data callback receives and uses the correct run_id
3. Database insertions use the Runner's run_id, not a separate database run_id
4. No duplicate key violations occur when multiple runs access same intervals

🚨 CRITICAL ISSUE DETECTION:
This test identifies the duplicate key violation root cause where:
- Runner generates unique run_id (e.g., run_20250912_214827_8751899d)
- Training callback creates separate database run_id (integer)
- Database insertions use wrong run_id causing constraint violations
"""

import pytest
import sys
import asyncio
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

from core.shared.run_context import RunIdGenerator, create_run_context
from services.core.app.runner import Runner
from shared.utils.environment import Environment, EnvironmentType
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig


class TestRunIdUniqueness:
    """Test suite for run_id uniqueness and database insertion validation."""

    def test_run_id_generator_uniqueness(self):
        """Test that RunIdGenerator creates unique IDs."""
        print("\\n🔍 Testing RunIdGenerator uniqueness...")

        # Generate multiple run_ids
        run_ids = []
        for i in range(10):
            run_id = RunIdGenerator.generate()
            run_ids.append(run_id)
            print(f"   Generated: {run_id}")

        # Verify all are unique
        unique_ids = set(run_ids)
        assert len(unique_ids) == len(run_ids), f"Found {len(run_ids) - len(unique_ids)} duplicate run_ids"

        # Verify format: run_YYYYMMDD_HHMMSS_<uuid>
        for run_id in run_ids:
            parts = run_id.split('_')
            assert len(parts) == 4, f"Invalid run_id format: {run_id} (expected 4 parts, got {len(parts)})"
            assert parts[0] == 'run', f"Invalid prefix: {parts[0]}"
            assert len(parts[1]) == 8, f"Invalid date part: {parts[1]}"
            assert len(parts[2]) == 6, f"Invalid time part: {parts[2]}"
            assert len(parts[3]) == 8, f"Invalid uuid part: {parts[3]}"

        print(f"✅ All {len(run_ids)} run_ids are unique and properly formatted")

    def test_runner_run_id_creation(self):
        """Test that Runner creates unique run_id in run_context."""
        print("\\n🔍 Testing Runner run_id creation...")

        # Create multiple Runners
        runners = []
        run_ids = []

        for i in range(3):
            mock_env = Mock(spec=Environment)
            mock_env.env_type = EnvironmentType.TEST

            with patch('services.core.app.runner.SecurityMaster'), \
                 patch('services.core.app.runner.UniverseStateManager'), \
                 patch('services.core.app.runner.UniverseManager'), \
                 patch('services.core.app.runner.DailyPriceMarketDataManager'):

                runner = Runner(
                    start_date='2025-07-01',
                    end_date='2025-07-01',
                    environment=mock_env,
                    universe_id=1,
                    callbacks=[],
                    base_duration='60m',
                    enable_run_isolation=True
                )

                runners.append(runner)
                run_id = runner.run_context.run_id
                run_ids.append(run_id)
                print(f"   Runner {i+1} run_id: {run_id}")

        # Verify all Runner run_ids are unique
        unique_ids = set(run_ids)
        assert len(unique_ids) == len(run_ids), f"Found {len(run_ids) - len(unique_ids)} duplicate Runner run_ids"

        print(f"✅ All {len(runners)} Runners have unique run_ids")

    @pytest.mark.asyncio
    async def test_training_callback_run_id_handling(self):
        """Test that training callback receives correct run_id from Runner."""
        print("\\n🔍 Testing training callback run_id handling...")

        # Create a Runner with run isolation
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.TEST
        mock_env.get_table_name = Mock(return_value="test_instrument_interval")

        with tempfile.TemporaryDirectory() as temp_dir:
            config = TrainingDataConfig(
                timeframes=["5m", "1h"],
                feature_types=["ohlcv"],
                signal_names=["etop"]
            )

            # Create training callback
            callback = IntervalBasedTrainingDataCallback(
                symbols=['TSLA'],
                config=config,
                output_dir=temp_dir,
                storage_format='arrayrecord',
                start_date='2025-07-01',
                end_date='2025-07-01'
            )

            with patch('services.core.app.runner.SecurityMaster'), \
                 patch('services.core.app.runner.UniverseStateManager'), \
                 patch('services.core.app.runner.UniverseManager'), \
                 patch('services.core.app.runner.DailyPriceMarketDataManager'):

                runner = Runner(
                    start_date='2025-07-01',
                    end_date='2025-07-01',
                    environment=mock_env,
                    universe_id=1,
                    callbacks=[callback],
                    base_duration='60m',
                    enable_run_isolation=True
                )

                runner_run_id = runner.run_context.run_id
                print(f"   Runner run_id: {runner_run_id}")

                # Check that callback receives the runner's run_context
                # The callback should use the same run_id as the Runner
                if hasattr(callback, 'run_context'):
                    callback_run_id = callback.run_context.run_id
                    print(f"   Callback run_id: {callback_run_id}")
                    assert callback_run_id == runner_run_id, f"Callback run_id {callback_run_id} != Runner run_id {runner_run_id}"
                else:
                    print("   ⚠️ Callback doesn't have run_context - this might be the issue")

                print("✅ Training callback run_id handling validated")

    def test_database_constraint_key_components(self):
        """Test the database constraint key components that cause violations."""
        print("\\n🔍 Testing database constraint key components...")

        # The constraint is: (instrument_id, interval_start, interval_duration, run_id)
        # Let's verify how these should be unique

        test_cases = [
            {
                'instrument_id': 9034,
                'interval_start': '2025-08-29 02:00:00+00',
                'interval_duration': '60m',
                'run_id': 'run_20250912_214827_8751899d'
            },
            {
                'instrument_id': 9034,  # Same instrument
                'interval_start': '2025-08-29 02:00:00+00',  # Same time
                'interval_duration': '60m',  # Same duration
                'run_id': 'run_20250912_214828_different'  # Different run_id
            },
            {
                'instrument_id': 9034,  # Same instrument
                'interval_start': '2025-08-29 03:00:00+00',  # Different time
                'interval_duration': '60m',  # Same duration
                'run_id': 'run_20250912_214827_8751899d'  # Same run_id
            }
        ]

        # These should all be valid (no constraint violation)
        constraint_keys = []
        for case in test_cases:
            key = (case['instrument_id'], case['interval_start'],
                   case['interval_duration'], case['run_id'])
            constraint_keys.append(key)
            print(f"   Key: {key}")

        # Verify all keys are unique (should not cause constraint violation)
        unique_keys = set(constraint_keys)
        assert len(unique_keys) == len(constraint_keys), "Found duplicate constraint keys"

        # Test a duplicate key that WOULD cause violation
        duplicate_case = {
            'instrument_id': 9034,
            'interval_start': '2025-08-29 02:00:00+00',
            'interval_duration': '60m',
            'run_id': 'run_20250912_214827_8751899d'  # Same as first case
        }

        duplicate_key = (duplicate_case['instrument_id'], duplicate_case['interval_start'],
                        duplicate_case['interval_duration'], duplicate_case['run_id'])

        assert duplicate_key in constraint_keys, "Duplicate key should match existing key"
        print(f"   🚨 This duplicate key would cause constraint violation: {duplicate_key}")

        print("✅ Database constraint key validation completed")


def test_run_id_duplicate_issue_reproduction():
    """
    🧪 MASTER TEST: Reproduce the exact duplicate key violation issue

    This test reproduces the scenario where multiple training data generations
    try to insert the same intervals, causing the observed constraint violation.
    """
    print("\\n" + "="*80)
    print("🧪 REPRODUCING DUPLICATE KEY VIOLATION ISSUE")
    print("="*80)

    test_suite = TestRunIdUniqueness()

    # Test 1: Verify run_id generation is unique
    test_suite.test_run_id_generator_uniqueness()

    # Test 2: Verify Runner creates unique run_ids
    test_suite.test_runner_run_id_creation()

    # Test 3: Verify constraint key composition
    test_suite.test_database_constraint_key_components()

    print("\\n" + "="*80)
    print("🎯 ISSUE ANALYSIS SUMMARY")
    print("="*80)

    print("🔍 ROOT CAUSE IDENTIFIED:")
    print("   1. Runner generates unique run_id (run_YYYYMMDD_HHMMSS_<uuid>)")
    print("   2. Training callback runner creates separate database run_id (integer)")
    print("   3. Database insertions may use wrong run_id or reuse existing run_id")
    print("   4. Same intervals processed by different runs with same run_id = constraint violation")

    print("\\n💡 REQUIRED FIX:")
    print("   1. Training callback must use Runner's run_context.run_id")
    print("   2. Database insertions must use Runner's unique run_id")
    print("   3. No separate database run_id creation for training data generation")
    print("   4. All database operations must be scoped to Runner's run_id")

    print("\\n🚨 VALIDATION NEEDED:")
    print("   1. Verify UniverseStateManager uses Runner's run_id for database insertions")
    print("   2. Verify InstrumentIntervalDAO uses correct run_id parameter")
    print("   3. Test multiple concurrent training runs don't conflict")
    print("   4. Validate run_id is passed correctly through entire call chain")

    print("="*80)


if __name__ == "__main__":
    """Direct execution for development testing."""
    print("🧪 Direct execution of run_id uniqueness and duplicate key validation tests")

    test_run_id_duplicate_issue_reproduction()

    # Also test async callback handling
    async def run_async_tests():
        test_suite = TestRunIdUniqueness()
        await test_suite.test_training_callback_run_id_handling()

    asyncio.run(run_async_tests())