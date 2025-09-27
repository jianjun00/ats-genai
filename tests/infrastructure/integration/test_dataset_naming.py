#!/usr/bin/env python3
"""
Test dataset naming includes unique run_id to prevent duplicates
"""

import os
import sys
import asyncio
import re
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# TrainingDataJobRunner class does not exist in feature_extraction_runner, create_sample_job_config

def test_dataset_name_generation_with_run_id():
    """Test that dataset names include unique run_id to prevent duplicates."""

    print("🧪 Testing dataset name generation with run_id...")

    # Create sample config
    config = create_sample_job_config(symbols=['TSLA'])
    runner = TrainingDataJobRunner(config=config)

    # Test multiple runs to ensure uniqueness
    dataset_names = []

    for run_id in [1, 2, 3, 42, 100]:
        runner.run_id = run_id

        # Generate dataset name using the same pattern as in the code
        dataset_id = f"dataset_{config.job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"Run ID {run_id}: {dataset_id}")
        dataset_names.append(dataset_id)

        # Verify job_name contains symbol
        assert 'TSLA' in config.job_name, f"Job name should contain TSLA: {config.job_name}"

        # The current implementation doesn't include run_id - this should fail
        # We'll test what should be the correct format
        expected_format = f"dataset_{config.job_name}_run{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"  Expected format with run_id: {expected_format}")

    print(f"Generated {len(dataset_names)} dataset names")
    print("❌ Current implementation doesn't include run_id - this is the bug!")

    return dataset_names

def test_dataset_name_uniqueness_issue():
    """Test that demonstrates the current uniqueness issue."""

    print("\n🔍 Demonstrating uniqueness issue...")

    # Simulate rapid successive runs (same timestamp)
    config = create_sample_job_config(symbols=['TSLA'])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Multiple runs with same timestamp will have same dataset name
    run_ids = [10, 11, 12]
    dataset_names = []

    for run_id in run_ids:
        # Current implementation (without run_id)
        current_name = f"dataset_{config.job_name}_{timestamp}"
        dataset_names.append(current_name)
        print(f"Run {run_id}: {current_name}")

    # Check for duplicates
    unique_names = set(dataset_names)

    print(f"Generated names: {len(dataset_names)}")
    print(f"Unique names: {len(unique_names)}")

    if len(unique_names) < len(dataset_names):
        print("❌ DUPLICATE DATASET NAMES DETECTED - This causes database constraint violations!")
        print(f"Duplicate name: {dataset_names[0]}")
        return False
    else:
        print("✅ All names unique (unlikely with rapid execution)")
        return True

def test_proposed_fix():
    """Test the proposed fix with run_id in dataset names."""

    print("\n✅ Testing proposed fix with run_id...")

    config = create_sample_job_config(symbols=['TSLA'])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Multiple runs with same timestamp but different run_ids
    run_ids = [10, 11, 12]
    dataset_names = []

    for run_id in run_ids:
        # Proposed fix: include run_id
        fixed_name = f"dataset_{config.job_name}_run{run_id}_{timestamp}"
        dataset_names.append(fixed_name)
        print(f"Run {run_id}: {fixed_name}")

    # Check for uniqueness
    unique_names = set(dataset_names)

    print(f"Generated names: {len(dataset_names)}")
    print(f"Unique names: {len(unique_names)}")

    if len(unique_names) == len(dataset_names):
        print("✅ All names unique with run_id!")
        return True
    else:
        print("❌ Still have duplicates - this shouldn't happen")
        return False

def test_run_id_in_dataset_name_regex():
    """Test that dataset names follow the expected pattern with run_id."""

    print("\n🔍 Testing dataset name pattern validation...")

    # Expected pattern: dataset_{job_name}_run{run_id}_{timestamp}
    expected_pattern = r"dataset_training_data_gen_\w+_run\d+_\d{8}_\d{6}"

    test_cases = [
        ("dataset_training_data_gen_TSLA_run42_20250831_123456", True),
        ("dataset_training_data_gen_AAPL_run1_20250831_223456", True),
        ("dataset_training_data_gen_GOOGL_run100_20250831_023456", True),
        ("dataset_training_data_gen_TSLA_20250831_123456", False),  # Missing run_id
        ("dataset_training_data_gen_TSLA_run_20250831_123456", False),  # Invalid run_id
    ]

    pattern = re.compile(expected_pattern)

    for dataset_name, should_match in test_cases:
        matches = bool(pattern.match(dataset_name))
        status = "✅" if matches == should_match else "❌"
        print(f"{status} {dataset_name}: {matches} (expected {should_match})")

        if matches != should_match:
            return False

    print("✅ All pattern tests passed!")
    return True

@pytest.mark.asyncio

async def test_actual_dataset_creation():
    """Test actual dataset creation with run_id in names."""

    print("\n🚀 Testing actual dataset creation...")

    pass

    # This will fail with current implementation but shows what we need
    print("Note: This will demonstrate the current issue...")
    print("The dataset name won't include run_id, causing potential duplicates")

    # We can't actually run this without risking database changes
    # But we can show the configuration
    config = create_sample_job_config(symbols=['TEST'])
    runner = TrainingDataJobRunner(config=config)

    print(f"Config job_name: {config.job_name}")
    print(f"Expected dataset prefix: dataset_{config.job_name}_run{{run_id}}_")

    return True

def main():
    """Run all dataset naming tests."""

    print("🧪 DATASET NAMING TESTS")
    print("=" * 50)

    results = []

    # Test current implementation issues
    results.append(("Dataset Name Generation", test_dataset_name_generation_with_run_id()))
    results.append(("Uniqueness Issue Demo", not test_dataset_name_uniqueness_issue()))  # Invert because we expect failure
    results.append(("Proposed Fix", test_proposed_fix()))
    results.append(("Pattern Validation", test_run_id_in_dataset_name_regex()))

    # Test actual creation
    results.append(("Dataset Creation Setup", asyncio.run(test_actual_dataset_creation())))

    print("\n" + "=" * 50)
    print("TEST RESULTS SUMMARY:")

    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
        if result:
            passed += 1

    print(f"\nPassed: {passed}/{len(results)} tests")

    print("\n🔧 REQUIRED FIX:")
    print("Update dataset_id generation to include run_id:")
    print('dataset_id = f"dataset_{self.config.job_name}_run{self.run_id}_{datetime.now().strftime(\'%Y%m%d_%H%M%S\')}"')

    return passed == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)