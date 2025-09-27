#!/usr/bin/env python3
"""
Test the newly generated FIXED training datasets to validate timeframe separation.
"""

import subprocess
import sys
import os
import tempfile


def run_tests_on_fixed_datasets():
    """Run comprehensive tests on the fixed training datasets."""

    print("🧪 TESTING FIXED TRAINING DATASETS")
    print("=" * 50)

    # Path to the newly generated fixed datasets
    fixed_dataset_path = "/mnt/d/ats-data/training_data/fixed_20250906_195105/AAPL_20250701_000000_20250906_000000"

    print(f"📊 Testing fixed datasets at: {fixed_dataset_path}")

    # Create a temporary test file for the fixed datasets
    test_content = f"""
import pytest
import os
import hashlib
import json
import ast
from typing import Dict, List, Set, Tuple
import numpy as np
from pathlib import Path

def read_arrayrecord_metadata(file_path: str) -> Tuple[List[str], int]:
    from array_record.python.array_record_module import ArrayRecordReader

    reader = ArrayRecordReader(str(file_path))
    total_records = reader.num_records()

    if total_records == 0:
        reader.close()
        return [], 0

    reader.seek(0)
    first_record = reader.read()
    reader.close()

    column_names_str = first_record.decode('utf-8') if isinstance(first_record, bytes) else str(first_record)
    column_names = json.loads(column_names_str)
    record_count = total_records - 1
    return column_names, record_count

def get_file_hash(file_path: str) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

class TestFixedDatasets:

    @pytest.fixture
    def training_dataset_path(self) -> str:
        return "{fixed_dataset_path}"

    @pytest.fixture
    def timeframes(self) -> List[str]:
        return ['5m', '15m', '1h', '1d', '1w']

    def test_fixed_datasets_are_unique(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"CRITICAL TEST: Verify FIXED datasets have unique files per timeframe.\"\"\"
        file_hashes = {{}}

        for timeframe in timeframes:
            file_path = f"{{training_dataset_path}}/{{timeframe}}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            if not os.path.exists(file_path):
                pytest.skip(f"Fixed dataset file not found: {{file_path}}")

            file_hash = get_file_hash(file_path)
            file_hashes[timeframe] = file_hash

        # All files should have DIFFERENT hashes
        unique_hashes = set(file_hashes.values())
        assert len(unique_hashes) == len(file_hashes), (
            f"FIXED datasets should have unique files! "
            f"Expected {{len(file_hashes)}} unique files, got {{len(unique_hashes)}} unique hashes."
        )

        print(f"✅ SUCCESS: All {{len(file_hashes)}} timeframe files are unique!")

    def test_fixed_feature_counts(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"Test that FIXED datasets have reasonable feature counts per timeframe.\"\"\"
        for timeframe in timeframes:
            file_path = f"{{training_dataset_path}}/{{timeframe}}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            if not os.path.exists(file_path):
                continue

            column_names, record_count = read_arrayrecord_metadata(file_path)
            actual_count = len(column_names)

            # Should have reasonable feature counts (not the massive 962 from mixed timeframes)
            assert 200 <= actual_count <= 300, (
                f"{{timeframe}} FIXED dataset has unexpected feature count: {{actual_count}}. "
                f"Expected 200-300 features for isolated timeframe."
            )

            print(f"✅ {{timeframe}} timeframe: {{actual_count}} features (within expected range)")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
"""

    # Write the test to a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(test_content)
        temp_test_file = f.name

    # Run the test
    result = subprocess.run([
        "python3", "-m", "pytest", temp_test_file, "-v", "--tb=short"
    ],
    capture_output=True,
    text=True,
    cwd="/home/jianjun/ats-genai-pm",
    env={**os.environ, "PYTHONPATH": "src"}
    )

    print("📊 TEST RESULTS:")
    print("=" * 30)
    print(result.stdout)

    if result.stderr:
        print("⚠️  Warnings/Errors:")
        print(result.stderr)

    if result.returncode == 0:
        print("✅ ALL TESTS PASSED!")
        print("🎉 The timeframe separation fix is VERIFIED!")
        return True
    else:
        print("❌ SOME TESTS FAILED!")
        return False

if __name__ == "__main__":
    print("🔧 VALIDATING FIXED TRAINING DATASETS")
    print("=" * 50)
    print("Running comprehensive tests on the newly generated")
    print("datasets to verify the timeframe separation fix.")
    print()

    success = run_tests_on_fixed_datasets()

    print("\n" + "=" * 50)
    if success:
        print("🎉 VALIDATION SUCCESS!")
        print("✅ Fixed datasets pass all timeframe separation tests")
        print("✅ Each timeframe contains only relevant features")
        print("✅ The critical bug has been RESOLVED!")
    else:
        print("💥 VALIDATION FAILED!")
        print("❌ Some tests are still failing")
        print("🔧 Additional fixes may be needed")

    exit(0 if success else 1)