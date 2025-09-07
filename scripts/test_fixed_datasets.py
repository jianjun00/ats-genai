#!/usr/bin/env python3
"""
Test the newly generated FIXED training datasets to validate timeframe separation.

This script runs our comprehensive test suite on the regenerated datasets
to confirm the timeframe separation bug has been resolved.
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

    # Create a temporary modified test file that uses the fixed dataset path
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
    try:
        column_names = json.loads(column_names_str)
    except json.JSONDecodeError:
        try:
            column_names = ast.literal_eval(column_names_str)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Failed to parse column names: {{e}}")

    record_count = total_records - 1
    return column_names, record_count

def get_file_hash(file_path: str) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def analyze_timeframe_columns(column_names: List[str]) -> Dict[str, List[str]]:
    timeframe_groups = {{
        '5m': [], '15m': [], '1h': [], '1d': [], '1w': [],
        'unknown': []
    }}

    base_features = ['open', 'high', 'low', 'close', 'volume', 'vwap']

    for col in column_names:
        col_lower = col.lower()

        if col_lower.startswith('5m_'):
            timeframe_groups['5m'].append(col)
        elif col_lower.startswith('15m_'):
            timeframe_groups['15m'].append(col)
        elif col_lower.startswith('1h_'):
            timeframe_groups['1h'].append(col)
        elif col_lower.startswith('1d_'):
            timeframe_groups['1d'].append(col)
        elif col_lower.startswith('1w_'):
            timeframe_groups['1w'].append(col)
        elif any(base in col_lower for base in base_features):
            timeframe_groups['unknown'].append(col)
        else:
            timeframe_groups['unknown'].append(col)

    return timeframe_groups

class TestFixedDatasets:

    @pytest.fixture
    def training_dataset_path(self) -> str:
        return "{fixed_dataset_path}"

    @pytest.fixture
    def timeframes(self) -> List[str]:
        return ['5m', '15m', '1h', '1d', '1w']

    def test_fixed_datasets_are_unique(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"
        CRITICAL TEST: Verify that the FIXED datasets have unique files per timeframe.
        This test should PASS, proving the fix worked.
        \"\"\"
        file_hashes = {{}}
        file_sizes = {{}}

        for timeframe in timeframes:
            file_path = f"{{training_dataset_path}}/{{timeframe}}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            if not os.path.exists(file_path):
                pytest.skip(f"Fixed dataset file not found: {{file_path}}")

            file_hash = get_file_hash(file_path)
            file_size = os.path.getsize(file_path)

            file_hashes[timeframe] = file_hash
            file_sizes[timeframe] = file_size

        # All files should have DIFFERENT hashes (timeframe-specific content)
        unique_hashes = set(file_hashes.values())
        assert len(unique_hashes) == len(file_hashes), (
            f"FIXED datasets should have unique files! "
            f"Expected {{len(file_hashes)}} unique files, got {{len(unique_hashes)}} unique hashes. "
            f"Hashes: {{file_hashes}}"
        )

        print(f"✅ SUCCESS: All {{len(file_hashes)}} timeframe files are unique!")
        for tf, hash_val in file_hashes.items():
            print(f"   {{tf}}: {{hash_val[:8]}}... ({{file_sizes[tf]}} bytes)")

    def test_fixed_timeframe_isolation(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"
        Test that FIXED datasets have proper timeframe feature isolation.
        \"\"\"
        for timeframe in timeframes:
            file_path = f"{{training_dataset_path}}/{{timeframe}}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            if not os.path.exists(file_path):
                continue

            column_names, record_count = read_arrayrecord_metadata(file_path)
            timeframe_groups = analyze_timeframe_columns(column_names)

            if timeframe == '5m':
                # 5m should have base features without prefixes
                base_features = timeframe_groups['unknown']  # Base features go to 'unknown'
                other_timeframe_features = (
                    timeframe_groups['15m'] + timeframe_groups['1h'] +
                    timeframe_groups['1d'] + timeframe_groups['1w']
                )

                assert len(other_timeframe_features) == 0, (
                    f"5m FIXED dataset contains features from other timeframes: {{other_timeframe_features[:5]}}... "
                    f"({{len(other_timeframe_features)}} total)"
                )

                # Should have some base features
                expected_base_count = 200  # Expect significant number of base features
                assert len(base_features) >= expected_base_count, (
                    f"5m FIXED dataset has too few base features: {{len(base_features)}} < {{expected_base_count}}"
                )

                print(f"✅ 5m timeframe: {{len(base_features)}} base features, 0 other timeframe features")

            else:
                # Non-5m timeframes should only contain prefixed features for that timeframe
                this_timeframe_features = timeframe_groups[timeframe]

                # Should NOT contain features from other timeframes
                other_timeframes = [tf for tf in timeframes if tf != timeframe]
                other_features = []
                for other_tf in other_timeframes:
                    other_features.extend(timeframe_groups[other_tf])

                # Should also NOT contain unprefixed base features (those belong to 5m only)
                base_features_without_prefix = timeframe_groups['unknown']

                assert len(other_features) == 0, (
                    f"{{timeframe}} FIXED dataset contains features from other timeframes: {{other_features[:5]}}... "
                    f"({{len(other_features)}} total)"
                )

                assert len(base_features_without_prefix) <= 2, (  # Allow timestamp, symbol
                    f"{{timeframe}} FIXED dataset contains unprefixed base features: "
                    f"{{base_features_without_prefix[:5]}}... ({{len(base_features_without_prefix)}} total)"
                )

                expected_min_features = 200  # Expect significant number of timeframe features
                assert len(this_timeframe_features) >= expected_min_features, (
                    f"{{timeframe}} FIXED dataset has too few timeframe features: {{len(this_timeframe_features)}} < {{expected_min_features}}"
                )

                print(f"✅ {{timeframe}} timeframe: {{len(this_timeframe_features)}} {{timeframe}} features, 0 other timeframe features")

    def test_fixed_feature_counts(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"
        Test that FIXED datasets have reasonable feature counts per timeframe.
        \"\"\"
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

    try:
        # Run the test
        result = subprocess.run([
            "python3", "-m", "pytest", temp_test_file, "-v", "--tb=short"
        ],
        capture_output=True,
        text=True,
        cwd="/home/jianjun/ats-genai-pm",
        env={{**os.environ, "PYTHONPATH": "src"}}
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
            print("🔧 The fix may need additional work.")
            return False

    finally:
        # Clean up temporary file
        os.unlink(temp_test_file)


if __name__ == "__main__":
    print("🔧 VALIDATING FIXED TRAINING DATASETS")
    print("=" * 50)
    print("Running comprehensive tests on the newly generated")
    print("datasets to verify the timeframe separation fix.")
    print()

    success = run_tests_on_fixed_datasets()

    print("\\n" + "=" * 50)
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
    """Run comprehensive tests on the fixed training datasets."""

    print("🧪 TESTING FIXED TRAINING DATASETS")
    print("=" * 50)

    # Path to the newly generated fixed datasets
    fixed_dataset_path = "/mnt/d/ats-data/training_data/fixed_20250906_195105/AAPL_20250701_000000_20250906_000000"

    print(f"📊 Testing fixed datasets at: {fixed_dataset_path}")

    # Create a temporary modified test file that uses the fixed dataset path
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
    try:
        column_names = json.loads(column_names_str)
    except json.JSONDecodeError:
        try:
            column_names = ast.literal_eval(column_names_str)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Failed to parse column names: {{e}}")

    record_count = total_records - 1
    return column_names, record_count

def get_file_hash(file_path: str) -> str:
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def analyze_timeframe_columns(column_names: List[str]) -> Dict[str, List[str]]:
    timeframe_groups = {{
        '5m': [], '15m': [], '1h': [], '1d': [], '1w': [],
        'unknown': []
    }}

    base_features = ['open', 'high', 'low', 'close', 'volume', 'vwap']

    for col in column_names:
        col_lower = col.lower()

        if col_lower.startswith('5m_'):
            timeframe_groups['5m'].append(col)
        elif col_lower.startswith('15m_'):
            timeframe_groups['15m'].append(col)
        elif col_lower.startswith('1h_'):
            timeframe_groups['1h'].append(col)
        elif col_lower.startswith('1d_'):
            timeframe_groups['1d'].append(col)
        elif col_lower.startswith('1w_'):
            timeframe_groups['1w'].append(col)
        elif any(base in col_lower for base in base_features):
            timeframe_groups['unknown'].append(col)
        else:
            timeframe_groups['unknown'].append(col)

    return timeframe_groups

class TestFixedDatasets:

    @pytest.fixture
    def training_dataset_path(self) -> str:
        return "{fixed_dataset_path}"

    @pytest.fixture
    def timeframes(self) -> List[str]:
        return ['5m', '15m', '1h', '1d', '1w']

    def test_fixed_datasets_are_unique(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"
        CRITICAL TEST: Verify that the FIXED datasets have unique files per timeframe.
        This test should PASS, proving the fix worked.
        \"\"\"
        file_hashes = {{}}
        file_sizes = {{}}

        for timeframe in timeframes:
            file_path = f"{{training_dataset_path}}/{{timeframe}}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            if not os.path.exists(file_path):
                pytest.skip(f"Fixed dataset file not found: {{file_path}}")

            file_hash = get_file_hash(file_path)
            file_size = os.path.getsize(file_path)

            file_hashes[timeframe] = file_hash
            file_sizes[timeframe] = file_size

        # All files should have DIFFERENT hashes (timeframe-specific content)
        unique_hashes = set(file_hashes.values())
        assert len(unique_hashes) == len(file_hashes), (
            f"FIXED datasets should have unique files! "
            f"Expected {{len(file_hashes)}} unique files, got {{len(unique_hashes)}} unique hashes. "
            f"Hashes: {{file_hashes}}"
        )

        print(f"✅ SUCCESS: All {{len(file_hashes)}} timeframe files are unique!")
        for tf, hash_val in file_hashes.items():
            print(f"   {{tf}}: {{hash_val[:8]}}... ({{file_sizes[tf]}} bytes)")

    def test_fixed_timeframe_isolation(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"
        Test that FIXED datasets have proper timeframe feature isolation.
        \"\"\"
        for timeframe in timeframes:
            file_path = f"{{training_dataset_path}}/{{timeframe}}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            if not os.path.exists(file_path):
                continue

            column_names, record_count = read_arrayrecord_metadata(file_path)
            timeframe_groups = analyze_timeframe_columns(column_names)

            if timeframe == '5m':
                # 5m should have base features without prefixes
                base_features = timeframe_groups['unknown']  # Base features go to 'unknown'
                other_timeframe_features = (
                    timeframe_groups['15m'] + timeframe_groups['1h'] +
                    timeframe_groups['1d'] + timeframe_groups['1w']
                )

                assert len(other_timeframe_features) == 0, (
                    f"5m FIXED dataset contains features from other timeframes: {{other_timeframe_features[:5]}}... "
                    f"({{len(other_timeframe_features)}} total)"
                )

                # Should have some base features
                expected_base_count = 200  # Expect significant number of base features
                assert len(base_features) >= expected_base_count, (
                    f"5m FIXED dataset has too few base features: {{len(base_features)}} < {{expected_base_count}}"
                )

                print(f"✅ 5m timeframe: {{len(base_features)}} base features, 0 other timeframe features")

            else:
                # Non-5m timeframes should only contain prefixed features for that timeframe
                this_timeframe_features = timeframe_groups[timeframe]

                # Should NOT contain features from other timeframes
                other_timeframes = [tf for tf in timeframes if tf != timeframe]
                other_features = []
                for other_tf in other_timeframes:
                    other_features.extend(timeframe_groups[other_tf])

                # Should also NOT contain unprefixed base features (those belong to 5m only)
                base_features_without_prefix = timeframe_groups['unknown']

                assert len(other_features) == 0, (
                    f"{{timeframe}} FIXED dataset contains features from other timeframes: {{other_features[:5]}}... "
                    f"({{len(other_features)}} total)"
                )

                assert len(base_features_without_prefix) <= 2, (  # Allow timestamp, symbol
                    f"{{timeframe}} FIXED dataset contains unprefixed base features: "
                    f"{{base_features_without_prefix[:5]}}... ({{len(base_features_without_prefix)}} total)"
                )

                expected_min_features = 200  # Expect significant number of timeframe features
                assert len(this_timeframe_features) >= expected_min_features, (
                    f"{{timeframe}} FIXED dataset has too few timeframe features: {{len(this_timeframe_features)}} < {{expected_min_features}}"
                )

                print(f"✅ {{timeframe}} timeframe: {{len(this_timeframe_features)}} {{timeframe}} features, 0 other timeframe features")

    def test_fixed_feature_counts(self, training_dataset_path: str, timeframes: List[str]):
        \"\"\"
        Test that FIXED datasets have reasonable feature counts per timeframe.
        \"\"\"
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

    try:
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
            print("🔧 The fix may need additional work.")
            return False

    finally:
        # Clean up temporary file
        os.unlink(temp_test_file)


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