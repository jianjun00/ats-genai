"""
Comprehensive tests to validate ArrayRecord timeframe separation.

These tests detect the critical bug where all timeframe ArrayRecord files
contain identical mixed-timeframe data instead of timeframe-specific features.

Based on TRAINING_DATASET_PRD_DRD.md QR4 requirements:
- Each timeframe ArrayRecord must contain ONLY features for that timeframe
- Single value per feature (not historical sequences)
- Training methodology: take N sequential rows from each timeframe and join by timestamp
"""

import pytest
import os
import hashlib
import json
import ast
from typing import Dict, List, Tuple
import numpy as np

# Test utilities
def read_arrayrecord_metadata(file_path: str) -> Tuple[List[str], int]:
    """Read column names and record count from ArrayRecord file"""
    from array_record.python.array_record_module import ArrayRecordReader

    reader = ArrayRecordReader(str(file_path))
    total_records = reader.num_records()

    if total_records == 0:
        reader.close()
        return [], 0

    # First record contains column names
    reader.seek(0)
    first_record = reader.read()
    reader.close()

    # Parse column names - handle both JSON and Python list format
    column_names_str = first_record.decode('utf-8') if isinstance(first_record, bytes) else str(first_record)
    column_names = json.loads(column_names_str)
    record_count = total_records - 1

    return column_names, record_count


def get_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of file to detect identical files"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def analyze_timeframe_columns(column_names: List[str]) -> Dict[str, List[str]]:
    """Analyze column names and group by detected timeframe"""
    timeframe_groups = {
        '5m': [], '15m': [], '1h': [], '1d': [], '1w': [],
        'unknown': []
    }

    # Expected base features per timeframe
    base_features = ['open', 'high', 'low', 'close', 'volume', 'vwap']

    for col in column_names:
        col_lower = col.lower()

        # Detect timeframe prefix
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
            # Base feature without prefix - should only exist in its native timeframe
            timeframe_groups['unknown'].append(col)
        else:
            timeframe_groups['unknown'].append(col)

    return timeframe_groups


class TestArrayRecordTimeframeSeparation:
    """Test suite to validate ArrayRecord timeframe separation requirements"""

    @pytest.fixture
    def training_dataset_path(self) -> str:
        """Path to training dataset for testing"""
        return "/mnt/d/ats-data/training_data/89/AAPL_20250701_000000_20250906_000000"

    @pytest.fixture
    def timeframes(self) -> List[str]:
        """Standard timeframes used in training datasets"""
        return ['5m', '15m', '1h', '1d', '1w']

    def test_arrayrecord_files_exist(self, training_dataset_path: str, timeframes: List[str]):
        """Verify all timeframe ArrayRecord files exist"""
        for timeframe in timeframes:
            file_path = f"{training_dataset_path}/{timeframe}/AAPL_20250701_000000_20250906_000000.arrayrecord"
            assert os.path.exists(file_path), f"Missing ArrayRecord file for {timeframe}: {file_path}"

    def test_critical_bug_detection_identical_files(self, training_dataset_path: str, timeframes: List[str]):
        """
        CRITICAL: Detect the bug where all timeframe ArrayRecord files are identical

        This test should FAIL with current implementation to prove the bug exists.
        After fixing the training dataset generation, this test should PASS.
        """
        file_hashes = {}
        file_sizes = {}

        for timeframe in timeframes:
            file_path = f"{training_dataset_path}/{timeframe}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            # Calculate hash and size
            file_hash = get_file_hash(file_path)
            file_size = os.path.getsize(file_path)

            file_hashes[timeframe] = file_hash
            file_sizes[timeframe] = file_size

        # All files should have DIFFERENT hashes (timeframe-specific content)
        unique_hashes = set(file_hashes.values())
        assert len(unique_hashes) == len(timeframes), (
            f"CRITICAL BUG: All timeframe files are identical! "
            f"Expected {len(timeframes)} unique files, got {len(unique_hashes)} unique hashes. "
            f"Hashes: {file_hashes}"
        )

        # All files should have DIFFERENT sizes (different feature counts)
        unique_sizes = set(file_sizes.values())
        assert len(unique_sizes) == len(timeframes), (
            f"CRITICAL BUG: All timeframe files have identical sizes! "
            f"Expected {len(timeframes)} unique sizes, got {len(unique_sizes)} unique sizes. "
            f"Sizes: {file_sizes}"
        )

    def test_timeframe_column_isolation(self, training_dataset_path: str, timeframes: List[str]):
        """
        Test that each timeframe ArrayRecord contains ONLY features for that timeframe

        Per QR4 requirement:
        - 5m ArrayRecord should contain: timestamp, symbol, open, high, low, close, volume, vwap
        - 1h ArrayRecord should contain: timestamp, symbol, 1h_open, 1h_high, 1h_low, 1h_close, 1h_volume, 1h_vwap
        """
        for timeframe in timeframes:
            file_path = f"{training_dataset_path}/{timeframe}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            # Read column metadata
            column_names, record_count = read_arrayrecord_metadata(file_path)

            # Analyze timeframe distribution in columns
            timeframe_groups = analyze_timeframe_columns(column_names)

            if timeframe == '5m':
                # Base 5m features should have no prefix
                expected_base_features = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap']
                base_features_found = [col for col in column_names if col.lower() in [f.lower() for f in expected_base_features]]

                # Should NOT contain other timeframe features
                other_timeframe_features = (
                    timeframe_groups['15m'] + timeframe_groups['1h'] +
                    timeframe_groups['1d'] + timeframe_groups['1w']
                )

                assert len(other_timeframe_features) == 0, (
                    f"5m ArrayRecord contains features from other timeframes: {other_timeframe_features[:10]}... "
                    f"({len(other_timeframe_features)} total)"
                )

            else:
                # Non-5m timeframes should only contain prefixed features for that timeframe
                expected_prefix = f"{timeframe}_"

                # Count features for this specific timeframe
                this_timeframe_features = timeframe_groups[timeframe]

                # Should NOT contain features from other timeframes
                other_timeframes = [tf for tf in timeframes if tf != timeframe]
                other_features = []
                for other_tf in other_timeframes:
                    other_features.extend(timeframe_groups[other_tf])

                # Should also NOT contain unprefixed base features (those belong to 5m only)
                base_features_without_prefix = timeframe_groups['unknown']

                assert len(other_features) == 0, (
                    f"{timeframe} ArrayRecord contains features from other timeframes: {other_features[:10]}... "
                    f"({len(other_features)} total)"
                )

                assert len(base_features_without_prefix) == 0, (
                    f"{timeframe} ArrayRecord contains unprefixed base features (should be 5m only): "
                    f"{base_features_without_prefix[:10]}... ({len(base_features_without_prefix)} total)"
                )

                assert len(this_timeframe_features) > 0, (
                    f"{timeframe} ArrayRecord contains no features for its own timeframe! "
                    f"Expected features with prefix '{expected_prefix}'"
                )

    def test_single_value_per_feature_requirement(self, training_dataset_path: str):
        """
        Test that each feature has single value, not historical sequences

        Per QR4: "Single value per feature: Each feature has ONE value, not historical sequences"
        """
        # Test with 5m timeframe as representative sample
        file_path = f"{training_dataset_path}/5m/AAPL_20250701_000000_20250906_000000.arrayrecord"

        from array_record.python.array_record_module import ArrayRecordReader

        reader = ArrayRecordReader(str(file_path))
        total_records = reader.num_records()

        if total_records <= 1:
            reader.close()
            pytest.skip("No data records to test (only metadata record)")

        # Sample first 5 data records (skip metadata at index 0)
        sample_size = min(5, total_records - 1)

        for i in range(1, sample_size + 1):  # Skip metadata record at index 0
            reader.seek(i)
            record = reader.read()

            # Decode float32 array
            float_array = np.frombuffer(record, dtype=np.float32)

            # Each record should have exactly one value per feature
            # (Not a sequence of historical values)
            column_names, _ = read_arrayrecord_metadata(file_path)
            expected_length = len(column_names)

            assert len(float_array) == expected_length, (
                f"Record {i}: Expected {expected_length} values (one per feature), "
                f"got {len(float_array)} values. "
                f"Features should contain single values, not sequences."
            )

        reader.close()

    def test_expected_feature_counts_by_timeframe(self, training_dataset_path: str, timeframes: List[str]):
        """
        Test expected number of features per timeframe

        Based on QR4 requirements:
        - Each timeframe should have: timestamp, symbol + 6 OHLCV features
        - Plus any technical indicators for that timeframe
        """
        expected_base_feature_counts = {
            '5m': 8,   # timestamp, symbol, open, high, low, close, volume, vwap
            '15m': 8,  # timestamp, symbol, 15m_open, 15m_high, 15m_low, 15m_close, 15m_volume, 15m_vwap
            '1h': 8,   # timestamp, symbol, 1h_open, 1h_high, 1h_low, 1h_close, 1h_volume, 1h_vwap
            '1d': 8,   # timestamp, symbol, 1d_open, 1d_high, 1d_low, 1d_close, 1d_volume, 1d_vwap
            '1w': 8,   # timestamp, symbol, 1w_open, 1w_high, 1w_low, 1w_close, 1w_volume, 1w_vwap
        }

        for timeframe in timeframes:
            file_path = f"{training_dataset_path}/{timeframe}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            column_names, record_count = read_arrayrecord_metadata(file_path)
            actual_count = len(column_names)

            # Should have AT LEAST the base features, possibly more with technical indicators
            min_expected = expected_base_feature_counts[timeframe]

            assert actual_count >= min_expected, (
                f"{timeframe} ArrayRecord has {actual_count} features, "
                f"expected at least {min_expected} (base OHLCV + timestamp/symbol)"
            )

            # Should NOT have the massive 962 columns (indicating mixed timeframes)
            assert actual_count < 100, (
                f"{timeframe} ArrayRecord has {actual_count} features, "
                f"which suggests mixed timeframes. Expected < 100 features for isolated timeframe."
            )

    def test_timestamp_alignment_across_timeframes(self, training_dataset_path: str, timeframes: List[str]):
        """
        Test that timestamps are properly aligned for training methodology

        Per QR4: "Training methodology: take N sequential rows from each timeframe and join by timestamp"
        """
        from array_record.python.array_record_module import ArrayRecordReader

        # Read first few timestamps from each timeframe
        timeframe_timestamps = {}

        for timeframe in timeframes:
            file_path = f"{training_dataset_path}/{timeframe}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            reader = ArrayRecordReader(str(file_path))
            total_records = reader.num_records()

            if total_records <= 1:
                reader.close()
                continue

            # Get first 3 data records (skip metadata at index 0)
            sample_size = min(3, total_records - 1)
            timestamps = []

            for i in range(1, sample_size + 1):
                reader.seek(i)
                record = reader.read()
                float_array = np.frombuffer(record, dtype=np.float32)
                timestamp = float_array[0]  # Assume first column is timestamp
                timestamps.append(timestamp)

            reader.close()
            timeframe_timestamps[timeframe] = timestamps

        # Verify timestamps are reasonable (not all zeros, not identical)
        for timeframe, timestamps in timeframe_timestamps.items():
            # Timestamps should not be all zeros
            assert not all(ts == 0.0 for ts in timestamps), (
                f"{timeframe} ArrayRecord has all zero timestamps"
            )

            # Timestamps should progress (not all identical)
            assert len(set(timestamps)) > 1, (
                f"{timeframe} ArrayRecord has identical timestamps: {timestamps}"
            )

    def test_symbol_consistency_across_timeframes(self, training_dataset_path: str, timeframes: List[str]):
        """
        Test that symbol field is consistent across all timeframe files
        """
        from array_record.python.array_record_module import ArrayRecordReader

        expected_symbol = "AAPL"  # Based on file path

        for timeframe in timeframes:
            file_path = f"{training_dataset_path}/{timeframe}/AAPL_20250701_000000_20250906_000000.arrayrecord"

            reader = ArrayRecordReader(str(file_path))
            total_records = reader.num_records()

            if total_records <= 1:
                reader.close()
                continue

            # For this test, we'll assume symbol is encoded in a way we can verify
            # (This may need adjustment based on actual symbol encoding in ArrayRecord)
            column_names, _ = read_arrayrecord_metadata(file_path)

            # Verify 'symbol' column exists
            symbol_columns = [col for col in column_names if 'symbol' in col.lower()]
            assert len(symbol_columns) > 0, (
                f"{timeframe} ArrayRecord missing symbol column. "
                f"Available columns: {column_names[:10]}..."
            )

            reader.close()


if __name__ == "__main__":
    """
    Run these tests to detect ArrayRecord timeframe separation issues:

    PYTHONPATH=src python -m pytest tests/integration/test_arrayrecord_timeframe_separation.py -v
    """
    pytest.main([__file__, "-v", "--tb=short"])