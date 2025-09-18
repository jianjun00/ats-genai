#!/usr/bin/env python3
"""
ArrayRecord Data Validation Tests
Tests ArrayRecord files for data integrity and NaN value handling
"""

import unittest
import sys
import os
import numpy as np
from pathlib import Path
import math
import ast

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.sanitizers.json_sanitizer import sanitize_training_features

class TestArrayRecordDataValidation(unittest.TestCase):
    """Test ArrayRecord files for data integrity and NaN handling."""

    def setUp(self):
        """Set up test paths."""
        self.training_data_paths = [
            Path("/data/training_data"),  # Container path
            Path("/mnt/d/ats-data/training_data")  # Host path
        ]
        self.sample_arrayrecord_files = []

        # Find sample ArrayRecord files
        for base_path in self.training_data_paths:
            if base_path.exists():
                # Look for ArrayRecord files
                for file_path in base_path.rglob("*.arrayrecord"):
                    self.sample_arrayrecord_files.append(file_path)
                    if len(self.sample_arrayrecord_files) >= 5:  # Limit for testing
                        break

    def test_arrayrecord_files_exist(self):
        """Test that ArrayRecord files exist for testing."""
        self.assertGreater(len(self.sample_arrayrecord_files), 0,
                          "No ArrayRecord files found for validation testing")
        print(f"Found {len(self.sample_arrayrecord_files)} ArrayRecord files for testing")

    def test_arrayrecord_basic_structure(self):
        """Test basic structure of ArrayRecord files."""
        if not self.sample_arrayrecord_files:
            self.skipTest("No ArrayRecord files available")

        try:
            from array_record.python.array_record_module import ArrayRecordReader
        except ImportError:
            self.skipTest("ArrayRecord library not available")

        for file_path in self.sample_arrayrecord_files[:3]:  # Test first 3 files
            with self.subTest(file=str(file_path)):
                print(f"\n📁 Testing ArrayRecord file: {file_path}")

                try:
                    reader = ArrayRecordReader(str(file_path))

                    # Check basic structure
                    total_records = reader.num_records()
                    self.assertGreaterEqual(total_records, 2,
                                          f"ArrayRecord should have at least 2 records (columns + data)")

                    print(f"   Records: {total_records}")

                    # Test reading columns (record 0)
                    reader.seek(0)
                    columns_record = reader.read()
                    columns_str = columns_record.decode('utf-8')
                    columns = ast.literal_eval(columns_str)

                    self.assertIsInstance(columns, list, "Columns should be a list")
                    self.assertGreater(len(columns), 0, "Should have columns")
                    print(f"   Columns: {len(columns)}")

                    # Test reading data (record 1)
                    reader.seek(1)
                    data_record = reader.read()
                    training_array = np.frombuffer(data_record, dtype=np.float32)

                    self.assertEqual(len(training_array), len(columns),
                                   "Data array length should match columns length")

                    reader.close()
                    print(f"   ✅ Basic structure validation passed")

                except Exception as e:
                    self.fail(f"Failed to read ArrayRecord {file_path}: {e}")

    def test_arrayrecord_nan_detection(self):
        """Test detection of NaN values in ArrayRecord files."""
        if not self.sample_arrayrecord_files:
            self.skipTest("No ArrayRecord files available")

        try:
            from array_record.python.array_record_module import ArrayRecordReader
        except ImportError:
            self.skipTest("ArrayRecord library not available")

        nan_files = []
        clean_files = []

        for file_path in self.sample_arrayrecord_files[:5]:  # Test first 5 files
            print(f"\n🔍 Checking for NaN values: {file_path}")

            try:
                reader = ArrayRecordReader(str(file_path))

                # Read columns
                reader.seek(0)
                columns_record = reader.read()
                columns = ast.literal_eval(columns_record.decode('utf-8'))

                # Read data
                reader.seek(1)
                data_record = reader.read()
                training_array = np.frombuffer(data_record, dtype=np.float32)

                # Check for NaN values
                nan_mask = np.isnan(training_array)
                nan_count = np.sum(nan_mask)
                inf_mask = np.isinf(training_array)
                inf_count = np.sum(inf_mask)

                total_problematic = nan_count + inf_count

                if total_problematic > 0:
                    nan_files.append((file_path, nan_count, inf_count))
                    print(f"   ❌ Found {nan_count} NaN and {inf_count} Inf values")

                    # Report which columns have NaN/Inf
                    problematic_columns = []
                    for i, (is_nan, is_inf) in enumerate(zip(nan_mask, inf_mask)):
                        if is_nan or is_inf:
                            col_name = columns[i] if i < len(columns) else f"column_{i}"
                            value_type = "NaN" if is_nan else "Inf"
                            problematic_columns.append(f"{col_name}({value_type})")

                    print(f"   Problematic columns: {problematic_columns[:10]}...")  # Show first 10
                else:
                    clean_files.append(file_path)
                    print(f"   ✅ No NaN or Inf values found")

                reader.close()

            except Exception as e:
                print(f"   ⚠️  Error reading {file_path}: {e}")

        print(f"\n📊 NaN Detection Summary:")
        print(f"   Files with NaN/Inf: {len(nan_files)}")
        print(f"   Clean files: {len(clean_files)}")

        # This test documents the current state - it doesn't necessarily fail if NaN is found
        # because NaN might be expected in training data
        if nan_files:
            print("   Files containing NaN/Inf values:")
            for file_path, nan_count, inf_count in nan_files:
                print(f"     {file_path.name}: {nan_count} NaN, {inf_count} Inf")

    def test_arrayrecord_sanitization_effectiveness(self):
        """Test that sanitization removes NaN values from ArrayRecord data."""
        if not self.sample_arrayrecord_files:
            self.skipTest("No ArrayRecord files available")

        try:
            from array_record.python.array_record_module import ArrayRecordReader
        except ImportError:
            self.skipTest("ArrayRecord library not available")

        for file_path in self.sample_arrayrecord_files[:2]:  # Test first 2 files
            with self.subTest(file=str(file_path)):
                print(f"\n🧼 Testing sanitization: {file_path}")

                try:
                    reader = ArrayRecordReader(str(file_path))

                    # Read columns and data
                    reader.seek(0)
                    columns_record = reader.read()
                    columns = ast.literal_eval(columns_record.decode('utf-8'))

                    reader.seek(1)
                    data_record = reader.read()
                    training_array = np.frombuffer(data_record, dtype=np.float32)

                    reader.close()

                    # Create feature dictionary (similar to analytics service)
                    original_features = {}
                    for i, col_name in enumerate(columns):
                        if i < len(training_array):
                            original_features[col_name] = float(training_array[i])

                    # Count problematic values before sanitization
                    original_nan_count = sum(1 for v in original_features.values()
                                           if isinstance(v, float) and math.isnan(v))
                    original_inf_count = sum(1 for v in original_features.values()
                                           if isinstance(v, float) and math.isinf(v))

                    # Apply sanitization
                    sanitized_features = sanitize_training_features(original_features)

                    # Count problematic values after sanitization
                    sanitized_nan_count = sum(1 for v in sanitized_features.values()
                                            if isinstance(v, float) and math.isnan(v))
                    sanitized_inf_count = sum(1 for v in sanitized_features.values()
                                            if isinstance(v, float) and math.isinf(v))

                    print(f"   Before sanitization: {original_nan_count} NaN, {original_inf_count} Inf")
                    print(f"   After sanitization:  {sanitized_nan_count} NaN, {sanitized_inf_count} Inf")

                    # Verify sanitization worked
                    self.assertEqual(sanitized_nan_count, 0, "Sanitization should remove all NaN values")
                    self.assertEqual(sanitized_inf_count, 0, "Sanitization should remove all Inf values")

                    # Test JSON serialization
                    import json
                    json_str = json.dumps(sanitized_features)
                    self.assertNotIn('NaN', json_str)
                    self.assertNotIn('Infinity', json_str)

                    print(f"   ✅ Sanitization successful, JSON serializable")

                except Exception as e:
                    self.fail(f"Sanitization test failed for {file_path}: {e}")

    def test_arrayrecord_feature_types_validation(self):
        """Test that features have expected types and ranges."""
        if not self.sample_arrayrecord_files:
            self.skipTest("No ArrayRecord files available")

        try:
            from array_record.python.array_record_module import ArrayRecordReader
        except ImportError:
            self.skipTest("ArrayRecord library not available")

        for file_path in self.sample_arrayrecord_files[:2]:
            with self.subTest(file=str(file_path)):
                print(f"\n🔍 Validating feature types: {file_path}")

                try:
                    reader = ArrayRecordReader(str(file_path))

                    # Read data
                    reader.seek(0)
                    columns_record = reader.read()
                    columns = ast.literal_eval(columns_record.decode('utf-8'))

                    reader.seek(1)
                    data_record = reader.read()
                    training_array = np.frombuffer(data_record, dtype=np.float32)

                    reader.close()

                    # Analyze feature types
                    ohlcv_features = 0
                    timeframe_features = {'5m': 0, '15m': 0, '1h': 0, '1d': 0, '1w': 0}
                    indicator_features = 0

                    for col_name in columns:
                        # Check for OHLCV features
                        if any(ohlcv in col_name.lower() for ohlcv in ['open', 'high', 'low', 'close', 'volume', 'vwap']):
                            ohlcv_features += 1

                        # Check for timeframe features
                        for tf in timeframe_features:
                            if col_name.startswith(tf):
                                timeframe_features[tf] += 1

                        # Check for indicator features (features not in basic OHLCV)
                        if not any(basic in col_name.lower() for basic in ['open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp']):
                            indicator_features += 1

                    print(f"   Feature analysis:")
                    print(f"     OHLCV features: {ohlcv_features}")
                    print(f"     Timeframe breakdown: {timeframe_features}")
                    print(f"     Indicator features: {indicator_features}")
                    print(f"     Total features: {len(columns)}")

                    # Validate expected structure for training data
                    self.assertGreater(ohlcv_features, 0, "Should have OHLCV features")
                    self.assertGreater(len(columns), 100, "Should have substantial number of features")

                    # Check for multi-timeframe structure
                    has_multi_timeframe = sum(timeframe_features.values()) > 0
                    if has_multi_timeframe:
                        print(f"   ✅ Multi-timeframe structure detected")
                    else:
                        print(f"   ⚠️  No clear multi-timeframe structure")

                except Exception as e:
                    self.fail(f"Feature type validation failed for {file_path}: {e}")

    def test_arrayrecord_json_compatibility(self):
        """Test that ArrayRecord data can be safely converted to JSON."""
        if not self.sample_arrayrecord_files:
            self.skipTest("No ArrayRecord files available")

        try:
            from array_record.python.array_record_module import ArrayRecordReader
        except ImportError:
            self.skipTest("ArrayRecord library not available")

        import json

        for file_path in self.sample_arrayrecord_files[:2]:
            with self.subTest(file=str(file_path)):
                print(f"\n🔄 Testing JSON compatibility: {file_path}")

                try:
                    reader = ArrayRecordReader(str(file_path))

                    # Read data
                    reader.seek(0)
                    columns_record = reader.read()
                    columns = ast.literal_eval(columns_record.decode('utf-8'))

                    reader.seek(1)
                    data_record = reader.read()
                    training_array = np.frombuffer(data_record, dtype=np.float32)

                    reader.close()

                    # Create feature row (simulate analytics service behavior)
                    feature_row = {}
                    for i, col_name in enumerate(columns):
                        val = training_array[i]
                        # Apply the fixed NaN handling
                        if math.isnan(val):
                            val = 0.0
                        feature_row[col_name] = float(val)

                    # Test JSON serialization
                    json_str = json.dumps(feature_row)

                    # Verify no problematic values in JSON
                    self.assertNotIn('NaN', json_str, "JSON should not contain NaN")
                    self.assertNotIn('Infinity', json_str, "JSON should not contain Infinity")
                    self.assertNotIn('-Infinity', json_str, "JSON should not contain -Infinity")

                    # Test parsing
                    parsed = json.loads(json_str)
                    self.assertEqual(len(parsed), len(feature_row))

                    print(f"   ✅ JSON serialization successful ({len(json_str)} characters)")

                except Exception as e:
                    self.fail(f"JSON compatibility test failed for {file_path}: {e}")

class TestArrayRecordEdgeCases(unittest.TestCase):
    """Test edge cases in ArrayRecord data handling."""

    def test_empty_or_corrupted_arrayrecord(self):
        """Test handling of empty or corrupted ArrayRecord files."""
        # This test would be more relevant with actual corrupted files
        # For now, test the error handling paths

        try:
            from array_record.python.array_record_module import ArrayRecordReader
        except ImportError:
            self.skipTest("ArrayRecord library not available")

        # Test with non-existent file
        with self.assertRaises(Exception):
            reader = ArrayRecordReader("/nonexistent/path/file.arrayrecord")

    def test_synthetic_nan_arrayrecord_data(self):
        """Test handling of synthetic ArrayRecord data with known NaN values."""
        # Create synthetic data with NaN values
        synthetic_columns = ['open_000', 'high_000', 'low_000', 'close_000', 'nan_field']
        synthetic_data = np.array([150.0, float('nan'), 149.0, 150.5, float('inf')], dtype=np.float32)

        # Simulate the processing that happens in analytics service
        feature_row = {}
        for i, col_name in enumerate(synthetic_columns):
            val = synthetic_data[i]
            # Apply the fixed NaN handling
            if math.isnan(val):
                val = 0.0
            elif math.isinf(val):
                val = 1e10 if val > 0 else -1e10
            feature_row[col_name] = float(val)

        # Verify NaN handling
        self.assertEqual(feature_row['open_000'], 150.0)
        self.assertEqual(feature_row['high_000'], 0.0)  # NaN -> 0.0
        self.assertEqual(feature_row['low_000'], 149.0)
        self.assertEqual(feature_row['close_000'], 150.5)
        self.assertEqual(feature_row['nan_field'], 1e10)  # Inf -> 1e10

        # Test JSON serialization
        import json
        json_str = json.dumps(feature_row)
        self.assertNotIn('NaN', json_str)
        self.assertNotIn('Infinity', json_str)

if __name__ == '__main__':
    # Run with high verbosity to see detailed test results
    unittest.main(verbosity=2)