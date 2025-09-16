#!/usr/bin/env python3
"""
Unit Tests for Training Data Serialization Issues

Tests to detect and prevent serialization errors that cause training data generation to fail silently.
This addresses the specific issue where datetime objects can't be JSON serialized.
"""

import pytest
import json
import numpy as np
from datetime import datetime, date

from ml.training_data.generators.training_data_metadata import TrainingDataMetadataManager, FeatureType


class TestTrainingDataSerialization:
    """Test serialization of training data metadata and objects."""

    def test_datetime_serialization_in_metadata(self):
        """Test that datetime objects can be properly serialized in metadata."""
        manager = TrainingDataMetadataManager()

        # Create test data with datetime objects
        test_data = {
            "start_time": datetime(2025, 9, 5, 10, 30, 0),
            "end_time": datetime(2025, 9, 5, 16, 0, 0),
            "creation_date": date(2025, 9, 5),
            "feature_count": 42,
            "symbols": ["AAPL", "TSLA"]
        }

        # This should NOT raise "Object of type datetime is not JSON serializable"
        try:
            serialized = json.dumps(test_data, default=manager._json_serializer)
            # Verify it's valid JSON
            deserialized = json.loads(serialized)

            # Verify datetime objects were converted to ISO format strings
            assert isinstance(deserialized["start_time"], str)
            assert deserialized["start_time"] == "2025-09-05T10:30:00"
            assert isinstance(deserialized["end_time"], str)
            assert deserialized["end_time"] == "2025-09-05T16:00:00"
            assert isinstance(deserialized["creation_date"], str)
            assert deserialized["creation_date"] == "2025-09-05"

        except TypeError as e:
            pytest.fail(f"Datetime serialization failed: {e}")

    def test_numpy_array_serialization(self):
        """Test that numpy arrays can be properly serialized."""
        manager = TrainingDataMetadataManager()

        test_data = {
            "features": np.array([1.0, 2.5, 3.7]),
            "integers": np.array([1, 2, 3], dtype=np.int32),
            "floats": np.array([1.1, 2.2, 3.3], dtype=np.float32)
        }

        try:
            serialized = json.dumps(test_data, default=manager._json_serializer)
            deserialized = json.loads(serialized)

            # Verify numpy arrays were converted to lists
            assert isinstance(deserialized["features"], list)
            assert deserialized["features"] == [1.0, 2.5, 3.7]
            assert isinstance(deserialized["integers"], list)
            assert deserialized["integers"] == [1, 2, 3]

        except TypeError as e:
            pytest.fail(f"Numpy array serialization failed: {e}")

    def test_enum_serialization(self):
        """Test that enum objects can be properly serialized."""
        manager = TrainingDataMetadataManager()

        test_data = {
            "feature_type": FeatureType.OHLC,
            "another_type": FeatureType.PRICE_INDICATOR
        }

        try:
            serialized = json.dumps(test_data, default=manager._json_serializer)
            deserialized = json.loads(serialized)

            # Verify enums were converted to their values
            assert deserialized["feature_type"] == "ohlc"
            assert deserialized["another_type"] == "price_indicator"

        except TypeError as e:
            pytest.fail(f"Enum serialization failed: {e}")

    def test_unsupported_object_raises_error(self):
        """Test that unsupported objects still raise proper errors."""
        manager = TrainingDataMetadataManager()

        class UnsupportedObject:
            def __init__(self):
                self.value = "test"

        test_data = {
            "unsupported": UnsupportedObject()
        }

        with pytest.raises(TypeError, match="Object of type .* is not JSON serializable"):
            json.dumps(test_data, default=manager._json_serializer)

    def test_real_world_metadata_serialization(self):
        """Test serialization of realistic training data metadata."""
        manager = TrainingDataMetadataManager()

        # This mimics the actual metadata structure from training data generation
        real_metadata = {
            "symbol": "AAPL",
            "start_time": datetime(2025, 7, 1, 0, 0, 0),
            "end_time": datetime(2025, 9, 5, 0, 0, 0),
            "creation_timestamp": datetime.now(),
            "feature_count": 256,
            "sequence_length": 60,
            "timeframes": ["5m", "15m", "1h", "1d"],
            "technical_indicators": [
                {"name": "envelope_top", "type": FeatureType.PRICE_INDICATOR},
                {"name": "envelope_bot", "type": FeatureType.PRICE_INDICATOR}
            ],
            "feature_arrays": {
                "ohlc_data": np.array([[100.0, 101.0, 99.5, 100.5]] * 60),
                "volumes": np.array([1000000] * 60, dtype=np.int64)
            },
            "data_quality_metrics": {
                "completeness": 0.98,
                "null_percentage": 0.02
            }
        }

        # This should serialize without errors
        try:
            serialized = json.dumps(real_metadata, default=manager._json_serializer)
            deserialized = json.loads(serialized)

            # Spot check key conversions
            assert isinstance(deserialized["start_time"], str)
            assert isinstance(deserialized["creation_timestamp"], str)
            assert isinstance(deserialized["feature_arrays"]["ohlc_data"], list)
            assert isinstance(deserialized["technical_indicators"][0]["type"], str)

        except TypeError as e:
            pytest.fail(f"Real-world metadata serialization failed: {e}")


class TestTrainingDataFailFast:
    """Test that training data generation fails fast on errors."""

    def test_arrayrecord_save_failure_raises_exception(self):
        """Test that ArrayRecord save failures raise exceptions instead of logging and continuing."""
        from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

        callback = IntervalBasedTrainingDataCallback(
            output_dir="/tmp/test_training_data",
            start_date=date(2025, 7, 1),
            end_date=date(2025, 9, 5)
        )

        # Mock a save operation that would fail
        import unittest.mock

        with unittest.mock.patch('pathlib.Path.mkdir', side_effect=OSError("Permission denied")):
            with pytest.raises(RuntimeError, match="Critical error saving ArrayRecord"):
                # This would previously log error and continue - now should raise
                import asyncio
                asyncio.run(callback._save_symbol_arrayrecord(
                    [{"test": "data"}],
                    callback.output_dir / "test.arrayrecord",
                    "TEST"
                ))

    def test_metadata_save_failure_raises_exception(self):
        """Test that metadata save failures raise exceptions instead of logging and continuing."""
        from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

        callback = IntervalBasedTrainingDataCallback(
            output_dir="/tmp/test_training_data",
            start_date=date(2025, 7, 1),
            end_date=date(2025, 9, 5)
        )

        # Mock a save operation that would fail
        import unittest.mock

        with unittest.mock.patch('builtins.open', side_effect=OSError("Permission denied")):
            with pytest.raises(RuntimeError, match="Critical error saving metadata"):
                import asyncio
                asyncio.run(callback._save_symbol_metadata(
                    [{"test": "data"}],
                    callback.output_dir / "test_metadata.json",
                    "TEST",
                    datetime.now()
                ))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])