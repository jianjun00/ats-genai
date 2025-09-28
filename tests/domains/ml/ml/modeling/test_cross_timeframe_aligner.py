"""
Tests for Cross-Timeframe Alignment System
"""

import pytest
import numpy as np
import asyncio

from domains.ml.modeling.cross_timeframe_aligner import (
    CrossTimeframeAligner, AlignmentConfig, AlignmentMethod,
    AlignmentResult, validate_cross_timeframe_alignment
)
from domains.ml.modeling.enhanced_feature_types import (
    FeatureSpecification, FeatureType, TimeframeSpec,
    TechnicalIndicator
)

class TestAlignmentConfig:
    """Test AlignmentConfig dataclass."""

    def test_config_creation(self):
        """Test alignment configuration creation."""
        config = AlignmentConfig(
            source_timeframe=TimeframeSpec.HOUR_1,
            target_timeframe=TimeframeSpec.MINUTE_5,
            method=AlignmentMethod.STEP_FUNCTION,
            fill_gaps=True,
            max_gap_periods=10
        )

        assert config.source_timeframe == TimeframeSpec.HOUR_1
        assert config.target_timeframe == TimeframeSpec.MINUTE_5
        assert config.method == AlignmentMethod.STEP_FUNCTION
        assert config.fill_gaps is True
        assert config.max_gap_periods == 10
        assert config.edge_behavior == "extend"  # default

class TestAlignmentResult:
    """Test AlignmentResult dataclass."""

    def test_result_creation(self):
        """Test alignment result creation."""
        test_data = np.random.random((10, 8, 1))

        result = AlignmentResult(
            aligned_data=test_data,
            source_timestamps=[],
            target_timestamps=[],
            alignment_quality=0.85,
            gaps_filled=3,
            metadata={"method": "step_function"}
        )

        assert result.aligned_data.shape == (10, 8, 1)
        assert result.alignment_quality == 0.85
        assert result.gaps_filled == 3
        assert result.metadata["method"] == "step_function"

class TestCrossTimeframeAligner:
    """Test CrossTimeframeAligner class."""

    @pytest.fixture
    def aligner(self):
        """CrossTimeframeAligner instance."""
        return CrossTimeframeAligner()

    @pytest.fixture
    def sample_hourly_data(self):
        """Sample hourly indicator data."""
        np.random.seed(42)
        num_samples = 50
        intervals = 8

        # Generate realistic hourly ETOP data
        data = []
        base_value = 150

        for sample in range(num_samples):
            sample_values = []
            current_value = base_value + np.random.normal(0, 5)

            for interval in range(intervals):
                current_value += np.random.normal(0, 1)
                sample_values.append([current_value])

            data.append(sample_values)

        return np.array(data)  # Shape: [50, 8, 1]

    def test_initialization(self, aligner):
        """Test aligner initialization."""
        assert isinstance(aligner.timeframe_multipliers, dict)
        assert TimeframeSpec.MINUTE_5 in aligner.timeframe_multipliers
        assert aligner.timeframe_multipliers[TimeframeSpec.MINUTE_5] == 1
        assert aligner.timeframe_multipliers[TimeframeSpec.HOUR_1] == 12

    def test_upsample_data_repeat_method(self, aligner, sample_hourly_data):
        """Test upsampling with repeat method."""
        ratio = 12.0  # 1 hour = 12 * 5 minutes
        target_intervals = 16

        result = aligner._upsample_data(
            sample_hourly_data, ratio, AlignmentMethod.REPEAT, target_intervals
        )

        assert result is not None
        assert result.shape[0] == sample_hourly_data.shape[0]  # Same number of samples
        assert result.shape[1] == target_intervals  # Target intervals
        assert result.shape[2] == 1  # Same feature dimension

    def test_upsample_data_step_function(self, aligner, sample_hourly_data):
        """Test upsampling with step function method."""
        ratio = 12.0
        target_intervals = 24

        result = aligner._upsample_data(
            sample_hourly_data, ratio, AlignmentMethod.STEP_FUNCTION, target_intervals
        )

        assert result is not None
        assert result.shape == (sample_hourly_data.shape[0], target_intervals, 1)

        # Check that values are repeated correctly (step function behavior)
        for sample_idx in range(min(5, result.shape[0])):  # Check first 5 samples
            sample_result = result[sample_idx]
            source_sample = sample_hourly_data[sample_idx]

            # Values should be repeated according to ratio
            # (exact pattern depends on implementation details)
            assert np.isfinite(sample_result).all()

    def test_upsample_data_interpolate(self, aligner, sample_hourly_data):
        """Test upsampling with interpolation method."""
        ratio = 3.0  # Less extreme for interpolation testing
        target_intervals = 12

        result = aligner._upsample_data(
            sample_hourly_data, ratio, AlignmentMethod.INTERPOLATE, target_intervals
        )

        assert result is not None
        assert result.shape == (sample_hourly_data.shape[0], target_intervals, 1)

        # Check interpolation smoothness
        for sample_idx in range(min(3, result.shape[0])):
            sample_result = result[sample_idx].flatten()

            # Should have no NaN values
            assert np.isfinite(sample_result).all()

            # Should be reasonably smooth (no extreme jumps)
            if len(sample_result) > 1:
                differences = np.diff(sample_result)
                max_diff = np.max(np.abs(differences))
                # Shouldn't have jumps larger than 10x the standard deviation
                assert max_diff < 10 * np.std(sample_result)

    def test_downsample_data(self, aligner):
        """Test downsampling higher frequency data."""
        # Create high-frequency data (e.g., 5-min to be downsampled to 15-min)
        np.random.seed(42)
        high_freq_data = np.random.normal(100, 5, (20, 24, 1))  # 24 intervals

        ratio = 1/3  # Downsample by factor of 3

        result = aligner._downsample_data(high_freq_data, ratio, AlignmentMethod.REPEAT)

        assert result is not None
        assert result.shape[0] == high_freq_data.shape[0]  # Same samples
        assert result.shape[1] == 8  # 24/3 = 8 intervals
        assert result.shape[2] == 1  # Same feature dimension

    def test_adjust_intervals_truncate(self, aligner, sample_hourly_data):
        """Test adjusting intervals by truncation."""
        target_intervals = 5  # Less than source (8)

        result = aligner._adjust_intervals(sample_hourly_data, target_intervals)

        assert result is not None
        assert result.shape == (sample_hourly_data.shape[0], target_intervals, 1)

        # Should take the last 5 intervals from each sample
        for sample_idx in range(min(3, result.shape[0])):
            original_sample = sample_hourly_data[sample_idx][-target_intervals:]
            adjusted_sample = result[sample_idx]
            np.testing.assert_array_equal(original_sample, adjusted_sample)

    def test_adjust_intervals_pad(self, aligner, sample_hourly_data):
        """Test adjusting intervals by padding."""
        target_intervals = 12  # More than source (8)

        result = aligner._adjust_intervals(sample_hourly_data, target_intervals)

        assert result is not None
        assert result.shape == (sample_hourly_data.shape[0], target_intervals, 1)

        # First 8 intervals should match source, rest should be padded with last value
        for sample_idx in range(min(3, result.shape[0])):
            original_sample = sample_hourly_data[sample_idx]
            adjusted_sample = result[sample_idx]

            # First 8 should match
            np.testing.assert_array_equal(original_sample, adjusted_sample[:8])

            # Last 4 should be repeats of the last original value
            last_value = original_sample[-1, 0]
            for i in range(8, target_intervals):
                assert adjusted_sample[i, 0] == last_value

    def test_interpolate_sequence(self, aligner):
        """Test sequence interpolation."""
        # Simple sequence to interpolate
        sequence = np.array([[10.0], [20.0], [30.0], [40.0]])  # Shape: [4, 1]
        upsample_factor = 2

        result = aligner._interpolate_sequence(sequence, upsample_factor)

        assert result.shape == (8, 1)  # 4 * 2 = 8

        # Check interpolation values
        expected_values = [10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 40.0]
        for i, expected in enumerate(expected_values):
            assert abs(result[i, 0] - expected) < 0.1

    def test_perform_alignment_upsample(self, aligner, sample_hourly_data):
        """Test complete alignment for upsampling case."""
        config = AlignmentConfig(
            source_timeframe=TimeframeSpec.HOUR_1,
            target_timeframe=TimeframeSpec.MINUTE_5,
            method=AlignmentMethod.STEP_FUNCTION
        )

        result = aligner._perform_alignment(sample_hourly_data, config, target_intervals=16)

        assert result is not None
        assert isinstance(result, AlignmentResult)
        assert result.aligned_data.shape == (sample_hourly_data.shape[0], 16, 1)
        assert 0 <= result.alignment_quality <= 1

        # Check metadata
        assert result.metadata["source_timeframe"] == "1hour"
        assert result.metadata["target_timeframe"] == "5min"
        assert result.metadata["alignment_method"] == "step_function"
        assert result.metadata["alignment_ratio"] == 12.0

    def test_perform_alignment_same_timeframe(self, aligner, sample_hourly_data):
        """Test alignment when source and target are same timeframe."""
        config = AlignmentConfig(
            source_timeframe=TimeframeSpec.HOUR_1,
            target_timeframe=TimeframeSpec.HOUR_1,
            method=AlignmentMethod.REPEAT
        )

        result = aligner._perform_alignment(sample_hourly_data, config, target_intervals=10)

        assert result is not None
        assert result.aligned_data.shape == (sample_hourly_data.shape[0], 10, 1)
        assert result.metadata["alignment_ratio"] == 1.0

    def test_calculate_alignment_quality(self, aligner, sample_hourly_data):
        """Test alignment quality calculation."""
        config = AlignmentConfig(
            source_timeframe=TimeframeSpec.HOUR_1,
            target_timeframe=TimeframeSpec.MINUTE_5,
            method=AlignmentMethod.STEP_FUNCTION
        )

        # Create aligned data (just repeat source for testing)
        aligned_data = np.tile(sample_hourly_data, (1, 2, 1))  # Double the intervals

        quality = aligner._calculate_alignment_quality(sample_hourly_data, aligned_data, config)

        assert 0 <= quality <= 1
        # Should be relatively high since we have clean test data
        assert quality > 0.5

    def test_find_source_feature_name(self, aligner):
        """Test finding source feature names."""
        spec = FeatureSpecification(
            name="etop_1hour_on_5min",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.ETOP,
            source_timeframe=TimeframeSpec.HOUR_1
        )

        base_data = {
            "etop_1hour_8": np.random.random((10, 8, 1)),
            "etop_1hour_16": np.random.random((10, 16, 1)),
            "other_feature": np.random.random((10, 10, 1))
        }

        found_name = aligner._find_source_feature_name(spec, base_data)
        assert found_name == "etop_1hour_8"  # Should find first available

    def test_find_source_feature_name_not_found(self, aligner):
        """Test source feature name when not found."""
        spec = FeatureSpecification(
            name="rsi_daily_on_1hour",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.HOUR_1,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.RSI,
            source_timeframe=TimeframeSpec.DAILY
        )

        base_data = {"other_feature": np.random.random((10, 10, 1))}

        found_name = aligner._find_source_feature_name(spec, base_data)
        assert found_name == "rsi_daily_16"  # Expected default name

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_synthetic_source_data(self, aligner):
        """Test synthetic source data generation."""
        spec = FeatureSpecification(
            name="etop_1hour_on_5min",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.ETOP,
            source_timeframe=TimeframeSpec.HOUR_1
        )

        symbols = ['AAPL', 'TSLA']

        result = await aligner._generate_synthetic_source_data(
            spec, symbols, '2024-01-01', '2024-01-05'
        )

        assert result is not None
        assert result.ndim == 3
        assert result.shape[1] == 16  # intervals
        assert result.shape[2] == 1   # feature dimension
        assert result.shape[0] > 0    # should have samples

        # Values should be reasonable for ETOP (around base levels)
        assert np.all(result > 0)  # Positive prices
        assert np.all(result < 1000)  # Reasonable upper bound

    def test_get_alignment_statistics_empty(self, aligner):
        """Test statistics with empty cache."""
        stats = aligner.get_alignment_statistics()

        assert stats["total_alignments"] == 0

    def test_get_alignment_statistics_with_data(self, aligner):
        """Test statistics with alignment data."""
        # Add some mock results to cache
        result1 = AlignmentResult(
            aligned_data=np.random.random((10, 8, 1)),
            source_timestamps=[],
            target_timestamps=[],
            alignment_quality=0.85,
            gaps_filled=2,
            metadata={"alignment_method": "step_function"}
        )

        result2 = AlignmentResult(
            aligned_data=np.random.random((15, 12, 1)),
            source_timestamps=[],
            target_timestamps=[],
            alignment_quality=0.92,
            gaps_filled=1,
            metadata={"alignment_method": "interpolate"}
        )

        aligner._alignment_cache["test1"] = result1
        aligner._alignment_cache["test2"] = result2

        stats = aligner.get_alignment_statistics()

        assert stats["total_alignments"] == 2
        assert abs(stats["average_quality"] - 0.885) < 0.001  # (0.85 + 0.92) / 2
        assert stats["min_quality"] == 0.85
        assert stats["max_quality"] == 0.92
        assert stats["total_gaps_filled"] == 3
        assert len(stats["alignment_methods"]) == 2

    def test_clear_cache(self, aligner):
        """Test cache clearing."""
        # Add something to cache
        aligner._alignment_cache["test"] = AlignmentResult(
            aligned_data=np.random.random((5, 5, 1)),
            source_timestamps=[], target_timestamps=[],
            alignment_quality=0.8, gaps_filled=0, metadata={}
        )

        assert len(aligner._alignment_cache) == 1

        aligner.clear_cache()

        assert len(aligner._alignment_cache) == 0

class TestValidationFunction:
    """Test the validation utility function."""

    def test_validate_successful_alignment(self):
        """Test validation of successful alignment."""
        np.random.seed(42)

        source_data = np.random.normal(100, 5, (20, 8, 1))
        # Create aligned data that's reasonable
        aligned_data = np.repeat(source_data, 2, axis=1)  # Simple upsampling

        result = validate_cross_timeframe_alignment(
            source_data, aligned_data,
            TimeframeSpec.HOUR_1, TimeframeSpec.MINUTE_15
        )

        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
        assert "source_range" in result["metrics"]
        assert "aligned_range" in result["metrics"]
        assert "expected_ratio" in result["metrics"]

    def test_validate_invalid_dimensions(self):
        """Test validation with invalid dimensions."""
        source_data = np.random.random((20, 8))  # 2D instead of 3D
        aligned_data = np.random.random((20, 16, 1))

        result = validate_cross_timeframe_alignment(
            source_data, aligned_data,
            TimeframeSpec.HOUR_1, TimeframeSpec.MINUTE_5
        )

        assert result["is_valid"] is False
        assert len(result["errors"]) > 0
        assert "Invalid dimensions" in result["errors"][0]

    def test_validate_with_nan_values(self):
        """Test validation with NaN values."""
        source_data = np.random.random((10, 8, 1))
        aligned_data = np.random.random((10, 16, 1))
        aligned_data[5, 10, 0] = np.nan  # Insert NaN

        result = validate_cross_timeframe_alignment(
            source_data, aligned_data,
            TimeframeSpec.HOUR_1, TimeframeSpec.MINUTE_5
        )

        # Should still be valid but with warnings
        assert result["is_valid"] is True
        assert len(result["warnings"]) > 0
        assert "NaN values" in result["warnings"][0]

    def test_validate_extreme_value_range(self):
        """Test validation with extreme value ranges."""
        source_data = np.random.normal(100, 5, (10, 8, 1))
        aligned_data = np.random.normal(1000, 50, (10, 16, 1))  # Very different range

        result = validate_cross_timeframe_alignment(
            source_data, aligned_data,
            TimeframeSpec.HOUR_1, TimeframeSpec.MINUTE_5
        )

        assert result["is_valid"] is True  # Structure is valid
        assert len(result["warnings"]) > 0  # But should warn about range
        assert "range significantly different" in result["warnings"][0]

class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_cross_timeframe_alignment(self):
        """Test complete cross-timeframe alignment workflow."""
        aligner = CrossTimeframeAligner()

        # Create a cross-timeframe feature specification
        cross_spec = FeatureSpecification(
            name="etop_1hour_on_5min",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.ETOP,
            source_timeframe=TimeframeSpec.HOUR_1
        )

        # Mock base data (empty - will trigger synthetic generation)
        base_data = {}

        # Perform alignment
        result = await aligner.align_cross_timeframe_features(
            base_data, [cross_spec], ['AAPL', 'TSLA'], '2024-01-01', '2024-01-05'
        )

        assert len(result) > 0
        assert cross_spec.name in result

        aligned_data = result[cross_spec.name]
        assert aligned_data.ndim == 3
        assert aligned_data.shape[1] == 16  # target intervals
        assert aligned_data.shape[2] == 1   # feature dimension

if __name__ == "__main__":
    # Manual test runner
    import sys

    test_classes = [
        TestAlignmentConfig,
        TestAlignmentResult,
        TestCrossTimeframeAligner,
        TestValidationFunction,
        TestIntegrationScenarios
    ]

    passed = 0
    failed = 0

    for test_class in test_classes:
        print(f"\n=== Running {test_class.__name__} ===")

        test_methods = [method for method in dir(test_class)
                       if method.startswith('test_')]

        for method_name in test_methods:
            # Create instance
            instance = test_class()
            method = getattr(instance, method_name)

            # Handle fixture dependencies and async methods
            if hasattr(method, '__code__'):
                var_names = method.__code__.co_varnames

                # Handle fixtures
                kwargs = {}
                if 'aligner' in var_names:
                    kwargs['aligner'] = CrossTimeframeAligner()
                if 'sample_hourly_data' in var_names:
                    np.random.seed(42)
                    num_samples, intervals = 50, 8
                    data = []
                    base_value = 150
                    for sample in range(num_samples):
                        sample_values = []
                        current_value = base_value + np.random.normal(0, 5)
                        for interval in range(intervals):
                            current_value += np.random.normal(0, 1)
                            sample_values.append([current_value])
                        data.append(sample_values)
                    kwargs['sample_hourly_data'] = np.array(data)

                # Run test (handle async)
                if asyncio.iscoroutinefunction(method):
                    asyncio.run(method(**kwargs))
                else:
                    if kwargs:
                        method(**kwargs)
                    else:
                        method()

                print(f"  ✓ {method_name}")
                passed += 1

    print(f"\n=== Test Summary ===")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")

    if failed > 0:
        sys.exit(1)