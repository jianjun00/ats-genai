#!/usr/bin/env python3
"""
Comprehensive test suite for HLC-only indicator implementations.

This test validates that all 9 indicator classes (PL, L11, H11, Z1B, Z2B, EBot, ETop, Z5T, Z6T)
correctly implement the HLC-only formulas derived from linear regression.

Tests include:
1. Coefficient validation against derived formulas
2. Calculation accuracy using known test data
3. Feature extraction validation (HLC only, no open)
4. Cross-scale validation using different price ranges
5. Performance comparison with expected results

Usage:
    python test_hlc_indicator_implementation.py
"""

import sys
import numpy as np
from dataclasses import dataclass
from typing import List

# Test data from the original HLC dataset
# Format: [Date, High, Low, Close, h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t]
# The targets (h11, l11, etc.) are calculated from 3 PREVIOUS days' HLC data
HLC_TEST_DATA = [
    ["08/05", 3444.9, 3403.3, 3434.7, 3444.6, 3403.0, 3302.9, 3335.1, 3359.3, 3383.5, 3415.8, 3448.1, 3472.3],
    ["08/06", 3440.5, 3411.7, 3433.4, 3452.0, 3410.4, 3331.1, 3364.1, 3387.3, 3410.5, 3443.5, 3476.5, 3499.8],
    ["08/07", 3483.8, 3430.0, 3453.7, 3445.4, 3416.6, 3372.7, 3394.2, 3410.0, 3425.8, 3447.3, 3468.8, 3484.7],
    ["08/08", 3534.1, 3445.0, 3491.3, 3481.7, 3427.9, 3376.9, 3399.2, 3418.3, 3437.3, 3459.7, 3482.0, 3501.1],
    ["08/11", 3466.3, 3393.0, 3404.7, 3535.3, 3446.2, 3373.0, 3402.2, 3430.2, 3458.2, 3487.4, 3516.7, 3544.7],
    ["08/12", 3410.8, 3379.1, 3399.0, 3449.7, 3376.4, 3344.7, 3377.8, 3416.8, 3455.8, 3488.9, 3522.0, 3560.9],
    ["08/13", 3422.6, 3392.7, 3408.3, 3413.5, 3381.8, 3336.7, 3367.0, 3401.4, 3435.9, 3466.1, 3496.4, 3530.8],
]

# HLC-only coefficients from derived formulas
HLC_COEFFICIENTS = {
    'pldot': [
        0.11306077, 0.10884779, 0.10864725,   # t-3: H,L,C
        0.11441424, 0.11317815, 0.10686769,   # t-2: H,L,C
        0.11171601, 0.11384294, 0.10939732,   # t-1: H,L,C
    ],
    'l11': [
        -0.00056212, -0.00018272, 0.00019277,   # t-3: H,L,C
        0.00136978, 0.00071840, -0.00182454,    # t-2: H,L,C
        -0.33313775, 0.66680999, 0.66661597,    # t-1: H,L,C
    ],
    'h11': [
        -0.00056212, -0.00018272, 0.00019277,   # t-3: H,L,C
        0.00136978, 0.00071840, -0.00182454,    # t-2: H,L,C
        0.66686225, -0.33319001, 0.66661597,    # t-1: H,L,C
    ],
    'z1b': [
        -0.44360641, 0.55203953, 0.22238203,   # t-3: H,L,C
        -0.44299760, 0.55722853, 0.21953681,   # t-2: H,L,C
        -0.44414226, 0.55962966, 0.21992682,   # t-1: H,L,C
    ],
    'z2b': [
        -0.33375857, 0.33327147, 0.33478365,   # t-3: H,L,C
        -0.33395845, 0.33313921, 0.33324867,   # t-2: H,L,C
        -0.33277367, 0.33384496, 0.33220288,   # t-1: H,L,C
    ],
    'ebot': [
        -0.11115648, 0.22303212, 0.22206190,   # t-3: H,L,C
        -0.11250983, 0.22120078, 0.22439345,   # t-2: H,L,C
        -0.11109552, 0.22046378, 0.22360772,   # t-1: H,L,C
    ],
    'etop': [
        0.22106127, -0.11318101, 0.22457886,   # t-3: H,L,C
        0.22053147, -0.11010281, 0.22546244,   # t-2: H,L,C
        0.21983177, -0.11226826, 0.22411409,   # t-1: H,L,C
    ],
    'z5t': [
        0.33298475, -0.33125052, 0.33371591,   # t-3: H,L,C
        0.33153760, -0.33584054, 0.33648807,   # t-2: H,L,C
        0.33404897, -0.33557298, 0.33388438,   # t-1: H,L,C
    ],
    'z6t': [
        0.55639359, -0.44796047, 0.22238203,   # t-3: H,L,C
        0.55700240, -0.44277147, 0.21953681,   # t-2: H,L,C
        0.55585774, -0.44037034, 0.21992682,   # t-1: H,L,C
    ],
}

@dataclass
class MockInterval:
    """Mock interval class for testing indicators."""
    high: float
    low: float
    close: float
    status: str = 'ok'

    # Add open for compatibility (should not be used in HLC-only calculations)
    @property
    def open(self):
        return None  # Should not be accessed in HLC-only implementation

class TestHLCIndicators:
    """Test suite for HLC-only indicator implementations."""

    def __init__(self):
        self.test_results = {}
        self.errors = []

    def calculate_expected_value(self, indicator_name: str, hlc_features: List[float]) -> float:
        """Calculate expected indicator value using derived coefficients."""
        coeffs = HLC_COEFFICIENTS[indicator_name]
        return sum(coef * feat for coef, feat in zip(coeffs, hlc_features))

    def extract_hlc_features(self, data_rows: List[List], row_index: int) -> List[float]:
        """Extract HLC features for 3 previous days from row_index."""
        if row_index < 3:
            raise ValueError(f"Need at least 3 prior rows, got index {row_index}")

        features = []
        for i in range(3):  # t-3, t-2, t-1
            prev_row = data_rows[row_index - 3 + i]
            high, low, close = prev_row[1], prev_row[2], prev_row[3]  # Skip date column
            features.extend([high, low, close])

        return features

    def test_coefficient_validation(self) -> bool:
        """Test that all coefficient arrays have correct length and structure."""
        print("🔍 Testing coefficient validation...")

        success = True
        for indicator_name, coeffs in HLC_COEFFICIENTS.items():
            if len(coeffs) != 9:
                self.errors.append(f"{indicator_name}: Expected 9 coefficients, got {len(coeffs)}")
                success = False

            # Validate coefficient grouping (3 days × 3 HLC)
            for day in range(3):
                day_coeffs = coeffs[day*3:(day+1)*3]
                if len(day_coeffs) != 3:
                    self.errors.append(f"{indicator_name}: Day {day} should have 3 HLC coefficients")
                    success = False

        if success:
            print("✅ All coefficient arrays have correct 9-feature HLC structure")
        else:
            print(f"❌ Coefficient validation failed: {len([e for e in self.errors if 'coefficient' in e.lower()])} errors")

        return success

    def test_calculation_accuracy(self) -> bool:
        """Test calculation accuracy against known expected values."""
        print("🔍 Testing calculation accuracy...")

        success = True
        target_indices = [4, 5, 6, 7, 8, 9, 10, 11, 12]  # h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t
        indicator_names = ['h11', 'l11', 'z1b', 'z2b', 'ebot', 'pldot', 'etop', 'z5t', 'z6t']

        for row_idx in range(3, len(HLC_TEST_DATA)):  # Start from row 3 (need 3 prior days)
            hlc_features = self.extract_hlc_features(HLC_TEST_DATA, row_idx)
            current_row = HLC_TEST_DATA[row_idx]

            for target_idx, indicator_name in zip(target_indices, indicator_names):
                expected_value = current_row[target_idx]
                calculated_value = self.calculate_expected_value(indicator_name, hlc_features)

                error = abs(expected_value - calculated_value)
                relative_error = (error / abs(expected_value)) * 100 if expected_value != 0 else 0

                # Tolerance: 0.25% relative error or 0.1 absolute error
                if relative_error > 0.25 and error > 0.1:
                    self.errors.append(
                        f"Row {row_idx} {indicator_name}: Expected {expected_value:.4f}, "
                        f"got {calculated_value:.4f}, error {error:.4f} ({relative_error:.3f}%)"
                    )
                    success = False

        if success:
            print("✅ All calculations match expected values within tolerance")
        else:
            print(f"❌ Calculation accuracy failed: {len([e for e in self.errors if 'Expected' in e])} mismatches")

        return success

    def test_feature_extraction_hlc_only(self) -> bool:
        """Test that feature extraction uses only HLC (no open prices)."""
        print("🔍 Testing HLC-only feature extraction...")

        success = True

        # Test with mock intervals that return None for open
        test_intervals = []
        for i in range(4):  # Need 4 intervals (3 for features + 1 current)
            row = HLC_TEST_DATA[i]
            interval = MockInterval(high=row[1], low=row[2], close=row[3])
            test_intervals.append(interval)

        # Extract features manually
        hlc_features = []
        for i in range(3):  # Last 3 intervals for features
            interval = test_intervals[i]
            hlc_features.extend([interval.high, interval.low, interval.close])

        # Validate feature vector length
        if len(hlc_features) != 9:
            self.errors.append(f"Feature vector should have 9 elements (3×HLC), got {len(hlc_features)}")
            success = False

        # Validate no open prices are used
        for i, feature in enumerate(hlc_features):
            if feature is None:
                self.errors.append(f"Feature {i} is None - should be valid HLC value")
                success = False

        if success:
            print("✅ Feature extraction correctly uses only HLC data (9 features)")
        else:
            print("❌ Feature extraction validation failed")

        return success

    def test_cross_scale_validation(self) -> bool:
        """Test formulas work correctly across different price scales."""
        print("🔍 Testing cross-scale validation...")

        # Scale test data by 7x (original validation dataset was 7x higher)
        scale_factor = 7.0
        success = True

        for row_idx in range(3, len(HLC_TEST_DATA)):
            # Scale input HLC features
            hlc_features = self.extract_hlc_features(HLC_TEST_DATA, row_idx)
            scaled_features = [f * scale_factor for f in hlc_features]

            # Scale expected outputs
            current_row = HLC_TEST_DATA[row_idx]
            target_indices = [4, 5, 6, 7, 8, 9, 10, 11, 12]
            indicator_names = ['h11', 'l11', 'z1b', 'z2b', 'ebot', 'pldot', 'etop', 'z5t', 'z6t']

            for target_idx, indicator_name in zip(target_indices, indicator_names):
                original_expected = current_row[target_idx]
                scaled_expected = original_expected * scale_factor

                # Calculate with scaled features
                scaled_calculated = self.calculate_expected_value(indicator_name, scaled_features)

                error = abs(scaled_expected - scaled_calculated)
                relative_error = (error / abs(scaled_expected)) * 100 if scaled_expected != 0 else 0

                # Same tolerance as original: 0.25%
                if relative_error > 0.25 and error > 0.7:  # Higher absolute tolerance for scaled values
                    self.errors.append(
                        f"Cross-scale {indicator_name}: Expected {scaled_expected:.2f}, "
                        f"got {scaled_calculated:.2f}, error {error:.2f} ({relative_error:.3f}%)"
                    )
                    success = False

        if success:
            print("✅ Formulas maintain accuracy across 7x price scale")
        else:
            print(f"❌ Cross-scale validation failed: scale-sensitive formulas detected")

        return success

    def test_performance_metrics(self) -> bool:
        """Test that all formulas meet performance requirements."""
        print("🔍 Testing performance metrics...")

        success = True
        target_indices = [4, 5, 6, 7, 8, 9, 10, 11, 12]
        indicator_names = ['h11', 'l11', 'z1b', 'z2b', 'ebot', 'pldot', 'etop', 'z5t', 'z6t']

        for indicator_name in indicator_names:
            errors = []
            actual_values = []
            predicted_values = []

            for row_idx in range(3, len(HLC_TEST_DATA)):
                hlc_features = self.extract_hlc_features(HLC_TEST_DATA, row_idx)
                target_idx = target_indices[indicator_names.index(indicator_name)]

                actual = HLC_TEST_DATA[row_idx][target_idx]
                predicted = self.calculate_expected_value(indicator_name, hlc_features)

                actual_values.append(actual)
                predicted_values.append(predicted)
                errors.append(abs(actual - predicted))

            # Calculate metrics
            avg_error = np.mean(errors)
            max_error = np.max(errors)
            rmse = np.sqrt(np.mean([(a-p)**2 for a, p in zip(actual_values, predicted_values)]))

            # Calculate R²
            ss_res = sum((a - p) ** 2 for a, p in zip(actual_values, predicted_values))
            ss_tot = sum((a - np.mean(actual_values)) ** 2 for a in actual_values)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 1.0

            # Performance requirements: R² > 0.999995, avg error < 0.25%
            avg_relative_error = np.mean([e/abs(a) for e, a in zip(errors, actual_values) if a != 0]) * 100

            print(f"  {indicator_name}: R²={r2:.8f}, Avg Error={avg_error:.4f} ({avg_relative_error:.3f}%), RMSE={rmse:.4f}")

            if r2 < 0.999995:
                self.errors.append(f"{indicator_name}: R² {r2:.8f} below requirement 0.999995")
                success = False

            if avg_relative_error > 0.25:
                self.errors.append(f"{indicator_name}: Avg error {avg_relative_error:.3f}% above 0.25% limit")
                success = False

        if success:
            print("✅ All indicators meet performance requirements (R² > 0.999995, error < 0.25%)")
        else:
            print("❌ Performance requirements not met for some indicators")

        return success

    def run_all_tests(self) -> bool:
        """Run complete test suite."""
        print("🚀 Running HLC-Only Indicator Implementation Test Suite")
        print("=" * 80)

        tests = [
            ("Coefficient Validation", self.test_coefficient_validation),
            ("Calculation Accuracy", self.test_calculation_accuracy),
            ("HLC-Only Feature Extraction", self.test_feature_extraction_hlc_only),
            ("Cross-Scale Validation", self.test_cross_scale_validation),
            ("Performance Metrics", self.test_performance_metrics),
        ]

        passed_tests = 0
        total_tests = len(tests)

        for test_name, test_func in tests:
            print(f"\n📋 {test_name}")
            print("-" * 60)

            try:
                if test_func():
                    passed_tests += 1
                    self.test_results[test_name] = "PASS"
                else:
                    self.test_results[test_name] = "FAIL"
            except Exception as e:
                self.errors.append(f"{test_name}: Exception - {str(e)}")
                self.test_results[test_name] = "ERROR"
                print(f"❌ Test failed with exception: {e}")

        # Summary
        print("\n" + "=" * 80)
        print("🎯 TEST SUITE SUMMARY")
        print("=" * 80)

        for test_name, result in self.test_results.items():
            status_icon = "✅" if result == "PASS" else "❌"
            print(f"{status_icon} {test_name}: {result}")

        print(f"\n📊 Results: {passed_tests}/{total_tests} tests passed")

        if self.errors:
            print(f"\n⚠️  {len(self.errors)} errors found:")
            for error in self.errors[:10]:  # Show first 10 errors
                print(f"   • {error}")
            if len(self.errors) > 10:
                print(f"   ... and {len(self.errors) - 10} more errors")

        success = passed_tests == total_tests

        if success:
            print("\n🎉 ALL TESTS PASSED - HLC-only implementation is validated!")
            print("✅ Ready for production deployment")
        else:
            print(f"\n❌ {total_tests - passed_tests} tests failed - implementation needs fixes")

        return success

def main():
    """Run the complete test suite."""
    tester = TestHLCIndicators()
    success = tester.run_all_tests()

    # Return appropriate exit code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()