#!/usr/bin/env python3
"""
Integration tests for ATS indicator classes.

This test suite directly tests the actual indicator implementations in src/signals/indicator.py
to ensure they work correctly with real data and edge cases.

Tests all 11 indicators:
- 9 HLC linear regression: PL, L11, H11, Z1B, Z2B, EBot, ETop, Z5T, Z6T
- 2 Five Nine arithmetic: FiveNineSell, FiveNineBuy

Usage:
    PYTHONPATH=src python test_indicator_integration.py
"""

import sys
import os
import time
import math
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

# Add src to path to import indicators
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from domains.trading.services.indicator import (
        PL, L11, H11, Z1B, Z2B, EBot, ETop, Z5T, Z6T,
        FiveNineSell, FiveNineBuy, InstrumentInterval
    )
except ImportError as e:
    print(f"❌ Cannot import indicators: {e}")
    print("Make sure to run: PYTHONPATH=src python test_indicator_integration.py")
    sys.exit(1)

@dataclass
class TestInstrumentInterval:
    """Test implementation of InstrumentInterval."""
    high: float
    low: float
    close: float
    open: Optional[float] = None
    status: str = 'ok'
    timestamp: Optional[datetime] = None
    volume: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.open is None:
            self.open = self.close  # Simple default

class IndicatorIntegrationTests:
    """Integration tests for real indicator implementations."""

    def __init__(self):
        self.test_results = {}
        self.errors = []
        self.performance_metrics = {}

        # Test data from original HLC dataset
        self.test_data = [
            [3444.9, 3403.3, 3434.7, 3444.6, 3403.0, 3302.9, 3335.1, 3359.3, 3383.5, 3415.8, 3448.1, 3472.3],
            [3440.5, 3411.7, 3433.4, 3452.0, 3410.4, 3331.1, 3364.1, 3387.3, 3410.5, 3443.5, 3476.5, 3499.8],
            [3483.8, 3430.0, 3453.7, 3445.4, 3416.6, 3372.7, 3394.2, 3410.0, 3425.8, 3447.3, 3468.8, 3484.7],
            [3534.1, 3445.0, 3491.3, 3481.7, 3427.9, 3376.9, 3399.2, 3418.3, 3437.3, 3459.7, 3482.0, 3501.1],
            [3466.3, 3393.0, 3404.7, 3535.3, 3446.2, 3373.0, 3402.2, 3430.2, 3458.2, 3487.4, 3516.7, 3544.7],
        ]

        # All indicator classes to test
        self.hlc_indicators = {
            'PL': PL,
            'L11': L11,
            'H11': H11,
            'Z1B': Z1B,
            'Z2B': Z2B,
            'EBot': EBot,
            'ETop': ETop,
            'Z5T': Z5T,
            'Z6T': Z6T,
        }

        self.five_nine_indicators = {
            'FiveNineSell': FiveNineSell,
            'FiveNineBuy': FiveNineBuy,
        }

    def create_test_intervals(self, data_rows: List[List[float]], count: int = None) -> List[TestInstrumentInterval]:
        """Create test intervals from data rows."""
        if count is None:
            count = len(data_rows)

        intervals = []
        for i in range(min(count, len(data_rows))):
            row = data_rows[i]
            high, low, close = row[0], row[1], row[2]
            intervals.append(TestInstrumentInterval(high=high, low=low, close=close))

        return intervals

    def test_hlc_indicator_instantiation(self) -> bool:
        """Test that all HLC indicators can be instantiated."""
        print("🔍 Testing HLC indicator instantiation...")

        success = True

        for name, indicator_class in self.hlc_indicators.items():
            try:
                indicator = indicator_class()

                # Check basic properties
                if not hasattr(indicator, 'update'):
                    self.errors.append(f"{name}: Missing update method")
                    success = False

                if not hasattr(indicator, 'get_value'):
                    self.errors.append(f"{name}: Missing get_value method")
                    success = False

                if not hasattr(indicator, 'status'):
                    self.errors.append(f"{name}: Missing status attribute")
                    success = False

                # Initial state should be appropriate
                initial_value = indicator.get_value()
                if initial_value is not None:
                    self.errors.append(f"{name}: Initial value should be None, got {initial_value}")
                    success = False

                print(f"  ✅ {name}: Instantiated successfully")

            except Exception as e:
                self.errors.append(f"{name}: Instantiation failed - {e}")
                success = False

        if success:
            print("✅ All HLC indicators instantiate correctly")
        else:
            print("❌ HLC indicator instantiation issues found")

        return success

    def test_five_nine_indicator_instantiation(self) -> bool:
        """Test that Five Nine indicators can be instantiated."""
        print("🔍 Testing Five Nine indicator instantiation...")

        success = True

        for name, indicator_class in self.five_nine_indicators.items():
            try:
                indicator = indicator_class()

                # Check basic properties
                if not hasattr(indicator, 'update'):
                    self.errors.append(f"{name}: Missing update method")
                    success = False

                if not hasattr(indicator, 'get_value'):
                    self.errors.append(f"{name}: Missing get_value method")
                    success = False

                if not hasattr(indicator, 'status'):
                    self.errors.append(f"{name}: Missing status attribute")
                    success = False

                # Initial state
                initial_value = indicator.get_value()
                if initial_value is not None:
                    self.errors.append(f"{name}: Initial value should be None, got {initial_value}")
                    success = False

                print(f"  ✅ {name}: Instantiated successfully")

            except Exception as e:
                self.errors.append(f"{name}: Instantiation failed - {e}")
                success = False

        if success:
            print("✅ All Five Nine indicators instantiate correctly")
        else:
            print("❌ Five Nine indicator instantiation issues found")

        return success

    def test_hlc_indicator_calculations(self) -> bool:
        """Test HLC indicator calculations with real data."""
        print("🔍 Testing HLC indicator calculations...")

        success = True

        # Create test intervals (need 4 for HLC indicators - they use 3 previous)
        test_intervals = self.create_test_intervals(self.test_data, 4)

        # Expected values for validation (from original dataset)
        expected_values = {
            # Row 3 (index 3) expected values: h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t
            # These are from columns 3-11 of test_data row 3
            'H11': 3481.7,   # h11
            'L11': 3427.9,   # l11
            'Z1B': 3376.9,   # z1b
            'Z2B': 3399.2,   # z2b
            'EBot': 3418.3,  # ebot
            'PL': 3437.3,    # pldot
            'ETop': 3459.7,  # etop
            'Z5T': 3482.0,   # z5t
            'Z6T': 3501.1,   # z6t
        }

        for name, indicator_class in self.hlc_indicators.items():
            try:
                indicator = indicator_class()

                # Update with test intervals
                indicator.update(test_intervals)

                # Check status
                if indicator.status != 'ok':
                    self.errors.append(f"{name}: Expected status 'ok', got '{indicator.status}'")
                    success = False
                    continue

                # Get calculated value
                calculated_value = indicator.get_value()

                if calculated_value is None:
                    self.errors.append(f"{name}: Calculated value is None")
                    success = False
                    continue

                # Validate against expected value
                if name in expected_values:
                    expected = expected_values[name]
                    error = abs(calculated_value - expected)
                    relative_error = (error / abs(expected)) * 100 if expected != 0 else 0

                    # Tolerance: 0.5% relative error or 1.0 absolute error
                    if relative_error > 0.5 and error > 1.0:
                        self.errors.append(
                            f"{name}: Expected {expected:.4f}, got {calculated_value:.4f}, "
                            f"error {error:.4f} ({relative_error:.3f}%)"
                        )
                        success = False
                    else:
                        print(f"  ✅ {name}: {calculated_value:.2f} (expected {expected:.2f}, error {error:.4f})")
                else:
                    print(f"  ✅ {name}: {calculated_value:.2f} (no validation reference)")

            except Exception as e:
                self.errors.append(f"{name}: Calculation failed - {e}")
                success = False

        if success:
            print("✅ All HLC indicator calculations validated")
        else:
            print("❌ HLC indicator calculation issues found")

        return success

    def test_five_nine_indicator_calculations(self) -> bool:
        """Test Five Nine indicator calculations with real data."""
        print("🔍 Testing Five Nine indicator calculations...")

        success = True

        # Create test intervals (need 2 for Five Nine indicators)
        test_intervals = self.create_test_intervals(self.test_data, 2)

        # Manual calculation for validation
        # FiveNineSell = 2 * high(t-1) - low(t-2)
        # FiveNineBuy = 2 * low(t-1) - high(t-2)
        # Using test_data[0] as t-2, test_data[1] as t-1
        prior_prior = test_intervals[0]  # test_data[0]: 3444.9, 3403.3, 3434.7
        prior = test_intervals[1]        # test_data[1]: 3440.5, 3411.7, 3433.4

        expected_sell = 2 * prior.high - prior_prior.low    # 2 * 3440.5 - 3403.3 = 3477.7
        expected_buy = 2 * prior.low - prior_prior.high     # 2 * 3411.7 - 3444.9 = 3378.5

        test_cases = [
            ('FiveNineSell', FiveNineSell, expected_sell),
            ('FiveNineBuy', FiveNineBuy, expected_buy),
        ]

        for name, indicator_class, expected in test_cases:
            try:
                indicator = indicator_class()

                # Update with test intervals
                indicator.update(test_intervals)

                # Check status
                if indicator.status != 'ok':
                    self.errors.append(f"{name}: Expected status 'ok', got '{indicator.status}'")
                    success = False
                    continue

                # Get calculated value
                calculated_value = indicator.get_value()

                if calculated_value is None:
                    self.errors.append(f"{name}: Calculated value is None")
                    success = False
                    continue

                # Validate calculation
                error = abs(calculated_value - expected)
                if error > 0.001:  # Very tight tolerance for simple arithmetic
                    self.errors.append(
                        f"{name}: Expected {expected:.4f}, got {calculated_value:.4f}, error {error:.6f}"
                    )
                    success = False
                else:
                    print(f"  ✅ {name}: {calculated_value:.2f} (expected {expected:.2f})")

            except Exception as e:
                self.errors.append(f"{name}: Calculation failed - {e}")
                success = False

        if success:
            print("✅ All Five Nine indicator calculations validated")
        else:
            print("❌ Five Nine indicator calculation issues found")

        return success

    def test_insufficient_data_handling(self) -> bool:
        """Test handling of insufficient data."""
        print("🔍 Testing insufficient data handling...")

        success = True

        # Test HLC indicators with insufficient data
        insufficient_test_cases = [
            ("No data", []),
            ("One interval", self.create_test_intervals(self.test_data, 1)),
            ("Two intervals", self.create_test_intervals(self.test_data, 2)),
        ]

        for case_name, intervals in insufficient_test_cases:
            # Test HLC indicators (need 3+ intervals)
            for name, indicator_class in self.hlc_indicators.items():
                try:
                    indicator = indicator_class()
                    indicator.update(intervals)

                    # Should have invalid status
                    if indicator.status == 'ok':
                        self.errors.append(f"{name} with {case_name}: Should have invalid status, got 'ok'")
                        success = False

                    # Value should be None
                    if indicator.get_value() is not None:
                        self.errors.append(f"{name} with {case_name}: Value should be None")
                        success = False

                except Exception as e:
                    # Exception is acceptable for invalid data
                    pass

            # Test Five Nine indicators (need 2+ intervals)
            if len(intervals) < 2:
                for name, indicator_class in self.five_nine_indicators.items():
                    try:
                        indicator = indicator_class()
                        indicator.update(intervals)

                        # Should have invalid status
                        if indicator.status == 'ok':
                            self.errors.append(f"{name} with {case_name}: Should have invalid status, got 'ok'")
                            success = False

                        # Value should be None
                        if indicator.get_value() is not None:
                            self.errors.append(f"{name} with {case_name}: Value should be None")
                            success = False

                    except Exception as e:
                        # Exception is acceptable for invalid data
                        pass

        if success:
            print("✅ Insufficient data handling validated")
        else:
            print("❌ Insufficient data handling issues found")

        return success

    def test_invalid_data_handling(self) -> bool:
        """Test handling of invalid data."""
        print("🔍 Testing invalid data handling...")

        success = True

        # Create intervals with invalid status
        invalid_intervals = [
            TestInstrumentInterval(100, 90, 95, status='invalid'),
            TestInstrumentInterval(110, 100, 105, status='error'),
            TestInstrumentInterval(120, 110, 115, status='pending'),
        ]

        # Test all indicators with invalid status intervals
        all_indicators = {**self.hlc_indicators, **self.five_nine_indicators}

        for name, indicator_class in all_indicators.items():
            try:
                indicator = indicator_class()
                indicator.update(invalid_intervals)

                # Should handle invalid status gracefully
                if indicator.status == 'ok':
                    self.errors.append(f"{name}: Should reject invalid status intervals")
                    success = False

                if indicator.get_value() is not None:
                    self.errors.append(f"{name}: Should return None for invalid data")
                    success = False

            except Exception as e:
                # Exception is acceptable for invalid data
                pass

        # Test with NaN values (if the implementation handles them)
        try:
            nan_intervals = [
                TestInstrumentInterval(float('nan'), 90, 95),
                TestInstrumentInterval(110, float('nan'), 105),
                TestInstrumentInterval(120, 110, float('nan')),
            ]

            for name, indicator_class in all_indicators.items():
                try:
                    indicator = indicator_class()
                    indicator.update(nan_intervals)

                    # Should handle NaN gracefully
                    if indicator.status == 'ok':
                        self.errors.append(f"{name}: Should reject NaN values")
                        success = False

                except Exception as e:
                    # Exception is expected and acceptable for NaN data
                    pass

        except ValueError:
            # NaN intervals may fail validation in __post_init__, which is acceptable
            pass

        if success:
            print("✅ Invalid data handling validated")
        else:
            print("❌ Invalid data handling issues found")

        return success

    def test_performance_with_real_indicators(self) -> bool:
        """Test performance with real indicator implementations."""
        print("🔍 Testing performance with real indicators...")

        success = True

        # Generate larger test dataset
        large_intervals = []
        base_price = 3400

        for i in range(1000):
            high = base_price + (i % 50) + 10
            low = base_price + (i % 50) - 10
            close = base_price + (i % 50) + (i % 20) - 10
            large_intervals.append(TestInstrumentInterval(high, low, close))

        # Benchmark HLC indicators
        hlc_times = {}

        for name, indicator_class in self.hlc_indicators.items():
            start_time = time.time()

            indicator = indicator_class()

            # Test with sliding window
            for i in range(3, min(100, len(large_intervals))):  # Test first 100 calculations
                test_intervals = large_intervals[i-3:i+1]  # 4 intervals
                indicator.update(test_intervals)
                result = indicator.get_value()

            end_time = time.time()
            calc_time = end_time - start_time
            hlc_times[name] = calc_time

            # Performance threshold: should be very fast
            if calc_time > 1.0:  # 1 second for 97 calculations
                self.errors.append(f"{name}: Performance too slow - {calc_time:.3f}s")
                success = False

            print(f"  ⚡ {name}: {calc_time:.4f}s for 97 calculations")

        # Benchmark Five Nine indicators
        five_nine_times = {}

        for name, indicator_class in self.five_nine_indicators.items():
            start_time = time.time()

            indicator = indicator_class()

            # Test with sliding window
            for i in range(2, min(100, len(large_intervals))):  # Test first 98 calculations
                test_intervals = large_intervals[i-2:i]  # 2 intervals
                indicator.update(test_intervals)
                result = indicator.get_value()

            end_time = time.time()
            calc_time = end_time - start_time
            five_nine_times[name] = calc_time

            if calc_time > 1.0:
                self.errors.append(f"{name}: Performance too slow - {calc_time:.3f}s")
                success = False

            print(f"  ⚡ {name}: {calc_time:.4f}s for 98 calculations")

        self.performance_metrics = {
            'hlc_times': hlc_times,
            'five_nine_times': five_nine_times,
            'test_size': len(large_intervals)
        }

        if success:
            print("✅ Performance benchmarks passed")
        else:
            print("❌ Performance issues detected")

        return success

    def test_state_management(self) -> bool:
        """Test indicator state management."""
        print("🔍 Testing indicator state management...")

        success = True

        # Test state persistence across updates
        test_intervals_1 = self.create_test_intervals(self.test_data[:3])
        test_intervals_2 = self.create_test_intervals(self.test_data[1:4])

        all_indicators = {**self.hlc_indicators, **self.five_nine_indicators}

        for name, indicator_class in all_indicators.items():
            try:
                indicator = indicator_class()

                # First update
                indicator.update(test_intervals_1)
                first_value = indicator.get_value()
                first_status = indicator.status
                first_update_time = indicator.update_at

                # Second update with different data
                time.sleep(0.001)  # Ensure different timestamp
                indicator.update(test_intervals_2)
                second_value = indicator.get_value()
                second_status = indicator.status
                second_update_time = indicator.update_at

                # State should be updated
                if second_update_time <= first_update_time:
                    self.errors.append(f"{name}: Update time not advancing")
                    success = False

                # Values should potentially be different (unless data is identical)
                # We don't enforce this as identical inputs should give identical outputs

                # Status should be managed properly
                if second_status != 'ok' and len(test_intervals_2) >= (3 if name in self.hlc_indicators else 2):
                    self.errors.append(f"{name}: Unexpected status '{second_status}' with sufficient data")
                    success = False

                print(f"  ✅ {name}: State management validated")

            except Exception as e:
                self.errors.append(f"{name}: State management failed - {e}")
                success = False

        if success:
            print("✅ State management validated")
        else:
            print("❌ State management issues found")

        return success

    def run_all_tests(self) -> bool:
        """Run all integration tests."""
        print("🚀 Running Indicator Integration Test Suite")
        print("=" * 70)
        print("Testing real indicator implementations from src/signals/indicator.py")
        print("=" * 70)

        tests = [
            ("HLC Indicator Instantiation", self.test_hlc_indicator_instantiation),
            ("Five Nine Indicator Instantiation", self.test_five_nine_indicator_instantiation),
            ("HLC Indicator Calculations", self.test_hlc_indicator_calculations),
            ("Five Nine Indicator Calculations", self.test_five_nine_indicator_calculations),
            ("Insufficient Data Handling", self.test_insufficient_data_handling),
            ("Invalid Data Handling", self.test_invalid_data_handling),
            ("Performance Testing", self.test_performance_with_real_indicators),
            ("State Management", self.test_state_management),
        ]

        passed_tests = 0
        total_tests = len(tests)
        start_time = time.time()

        for test_name, test_func in tests:
            print(f"\n📋 {test_name}")
            print("-" * 50)

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

        total_time = time.time() - start_time

        # Summary
        print("\n" + "=" * 70)
        print("🎯 INTEGRATION TEST SUMMARY")
        print("=" * 70)

        for test_name, result in self.test_results.items():
            status_icon = "✅" if result == "PASS" else "❌" if result == "FAIL" else "⚠️"
            print(f"{status_icon} {test_name}: {result}")

        print(f"\n📊 Results: {passed_tests}/{total_tests} tests passed")
        print(f"⏱️ Total execution time: {total_time:.2f}s")

        # Performance summary
        if self.performance_metrics:
            print(f"\n⚡ Performance Summary:")
            if 'hlc_times' in self.performance_metrics:
                avg_hlc_time = sum(self.performance_metrics['hlc_times'].values()) / len(self.performance_metrics['hlc_times'])
                print(f"   • HLC Indicators: {avg_hlc_time:.4f}s average")
            if 'five_nine_times' in self.performance_metrics:
                avg_five_nine_time = sum(self.performance_metrics['five_nine_times'].values()) / len(self.performance_metrics['five_nine_times'])
                print(f"   • Five Nine Indicators: {avg_five_nine_time:.4f}s average")

        if self.errors:
            print(f"\n⚠️  {len(self.errors)} errors found:")
            for error in self.errors[:10]:  # Show first 10 errors
                print(f"   • {error}")
            if len(self.errors) > 10:
                print(f"   ... and {len(self.errors) - 10} more errors")

        success = passed_tests == total_tests

        if success:
            print("\n🎉 ALL INTEGRATION TESTS PASSED!")
            print("✅ All 11 real indicator implementations work correctly")
            print("✅ Ready for production deployment")
        else:
            print(f"\n❌ {total_tests - passed_tests} tests failed")
            print("🔧 Fix implementation issues in src/signals/indicator.py")

        return success

def main():
    """Run the integration test suite."""
    tester = IndicatorIntegrationTests()
    success = tester.run_all_tests()

    # Return appropriate exit code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()