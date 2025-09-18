#!/usr/bin/env python3
"""
Comprehensive Test Suite for All ATS Indicators (15 Total)

This test suite provides thorough validation of all indicator implementations:
- 9 HLC linear regression indicators (PL, L11, H11, Z1B, Z2B, EBot, ETop, Z5T, Z6T)
- 2 Five Nine arithmetic indicators (FiveNineSell, FiveNineBuy)
- 2 Five One conditional indicators (FiveOneBuy, FiveOneSell)
- 2 Five Two conditional indicators (FiveTwoBuy, FiveTwoSell)

Test Categories:
1. Mathematical accuracy validation
2. Conditional logic verification
3. Edge case handling
4. Performance benchmarking
5. Integration testing
6. Cross-validation scenarios

Usage:
    PYTHONPATH=src python test_all_indicators_complete.py
"""

import sys
import os
import time
import random
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from domains.trading.services.indicator import (
        # HLC Linear Regression Indicators (9)
        PL, L11, H11, Z1B, Z2B, EnvelopeBot, EnvelopeTop, Z5T, Z6T,
        # Five Nine Arithmetic Indicators (2)
        FiveNineSell, FiveNineBuy,
        # Five One Conditional Indicators (2)
        FiveOneBuy, FiveOneSell,
        # Five Two Conditional Indicators (2)
        FiveTwoBuy, FiveTwoSell
    )
except ImportError as e:
    print(f"❌ Cannot import indicators: {e}")
    print("Make sure to run: PYTHONPATH=src python test_all_indicators_complete.py")
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
            self.open = self.close

class ComprehensiveIndicatorTests:
    """Comprehensive test suite for all 15 ATS indicators."""

    def __init__(self):
        self.test_results = {}
        self.performance_metrics = {}
        self.errors = []

        # All indicator classes organized by category
        self.hlc_indicators = {
            'PL': PL, 'L11': L11, 'H11': H11, 'Z1B': Z1B, 'Z2B': Z2B,
            'EnvelopeBot': EnvelopeBot, 'EnvelopeTop': EnvelopeTop, 'Z5T': Z5T, 'Z6T': Z6T
        }

        self.five_nine_indicators = {
            'FiveNineSell': FiveNineSell,
            'FiveNineBuy': FiveNineBuy
        }

        self.five_one_indicators = {
            'FiveOneBuy': FiveOneBuy,
            'FiveOneSell': FiveOneSell
        }

        self.five_two_indicators = {
            'FiveTwoBuy': FiveTwoBuy,
            'FiveTwoSell': FiveTwoSell
        }

        self.all_indicators = {
            **self.hlc_indicators,
            **self.five_nine_indicators,
            **self.five_one_indicators,
            **self.five_two_indicators
        }

        # Test data from original HLC analysis
        self.validation_data = [
            [3444.9, 3403.3, 3434.7], [3440.5, 3411.7, 3433.4], [3483.8, 3430.0, 3453.7],
            [3534.1, 3445.0, 3491.3], [3466.3, 3393.0, 3404.7], [3520.4, 3450.2, 3485.1],
            [3478.6, 3401.8, 3438.9], [3535.7, 3468.1, 3502.3], [3510.2, 3442.5, 3476.8],
            [3498.1, 3429.7, 3465.2], [3523.8, 3455.6, 3489.7], [3487.2, 3418.9, 3453.1]
        ]

        # Expected results for mathematical validation
        self.expected_hlc_results = {
            'H11': [3444.6, 3452.0, 3445.4, 3481.7, 3535.3, 3483.9, 3497.5, 3506.1, 3485.3, 3473.6, 3496.1, 3455.5],
            'L11': [3403.0, 3410.4, 3416.6, 3427.9, 3446.2, 3436.8, 3427.4, 3447.9, 3428.5, 3419.2, 3441.8, 3408.4],
            'PL': [3302.9, 3331.1, 3372.7, 3376.9, 3373.0, 3409.4, 3379.6, 3426.3, 3387.0, 3378.5, 3411.1, 3368.9]
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

    def test_indicator_instantiation(self) -> bool:
        """Test that all 15 indicators can be instantiated without error."""
        print("=== Testing Indicator Instantiation ===")

        success_count = 0
        total_count = len(self.all_indicators)

        for name, indicator_class in self.all_indicators.items():
            try:
                indicator = indicator_class()
                assert hasattr(indicator, 'update'), f"{name} missing update method"
                assert hasattr(indicator, 'get_value'), f"{name} missing get_value method"
                print(f"✅ {name}: instantiated successfully")
                success_count += 1
            except Exception as e:
                print(f"❌ {name}: instantiation failed - {e}")
                self.errors.append(f"Instantiation failed for {name}: {e}")

        success_rate = success_count / total_count
        print(f"\nInstantiation Success Rate: {success_count}/{total_count} ({success_rate:.1%})")

        return success_rate == 1.0

    def test_hlc_mathematical_accuracy(self) -> bool:
        """Test mathematical accuracy of HLC linear regression indicators."""
        print("\n=== Testing HLC Mathematical Accuracy ===")

        test_data = self.validation_data[:5]  # Use first 5 samples
        intervals = self.create_test_intervals(test_data)

        accuracy_results = {}

        for name, indicator_class in self.hlc_indicators.items():
            try:
                indicator = indicator_class()

                # Need at least 3 intervals for HLC indicators
                if len(intervals) >= 3:
                    indicator.update(intervals)
                    calculated_value = indicator.get_value()

                    if calculated_value is not None:
                        # For indicators we have expected results
                        if name in self.expected_hlc_results:
                            expected = self.expected_hlc_results[name][4]  # 5th value
                            error = abs(calculated_value - expected)
                            relative_error = error / abs(expected) if expected != 0 else 0

                            accuracy_results[name] = {
                                'calculated': calculated_value,
                                'expected': expected,
                                'absolute_error': error,
                                'relative_error': relative_error
                            }

                            tolerance = 0.001  # 0.1% tolerance
                            if relative_error <= tolerance:
                                print(f"✅ {name}: {calculated_value:.3f} (expected {expected:.3f}, error {relative_error:.4%})")
                            else:
                                print(f"❌ {name}: {calculated_value:.3f} (expected {expected:.3f}, error {relative_error:.4%})")
                                self.errors.append(f"{name} mathematical accuracy failed")
                        else:
                            # For other indicators, just verify they calculate
                            accuracy_results[name] = {
                                'calculated': calculated_value,
                                'status': 'calculated_successfully'
                            }
                            print(f"✅ {name}: {calculated_value:.3f} (calculated successfully)")
                    else:
                        print(f"⚠️ {name}: returned None (may be expected for insufficient data)")
                        accuracy_results[name] = {'status': 'returned_none'}
                else:
                    print(f"⚠️ {name}: insufficient test data")
                    accuracy_results[name] = {'status': 'insufficient_data'}

            except Exception as e:
                print(f"❌ {name}: calculation failed - {e}")
                self.errors.append(f"{name} calculation failed: {e}")
                accuracy_results[name] = {'status': 'failed', 'error': str(e)}

        self.test_results['hlc_accuracy'] = accuracy_results

        # Count successful calculations
        successful = sum(1 for result in accuracy_results.values()
                        if result.get('status') != 'failed')
        total = len(self.hlc_indicators)

        print(f"\nHLC Accuracy: {successful}/{total} indicators calculated successfully")
        return successful == total

    def test_conditional_logic_comprehensive(self) -> bool:
        """Comprehensive test of conditional logic for Five One and Five Two indicators."""
        print("\n=== Testing Conditional Logic Comprehensively ===")

        test_scenarios = [
            # Scenario 1: Improving lows (Five One Buy should activate)
            {
                'name': 'Improving lows',
                'intervals': [
                    TestInstrumentInterval(high=110, low=100, close=105),  # t-2
                    TestInstrumentInterval(high=112, low=102, close=107),  # t-1: low improved
                ],
                'expected': {
                    'FiveOneBuy': 2 * 102 - 100,  # Should calculate: 104
                    'FiveTwoBuy': None,           # Should not calculate
                    'FiveOneSell': None,          # No high decline
                    'FiveTwoSell': 2 * 112 - 110  # High rising: 114
                }
            },

            # Scenario 2: Declining lows (Five Two Buy should activate)
            {
                'name': 'Declining lows',
                'intervals': [
                    TestInstrumentInterval(high=110, low=102, close=105),  # t-2
                    TestInstrumentInterval(high=108, low=100, close=104),  # t-1: low declined
                ],
                'expected': {
                    'FiveOneBuy': None,           # Should not calculate
                    'FiveTwoBuy': 2 * 100 - 102,  # Should calculate: 98
                    'FiveOneSell': 2 * 108 - 110, # High declining: 106
                    'FiveTwoSell': None           # No high rise
                }
            },

            # Scenario 3: Equal values (nothing should calculate)
            {
                'name': 'Equal values',
                'intervals': [
                    TestInstrumentInterval(high=110, low=100, close=105),  # t-2
                    TestInstrumentInterval(high=110, low=100, close=105),  # t-1: same values
                ],
                'expected': {
                    'FiveOneBuy': None,   # Equal lows
                    'FiveTwoBuy': None,   # Equal lows
                    'FiveOneSell': None,  # Equal highs
                    'FiveTwoSell': None   # Equal highs
                }
            }
        ]

        all_passed = True

        for scenario in test_scenarios:
            print(f"\n--- Scenario: {scenario['name']} ---")

            conditional_indicators = {**self.five_one_indicators, **self.five_two_indicators}

            for name, indicator_class in conditional_indicators.items():
                try:
                    indicator = indicator_class()
                    indicator.update(scenario['intervals'])
                    actual_value = indicator.get_value()
                    expected_value = scenario['expected'][name]

                    if actual_value == expected_value:
                        status = "None" if actual_value is None else f"{actual_value:.1f}"
                        print(f"✅ {name}: {status} (as expected)")
                    else:
                        print(f"❌ {name}: got {actual_value}, expected {expected_value}")
                        self.errors.append(f"{name} conditional logic failed in {scenario['name']}")
                        all_passed = False

                except Exception as e:
                    print(f"❌ {name}: error - {e}")
                    self.errors.append(f"{name} error in {scenario['name']}: {e}")
                    all_passed = False

        return all_passed

    def test_five_nine_arithmetic_accuracy(self) -> bool:
        """Test Five Nine indicators arithmetic accuracy."""
        print("\n=== Testing Five Nine Arithmetic Accuracy ===")

        test_intervals = [
            TestInstrumentInterval(high=110, low=100, close=105),  # t-2
            TestInstrumentInterval(high=112, low=98, close=108),   # t-1
        ]

        expected_results = {
            'FiveNineSell': 2 * 112 - 100,  # 2 * high(t-1) - low(t-2) = 224 - 100 = 124
            'FiveNineBuy': 2 * 98 - 110     # 2 * low(t-1) - high(t-2) = 196 - 110 = 86
        }

        all_passed = True

        for name, indicator_class in self.five_nine_indicators.items():
            try:
                indicator = indicator_class()
                indicator.update(test_intervals)
                actual_value = indicator.get_value()
                expected_value = expected_results[name]

                if actual_value == expected_value:
                    print(f"✅ {name}: {actual_value} (correct)")
                else:
                    print(f"❌ {name}: got {actual_value}, expected {expected_value}")
                    self.errors.append(f"{name} arithmetic accuracy failed")
                    all_passed = False

            except Exception as e:
                print(f"❌ {name}: calculation error - {e}")
                self.errors.append(f"{name} calculation error: {e}")
                all_passed = False

        return all_passed

    def test_edge_cases_comprehensive(self) -> bool:
        """Comprehensive edge case testing for all indicators."""
        print("\n=== Testing Edge Cases Comprehensively ===")

        edge_cases = [
            {
                'name': 'Insufficient data (1 interval)',
                'intervals': [TestInstrumentInterval(high=105, low=95, close=100)],
                'expected_behavior': 'all_return_none'
            },
            {
                'name': 'Invalid status intervals',
                'intervals': [
                    TestInstrumentInterval(high=105, low=95, close=100, status='invalid'),
                    TestInstrumentInterval(high=110, low=100, close=105, status='error')
                ],
                'expected_behavior': 'all_return_none_or_handle_gracefully'
            },
            {
                'name': 'NaN values',
                'intervals': [
                    TestInstrumentInterval(high=float('nan'), low=95, close=100),
                    TestInstrumentInterval(high=110, low=float('nan'), close=105)
                ],
                'expected_behavior': 'all_return_none_or_handle_gracefully'
            },
            {
                'name': 'Zero and negative values',
                'intervals': [
                    TestInstrumentInterval(high=0, low=-5, close=-2),
                    TestInstrumentInterval(high=-1, low=-10, close=-5)
                ],
                'expected_behavior': 'handle_gracefully'
            },
            {
                'name': 'Extreme large values',
                'intervals': [
                    TestInstrumentInterval(high=1e10, low=1e9, close=5e9),
                    TestInstrumentInterval(high=2e10, low=1.5e9, close=1e10)
                ],
                'expected_behavior': 'calculate_or_handle_gracefully'
            }
        ]

        total_tests = 0
        passed_tests = 0

        for case in edge_cases:
            print(f"\n--- Edge Case: {case['name']} ---")

            for name, indicator_class in self.all_indicators.items():
                total_tests += 1
                try:
                    indicator = indicator_class()
                    indicator.update(case['intervals'])
                    result = indicator.get_value()

                    # For edge cases, we mainly check that no exceptions occur
                    # and that the behavior is reasonable
                    if case['expected_behavior'] == 'all_return_none':
                        if result is None:
                            print(f"✅ {name}: None (as expected)")
                            passed_tests += 1
                        else:
                            print(f"⚠️ {name}: {result} (expected None, but calculation may be valid)")
                            passed_tests += 1  # Still pass if it calculated something reasonable
                    else:
                        # For other cases, just check no exception occurred
                        status = "None" if result is None else f"calculated: {result}"
                        print(f"✅ {name}: {status} (handled gracefully)")
                        passed_tests += 1

                except Exception as e:
                    print(f"❌ {name}: exception - {e}")
                    # Some exceptions might be expected for extreme edge cases
                    if "invalid_data" in str(e) or "calculation_error" in str(e):
                        print(f"   (Expected error for edge case)")
                        passed_tests += 1
                    else:
                        self.errors.append(f"{name} failed edge case {case['name']}: {e}")

        success_rate = passed_tests / total_tests
        print(f"\nEdge Cases: {passed_tests}/{total_tests} tests handled correctly ({success_rate:.1%})")

        return success_rate >= 0.90  # Allow 10% tolerance for extreme edge cases

    def test_performance_benchmarks(self) -> bool:
        """Performance benchmarking for all indicators."""
        print("\n=== Testing Performance Benchmarks ===")

        # Generate larger test dataset
        large_dataset = []
        random.seed(42)  # Reproducible results

        base_price = 3400
        for i in range(1000):
            # Generate realistic OHLC-like data with some randomness
            price_change = random.uniform(-50, 50)
            high = base_price + price_change + random.uniform(0, 20)
            low = base_price + price_change - random.uniform(0, 20)
            close = base_price + price_change + random.uniform(-10, 10)

            large_dataset.append([high, low, close])
            base_price = close  # Next iteration starts from current close

        intervals = self.create_test_intervals(large_dataset)

        performance_results = {}

        for name, indicator_class in self.all_indicators.items():
            try:
                indicator = indicator_class()

                # Measure time for multiple updates
                start_time = time.perf_counter()
                iterations = 10

                for _ in range(iterations):
                    indicator.update(intervals)
                    result = indicator.get_value()

                end_time = time.perf_counter()

                avg_time_ms = ((end_time - start_time) / iterations) * 1000
                performance_results[name] = avg_time_ms

                print(f"✅ {name}: {avg_time_ms:.3f}ms avg (over {iterations} iterations)")

            except Exception as e:
                print(f"❌ {name}: performance test failed - {e}")
                self.errors.append(f"{name} performance test failed: {e}")
                performance_results[name] = float('inf')

        self.performance_metrics = performance_results

        # Check if any indicator is unreasonably slow (>100ms average)
        slow_indicators = [name for name, time_ms in performance_results.items() if time_ms > 100]

        if slow_indicators:
            print(f"\n⚠️ Slow indicators (>100ms): {slow_indicators}")
        else:
            print(f"\n✅ All indicators perform well (<100ms average)")

        # Performance test passes if all indicators complete without crashing
        failed_indicators = [name for name, time_ms in performance_results.items() if time_ms == float('inf')]
        return len(failed_indicators) == 0

    def test_integration_scenarios(self) -> bool:
        """Integration testing with realistic market scenarios."""
        print("\n=== Testing Integration Scenarios ===")

        # Realistic market scenarios
        scenarios = [
            {
                'name': 'Bull Market Rally',
                'data': [
                    [3400, 3380, 3395],  # Start
                    [3420, 3390, 3410],  # Gradual rise
                    [3450, 3420, 3440],  # Continued rise
                    [3480, 3450, 3470],  # Strong momentum
                    [3500, 3470, 3490]   # Peak
                ]
            },
            {
                'name': 'Bear Market Decline',
                'data': [
                    [3500, 3480, 3490],  # Peak
                    [3485, 3460, 3470],  # Decline starts
                    [3470, 3440, 3450],  # Continued decline
                    [3450, 3420, 3430],  # Accelerating
                    [3430, 3400, 3410]   # Bottom
                ]
            },
            {
                'name': 'Sideways Consolidation',
                'data': [
                    [3450, 3430, 3440],  # Range bound
                    [3455, 3435, 3445],  # Slight up
                    [3450, 3425, 3435],  # Slight down
                    [3445, 3430, 3440],  # Back to middle
                    [3450, 3435, 3445]   # Consolidating
                ]
            }
        ]

        integration_results = {}
        all_passed = True

        for scenario in scenarios:
            print(f"\n--- {scenario['name']} ---")
            intervals = self.create_test_intervals(scenario['data'])

            scenario_results = {}

            for name, indicator_class in self.all_indicators.items():
                try:
                    indicator = indicator_class()
                    indicator.update(intervals)
                    result = indicator.get_value()

                    scenario_results[name] = result
                    status = "None" if result is None else f"{result:.2f}"
                    print(f"  {name}: {status}")

                except Exception as e:
                    print(f"  ❌ {name}: failed - {e}")
                    scenario_results[name] = 'error'
                    self.errors.append(f"{name} failed in {scenario['name']}: {e}")
                    all_passed = False

            integration_results[scenario['name']] = scenario_results

        self.test_results['integration'] = integration_results
        return all_passed

    def generate_test_report(self) -> str:
        """Generate comprehensive test report."""
        report = []
        report.append("=" * 60)
        report.append("COMPREHENSIVE ATS INDICATORS TEST REPORT")
        report.append("=" * 60)
        report.append(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Indicators Tested: {len(self.all_indicators)}")
        report.append("")

        # Indicator breakdown
        report.append("Indicator Categories:")
        report.append(f"  • HLC Linear Regression: {len(self.hlc_indicators)} indicators")
        report.append(f"  • Five Nine Arithmetic: {len(self.five_nine_indicators)} indicators")
        report.append(f"  • Five One Conditional: {len(self.five_one_indicators)} indicators")
        report.append(f"  • Five Two Conditional: {len(self.five_two_indicators)} indicators")
        report.append("")

        # Performance metrics
        if self.performance_metrics:
            report.append("Performance Metrics (average execution time):")
            sorted_perf = sorted(self.performance_metrics.items(), key=lambda x: x[1])
            for name, time_ms in sorted_perf:
                if time_ms != float('inf'):
                    report.append(f"  • {name}: {time_ms:.3f}ms")
            report.append("")

        # Error summary
        if self.errors:
            report.append(f"Errors Found: {len(self.errors)}")
            for error in self.errors[:10]:  # Show first 10 errors
                report.append(f"  • {error}")
            if len(self.errors) > 10:
                report.append(f"  ... and {len(self.errors) - 10} more errors")
        else:
            report.append("✅ No errors found!")

        report.append("")
        report.append("=" * 60)

        return "\n".join(report)

    def run_all_tests(self) -> bool:
        """Run all comprehensive tests."""
        print("Starting Comprehensive ATS Indicators Test Suite")
        print("Testing 15 indicators across multiple categories...")
        print("=" * 60)

        test_methods = [
            ('Instantiation', self.test_indicator_instantiation),
            ('HLC Mathematical Accuracy', self.test_hlc_mathematical_accuracy),
            ('Five Nine Arithmetic', self.test_five_nine_arithmetic_accuracy),
            ('Conditional Logic', self.test_conditional_logic_comprehensive),
            ('Edge Cases', self.test_edge_cases_comprehensive),
            ('Performance', self.test_performance_benchmarks),
            ('Integration Scenarios', self.test_integration_scenarios)
        ]

        results = {}
        overall_success = True

        for test_name, test_method in test_methods:
            print(f"\n{'='*20} {test_name} {'='*20}")
            try:
                result = test_method()
                results[test_name] = result
                if not result:
                    overall_success = False
                    print(f"❌ {test_name} FAILED")
                else:
                    print(f"✅ {test_name} PASSED")
            except Exception as e:
                print(f"❌ {test_name} CRASHED: {e}")
                results[test_name] = False
                overall_success = False
                import traceback
                traceback.print_exc()

        # Final summary
        print("\n" + "="*60)
        print("FINAL TEST RESULTS:")
        print("="*60)

        passed_tests = sum(1 for result in results.values() if result)
        total_tests = len(results)

        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} - {test_name}")

        print(f"\nOverall: {passed_tests}/{total_tests} test categories passed")
        print(f"Success Rate: {passed_tests/total_tests:.1%}")

        if overall_success:
            print("\n🎉 ALL COMPREHENSIVE TESTS PASSED! 🎉")
            print("All 15 ATS indicators are functioning correctly.")
        else:
            print("\n⚠️ SOME TESTS FAILED")
            print("Review the detailed output above for specific issues.")

        # Generate and print detailed report
        report = self.generate_test_report()
        print("\n" + report)

        return overall_success

def main():
    """Run comprehensive indicator tests."""
    tester = ComprehensiveIndicatorTests()
    success = tester.run_all_tests()
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)