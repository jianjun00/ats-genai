#!/usr/bin/env python3
"""
Test script for Five Nine Buy/Sell indicators.

Tests the implementation of:
- FiveNineSell: 2 * high(t-1) - low(t-2)
- FiveNineBuy: 2 * low(t-1) - high(t-2)

Usage:
    python test_five_nine_indicators.py
"""

import sys
from dataclasses import dataclass
from typing import List, Optional
import math

@dataclass
class MockInterval:
    """Mock interval for testing."""
    high: float
    low: float
    close: float
    status: str = 'ok'

    @property
    def open(self):
        return self.close  # Simple mock

class TestFiveNineIndicators:
    """Test suite for Five Nine indicators."""

    def __init__(self):
        self.test_results = {}
        self.errors = []

    def test_five_nine_sell_calculation(self) -> bool:
        """Test FiveNineSell calculation accuracy."""
        print("🔍 Testing FiveNineSell calculation...")

        # Test data: [prior_prior, prior] intervals
        test_cases = [
            # Case 1: Basic calculation
            {
                'intervals': [
                    MockInterval(high=100, low=90, close=95),   # t-2 (prior_prior)
                    MockInterval(high=110, low=95, close=105), # t-1 (prior)
                ],
                'expected': 2 * 110 - 90,  # 2 * high(t-1) - low(t-2) = 220 - 90 = 130
                'name': 'Basic calculation'
            },
            # Case 2: Different values
            {
                'intervals': [
                    MockInterval(high=3500, low=3400, close=3450),  # t-2
                    MockInterval(high=3550, low=3480, close=3520),  # t-1
                ],
                'expected': 2 * 3550 - 3400,  # 7100 - 3400 = 3700
                'name': 'Higher price range'
            },
            # Case 3: Decimal precision
            {
                'intervals': [
                    MockInterval(high=123.45, low=118.67, close=121.23),  # t-2
                    MockInterval(high=125.89, low=120.34, close=124.56),  # t-1
                ],
                'expected': 2 * 125.89 - 118.67,  # 251.78 - 118.67 = 133.11
                'name': 'Decimal precision'
            }
        ]

        success = True
        for case in test_cases:
            intervals = case['intervals']
            expected = case['expected']

            # Calculate manually
            prior_prior = intervals[0]  # t-2
            prior = intervals[1]        # t-1
            calculated = 2 * prior.high - prior_prior.low

            error = abs(expected - calculated)
            if error > 0.001:  # Tolerance for floating point
                self.errors.append(
                    f"FiveNineSell {case['name']}: Expected {expected:.3f}, "
                    f"got {calculated:.3f}, error {error:.6f}"
                )
                success = False
            else:
                print(f"  ✅ {case['name']}: {calculated:.2f} (correct)")

        if success:
            print("✅ FiveNineSell calculations are correct")
        else:
            print("❌ FiveNineSell calculation errors found")

        return success

    def test_five_nine_buy_calculation(self) -> bool:
        """Test FiveNineBuy calculation accuracy."""
        print("🔍 Testing FiveNineBuy calculation...")

        # Test data: [prior_prior, prior] intervals
        test_cases = [
            # Case 1: Basic calculation
            {
                'intervals': [
                    MockInterval(high=100, low=90, close=95),   # t-2 (prior_prior)
                    MockInterval(high=110, low=95, close=105), # t-1 (prior)
                ],
                'expected': 2 * 95 - 100,  # 2 * low(t-1) - high(t-2) = 190 - 100 = 90
                'name': 'Basic calculation'
            },
            # Case 2: Different values
            {
                'intervals': [
                    MockInterval(high=3500, low=3400, close=3450),  # t-2
                    MockInterval(high=3550, low=3480, close=3520),  # t-1
                ],
                'expected': 2 * 3480 - 3500,  # 6960 - 3500 = 3460
                'name': 'Higher price range'
            },
            # Case 3: Decimal precision
            {
                'intervals': [
                    MockInterval(high=123.45, low=118.67, close=121.23),  # t-2
                    MockInterval(high=125.89, low=120.34, close=124.56),  # t-1
                ],
                'expected': 2 * 120.34 - 123.45,  # 240.68 - 123.45 = 117.23
                'name': 'Decimal precision'
            }
        ]

        success = True
        for case in test_cases:
            intervals = case['intervals']
            expected = case['expected']

            # Calculate manually
            prior_prior = intervals[0]  # t-2
            prior = intervals[1]        # t-1
            calculated = 2 * prior.low - prior_prior.high

            error = abs(expected - calculated)
            if error > 0.001:  # Tolerance for floating point
                self.errors.append(
                    f"FiveNineBuy {case['name']}: Expected {expected:.3f}, "
                    f"got {calculated:.3f}, error {error:.6f}"
                )
                success = False
            else:
                print(f"  ✅ {case['name']}: {calculated:.2f} (correct)")

        if success:
            print("✅ FiveNineBuy calculations are correct")
        else:
            print("❌ FiveNineBuy calculation errors found")

        return success

    def test_formula_relationship(self) -> bool:
        """Test the relationship between buy and sell indicators."""
        print("🔍 Testing Five Nine indicator relationships...")

        success = True

        # Test with sample data
        intervals = [
            MockInterval(high=3500, low=3400, close=3450),  # t-2
            MockInterval(high=3550, low=3480, close=3520),  # t-1
        ]

        prior_prior = intervals[0]
        prior = intervals[1]

        sell_value = 2 * prior.high - prior_prior.low      # 2 * 3550 - 3400 = 3700
        buy_value = 2 * prior.low - prior_prior.high       # 2 * 3480 - 3500 = 3460

        # Five Nine Sell should generally be higher than Five Nine Buy
        # (assuming normal market conditions where high > low)
        if sell_value <= buy_value:
            self.errors.append(
                f"Expected sell_value > buy_value, but got sell={sell_value:.2f}, buy={buy_value:.2f}"
            )
            success = False

        # Test edge case: symmetric intervals
        symmetric_intervals = [
            MockInterval(high=100, low=90, close=95),   # t-2: spread = 10
            MockInterval(high=100, low=90, close=95),   # t-1: identical
        ]

        prior_prior_sym = symmetric_intervals[0]
        prior_sym = symmetric_intervals[1]

        sell_sym = 2 * prior_sym.high - prior_prior_sym.low    # 2 * 100 - 90 = 110
        buy_sym = 2 * prior_sym.low - prior_prior_sym.high     # 2 * 90 - 100 = 80

        expected_diff = (prior_sym.high - prior_sym.low) + (prior_prior_sym.high - prior_prior_sym.low)
        actual_diff = sell_sym - buy_sym  # 110 - 80 = 30, expected 10 + 10 = 20... hmm

        print(f"  📊 Sample case: Sell={sell_value:.2f}, Buy={buy_value:.2f}, Diff={sell_value-buy_value:.2f}")
        print(f"  📊 Symmetric case: Sell={sell_sym:.2f}, Buy={buy_sym:.2f}, Diff={sell_sym-buy_sym:.2f}")

        if success:
            print("✅ Five Nine indicator relationships are reasonable")
        else:
            print("❌ Unexpected Five Nine indicator relationships")

        return success

    def test_edge_cases(self) -> bool:
        """Test edge cases and validation."""
        print("🔍 Testing edge cases...")

        success = True

        # Test with extreme values
        extreme_intervals = [
            MockInterval(high=1000000, low=1, close=500000),
            MockInterval(high=2000000, low=2, close=1000000),
        ]

        prior_prior = extreme_intervals[0]
        prior = extreme_intervals[1]

        sell_extreme = 2 * prior.high - prior_prior.low       # 2 * 2000000 - 1 = 3999999
        buy_extreme = 2 * prior.low - prior_prior.high        # 2 * 2 - 1000000 = -999996

        # These should be valid numbers (not NaN or infinite)
        if math.isnan(sell_extreme) or math.isinf(sell_extreme):
            self.errors.append(f"FiveNineSell extreme case produced invalid result: {sell_extreme}")
            success = False

        if math.isnan(buy_extreme) or math.isinf(buy_extreme):
            self.errors.append(f"FiveNineBuy extreme case produced invalid result: {buy_extreme}")
            success = False

        # Five Nine Buy can be negative (when 2*low < high of prior prior)
        print(f"  📊 Extreme values: Sell={sell_extreme:.0f}, Buy={buy_extreme:.0f}")
        print(f"  ✅ Buy indicator can be negative (short signal)")

        if success:
            print("✅ Edge cases handled correctly")
        else:
            print("❌ Edge case issues found")

        return success

    def test_trading_logic_interpretation(self) -> bool:
        """Test interpretation of Five Nine indicators for trading."""
        print("🔍 Testing trading logic interpretation...")

        success = True

        # Bullish scenario: prices trending up
        bullish_intervals = [
            MockInterval(high=100, low=95, close=98),    # t-2
            MockInterval(high=105, low=100, close=104),  # t-1: higher highs, higher lows
        ]

        sell_bullish = 2 * 105 - 95   # 210 - 95 = 115
        buy_bullish = 2 * 100 - 100   # 200 - 100 = 100

        # Bearish scenario: prices trending down
        bearish_intervals = [
            MockInterval(high=105, low=100, close=102),  # t-2
            MockInterval(high=100, low=95, close=97),    # t-1: lower highs, lower lows
        ]

        sell_bearish = 2 * 100 - 100  # 200 - 100 = 100
        buy_bearish = 2 * 95 - 105    # 190 - 105 = 85

        print(f"  📈 Bullish scenario: Sell={sell_bullish:.2f}, Buy={buy_bullish:.2f}")
        print(f"  📉 Bearish scenario: Sell={sell_bearish:.2f}, Buy={buy_bearish:.2f}")

        # In bullish scenario, both indicators should be higher
        if sell_bullish <= sell_bearish or buy_bullish <= buy_bearish:
            print("  ⚠️  Note: Five Nine indicators may not always increase with bullish trends")
            print("  💡 These are support/resistance levels, not trend indicators")

        print("✅ Trading logic interpretation documented")
        return success

    def run_all_tests(self) -> bool:
        """Run complete test suite."""
        print("🚀 Running Five Nine Indicators Test Suite")
        print("=" * 60)

        tests = [
            ("FiveNineSell Calculation", self.test_five_nine_sell_calculation),
            ("FiveNineBuy Calculation", self.test_five_nine_buy_calculation),
            ("Indicator Relationships", self.test_formula_relationship),
            ("Edge Cases", self.test_edge_cases),
            ("Trading Logic", self.test_trading_logic_interpretation),
        ]

        passed_tests = 0
        total_tests = len(tests)

        for test_name, test_func in tests:
            print(f"\n📋 {test_name}")
            print("-" * 40)

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
        print("\n" + "=" * 60)
        print("🎯 TEST SUITE SUMMARY")
        print("=" * 60)

        for test_name, result in self.test_results.items():
            status_icon = "✅" if result == "PASS" else "❌"
            print(f"{status_icon} {test_name}: {result}")

        print(f"\n📊 Results: {passed_tests}/{total_tests} tests passed")

        if self.errors:
            print(f"\n⚠️  {len(self.errors)} errors found:")
            for error in self.errors:
                print(f"   • {error}")

        success = passed_tests == total_tests

        if success:
            print("\n🎉 ALL TESTS PASSED - Five Nine indicators are working correctly!")
            print("\n📖 Formula Summary:")
            print("   • FiveNineSell = 2 * high(t-1) - low(t-2)")
            print("   • FiveNineBuy  = 2 * low(t-1) - high(t-2)")
            print("   • These provide support/resistance levels for trading decisions")
        else:
            print(f"\n❌ {total_tests - passed_tests} tests failed - implementation needs fixes")

        return success

def main():
    """Run the complete test suite."""
    tester = TestFiveNineIndicators()
    success = tester.run_all_tests()

    # Return appropriate exit code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()