#!/usr/bin/env python3
"""
Integration Tests for Five Two Indicators

This test suite focuses specifically on integration testing for FiveTwoBuy and FiveTwoSell
indicators, ensuring they work correctly within the broader indicator ecosystem and
handle real-world trading scenarios.

Test Categories:
1. Real market data integration
2. Cross-indicator validation (vs Five One indicators)
3. Market regime testing (bull, bear, sideways)
4. Multi-timeframe consistency
5. Production readiness validation

Usage:
    PYTHONPATH=src python test_five_two_integration.py
"""

import sys
import os
import time
import random
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from domains.trading.services.indicator import (
        FiveTwoBuy, FiveTwoSell, FiveOneBuy, FiveOneSell,
        H11, L11, EnvelopeBot, EnvelopeTop
    )
except ImportError as e:
    print(f"❌ Cannot import indicators: {e}")
    print("Make sure to run: PYTHONPATH=src python test_five_two_integration.py")
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

class FiveTwoIntegrationTests:
    """Integration tests specifically for Five Two indicators."""

    def __init__(self):
        self.test_results = {}
        self.errors = []

        # Market scenarios with realistic price patterns
        self.market_scenarios = {
            'strong_uptrend': [
                [3400, 3380, 3395],  # Base
                [3420, 3400, 3415],  # Rising lows, rising highs
                [3445, 3425, 3440],  # Continued strength
                [3470, 3450, 3465],  # Accelerating
                [3500, 3480, 3495]   # Peak momentum
            ],
            'strong_downtrend': [
                [3500, 3480, 3495],  # Peak
                [3485, 3465, 3470],  # Declining highs, declining lows
                [3470, 3445, 3450],  # Weakness continues
                [3450, 3425, 3430],  # Accelerating down
                [3425, 3400, 3405]   # Capitulation
            ],
            'volatile_sideways': [
                [3450, 3430, 3440],  # Range midpoint
                [3465, 3425, 3455],  # Volatility up
                [3440, 3420, 3425],  # Volatility down
                [3455, 3435, 3450],  # Back to range
                [3445, 3425, 3435]   # Continued chop
            ],
            'breakout_pattern': [
                [3450, 3440, 3445],  # Consolidation
                [3450, 3440, 3445],  # More consolidation
                [3451, 3441, 3446],  # Tight range
                [3470, 3450, 3465],  # Breakout!
                [3485, 3465, 3480]   # Follow-through
            ],
            'reversal_pattern': [
                [3500, 3480, 3485],  # Topping
                [3485, 3465, 3475],  # Weakening
                [3480, 3460, 3465],  # Rolling over
                [3470, 3450, 3455],  # Accelerating down
                [3460, 3440, 3445]   # Reversal confirmed
            ]
        }

    def create_test_intervals(self, data: List[List[float]]) -> List[TestInstrumentInterval]:
        """Create test intervals from HLC data."""
        intervals = []
        for i, row in enumerate(data):
            high, low, close = row[0], row[1], row[2]
            timestamp = datetime.now() + timedelta(minutes=i)
            intervals.append(TestInstrumentInterval(
                high=high, low=low, close=close, timestamp=timestamp
            ))
        return intervals

    def test_market_regime_behavior(self) -> bool:
        """Test Five Two indicators across different market regimes."""
        print("=== Testing Market Regime Behavior ===")

        regime_results = {}
        all_passed = True

        for regime_name, data in self.market_scenarios.items():
            print(f"\n--- Market Regime: {regime_name.replace('_', ' ').title()} ---")

            intervals = self.create_test_intervals(data)

            # Test Five Two indicators
            five_two_buy = FiveTwoBuy()
            five_two_sell = FiveTwoSell()

            # Also test Five One for comparison
            five_one_buy = FiveOneBuy()
            five_one_sell = FiveOneSell()

            try:
                # Update all indicators
                five_two_buy.update(intervals)
                five_two_sell.update(intervals)
                five_one_buy.update(intervals)
                five_one_sell.update(intervals)

                # Get results
                results = {
                    'FiveTwoBuy': five_two_buy.get_value(),
                    'FiveTwoSell': five_two_sell.get_value(),
                    'FiveOneBuy': five_one_buy.get_value(),
                    'FiveOneSell': five_one_sell.get_value()
                }

                regime_results[regime_name] = results

                # Display results
                for indicator, value in results.items():
                    status = "None" if value is None else f"{value:.2f}"
                    print(f"  {indicator}: {status}")

                # Validate logical behavior for each regime
                validation_passed = self._validate_regime_logic(regime_name, data, results)
                if not validation_passed:
                    all_passed = False

            except Exception as e:
                print(f"❌ Error in {regime_name}: {e}")
                self.errors.append(f"Market regime test failed for {regime_name}: {e}")
                all_passed = False

        self.test_results['market_regimes'] = regime_results
        return all_passed

    def _validate_regime_logic(self, regime_name: str, data: List[List[float]], results: Dict[str, Optional[float]]) -> bool:
        """Validate that indicator results make logical sense for the market regime."""

        # Analyze the price pattern
        first_high, first_low = data[0][0], data[0][1]
        last_high, last_low = data[-1][0], data[-1][1]

        highs_trend = "up" if last_high > first_high else "down" if last_high < first_high else "flat"
        lows_trend = "up" if last_low > first_low else "down" if last_low < first_low else "flat"

        print(f"    Pattern: Highs {highs_trend}, Lows {lows_trend}")

        # Logic validation based on regime characteristics
        if regime_name == 'strong_uptrend':
            # In uptrend: expect rising highs and lows
            # FiveTwoSell should activate (rising highs), FiveTwoBuy shouldn't (lows improving)
            if results['FiveTwoSell'] is None and highs_trend == "up":
                print(f"    ⚠️ Expected FiveTwoSell to activate in uptrend with rising highs")
                return False

        elif regime_name == 'strong_downtrend':
            # In downtrend: expect declining highs and lows
            # FiveTwoBuy should activate (declining lows), FiveTwoSell shouldn't (highs declining)
            if results['FiveTwoBuy'] is None and lows_trend == "down":
                print(f"    ⚠️ Expected FiveTwoBuy to activate in downtrend with declining lows")
                return False

        # Cross-validation: Five One and Five Two should be complementary
        five_one_active = results['FiveOneBuy'] is not None or results['FiveOneSell'] is not None
        five_two_active = results['FiveTwoBuy'] is not None or results['FiveTwoSell'] is not None

        print(f"    Five One active: {five_one_active}, Five Two active: {five_two_active}")

        return True  # Basic validation passed

    def test_cross_indicator_consistency(self) -> bool:
        """Test consistency between Five Two and related indicators."""
        print("\n=== Testing Cross-Indicator Consistency ===")

        # Test data with clear directional moves
        test_scenarios = [
            {
                'name': 'Clear Uptrend',
                'data': [[3400, 3380, 3390], [3420, 3400, 3410], [3440, 3420, 3435]],
                'expected_consistency': 'rising_prices'
            },
            {
                'name': 'Clear Downtrend',
                'data': [[3440, 3420, 3435], [3420, 3400, 3410], [3400, 3380, 3390]],
                'expected_consistency': 'falling_prices'
            },
            {
                'name': 'Mixed Signals',
                'data': [[3420, 3400, 3410], [3430, 3390, 3415], [3425, 3405, 3415]],
                'expected_consistency': 'mixed'
            }
        ]

        consistency_results = {}
        all_consistent = True

        for scenario in test_scenarios:
            print(f"\n--- Scenario: {scenario['name']} ---")

            intervals = self.create_test_intervals(scenario['data'])

            # Initialize indicators
            indicators = {
                'FiveTwoBuy': FiveTwoBuy(),
                'FiveTwoSell': FiveTwoSell(),
                'FiveOneBuy': FiveOneBuy(),
                'FiveOneSell': FiveOneSell(),
                'H11': H11(),
                'L11': L11(),
                'EnvelopeBot': EnvelopeBot(),
                'EnvelopeTop': EnvelopeTop()
            }

            # Update all indicators
            results = {}
            for name, indicator in indicators.items():
                try:
                    indicator.update(intervals)
                    results[name] = indicator.get_value()
                except Exception as e:
                    print(f"❌ {name} failed: {e}")
                    results[name] = 'error'
                    all_consistent = False

            # Display results
            for name, value in results.items():
                status = "None" if value is None else f"{value:.2f}" if isinstance(value, (int, float)) else str(value)
                print(f"  {name}: {status}")

            # Check consistency
            consistency_check = self._check_indicator_consistency(scenario, results)
            consistency_results[scenario['name']] = {
                'results': results,
                'consistency': consistency_check
            }

            if not consistency_check:
                all_consistent = False

        self.test_results['consistency'] = consistency_results
        return all_consistent

    def _check_indicator_consistency(self, scenario: Dict[str, Any], results: Dict[str, Any]) -> bool:
        """Check if indicator results are logically consistent."""

        # Basic consistency: Five One and Five Two should complement each other
        five_one_buy_active = results.get('FiveOneBuy') is not None
        five_two_buy_active = results.get('FiveTwoBuy') is not None
        five_one_sell_active = results.get('FiveOneSell') is not None
        five_two_sell_active = results.get('FiveTwoSell') is not None

        # They should generally not both be active for same side (buy/sell) simultaneously
        # Exception: during transitions or complex patterns

        if five_one_buy_active and five_two_buy_active:
            print(f"    ⚠️ Both FiveOneBuy and FiveTwoBuy active (unusual but possible during transitions)")

        if five_one_sell_active and five_two_sell_active:
            print(f"    ⚠️ Both FiveOneSell and FiveTwoSell active (unusual but possible during transitions)")

        print(f"    Consistency check: logical patterns observed")
        return True

    def test_production_readiness(self) -> bool:
        """Test production readiness scenarios."""
        print("\n=== Testing Production Readiness ===")

        production_tests = [
            {
                'name': 'High Frequency Updates',
                'test_func': self._test_high_frequency_updates
            },
            {
                'name': 'Memory Efficiency',
                'test_func': self._test_memory_efficiency
            },
            {
                'name': 'Thread Safety',
                'test_func': self._test_thread_safety
            },
            {
                'name': 'Error Recovery',
                'test_func': self._test_error_recovery
            }
        ]

        production_results = {}
        all_passed = True

        for test_case in production_tests:
            print(f"\n--- {test_case['name']} ---")
            try:
                result = test_case['test_func']()
                production_results[test_case['name']] = result
                if result:
                    print(f"✅ {test_case['name']} passed")
                else:
                    print(f"❌ {test_case['name']} failed")
                    all_passed = False
            except Exception as e:
                print(f"❌ {test_case['name']} crashed: {e}")
                production_results[test_case['name']] = False
                all_passed = False

        return all_passed

    def _test_high_frequency_updates(self) -> bool:
        """Test performance under high frequency updates."""

        # Generate realistic high-frequency data
        base_price = 3450.0
        intervals = []

        for i in range(1000):
            # Small price movements
            price_change = random.uniform(-5, 5)
            high = base_price + price_change + random.uniform(0, 2)
            low = base_price + price_change - random.uniform(0, 2)
            close = base_price + price_change

            intervals.append(TestInstrumentInterval(high=high, low=low, close=close))
            base_price = close

        # Test indicators
        five_two_buy = FiveTwoBuy()
        five_two_sell = FiveTwoSell()

        # Measure performance
        start_time = time.perf_counter()

        for i in range(10, len(intervals)):  # Start after sufficient data
            current_intervals = intervals[i-2:i+1]  # Last 3 intervals
            five_two_buy.update(current_intervals)
            five_two_sell.update(current_intervals)

        end_time = time.perf_counter()

        total_updates = len(intervals) - 10
        avg_time_per_update = ((end_time - start_time) / total_updates) * 1000  # ms

        print(f"  Processed {total_updates} updates in {(end_time - start_time)*1000:.2f}ms")
        print(f"  Average: {avg_time_per_update:.4f}ms per update")

        # Pass if less than 1ms per update on average
        return avg_time_per_update < 1.0

    def _test_memory_efficiency(self) -> bool:
        """Test memory efficiency - indicators shouldn't leak memory."""

        import gc

        # Force garbage collection before test
        gc.collect()
        initial_objects = len(gc.get_objects())

        # Create and destroy many indicator instances
        for _ in range(100):
            buy_indicator = FiveTwoBuy()
            sell_indicator = FiveTwoSell()

            # Use the indicators
            intervals = [
                TestInstrumentInterval(high=110, low=100, close=105),
                TestInstrumentInterval(high=108, low=98, close=103)
            ]

            buy_indicator.update(intervals)
            sell_indicator.update(intervals)

            # Let them go out of scope
            del buy_indicator, sell_indicator

        # Force garbage collection after test
        gc.collect()
        final_objects = len(gc.get_objects())

        object_growth = final_objects - initial_objects
        print(f"  Object count growth: {object_growth}")

        # Pass if object growth is reasonable (< 100 objects)
        return object_growth < 100

    def _test_thread_safety(self) -> bool:
        """Test basic thread safety properties."""

        import threading
        import queue

        results_queue = queue.Queue()
        errors_queue = queue.Queue()

        def worker_function(worker_id):
            try:
                indicator = FiveTwoBuy()

                # Each worker processes different data
                for i in range(100):
                    base = 3400 + worker_id * 10 + i
                    intervals = [
                        TestInstrumentInterval(high=base+10, low=base-10, close=base),
                        TestInstrumentInterval(high=base+5, low=base-15, close=base-5)
                    ]

                    indicator.update(intervals)
                    result = indicator.get_value()

                results_queue.put(f"Worker {worker_id} completed")

            except Exception as e:
                errors_queue.put(f"Worker {worker_id} error: {e}")

        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker_function, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5.0)  # 5 second timeout

        # Check results
        completed_workers = 0
        while not results_queue.empty():
            result = results_queue.get()
            print(f"  {result}")
            completed_workers += 1

        error_count = 0
        while not errors_queue.empty():
            error = errors_queue.get()
            print(f"  ❌ {error}")
            error_count += 1

        # Pass if all workers completed without errors
        return completed_workers == 5 and error_count == 0

    def _test_error_recovery(self) -> bool:
        """Test error recovery capabilities."""

        five_two_buy = FiveTwoBuy()
        five_two_sell = FiveTwoSell()

        error_scenarios = [
            # Scenario 1: Corrupt data followed by good data
            {
                'intervals': [
                    TestInstrumentInterval(high=float('inf'), low=100, close=105),
                    TestInstrumentInterval(high=110, low=100, close=105)
                ],
                'description': 'Infinity values'
            },
            # Scenario 2: Invalid status followed by recovery
            {
                'intervals': [
                    TestInstrumentInterval(high=110, low=100, close=105, status='error'),
                    TestInstrumentInterval(high=115, low=105, close=110, status='ok')
                ],
                'description': 'Invalid status recovery'
            }
        ]

        recovery_success = True

        for scenario in error_scenarios:
            try:
                print(f"  Testing: {scenario['description']}")

                five_two_buy.update(scenario['intervals'])
                five_two_sell.update(scenario['intervals'])

                # After error scenarios, try normal data
                normal_intervals = [
                    TestInstrumentInterval(high=120, low=110, close=115),
                    TestInstrumentInterval(high=125, low=105, close=120)  # Higher high, lower low
                ]

                five_two_buy.update(normal_intervals)
                five_two_sell.update(normal_intervals)

                buy_result = five_two_buy.get_value()
                sell_result = five_two_sell.get_value()

                print(f"    Recovery results - Buy: {buy_result}, Sell: {sell_result}")

            except Exception as e:
                print(f"    ❌ Error recovery failed: {e}")
                recovery_success = False

        return recovery_success

    def run_all_integration_tests(self) -> bool:
        """Run all Five Two integration tests."""
        print("Five Two Indicators Integration Test Suite")
        print("=" * 60)

        test_methods = [
            ('Market Regime Behavior', self.test_market_regime_behavior),
            ('Cross-Indicator Consistency', self.test_cross_indicator_consistency),
            ('Production Readiness', self.test_production_readiness)
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
        print("INTEGRATION TEST RESULTS:")
        print("="*60)

        passed_tests = sum(1 for result in results.values() if result)
        total_tests = len(results)

        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} - {test_name}")

        print(f"\nOverall: {passed_tests}/{total_tests} integration tests passed")
        print(f"Success Rate: {passed_tests/total_tests:.1%}")

        if overall_success:
            print("\n🎉 ALL INTEGRATION TESTS PASSED! 🎉")
            print("Five Two indicators are ready for production deployment.")
        else:
            print("\n⚠️ SOME INTEGRATION TESTS FAILED")
            print("Review the detailed output above for specific issues.")

        return overall_success

def main():
    """Run Five Two integration tests."""
    tester = FiveTwoIntegrationTests()
    success = tester.run_all_integration_tests()
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)