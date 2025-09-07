#!/usr/bin/env python3

import sys
sys.path.append('src')

import numpy as np
from src.signals.indicator import FiveOneBuy, FiveOneSell
from src.storage.file_based_minute_manager import MinuteBar
from datetime import datetime, timedelta

def test_five_one_buy():
    """Test FiveOneBuy indicator with conditional logic"""
    print("=== Testing FiveOneBuy Indicator ===")

    indicator = FiveOneBuy()

    # Test case 1: low(t-1) > low(t-2) - should calculate
    print("\nTest Case 1: low(t-1) > low(t-2) (should calculate)")
    base_time = datetime.now()

    # t-2: low = 100
    bar1 = MinuteBar(
        symbol="TEST", timestamp=base_time,
        open=105, high=110, low=100, close=108,
        volume=1000
    )
    indicator.update([bar1])
    print(f"After bar1 (low=100): latest_five_one_buy = {indicator.latest_five_one_buy}")

    # t-1: low = 102 (higher than previous)
    bar2 = MinuteBar(
        symbol="TEST", timestamp=base_time + timedelta(minutes=1),
        open=103, high=108, low=102, close=106,
        volume=1000
    )
    indicator.update([bar2])
    expected = 2 * 102 - 100  # 2 * low(t-1) - low(t-2) = 204 - 100 = 104
    print(f"After bar2 (low=102 > 100): latest_five_one_buy = {indicator.latest_five_one_buy}")
    print(f"Expected: 2 * 102 - 100 = {expected}")

    assert indicator.latest_five_one_buy == expected, f"Expected {expected}, got {indicator.latest_five_one_buy}"
    assert indicator.get_value() == expected
    print("✅ Case 1 passed")

    # Test case 2: low(t-1) <= low(t-2) - should not calculate
    print("\nTest Case 2: low(t-1) <= low(t-2) (should not calculate)")

    # t: low = 101 (lower than t-1)
    bar3 = MinuteBar(
        symbol="TEST", timestamp=base_time + timedelta(minutes=2),
        open=104, high=107, low=101, close=105,
        volume=1000
    )
    indicator.update([bar3])
    print(f"After bar3 (low=101 <= 102): latest_five_one_buy = {indicator.latest_five_one_buy}")
    print(f"get_value() returns: {indicator.get_value()}")

    assert indicator.latest_five_one_buy is None, f"Expected None, got {indicator.latest_five_one_buy}"
    assert indicator.get_value() is None
    print("✅ Case 2 passed")

    # Test case 3: Equal lows - should not calculate
    print("\nTest Case 3: low(t-1) == low(t-2) (should not calculate)")

    # t+1: low = 101 (equal to previous)
    bar4 = MinuteBar(
        symbol="TEST", timestamp=base_time + timedelta(minutes=3),
        open=103, high=106, low=101, close=104,
        volume=1000
    )
    indicator.update([bar4])
    print(f"After bar4 (low=101 == 101): latest_five_one_buy = {indicator.latest_five_one_buy}")

    assert indicator.latest_five_one_buy is None, f"Expected None, got {indicator.latest_five_one_buy}"
    print("✅ Case 3 passed")

def test_five_one_sell():
    """Test FiveOneSell indicator with conditional logic"""
    print("\n=== Testing FiveOneSell Indicator ===")

    indicator = FiveOneSell()

    # Test case 1: high(t-1) < high(t-2) - should calculate
    print("\nTest Case 1: high(t-1) < high(t-2) (should calculate)")
    base_time = datetime.now()

    # t-2: high = 110
    bar1 = MinuteBar(
        symbol="TEST", timestamp=base_time,
        open=105, high=110, low=100, close=108,
        volume=1000
    )
    indicator.update([bar1])
    print(f"After bar1 (high=110): latest_five_one_sell = {indicator.latest_five_one_sell}")

    # t-1: high = 108 (lower than previous)
    bar2 = MinuteBar(
        symbol="TEST", timestamp=base_time + timedelta(minutes=1),
        open=103, high=108, low=102, close=106,
        volume=1000
    )
    indicator.update([bar2])
    expected = 2 * 108 - 110  # 2 * high(t-1) - high(t-2) = 216 - 110 = 106
    print(f"After bar2 (high=108 < 110): latest_five_one_sell = {indicator.latest_five_one_sell}")
    print(f"Expected: 2 * 108 - 110 = {expected}")

    assert indicator.latest_five_one_sell == expected, f"Expected {expected}, got {indicator.latest_five_one_sell}"
    assert indicator.get_value() == expected
    print("✅ Case 1 passed")

    # Test case 2: high(t-1) >= high(t-2) - should not calculate
    print("\nTest Case 2: high(t-1) >= high(t-2) (should not calculate)")

    # t: high = 109 (higher than t-1)
    bar3 = MinuteBar(
        symbol="TEST", timestamp=base_time + timedelta(minutes=2),
        open=104, high=109, low=101, close=105,
        volume=1000
    )
    indicator.update([bar3])
    print(f"After bar3 (high=109 >= 108): latest_five_one_sell = {indicator.latest_five_one_sell}")
    print(f"get_value() returns: {indicator.get_value()}")

    assert indicator.latest_five_one_sell is None, f"Expected None, got {indicator.latest_five_one_sell}"
    assert indicator.get_value() is None
    print("✅ Case 2 passed")

    # Test case 3: Equal highs - should not calculate
    print("\nTest Case 3: high(t-1) == high(t-2) (should not calculate)")

    # t+1: high = 109 (equal to previous)
    bar4 = MinuteBar(
        symbol="TEST", timestamp=base_time + timedelta(minutes=3),
        open=103, high=109, low=101, close=104,
        volume=1000
    )
    indicator.update([bar4])
    print(f"After bar4 (high=109 == 109): latest_five_one_sell = {indicator.latest_five_one_sell}")

    assert indicator.latest_five_one_sell is None, f"Expected None, got {indicator.latest_five_one_sell}"
    print("✅ Case 3 passed")

def test_edge_cases():
    """Test edge cases and error handling"""
    print("\n=== Testing Edge Cases ===")

    # Test insufficient data
    print("\nTest: Insufficient data")
    buy_indicator = FiveOneBuy()
    sell_indicator = FiveOneSell()

    base_time = datetime.now()
    bar1 = MinuteBar(symbol="TEST", timestamp=base_time, open=100, high=105, low=95, close=102, volume=1000)

    # Only one bar - should not calculate
    buy_indicator.update([bar1])
    sell_indicator.update([bar1])

    print(f"FiveOneBuy with 1 bar: {buy_indicator.get_value()}")
    print(f"FiveOneSell with 1 bar: {sell_indicator.get_value()}")

    assert buy_indicator.get_value() is None
    assert sell_indicator.get_value() is None
    print("✅ Insufficient data test passed")

    # Test extreme values
    print("\nTest: Extreme values")
    bar2 = MinuteBar(symbol="TEST", timestamp=base_time + timedelta(minutes=1),
                     open=1, high=1000000, low=0.01, close=500000, volume=1000)

    buy_indicator.update([bar2])
    sell_indicator.update([bar2])

    # Should handle extreme values without error
    buy_val = buy_indicator.get_value()
    sell_val = sell_indicator.get_value()

    print(f"FiveOneBuy with extreme values: {buy_val}")
    print(f"FiveOneSell with extreme values: {sell_val}")
    print("✅ Extreme values test passed")

def test_integration_with_existing_indicators():
    """Test that Five One indicators work alongside existing indicators"""
    print("\n=== Testing Integration ===")

    from src.signals.indicator import H11, L11, EBOT, ETOP

    # Create multiple indicators
    buy_indicator = FiveOneBuy()
    sell_indicator = FiveOneSell()
    h11 = H11()
    l11 = L11()
    ebot = EBOT()
    etop = ETOP()

    # Create test data
    base_time = datetime.now()
    bars = []
    for i in range(10):
        # Create varied price data
        base_price = 100 + i * 2
        bar = MinuteBar(
            symbol="AAPL", timestamp=base_time + timedelta(minutes=i),
            open=base_price + np.random.uniform(-1, 1),
            high=base_price + np.random.uniform(0, 2),
            low=base_price + np.random.uniform(-2, 0),
            close=base_price + np.random.uniform(-1, 1),
            volume=1000
        )
        bars.append(bar)

    # Update all indicators
    for bar in bars:
        buy_indicator.update([bar])
        sell_indicator.update([bar])
        h11.update([bar])
        l11.update([bar])
        ebot.update([bar])
        etop.update([bar])

    print(f"FiveOneBuy final value: {buy_indicator.get_value()}")
    print(f"FiveOneSell final value: {sell_indicator.get_value()}")
    print(f"H11 final value: {h11.get_value()}")
    print(f"L11 final value: {l11.get_value()}")
    print(f"EBOT final value: {ebot.get_value()}")
    print(f"ETOP final value: {etop.get_value()}")

    # All should return values or None without errors
    print("✅ Integration test passed")

def main():
    """Run all Five One indicator tests"""
    print("Five One Indicators Comprehensive Test Suite")
    print("=" * 50)

    try:
        test_five_one_buy()
        test_five_one_sell()
        test_edge_cases()
        test_integration_with_existing_indicators()

        print("\n" + "=" * 50)
        print("🎉 ALL FIVE ONE INDICATOR TESTS PASSED! 🎉")
        print("=" * 50)

        return True

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)