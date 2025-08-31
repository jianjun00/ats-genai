#!/usr/bin/env python3

import sys
sys.path.append('src')

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    from src.signals.indicator import FiveTwoBuy, FiveTwoSell
except ImportError as e:
    print(f"❌ Cannot import indicators: {e}")
    print("Make sure to run: PYTHONPATH=src python test_five_two_indicators.py")
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

def test_five_two_buy():
    """Test FiveTwoBuy indicator with conditional logic"""
    print("=== Testing FiveTwoBuy Indicator ===")
    
    indicator = FiveTwoBuy()
    
    # Test case 1: low(t-1) < low(t-2) - should calculate
    print("\nTest Case 1: low(t-1) < low(t-2) (should calculate)")
    
    # Create intervals: t-2 (low=102), t-1 (low=100 < 102)
    intervals = [
        TestInstrumentInterval(high=108, low=102, close=106),  # t-2
        TestInstrumentInterval(high=106, low=100, close=104),  # t-1: low=100 < 102
    ]
    
    indicator.update(intervals)
    expected = 2 * 100 - 102  # 2 * low(t-1) - low(t-2) = 200 - 102 = 98
    print(f"latest_five_two_buy = {indicator.latest_five_two_buy}")
    print(f"Expected: 2 * 100 - 102 = {expected}")
    
    assert indicator.latest_five_two_buy == expected, f"Expected {expected}, got {indicator.latest_five_two_buy}"
    assert indicator.get_value() == expected
    print("✅ Case 1 passed")
    
    # Test case 2: low(t-1) >= low(t-2) - should not calculate
    print("\nTest Case 2: low(t-1) >= low(t-2) (should not calculate)")
    
    # Create intervals where new low >= previous low
    intervals = [
        TestInstrumentInterval(high=106, low=100, close=104),  # t-1
        TestInstrumentInterval(high=108, low=103, close=105),  # t: low=103 >= 100
    ]
    
    indicator.update(intervals)
    print(f"latest_five_two_buy = {indicator.latest_five_two_buy}")
    print(f"get_value() returns: {indicator.get_value()}")
    
    assert indicator.latest_five_two_buy is None, f"Expected None, got {indicator.latest_five_two_buy}"
    assert indicator.get_value() is None
    print("✅ Case 2 passed")
    
    # Test case 3: Equal lows - should not calculate
    print("\nTest Case 3: low(t-1) == low(t-2) (should not calculate)")
    
    # Create intervals with equal lows
    intervals = [
        TestInstrumentInterval(high=108, low=103, close=105),  # t-1
        TestInstrumentInterval(high=107, low=103, close=104),  # t: low=103 == 103
    ]
    
    indicator.update(intervals)
    print(f"latest_five_two_buy = {indicator.latest_five_two_buy}")
    
    assert indicator.latest_five_two_buy is None, f"Expected None, got {indicator.latest_five_two_buy}"
    print("✅ Case 3 passed")

def test_five_two_sell():
    """Test FiveTwoSell indicator with conditional logic"""
    print("\n=== Testing FiveTwoSell Indicator ===")
    
    indicator = FiveTwoSell()
    
    # Test case 1: high(t-1) > high(t-2) - should calculate
    print("\nTest Case 1: high(t-1) > high(t-2) (should calculate)")
    
    # Create intervals: t-2 (high=108), t-1 (high=110 > 108)
    intervals = [
        TestInstrumentInterval(high=108, low=102, close=106),  # t-2
        TestInstrumentInterval(high=110, low=104, close=108),  # t-1: high=110 > 108
    ]
    
    indicator.update(intervals)
    expected = 2 * 110 - 108  # 2 * high(t-1) - high(t-2) = 220 - 108 = 112
    print(f"latest_five_two_sell = {indicator.latest_five_two_sell}")
    print(f"Expected: 2 * 110 - 108 = {expected}")
    
    assert indicator.latest_five_two_sell == expected, f"Expected {expected}, got {indicator.latest_five_two_sell}"
    assert indicator.get_value() == expected
    print("✅ Case 1 passed")
    
    # Test case 2: high(t-1) <= high(t-2) - should not calculate
    print("\nTest Case 2: high(t-1) <= high(t-2) (should not calculate)")
    
    # Create intervals where new high <= previous high
    intervals = [
        TestInstrumentInterval(high=110, low=104, close=108),  # t-1
        TestInstrumentInterval(high=107, low=101, close=105),  # t: high=107 <= 110
    ]
    
    indicator.update(intervals)
    print(f"latest_five_two_sell = {indicator.latest_five_two_sell}")
    print(f"get_value() returns: {indicator.get_value()}")
    
    assert indicator.latest_five_two_sell is None, f"Expected None, got {indicator.latest_five_two_sell}"
    assert indicator.get_value() is None
    print("✅ Case 2 passed")
    
    # Test case 3: Equal highs - should not calculate
    print("\nTest Case 3: high(t-1) == high(t-2) (should not calculate)")
    
    # Create intervals with equal highs
    intervals = [
        TestInstrumentInterval(high=107, low=101, close=105),  # t-1
        TestInstrumentInterval(high=107, low=102, close=104),  # t: high=107 == 107
    ]
    
    indicator.update(intervals)
    print(f"latest_five_two_sell = {indicator.latest_five_two_sell}")
    
    assert indicator.latest_five_two_sell is None, f"Expected None, got {indicator.latest_five_two_sell}"
    print("✅ Case 3 passed")

def test_five_two_opposite_conditions():
    """Test that Five Two indicators work opposite to Five One indicators"""
    print("\n=== Testing Five Two vs Five One Opposite Conditions ===")
    
    from src.signals.indicator import FiveOneBuy, FiveOneSell
    
    # Test scenario: declining lows (102 -> 100)
    print("\nScenario: Declining lows (102 -> 100)")
    
    five_one_buy = FiveOneBuy()
    five_two_buy = FiveTwoBuy()
    
    intervals_declining_low = [
        TestInstrumentInterval(high=108, low=102, close=106),  # t-2
        TestInstrumentInterval(high=106, low=100, close=104),  # t-1: low=100 < 102 (declining)
    ]
    
    five_one_buy.update(intervals_declining_low)
    five_two_buy.update(intervals_declining_low)
    
    print(f"FiveOneBuy (needs improving lows): {five_one_buy.get_value()}")  # Should be None
    print(f"FiveTwoBuy (needs declining lows): {five_two_buy.get_value()}")  # Should calculate
    
    assert five_one_buy.get_value() is None, "FiveOneBuy should be None for declining lows"
    assert five_two_buy.get_value() is not None, "FiveTwoBuy should calculate for declining lows"
    assert five_two_buy.get_value() == 98, f"Expected 98, got {five_two_buy.get_value()}"
    
    # Test scenario: rising highs (108 -> 110)
    print("\nScenario: Rising highs (108 -> 110)")
    
    five_one_sell = FiveOneSell()
    five_two_sell = FiveTwoSell()
    
    intervals_rising_high = [
        TestInstrumentInterval(high=108, low=102, close=106),  # t-2
        TestInstrumentInterval(high=110, low=104, close=108),  # t-1: high=110 > 108 (rising)
    ]
    
    five_one_sell.update(intervals_rising_high)
    five_two_sell.update(intervals_rising_high)
    
    print(f"FiveOneSell (needs declining highs): {five_one_sell.get_value()}")  # Should be None
    print(f"FiveTwoSell (needs rising highs): {five_two_sell.get_value()}")    # Should calculate
    
    assert five_one_sell.get_value() is None, "FiveOneSell should be None for rising highs"
    assert five_two_sell.get_value() is not None, "FiveTwoSell should calculate for rising highs"
    assert five_two_sell.get_value() == 112, f"Expected 112, got {five_two_sell.get_value()}"
    
    print("✅ Opposite conditions test passed")

def test_edge_cases():
    """Test edge cases and error handling"""
    print("\n=== Testing Edge Cases ===")
    
    # Test insufficient data
    print("\nTest: Insufficient data")
    buy_indicator = FiveTwoBuy()
    sell_indicator = FiveTwoSell()
    
    # Only one interval - should not calculate
    intervals = [TestInstrumentInterval(high=105, low=95, close=102)]
    
    buy_indicator.update(intervals)
    sell_indicator.update(intervals)
    
    print(f"FiveTwoBuy with 1 interval: {buy_indicator.get_value()}")
    print(f"FiveTwoSell with 1 interval: {sell_indicator.get_value()}")
    
    assert buy_indicator.get_value() is None
    assert sell_indicator.get_value() is None
    print("✅ Insufficient data test passed")
    
    # Test extreme values
    print("\nTest: Extreme values")
    intervals = [
        TestInstrumentInterval(high=1000000, low=500000, close=750000),
        TestInstrumentInterval(high=500000, low=100000, close=300000),  # Declining prices
    ]
    
    buy_indicator.update(intervals)
    sell_indicator.update(intervals)
    
    # Should handle extreme values without error
    buy_val = buy_indicator.get_value()
    sell_val = sell_indicator.get_value()
    
    print(f"FiveTwoBuy with extreme values: {buy_val}")
    print(f"FiveTwoSell with extreme values: {sell_val}")
    
    # FiveTwoBuy should calculate (low declined: 500000 -> 100000)
    # FiveTwoSell should be None (high declined: 1000000 -> 500000)
    assert buy_val is not None, "FiveTwoBuy should calculate for declining lows"
    assert sell_val is None, "FiveTwoSell should be None for declining highs"
    
    print("✅ Extreme values test passed")

def main():
    """Run all Five Two indicator tests"""
    print("Five Two Indicators Comprehensive Test Suite")
    print("=" * 50)
    
    try:
        test_five_two_buy()
        test_five_two_sell()
        test_five_two_opposite_conditions()
        test_edge_cases()
        
        print("\n" + "=" * 50)
        print("🎉 ALL FIVE TWO INDICATOR TESTS PASSED! 🎉")
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