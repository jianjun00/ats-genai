#!/usr/bin/env python3

import sys
sys.path.append('src')

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    from src.signals.indicator import FiveOneBuy, FiveOneSell
except ImportError as e:
    print(f"❌ Cannot import indicators: {e}")
    print("Make sure to run: PYTHONPATH=src python test_five_one_indicators_simple.py")
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

def test_five_one_buy():
    """Test FiveOneBuy indicator with conditional logic"""
    print("=== Testing FiveOneBuy Indicator ===")
    
    indicator = FiveOneBuy()
    
    # Test case 1: low(t-1) > low(t-2) - should calculate
    print("\nTest Case 1: low(t-1) > low(t-2) (should calculate)")
    
    # Create intervals: t-2 (low=100), t-1 (low=102 > 100)
    intervals = [
        TestInstrumentInterval(high=110, low=100, close=108),  # t-2
        TestInstrumentInterval(high=108, low=102, close=106),  # t-1: low=102 > 100
    ]
    
    indicator.update(intervals)
    expected = 2 * 102 - 100  # 2 * low(t-1) - low(t-2) = 204 - 100 = 104
    print(f"latest_five_one_buy = {indicator.latest_five_one_buy}")
    print(f"Expected: 2 * 102 - 100 = {expected}")
    
    assert indicator.latest_five_one_buy == expected, f"Expected {expected}, got {indicator.latest_five_one_buy}"
    assert indicator.get_value() == expected
    print("✅ Case 1 passed")
    
    # Test case 2: low(t-1) <= low(t-2) - should not calculate
    print("\nTest Case 2: low(t-1) <= low(t-2) (should not calculate)")
    
    # Create intervals where new low <= previous low
    intervals = [
        TestInstrumentInterval(high=108, low=102, close=106),  # t-1
        TestInstrumentInterval(high=107, low=101, close=105),  # t: low=101 <= 102
    ]
    
    indicator.update(intervals)
    print(f"latest_five_one_buy = {indicator.latest_five_one_buy}")
    print(f"get_value() returns: {indicator.get_value()}")
    
    assert indicator.latest_five_one_buy is None, f"Expected None, got {indicator.latest_five_one_buy}"
    assert indicator.get_value() is None
    print("✅ Case 2 passed")
    
    # Test case 3: Equal lows - should not calculate
    print("\nTest Case 3: low(t-1) == low(t-2) (should not calculate)")
    
    # Create intervals with equal lows
    intervals = [
        TestInstrumentInterval(high=107, low=101, close=105),  # t-1
        TestInstrumentInterval(high=106, low=101, close=104),  # t: low=101 == 101
    ]
    
    indicator.update(intervals)
    print(f"latest_five_one_buy = {indicator.latest_five_one_buy}")
    
    assert indicator.latest_five_one_buy is None, f"Expected None, got {indicator.latest_five_one_buy}"
    print("✅ Case 3 passed")

def test_five_one_sell():
    """Test FiveOneSell indicator with conditional logic"""
    print("\n=== Testing FiveOneSell Indicator ===")
    
    indicator = FiveOneSell()
    
    # Test case 1: high(t-1) < high(t-2) - should calculate
    print("\nTest Case 1: high(t-1) < high(t-2) (should calculate)")
    
    # Create intervals: t-2 (high=110), t-1 (high=108 < 110)
    intervals = [
        TestInstrumentInterval(high=110, low=100, close=108),  # t-2
        TestInstrumentInterval(high=108, low=102, close=106),  # t-1: high=108 < 110
    ]
    
    indicator.update(intervals)
    expected = 2 * 108 - 110  # 2 * high(t-1) - high(t-2) = 216 - 110 = 106
    print(f"latest_five_one_sell = {indicator.latest_five_one_sell}")
    print(f"Expected: 2 * 108 - 110 = {expected}")
    
    assert indicator.latest_five_one_sell == expected, f"Expected {expected}, got {indicator.latest_five_one_sell}"
    assert indicator.get_value() == expected
    print("✅ Case 1 passed")
    
    # Test case 2: high(t-1) >= high(t-2) - should not calculate
    print("\nTest Case 2: high(t-1) >= high(t-2) (should not calculate)")
    
    # Create intervals where new high >= previous high
    intervals = [
        TestInstrumentInterval(high=108, low=102, close=106),  # t-1
        TestInstrumentInterval(high=109, low=101, close=105),  # t: high=109 >= 108
    ]
    
    indicator.update(intervals)
    print(f"latest_five_one_sell = {indicator.latest_five_one_sell}")
    print(f"get_value() returns: {indicator.get_value()}")
    
    assert indicator.latest_five_one_sell is None, f"Expected None, got {indicator.latest_five_one_sell}"
    assert indicator.get_value() is None
    print("✅ Case 2 passed")
    
    # Test case 3: Equal highs - should not calculate
    print("\nTest Case 3: high(t-1) == high(t-2) (should not calculate)")
    
    # Create intervals with equal highs
    intervals = [
        TestInstrumentInterval(high=109, low=101, close=105),  # t-1
        TestInstrumentInterval(high=109, low=101, close=104),  # t: high=109 == 109
    ]
    
    indicator.update(intervals)
    print(f"latest_five_one_sell = {indicator.latest_five_one_sell}")
    
    assert indicator.latest_five_one_sell is None, f"Expected None, got {indicator.latest_five_one_sell}"
    print("✅ Case 3 passed")

def test_edge_cases():
    """Test edge cases and error handling"""
    print("\n=== Testing Edge Cases ===")
    
    # Test insufficient data
    print("\nTest: Insufficient data")
    buy_indicator = FiveOneBuy()
    sell_indicator = FiveOneSell()
    
    # Only one interval - should not calculate
    intervals = [TestInstrumentInterval(high=105, low=95, close=102)]
    
    buy_indicator.update(intervals)
    sell_indicator.update(intervals)
    
    print(f"FiveOneBuy with 1 interval: {buy_indicator.get_value()}")
    print(f"FiveOneSell with 1 interval: {sell_indicator.get_value()}")
    
    assert buy_indicator.get_value() is None
    assert sell_indicator.get_value() is None
    print("✅ Insufficient data test passed")
    
    # Test extreme values
    print("\nTest: Extreme values")
    intervals = [
        TestInstrumentInterval(high=0.01, low=0.001, close=0.005),
        TestInstrumentInterval(high=1000000, low=500000, close=750000),
    ]
    
    buy_indicator.update(intervals)
    sell_indicator.update(intervals)
    
    # Should handle extreme values without error
    buy_val = buy_indicator.get_value()
    sell_val = sell_indicator.get_value()
    
    print(f"FiveOneBuy with extreme values: {buy_val}")
    print(f"FiveOneSell with extreme values: {sell_val}")
    print("✅ Extreme values test passed")

def main():
    """Run all Five One indicator tests"""
    print("Five One Indicators Simple Test Suite")
    print("=" * 50)
    
    try:
        test_five_one_buy()
        test_five_one_sell()
        test_edge_cases()
        
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