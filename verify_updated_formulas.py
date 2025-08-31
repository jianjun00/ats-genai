"""
Verification script to test the updated exact linear formulas for technical indicators.
Compares the new indicator calculations against the original data to verify accuracy.
"""

import sys
import os
sys.path.append('src')

from signals.indicator import PL, L11, Z1B, Z2B, EBot, ETop, Z5T, Z6T
from state.instrument_interval import InstrumentInterval
from datetime import datetime, date
import math

# Test data from original analysis (first 5 samples)
test_data = [
    # (date, [o, h, l, c], expected_values_dict)
    ('08/04', 
     [(23452, 23689, 23356.5, 23480.5), (23705, 23845, 23290.5, 23365), (23308.5, 23347.5, 22775, 22883.75)],
     {'h11': 23229.17, 'l11': 22656.67, 'z1b': 22560.28, 'z2b': 22756.58, 'ebot': 23046.78, 'pldot': 23336.97, 'etop': 23533.28, 'z5t': 23729.58, 'z6t': 24019.78}),
    
    ('08/05',
     [(23705, 23845, 23290.5, 23365), (23308.5, 23347.5, 22775, 22883.75), (22850, 23352.5, 22821.75, 23296.5)],
     {'h11': 23492.08, 'l11': 22961.33, 'z1b': 22371.86, 'z2b': 22629.17, 'ebot': 22924.44, 'pldot': 23219.72, 'etop': 23477.03, 'z5t': 23734.33, 'z6t': 24029.61}),
     
    ('08/07',  # Skip anomalous 08/06
     [(23308.5, 23347.5, 22775, 22883.75), (22850, 23352.5, 22821.75, 23296.5), (23418.5, 23671, 23329, 23496.25)],
     {'h11': 23563.5, 'l11': 23163.75, 'z1b': 22628.14, 'z2b': 22867, 'ebot': 23044.89, 'pldot': 23222.78, 'etop': 23461.61, 'z5t': 23700.5, 'z6t': 23878.39}),
]

def create_interval(ohlc_data, interval_date):
    """Create InstrumentInterval from OHLC data."""
    open_price, high_price, low_price, close_price = ohlc_data
    
    interval = InstrumentInterval(
        instrument_id=1,
        start_date_time=datetime.combine(interval_date, datetime.min.time()),
        end_date_time=datetime.combine(interval_date, datetime.max.time()),
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        traded_volume=1000,  # Dummy volume
        traded_dollar=close_price * 1000,  # Dummy dollar volume
        status='ok'
    )
    
    return interval

def test_indicator_accuracy():
    """Test all updated indicators against expected values."""
    
    print("VERIFICATION OF UPDATED EXACT LINEAR FORMULAS")
    print("=" * 60)
    
    # Initialize indicators
    indicators = {
        'pldot': PL(),
        'l11': L11(), 
        'z1b': Z1B(),
        'z2b': Z2B(),
        'ebot': EBot(),
        'etop': ETop(),
        'z5t': Z5T(),
        'z6t': Z6T()
    }
    
    total_tests = 0
    passed_tests = 0
    
    for test_date, ohlc_history, expected_values in test_data:
        print(f"\nTesting {test_date}:")
        print("-" * 30)
        
        # Create intervals from OHLC history
        intervals = []
        base_date = date(2024, 8, 1)  # Base date for intervals
        
        for i, ohlc in enumerate(ohlc_history):
            interval_date = date(base_date.year, base_date.month, base_date.day + i)
            interval = create_interval(ohlc, interval_date)
            intervals.append(interval)
        
        # Test each indicator
        for indicator_name, indicator in indicators.items():
            if indicator_name in expected_values:
                # Update indicator with intervals
                indicator.update(intervals)
                
                # Get calculated value
                calculated_value = indicator.get_value()
                expected_value = expected_values[indicator_name]
                
                if calculated_value is not None:
                    error = abs(calculated_value - expected_value)
                    error_pct = (error / expected_value) * 100 if expected_value != 0 else 0
                    
                    # Test passes if error < 0.01 (very small)
                    test_passed = error < 0.01
                    status = "✅ PASS" if test_passed else "❌ FAIL"
                    
                    print(f"  {indicator_name.upper():6}: Expected={expected_value:10.2f}, Calculated={calculated_value:10.2f}, Error={error:8.4f} ({error_pct:6.4f}%) {status}")
                    
                    total_tests += 1
                    if test_passed:
                        passed_tests += 1
                        
                else:
                    print(f"  {indicator_name.upper():6}: Expected={expected_value:10.2f}, Calculated=None, Status={indicator.status} ❌ FAIL")
                    total_tests += 1
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    if passed_tests == total_tests:
        print("\n🎉 ALL TESTS PASSED! Exact formulas are working correctly.")
    else:
        print(f"\n⚠️  {total_tests - passed_tests} tests failed. Check formula implementation.")
    
    return passed_tests == total_tests

if __name__ == "__main__":
    success = test_indicator_accuracy()
    sys.exit(0 if success else 1)