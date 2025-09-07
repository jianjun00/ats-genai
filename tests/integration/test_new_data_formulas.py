"""
Test the exact linear formulas against new data provided by user.
This will verify if our formulas work on different price ranges and time periods.
"""

import sys
import os
sys.path.append('src')

from domains.trading.services.indicator import PL, L11, Z1B, Z2B, EBot, ETop, Z5T, Z6T
from state.instrument_interval import InstrumentInterval
from datetime import datetime, date
import math

# New test data provided by user (corrected 08/28 values)
test_data_raw = """
08/14    3407.2    3423.8    3375.5    3383.2    3423    3393.1    3338.8    3359    3383.8    3408.5    3428.7    3449    3473.7
08/15    3383.2    3394.8    3377.7    3382.6    3412.8    3364.5    3343.2    3360.2    3379.8    3399.4    3416.5    3433.5    3453.1
08/18    3382.4    3403.6    3368    3378    3392.4    3375.3    3345.9    3359.6    3377.6    3395.7    3409.4    3423.1    3441.2
08/19    3378.3    3389.7    3358.1    3358.7    3398.4    3362.8    3333.9    3347.6    3367.5    3387.5    3401.2    3414.9    3434.9
08/20    3359    3394.3    3353.4    3388.5    3379.6    3348    3333.9    3345    3362    3379    3390.1    3401.2    3418.2
08/21    3392.2    3394.4    3367.4    3383.5    3404.1    3363.2    3321.9    3339    3358    3376.9    3394    3411.1    3430
08/22    3383.3    3423.4    3362.8    3418.5    3396.1    3369.1    3326.9    3343.7    3360.1    3376.4    3393.3    3410.1    3426.4
08/25    3417.6    3421.5    3405.5    3417.5    3440.3    3379.7    3327.8    3354    3370.7    3387.4    3413.5    3439.7    3456.3
08/26    3410.8    3443.3    3396.1    3433    3424.2    3408.2    3351.1    3372    3385.7    3399.4    3420.2    3441    3454.7
08/27    3443    3452.5    3422.2    3448.6    3452.2    3405    3356.4    3381.7    3397.6    3413.5    3438.9    3464.3    3480.2
08/28    3452.6    3478.7    3442.5    3474.3    3460    3429.7    3383.1    3401.9    3414.3    3426.7    3445.4    3464.2    3476.6
08/29    3477.2    3479.6    3463    3467.6    3487.8    3451.6    3390.9    3414.1    3428.8    3443.5    3466.7    3489.9    3504.6
"""

def parse_test_data():
    """Parse the provided test data into structured format."""
    lines = [line.strip() for line in test_data_raw.strip().split('\n') if line.strip()]

    parsed_data = []
    for line in lines:
        parts = line.split()
        date_str = parts[0]
        values = [float(x) for x in parts[1:]]

        # Values: open, high, low, close, h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t
        ohlc = values[:4]
        expected = {
            'h11': values[4], 'l11': values[5], 'z1b': values[6], 'z2b': values[7],
            'ebot': values[8], 'pldot': values[9], 'etop': values[10], 'z5t': values[11], 'z6t': values[12]
        }

        parsed_data.append((date_str, ohlc, expected))

    return parsed_data

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
        traded_volume=1000,
        traded_dollar=close_price * 1000,
        status='ok'
    )

    return interval

def test_formulas_on_new_data():
    """Test formulas against the new dataset."""

    print("TESTING EXACT LINEAR FORMULAS ON NEW DATA")
    print("=" * 60)
    print("Data range: 08/14 - 08/29 (different price levels)")
    print("Price range: ~3300-3520 (vs original ~22000-24000)")
    print("=" * 60)

    # Parse the test data
    parsed_data = parse_test_data()
    print(f"Parsed {len(parsed_data)} data points")

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

    # Test each date starting from the 4th day (need 3 prior days)
    results = []
    total_tests = 0
    passed_tests = 0
    error_threshold = 1.0  # Allow larger errors for different price scale

    for i in range(3, len(parsed_data)):  # Start from 4th day
        test_date = parsed_data[i][0]
        current_expected = parsed_data[i][2]

        print(f"\nTesting {test_date}:")
        print("-" * 40)

        # Create intervals for the past 3 days + current day
        intervals = []
        base_date = date(2024, 8, 14)

        for j in range(i-2, i+1):  # Previous 3 days including current
            day_offset = j - 3  # Adjust for base date
            interval_date = date(base_date.year, base_date.month, base_date.day + day_offset)
            ohlc = parsed_data[j][1]
            interval = create_interval(ohlc, interval_date)
            intervals.append(interval)

        print(f"Using OHLC data from days {i-2} to {i} for prediction")

        # Test each indicator
        day_results = {'date': test_date, 'tests': {}}

        for indicator_name, indicator in indicators.items():
            if indicator_name in current_expected:
                # Update indicator with past 3 days
                indicator.update(intervals)

                calculated_value = indicator.get_value()
                expected_value = current_expected[indicator_name]

                if calculated_value is not None:
                    error = abs(calculated_value - expected_value)
                    error_pct = (error / expected_value) * 100 if expected_value != 0 else 0

                    test_passed = error < error_threshold
                    status = "✅ PASS" if test_passed else "❌ FAIL"

                    print(f"  {indicator_name.upper():6}: Expected={expected_value:8.1f}, Calculated={calculated_value:8.1f}, Error={error:6.2f} ({error_pct:5.2f}%) {status}")

                    day_results['tests'][indicator_name] = {
                        'expected': expected_value,
                        'calculated': calculated_value,
                        'error': error,
                        'error_pct': error_pct,
                        'passed': test_passed
                    }

                    total_tests += 1
                    if test_passed:
                        passed_tests += 1

                else:
                    print(f"  {indicator_name.upper():6}: Expected={expected_value:8.1f}, Calculated=None, Status={indicator.status} ❌ FAIL")
                    total_tests += 1
                    day_results['tests'][indicator_name] = {
                        'expected': expected_value,
                        'calculated': None,
                        'error': float('inf'),
                        'passed': False
                    }

        results.append(day_results)

    # Summary statistics
    print("\n" + "=" * 60)
    print("DETAILED ANALYSIS")
    print("=" * 60)

    # Calculate average errors per indicator
    indicator_stats = {}
    for indicator_name in indicators.keys():
        errors = []
        passed_count = 0
        total_count = 0

        for day_result in results:
            if indicator_name in day_result['tests']:
                test_result = day_result['tests'][indicator_name]
                if test_result['calculated'] is not None:
                    errors.append(test_result['error'])
                    if test_result['passed']:
                        passed_count += 1
                total_count += 1

        if errors:
            avg_error = sum(errors) / len(errors)
            max_error = max(errors)
            min_error = min(errors)

            indicator_stats[indicator_name] = {
                'avg_error': avg_error,
                'max_error': max_error,
                'min_error': min_error,
                'pass_rate': (passed_count / total_count) * 100 if total_count > 0 else 0,
                'total_tests': total_count
            }

    print(f"{'Indicator':<8} {'Avg Error':>10} {'Max Error':>10} {'Min Error':>10} {'Pass Rate':>10} {'Tests':>6}")
    print("-" * 60)
    for indicator_name, stats in indicator_stats.items():
        print(f"{indicator_name.upper():<8} {stats['avg_error']:>10.2f} {stats['max_error']:>10.2f} {stats['min_error']:>10.2f} {stats['pass_rate']:>9.1f}% {stats['total_tests']:>6}")

    print("\n" + "=" * 60)
    print("OVERALL SUMMARY")
    print("=" * 60)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")

    # Determine formula accuracy
    overall_success = passed_tests / total_tests if total_tests > 0 else 0

    if overall_success > 0.95:
        print(f"\n🎉 EXCELLENT! Formulas work perfectly on new data ({overall_success*100:.1f}% success)")
    elif overall_success > 0.8:
        print(f"\n✅ GOOD! Formulas work well on new data ({overall_success*100:.1f}% success)")
    elif overall_success > 0.6:
        print(f"\n⚠️  MODERATE: Formulas partially work on new data ({overall_success*100:.1f}% success)")
    else:
        print(f"\n❌ POOR: Formulas may not generalize well ({overall_success*100:.1f}% success)")

    return overall_success > 0.8

if __name__ == "__main__":
    success = test_formulas_on_new_data()
    sys.exit(0 if success else 1)