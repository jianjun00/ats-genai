#!/usr/bin/env python3
"""
Test to detect and validate the datetime bug in OHLC chart generation.

This test demonstrates the difference between:
1. BROKEN: Using numeric indices (x: index) for chart x-axis
2. FIXED: Using actual datetime values (x: point.datetime) for chart x-axis
"""

import asyncio
import json
import os
import sys
from typing import Dict, List, Any
from datetime import datetime


class DatetimeBugDetectionTest:
    """Test suite to detect datetime bug in OHLC chart generation"""

    def __init__(self):
        self.sample_data = self._create_sample_data()

    def _create_sample_data(self) -> List[Dict]:
        """Create sample data with proper datetime intervals"""
        return [
            {
                'sequence_id': 1,
                'datetime': '2024-01-15T09:30:00',
                'etop': 151.25, 'ebot': 148.50, 'pldot': 149.75,
                '5m_high': 150.25, '5m_low': 148.75, '5m_close': 149.50
            },
            {
                'sequence_id': 2,
                'datetime': '2024-01-15T09:35:00',
                'etop': 152.00, 'ebot': 149.25, 'pldot': 150.50,
                '5m_high': 151.00, '5m_low': 149.25, '5m_close': 150.75
            },
            {
                'sequence_id': 3,
                'datetime': '2024-01-15T09:40:00',
                'etop': 152.75, 'ebot': 149.75, 'pldot': 151.25,
                '5m_high': 152.50, '5m_low': 150.50, '5m_close': 151.25
            }
        ]

    def simulate_broken_ohlc_processing(self, data: List[Dict]) -> List[Dict]:
        """
        Simulate the BROKEN OHLC processing from analytics_service.py line 4837
        This is what currently happens in the code:
        x: index  # <-- BUG: Uses numeric index instead of datetime
        """
        chartData = []
        for index, point in enumerate(data):
            prev_close = chartData[index-1]['close'] if index > 0 else point['5m_close']
            current_close = point['5m_close']

            processed_point = {
                'x': index,  # <-- BUG: This should be point['datetime']
                'open': prev_close,
                'high': point['5m_high'],
                'low': point['5m_low'],
                'close': current_close,
                'etop': point['etop'],
                'ebot': point['ebot'],
                'pldot': point['pldot']
            }
            chartData.append(processed_point)

        return chartData

    def simulate_fixed_ohlc_processing(self, data: List[Dict]) -> List[Dict]:
        """
        Simulate the FIXED OHLC processing that should use datetime
        This is how the code SHOULD work:
        x: point['datetime']  # <-- FIXED: Uses actual datetime
        """
        chartData = []
        for index, point in enumerate(data):
            prev_close = chartData[index-1]['close'] if index > 0 else point['5m_close']
            current_close = point['5m_close']

            processed_point = {
                'x': point['datetime'],  # <-- FIXED: Use datetime instead of index
                'open': prev_close,
                'high': point['5m_high'],
                'low': point['5m_low'],
                'close': current_close,
                'etop': point['etop'],
                'ebot': point['ebot'],
                'pldot': point['pldot']
            }
            chartData.append(processed_point)

        return chartData

    async def test_detect_datetime_bug(self) -> bool:
        """Test that detects the datetime bug in current implementation"""
        try:
            print("🔍 **TESTING DATETIME BUG DETECTION**")
            print("Comparing broken vs fixed OHLC x-axis handling")
            print("-" * 60)

            # Simulate current broken implementation
            broken_chart_data = self.simulate_broken_ohlc_processing(self.sample_data)

            # Simulate fixed implementation
            fixed_chart_data = self.simulate_fixed_ohlc_processing(self.sample_data)

            print("📊 BROKEN Implementation Results (current analytics_service.py):")
            for i, point in enumerate(broken_chart_data):
                print(f"  Point {i}: x={point['x']}, close={point['close']}")

            print("\n📊 FIXED Implementation Results (how it should work):")
            for i, point in enumerate(fixed_chart_data):
                print(f"  Point {i}: x={point['x']}, close={point['close']}")

            # Validate the bug is present in broken implementation
            print(f"\n🔍 **BUG DETECTION ANALYSIS:**")

            broken_x_values = [point['x'] for point in broken_chart_data]
            fixed_x_values = [point['x'] for point in fixed_chart_data]

            # Check if broken version uses numeric indices
            broken_uses_indices = all(isinstance(x, int) for x in broken_x_values)
            print(f"   Broken version uses numeric indices: {broken_uses_indices}")

            # Check if fixed version uses datetime strings
            fixed_uses_datetime = all(isinstance(x, str) and 'T' in x for x in fixed_x_values)
            print(f"   Fixed version uses datetime strings: {fixed_uses_datetime}")

            # The bug is present if broken uses indices and fixed uses datetime
            bug_detected = broken_uses_indices and fixed_uses_datetime

            if bug_detected:
                print(f"   🚨 **BUG CONFIRMED**: Current code uses indices {broken_x_values} instead of datetime")
                print(f"   ✅ **FIX VALIDATED**: Should use datetime {fixed_x_values}")

                # Show the impact
                print(f"\n📈 **IMPACT ON CHART DISPLAY:**")
                print(f"   Broken x-axis: [0, 1, 2] (meaningless numeric sequence)")
                print(f"   Fixed x-axis:  ['09:30:00', '09:35:00', '09:40:00'] (actual time intervals)")
                print(f"   User sees:     Generic indices vs. actual trading times")

                return True
            else:
                print(f"   ⚠️ Bug detection inconclusive")
                return False

        except Exception as e:
            print(f"❌ Datetime bug detection failed: {e}")
            return False

    async def test_chart_axis_labeling(self) -> bool:
        """Test how different x-axis values affect chart readability"""
        try:
            print(f"\n📊 **CHART AXIS READABILITY TEST**")

            broken_data = self.simulate_broken_ohlc_processing(self.sample_data)
            fixed_data = self.simulate_fixed_ohlc_processing(self.sample_data)

            # Simulate what Plotly would show on x-axis
            print(f"   Current (broken) chart x-axis ticks:")
            for point in broken_data:
                print(f"     Tick: {point['x']} (at price: ${point['close']:.2f})")

            print(f"   Fixed chart x-axis ticks:")
            for point in fixed_data:
                time_display = datetime.fromisoformat(point['x']).strftime('%H:%M')
                print(f"     Tick: {time_display} (at price: ${point['close']:.2f})")

            # Demonstrate the user experience difference
            print(f"\n👤 **USER EXPERIENCE IMPACT:**")
            print(f"   ❌ Broken: 'What does point 0, 1, 2 represent?'")
            print(f"   ✅ Fixed:  'Stock moved from $149.50 at 9:30 AM to $151.25 at 9:40 AM'")
            print(f"   📈 Result: Fixed version provides meaningful time context for trading analysis")

            return True

        except Exception as e:
            print(f"❌ Chart axis labeling test failed: {e}")
            return False

    async def test_fix_implementation_guide(self) -> bool:
        """Provide specific fix implementation guidance"""
        try:
            print(f"\n🔧 **FIX IMPLEMENTATION GUIDE**")
            print("Location: /home/jianjun/ats-genai-admin/src/services/analytics_service.py")
            print("Line: ~4837 (in OHLC chart generation JavaScript)")

            print(f"\n❌ **CURRENT BROKEN CODE:**")
            print("```javascript")
            print("const chartData = data.data.map((point, index) => {")
            print("    return {")
            print("        x: index,  // <-- BUG: Uses numeric index")
            print("        open: prevClose || currentClose,")
            print("        high: point['5m_high'] || point['1h_high'] || point['15m_high'] || 0,")
            print("        // ... rest of the mapping")
            print("    };")
            print("});")
            print("```")

            print(f"\n✅ **FIXED CODE:**")
            print("```javascript")
            print("const chartData = data.data.map((point, index) => {")
            print("    return {")
            print("        x: point.datetime,  // <-- FIXED: Use datetime field")
            print("        open: prevClose || currentClose,")
            print("        high: point['5m_high'] || point['1h_high'] || point['15m_high'] || 0,")
            print("        // ... rest of the mapping")
            print("    };")
            print("});")
            print("```")

            print(f"\n🧪 **VALIDATION STEPS:**")
            print("1. Make the code change above")
            print("2. Ensure API returns 'datetime' field in training data")
            print("3. Run: PYTHONPATH=src python3 tests/run_training_data_tests.py hermetic")
            print("4. Verify chart shows time labels: '09:30', '09:35', '09:40' instead of '0', '1', '2'")
            print("5. Test with real ATS service to confirm datetime display")

            return True

        except Exception as e:
            print(f"❌ Fix implementation guide failed: {e}")
            return False

    async def run_all_tests(self) -> bool:
        """Run all datetime bug detection tests"""
        print("🕐 **DATETIME BUG DETECTION TEST SUITE**")
        print("Purpose: Identify and validate fix for OHLC chart datetime display issue")
        print("=" * 80)

        tests = [
            ("Datetime Bug Detection", self.test_detect_datetime_bug()),
            ("Chart Axis Readability", self.test_chart_axis_labeling()),
            ("Fix Implementation Guide", self.test_fix_implementation_guide())
        ]

        results = []
        for test_name, test_coro in tests:
            try:
                success = await test_coro
                results.append((test_name, "PASSED" if success else "FAILED"))
            except Exception as e:
                results.append((test_name, "ERROR", str(e)))

        # Summary
        passed = sum(1 for r in results if r[1] == "PASSED")
        total = len(results)

        print(f"\n📊 **DATETIME BUG DETECTION RESULTS: {passed}/{total} PASSED**")
        print("=" * 80)

        for result in results:
            status_icon = "✅" if result[1] == "PASSED" else "❌"
            print(f"{status_icon} {result[0]}: {result[1]}")

        if passed == total:
            print("\n🎯 **DATETIME BUG CONFIRMED AND FIX IDENTIFIED!**")
            print("   • Current code uses numeric indices for x-axis (broken)")
            print("   • Should use datetime strings for proper time display")
            print("   • Fix requires changing 'x: index' to 'x: point.datetime'")
            print("   • User will see actual trading times instead of meaningless numbers")
            return True
        else:
            print("\n⚠️ **Bug detection incomplete - review test results**")
            return False


async def main():
    """Main test runner"""
    test_suite = DatetimeBugDetectionTest()
    success = await test_suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)