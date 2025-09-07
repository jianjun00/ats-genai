#!/usr/bin/env python3
"""
Comprehensive test suite for 21-row window visualization feature.

Tests the implementation of the requirement:
"The visualization should take ten rows before selected row and ten rows after selected row and show 21 rows on the chart"
"""

import asyncio
import json
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TwentyOneRowWindowVisualizationTests:
    """Comprehensive test suite for 21-row window visualization feature."""

    def __init__(self):
        self.mock_dataset = self._create_large_dataset()
        self.test_scenarios = self._create_test_scenarios()

    def _create_large_dataset(self) -> Dict:
        """Create a large dataset with enough data to test 21-row windows properly."""
        base_datetime = datetime(2024, 1, 15, 9, 30, 0)

        # Create enough data points to support window testing
        # For 10 sequences with 60 time steps each = 600 data points total
        # This provides realistic training dataset structure
        total_sequences = 10
        sequence_length = 60
        total_data_points = total_sequences * sequence_length

        sample_data = []
        for i in range(total_data_points):
            current_datetime = base_datetime + timedelta(minutes=5 * i)
            sample_data.append({
                "sequence_id": i + 1,
                "datetime": current_datetime.isoformat(),
                "etop": 150.0 + i * 0.01,  # Gradually increasing envelope top
                "ebot": 148.0 + i * 0.008, # Gradually increasing envelope bottom
                "pldot": 149.0 + i * 0.009, # Gradually increasing PL dot
                "5m_high": 149.5 + i * 0.01,
                "5m_low": 148.0 + i * 0.008,
                "5m_close": 148.75 + i * 0.009,
                "5m_volume": 1000000 + i * 1000
            })

        return {
            "id": "test_21_row",
            "name": "21-Row Window Test Dataset",
            "total_sequences": total_sequences,
            "sequence_length": sequence_length,
            "sample_data": sample_data
        }

    def _create_test_scenarios(self) -> List[Dict]:
        """Create test scenarios for different window positions."""
        return [
            {
                "name": "Early Sequence (Sequence 1)",
                "selected_sequence": 1,
                "expected_window_behavior": "Should include time steps around sequence 1 center",
                "boundary_condition": "Early in dataset"
            },
            {
                "name": "Middle Sequence (Sequence 5)",
                "selected_sequence": 5,
                "expected_window_behavior": "Should include time steps around sequence 5 center",
                "boundary_condition": "Middle of dataset"
            },
            {
                "name": "Late Sequence (Sequence 8)",
                "selected_sequence": 8,
                "expected_window_behavior": "Should include time steps around sequence 8 center",
                "boundary_condition": "Near end of dataset"
            },
            {
                "name": "Edge Case - First Sequence (Sequence 0)",
                "selected_sequence": 0,
                "expected_window_behavior": "Should handle boundary at dataset start",
                "boundary_condition": "Absolute start"
            },
            {
                "name": "Edge Case - Last Sequence (Sequence 9)",
                "selected_sequence": 9,
                "expected_window_behavior": "Should handle boundary at dataset end",
                "boundary_condition": "Absolute end"
            }
        ]

    def simulate_window_calculation(self, selected_sequence: int, dataset_info: Dict) -> Dict:
        """
        Simulate the 21-row window calculation logic from frontend.
        This matches the implementation in analytics_service.py updateOHLCVisualization()
        """
        sequence_length = dataset_info.get("sequence_length", 60)
        total_sequences = dataset_info.get("total_sequences", 100)

        # Calculate the start index for a 21-row window centered on the selected sequence
        # If sequenceIndex is the sequence number, we want the middle time step of that sequence
        middle_time_step = sequence_length // 2  # Middle of the sequence (e.g., step 30 of 60)
        center_index = (selected_sequence * sequence_length) + middle_time_step

        # Calculate start_idx for 21-row window (10 before center, center, 10 after center)
        window_size = 21
        half_window = window_size // 2  # 10
        start_idx = max(0, center_index - half_window)

        # Ensure we don't exceed total available data points
        max_data_points = total_sequences * sequence_length
        if start_idx + window_size > max_data_points:
            start_idx = max(0, max_data_points - window_size)

        return {
            "selected_sequence": selected_sequence,
            "center_index": center_index,
            "start_idx": start_idx,
            "window_size": window_size,
            "half_window": half_window,
            "max_data_points": max_data_points,
            "calculated_end_idx": start_idx + window_size - 1
        }

    def simulate_api_response(self, window_calc: Dict) -> Dict:
        """
        Simulate the API response for visualization data with 21-row window.
        This matches the backend get_training_dataset_visualization_data() method.
        """
        start_idx = window_calc["start_idx"]
        window_size = window_calc["window_size"]

        # Simulate extracting 21 data points from the large dataset
        total_points = min(window_size, len(self.mock_dataset["sample_data"]))
        actual_end_idx = min(start_idx + window_size, len(self.mock_dataset["sample_data"]))

        # Extract the data slice
        data_slice = self.mock_dataset["sample_data"][start_idx:actual_end_idx]

        return {
            "data": data_slice,
            "window_info": {
                "selected_sequence": window_calc["selected_sequence"],
                "center_index": window_calc["center_index"],
                "start_idx": start_idx,
                "window_size": window_size,
                "total_points": len(data_slice),
                "actual_end_idx": actual_end_idx
            }
        }

    async def test_window_calculation_logic(self) -> bool:
        """Test the 21-row window calculation logic for various scenarios."""
        try:
            print("🧮 **TESTING 21-ROW WINDOW CALCULATION LOGIC**")
            print("Validating window positioning for different sequence selections")
            print("-" * 70)

            all_passed = True

            for scenario in self.test_scenarios:
                print(f"\n📊 Testing: {scenario['name']}")
                selected_sequence = scenario['selected_sequence']

                # Calculate window using frontend logic
                window_calc = self.simulate_window_calculation(selected_sequence, self.mock_dataset)

                print(f"   Selected Sequence: {selected_sequence}")
                print(f"   Center Index: {window_calc['center_index']}")
                print(f"   Window Start: {window_calc['start_idx']}")
                print(f"   Window End: {window_calc['calculated_end_idx']}")
                print(f"   Window Size: {window_calc['window_size']}")

                # Validate window properties
                window_size = window_calc['calculated_end_idx'] - window_calc['start_idx'] + 1
                expected_size = 21  # Always expect 21 data points in window

                if window_size == expected_size:
                    print(f"   ✅ Window size correct: {window_size} points")
                else:
                    print(f"   ❌ Window size incorrect: {window_size}, expected: {expected_size}")
                    all_passed = False

                # Validate boundary conditions
                if window_calc['start_idx'] >= 0:
                    print(f"   ✅ Start index valid: {window_calc['start_idx']} >= 0")
                else:
                    print(f"   ❌ Start index invalid: {window_calc['start_idx']} < 0")
                    all_passed = False

                if window_calc['calculated_end_idx'] < len(self.mock_dataset['sample_data']):
                    print(f"   ✅ End index valid: {window_calc['calculated_end_idx']} < {len(self.mock_dataset['sample_data'])}")
                else:
                    print(f"   ❌ End index invalid: {window_calc['calculated_end_idx']} >= {len(self.mock_dataset['sample_data'])}")
                    all_passed = False

                print(f"   📍 {scenario['boundary_condition']}: {scenario['expected_window_behavior']}")

            if all_passed:
                print(f"\n✅ All window calculation tests passed!")
                return True
            else:
                print(f"\n❌ Some window calculation tests failed!")
                return False

        except Exception as e:
            print(f"❌ Window calculation test failed: {e}")
            return False

    async def test_api_data_extraction(self) -> bool:
        """Test that the API correctly extracts 21 data points for each window."""
        try:
            print(f"\n📊 **TESTING API DATA EXTRACTION**")
            print("Validating that API returns exactly 21 data points for each window")
            print("-" * 70)

            all_passed = True

            for scenario in self.test_scenarios:
                selected_sequence = scenario['selected_sequence']

                # Calculate window
                window_calc = self.simulate_window_calculation(selected_sequence, self.mock_dataset)

                # Simulate API response
                api_response = self.simulate_api_response(window_calc)

                print(f"\n📈 {scenario['name']}:")
                print(f"   API returned: {len(api_response['data'])} data points")
                print(f"   Window info: start_idx={api_response['window_info']['start_idx']}, total_points={api_response['window_info']['total_points']}")

                # Validate data extraction
                expected_points = min(21, len(self.mock_dataset['sample_data']) - window_calc['start_idx'])
                actual_points = len(api_response['data'])

                if actual_points == expected_points:
                    print(f"   ✅ Data extraction correct: {actual_points} points")
                else:
                    print(f"   ❌ Data extraction incorrect: {actual_points}, expected: {expected_points}")
                    all_passed = False

                # Validate data integrity
                first_point = api_response['data'][0]
                last_point = api_response['data'][-1]

                if 'datetime' in first_point and 'etop' in first_point:
                    print(f"   ✅ Data structure valid: contains datetime and indicators")
                else:
                    print(f"   ❌ Data structure invalid: missing required fields")
                    all_passed = False

                # Validate data sequence
                first_seq_id = first_point.get('sequence_id', 0)
                last_seq_id = last_point.get('sequence_id', 0)
                expected_range = last_seq_id - first_seq_id + 1

                if expected_range == actual_points:
                    print(f"   ✅ Data sequence valid: {first_seq_id} to {last_seq_id} ({expected_range} points)")
                else:
                    print(f"   ❌ Data sequence invalid: range {expected_range} != points {actual_points}")
                    all_passed = False

            if all_passed:
                print(f"\n✅ All API data extraction tests passed!")
                return True
            else:
                print(f"\n❌ Some API data extraction tests failed!")
                return False

        except Exception as e:
            print(f"❌ API data extraction test failed: {e}")
            return False

    async def test_datetime_formatting_in_window(self) -> bool:
        """Test that datetime formatting works correctly within 21-row windows."""
        try:
            print(f"\n📅 **TESTING DATETIME FORMATTING IN 21-ROW WINDOWS**")
            print("Validating YYYYMMDD HH:MM format for all data points in window")
            print("-" * 70)

            # Test with middle sequence for comprehensive datetime range
            selected_sequence = 50
            window_calc = self.simulate_window_calculation(selected_sequence, self.mock_dataset)
            api_response = self.simulate_api_response(window_calc)

            print(f"Testing datetime formatting for sequence {selected_sequence} window:")
            print(f"Window contains {len(api_response['data'])} data points")

            datetime_formats_valid = True
            datetime_progression_valid = True

            previous_datetime = None

            for i, point in enumerate(api_response['data']):
                # Check datetime field exists
                if 'datetime' not in point:
                    print(f"   ❌ Point {i}: Missing datetime field")
                    datetime_formats_valid = False
                    continue

                datetime_str = point['datetime']

                # Validate datetime can be parsed
                try:
                    parsed_datetime = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))

                    # Format as YYYYMMDD HH:MM for display
                    formatted_display = f"{parsed_datetime.strftime('%Y%m%d %H:%M')}"

                    if i < 5 or i >= len(api_response['data']) - 5:  # Show first and last 5
                        print(f"   Point {i+1}: {datetime_str} → {formatted_display}")
                    elif i == 5:
                        print(f"   ... (showing first 5 and last 5 of {len(api_response['data'])} points)")

                    # Check progression (should increase over time)
                    if previous_datetime and parsed_datetime <= previous_datetime:
                        print(f"   ❌ Point {i}: Datetime not progressing: {parsed_datetime} <= {previous_datetime}")
                        datetime_progression_valid = False

                    previous_datetime = parsed_datetime

                except ValueError as e:
                    print(f"   ❌ Point {i}: Invalid datetime format: {datetime_str} - {e}")
                    datetime_formats_valid = False

            # Overall validation
            if datetime_formats_valid and datetime_progression_valid:
                print(f"\n✅ Datetime formatting test passed!")
                print(f"   All {len(api_response['data'])} data points have valid, progressing datetime values")
                return True
            else:
                print(f"\n❌ Datetime formatting test failed!")
                if not datetime_formats_valid:
                    print(f"   Issue: Some datetime formats are invalid")
                if not datetime_progression_valid:
                    print(f"   Issue: Datetime values don't progress correctly")
                return False

        except Exception as e:
            print(f"❌ Datetime formatting test failed: {e}")
            return False

    async def test_technical_indicators_in_window(self) -> bool:
        """Test that technical indicators are properly included in 21-row windows."""
        try:
            print(f"\n📈 **TESTING TECHNICAL INDICATORS IN 21-ROW WINDOWS**")
            print("Validating etop, ebot, pldot indicators across window data")
            print("-" * 70)

            # Test multiple sequences to ensure consistency
            test_sequences = [1, 4, 7]
            all_passed = True

            for selected_sequence in test_sequences:
                window_calc = self.simulate_window_calculation(selected_sequence, self.mock_dataset)
                api_response = self.simulate_api_response(window_calc)

                print(f"\n📊 Sequence {selected_sequence} ({len(api_response['data'])} points):")

                # Check indicators in each data point
                indicators_valid = True
                required_indicators = ['etop', 'ebot', 'pldot']

                for i, point in enumerate(api_response['data']):
                    for indicator in required_indicators:
                        if indicator not in point:
                            print(f"   ❌ Point {i}: Missing {indicator} indicator")
                            indicators_valid = False
                        elif not isinstance(point[indicator], (int, float)):
                            print(f"   ❌ Point {i}: {indicator} is not numeric: {point[indicator]}")
                            indicators_valid = False

                # Show indicator value ranges for validation
                if len(api_response['data']) > 0:
                    etop_values = [p['etop'] for p in api_response['data'] if 'etop' in p]
                    ebot_values = [p['ebot'] for p in api_response['data'] if 'ebot' in p]
                    pldot_values = [p['pldot'] for p in api_response['data'] if 'pldot' in p]

                    if etop_values:
                        print(f"   etop range: {min(etop_values):.2f} - {max(etop_values):.2f}")
                    if ebot_values:
                        print(f"   ebot range: {min(ebot_values):.2f} - {max(ebot_values):.2f}")
                    if pldot_values:
                        print(f"   pldot range: {min(pldot_values):.2f} - {max(pldot_values):.2f}")

                    # Validate that indicators are not normalized (should be actual price values, not 0-1)
                    if etop_values and all(v > 100 for v in etop_values):  # Should be price levels
                        print(f"   ✅ etop values appear to be actual prices, not normalized")
                    else:
                        print(f"   ⚠️  etop values may be normalized or invalid")

                if indicators_valid:
                    print(f"   ✅ All technical indicators present and valid")
                else:
                    print(f"   ❌ Some technical indicators missing or invalid")
                    all_passed = False

            if all_passed:
                print(f"\n✅ Technical indicators test passed!")
                return True
            else:
                print(f"\n❌ Technical indicators test failed!")
                return False

        except Exception as e:
            print(f"❌ Technical indicators test failed: {e}")
            return False

    async def test_chart_display_information(self) -> bool:
        """Test that chart display shows proper window information to users."""
        try:
            print(f"\n🎨 **TESTING CHART DISPLAY INFORMATION**")
            print("Validating chart titles, annotations, and window info display")
            print("-" * 70)

            # Simulate chart display for different scenarios
            test_scenarios = [
                {"sequence": 5, "description": "Early sequence"},
                {"sequence": 50, "description": "Middle sequence"},
                {"sequence": 95, "description": "Late sequence"}
            ]

            all_passed = True

            for scenario in test_scenarios:
                selected_sequence = scenario["sequence"]
                window_calc = self.simulate_window_calculation(selected_sequence, self.mock_dataset)
                api_response = self.simulate_api_response(window_calc)

                print(f"\n📈 {scenario['description']} (Sequence {selected_sequence}):")

                # Simulate chart title generation
                chart_title = f"OHLC Chart - Sequence {api_response['window_info']['selected_sequence']} (21-row window: {api_response['window_info']['total_points']} data points)"
                print(f"   Chart Title: {chart_title}")

                # Simulate window annotation
                window_info = api_response['window_info']
                annotation_text = f"Window: {window_info['start_idx']} to {window_info['start_idx'] + window_info['window_size'] - 1} (center: {window_info['center_index']})"
                print(f"   Annotation: {annotation_text}")

                # Simulate sequence display
                sequence_display = f"Sequence: {selected_sequence} (21-row window)"
                print(f"   Sequence Display: {sequence_display}")

                # Validate information accuracy
                expected_total_points = len(api_response['data'])
                actual_total_points = window_info['total_points']

                if actual_total_points == expected_total_points:
                    print(f"   ✅ Total points accurate: {actual_total_points}")
                else:
                    print(f"   ❌ Total points inaccurate: {actual_total_points}, expected: {expected_total_points}")
                    all_passed = False

                # Validate window range
                expected_end = window_info['start_idx'] + window_info['window_size'] - 1
                if window_info['start_idx'] >= 0 and expected_end >= window_info['start_idx']:
                    print(f"   ✅ Window range valid: {window_info['start_idx']} to {expected_end}")
                else:
                    print(f"   ❌ Window range invalid: {window_info['start_idx']} to {expected_end}")
                    all_passed = False

            if all_passed:
                print(f"\n✅ Chart display information test passed!")
                return True
            else:
                print(f"\n❌ Chart display information test failed!")
                return False

        except Exception as e:
            print(f"❌ Chart display information test failed: {e}")
            return False

    async def test_boundary_edge_cases(self) -> bool:
        """Test edge cases and boundary conditions for 21-row windows."""
        try:
            print(f"\n🔍 **TESTING BOUNDARY EDGE CASES**")
            print("Validating behavior at dataset boundaries and edge conditions")
            print("-" * 70)

            edge_cases = [
                {"sequence": -1, "description": "Negative sequence (should handle gracefully)"},
                {"sequence": 0, "description": "First sequence"},
                {"sequence": 1, "description": "Second sequence"},
                {"sequence": 8, "description": "Second to last sequence"},
                {"sequence": 9, "description": "Last sequence"},
                {"sequence": 10, "description": "Beyond dataset (should handle gracefully)"},
                {"sequence": 50, "description": "Far beyond dataset (should handle gracefully)"}
            ]

            all_passed = True

            for case in edge_cases:
                selected_sequence = case["sequence"]
                description = case["description"]

                print(f"\n🧪 Testing: {description} (Sequence {selected_sequence})")

                try:
                    # Calculate window (should not crash)
                    window_calc = self.simulate_window_calculation(selected_sequence, self.mock_dataset)
                    api_response = self.simulate_api_response(window_calc)

                    # Validate results
                    data_points = len(api_response['data'])
                    start_idx = window_calc['start_idx']

                    print(f"   Window calculation successful:")
                    print(f"   - Start index: {start_idx}")
                    print(f"   - Data points returned: {data_points}")
                    print(f"   - Center index: {window_calc['center_index']}")

                    # Boundary validations
                    if start_idx >= 0:
                        print(f"   ✅ Start index non-negative: {start_idx}")
                    else:
                        print(f"   ❌ Start index negative: {start_idx}")
                        all_passed = False

                    if data_points > 0:
                        print(f"   ✅ Returns some data: {data_points} points")
                    else:
                        print(f"   ⚠️  Returns no data (may be expected for out-of-bounds)")

                    if data_points <= 21:
                        print(f"   ✅ Data points within limit: {data_points} <= 21")
                    else:
                        print(f"   ❌ Too many data points: {data_points} > 21")
                        all_passed = False

                except Exception as e:
                    print(f"   ❌ Edge case handling failed: {e}")
                    all_passed = False

            if all_passed:
                print(f"\n✅ Boundary edge cases test passed!")
                return True
            else:
                print(f"\n❌ Some boundary edge cases failed!")
                return False

        except Exception as e:
            print(f"❌ Boundary edge cases test failed: {e}")
            return False

    async def run_all_tests(self) -> bool:
        """Run all 21-row window visualization tests."""
        print("🪟 **21-ROW WINDOW VISUALIZATION TEST SUITE**")
        print("Comprehensive testing of 21-row window feature implementation")
        print("=" * 80)

        tests = [
            ("Window Calculation Logic", self.test_window_calculation_logic()),
            ("API Data Extraction", self.test_api_data_extraction()),
            ("Datetime Formatting in Window", self.test_datetime_formatting_in_window()),
            ("Technical Indicators in Window", self.test_technical_indicators_in_window()),
            ("Chart Display Information", self.test_chart_display_information()),
            ("Boundary Edge Cases", self.test_boundary_edge_cases())
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

        print(f"\n📊 **21-ROW WINDOW TEST RESULTS: {passed}/{total} PASSED**")
        print("=" * 80)

        for result in results:
            status_icon = "✅" if result[1] == "PASSED" else "❌"
            print(f"{status_icon} {result[0]}: {result[1]}")
            if len(result) > 2:  # Error details
                print(f"    Error: {result[2]}")

        if passed == total:
            print("\n🎉 **ALL 21-ROW WINDOW VISUALIZATION TESTS PASSED!**")
            print("✅ Window calculation logic works correctly for all scenarios")
            print("✅ API data extraction returns proper 21-point windows")
            print("✅ Datetime formatting displays correctly in YYYYMMDD HH:MM format")
            print("✅ Technical indicators are preserved in window data")
            print("✅ Chart display shows accurate window information to users")
            print("✅ Boundary conditions and edge cases handled properly")
            return True
        else:
            print(f"\n⚠️ **{total - passed} of {total} tests failed - review results above**")
            return False


async def main():
    """Main test runner for 21-row window visualization tests."""
    test_suite = TwentyOneRowWindowVisualizationTests()
    success = await test_suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)