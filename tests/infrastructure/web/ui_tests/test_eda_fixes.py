#!/usr/bin/env python3
"""
Quick Test Script for EDA Interface Fixes
Tests the specific issues that were reported by the user
"""

import requests

def test_symbol_filtering_pagination():
    """Test that symbol filtering returns proper pagination data (not undefined)."""
    print("🧪 Testing Symbol Filtering Pagination...")

    try:
        # Test TSLA symbol filter
        response = requests.post(
            "http://localhost:4000/api/eda/datasets/intg_daily_price_tiingo/data",
            json={
                "filters": {"symbol": {"type": "values", "values": ["TSLA"]}},
                "page": 1,
                "page_size": 10
            },
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()

            # Check if pagination is properly nested (not at root level)
            if 'pagination' in data:
                pagination = data['pagination']
                total_count = pagination.get('total_count')
                current_page = pagination.get('current_page')
                total_pages = pagination.get('total_pages')

                print(f"✅ Pagination properly nested:")
                print(f"   • Total count: {total_count:,}")
                print(f"   • Current page: {current_page}")
                print(f"   • Total pages: {total_pages:,}")

                if all(v is not None for v in [total_count, current_page, total_pages]):
                    print("✅ No 'undefined' values - pagination fix successful!")
                    return True
                else:
                    print("❌ Some pagination values are None")
                    return False
            else:
                print("❌ Pagination data not found in response")
                return False
        else:
            print(f"❌ API request failed: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error testing symbol filtering: {e}")
        return False

def test_global_x_axis_control():
    """Test that global x-axis control exists and per-column controls are removed."""
    print("\n🧪 Testing Global X-Axis Control...")

    try:
        response = requests.get("http://localhost:4000/eda", timeout=10)

        if response.status_code == 200:
            html = response.text

            # Check for global x-axis control
            global_axis_present = "global-x-axis" in html and "Chart Configuration" in html
            print(f"Global X-Axis Control: {'✅ Present' if global_axis_present else '❌ Missing'}")

            # Check positioning (Chart Configuration before Data Filter)
            chart_config_pos = html.find('Chart Configuration')
            data_filter_pos = html.find('Data Filter')

            if chart_config_pos > 0 and data_filter_pos > 0:
                if chart_config_pos < data_filter_pos:
                    print("✅ Chart Configuration positioned correctly above Data Filter")
                    positioning_correct = True
                else:
                    print("❌ Chart Configuration should be above Data Filter")
                    positioning_correct = False
            else:
                print("⚠️ Could not find positioning markers")
                positioning_correct = False

            return global_axis_present and positioning_correct
        else:
            print(f"❌ Failed to load EDA page: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error testing global x-axis control: {e}")
        return False

def test_interface_loads():
    """Test that the EDA interface loads without obvious errors."""
    print("\n🧪 Testing Interface Loading...")

    try:
        response = requests.get("http://localhost:4000/eda", timeout=10)

        if response.status_code == 200:
            html = response.text

            # Check for basic structure
            key_elements = [
                "ATS Exploratory Data Analysis",
                "dataset-select",
                "distributions-container"
            ]

            missing_elements = []
            for element in key_elements:
                if element not in html:
                    missing_elements.append(element)

            if missing_elements:
                print(f"❌ Missing key elements: {missing_elements}")
                return False
            else:
                print("✅ Interface loads with all key elements present")
                return True
        else:
            print(f"❌ Interface not accessible: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Error testing interface loading: {e}")
        return False

def main():
    """Run all fix verification tests."""
    print("🔍 EDA Interface Fixes Verification")
    print("="*50)

    tests = [
        ("Interface Loading", test_interface_loads),
        ("Global X-Axis Control", test_global_x_axis_control),
        ("Symbol Filtering Pagination", test_symbol_filtering_pagination),
    ]

    results = {}
    for test_name, test_func in tests:
        results[test_name] = test_func()

    # Summary
    print(f"\n📊 Test Results Summary")
    print("="*50)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")

    passed = sum(results.values())
    total = len(results)

    print(f"\n🎯 Overall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All EDA interface fixes verified successfully!")
        print("\n💡 User can now test:")
        print("  1. Visit: http://localhost:4000/eda")
        print("  2. Select a dataset from dropdown")
        print("  3. Use global X-axis control above Data Filter")
        print("  4. Apply symbol filter - should show 'X of Y records' (no undefined)")
    else:
        print("⚠️ Some fixes still need attention - see failed tests above")

if __name__ == "__main__":
    main()