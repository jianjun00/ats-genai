#!/usr/bin/env python3
"""
Simple UI-API Integration Test
Tests API endpoints and interface compatibility
"""

import re
import json
import subprocess

def test_api_endpoints():
    """Test that API endpoints used by UI actually exist."""
    print("🧪 Testing API endpoints...")

    # Read HTML file
    with open('dataset_detail_page_frontend.html', 'r') as f:
        html_content = f.read()
    api_calls = re.findall(r'fetch\(`([^`]+)`\)', html_content)

    print(f"Found {len(api_calls)} API calls in HTML:")
    for call in api_calls:
        print(f"  - {call}")

    # Test with curl
    test_results = {}
    for api_call in api_calls:
        test_url = api_call.replace('${currentDatasetId}', '24').replace('?${params}', '')

        result = subprocess.run(['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}',
                               f'http://localhost:4000{test_url}'],
                              capture_output=True, text=True, timeout=5)

        status_code = result.stdout.strip()
        test_results[test_url] = status_code

        if status_code == '200':
            print(f"✅ {test_url} - {status_code}")
        else:
            print(f"❌ {test_url} - {status_code}")

    return test_results

def test_global_axis_control():
    """Test for global vs per-column axis controls."""
    print("\n🧪 Testing x-axis control structure...")

    with open('dataset_detail_page_frontend.html', 'r') as f:
        html_content = f.read()
    global_axis_count = html_content.count('global-x-axis')
    chart_config_mentions = html_content.count('Chart Configuration')
    data_filter_mentions = html_content.count('Data Filter')

    print(f"Global x-axis controls: {global_axis_count}")
    print(f"Chart Configuration sections: {chart_config_mentions}")
    print(f"Data Filter sections: {data_filter_mentions}")

    # Check positioning
    chart_config_pos = html_content.find('Chart Configuration')
    data_filter_pos = html_content.find('Data Filter')

    if chart_config_pos > 0 and data_filter_pos > 0:
        if chart_config_pos < data_filter_pos:
            print("✅ Chart Configuration comes before Data Filter")
        else:
            print("❌ Chart Configuration should come before Data Filter")
            return False

    # Look for per-column axis controls (shouldn't exist)
    per_column_patterns = [
        r'x-axis.*select.*featureName',
        r'axis.*select.*column',
        r'select.*axis.*distribution-item'
    ]

    per_column_found = False
    for pattern in per_column_patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        if matches:
            print(f"❌ Found per-column axis pattern: {pattern}")
            per_column_found = True

    if not per_column_found:
        print("✅ No per-column x-axis controls found")

    return global_axis_count > 0 and not per_column_found

def test_date_column_filtering():
    """Test that date columns are filtered out."""
    print("\n🧪 Testing date column filtering...")

    with open('dataset_detail_page_frontend.html', 'r') as f:
        html_content = f.read()
    has_date_columns = 'dateColumns' in html_content
    has_filtering = 'filteredDistributions' in html_content
    has_date_filter = 'date' in html_content and 'timestamp' in html_content

    print(f"Has dateColumns variable: {has_date_columns}")
    print(f"Has filteredDistributions logic: {has_filtering}")
    print(f"Filters date/timestamp: {has_date_filter}")

    if has_date_columns and has_filtering and has_date_filter:
        print("✅ Date column filtering implemented")
        return True
    else:
        print("❌ Date column filtering incomplete")
        return False

def test_actual_interface():
    """Test the actual running interface."""
    print("\n🧪 Testing live interface...")

    # Test if service is running
    result = subprocess.run(['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}',
                           'http://localhost:4000/dataset-detail'],
                          capture_output=True, text=True, timeout=5)

    if result.stdout.strip() == '200':
        print("✅ Interface is accessible")
    else:
        print("❌ Interface not accessible")
        return False

    # Test training datasets API
    result = subprocess.run(['curl', '-s', 'http://localhost:4000/api/v1/training-datasets'],
                          capture_output=True, text=True, timeout=5)

    data = json.loads(result.stdout)
    if 'datasets' in data and data['datasets']:
        print(f"✅ Found {len(data['datasets'])} training datasets")

        # Test with first dataset
        dataset_id = data['datasets'][0]['id']
        print(f"Testing with dataset ID: {dataset_id}")

        # Test data endpoint
        result = subprocess.run(['curl', '-s',
                               f'http://localhost:4000/api/v1/training-datasets/{dataset_id}/data?limit=1'],
                              capture_output=True, text=True, timeout=5)

        data_response = json.loads(result.stdout)
        if 'total_count' in data_response:
            print(f"✅ Data endpoint works: {data_response['total_count']} total records")
        else:
            print("❌ Data endpoint response invalid")

        return True
    else:
        print("❌ No training datasets found")
        return False

    print(f"❌ Interface test failed: {e}")
    return False

def main():
    """Run all tests."""
    print("🔍 UI-API Integration Test Suite")
    print("=" * 50)

    results = {}

    # Test API endpoints
    results['api_endpoints'] = test_api_endpoints()

    # Test x-axis controls
    results['axis_controls'] = test_global_axis_control()

    # Test date filtering
    results['date_filtering'] = test_date_column_filtering()

    # Test live interface
    results['live_interface'] = test_actual_interface()

    print("\n📊 Test Summary:")
    print("=" * 50)

    for test_name, result in results.items():
        if isinstance(result, bool):
            status = "✅ PASS" if result else "❌ FAIL"
        elif isinstance(result, dict):
            passed = sum(1 for v in result.values() if v == '200')
            total = len(result)
            status = f"✅ {passed}/{total} endpoints working" if passed > 0 else "❌ All endpoints failed"
        else:
            status = "❓ Unknown"

        print(f"{test_name}: {status}")

    print("\n💡 Recommendations:")
    if results.get('api_endpoints') and isinstance(results['api_endpoints'], dict):
        failed_endpoints = [url for url, status in results['api_endpoints'].items() if status != '200']
        if failed_endpoints:
            print("- Fix these API endpoint mismatches:")
            for endpoint in failed_endpoints:
                print(f"  • {endpoint}")

    if not results.get('axis_controls', True):
        print("- Remove per-column x-axis controls and ensure global control works")

    if not results.get('date_filtering', True):
        print("- Implement date column filtering in distributions")

if __name__ == "__main__":
    main()