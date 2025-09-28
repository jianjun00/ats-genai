#!/usr/bin/env python3
"""
Browser-like EDA Interface Testing (Simulated)
Simulates browser behavior for testing EDA interface without requiring Playwright dependencies
"""

import requests
import json
import re

class EDABrowserSimulator:
    """Simulates browser behavior for testing EDA interface."""

    def __init__(self, base_url="http://localhost:4000"):
        self.base_url = base_url
        self.session = requests.Session()

    def test_page_accessibility(self):
        """Test that EDA page is accessible and returns valid HTML."""
        print("🧪 Testing Page Accessibility...")

        response = self.session.get(f"{self.base_url}/eda", timeout=10)

        if response.status_code == 200:
            print("✅ EDA page accessible (200 OK)")

            # Check for basic HTML structure
            html = response.text
            if "<html" in html and "</html>" in html:
                print("✅ Valid HTML structure found")
            else:
                print("❌ Invalid HTML structure")
                return False

            # Check for key elements
            key_elements = [
                "ATS Exploratory Data Analysis",
                "dataset-select",
                "global-x-axis",
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
                print("✅ All key elements found in HTML")

            return True
        else:
            print(f"❌ Page not accessible: {response.status_code}")
            return False

    def test_javascript_syntax(self):
        """Test for JavaScript syntax errors by parsing the HTML."""
        print("\n🧪 Testing JavaScript Syntax...")

        response = self.session.get(f"{self.base_url}/eda", timeout=10)
        html = response.text

        # Extract JavaScript from script tags
        script_patterns = [
            r'<script[^>]*>(.*?)</script>',
            r"<script[^>]*>(.*?)</script>"
        ]

        js_content = ""
        for pattern in script_patterns:
            matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
            js_content += "\n".join(matches)

        if not js_content:
            print("⚠️ No JavaScript found in HTML")
            return True

        # Check for common syntax errors
        syntax_issues = []

        # Unmatched braces
        brace_open = js_content.count('{')
        brace_close = js_content.count('}')
        if brace_open != brace_close:
            syntax_issues.append(f"Unmatched braces: {brace_open} open, {brace_close} close")

        # Unmatched parentheses
        paren_open = js_content.count('(')
        paren_close = js_content.count(')')
        if paren_open != paren_close:
            syntax_issues.append(f"Unmatched parentheses: {paren_open} open, {paren_close} close")

        # Check for orphaned code patterns
        orphaned_patterns = [
            r'^\s*(const|let|var)\s+\w+',  # Variable declarations not in functions
            r'^\s*\w+\s*\(',  # Function calls not in functions
            r'^\s*[\w.]+\s*=',  # Assignments not in functions
        ]

        lines = js_content.split('\n')
        orphaned_code = []
        in_function = False
        brace_level = 0

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped or stripped.startswith('//') or stripped.startswith('*'):
                continue

            # Track function context
            if 'function' in stripped or '=>' in stripped:
                in_function = True

            brace_level += stripped.count('{') - stripped.count('}')
            if brace_level <= 0:
                in_function = False

            # Check for orphaned code
            if not in_function and stripped:
                for pattern in orphaned_patterns:
                    if re.match(pattern, stripped):
                        orphaned_code.append(f"Line {i+1}: {stripped[:50]}...")
                        break

        if syntax_issues:
            print("❌ JavaScript syntax issues found:")
            for issue in syntax_issues:
                print(f"  - {issue}")
            return False

        if orphaned_code:
            print("❌ Orphaned JavaScript code found:")
            for code in orphaned_code[:5]:  # Show first 5
                print(f"  - {code}")
            return False

        print("✅ No obvious JavaScript syntax issues found")
        return True

    def test_api_endpoints(self):
        """Test the API endpoints used by the EDA interface."""
        print("\n🧪 Testing API Endpoints...")

        endpoints = [
            "/api/eda/datasets",
            "/api/eda/analyze",
        ]

        results = {}

        for endpoint in endpoints:
            if endpoint == "/api/eda/analyze":
                # POST request for analyze
                response = self.session.post(
                    f"{self.base_url}{endpoint}",
                    json={"dataset_name": "test", "column": "test", "filters": {}},
                    timeout=10
                )
            else:
                # GET request
                response = self.session.get(f"{self.base_url}{endpoint}", timeout=10)

            results[endpoint] = response.status_code

            if response.status_code == 200:
                print(f"✅ {endpoint} - 200 OK")

                # For datasets endpoint, check response structure
                if endpoint == "/api/eda/datasets":
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        print(f"   • Found {len(data)} datasets")
                        first_dataset = data[0]
                        required_fields = ['name', 'row_count', 'column_count']
                        missing_fields = [f for f in required_fields if f not in first_dataset]
                        if missing_fields:
                            print(f"   ⚠️ Missing fields in dataset: {missing_fields}")
                        else:
                            print("   ✅ Dataset structure looks correct")
                    else:
                        print("   ⚠️ No datasets returned")
                print(f"❌ {endpoint} - {response.status_code}")

        return results

    def test_symbol_filtering_api(self):
        """Test the symbol filtering functionality that causes 'undefined' issue."""
        print("\n🧪 Testing Symbol Filtering API...")

        # First get available datasets
        datasets_response = self.session.get(f"{self.base_url}/api/eda/datasets", timeout=10)
        if datasets_response.status_code != 200:
            print("❌ Cannot get datasets for filtering test")
            return False

        datasets = datasets_response.json()
        if not datasets:
            print("❌ No datasets available for filtering test")
            return False

        first_dataset = datasets[0]
        dataset_name = first_dataset['name']
        print(f"Testing with dataset: {dataset_name}")

        # Test symbol filtering
        filter_payload = {
            "filters": {
                "symbol": {
                    "type": "values",
                    "values": ["TSLA"]
                }
            },
            "page": 1,
            "page_size": 10
        }

        filter_response = self.session.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            json=filter_payload,
            timeout=10
        )

        if filter_response.status_code == 200:
            data = filter_response.json()

            # Check for undefined values
            undefined_fields = []
            key_fields = ['total_count', 'current_page', 'total_pages']

            for field in key_fields:
                value = data.get(field)
                if value is None or value == 'undefined':
                    undefined_fields.append(field)

            if undefined_fields:
                print(f"❌ Found undefined/null fields: {undefined_fields}")
                print("   This explains the user's 'Showing X of undefined records' issue!")

                # Show actual response for debugging
                print("   Response structure:")
                for key, value in data.items():
                    print(f"     {key}: {value} (type: {type(value).__name__})")

                return False
            else:
                print("✅ All pagination fields have valid values")
                print(f"   • total_count: {data.get('total_count')}")
                print(f"   • current_page: {data.get('current_page')}")
                print(f"   • total_pages: {data.get('total_pages')}")
                return True
        else:
            print(f"❌ Symbol filtering API failed: {filter_response.status_code}")
            error_data = filter_response.json()
            print(f"   Error response: {error_data}")
            return False

    def test_dom_structure(self):
        """Test the DOM structure for x-axis controls and other elements."""
        print("\n🧪 Testing DOM Structure...")

        response = self.session.get(f"{self.base_url}/eda", timeout=10)
        html = response.text

        # Test global x-axis control
        global_axis_count = html.count('global-x-axis')
        print(f"Global x-axis controls found: {global_axis_count}")

        if global_axis_count >= 1:
            print("✅ Global x-axis control present")
        else:
            print("❌ Global x-axis control missing")

        # Test for per-column x-axis controls (should be 0)
        per_column_patterns = [
            r'xaxis-\${col\.name}',
            r'Select X-axis.*optional',
            r'visualization-controls.*select'
        ]

        per_column_found = 0
        for pattern in per_column_patterns:
            matches = len(re.findall(pattern, html, re.IGNORECASE))
            per_column_found += matches
            if matches > 0:
                print(f"❌ Found {matches} matches for per-column pattern: {pattern}")

        if per_column_found == 0:
            print("✅ No per-column x-axis controls found")
        else:
            print(f"❌ Found {per_column_found} per-column control patterns")

        # Test positioning (Chart Configuration before Data Filter)
        chart_config_pos = html.find('Chart Configuration')
        data_filter_pos = html.find('Data Filter')

        if chart_config_pos > 0 and data_filter_pos > 0:
            if chart_config_pos < data_filter_pos:
                print("✅ Chart Configuration positioned before Data Filter")
            else:
                print("❌ Chart Configuration positioned after Data Filter")
        else:
            print("⚠️ Could not find both Chart Configuration and Data Filter sections")

        return per_column_found == 0 and global_axis_count >= 1

    def run_all_tests(self):
        """Run all tests and return summary."""
        print("🔍 EDA Browser Simulation Test Suite")
        print("="*60)

        tests = [
            ("Page Accessibility", self.test_page_accessibility),
            ("JavaScript Syntax", self.test_javascript_syntax),
            ("API Endpoints", self.test_api_endpoints),
            ("Symbol Filtering", self.test_symbol_filtering_api),
            ("DOM Structure", self.test_dom_structure),
        ]

        results = {}

        for test_name, test_func in tests:
            result = test_func()
            results[test_name] = "✅ PASS" if result else "❌ FAIL"
        print(f"\n📊 Test Results Summary")
        print("="*60)

        for test_name, result in results.items():
            print(f"{test_name}: {result}")

        passed = sum(1 for result in results.values() if result.startswith("✅"))
        total = len(results)

        print(f"\n🎯 Overall: {passed}/{total} tests passed")

        if passed == total:
            print("🎉 All browser simulation tests passed!")
        else:
            print("⚠️ Some tests failed - see details above")

        return results


if __name__ == "__main__":
    # Run the browser simulation tests
    simulator = EDABrowserSimulator()
    results = simulator.run_all_tests()

    print("\n💡 Next Steps:")
    print("- Install Playwright dependencies: sudo playwright install-deps")
    print("- Run real browser tests: python3 tests/playwright/test_eda_interface.py")
    print("- Access interface manually: http://localhost:4000/eda")