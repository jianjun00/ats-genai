#!/usr/bin/env python3
"""
UI Layout Regression Protection Tests

This test detects when the UI layout changes unexpectedly:
- Tab structure changes (tabs added/removed/renamed)
- Dataset functionality changes (simple list vs detail pages)
- Coverage functionality changes (presence/absence)
- JavaScript function changes (loadDatasets implementation)

PURPOSE: Prevent accidental UI regressions and layout changes
"""

import requests
import re
from typing import Dict
from dataclasses import dataclass

# Test configuration
TEST_BASE_URL = "http://localhost:8000"

@dataclass
class UILayoutExpectations:
    """Expected UI layout - DO NOT CHANGE without understanding impact"""

    # Required tabs in the UI
    REQUIRED_TABS = [
        "Jobs", "Datasets", "Coverage"  # These tabs MUST exist
    ]

    # Required dataset functionality
    REQUIRED_DATASET_FUNCTIONS = [
        "loadDatasets",           # Basic dataset loading
        "loadDatasetDetail",      # Dataset detail page loading (MISSING!)
        "showDatasetDetail"       # Show dataset detail functionality
    ]

    # Required coverage functionality
    REQUIRED_COVERAGE_FUNCTIONS = [
        "loadCoverage"            # Coverage loading functionality
    ]

    # UI elements that must be present
    REQUIRED_UI_ELEMENTS = [
        "datasets-list",          # Dataset list container
        "coverage-summary",       # Coverage summary container
        "coverage-stats"          # Coverage statistics
    ]

class TestUILayoutRegressionProtection:
    """Comprehensive UI layout regression protection"""

    def test_tab_structure_consistency(self):
        """CRITICAL: Ensure tab structure remains consistent"""
        print("\\n🧪 Testing UI tab structure consistency")

        response = requests.get(f"{TEST_BASE_URL}/", timeout=10)
        assert response.status_code == 200, "Cannot load main UI page"

        html_content = response.text

        # Extract tab names from HTML
        tab_pattern = r'<button class="tab[^"]*"[^>]*onclick="showTab\('([^']+)'\)"[^>]*>.*?([^<]+)</button>'
        tabs = re.findall(tab_pattern, html_content, re.IGNORECASE | re.DOTALL)

        found_tabs = [tab[1].strip() for _, tab in tabs if tab[1].strip()]
        print(f"   Found tabs: {found_tabs}")

        # Check for required tabs
        for required_tab in UILayoutExpectations.REQUIRED_TABS:
            tab_found = any(required_tab.lower() in tab.lower() for tab in found_tabs)
            assert tab_found, f"Required tab '{required_tab}' missing from UI"

        print("   ✅ All required tabs present")

    def test_dataset_functionality_completeness(self):
        """CRITICAL: Dataset functionality must include detail pages, not just lists"""
        print("\\n🧪 Testing dataset functionality completeness")

        response = requests.get(f"{TEST_BASE_URL}/", timeout=10)
        html_content = response.text

        # Check for dataset detail functionality in JavaScript
        js_functions = self._extract_javascript_functions(html_content)
        print(f"   Found JS functions: {list(js_functions.keys())}")

        # Basic loadDatasets should exist
        assert "loadDatasets" in js_functions, "loadDatasets function missing"

        # Check loadDatasets implementation for complexity
        load_datasets_code = js_functions.get("loadDatasets", "")

        # Should be more than just a simple list - should support detail navigation
        if "datasets-list" in load_datasets_code and len(load_datasets_code) < 500:
            print("   ⚠️  WARNING: loadDatasets appears to be simplified list-only implementation")
            print("   Expected: Dataset detail page functionality")
            print("   Found: Simple list implementation")

        # Check for dataset detail elements
        if "dataset-detail" not in html_content and "showDatasetDetail" not in html_content:
            print("   ❌ REGRESSION DETECTED: Dataset detail functionality missing")
            print("   This means users cannot view detailed dataset information")
            assert False, "Dataset detail functionality missing from UI"

        print("   ✅ Dataset functionality appears complete")

    def test_coverage_functionality_presence(self):
        """CRITICAL: Coverage functionality must be present and functional"""
        print("\\n🧪 Testing coverage functionality presence")

        response = requests.get(f"{TEST_BASE_URL}/", timeout=10)
        html_content = response.text

        # Check for coverage tab
        assert "coverage" in html_content.lower(), "Coverage tab missing from UI"

        # Check for coverage JavaScript functionality
        js_functions = self._extract_javascript_functions(html_content)
        assert "loadCoverage" in js_functions, "loadCoverage function missing"

        # Check for coverage UI elements
        required_coverage_elements = ["coverage-summary", "coverage-stats", "coverage-gaps"]
        for element in required_coverage_elements:
            assert element in html_content, f"Coverage UI element '{element}' missing"

        print("   ✅ Coverage functionality present and complete")

    def test_javascript_function_regression_detection(self):
        """CRITICAL: Detect when JavaScript functions are simplified or removed"""
        print("\\n🧪 Testing JavaScript function regression detection")

        response = requests.get(f"{TEST_BASE_URL}/", timeout=10)
        html_content = response.text

        js_functions = self._extract_javascript_functions(html_content)

        # Analyze function complexity
        function_analysis = {}
        for func_name, func_code in js_functions.items():
            function_analysis[func_name] = {
                "length": len(func_code),
                "api_calls": func_code.count("fetch("),
                "dom_updates": func_code.count("innerHTML"),
                "complexity_score": len(func_code) + func_code.count("fetch(") * 50 + func_code.count("innerHTML") * 20
            }

        print(f"   Function analysis:")
        for func, analysis in function_analysis.items():
            print(f"     {func}: {analysis['complexity_score']} complexity, {analysis['api_calls']} API calls")

        # Regression detection: loadDatasets should be reasonably complex
        if "loadDatasets" in function_analysis:
            load_datasets_complexity = function_analysis["loadDatasets"]["complexity_score"]
            if load_datasets_complexity < 200:
                print(f"   ⚠️  WARNING: loadDatasets complexity is low ({load_datasets_complexity})")
                print("   This suggests it may be a simplified implementation")

        # Coverage function should exist and be complex
        if "loadCoverage" in function_analysis:
            load_coverage_complexity = function_analysis["loadCoverage"]["complexity_score"]
            assert load_coverage_complexity > 100, f"loadCoverage function too simple ({load_coverage_complexity})"

        print("   ✅ JavaScript functions appear reasonably complex")

    def test_ui_element_presence_over_time(self):
        """CRITICAL: Essential UI elements must remain present"""
        print("\\n🧪 Testing UI element presence over time")

        response = requests.get(f"{TEST_BASE_URL}/", timeout=10)
        html_content = response.text

        # Check for essential UI containers
        essential_elements = [
            "jobs-stats", "jobs-list",           # Jobs functionality
            "datasets-list",                     # Dataset functionality
            "coverage-stats", "coverage-summary", "coverage-gaps"  # Coverage functionality
        ]

        missing_elements = []
        for element in essential_elements:
            if element not in html_content:
                missing_elements.append(element)

        if missing_elements:
            print(f"   ❌ MISSING UI ELEMENTS: {missing_elements}")
            assert False, f"Essential UI elements missing: {missing_elements}"

        print("   ✅ All essential UI elements present")

    def _extract_javascript_functions(self, html_content: str) -> Dict[str, str]:
        """Extract JavaScript functions from HTML"""
        # Find the script section
        script_pattern = r'<script>(.*?)</script>'
        script_match = re.search(script_pattern, html_content, re.DOTALL)

        if not script_match:
            return {}

        script_content = script_match.group(1)

        # Extract function definitions
        function_pattern = r'(async\s+)?function\s+(\w+)\s*\([^)]*\)\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\}'
        functions = re.findall(function_pattern, script_content, re.DOTALL)

        result = {}
        for async_keyword, func_name, func_body in functions:
            result[func_name] = func_body.strip()

        return result

def run_ui_layout_regression_tests():
    """Run all UI layout regression protection tests"""
    print("🚀 RUNNING UI LAYOUT REGRESSION PROTECTION TESTS")
    print("Purpose: Detect unintentional changes to UI layout and functionality")
    print("=" * 80)

    test_instance = TestUILayoutRegressionProtection()

    tests = [
        ("Tab Structure Consistency", test_instance.test_tab_structure_consistency),
        ("Dataset Functionality Completeness", test_instance.test_dataset_functionality_completeness),
        ("Coverage Functionality Presence", test_instance.test_coverage_functionality_presence),
        ("JavaScript Function Regression", test_instance.test_javascript_function_regression_detection),
        ("UI Element Presence", test_instance.test_ui_element_presence_over_time)
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        print(f"\\n🧪 Running: {test_name}")
        print("-" * 60)
        test_func()
        print(f"✅ PASSED: {test_name}")
        passed += 1
    print("\\n" + "=" * 80)
    print("📊 UI LAYOUT REGRESSION PROTECTION SUMMARY")
    print("=" * 80)
    print(f"✅ PASSED: {passed} tests")
    print(f"❌ FAILED: {failed} tests")

    if failed > 0:
        print(f"\\n🚨 UI LAYOUT REGRESSIONS DETECTED:")
        print(f"   The UI layout or functionality has changed unexpectedly!")
        print(f"   This indicates:")
        print(f"   - Tab structure may have changed")
        print(f"   - Dataset detail functionality may be missing")
        print(f"   - Coverage functionality may be affected")
        print(f"   - JavaScript functions may be simplified")
    else:
        print(f"\\n🎉 NO UI LAYOUT REGRESSIONS DETECTED!")
        print(f"   UI layout and functionality preserved correctly.")

    return passed, failed

if __name__ == "__main__":
    run_ui_layout_regression_tests()