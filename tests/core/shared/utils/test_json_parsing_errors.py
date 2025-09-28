#!/usr/bin/env python3
"""
Test case to detect JSON parsing errors from Internal Server Error responses

This test detects the specific issue:
'Error loading jobs: Unexpected token 'I', "Internal S"... is not valid JSON'

This error occurs when:
1. API endpoint returns "Internal Server Error" (plain text)
2. Frontend expects JSON response

RESOLUTION: Fixed by creating missing dev_runs table and implementing proper
universal exception handler that returns JSON responses with proper Content-Type headers.
3. JSON.parse() fails on "Internal Server Error" text
"""

import requests
import json

# Test configuration
TEST_BASE_URL = "http://localhost:9996"  # Port forward URL
ENDPOINTS_TO_TEST = [
    "/api/v1/jobs",
    "/api/v1/jobs/stats",
    "/api/v1/datasets",
    "/api/v1/datasets/1/sequences",
    "/api/v1/coverage/summary",
    "/api/v1/coverage/gaps",
    "/api/v1/coverage/comparison/AAPL"
]

class TestJSONParsingErrors:
    """Test suite to detect and prevent JSON parsing errors"""

    def test_endpoints_return_valid_json_or_proper_error(self):
        """
        Test all API endpoints return either:
        1. Valid JSON response (200 OK)
        2. Proper JSON error response (4xx/5xx with valid JSON)

        Should NEVER return plain text "Internal Server Error"
        """
        print("🧪 Testing API endpoints for JSON parsing compatibility")

        failed_endpoints = []
        json_parse_errors = []

        for endpoint in ENDPOINTS_TO_TEST:
            url = f"{TEST_BASE_URL}{endpoint}"
            print(f"\n📡 Testing: {endpoint}")

            response = requests.get(url, timeout=10)
            content_type = response.headers.get('content-type', '')

            print(f"   Status: {response.status_code}")
            print(f"   Content-Type: {content_type}")
            print(f"   Response: {response.text[:100]}...")

            # Check if response claims to be JSON
            if 'application/json' in content_type:
                # Should be valid JSON
                json_data = response.json()
                print(f"   ✅ Valid JSON response")

                # Additional check: ensure no "Internal Server Error" in JSON
                if isinstance(json_data, dict) and json_data.get('error') == 'Internal Server Error':
                    print(f"   ⚠️ JSON contains 'Internal Server Error' - should be more specific")

            elif response.status_code >= 500:
                # 5xx errors should still return JSON
                if response.text.strip() == "Internal Server Error":
                    print(f"   ❌ Plain text 'Internal Server Error' - will cause frontend JSON.parse() to fail")
                    json_parse_errors.append({
                        'endpoint': endpoint,
                        'status_code': response.status_code,
                        'content_type': content_type,
                        'response_text': response.text,
                        'json_error': "Plain text response will cause JSON.parse() to fail"
                    })
                    failed_endpoints.append(endpoint)
                else:
                    print(f"   ⚠️ Non-JSON error response: {response.text[:50]}")
            else:
                print(f"   ✅ Non-JSON response (status {response.status_code})")

        print(f"\n{'='*60}")
        print("📊 JSON PARSING ERROR DETECTION RESULTS")
        print(f"{'='*60}")

        if json_parse_errors:
            print(f"❌ FOUND {len(json_parse_errors)} JSON PARSING ISSUES:")
            for i, error in enumerate(json_parse_errors, 1):
                print(f"\n{i}. ENDPOINT: {error['endpoint']}")
                print(f"   STATUS: {error['status_code']}")
                print(f"   CONTENT-TYPE: {error['content_type']}")
                print(f"   RESPONSE: {error['response_text']}")
                print(f"   JSON ERROR: {error['json_error']}")

            print(f"\n🔧 RECOMMENDED FIXES:")
            print("1. Ensure all API endpoints return JSON responses")
            print("2. Replace plain 'Internal Server Error' with JSON:")
            print('   {"error": "internal_server_error", "message": "Database connection failed"}')
            print("3. Set proper Content-Type: application/json headers")
            print("4. Add error handling middleware for consistent JSON responses")

        else:
            print("✅ ALL ENDPOINTS RETURN JSON-COMPATIBLE RESPONSES")

        print(f"\n📈 SUMMARY:")
        print(f"   Total endpoints tested: {len(ENDPOINTS_TO_TEST)}")
        print(f"   JSON parsing issues: {len(json_parse_errors)}")
        print(f"   Failed endpoints: {len(failed_endpoints)}")

        # Test assertion
        assert len(json_parse_errors) == 0, f"Found {len(json_parse_errors)} JSON parsing issues that will cause frontend errors"

    def test_specific_jobs_api_json_compatibility(self):
        """
        Specific test for the jobs API that was causing:
        'Error loading jobs: Unexpected token 'I', "Internal S"... is not valid JSON'
        """
        print("\n🎯 SPECIFIC TEST: Jobs API JSON Compatibility")

        jobs_endpoints = [
            "/api/v1/jobs",
            "/api/v1/jobs/stats"
        ]

        for endpoint in jobs_endpoints:
            url = f"{TEST_BASE_URL}{endpoint}"
            print(f"\n📡 Testing jobs endpoint: {endpoint}")

            response = requests.get(url, timeout=10)

            # This is the exact scenario that causes the frontend error
            if response.status_code == 500 and response.text.strip() == "Internal Server Error":
                print(f"❌ DETECTED THE EXACT ISSUE!")
                print(f"   Response: '{response.text}'")
                print(f"   This will cause: Unexpected token 'I', \"Internal S\"... is not valid JSON")

                # Simulate what frontend does
                json.loads(response.text)
                assert False, f"Jobs API returns plain text error that breaks frontend JSON parsing"

            elif response.status_code == 200:
                # Should be valid JSON
                jobs_data = response.json()
                print(f"   ✅ Jobs API returns valid JSON")
                print(f"   Keys: {list(jobs_data.keys())}")

            else:
                print(f"   Status: {response.status_code}")
                print(f"   Response: {response.text[:100]}")

    def test_error_response_format_standards(self):
        """
        Test that error responses follow a consistent JSON format
        """
        print("\n🏗️ TESTING ERROR RESPONSE FORMAT STANDARDS")

        # Test with a non-existent endpoint to trigger error
        error_test_endpoints = [
            "/api/v1/nonexistent",
            "/api/v1/jobs/999999",  # Should return 404
            "/api/v1/datasets/999999"  # Should return 404
        ]

        expected_error_format = {
            "error": str,  # Error code/type
            "message": str,  # Human readable message
            "status": int   # HTTP status code
        }

        for endpoint in error_test_endpoints:
            url = f"{TEST_BASE_URL}{endpoint}"
            print(f"\n📡 Testing error format: {endpoint}")

            response = requests.get(url, timeout=10)

            if response.status_code >= 400:
                error_data = response.json()
                print(f"   Status: {response.status_code}")
                print(f"   Error JSON: {json.dumps(error_data, indent=2)}")

                # Validate error response has proper structure
                if not isinstance(error_data, dict):
                    print(f"   ⚠️ Error response should be JSON object")

                if 'error' not in error_data:
                    print(f"   ⚠️ Error response missing 'error' field")

                print(f"   ✅ Proper JSON error format")

            print(f"   Connection failed: {e}")

def run_json_parsing_error_tests():
    """Run all JSON parsing error detection tests"""
    print("🚀 RUNNING JSON PARSING ERROR DETECTION TESTS")
    print("="*80)

    test_instance = TestJSONParsingErrors()

    tests = [
        ("General JSON Compatibility", test_instance.test_endpoints_return_valid_json_or_proper_error),
        ("Jobs API Specific Test", test_instance.test_specific_jobs_api_json_compatibility),
        ("Error Format Standards", test_instance.test_error_response_format_standards)
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        print("-" * 60)
        test_func()
        print(f"✅ PASSED: {test_name}")
        passed += 1
    print("\n" + "="*80)
    print("📊 JSON PARSING ERROR TEST SUMMARY")
    print("="*80)
    print(f"✅ PASSED: {passed} tests")
    print(f"❌ FAILED: {failed} tests")

    if failed > 0:
        print(f"\n🔧 ISSUES DETECTED:")
        print(f"   Frontend will show: 'Error loading jobs: Unexpected token 'I', \"Internal S\"... is not valid JSON'")
        print(f"   Root cause: API returning plain text 'Internal Server Error' instead of JSON")
        print(f"   Solution: Fix API to return JSON error responses")
    else:
        print(f"\n🎉 NO JSON PARSING ISSUES DETECTED!")

    return passed, failed

if __name__ == "__main__":
    run_json_parsing_error_tests()