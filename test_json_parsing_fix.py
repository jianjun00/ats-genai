#!/usr/bin/env python3
"""
Simple test to detect the specific JSON parsing error:
'Error loading jobs: Unexpected token 'I', "Internal S"... is not valid JSON'

This test can be run quickly to verify if the frontend JSON parsing issue is fixed.
"""

import requests
import json

def test_json_parsing_issue():
    """Test for the specific JSON parsing issue that breaks frontend"""
    print("🔍 Testing for JSON Parsing Issue")
    print("="*50)
    
    # Test both access methods
    test_urls = [
        ("Fixed Port Forward", "http://localhost:9990"),
        ("External NodePort", "http://192.168.49.2:30001")
    ]
    
    test_endpoints = ["/api/v1/jobs", "/api/v1/jobs/stats", "/api/v1/datasets"]
    
    total_issues = 0
    
    for url_name, base_url in test_urls:
        print(f"\n📡 Testing {url_name}: {base_url}")
        
        issues_found = 0
        
        for endpoint in test_endpoints:
            url = f"{base_url}{endpoint}"
            
            try:
                response = requests.get(url, timeout=10)
                
                print(f"   {endpoint}: Status {response.status_code}")
                
                # Check for the exact issue that breaks frontend
                if response.status_code == 500 and response.text.strip() == "Internal Server Error":
                    print(f"   ❌ ISSUE DETECTED: Plain text 'Internal Server Error'")
                    print(f"      Content-Type: {response.headers.get('content-type')}")
                    print(f"      This will cause frontend error: 'Unexpected token 'I', \"Internal S\"... is not valid JSON'")
                    issues_found += 1
                    total_issues += 1
                    
                    # Simulate what happens in frontend
                    try:
                        json.loads(response.text)
                    except json.JSONDecodeError as e:
                        print(f"      JSON Parse Error: {e}")
                        
                elif response.status_code == 200:
                    # Verify it's valid JSON
                    try:
                        data = response.json()
                        print(f"   ✅ Returns valid JSON")
                    except json.JSONDecodeError:
                        print(f"   ❌ ISSUE: Status 200 but invalid JSON")
                        issues_found += 1
                        total_issues += 1
                        
                else:
                    print(f"   ⚠️  Status {response.status_code}: {response.text[:50]}...")
                    
            except requests.RequestException as e:
                print(f"   🔌 Connection failed: {e}")
        
        if issues_found == 0:
            print(f"   ✅ {url_name}: No JSON parsing issues detected")
        else:
            print(f"   ❌ {url_name}: Found {issues_found} JSON parsing issues")
    
    print(f"\n{'='*50}")
    print(f"📊 SUMMARY")
    print(f"{'='*50}")
    
    if total_issues > 0:
        print(f"❌ FOUND {total_issues} JSON PARSING ISSUES")
        print(f"   Root Cause: API returning plain text 'Internal Server Error'")
        print(f"   Frontend Impact: 'Error loading jobs: Unexpected token 'I', \"Internal S\"... is not valid JSON'")
        print(f"   Solution: API should return JSON error responses instead")
        print(f"   Example fix: {{\"error\": \"internal_server_error\", \"message\": \"Database connection failed\"}}")
        return False
    else:
        print(f"✅ NO JSON PARSING ISSUES DETECTED")
        print(f"   All API endpoints return JSON-compatible responses")
        return True

if __name__ == "__main__":
    success = test_json_parsing_issue()
    exit(0 if success else 1)