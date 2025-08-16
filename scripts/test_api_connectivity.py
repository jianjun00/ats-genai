#!/usr/bin/env python3
"""
Simple script to test API connectivity and database integration.
"""

import requests
import json
import sys

def test_api_connectivity():
    """Test API connectivity and database integration."""
    base_url = "http://localhost:8090"
    
    print("Testing API connectivity...")
    
    # Test endpoints
    endpoints = [
        "/",
        "/health",
        "/api/v1/db-check",
        "/api/v1/instruments"
    ]
    
    success = True
    
    for endpoint in endpoints:
        print(f"\nTesting endpoint: {endpoint}")
        try:
            response = requests.get(f"{base_url}{endpoint}", timeout=5)
            print(f"Status code: {response.status_code}")
            
            if response.status_code == 200:
                print("Response:")
                try:
                    data = response.json()
                    print(json.dumps(data, indent=2))
                    
                    # For db-check endpoint, verify tables
                    if endpoint == "/api/v1/db-check" and "tables" in data:
                        print("\nVerified tables:")
                        for table in data["tables"]:
                            print(f"- {table['table_name']}: {table['row_count']} rows")
                except Exception as e:
                    print(f"Error parsing JSON: {e}")
                    print(response.text)
            else:
                print(f"Error: Unexpected status code {response.status_code}")
                print(response.text)
                success = False
                
        except Exception as e:
            print(f"Error connecting to API: {e}")
            success = False
    
    return success

if __name__ == "__main__":
    success = test_api_connectivity()
    sys.exit(0 if success else 1)
