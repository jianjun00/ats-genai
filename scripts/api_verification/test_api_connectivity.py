#!/usr/bin/env python3
import requests
import json
import sys

def print_json(data):
    """Print JSON data in a readable format"""
    print(json.dumps(data, indent=2))

def test_api_connectivity():
    """Test API connectivity with the database"""
    base_url = "http://localhost:8080"
    
    # Test health endpoint
    print("Testing health endpoint...")
    try:
        response = requests.get(f"{base_url}/health")
        response.raise_for_status()
        print("Health check successful:")
        print_json(response.json())
        print()
    except Exception as e:
        print(f"Health check failed: {e}")
        sys.exit(1)
    
    # Test DB check endpoint
    print("Testing database connectivity...")
    try:
        response = requests.get(f"{base_url}/api/v1/db-check")
        response.raise_for_status()
        print("Database connectivity successful:")
        print_json(response.json())
        print()
    except Exception as e:
        print(f"Database connectivity failed: {e}")
        sys.exit(1)
    
    # Test instruments endpoint
    print("Testing instruments endpoint...")
    try:
        response = requests.get(f"{base_url}/api/v1/instruments")
        response.raise_for_status()
        data = response.json()
        print(f"Found {data['count']} instruments")
        # Print first 3 instruments only to avoid overwhelming output
        if data['instruments']:
            print("Sample instruments:")
            for instrument in data['instruments'][:3]:
                print_json(instrument)
        print()
    except Exception as e:
        print(f"Instruments endpoint failed: {e}")
    
    # Test specific instrument endpoint
    print("Testing specific instrument endpoint...")
    symbol = "AAPL"
    try:
        response = requests.get(f"{base_url}/api/v1/instrument/{symbol}")
        response.raise_for_status()
        print(f"Instrument {symbol} details:")
        print_json(response.json())
    except Exception as e:
        print(f"Instrument endpoint failed: {e}")

if __name__ == "__main__":
    test_api_connectivity()
