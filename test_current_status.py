#!/usr/bin/env python3
"""
Current System Status Test
Validates what's working and what's broken after network restart
"""

import requests
import time

def test_current_status():
    base_url = "http://localhost:3000"
    
    print("🔍 Current Ray EDA System Status Check")
    print("=" * 50)
    
    # Test 1: Service health
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        print(f"✅ Service Health: {response.status_code}")
        health = response.json()
        print(f"   Status: {health.get('status', 'unknown')}")
    except Exception as e:
        print(f"❌ Service Health: {e}")
    
    # Test 2: Datasets endpoint
    try:
        response = requests.get(f"{base_url}/api/eda/datasets", timeout=10)
        if response.status_code == 200:
            datasets = response.json()
            print(f"✅ Datasets: {len(datasets)} datasets loaded")
            large_datasets = [d for d in datasets if 'daily_prices' in d.get('name', '')]
            print(f"   Large datasets: {len(large_datasets)}")
        else:
            print(f"❌ Datasets: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        print(f"❌ Datasets: {e}")
    
    # Test 3: Schema endpoint  
    try:
        response = requests.get(f"{base_url}/api/eda/datasets/dev_daily_prices_tiingo/schema", timeout=10)
        if response.status_code == 200:
            print(f"✅ Schema: Working")
        else:
            print(f"❌ Schema: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        print(f"❌ Schema: {e}")
    
    # Test 4: Column values (Ray)
    try:
        start = time.time()
        response = requests.get(f"{base_url}/api/eda/datasets/dev_daily_prices_tiingo/columns/symbol/values?limit=5", timeout=15)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            ray_powered = data.get('ray_powered', False)
            print(f"✅ Column Values (Ray): {elapsed:.2f}s, Ray: {ray_powered}")
            print(f"   Data type: {data.get('data_type', 'unknown')}")
        else:
            print(f"❌ Column Values: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        print(f"❌ Column Values: {e}")
    
    # Test 5: Analyze endpoint
    try:
        payload = {"dataset_name": "dev_daily_prices_tiingo", "column": "symbol", "filters": {}}
        response = requests.post(f"{base_url}/api/eda/analyze", json=payload, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            if 'error' in data:
                print(f"⚠️ Analyze Endpoint: Error - {data['error']}")
            else:
                print(f"✅ Analyze Endpoint: Working")
        else:
            print(f"❌ Analyze Endpoint: {response.status_code} - {response.text[:100]}")
    except Exception as e:
        print(f"❌ Analyze Endpoint: {e}")
    
    print("\n" + "=" * 50)
    print("📋 DIAGNOSIS:")
    
    # Test basic database connectivity via direct query
    try:
        import subprocess
        result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', 'SELECT 1'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ Database connectivity via run_dev: Working")
        else:
            print(f"❌ Database connectivity via run_dev: {result.stderr}")
    except Exception as e:
        print(f"❌ Database connectivity test failed: {e}")

if __name__ == "__main__":
    test_current_status()