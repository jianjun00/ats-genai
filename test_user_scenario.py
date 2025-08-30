#!/usr/bin/env python3
"""
Manual User Scenario Test
Tests the specific user workflow that was failing
"""

import requests
import json
import time

def test_user_scenario():
    """Test the exact scenario the user reported"""
    base_url = "http://localhost:3000"
    
    print("🧪 Testing User Scenario: EDA Filter and Visualization Loading")
    print("=" * 60)
    
    # Step 1: Check service health
    print("\n1️⃣ Checking service health...")
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        print(f"✅ Service healthy: {response.status_code}")
    except Exception as e:
        print(f"❌ Service not available: {e}")
        return False
    
    # Step 2: Load datasets
    print("\n2️⃣ Loading datasets...")
    try:
        response = requests.get(f"{base_url}/api/eda/datasets", timeout=30)
        datasets = response.json()
        large_datasets = [d for d in datasets if 'daily_prices' in d['name']]
        print(f"✅ Found {len(datasets)} total datasets")
        print(f"✅ Found {len(large_datasets)} large datasets requiring Ray")
        for ds in large_datasets[:3]:
            print(f"   📊 {ds['name']} - {ds['row_count']:,} rows")
    except Exception as e:
        print(f"❌ Failed to load datasets: {e}")
        return False
    
    # Step 3: Test filter loading for large dataset
    print("\n3️⃣ Testing filter loading (user's main issue)...")
    dataset_name = "dev_daily_prices_tiingo"
    try:
        # Get schema
        response = requests.get(f"{base_url}/api/eda/datasets/{dataset_name}/schema", timeout=10)
        schema = response.json()
        columns = schema['columns']
        print(f"✅ Schema loaded: {len(columns)} columns")
        
        # Test column values (this should work with Ray)
        test_column = 'symbol'  # categorical column
        start_time = time.time()
        response = requests.get(f"{base_url}/api/eda/datasets/{dataset_name}/columns/{test_column}/values?limit=10", timeout=15)
        end_time = time.time()
        
        if response.status_code == 200:
            data = response.json()
            ray_powered = data.get('ray_powered', False)
            print(f"✅ Column values loaded in {end_time-start_time:.2f}s (Ray: {ray_powered})")
            print(f"   📈 Data type: {data.get('data_type', 'unknown')}")
            if 'values' in data:
                print(f"   🔝 Top values: {[v['value'] for v in data['values'][:3]]}")
        else:
            print(f"❌ Column values failed: {response.status_code}")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Filter loading test failed: {e}")
        return False
    
    # Step 4: Test multiple columns for visualizations
    print("\n4️⃣ Testing column distribution visualization loading...")
    test_columns = ['symbol', 'volume', 'close']
    results = []
    
    for col in test_columns:
        try:
            start_time = time.time()
            response = requests.get(f"{base_url}/api/eda/datasets/{dataset_name}/columns/{col}/values?limit=5", timeout=15)
            end_time = time.time()
            
            if response.status_code == 200:
                data = response.json()
                results.append({
                    'column': col,
                    'time': end_time - start_time,
                    'ray_powered': data.get('ray_powered', False),
                    'data_type': data.get('data_type', 'unknown')
                })
                print(f"   ✅ {col}: {data.get('data_type', 'unknown')} ({end_time-start_time:.2f}s, Ray: {data.get('ray_powered', False)})")
            else:
                print(f"   ❌ {col}: Failed ({response.status_code})")
                
        except Exception as e:
            print(f"   ❌ {col}: Error - {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    
    if len(results) == len(test_columns):
        ray_count = sum(1 for r in results if r['ray_powered'])
        avg_time = sum(r['time'] for r in results) / len(results)
        print(f"✅ All {len(test_columns)} columns loaded successfully")
        print(f"✅ Ray acceleration: {ray_count}/{len(test_columns)} columns")
        print(f"✅ Average response time: {avg_time:.2f}s")
        
        if avg_time < 2.0:
            print("🎉 PERFORMANCE EXCELLENT: Sub-2s response times achieved!")
        elif avg_time < 5.0:
            print("✅ PERFORMANCE GOOD: Under 5s response times")
        else:
            print("⚠️ PERFORMANCE NEEDS IMPROVEMENT: >5s response times")
            
        return True
    else:
        failed_count = len(test_columns) - len(results)
        print(f"❌ {failed_count} columns failed to load")
        print("🔧 User will see 'No filter data available' and missing visualizations")
        return False

if __name__ == "__main__":
    success = test_user_scenario()
    print(f"\n{'🎉 USER EXPERIENCE: WORKING' if success else '❌ USER EXPERIENCE: BROKEN'}")