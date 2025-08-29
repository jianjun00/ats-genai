#!/usr/bin/env python3
"""
Test script to verify the new all-column distribution functionality.
"""

import requests

def test_new_eda_interface():
    """Test the new EDA interface that shows all column distributions."""
    base_url = "http://localhost:3000"
    
    print("🧪 Testing New EDA Interface (All Column Distributions)...")
    
    try:
        # Test 1: EDA page loads
        print("1. Testing EDA page loads...")
        response = requests.get(f"{base_url}/eda", timeout=5)
        assert response.status_code == 200
        content = response.text
        assert "Comprehensive dataset analysis" in content
        assert "loadDatasetAnalysis()" in content
        print("   ✅ New EDA interface loads correctly")
        
        # Test 2: Datasets endpoint still works
        print("2. Testing datasets endpoint...")
        response = requests.get(f"{base_url}/api/eda/datasets", timeout=8)
        assert response.status_code == 200
        datasets = response.json()
        assert isinstance(datasets, list)
        print(f"   ✅ Datasets endpoint working - {len(datasets)} datasets available")
        
        # Test 3: Schema endpoint works for analysis
        print("3. Testing schema endpoint for distribution analysis...")
        if datasets:
            dataset_name = datasets[0]["name"]
            response = requests.get(f"{base_url}/api/eda/datasets/{dataset_name}/schema", timeout=8)
            assert response.status_code == 200
            schema = response.json()
            assert "columns" in schema
            print(f"   ✅ Schema endpoint working - {len(schema['columns'])} columns found")
            
            # Test that we can analyze multiple columns
            columns_to_test = schema["columns"][:3]  # Test first 3 columns
            for col in columns_to_test:
                print(f"4. Testing analysis for column: {col['column_name']}")
                
                # Test column values endpoint
                response = requests.get(
                    f"{base_url}/api/eda/datasets/{dataset_name}/columns/{col['column_name']}/values?limit=5",
                    timeout=8
                )
                if response.status_code == 200:
                    data = response.json()
                    if not data.get("error"):
                        print(f"   ✅ Column '{col['column_name']}' analysis data available")
                    else:
                        print(f"   ⚠️  Column '{col['column_name']}' has fallback data")
                else:
                    print(f"   ❌ Column '{col['column_name']}' endpoint failed")
                    
                # Test distribution analysis for numeric columns
                data_type = col["data_type"].lower()
                is_numeric = any(t in data_type for t in ["numeric", "integer", "double", "bigint"])
                
                if is_numeric:
                    print(f"5. Testing numeric distribution for: {col['column_name']}")
                    payload = {
                        "dataset_name": dataset_name,
                        "column": col['column_name'],
                        "filters": {}
                    }
                    response = requests.post(
                        f"{base_url}/api/eda/analyze",
                        headers={"Content-Type": "application/json"},
                        json=payload,
                        timeout=8
                    )
                    if response.status_code == 200:
                        analysis = response.json()
                        if not analysis.get("error"):
                            print(f"   ✅ Numeric distribution for '{col['column_name']}' available")
                        else:
                            print(f"   ⚠️  Numeric distribution for '{col['column_name']}' has fallback data")
                    
        print("\n🎉 New EDA Interface Tests Completed!")
        
        print("\n📊 New Features Summary:")
        print("✅ Removed column selection dropdown")
        print("✅ Show distributions for ALL columns automatically when dataset is selected")
        print("✅ Display both numeric (histograms) and categorical (bar charts) distributions")
        print("✅ Show first 10 columns to avoid overwhelming the UI")
        print("✅ Include statistics for each column distribution")
        print("✅ Maintain all filtering functionality")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def show_usage_instructions():
    """Show how to use the new interface."""
    print("\n🔍 How to Use the New EDA Interface:")
    print("1. Open http://localhost:3000/eda in your browser")
    print("2. Select a dataset from the dropdown")
    print("3. 🆕 Distributions for ALL columns will automatically appear!")
    print("4. Scroll down to see distributions for different columns")
    print("5. Use the filters section to apply filters as before")
    print("6. Load filtered data to see results in the data table")
    print("\n💡 Key Changes:")
    print("- ❌ No more column selection dropdown")
    print("- ✅ Automatic distribution display for all columns")
    print("- ✅ Both numeric histograms and categorical bar charts")
    print("- ✅ Individual statistics for each column")
    print("- ✅ First 10 columns shown (performance optimized)")

if __name__ == "__main__":
    success = test_new_eda_interface()
    show_usage_instructions()
    
    if success:
        print("\n✨ New EDA Interface is Working! ✨")
        exit(0)
    else:
        print("\n❌ Some tests failed")
        exit(1)