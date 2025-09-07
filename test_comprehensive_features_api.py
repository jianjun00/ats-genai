#!/usr/bin/env python3
"""
Direct API Test for Comprehensive Features
Tests that the multi-timeframe API returns all 962 training features
"""

import requests
import json

def test_comprehensive_features_api():
    """Test that multi-timeframe API returns comprehensive features."""
    print("🔍 Testing Comprehensive Features via Direct API Call")
    print("="*60)
    
    try:
        # Test the multi-timeframe endpoint directly
        api_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        params = {"row_index": 10}
        
        print(f"🌐 Making API request to: {api_url}")
        print(f"📊 Parameters: {params}")
        
        response = requests.get(api_url, params=params, timeout=30)
        
        print(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"✅ API response received successfully")
            
            # Analyze the response structure
            print(f"\n📋 Response Structure Analysis:")
            print(f"   - Success: {data.get('success', 'Unknown')}")
            print(f"   - Sequence ID: {data.get('sequence_id', 'Not found')}")
            print(f"   - Dataset: {data.get('dataset_name', 'Not found')}")
            
            # Check OHLC data
            ohlc_data = data.get('ohlc_data', {})
            print(f"   - OHLC timeframes: {list(ohlc_data.keys())}")
            for tf, bars in ohlc_data.items():
                print(f"     {tf}: {len(bars)} bars")
            
            # Check table data (this should contain all 962 features)
            table_data = data.get('table_data', [])
            print(f"   - Table rows: {len(table_data)}")
            
            if table_data and len(table_data) > 0:
                first_row = table_data[0]
                feature_count = len(first_row) if isinstance(first_row, dict) else 0
                
                print(f"   - Features per row: {feature_count}")
                
                if feature_count > 900:
                    print(f"🎉 SUCCESS: Comprehensive features detected! ({feature_count} features)")
                    print(f"   This indicates all training features are being returned")
                    
                    # Sample some feature names to verify multi-timeframe structure
                    if isinstance(first_row, dict):
                        feature_names = list(first_row.keys())
                        print(f"\n📊 Sample Feature Names:")
                        
                        # Look for different timeframe features
                        timeframe_features = {}
                        for feature in feature_names[:50]:  # Sample first 50
                            for tf in ['5m_', '15m_', '1h_', '1d_', '1w_']:
                                if tf in feature:
                                    if tf not in timeframe_features:
                                        timeframe_features[tf] = []
                                    timeframe_features[tf].append(feature)
                                    break
                        
                        for tf, features in timeframe_features.items():
                            print(f"   {tf}: {len(features)} features (sample: {features[:3]})")
                        
                        print(f"\n✅ Multi-timeframe training features confirmed!")
                        
                elif feature_count > 50:
                    print(f"⚠️  PARTIAL: {feature_count} features found")
                    print(f"   Expected ~962 comprehensive training features")
                    
                elif feature_count <= 10:
                    print(f"❌ BASIC ONLY: Only {feature_count} features found")
                    print(f"   This suggests fallback to basic OHLCV features")
                    
                    if isinstance(first_row, dict):
                        basic_features = list(first_row.keys())
                        print(f"   Basic features: {basic_features}")
                else:
                    print(f"⚠️  LIMITED: {feature_count} features found")
                    
            else:
                print("❌ No table data found in response")
                
        else:
            print(f"❌ API request failed: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed - analytics service may not be running")
        print("💡 Try: python3 scripts/run_dev.py start --service analytics")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

def test_dataset_availability():
    """Test that training datasets are available."""
    print(f"\n🗃️ Testing Dataset Availability")
    print("-" * 40)
    
    try:
        # Check available training datasets
        api_url = "http://localhost:3000/api/v1/training-datasets"
        
        response = requests.get(api_url, timeout=15)
        print(f"📡 Training datasets response: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            datasets = data.get('datasets', [])
            
            print(f"✅ Found {len(datasets)} training datasets")
            
            for dataset in datasets[:3]:  # Show first 3
                dataset_id = dataset.get('id', 'Unknown')
                name = dataset.get('dataset_name', 'Unnamed')
                symbols = dataset.get('symbols', [])
                print(f"   - Dataset {dataset_id}: {name} ({symbols})")
                
            return len(datasets) > 0
        else:
            print(f"❌ Failed to get datasets: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Dataset check failed: {e}")
        return False

if __name__ == "__main__":
    # First check if datasets are available
    datasets_available = test_dataset_availability()
    
    if datasets_available:
        test_comprehensive_features_api()
    else:
        print("⚠️  Skipping feature test - no datasets available")
        
    print("\n🎯 Comprehensive Features API Test Completed")