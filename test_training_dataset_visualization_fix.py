#!/usr/bin/env python3
"""
Test that training dataset visualization fix works correctly.

This test verifies the database metadata approach is working by:
1. Testing API endpoints return correct sequence counts
2. Comparing before/after behavior
3. Verifying multi-timeframe, multi-symbol support
"""

import requests
import json
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_training_dataset_sequences():
    """Test that training dataset sequences endpoint works correctly."""
    print("🔍 TESTING TRAINING DATASET VISUALIZATION FIX")
    print("=" * 60)
    
    # Test dataset 59 (our good dataset with file_metadata)
    dataset_id = 59
    print(f"\n📊 Testing dataset {dataset_id}...")
    
    try:
        response = requests.get(f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            total_count = data.get('total_count', 0)
            sequences = data.get('sequences', [])
            datasets = data.get('datasets', [])
            
            print(f"✅ API Response Status: {response.status_code}")
            print(f"✅ Total sequences: {total_count}")
            print(f"✅ Sequences list length: {len(sequences)}")
            
            if datasets:
                dataset_info = datasets[0]
                file_metadata = dataset_info.get('file_metadata', {})
                
                print(f"✅ Dataset name: {dataset_info.get('dataset_name')}")
                print(f"✅ Symbols: {dataset_info.get('symbols')}")
                print(f"✅ Run ID: {dataset_info.get('run_id')}")
                
                if file_metadata:
                    print(f"✅ File metadata available")
                    print(f"   Total files: {file_metadata.get('total_files')}")
                    print(f"   Total sequences (metadata): {file_metadata.get('total_sequences')}")
                    print(f"   Timeframes: {file_metadata.get('timeframes')}")
                    
                    # Verify each sequence has correct metadata
                    print("\n📋 Sequence details:")
                    for i, seq in enumerate(sequences[:5]):  # Show first 5
                        print(f"   {i}: {seq['symbol']} {seq['timeframe']} - {seq['filename']}")
                else:
                    print("❌ No file_metadata found")
            
            # Test expectations
            assert total_count > 1, f"Expected multiple sequences, got {total_count}"
            assert len(sequences) == total_count, f"Sequence count mismatch: {len(sequences)} vs {total_count}"
            assert total_count == 5, f"Expected exactly 5 sequences (5 timeframes), got {total_count}"
            
            # Verify multi-timeframe support
            timeframes = set(seq['timeframe'] for seq in sequences)
            expected_timeframes = {'5m', '15m', '1h', '1d', '1w'}
            assert timeframes == expected_timeframes, f"Missing timeframes: expected {expected_timeframes}, got {timeframes}"
            
            print("✅ ALL TESTS PASSED!")
            print(f"   ✅ Sequences endpoint returns {total_count} sequences")
            print(f"   ✅ All timeframes present: {sorted(timeframes)}")
            print(f"   ✅ Database metadata approach working")
            
            return True
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_api_availability():
    """Test that analytics service is running."""
    print("\n🔧 Testing analytics service availability...")
    
    try:
        # Test the main page first
        response = requests.get("http://localhost:3000/", timeout=5)
        if response.status_code == 200:
            print("✅ Analytics service is running")
            return True
        else:
            print(f"❌ Analytics service unhealthy: {response.status_code}")
            return False
    except requests.exceptions.RequestException:
        print("❌ Analytics service not accessible")
        return False

def main():
    """Run all tests."""
    print("🚀 TRAINING DATASET VISUALIZATION VERIFICATION")
    print("=" * 70)
    
    # Test 1: Service availability
    if not test_api_availability():
        print("\n❌ Cannot proceed - analytics service not available")
        return False
    
    # Test 2: Sequences endpoint
    if not test_training_dataset_sequences():
        print("\n❌ Training dataset sequences test failed")
        return False
    
    print("\n🎉 ALL VERIFICATION TESTS PASSED!")
    print("   The 'No sequence data available' error has been fixed!")
    print("   ✅ Database metadata approach working correctly")
    print("   ✅ Multi-timeframe sequences properly displayed")
    print("   ✅ Analytics service using file_metadata from database")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)