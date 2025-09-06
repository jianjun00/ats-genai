#!/usr/bin/env python3
"""
Debug the analytics service sequence data API to understand why only 1 sequence
is returned when the dataset claims 3216 sequences.

This script tests the analytics service API endpoints directly.
"""

import asyncio
import json

async def debug_analytics_sequence_api():
    """Debug the analytics service sequence data API."""
    
    print("🔍 DEBUGGING ANALYTICS SERVICE SEQUENCE DATA API")
    print("=" * 60)
    
    # Use curl to test the API endpoints since we don't have aiohttp
    import subprocess
    
    def run_curl(url, description):
        """Run curl command and return parsed JSON response."""
        print(f"\n🔍 {description}")
        print(f"URL: {url}")
        
        try:
            result = subprocess.run(['curl', '-s', url], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    return data
                except json.JSONDecodeError:
                    print(f"❌ Invalid JSON response: {result.stdout[:500]}")
                    return None
            else:
                print(f"❌ Curl failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print("❌ Request timed out")
            return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    # Test 1: Get training datasets list
    datasets_data = run_curl(
        'http://localhost:3000/api/v1/training-datasets',
        'Get training datasets list'
    )
    
    if not datasets_data or 'datasets' not in datasets_data:
        print("❌ Cannot get training datasets - stopping debug")
        return
    
    datasets = datasets_data['datasets']
    print(f"✅ Found {len(datasets)} training datasets")
    
    # Find dataset 58 (our test dataset)
    target_dataset = None
    for dataset in datasets:
        if dataset['id'] == 58:
            target_dataset = dataset
            break
    
    if not target_dataset:
        print("❌ Dataset 58 not found - using first available dataset")
        target_dataset = datasets[0]
    
    dataset_id = target_dataset['id']
    expected_sequences = target_dataset.get('total_sequences', 0)
    
    print(f"\n📊 Testing Dataset ID: {dataset_id}")
    print(f"📊 Dataset name: {target_dataset['dataset_name']}")
    print(f"📊 Expected sequences: {expected_sequences}")
    print(f"📊 Symbols: {target_dataset['symbols']}")
    
    # Test 2: Get sequences for this dataset
    sequences_data = run_curl(
        f'http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences',
        'Get sequences for dataset'
    )
    
    if not sequences_data:
        print("❌ Cannot get sequences data")
        return
    
    sequences = sequences_data.get('sequences', [])
    actual_count = sequences_data.get('total_count', 0)
    
    print(f"✅ Sequences API Response:")
    print(f"   - Sequences returned: {len(sequences)}")
    print(f"   - Total count reported: {actual_count}")
    print(f"   - Expected from metadata: {expected_sequences}")
    
    # This is the key issue!
    if expected_sequences > actual_count:
        print(f"\n❌ PROBLEM CONFIRMED:")
        print(f"   Expected {expected_sequences} sequences")
        print(f"   Only {actual_count} sequences available")
        print(f"   Missing {expected_sequences - actual_count} sequences")
        
        # Check if this is a data generation issue or API issue
        print(f"\n🔍 Investigating root cause...")
        
        # Check the dataset metadata
        datasets_info = sequences_data.get('datasets', [])
        if datasets_info:
            dataset_info = datasets_info[0]
            print(f"   Dataset path: {dataset_info.get('dataset_path', 'N/A')}")
            print(f"   Symbol files: {dataset_info.get('symbol_files', {})}")
    
    # Test 3: If we have sequences, test sequence data retrieval
    if sequences:
        first_sequence = sequences[0]
        sequence_id = first_sequence['id']
        
        print(f"\n🔍 Testing sequence data retrieval for sequence {sequence_id}")
        print(f"   Sequence details: {json.dumps(first_sequence, indent=2)}")
        
        # Test sequence data endpoint
        sequence_data = run_curl(
            f'http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/data',
            'Get sequence OHLC data'
        )
        
        if sequence_data:
            print(f"✅ Sequence data API Response:")
            print(f"   Response keys: {list(sequence_data.keys())}")
            
            # Look for OHLC data
            ohlc_keys = [key for key in sequence_data.keys() 
                        if any(x in key.lower() for x in ['ohlc', 'data', 'sequence'])]
            
            if ohlc_keys:
                print(f"   OHLC-related keys found: {ohlc_keys}")
                for key in ohlc_keys:
                    value = sequence_data[key]
                    print(f"     {key}: {type(value)} - {len(value) if hasattr(value, '__len__') else 'N/A'}")
            else:
                print(f"   ❌ No OHLC data found in response")
                print(f"   Available keys: {list(sequence_data.keys())}")
    
    # Test 4: Check if there are other sequence-related endpoints
    print(f"\n🔍 Testing additional endpoints...")
    
    additional_endpoints = [
        f'/api/v1/training-datasets/{dataset_id}/sequences/0/ohlc',
        f'/api/v1/training-datasets/{dataset_id}/sequences/0/features',
        f'/api/ray-analytics/{dataset_id}',
        f'/api/v1/training-datasets/{dataset_id}/metadata',
    ]
    
    for endpoint in additional_endpoints:
        url = f'http://localhost:3000{endpoint}'
        data = run_curl(url, f'Test endpoint {endpoint}')
        
        if data:
            print(f"   ✅ {endpoint}: {type(data)} with keys {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
        else:
            print(f"   ❌ {endpoint}: Failed or no data")
    
    # Test 5: Check analytics service logs for errors
    print(f"\n🔍 Checking analytics service logs...")
    
    try:
        result = subprocess.run(['docker', 'logs', '--tail', '20', 'ats-dev-analytics'], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            logs = result.stdout
            
            # Look for relevant log lines
            relevant_lines = []
            for line in logs.split('\n'):
                if any(keyword in line.lower() for keyword in 
                      ['sequence', 'training', 'dataset', 'error', 'exception', 'arrayrecord']):
                    relevant_lines.append(line)
            
            if relevant_lines:
                print(f"   📋 Relevant log lines:")
                for line in relevant_lines[-10:]:  # Last 10 relevant lines
                    print(f"     {line}")
            else:
                print(f"   📋 No relevant log lines found")
                print(f"   📋 Recent logs:")
                for line in logs.split('\n')[-5:]:  # Last 5 lines
                    if line.strip():
                        print(f"     {line}")
        else:
            print(f"   ❌ Error getting logs: {result.stderr}")
            
    except Exception as e:
        print(f"   ❌ Error checking logs: {e}")
    
    print(f"\n🔍 SUMMARY")
    print("=" * 60)
    print(f"1. Dataset metadata shows {expected_sequences} sequences")
    print(f"2. API only returns {actual_count} sequences")
    print(f"3. This explains the 'No sequence data available' UI issue")
    print(f"4. Root cause: Sequence data generation or storage problem")
    print(f"5. Need to investigate ArrayRecord file reading in analytics service")

if __name__ == "__main__":
    asyncio.run(debug_analytics_sequence_api())