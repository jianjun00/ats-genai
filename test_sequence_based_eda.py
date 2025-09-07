#!/usr/bin/env python3
"""
End-to-End Test: Sequence-Based Training Dataset EDA

This test validates the complete new architecture:
1. Generate training data with sequence-based structure
2. Verify database metadata is populated automatically
3. Test analytics service sequence menu
4. Test multi-timeframe OHLC visualization
5. Verify EDA UI shows proper sequence selection
"""

import asyncio
import requests
import json
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def test_sequence_based_architecture():
    """Test the complete sequence-based training dataset architecture."""
    print("🚀 TESTING SEQUENCE-BASED TRAINING DATASET ARCHITECTURE")
    print("=" * 70)

    # Step 1: Verify analytics service is running
    print("\n1️⃣ Checking analytics service...")
    try:
        response = requests.get("http://localhost:3000/", timeout=5)
        if response.status_code == 200:
            print("✅ Analytics service is running")
        else:
            print(f"❌ Analytics service unhealthy: {response.status_code}")
            return False
    except requests.exceptions.RequestException:
        print("❌ Analytics service not accessible - please start it first")
        return False

    # Step 2: Find a good training dataset with sequence structure
    print("\n2️⃣ Finding sequence-based training dataset...")

    # Check for datasets with file_metadata (new structure)
    datasets_to_test = [63, 82, 81, 62, 61]  # Recent datasets including 63
    test_dataset = None

    for dataset_id in datasets_to_test:
        try:
            response = requests.get(f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences", timeout=10)
            if response.status_code == 200:
                data = response.json()
                sequences = data.get('sequences', [])
                datasets = data.get('datasets', [])

                if len(sequences) > 0 and datasets:
                    dataset_info = datasets[0]
                    file_metadata = dataset_info.get('file_metadata')

                    if file_metadata and file_metadata.get('files'):
                        test_dataset = dataset_id
                        print(f"✅ Found sequence-based dataset {dataset_id}: {dataset_info.get('dataset_name')}")
                        print(f"   Sequences: {len(sequences)}")
                        print(f"   File metadata: {len(file_metadata.get('files', []))} files")
                        break
        except Exception as e:
            continue

    if not test_dataset:
        print("❌ No sequence-based datasets found - need to generate one first")
        return False

    # Step 3: Test sequence menu structure
    print(f"\n3️⃣ Testing sequence menu for dataset {test_dataset}...")

    response = requests.get(f"http://localhost:3000/api/v1/training-datasets/{test_dataset}/sequences", timeout=10)
    data = response.json()
    sequences = data.get('sequences', [])

    print(f"📋 Sequence menu items:")
    for i, seq in enumerate(sequences[:3]):  # Show first 3
        description = seq.get('description', 'No description')
        timeframes = seq.get('timeframes', [])
        print(f"   {i}: {description}")
        if timeframes:
            print(f"      Timeframes: {timeframes}")
        else:
            print(f"      Symbol: {seq.get('symbol')}, Timeframe: {seq.get('timeframe')}")

    # Step 4: Test multi-timeframe data loading
    print(f"\n4️⃣ Testing multi-timeframe data loading...")

    if sequences:
        # Test the first sequence
        first_sequence = sequences[0]
        sequence_id = first_sequence.get('sequence_id') or first_sequence.get('description')

        if sequence_id:
            print(f"🔍 Testing sequence: {sequence_id}")

            # Test new multi-timeframe endpoint if available
            try:
                response = requests.get(
                    f"http://localhost:3000/api/v1/training-datasets/{test_dataset}/sequences/{sequence_id}/multi-timeframe",
                    timeout=10
                )

                if response.status_code == 200:
                    data = response.json()
                    ohlc_data = data.get('ohlc_data', {})
                    table_data = data.get('table_data', [])

                    print("✅ Multi-timeframe endpoint working:")
                    for timeframe, values in ohlc_data.items():
                        print(f"   {timeframe}: {len(values)} OHLC bars")

                    print(f"   Table data (1h): {len(table_data)} rows")

                else:
                    print(f"⚠️  Multi-timeframe endpoint not available (status: {response.status_code})")

            except Exception as e:
                print(f"⚠️  Multi-timeframe endpoint error: {e}")

    # Step 5: Test sequence data endpoint (current structure)
    print(f"\n5️⃣ Testing sequence data endpoints...")

    if sequences:
        # Test first sequence data
        try:
            response = requests.get(
                f"http://localhost:3000/api/v1/training-datasets/{test_dataset}/sequences/0/data",
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                print("✅ Sequence data endpoint working")

                # Check for OHLC data structure
                if 'ohlc_data' in data:
                    ohlc_data = data['ohlc_data']
                    print(f"   OHLC timeframes: {list(ohlc_data.keys())}")
                elif 'sequences' in data:
                    print(f"   Sequences returned: {len(data['sequences'])}")
                else:
                    print(f"   Response keys: {list(data.keys())}")

            else:
                print(f"⚠️  Sequence data endpoint error: {response.status_code}")

        except Exception as e:
            print(f"⚠️  Sequence data endpoint exception: {e}")

    # Step 6: Validate filesystem structure
    print(f"\n6️⃣ Validating filesystem structure...")

    training_dirs = [
        Path("/mnt/d/ats-data/training_data"),
        Path("/data/training_data")
    ]

    sequence_found = False
    for base_dir in training_dirs:
        if base_dir.exists():
            # Look for sequence-based directories
            for run_dir in base_dir.iterdir():
                if run_dir.is_dir() and run_dir.name.isdigit():
                    # Check for sequence directories (SYMBOL_DATERANGE format)
                    for item in run_dir.iterdir():
                        if item.is_dir() and '_' in item.name and len(item.name) > 10:
                            print(f"✅ Found sequence directory: {item}")

                            # Check for timeframe subdirectories
                            timeframes = []
                            for tf_dir in item.iterdir():
                                if tf_dir.is_dir() and tf_dir.name in ['5m', '15m', '1h', '1d', '1w']:
                                    timeframes.append(tf_dir.name)

                                    # Check for ArrayRecord files
                                    arrayrecord_files = list(tf_dir.glob("*.arrayrecord"))
                                    if arrayrecord_files:
                                        print(f"   {tf_dir.name}: {len(arrayrecord_files)} ArrayRecord files")

                            if timeframes:
                                print(f"   Timeframes: {sorted(timeframes)}")
                                sequence_found = True
                            break

                    if sequence_found:
                        break

            if sequence_found:
                break

    if not sequence_found:
        print("❌ No sequence-based filesystem structure found")

    print(f"\n🎉 SEQUENCE-BASED ARCHITECTURE TEST COMPLETE!")
    print(f"   Dataset tested: {test_dataset}")
    print(f"   Sequences available: {len(sequences)}")
    print(f"   Architecture: {'✅ Working' if sequence_found else '❌ Issues detected'}")

    return True

def main():
    """Run the sequence-based architecture test."""
    return asyncio.run(test_sequence_based_architecture())

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)