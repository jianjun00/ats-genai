#!/usr/bin/env python3
"""
Debug script for training dataset sequence data pipeline.

Investigates the complete data flow from ArrayRecord storage files 
to API endpoints to understand why sequence data is not available.
"""

import os
import sys
import asyncio
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

async def debug_sequence_data_pipeline():
    """Debug the complete sequence data pipeline."""
    
    print("🔍 DEBUGGING TRAINING DATASET SEQUENCE DATA PIPELINE")
    print("=" * 60)
    
    # Step 1: Check training data file storage
    print("\n🔍 STEP 1: Check training data file storage")
    print("-" * 40)
    
    training_data_dirs = [
        "/mnt/d/ats-data/training",
        "/mnt/d/ats-data/training_data", 
        "/data/training",
        "/data/training_data"
    ]
    
    for data_dir in training_data_dirs:
        if os.path.exists(data_dir):
            print(f"✅ Found training data directory: {data_dir}")
            
            # List subdirectories (run IDs)
            run_dirs = []
            for item in os.listdir(data_dir):
                item_path = os.path.join(data_dir, item)
                if os.path.isdir(item_path):
                    run_dirs.append(item)
            
            print(f"📁 Run directories found: {sorted(run_dirs)}")
            
            # Check the latest run directory (76 from our training)
            if '76' in run_dirs:
                run_76_path = os.path.join(data_dir, '76')
                print(f"\n📂 Investigating Run 76: {run_76_path}")
                
                # Check timeframe directories
                timeframe_dirs = []
                for item in os.listdir(run_76_path):
                    item_path = os.path.join(run_76_path, item)
                    if os.path.isdir(item_path):
                        timeframe_dirs.append(item)
                
                print(f"⏰ Timeframe directories: {sorted(timeframe_dirs)}")
                
                # Check files in each timeframe
                for timeframe in sorted(timeframe_dirs):
                    timeframe_path = os.path.join(run_76_path, timeframe)
                    files = os.listdir(timeframe_path)
                    total_size = sum(os.path.getsize(os.path.join(timeframe_path, f)) 
                                   for f in files if os.path.isfile(os.path.join(timeframe_path, f)))
                    
                    print(f"  📊 {timeframe}: {len(files)} files, {total_size / 1024 / 1024:.2f} MB")
                    for file in files[:3]:  # Show first 3 files
                        file_path = os.path.join(timeframe_path, file)
                        size = os.path.getsize(file_path) / 1024
                        print(f"    - {file} ({size:.1f} KB)")
                    if len(files) > 3:
                        print(f"    ... and {len(files) - 3} more files")
        else:
            print(f"❌ Training data directory not found: {data_dir}")
    
    # Step 2: Check database records
    print("\n🔍 STEP 2: Check database training dataset records")
    print("-" * 40)
    
    try:
        from core.config.environment import Environment
        
        # Initialize environment for database access
        env = Environment()
        
        import asyncpg
        
        # Connect to database
        conn = await asyncpg.connect(
            host=env.database.host,
            port=env.database.port, 
            user=env.database.user,
            password=env.database.password,
            database=env.database.database
        )
        
        # Query training datasets
        datasets = await conn.fetch("""
            SELECT id, dataset_name, total_sequences, sequence_length, 
                   feature_count, file_size_mb, symbols
            FROM dev_training_dataset 
            ORDER BY id DESC LIMIT 5
        """)
        
        print("📊 Recent training datasets:")
        for dataset in datasets:
            print(f"  ID {dataset['id']}: {dataset['dataset_name']}")
            print(f"    Sequences: {dataset['total_sequences']}, Length: {dataset['sequence_length']}")
            print(f"    Features: {dataset['feature_count']}, Size: {dataset['file_size_mb']} MB")
            print(f"    Symbols: {dataset['symbols']}")
            print()
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Database connection error: {e}")
    
    # Step 3: Test API endpoints directly
    print("\n🔍 STEP 3: Test API endpoints directly")
    print("-" * 40)
    
    import aiohttp
    
    try:
        async with aiohttp.ClientSession() as session:
            
            # Test training datasets endpoint
            async with session.get('http://localhost:3000/api/v1/training-datasets') as resp:
                if resp.status == 200:
                    data = await resp.json()
                    datasets = data.get('datasets', [])
                    print(f"✅ Training datasets API: {len(datasets)} datasets found")
                    
                    if datasets:
                        first_dataset = datasets[0]
                        dataset_id = first_dataset['id']
                        print(f"📊 Testing dataset ID {dataset_id}")
                        
                        # Test sequences endpoint
                        async with session.get(f'http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences') as seq_resp:
                            if seq_resp.status == 200:
                                seq_data = await seq_resp.json()
                                sequences = seq_data.get('sequences', [])
                                total_count = seq_data.get('total_count', 0)
                                
                                print(f"📊 Sequences API response:")
                                print(f"  - Sequences returned: {len(sequences)}")
                                print(f"  - Total count: {total_count}")
                                print(f"  - Expected from metadata: {first_dataset.get('total_sequences', 0)}")
                                
                                if sequences:
                                    first_seq = sequences[0]
                                    print(f"  - First sequence: {first_seq}")
                                    
                                    # Test sequence data endpoint
                                    seq_id = first_seq['id']
                                    async with session.get(f'http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences/{seq_id}/data') as data_resp:
                                        if data_resp.status == 200:
                                            seq_detail = await data_resp.json()
                                            print(f"📊 Sequence data endpoint response:")
                                            print(f"  - Response keys: {list(seq_detail.keys())}")
                                            
                                            # Check if there's actual OHLC data
                                            if 'ohlc_data' in seq_detail:
                                                print(f"  - OHLC data found: {len(seq_detail['ohlc_data'])} records")
                                            elif 'data' in seq_detail:
                                                print(f"  - Data found: {type(seq_detail['data'])}")
                                            else:
                                                print("  ❌ No OHLC data in response")
                                        else:
                                            print(f"❌ Sequence data endpoint failed: {data_resp.status}")
                            else:
                                print(f"❌ Sequences endpoint failed: {seq_resp.status}")
                else:
                    print(f"❌ Training datasets API failed: {resp.status}")
                    
    except Exception as e:
        print(f"❌ API testing error: {e}")
    
    # Step 4: Check ArrayRecord file reading capability
    print("\n🔍 STEP 4: Test ArrayRecord file reading")
    print("-" * 40)
    
    try:
        import array_record
        
        # Try to find and read ArrayRecord files
        for data_dir in training_data_dirs:
            if os.path.exists(data_dir):
                for root, dirs, files in os.walk(data_dir):
                    for file in files:
                        if file.endswith('.arrayrecord'):
                            file_path = os.path.join(root, file)
                            try:
                                print(f"📄 Reading ArrayRecord: {file_path}")
                                
                                # Try to read the file
                                with array_record.ArrayRecordReader(file_path) as reader:
                                    record_count = len(reader)
                                    print(f"  - Records in file: {record_count}")
                                    
                                    if record_count > 0:
                                        # Read first record
                                        first_record = reader[0]
                                        print(f"  - First record type: {type(first_record)}")
                                        print(f"  - First record preview: {str(first_record)[:200]}...")
                                        
                                        # Try to parse as JSON or other format
                                        if isinstance(first_record, bytes):
                                            try:
                                                parsed = json.loads(first_record.decode())
                                                print(f"  - Parsed JSON keys: {list(parsed.keys()) if isinstance(parsed, dict) else 'Not a dict'}")
                                            except:
                                                print("  - Not JSON format")
                                    
                                # Limit to first few files to avoid overwhelming output
                                break
                                        
                            except Exception as e:
                                print(f"  ❌ Error reading {file_path}: {e}")
                
    except ImportError:
        print("❌ array_record library not available")
    except Exception as e:
        print(f"❌ ArrayRecord testing error: {e}")
    
    # Step 5: Check analytics service logs
    print("\n🔍 STEP 5: Check analytics service logs")
    print("-" * 40)
    
    import subprocess
    
    try:
        # Get recent logs from analytics container
        result = subprocess.run(['docker', 'logs', '--tail', '50', 'ats-dev-analytics'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            logs = result.stdout
            print("📋 Recent analytics service logs:")
            
            # Filter for relevant lines
            relevant_lines = []
            for line in logs.split('\n'):
                if any(keyword in line.lower() for keyword in 
                      ['sequence', 'training', 'dataset', 'error', 'exception', 'arrayrecord']):
                    relevant_lines.append(line)
            
            if relevant_lines:
                for line in relevant_lines[-20:]:  # Last 20 relevant lines
                    print(f"  {line}")
            else:
                print("  No sequence/training related logs found")
        else:
            print(f"❌ Error getting logs: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Log checking error: {e}")
    
    print("\n🔍 DEBUGGING SUMMARY")
    print("=" * 60)
    print("1. Check if ArrayRecord files exist and contain data")
    print("2. Verify API endpoints can read ArrayRecord files") 
    print("3. Confirm sequence data structure matches UI expectations")
    print("4. Test complete data flow from files → API → UI")

if __name__ == "__main__":
    asyncio.run(debug_sequence_data_pipeline())