#!/usr/bin/env python3
"""
FOCUSED AAPL TRAINING DATA TEST - Based on Debug Findings

Based on the comprehensive debugging, we found:
1. ✅ AAPL data exists (16,635 records in July 2025)
2. ✅ Database connectivity works
3. ✅ AAPL instrument resolution works (ID: 31)
4. ❌ API usage issues in data managers

This script tests training data generation with the correct APIs.
"""

import asyncio
import sys
import os
from datetime import datetime, date
from pathlib import Path

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

# Set environment to skip gin loading
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

from core.platform.config.environment import EnvironmentType


class MinimalEnvironment:
    """Minimal environment for testing."""
    
    def __init__(self):
        self.environment_type = EnvironmentType.INTEGRATION
        self.db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"
        
    def get_database_url(self):
        return self.db_url
        
    def get_table_name(self, base_name):
        return f"intg_{base_name}"


async def test_aapl_training_data_generation():
    """Test AAPL training data generation with minimal, focused approach."""
    
    print("🎯 FOCUSED AAPL TRAINING DATA GENERATION TEST")
    print("=" * 60)
    
    # Create minimal environment
    env = MinimalEnvironment()
    print(f"✅ Environment: {env.environment_type}")
    print(f"✅ Database: {env.get_database_url()}")
    
    # Test 1: Direct minute bar file reading
    print(f"\n📁 TEST 1: Direct minute bar file access")
    
    import pandas as pd
    july_file = Path("/data/minute-bars/firstrate/A/AAPL/2025/07/AAPL_2025_07.parquet")
    
    if july_file.exists():
        df = pd.read_parquet(july_file)
        print(f"✅ AAPL July 2025 data: {len(df):,} records")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        # Filter for July 1st data
        july_1_data = df[df['timestamp'].dt.date == date(2025, 7, 1)]
        print(f"✅ July 1st data: {len(july_1_data)} records")
        
        if len(july_1_data) > 0:
            print(f"   Sample: {july_1_data.iloc[0].to_dict()}")
        else:
            print("❌ No data for July 1st specifically")
            
    else:
        print(f"❌ File not found: {july_file}")
        return
        
    # Test 2: FileBasedMinuteManager query
    print(f"\n⚙️ TEST 2: FileBasedMinuteManager query")
    
    from infrastructure.storage.file_based_minute_manager import FileBasedMinuteManager
    
    minute_manager = FileBasedMinuteManager("/data/minute-bars")
    print(f"✅ FileBasedMinuteManager initialized")
    
    # Query AAPL data for July 1st
    start_datetime = datetime(2025, 7, 1, 0, 0, 0)
    end_datetime = datetime(2025, 7, 1, 23, 59, 59)
    
    try:
        minute_data = await minute_manager.query_minute_data(
            symbol="AAPL",
            start_time=start_datetime,
            end_time=end_datetime
        )
        
        if minute_data is not None and len(minute_data) > 0:
            print(f"✅ Query successful: {len(minute_data)} records")
            print(f"   Sample: {minute_data.iloc[0].to_dict()}")
        else:
            print(f"⚠️ Query returned no data")
            
    except Exception as e:
        print(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
        
    # Test 3: FileBasedMinuteMarketDataManager
    print(f"\n⚙️ TEST 3: FileBasedMinuteMarketDataManager")
    
    from domains.market_data.services.core.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
    
    try:
        market_data_manager = FileBasedMinuteMarketDataManager(env, "/data/minute-bars")
        print(f"✅ FileBasedMinuteMarketDataManager initialized")
        
        # Query AAPL data using the market data manager API
        batch_data = await market_data_manager.get_minute_ohlc_batch(
            symbols=["AAPL"],
            start=start_datetime,
            end=end_datetime,
            timeframe_minutes=1
        )
        
        if "AAPL" in batch_data and len(batch_data["AAPL"]) > 0:
            aapl_data = batch_data["AAPL"]
            print(f"✅ Batch query successful: {len(aapl_data)} records")
            print(f"   Sample: {aapl_data.iloc[0].to_dict()}")
        else:
            print(f"⚠️ Batch query returned no AAPL data")
            print(f"   Batch results: {list(batch_data.keys())}")
            
    except Exception as e:
        print(f"❌ Market data manager test failed: {e}")
        import traceback
        traceback.print_exc()
        
    # Test 4: Run actual training data generation for one day
    print(f"\n🚀 TEST 4: Actual training data generation (1 day)")
    
    try:
        # Use the working training data runner command but for just 1 day
        import subprocess
        
        training_cmd = [
            "python3", "src/domains/ml/services/training_data/runners/training_data_callback_runner.py",
            "--symbols", "AAPL",
            "--start-date", "2025-07-01", 
            "--end-date", "2025-07-01",
            "--environment", "intg",
            "--storage-format", "arrayrecord",
            "--output-dir", "/data/training_data",
            "--debug",
            "--gin-config", "config/training_data.gin",
            "--base-duration", "60m"
        ]
        
        env_vars = {
            **os.environ,
            "ENVIRONMENT_TYPE": "intg",
            "DB_HOST": "localhost",
            "DB_PORT": "4432", 
            "DB_USER": "postgres",
            "DB_PASSWORD": "intg_password",
            "DB_NAME": "intg_db",
            "PYTHONPATH": "src"
        }
        
        print(f"🔄 Running training data generation...")
        print(f"   Command: {' '.join(training_cmd)}")
        
        result = subprocess.run(
            training_cmd,
            cwd="/home/jianjun/ats-genai-admin",
            env=env_vars,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        print(f"\n📊 TRAINING DATA GENERATION RESULTS:")
        print(f"   Return code: {result.returncode}")
        
        if result.returncode == 0:
            print(f"✅ Training data generation succeeded!")
        else:
            print(f"❌ Training data generation failed")
            
        # Show last 20 lines of output
        if result.stdout:
            stdout_lines = result.stdout.split('\n')
            print(f"\n📝 Last 20 lines of stdout:")
            for line in stdout_lines[-20:]:
                if line.strip():
                    print(f"   {line}")
                    
        if result.stderr:
            stderr_lines = result.stderr.split('\n')
            print(f"\n🚨 Last 10 lines of stderr:")
            for line in stderr_lines[-10:]:
                if line.strip():
                    print(f"   {line}")
                    
        # Check if training data files were created
        training_data_dir = Path("/data/training_data")
        if training_data_dir.exists():
            recent_datasets = sorted([d for d in training_data_dir.iterdir() if d.is_dir() and d.name.startswith("dataset_")], 
                                   key=lambda x: x.stat().st_mtime, reverse=True)
            
            if recent_datasets:
                latest_dataset = recent_datasets[0]
                print(f"\n📁 Latest dataset: {latest_dataset.name}")
                
                # Check contents
                arrayrecord_files = list(latest_dataset.rglob("*.arrayrecord"))
                metadata_files = list(latest_dataset.glob("*.json"))
                
                print(f"   ArrayRecord files: {len(arrayrecord_files)}")
                print(f"   Metadata files: {len(metadata_files)}")
                
                for ar_file in arrayrecord_files:
                    file_size = ar_file.stat().st_size
                    print(f"     📄 {ar_file.name}: {file_size:,} bytes")
                    
                if metadata_files:
                    import json
                    for meta_file in metadata_files:
                        if meta_file.name.endswith('metadata.json'):
                            try:
                                with open(meta_file) as f:
                                    metadata = json.load(f)
                                print(f"     📋 Status: {metadata.get('status', 'unknown')}")
                                print(f"     📋 Intervals: {metadata.get('actual_intervals_processed', 'unknown')}")
                            except Exception as e:
                                print(f"     ❌ Error reading metadata: {e}")
                                
    except subprocess.TimeoutExpired:
        print(f"❌ Training data generation timed out after 5 minutes")
    except Exception as e:
        print(f"❌ Training data generation test failed: {e}")
        import traceback
        traceback.print_exc()
        
    print(f"\n" + "=" * 60)
    print(f"✅ FOCUSED AAPL TRAINING DATA TEST COMPLETE")


if __name__ == "__main__":
    asyncio.run(test_aapl_training_data_generation())