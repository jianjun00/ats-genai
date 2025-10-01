#!/usr/bin/env python3
"""
Test Data Setup for UnifiedMarketDataManager Golden Tests

Extracts a small subset of real FirstRate minute bar data for deterministic testing.

STRATEGY:
- Copy small subset (2 days) of AAPL and TSLA data
- Ensure data covers market open, intraday, and close scenarios
- Store in test directory for isolation from production data
- Create checksums for data integrity validation
"""

import sys
import os
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime, date
import hashlib

# Add src to path
sys.path.append('src')

# Paths
PRODUCTION_DATA_PATH = Path("/mnt/d/ats-data/minute-bars/firstrate")
TEST_DATA_PATH = Path(__file__).parent / "firstrate"
CHECKSUMS_FILE = Path(__file__).parent / "data_checksums.txt"

def setup_test_directories():
    """Create test data directory structure."""
    print("📁 Setting up test directories...")
    
    # Create directory structure matching FirstRate format
    for symbol in ["AAPL", "TSLA"]:
        symbol_dir = TEST_DATA_PATH / symbol[0] / symbol / "2024" / "08"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        print(f"   Created: {symbol_dir}")

def extract_test_data():
    """Extract small subset of real FirstRate data for testing."""
    print("📊 Extracting test data...")
    
    symbols = ["AAPL", "TSLA"]
    test_date = "2024-08"  # August 2024
    
    for symbol in symbols:
        print(f"\n🔍 Processing {symbol}...")
        
        # Source file path (production data)
        source_file = PRODUCTION_DATA_PATH / symbol[0] / symbol / "2024" / "08" / f"{symbol}_2024_08.parquet"
        
        # Destination file path (test data)
        dest_file = TEST_DATA_PATH / symbol[0] / symbol / "2024" / "08" / f"{symbol}_2024_08.parquet"
        
        if not source_file.exists():
            print(f"   ⚠️  Source file not found: {source_file}")
            print(f"   📝 Creating minimal test data for {symbol}...")
            create_minimal_test_data(symbol, dest_file)
            continue
            
        try:
            # Read full month data
            print(f"   📖 Reading: {source_file}")
            df = pd.read_parquet(source_file)
            
            if df.empty:
                print(f"   ⚠️  Empty data file for {symbol}")
                create_minimal_test_data(symbol, dest_file)
                continue
            
            print(f"   📊 Full dataset: {len(df)} rows from {df.index.min()} to {df.index.max()}")
            
            # Extract first 2 trading days for testing (Aug 1-2, 2024)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')
            
            # Filter to first 2 days of August 2024
            start_date = datetime(2024, 8, 1, 9, 30)  # Market open
            end_date = datetime(2024, 8, 2, 16, 0)    # Market close next day
            
            mask = (df.index >= start_date) & (df.index <= end_date)
            test_df = df[mask].copy()
            
            if test_df.empty:
                print(f"   ⚠️  No data in test date range for {symbol}")
                create_minimal_test_data(symbol, dest_file)
                continue
            
            print(f"   ✂️  Filtered to test range: {len(test_df)} rows from {test_df.index.min()} to {test_df.index.max()}")
            
            # Reset timestamp as column for parquet storage
            test_df = test_df.reset_index()
            
            # Save test data
            print(f"   💾 Saving: {dest_file}")
            test_df.to_parquet(dest_file, index=False)
            
            # Verify saved data
            verify_df = pd.read_parquet(dest_file)
            print(f"   ✅ Verified: {len(verify_df)} rows saved")
            
        except Exception as e:
            print(f"   ❌ Error processing {symbol}: {e}")
            print(f"   📝 Creating minimal test data as fallback...")
            create_minimal_test_data(symbol, dest_file)

def create_minimal_test_data(symbol: str, dest_file: Path):
    """Create minimal synthetic test data if real data unavailable."""
    print(f"   🔧 Creating minimal test data for {symbol}...")
    
    # Generate 2 days of minute data (market hours only)
    timestamps = []
    
    # Aug 1, 2024 - full trading day
    base_date = datetime(2024, 8, 1, 9, 30)  # 9:30 AM
    for minute in range(390):  # 6.5 hours * 60 minutes
        timestamps.append(base_date + pd.Timedelta(minutes=minute))
    
    # Aug 2, 2024 - full trading day  
    base_date = datetime(2024, 8, 2, 9, 30)
    for minute in range(390):
        timestamps.append(base_date + pd.Timedelta(minutes=minute))
    
    # Create realistic OHLCV data
    import numpy as np
    np.random.seed(42)  # Deterministic
    
    base_price = 150.0 if symbol == "AAPL" else 250.0
    
    data = []
    current_price = base_price
    
    for timestamp in timestamps:
        # Simulate realistic price movement
        change = np.random.normal(0, 0.1)  # Small random changes
        current_price = max(current_price + change, base_price * 0.9)  # Floor at 90% of base
        
        high = current_price + abs(np.random.normal(0, 0.2))
        low = current_price - abs(np.random.normal(0, 0.2))
        close = current_price + np.random.normal(0, 0.1)
        volume = int(np.random.uniform(1000, 10000))
        
        data.append({
            'timestamp': timestamp,
            'open': round(current_price, 2),
            'high': round(high, 2),
            'low': round(low, 2), 
            'close': round(close, 2),
            'volume': volume
        })
        
        current_price = close  # Next iteration starts from close
    
    df = pd.DataFrame(data)
    
    # Save minimal test data
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest_file, index=False)
    
    print(f"   ✅ Created {len(df)} rows of synthetic test data")

def calculate_checksums():
    """Calculate checksums for test data integrity."""
    print("\n🔐 Calculating data checksums...")
    
    checksums = {}
    
    for parquet_file in TEST_DATA_PATH.rglob("*.parquet"):
        print(f"   📋 Checksum: {parquet_file.name}")
        
        # Read file and calculate hash
        df = pd.read_parquet(parquet_file)
        data_str = df.to_string()
        checksum = hashlib.md5(data_str.encode()).hexdigest()
        
        relative_path = parquet_file.relative_to(TEST_DATA_PATH)
        checksums[str(relative_path)] = checksum
        
        print(f"      Hash: {checksum}")
    
    # Save checksums
    with open(CHECKSUMS_FILE, 'w') as f:
        for file_path, checksum in sorted(checksums.items()):
            f.write(f"{checksum}  {file_path}\n")
    
    print(f"   💾 Saved checksums to: {CHECKSUMS_FILE}")

def verify_test_data():
    """Verify test data is valid and accessible."""
    print("\n✅ Verifying test data...")
    
    symbols = ["AAPL", "TSLA"]
    
    for symbol in symbols:
        test_file = TEST_DATA_PATH / symbol[0] / symbol / "2024" / "08" / f"{symbol}_2024_08.parquet"
        
        if not test_file.exists():
            print(f"   ❌ Missing test file: {test_file}")
            continue
            
        try:
            df = pd.read_parquet(test_file)
            print(f"   ✅ {symbol}: {len(df)} rows, columns: {list(df.columns)}")
            
            # Check data range
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                print(f"      Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            # Check required columns
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"      ⚠️  Missing columns: {missing_cols}")
            else:
                print(f"      ✅ All required columns present")
                
        except Exception as e:
            print(f"   ❌ Error reading {symbol}: {e}")

def main():
    """Main setup function."""
    print("🚀 Setting up UnifiedMarketDataManager test data")
    print("=" * 60)
    
    setup_test_directories()
    extract_test_data()
    calculate_checksums()
    verify_test_data()
    
    print("\n" + "=" * 60)
    print("✅ Test data setup complete!")
    print("\nTo run golden file tests:")
    print("   PYTHONPATH=src python -m pytest tests/domains/market_data/services/core/test_unified_market_data_manager_golden.py -v")
    print("\nTo update golden files:")
    print("   PYTHONPATH=src python -m pytest tests/domains/market_data/services/core/test_unified_market_data_manager_golden.py --update-golden")

if __name__ == "__main__":
    main()