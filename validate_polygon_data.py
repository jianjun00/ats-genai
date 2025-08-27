#!/usr/bin/env python3
"""Validate the collected Polygon minute bar data"""

import pandas as pd
import os

def validate_data_file(file_path, symbol):
    """Validate a single Parquet file"""
    print(f"\n=== {symbol} Data Validation ===")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    try:
        df = pd.read_parquet(file_path)
        print(f"✅ File loaded successfully")
        print(f"📊 Data Shape: {df.shape}")
        print(f"📅 Columns: {list(df.columns)}")
        
        if 'timestamp' in df.columns:
            print(f"🕐 Date Range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        if 'close' in df.columns:
            print(f"💰 Price Range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
            
        if 'volume' in df.columns:
            print(f"📈 Volume Stats: min={df['volume'].min():,}, max={df['volume'].max():,}, avg={df['volume'].mean():,.0f}")
        
        print(f"📋 Sample Data (first 3 rows):")
        print(df.head(3))
        return True
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False

def main():
    """Main validation function"""
    print("🔍 POLYGON MINUTE BAR DATA VALIDATION")
    print("=" * 50)
    
    base_path = "/mnt/d/ats-data/minute-bars"
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    
    all_valid = True
    for symbol in symbols:
        file_path = f"{base_path}/{symbol}/2024/08/{symbol}_2024_08.parquet"
        valid = validate_data_file(file_path, symbol)
        all_valid = all_valid and valid
    
    print(f"\n{'='*50}")
    if all_valid:
        print("🎉 ALL DATA FILES VALIDATED SUCCESSFULLY!")
    else:
        print("⚠️  Some data validation issues found")
    print("=" * 50)

if __name__ == "__main__":
    main()