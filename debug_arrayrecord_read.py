#!/usr/bin/env python3
"""
Direct ArrayRecord file reading test to understand the sequence data issue.
"""

import sys
import numpy as np
import pandas as pd
import json

try:
    import array_record
except ImportError:
    print("❌ array_record library not available")
    sys.exit(1)

def debug_arrayrecord_file(file_path):
    """Debug a specific ArrayRecord file."""
    
    print(f"🔍 Reading ArrayRecord file: {file_path}")
    print("=" * 60)
    
    try:
        # Read the ArrayRecord file
        with array_record.ArrayRecordReader(file_path) as reader:
            record_count = len(reader)
            print(f"✅ Total records in file: {record_count}")
            
            if record_count == 0:
                print("❌ No records found in file")
                return
            
            # Read all records
            all_records = []
            for i in range(min(5, record_count)):  # Read first 5 records
                record = reader[i]
                all_records.append(record)
                print(f"📊 Record {i}: type={type(record)}, size={len(record) if hasattr(record, '__len__') else 'N/A'}")
                
                # Try to interpret the record
                if isinstance(record, bytes):
                    print(f"   Raw bytes (first 100): {record[:100]}")
                    try:
                        # Try JSON parsing
                        json_data = json.loads(record.decode('utf-8'))
                        print(f"   ✅ JSON parsed successfully")
                        print(f"   Keys: {list(json_data.keys()) if isinstance(json_data, dict) else 'Not a dict'}")
                        
                        # Look for OHLC data
                        if isinstance(json_data, dict):
                            for key, value in json_data.items():
                                if any(ohlc in key.lower() for ohlc in ['open', 'high', 'low', 'close', 'volume']):
                                    print(f"   OHLC data found: {key} = {type(value)}")
                                    if isinstance(value, list) and len(value) > 0:
                                        print(f"     Sample values: {value[:3]}...")
                    except json.JSONDecodeError:
                        print("   ❌ Not valid JSON")
                        
                        # Try as raw numpy array
                        try:
                            arr = np.frombuffer(record, dtype=np.float64)
                            print(f"   Numpy array interpretation: shape={arr.shape}, dtype={arr.dtype}")
                            print(f"   Sample values: {arr[:10] if len(arr) > 10 else arr}")
                        except:
                            print("   ❌ Could not interpret as numpy array")
                            
                elif isinstance(record, np.ndarray):
                    print(f"   ✅ Numpy array: shape={record.shape}, dtype={record.dtype}")
                    print(f"   Sample values: {record.flat[:10] if record.size > 10 else record.flat[:]}")
                    
                elif isinstance(record, dict):
                    print(f"   ✅ Dictionary with keys: {list(record.keys())}")
                    
                else:
                    print(f"   Record content: {str(record)[:200]}...")
            
            # Try to read the entire file as a dataframe-like structure
            print("\n🔍 Attempting to read as structured data...")
            try:
                # Read all records and try to convert to DataFrame
                all_data = []
                for i in range(record_count):
                    record = reader[i]
                    if isinstance(record, bytes):
                        try:
                            parsed = json.loads(record.decode('utf-8'))
                            all_data.append(parsed)
                        except:
                            pass
                    elif isinstance(record, dict):
                        all_data.append(record)
                
                if all_data:
                    print(f"✅ Successfully parsed {len(all_data)} records")
                    
                    # Create DataFrame
                    df = pd.DataFrame(all_data)
                    print(f"DataFrame shape: {df.shape}")
                    print(f"Columns: {list(df.columns)[:10]}...")  # First 10 columns
                    
                    # Look for OHLC columns
                    ohlc_cols = [col for col in df.columns if any(x in col.lower() for x in ['open', 'high', 'low', 'close', 'volume'])]
                    print(f"OHLC-related columns found: {len(ohlc_cols)}")
                    
                    if 'symbol' in df.columns:
                        print(f"Symbols in data: {df['symbol'].unique()}")
                        
                    if 'timestamp' in df.columns:
                        print(f"Timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                        
                    # Show sample data
                    print("\nSample data (first row):")
                    if not df.empty:
                        first_row = df.iloc[0]
                        for col in list(df.columns)[:20]:  # First 20 columns
                            print(f"  {col}: {first_row[col]}")
                            
                else:
                    print("❌ No structured data found")
                    
            except Exception as e:
                print(f"❌ Error reading as structured data: {e}")
                
    except Exception as e:
        print(f"❌ Error reading ArrayRecord file: {e}")

if __name__ == "__main__":
    # Test the AAPL 1h file
    aapl_file = "/mnt/d/ats-data/training_data/76/1h/AAPL_20250701_000000_20250906_000000.arrayrecord"
    debug_arrayrecord_file(aapl_file)
    
    print("\n" + "=" * 60)
    
    # Test the TSLA 1h file
    tsla_file = "/mnt/d/ats-data/training_data/76/1h/TSLA_20250701_000000_20250906_000000.arrayrecord"
    debug_arrayrecord_file(tsla_file)