#!/usr/bin/env python3
"""
Direct test to verify ArrayRecord content for the specific failing file
Following CLAUDE.md fail-fast principles
"""

import os
import sys
from pathlib import Path

def test_specific_arrayrecord_file():
    """Test the specific file that was reported as empty"""
    
    # The exact file from the error message
    file_path = "/data/training_data/dataset_20250922_182618/technical_momentum/AAPL_2025_07/1w/AAPL_2025_07_technical_momentum.arrayrecord"
    
    print("🔍 SPECIFIC ARRAYRECORD FILE VERIFICATION")
    print("=" * 60)
    print(f"File: {file_path}")
    
    if not os.path.exists(file_path):
        print("❌ File does not exist")
        return "FILE_NOT_FOUND"
    
    # Basic file info
    file_size = os.path.getsize(file_path)
    print(f"📊 File size: {file_size:,} bytes")
    
    if file_size == 0:
        print("❌ File is empty (0 bytes)")
        return "EMPTY_FILE"
    
    # Read file header to check if it's a valid ArrayRecord
    with open(file_path, 'rb') as f:
        header = f.read(16)  # Read first 16 bytes
        print(f"🔍 Header (hex): {header.hex()}")
        print(f"🔍 Header (ascii): {header}")
    
    # Check if file contains actual data beyond header
    if file_size == 131072:  # Exact size we saw
        print("ℹ️  File size is exactly 128KB (131072 bytes)")
        print("ℹ️  This suggests it might be a fixed-size buffer or header-only file")
    
    # Try to identify if this is a sparse file or placeholder
    with open(file_path, 'rb') as f:
        # Read in chunks to see if file is mostly zeros
        chunk_size = 1024
        non_zero_chunks = 0
        total_chunks = 0
        
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            
            total_chunks += 1
            if any(byte != 0 for byte in chunk):
                non_zero_chunks += 1
    
    print(f"📊 Non-zero chunks: {non_zero_chunks}/{total_chunks}")
    
    if non_zero_chunks == 0:
        print("❌ File contains only zeros - effectively empty")
        return "ZERO_FILLED_FILE"
    elif non_zero_chunks < total_chunks * 0.1:  # Less than 10% has data
        print("⚠️  File is mostly empty (sparse data)")
        return "SPARSE_FILE"
    else:
        print("✅ File contains substantial data")
        return "VALID_FILE"

def check_arrayrecord_structure():
    """Check if we can understand the ArrayRecord structure"""
    
    print("\n🔍 ARRAYRECORD STRUCTURE ANALYSIS")
    print("=" * 60)
    
    # Check if we have any ArrayRecord reader available
    arrayrecord_paths = [
        "src/infrastructure/storage/arrayrecord",
        "src/domains/ml/storage",
        "src/core/storage"
    ]
    
    reader_found = False
    for path in arrayrecord_paths:
        if os.path.exists(path):
            print(f"📁 Found potential ArrayRecord code: {path}")
            reader_found = True
    
    if not reader_found:
        print("❌ No ArrayRecord reader implementation found")
        return "NO_READER"
    
    # Try to use the array_record library directly if available
    try_import_arrayrecord()
    
    return "ANALYSIS_COMPLETE"

def try_import_arrayrecord():
    """Try to import and use array_record library directly"""
    
    print("\n🔍 TRYING DIRECT ARRAY_RECORD IMPORT")
    print("=" * 40)
    
    # The exact file we're testing
    file_path = "/data/training_data/dataset_20250922_182618/technical_momentum/AAPL_2025_07/1w/AAPL_2025_07_technical_momentum.arrayrecord"
    
    # Direct import without exception handling (fail-fast)
    import array_record.python.array_record_module as array_record
    
    print("✅ array_record library imported successfully")
    
    # Try to read the file with correct API
    print(f"📖 Reading ArrayRecord file: {file_path}")
    
    # Use the correct ArrayRecordReader
    reader = array_record.ArrayRecordReader(file_path)
    
    print("✅ ArrayRecord file opened successfully")
    
    # Get record count  
    record_count = len(reader)
    print(f"📊 Record count: {record_count}")
    
    if record_count == 0:
        print("❌ ArrayRecord contains 0 records - this explains the 'empty' error")
        return "ZERO_RECORDS_CONFIRMED"
    
    # Read first few records to check structure
    print(f"🔍 Reading first few records...")
    
    for i in range(min(3, record_count)):
        record = reader[i]
        print(f"   Record {i}: {type(record)} - size: {len(record)} bytes")
        
        # Try to decode if it's serialized data
        if len(record) > 0:
            print(f"     First 50 bytes: {record[:50]}")
    
    return "RECORDS_FOUND"

def main():
    """Run ArrayRecord content verification"""
    
    print("🚨 ARRAYRECORD CONTENT VERIFICATION")
    print("=" * 80)
    print("Testing the specific file reported as empty")
    print()
    
    # Test the specific file
    file_result = test_specific_arrayrecord_file()
    print(f"\n📊 File test result: {file_result}")
    
    # Check structure
    structure_result = check_arrayrecord_structure()
    print(f"📊 Structure test result: {structure_result}")
    
    # Try direct reading if file seems valid
    if file_result in ["VALID_FILE", "SPARSE_FILE"]:
        print(f"\n🔄 Attempting direct ArrayRecord reading...")
        read_result = try_import_arrayrecord()
        print(f"📊 Read result: {read_result}")
    
    # Summary
    print(f"\n📋 FINAL DIAGNOSIS")
    print("=" * 30)
    
    if file_result == "ZERO_FILLED_FILE":
        print("❌ Issue: File exists but contains only zeros")
        print("💡 Root cause: ArrayRecord generation created empty buffer")
    elif "ZERO_RECORDS" in str(read_result):
        print("❌ Issue: ArrayRecord file has valid structure but 0 records")
        print("💡 Root cause: 1w timeframe aggregation produced no data")
    elif file_result == "SPARSE_FILE":
        print("⚠️  Issue: File has minimal data")
        print("💡 Root cause: 1w timeframe aggregation produced very little data")
    else:
        print("✅ File appears valid - issue may be in reader interpretation")
    
    print("\n✅ ARRAYRECORD CONTENT VERIFICATION COMPLETE")

if __name__ == "__main__":
    main()