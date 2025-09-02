#!/usr/bin/env python3
"""
Read and analyze Riegeli files by finding and decompressing zlib data.
Usage: python3 read_riegeli.py <riegeli_file_path>
"""

import os
import sys
import zlib
import struct

def find_and_decompress_riegeli(file_path):
    """Find zlib compressed data in Riegeli file and decompress it."""
    print(f"📂 Analyzing: {file_path}")
    
    with open(file_path, 'rb') as f:
        # Read the entire file for analysis
        data = f.read()
        print(f"📏 File size: {len(data):,} bytes")
        
        # Look for zlib magic bytes (0x78 0x9C is most common)
        zlib_signatures = [b'\x78\x9c', b'\x78\xda', b'\x78\x01']
        
        print(f"\n🔍 Searching for zlib compressed data...")
        
        found_data = []
        for i, sig in enumerate(zlib_signatures):
            offset = data.find(sig)
            if offset != -1:
                print(f"✅ Found zlib signature {sig.hex()} at offset {offset}")
                
                # Try to decompress from this point
                try:
                    compressed_data = data[offset:]
                    decompressed = zlib.decompress(compressed_data)
                    print(f"📦 Decompressed {len(compressed_data)} -> {len(decompressed)} bytes")
                    found_data.append((offset, decompressed))
                    break
                except Exception as e:
                    print(f"❌ Decompression failed: {e}")
                    # Try with just a portion of the data
                    for chunk_size in [1000, 5000, 10000, 50000]:
                        try:
                            chunk = data[offset:offset+chunk_size]
                            decompressed = zlib.decompress(chunk)
                            print(f"✅ Partial decompression successful: {chunk_size} -> {len(decompressed)} bytes")
                            found_data.append((offset, decompressed))
                            break
                        except:
                            continue
        
        if found_data:
            offset, decompressed = found_data[0]
            print(f"\n📄 DECOMPRESSED CONTENT:")
            print(f"=" * 60)
            print(f"📏 Decompressed size: {len(decompressed):,} bytes")
            print(f"🔍 First 200 bytes (hex): {decompressed[:200].hex()}")
            print(f"🔍 First 500 chars (text): {repr(decompressed[:500].decode('utf-8', errors='ignore'))}")
            
            # Look for patterns that might indicate TensorFlow Example
            analyze_decompressed_data(decompressed)
            
        else:
            print("❌ No zlib compressed data found")
            
            # Try to find other patterns
            print(f"\n🔍 Looking for other patterns...")
            
            # Look for repeated byte patterns that might indicate record boundaries
            for pattern_len in [4, 8, 12, 16]:
                for i in range(0, min(1000, len(data) - pattern_len)):
                    pattern = data[i:i+pattern_len]
                    count = data.count(pattern)
                    if count > 3 and len(set(pattern)) > 1:  # Pattern repeats and isn't all same byte
                        print(f"   Pattern {pattern.hex()} repeats {count} times")
                        break

def analyze_decompressed_data(data):
    """Analyze decompressed data for TensorFlow Example patterns."""
    print(f"\n🧪 Analyzing decompressed data...")
    
    # Look for TensorFlow Example field tags
    # Field 1 (features): tag = 0x0A
    if b'\x0a' in data[:50]:
        print("✅ Found potential TensorFlow Example features field")
    
    # Look for common feature names
    feature_names = [
        b'open', b'high', b'low', b'close', b'volume',
        b'symbol', b'date', b'timestamp', b'price',
        b'etop', b'ebot', b'pldot'  # Technical indicators
    ]
    
    found_features = []
    for name in feature_names:
        if name in data:
            found_features.append(name.decode('ascii'))
    
    if found_features:
        print(f"🏷️  Potential feature names found: {found_features}")
    
    # Try to extract numeric values
    print(f"\n🔢 Extracting potential numeric data...")
    float_values = []
    for i in range(0, len(data) - 4, 4):
        try:
            val = struct.unpack('<f', data[i:i+4])[0]
            if 0.001 < abs(val) < 10000:  # Reasonable range for stock prices/volumes
                float_values.append(val)
            if len(float_values) >= 20:  # Don't extract too many
                break
        except:
            continue
    
    if float_values:
        print(f"💹 Sample numeric values (first 10): {float_values[:10]}")
        print(f"📊 Value range: {min(float_values):.6f} to {max(float_values):.6f}")
    
    # Look for timestamp patterns (Unix timestamps)
    print(f"\n⏰ Looking for timestamps...")
    for i in range(0, len(data) - 8, 4):
        try:
            # Try as Unix timestamp (seconds)
            timestamp = struct.unpack('<I', data[i:i+4])[0]
            if 1640000000 < timestamp < 1730000000:  # 2022-2025 range
                from datetime import datetime
                dt = datetime.fromtimestamp(timestamp)
                print(f"📅 Potential timestamp at offset {i}: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
                break
        except:
            continue

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 read_riegeli.py <riegeli_file_path>")
        print("\nExample:")
        print("  python3 read_riegeli.py /mnt/d/ats-data/training/run_20250901_193706/TSLA/20250128_000000_20250901_000000.riegeli")
        sys.exit(1)
    
    riegeli_file = sys.argv[1]
    
    if os.path.exists(riegeli_file):
        find_and_decompress_riegeli(riegeli_file)
    else:
        print(f"❌ File not found: {riegeli_file}")
        sys.exit(1)