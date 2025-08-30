#!/usr/bin/env python3
"""
Test FirstRate data insertion using ATS dev CLI
"""

import subprocess
import zipfile
import csv
import io
from datetime import datetime
from pathlib import Path

def run_query(query):
    """Run database query using ATS dev CLI."""
    try:
        result = subprocess.run([
            'python3', 'scripts/run_dev.py', 'query', '--query', query
        ], cwd='/home/jianjun/ats-genai-data', capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"Query failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"Query execution failed: {e}")
        return None

def main():
    print("🚀 FirstRate Database Insert Test")
    print("=" * 50)
    
    # Test 1: Parse FirstRate data
    print("📊 Parsing FirstRate AAPL data...")
    
    data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    zip_files = list(data_path.glob("stock_A_*.zip"))
    
    if not zip_files:
        print("❌ No FirstRate zip files found")
        return
        
    zip_file = zip_files[0]
    print(f"📂 Processing: {zip_file.name}")
    
    sample_bars = []
    
    with zipfile.ZipFile(zip_file, 'r') as zf:
        aapl_file = "AAPL_full_1min_adjsplitdiv.txt"
        
        if aapl_file in zf.namelist():
            print(f"📈 Found AAPL data")
            
            with zf.open(aapl_file, 'r') as f:
                text_data = io.TextIOWrapper(f, encoding='utf-8')
                csv_reader = csv.reader(text_data)
                
                for row_num, row in enumerate(csv_reader):
                    if len(sample_bars) >= 5:  # Just get 5 sample bars
                        break
                        
                    if len(row) == 6:
                        try:
                            timestamp_str, open_str, high_str, low_str, close_str, volume_str = row
                            
                            timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')
                            
                            # Only recent data
                            if timestamp.year < 2020:
                                continue
                                
                            open_price = float(open_str.strip())
                            high_price = float(high_str.strip())
                            low_price = float(low_str.strip())
                            close_price = float(close_str.strip())
                            volume = int(float(volume_str.strip()))
                            
                            # Basic validation
                            if (low_price <= open_price <= high_price and 
                                low_price <= close_price <= high_price and
                                all(p > 0 for p in [open_price, high_price, low_price, close_price])):
                                
                                sample_bars.append({
                                    'timestamp': timestamp,
                                    'open': open_price,
                                    'high': high_price, 
                                    'low': low_price,
                                    'close': close_price,
                                    'volume': volume
                                })
                                
                        except (ValueError, IndexError):
                            continue
            
            print(f"✅ Parsed {len(sample_bars)} sample AAPL bars")
            
            if sample_bars:
                # Show sample
                bar = sample_bars[0]
                print(f"📊 Sample: {bar['timestamp']} | O:{bar['open']:.4f} H:{bar['high']:.4f} L:{bar['low']:.4f} C:{bar['close']:.4f} | V:{bar['volume']:,}")
                
                # Test 2: Insert one sample bar
                print("\n💾 Testing database insertion...")
                
                bar = sample_bars[0]
                insert_query = f"""
                INSERT INTO minute_bars (
                    symbol, timestamp, open, high, low, close, volume, vendor, quality_score
                ) VALUES (
                    'AAPL', 
                    '{bar['timestamp']}'::timestamptz,
                    {bar['open']},
                    {bar['high']},
                    {bar['low']},
                    {bar['close']},
                    {bar['volume']},
                    'firstrate',
                    1.0
                ) ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    vendor = EXCLUDED.vendor,
                    quality_score = EXCLUDED.quality_score
                """
                
                result = run_query(insert_query)
                if result is not None:
                    print("✅ Successfully inserted sample bar")
                    
                    # Test 3: Verify insertion
                    count_query = "SELECT COUNT(*) FROM minute_bars WHERE symbol = 'AAPL' AND vendor = 'firstrate'"
                    count_result = run_query(count_query)
                    
                    if count_result:
                        count_lines = count_result.split('\n')
                        if len(count_lines) >= 3:
                            count = count_lines[2].strip()
                            print(f"✅ Found {count} AAPL FirstRate bars in database")
                    
                    # Test 4: Show sample data
                    sample_query = """
                    SELECT timestamp, open, high, low, close, volume 
                    FROM minute_bars 
                    WHERE symbol = 'AAPL' AND vendor = 'firstrate' 
                    ORDER BY timestamp 
                    LIMIT 3
                    """
                    
                    sample_result = run_query(sample_query)
                    if sample_result:
                        print("\n📊 Sample database records:")
                        print(sample_result)
                    
                    print("\n🎯 SUCCESS: FirstRate integration test completed!")
                    print("💡 Ready to scale up to full AAPL backfill")
                    
                else:
                    print("❌ Database insertion failed")
            else:
                print("❌ No valid sample bars found")
        else:
            print("❌ AAPL file not found in zip")

if __name__ == '__main__':
    main()