#!/usr/bin/env python3
"""
Bulk AAPL backfill from FirstRate data
"""

import subprocess
import zipfile
import csv
import io
from datetime import datetime
from pathlib import Path
import time

def run_query(query):
    """Run database query using ATS dev CLI."""
    try:
        result = subprocess.run([
            'python3', 'scripts/run_dev.py', 'query', '--query', query
        ], cwd='/home/jianjun/ats-genai-data', capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"Query failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"Query execution failed: {e}")
        return None

def bulk_insert_bars(bars_batch):
    """Insert a batch of bars using SQL VALUES."""
    
    if not bars_batch:
        return False
        
    # Create VALUES clause for bulk insert
    values_list = []
    for bar in bars_batch:
        values_list.append(
            f"('AAPL', '{bar['timestamp']}'::timestamptz, {bar['open']}, {bar['high']}, "
            f"{bar['low']}, {bar['close']}, {bar['volume']}, 'firstrate', 1.0)"
        )
    
    values_clause = ',\n    '.join(values_list)
    
    bulk_query = f"""
    INSERT INTO minute_bars (
        symbol, timestamp, open, high, low, close, volume, vendor, quality_score
    ) VALUES
        {values_clause}
    ON CONFLICT (symbol, timestamp) DO NOTHING
    """
    
    result = run_query(bulk_query)
    return result is not None

def main():
    print("🚀 Bulk AAPL FirstRate Backfill")
    print("=" * 50)
    
    start_time = time.time()
    
    # Parse FirstRate data
    print("📊 Parsing AAPL data from FirstRate...")
    
    data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    zip_files = list(data_path.glob("stock_A_*.zip"))
    
    if not zip_files:
        print("❌ No FirstRate zip files found")
        return
        
    zip_file = zip_files[0]
    print(f"📂 Processing: {zip_file.name}")
    
    all_bars = []
    batch_size = 500  # Insert in batches
    
    with zipfile.ZipFile(zip_file, 'r') as zf:
        aapl_file = "AAPL_full_1min_adjsplitdiv.txt"
        
        if aapl_file in zf.namelist():
            print(f"📈 Parsing AAPL minute bars...")
            
            with zf.open(aapl_file, 'r') as f:
                text_data = io.TextIOWrapper(f, encoding='utf-8')
                csv_reader = csv.reader(text_data)
                
                bars_parsed = 0
                bars_valid = 0
                current_batch = []
                batches_inserted = 0
                
                for row_num, row in enumerate(csv_reader):
                    if bars_parsed >= 10000:  # Limit for testing (10K bars)
                        break
                        
                    if len(row) == 6:
                        try:
                            timestamp_str, open_str, high_str, low_str, close_str, volume_str = row
                            
                            timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')
                            
                            # Focus on recent data (2020+)
                            if timestamp.year < 2020:
                                continue
                                
                            open_price = float(open_str.strip())
                            high_price = float(high_str.strip())
                            low_price = float(low_str.strip())
                            close_price = float(close_str.strip())
                            volume = int(float(volume_str.strip()))
                            
                            # Validation
                            if (low_price <= open_price <= high_price and 
                                low_price <= close_price <= high_price and
                                all(p > 0 for p in [open_price, high_price, low_price, close_price]) and
                                volume >= 0):
                                
                                current_batch.append({
                                    'timestamp': timestamp,
                                    'open': open_price,
                                    'high': high_price,
                                    'low': low_price,
                                    'close': close_price,
                                    'volume': volume
                                })
                                
                                bars_valid += 1
                                
                                # Insert batch when full
                                if len(current_batch) >= batch_size:
                                    print(f"💾 Inserting batch {batches_inserted + 1} ({len(current_batch)} bars)...")
                                    if bulk_insert_bars(current_batch):
                                        batches_inserted += 1
                                        print(f"✅ Batch {batches_inserted} inserted successfully")
                                    else:
                                        print(f"❌ Batch {batches_inserted + 1} failed")
                                        
                                    current_batch = []
                                    
                                    # Progress update
                                    if batches_inserted % 5 == 0:
                                        elapsed = time.time() - start_time
                                        rate = bars_valid / elapsed if elapsed > 0 else 0
                                        print(f"📊 Progress: {bars_valid:,} bars processed ({rate:.0f} bars/sec)")
                                        
                            bars_parsed += 1
                            
                        except (ValueError, IndexError, TypeError) as e:
                            continue
                
                # Insert remaining bars
                if current_batch:
                    print(f"💾 Inserting final batch ({len(current_batch)} bars)...")
                    if bulk_insert_bars(current_batch):
                        batches_inserted += 1
                        print(f"✅ Final batch inserted successfully")
                
                print(f"\n📊 Parsing complete:")
                print(f"  Total rows processed: {bars_parsed:,}")
                print(f"  Valid bars found: {bars_valid:,}")
                print(f"  Batches inserted: {batches_inserted}")
        
        # Verify insertion
        print("\n🔍 Verifying database insertion...")
        
        count_result = run_query("SELECT COUNT(*) FROM minute_bars WHERE symbol = 'AAPL' AND vendor = 'firstrate'")
        if count_result:
            count_lines = count_result.split('\n')
            if len(count_lines) >= 3:
                total_count = count_lines[2].strip()
                print(f"✅ Total AAPL bars in database: {total_count}")
        
        # Get date range
        date_range_query = """
        SELECT 
            MIN(timestamp) as first_bar, 
            MAX(timestamp) as last_bar,
            COUNT(DISTINCT DATE(timestamp)) as trading_days
        FROM minute_bars 
        WHERE symbol = 'AAPL' AND vendor = 'firstrate'
        """
        
        date_result = run_query(date_range_query)
        if date_result:
            print(f"📅 Date range information:")
            print(date_result)
        
        # Performance stats
        elapsed_time = time.time() - start_time
        if bars_valid > 0:
            rate = bars_valid / elapsed_time
            print(f"\n⚡ Performance:")
            print(f"  Processing time: {elapsed_time:.1f} seconds")
            print(f"  Processing rate: {rate:.0f} bars/second")
        
        print(f"\n🎯 SUCCESS: AAPL bulk backfill completed!")
        print(f"Ready to scale to full dataset or other symbols")

if __name__ == '__main__':
    main()