#!/usr/bin/env python3
"""
Full FirstRate Dataset Backfill - ALL SYMBOLS
Removes artificial limitations and processes complete dataset
"""

import subprocess
import zipfile
import csv
import io
from datetime import datetime
from pathlib import Path
import time
import sys

def run_query(query):
    """Run database query using ATS dev CLI."""
    try:
        result = subprocess.run([
            'python3', 'scripts/run_dev.py', 'query', '--query', query
        ], cwd='/home/jianjun/ats-genai-data', capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print(f"Query failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"Query execution failed: {e}")
        return None

def bulk_insert_bars(symbol, bars_batch):
    """Insert a batch of bars for a specific symbol."""
    
    if not bars_batch:
        return False
        
    # Create VALUES clause for bulk insert
    values_list = []
    for bar in bars_batch:
        values_list.append(
            f"('{symbol}', '{bar['timestamp']}'::timestamptz, {bar['open']}, {bar['high']}, "
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

def process_symbol_file(zf, symbol, filename, max_bars_per_symbol=5000):
    """Process a single symbol file from the zip."""
    
    try:
        with zf.open(filename, 'r') as f:
            text_data = io.TextIOWrapper(f, encoding='utf-8')
            csv_reader = csv.reader(text_data)
            
            current_batch = []
            batch_size = 1000
            bars_processed = 0
            bars_valid = 0
            batches_inserted = 0
            
            for row_num, row in enumerate(csv_reader):
                if bars_valid >= max_bars_per_symbol:
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
                                if bulk_insert_bars(symbol, current_batch):
                                    batches_inserted += 1
                                current_batch = []
                                
                        bars_processed += 1
                        
                    except (ValueError, IndexError, TypeError):
                        continue
            
            # Insert remaining bars
            if current_batch:
                if bulk_insert_bars(symbol, current_batch):
                    batches_inserted += 1
            
            return bars_valid
            
    except Exception as e:
        print(f"    ❌ {symbol}: Error - {e}")
        return 0

def process_all_letters():
    """Process ALL symbols from ALL letters."""
    
    print("🚀 FULL FirstRate Dataset Backfill - ALL SYMBOLS")
    print("=" * 70)
    
    data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    zip_files = list(data_path.glob("stock_*_full_1min_adjsplitdiv_*.zip"))
    
    print(f"📁 Found {len(zip_files)} FirstRate zip files")
    
    # Get current status
    current_status = run_query("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols,
            COUNT(*) as total_bars
        FROM minute_bars 
        WHERE vendor = 'firstrate'
    """)
    print(f"📊 Current database: {current_status}")
    
    overall_start = time.time()
    total_symbols_processed = 0
    total_bars_loaded = 0
    
    # Process each zip file
    for zip_file in sorted(zip_files):
        letter = zip_file.name.split('_')[1]
        print(f"\n🔤 Processing Letter {letter} - {zip_file.name}")
        print("=" * 60)
        
        symbols_processed = 0
        bars_loaded = 0
        
        with zipfile.ZipFile(zip_file, 'r') as zf:
            # Get all available symbols
            available_files = {
                f.replace('_full_1min_adjsplitdiv.txt', ''): f 
                for f in zf.namelist() 
                if f.endswith('_full_1min_adjsplitdiv.txt')
            }
            
            available_symbols = sorted(available_files.keys())
            print(f"📊 Found {len(available_symbols)} symbols in {letter} zip")
            print(f"🎯 Processing ALL {len(available_symbols)} symbols")
            
            start_time = time.time()
            
            for i, symbol in enumerate(available_symbols):
                # Check if already exists
                existing_count = 0
                count_result = run_query(f"SELECT COUNT(*) FROM minute_bars WHERE symbol = '{symbol}' AND vendor = 'firstrate'")
                if count_result:
                    try:
                        count_lines = count_result.split('\n')
                        existing_count = int(count_lines[2].strip())
                    except:
                        existing_count = 0
                
                if existing_count > 0:
                    if i % 50 == 0:  # Show progress less frequently for existing symbols
                        print(f"  ⏭️  [{i+1}/{len(available_symbols)}] {symbol}: Skipping (has {existing_count:,} bars)")
                    continue
                
                print(f"  📊 [{i+1}/{len(available_symbols)}] Processing {symbol}...", end="", flush=True)
                
                filename = available_files[symbol]
                bars_processed = process_symbol_file(zf, symbol, filename, max_bars_per_symbol=5000)
                
                if bars_processed > 0:
                    print(f" ✅ {bars_processed:,} bars")
                    symbols_processed += 1
                    bars_loaded += bars_processed
                else:
                    print(f" ❌ Failed")
                
                # Progress update every 25 symbols
                if (i + 1) % 25 == 0:
                    elapsed = time.time() - start_time
                    rate = bars_loaded / elapsed if elapsed > 0 else 0
                    print(f"    📈 Letter {letter} Progress: {i+1}/{len(available_symbols)} symbols, {bars_loaded:,} bars ({rate:.0f} bars/sec)")
        
        # Letter completion
        letter_elapsed = time.time() - start_time
        total_symbols_processed += symbols_processed
        total_bars_loaded += bars_loaded
        
        print(f"✅ Letter {letter} complete: {symbols_processed} new symbols, {bars_loaded:,} new bars ({letter_elapsed:.1f}s)")
    
    # Final summary
    total_elapsed = time.time() - overall_start
    
    print(f"\n" + "=" * 70)
    print(f"🎯 FULL FIRSTRATE DATASET PROCESSING COMPLETED")
    print(f"=" * 70)
    print(f"🔢 New symbols processed: {total_symbols_processed}")
    print(f"💾 New bars loaded: {total_bars_loaded:,}")
    print(f"⏱️  Total time: {total_elapsed/3600:.1f} hours")
    
    if total_bars_loaded > 0:
        rate = total_bars_loaded / total_elapsed
        print(f"⚡ Average rate: {rate:.0f} bars/second")
    
    # Final database status
    print(f"\n📊 Final database status:")
    final_status = run_query("""
        SELECT 
            COUNT(DISTINCT symbol) as total_symbols,
            COUNT(*) as total_bars,
            MIN(timestamp)::date as earliest_date,
            MAX(timestamp)::date as latest_date,
            ROUND(SUM(volume) / 1000000000.0, 2) as total_volume_billions
        FROM minute_bars 
        WHERE vendor = 'firstrate'
    """)
    print(final_status)
    
    # Summary by letter
    print(f"\n📋 Final symbols by letter:")
    by_letter_query = """
    SELECT 
        SUBSTRING(symbol FROM 1 FOR 1) as letter,
        COUNT(DISTINCT symbol) as symbols,
        COUNT(*) as bars,
        ROUND(AVG(volume), 0) as avg_volume
    FROM minute_bars 
    WHERE vendor = 'firstrate'
    GROUP BY SUBSTRING(symbol FROM 1 FOR 1)
    ORDER BY letter
    """
    
    by_letter_result = run_query(by_letter_query)
    if by_letter_result:
        print(by_letter_result)
    
    print(f"\n🎉 FULL FirstRate dataset backfill completed!")

if __name__ == '__main__':
    process_all_letters()