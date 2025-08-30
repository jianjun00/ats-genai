#!/usr/bin/env python3
"""
Multi-letter bulk backfill for FirstRate data
Processes multiple alphabet letters efficiently
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
        ], cwd='/home/jianjun/ats-genai-data', capture_output=True, text=True, timeout=180)
        
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

def process_symbol_file(zf, symbol, filename, max_bars_per_symbol=3000):
    """Process a single symbol file from the zip."""
    
    try:
        with zf.open(filename, 'r') as f:
            text_data = io.TextIOWrapper(f, encoding='utf-8')
            csv_reader = csv.reader(text_data)
            
            current_batch = []
            batch_size = 500
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

def process_letter(letter, target_symbols_count=None):
    """Process all symbols for a given letter."""
    
    print(f"\n🔤 Processing Letter {letter}")
    print("=" * 50)
    
    # Find zip file for this letter
    data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    zip_files = list(data_path.glob(f"stock_{letter}_*.zip"))
    
    if not zip_files:
        print(f"❌ No zip file found for letter {letter}")
        return 0, 0
        
    zip_file = zip_files[0]
    print(f"📂 Processing: {zip_file.name}")
    
    symbols_processed = 0
    total_bars_loaded = 0
    symbols_success = []
    
    with zipfile.ZipFile(zip_file, 'r') as zf:
        # Get all available symbols
        available_files = {
            f.replace('_full_1min_adjsplitdiv.txt', ''): f 
            for f in zf.namelist() 
            if f.endswith('_full_1min_adjsplitdiv.txt')
        }
        
        available_symbols = sorted(available_files.keys())
        print(f"📊 Found {len(available_symbols)} symbols in zip")
        
        # Process symbols (all available if no limit specified)
        if target_symbols_count is None:
            target_symbols = available_symbols
            print(f"🎯 Processing ALL {len(target_symbols)} symbols")
        else:
            target_symbols = available_symbols[:target_symbols_count]
            print(f"🎯 Processing first {len(target_symbols)} symbols")
        
        start_time = time.time()
        
        for i, symbol in enumerate(target_symbols):
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
                print(f"  ⏭️  {symbol}: Skipping (already has {existing_count:,} bars)")
                symbols_success.append(symbol)
                continue
            
            print(f"  📊 [{i+1}/{len(target_symbols)}] Processing {symbol}...", end="", flush=True)
            
            filename = available_files[symbol]
            bars_loaded = process_symbol_file(zf, symbol, filename)
            
            if bars_loaded > 0:
                print(f" ✅ {bars_loaded:,} bars")
                symbols_success.append(symbol)
                total_bars_loaded += bars_loaded
                symbols_processed += 1
            else:
                print(f" ❌ Failed")
            
            # Progress update every 10 symbols for larger batches
            if (i + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_bars_loaded / elapsed if elapsed > 0 else 0
                eta_remaining = (len(target_symbols) - i - 1) * (elapsed / (i + 1)) if i > 0 else 0
                print(f"    📈 Progress: {i+1}/{len(target_symbols)} symbols, {total_bars_loaded:,} bars ({rate:.0f} bars/sec, ETA: {eta_remaining:.0f}s)")
    
    return symbols_processed, total_bars_loaded

def main():
    """Main processing function."""
    
    if len(sys.argv) > 1:
        letters_to_process = [l.upper() for l in sys.argv[1].split(',')]
    else:
        letters_to_process = ['B', 'C', 'D', 'E', 'F']  # Default batch
    
    print("🚀 FirstRate Multi-Letter Bulk Backfill")
    print("=" * 60)
    print(f"🔤 Letters to process: {', '.join(letters_to_process)}")
    
    overall_start = time.time()
    total_symbols_processed = 0
    total_bars_loaded = 0
    
    # Get current database status
    print(f"\n📊 Current database status:")
    current_status = run_query("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols,
            COUNT(*) as total_bars
        FROM minute_bars 
        WHERE vendor = 'firstrate'
    """)
    print(current_status)
    
    # Process each letter
    for letter in letters_to_process:
        try:
            symbols_proc, bars_loaded = process_letter(letter, target_symbols_count=None)
            total_symbols_processed += symbols_proc
            total_bars_loaded += bars_loaded
            
            # Show intermediate progress
            elapsed = time.time() - overall_start
            print(f"✅ Letter {letter} complete: {symbols_proc} symbols, {bars_loaded:,} bars ({elapsed:.1f}s)")
            
        except KeyboardInterrupt:
            print(f"\n⏸️  Processing interrupted by user")
            break
        except Exception as e:
            print(f"❌ Error processing letter {letter}: {e}")
            continue
    
    # Final summary
    total_elapsed = time.time() - overall_start
    
    print(f"\n" + "=" * 60)
    print(f"🎯 MULTI-LETTER PROCESSING COMPLETED")
    print(f"=" * 60)
    print(f"📊 Letters processed: {', '.join(letters_to_process)}")
    print(f"🔢 Symbols processed: {total_symbols_processed}")
    print(f"💾 Total bars loaded: {total_bars_loaded:,}")
    print(f"⏱️  Total time: {total_elapsed:.1f} seconds")
    
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
            MAX(timestamp)::date as latest_date
        FROM minute_bars 
        WHERE vendor = 'firstrate'
    """)
    print(final_status)
    
    # Show symbols by letter
    print(f"\n📋 Symbols by letter:")
    by_letter_query = """
    SELECT 
        SUBSTRING(symbol FROM 1 FOR 1) as letter,
        COUNT(*) as symbols,
        SUM(CASE WHEN vendor = 'firstrate' THEN 1 ELSE 0 END) as firstrate_symbols
    FROM (
        SELECT DISTINCT symbol, vendor 
        FROM minute_bars 
        WHERE vendor = 'firstrate'
    ) t
    GROUP BY SUBSTRING(symbol FROM 1 FOR 1)
    ORDER BY letter
    """
    
    by_letter_result = run_query(by_letter_query)
    if by_letter_result:
        print(by_letter_result)
    
    print(f"\n🎉 Multi-letter backfill completed successfully!")

if __name__ == '__main__':
    main()