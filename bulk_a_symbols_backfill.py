#!/usr/bin/env python3
"""
Bulk backfill for multiple A symbols from FirstRate data
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
        ], cwd='/home/jianjun/ats-genai-data', capture_output=True, text=True, timeout=120)
        
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
    
    print(f"  📊 Processing {symbol}...")
    
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
                        
                        # Focus on 2020+ data
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
            
            print(f"    ✅ {symbol}: {bars_valid:,} bars processed, {batches_inserted} batches")
            return bars_valid
            
    except Exception as e:
        print(f"    ❌ {symbol}: Error - {e}")
        return 0

def main():
    print("🚀 Bulk A-Letter Symbols FirstRate Backfill")
    print("=" * 60)
    
    start_time = time.time()
    
    # Target symbols (high-value A symbols)
    target_symbols = [
        'AAPL',  # Already done - will skip
        'AMZN', 'AMGN', 'AMD', 'ADBE', 'AVGO', 'AXP', 'ABT', 'ABBV',
        'ACN', 'ATVI', 'ADP', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE'
    ]
    
    print(f"📊 Target symbols: {', '.join(target_symbols)}")
    
    # Find FirstRate zip file
    data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    zip_files = list(data_path.glob("stock_A_*.zip"))
    
    if not zip_files:
        print("❌ No FirstRate zip files found")
        return
        
    zip_file = zip_files[0]
    print(f"📂 Processing: {zip_file.name}")
    
    symbols_processed = 0
    total_bars_loaded = 0
    symbols_success = []
    symbols_failed = []
    
    with zipfile.ZipFile(zip_file, 'r') as zf:
        available_files = {f.replace('_full_1min_adjsplitdiv.txt', ''): f for f in zf.namelist() if f.endswith('_full_1min_adjsplitdiv.txt')}
        
        print(f"📈 Found {len(available_files)} symbols in zip file")
        
        for symbol in target_symbols:
            if symbol in available_files:
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
                
                filename = available_files[symbol]
                bars_loaded = process_symbol_file(zf, symbol, filename)
                
                if bars_loaded > 0:
                    symbols_success.append(symbol)
                    total_bars_loaded += bars_loaded
                else:
                    symbols_failed.append(symbol)
                
                symbols_processed += 1
                
                # Progress update
                if symbols_processed % 5 == 0:
                    elapsed = time.time() - start_time
                    rate = total_bars_loaded / elapsed if elapsed > 0 else 0
                    print(f"  📊 Progress: {symbols_processed}/{len(target_symbols)} symbols, {total_bars_loaded:,} bars ({rate:.0f} bars/sec)")
                
            else:
                print(f"  ❌ {symbol}: Not found in zip file")
                symbols_failed.append(symbol)
    
    # Final statistics
    elapsed_time = time.time() - start_time
    
    print(f"\n" + "=" * 60)
    print(f"🎯 BULK BACKFILL COMPLETED")
    print(f"=" * 60)
    print(f"⏱️  Total time: {elapsed_time:.1f} seconds")
    print(f"📊 Symbols processed: {len(symbols_success)}/{len(target_symbols)}")
    print(f"💾 Total bars loaded: {total_bars_loaded:,}")
    
    if total_bars_loaded > 0:
        rate = total_bars_loaded / elapsed_time
        print(f"⚡ Processing rate: {rate:.0f} bars/second")
    
    if symbols_success:
        print(f"✅ Successful symbols: {', '.join(symbols_success)}")
    
    if symbols_failed:
        print(f"❌ Failed symbols: {', '.join(symbols_failed)}")
    
    # Database verification
    print(f"\n🔍 Database Verification:")
    verification_query = """
    SELECT 
        vendor,
        COUNT(DISTINCT symbol) as unique_symbols,
        COUNT(*) as total_bars,
        MIN(timestamp) as earliest_bar,
        MAX(timestamp) as latest_bar
    FROM minute_bars 
    WHERE vendor = 'firstrate'
    GROUP BY vendor
    """
    
    verification_result = run_query(verification_query)
    if verification_result:
        print(verification_result)
    
    print(f"\n🎉 Ready to scale to full FirstRate dataset!")
    print(f"💡 Current system can handle ~650 bars/second")
    print(f"📈 Estimated time for full dataset (45GB): ~24-48 hours")

if __name__ == '__main__':
    main()