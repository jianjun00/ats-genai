#!/usr/bin/env python3
"""
Transfer daily prices from dev to intg in batches
"""

import subprocess
import sys

def run_dev_query(query):
    """Execute query on dev database"""
    result = subprocess.run(
        ['python3', 'scripts/run_dev.py', '--environment', 'dev', 'query', '--query', query],
        capture_output=True, text=True
    )
    return result.stdout if result.returncode == 0 else ""

def run_intg_query(query):
    """Execute query on intg database"""
    result = subprocess.run(
        ['python3', 'scripts/run_intg.py', 'query', '--query', query],
        capture_output=True, text=True
    )
    return result.stdout if result.returncode == 0 else ""

def copy_files_between_containers(filename):
    """Copy file between dev and intg containers"""
    subprocess.run([
        'docker', 'cp', f'ats-dev-postgres:/tmp/{filename}', f'/tmp/{filename}'
    ])
    subprocess.run([
        'docker', 'cp', f'/tmp/{filename}', f'ats-intg-postgres:/tmp/{filename}'
    ])

def transfer_vendor_data(vendor, table_name, batch_size=500000):
    """Transfer data for a specific vendor"""
    print(f"🚀 Starting {vendor} daily prices transfer...")
    
    # Get total count
    count_query = f"SELECT COUNT(*) FROM {table_name}"
    count_result = run_dev_query(count_query)
    
    total_count = 0
    for line in count_result.split('\n'):
        if line.strip().isdigit():
            total_count = int(line.strip())
            break
    
    print(f"📊 Total {vendor} records to transfer: {total_count}")
    
    transferred = 0
    offset = 0
    
    while offset < total_count:
        print(f"📦 Processing {vendor} batch: offset {offset}, size {min(batch_size, total_count - offset)}")
        
        # Export batch from dev
        if vendor == 'polygon':
            export_query = f"""
            COPY (SELECT date, symbol, open, high, low, close, volume, instrument_id, 
                         'polygon' as source, 'active' as status
                  FROM {table_name} 
                  ORDER BY date, instrument_id 
                  LIMIT {batch_size} OFFSET {offset}) 
            TO '/tmp/{vendor}_batch.csv' WITH CSV HEADER
            """
        else:  # tiingo or eodhd
            export_query = f"""
            COPY (SELECT date, symbol, open, high, low, close, adjclose as adjusted_price, volume, 
                         instrument_id, '{vendor}' as source, 'active' as status
                  FROM {table_name} 
                  ORDER BY date, instrument_id 
                  LIMIT {batch_size} OFFSET {offset}) 
            TO '/tmp/{vendor}_batch.csv' WITH CSV HEADER
            """
        
        export_result = run_dev_query(export_query)
        if 'COPY' not in export_result:
            print(f"❌ Failed to export {vendor} batch: {export_result}")
            break
        
        # Copy file between containers
        copy_files_between_containers(f'{vendor}_batch.csv')
        
        # Import to intg
        if vendor == 'polygon':
            import_query = f"""
            COPY intg_daily_prices (date, symbol, open, high, low, close, volume, 
                                   instrument_id, source, status)
            FROM '/tmp/{vendor}_batch.csv' WITH CSV HEADER
            """
        else:  # tiingo or eodhd  
            import_query = f"""
            COPY intg_daily_prices (date, symbol, open, high, low, close, adjusted_price, 
                                   volume, instrument_id, source, status)
            FROM '/tmp/{vendor}_batch.csv' WITH CSV HEADER
            """
        
        import_result = run_intg_query(import_query)
        if 'COPY' not in import_result:
            print(f"❌ Failed to import {vendor} batch: {import_result}")
            break
        
        # Extract number of records copied
        records_copied = 0
        for line in import_result.split('\n'):
            if 'COPY' in line:
                try:
                    records_copied = int(line.split()[1])
                    break
                except:
                    pass
        
        transferred += records_copied
        offset += batch_size
        
        progress = (transferred / total_count) * 100
        print(f"📈 {vendor} progress: {transferred:,}/{total_count:,} ({progress:.1f}%)")
        
        if records_copied < batch_size:
            print(f"✅ {vendor} transfer completed - reached end of data")
            break
    
    print(f"✅ {vendor} transfer completed: {transferred:,} records")
    return transferred

def main():
    """Main transfer function"""
    print("🚀 Starting daily prices transfer from dev to intg...")
    
    # Define vendor tables
    vendors = [
        ('polygon', 'dev_daily_prices_polygon'),
        # ('tiingo', 'dev_daily_prices_tiingo'),    # Skip large tables for now
        # ('eodhd', 'dev_daily_prices_eodhd')
    ]
    
    total_transferred = 0
    
    for vendor, table_name in vendors:
        records_transferred = transfer_vendor_data(vendor, table_name)
        total_transferred += records_transferred
    
    # Final verification
    intg_count_result = run_intg_query("SELECT COUNT(*) FROM intg_daily_prices")
    intg_count = 0
    for line in intg_count_result.split('\n'):
        if line.strip().isdigit():
            intg_count = int(line.strip())
            break
    
    print(f"🎉 Daily prices transfer completed!")
    print(f"📊 Total transferred: {total_transferred:,}")
    print(f"📊 Total in intg_daily_prices: {intg_count:,}")
    
    return intg_count > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)