#!/usr/bin/env python3
"""
Transfer instruments from dev to intg
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

def transfer_instruments_batch(offset, batch_size=1000):
    """Transfer a batch of instruments"""
    print(f"Transferring instruments batch: offset {offset}, size {batch_size}")
    
    # Get batch from dev
    select_query = f"""
    SELECT id, symbol, name, exchange, type, currency, figi, isin, cusip, 
           composite_figi, active, list_date, delist_date, created_at, updated_at,
           sector, instrument_type, exchange_code, currency_code
    FROM dev_instruments 
    ORDER BY id 
    LIMIT {batch_size} OFFSET {offset}
    """
    
    dev_result = run_dev_query(select_query)
    if not dev_result or 'ERROR' in dev_result:
        print(f"Failed to get batch from dev: {dev_result}")
        return False
    
    lines = dev_result.strip().split('\n')
    if len(lines) < 3:  # No data
        return False
    
    headers = [h.strip() for h in lines[0].split('|')]
    
    inserted = 0
    for line in lines[2:]:  # Skip header and separator
        if '|' in line and not line.strip().startswith('('):
            values = [v.strip() for v in line.split('|')]
            if len(values) >= len(headers):
                # Build insert query
                escaped_values = []
                for val in values[:len(headers)]:
                    if val == '' or val == 'NULL':
                        escaped_values.append('NULL')
                    elif val in ['t', 'f']:  # boolean
                        escaped_values.append(val)
                    else:
                        # Escape single quotes
                        escaped_val = val.replace("'", "''") 
                        escaped_values.append(f"'{escaped_val}'")
                
                insert_query = f"""
                INSERT INTO intg_instruments ({', '.join(headers)})
                VALUES ({', '.join(escaped_values)})
                ON CONFLICT (id) DO NOTHING
                """
                
                result = run_intg_query(insert_query)
                if result and 'INSERT' in result:
                    inserted += 1
    
    print(f"Inserted {inserted} instruments from batch")
    return inserted > 0

def main():
    """Main transfer function"""
    print("🚀 Starting instruments transfer from dev to intg...")
    
    # Get total count
    total_query = "SELECT COUNT(*) FROM dev_instruments"
    total_result = run_dev_query(total_query)
    
    total_count = 0
    for line in total_result.split('\n'):
        if line.strip().isdigit():
            total_count = int(line.strip())
            break
    
    print(f"📊 Total instruments to transfer: {total_count}")
    
    batch_size = 500
    offset = 0
    total_transferred = 0
    
    while offset < total_count:
        if transfer_instruments_batch(offset, batch_size):
            total_transferred += batch_size
            offset += batch_size
            
            # Progress update
            progress = min(offset, total_count) / total_count * 100
            print(f"📈 Progress: {min(offset, total_count)}/{total_count} ({progress:.1f}%)")
        else:
            print("No more data to transfer")
            break
    
    # Final verification
    intg_count_result = run_intg_query("SELECT COUNT(*) FROM intg_instruments")
    intg_count = 0
    for line in intg_count_result.split('\n'):
        if line.strip().isdigit():
            intg_count = int(line.strip())
            break
    
    print(f"✅ Transfer completed: {intg_count} instruments in intg database")
    return intg_count > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)