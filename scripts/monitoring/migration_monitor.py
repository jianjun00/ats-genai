#!/usr/bin/env python3
"""
Migration Progress Monitor

Real-time monitoring of the full database-to-file migration progress.
"""

import subprocess
import time
import re
from datetime import datetime, timedelta
import json

def get_job_logs():
    """Get the latest logs from the migration job"""
    try:
        result = subprocess.run([
            'kubectl', 'logs', 'job/full-historical-migration', 
            '-n', 'ats-dev', '--tail=50'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Error getting logs: {result.stderr}"
    except Exception as e:
        return f"Exception getting logs: {e}"

def parse_progress_info(logs):
    """Extract progress information from logs"""
    progress_info = {
        'current_table': 'Unknown',
        'records_processed': 0,
        'total_records': 0,
        'progress_percentage': 0.0,
        'current_speed': 0,
        'files_created': 0,
        'batch_number': 0,
        'memory_usage': 'N/A',
        'disk_usage': 'N/A'
    }
    
    lines = logs.split('\n')
    
    for line in lines:
        # Current table being processed
        if 'Processing table:' in line:
            table_match = re.search(r'Processing table: (\w+)', line)
            if table_match:
                progress_info['current_table'] = table_match.group(1)
        
        # Progress information
        if 'Batch' in line and 'Progress:' in line:
            # Extract batch number
            batch_match = re.search(r'Batch (\d+):', line)
            if batch_match:
                progress_info['batch_number'] = int(batch_match.group(1))
            
            # Extract records info
            records_match = re.search(r'(\d+) records, (\d+) files, ([\d,]+) rec/sec, Progress: ([\d,]+)/([\d,]+) \(([\d.]+)%\)', line)
            if records_match:
                batch_records = int(records_match.group(1))
                files_in_batch = int(records_match.group(2))
                speed = int(records_match.group(3).replace(',', ''))
                processed = int(records_match.group(4).replace(',', ''))
                total = int(records_match.group(5).replace(',', ''))
                percentage = float(records_match.group(6))
                
                progress_info['records_processed'] = processed
                progress_info['total_records'] = total
                progress_info['progress_percentage'] = percentage
                progress_info['current_speed'] = speed
                progress_info['files_created'] += files_in_batch
        
        # Resource usage
        if 'Memory:' in line:
            memory_match = re.search(r'Memory: ([\d.]+)% \(([\d.]+)GB/([\d.]+)GB\)', line)
            if memory_match:
                progress_info['memory_usage'] = f"{memory_match.group(1)}% ({memory_match.group(2)}GB/{memory_match.group(3)}GB)"
        
        if 'Disk:' in line:
            disk_match = re.search(r'Disk: ([\d.]+)% \(([\d.]+)GB/([\d.]+)GB\)', line)
            if disk_match:
                progress_info['disk_usage'] = f"{disk_match.group(1)}% ({disk_match.group(2)}GB/{disk_match.group(3)}GB)"
    
    return progress_info

def estimate_completion_time(processed, total, current_speed):
    """Estimate completion time based on current progress"""
    if current_speed == 0 or processed >= total:
        return "Unable to estimate"
    
    remaining_records = total - processed
    remaining_seconds = remaining_records / current_speed
    
    completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
    return completion_time.strftime("%Y-%m-%d %H:%M:%S")

def format_number(num):
    """Format large numbers with commas"""
    return f"{num:,}"

def main():
    """Main monitoring loop"""
    print("🔍 Migration Progress Monitor Started")
    print("="*80)
    
    monitor_start = datetime.now()
    last_processed = 0
    
    while True:
        try:
            # Get current logs
            logs = get_job_logs()
            
            # Parse progress
            progress = parse_progress_info(logs)
            
            # Calculate average speed
            elapsed_time = (datetime.now() - monitor_start).total_seconds()
            if elapsed_time > 0 and progress['records_processed'] > 0:
                avg_speed = progress['records_processed'] / elapsed_time
            else:
                avg_speed = 0
            
            # Clear screen and show status
            print("\033[2J\033[H")  # Clear screen
            print("🚀 FULL DATABASE MIGRATION - LIVE PROGRESS MONITOR")
            print("="*80)
            print(f"📊 Current Status: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🔄 Processing Table: {progress['current_table']}")
            print(f"📈 Batch Number: {progress['batch_number']}")
            print()
            print("📊 PROGRESS METRICS:")
            print(f"   Records Processed: {format_number(progress['records_processed'])} / {format_number(progress['total_records'])}")
            print(f"   Progress: {progress['progress_percentage']:.1f}% complete")
            print(f"   Files Created: {format_number(progress['files_created'])}")
            print()
            print("⚡ PERFORMANCE METRICS:")
            print(f"   Current Speed: {format_number(progress['current_speed'])} records/sec")
            print(f"   Average Speed: {format_number(int(avg_speed))} records/sec")
            print(f"   Estimated Completion: {estimate_completion_time(progress['records_processed'], progress['total_records'], progress['current_speed'])}")
            print()
            print("💾 RESOURCE USAGE:")
            print(f"   Memory Usage: {progress['memory_usage']}")
            print(f"   Disk Usage: {progress['disk_usage']}")
            print()
            print("⏰ TIMING:")
            print(f"   Monitor Runtime: {elapsed_time/3600:.1f} hours")
            
            # Progress bar
            if progress['total_records'] > 0:
                bar_width = 50
                filled = int(bar_width * progress['progress_percentage'] / 100)
                bar = "█" * filled + "░" * (bar_width - filled)
                print(f"   Progress Bar: [{bar}] {progress['progress_percentage']:.1f}%")
            
            print("="*80)
            print("Press Ctrl+C to exit monitor")
            
            # Check if migration is complete
            if progress['progress_percentage'] >= 100 or "MIGRATION COMPLETED" in logs:
                print("\n🎉 MIGRATION COMPLETED!")
                break
            
            # Update tracking
            last_processed = progress['records_processed']
            
            # Wait before next update
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n🛑 Monitor stopped by user")
            break
        except Exception as e:
            print(f"\n❌ Monitor error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()