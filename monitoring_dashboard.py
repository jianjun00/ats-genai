#!/usr/bin/env python3
"""
48-Hour Monitoring Dashboard
Shows real-time status of cleanup monitoring
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

def load_latest_metrics():
    """Load the most recent metrics"""
    metrics_files = list(Path('.').glob('monitoring_metrics_*.json'))
    if not metrics_files:
        return None
    
    latest_file = max(metrics_files, key=os.path.getctime)
    with open(latest_file) as f:
        return json.load(f)

def show_health_status():
    """Show health check status"""
    log_file = Path('monitoring_health_log.txt')
    if log_file.exists():
        with open(log_file) as f:
            lines = f.readlines()
        
        recent_lines = lines[-5:] if len(lines) >= 5 else lines
        print("🏥 Recent Health Checks:")
        for line in recent_lines:
            print(f"   {line.strip()}")
    else:
        print("⚠️ No health check log found")

def main():
    """Main dashboard"""
    print("📊 ATS 48-Hour Monitoring Dashboard")
    print("=" * 50)
    print(f"🕒 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load monitoring config
    config_file = Path('monitoring_48h_config.json')
    if config_file.exists():
        with open(config_file) as f:
            config = json.load(f)
        
        start_time = datetime.fromisoformat(config['monitoring_period']['start_time'])
        end_time = datetime.fromisoformat(config['monitoring_period']['end_time'])
        
        elapsed = datetime.now() - start_time
        remaining = end_time - datetime.now()
        
        print(f"⏱️ Monitoring started: {start_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"⏳ Time elapsed: {elapsed}")
        print(f"⏰ Time remaining: {remaining}")
        print(f"📈 Progress: {(elapsed.total_seconds() / (48*3600)) * 100:.1f}%")
    
    print()
    
    # Show latest metrics
    latest_metrics = load_latest_metrics()
    if latest_metrics:
        print("📊 Latest Metrics:")
        if 'error' not in latest_metrics:
            code_metrics = latest_metrics['code_metrics']
            db_metrics = latest_metrics['database_metrics']
            
            print(f"   🔧 Function calls: {code_metrics['total_function_calls']}")
            print(f"   📁 Unique functions: {code_metrics['unique_functions_called']}")
            print(f"   🗄️ Table accesses: {db_metrics['total_table_accesses']}")
            print(f"   📚 Unique tables: {db_metrics['unique_tables_accessed']}")
            
            print("   🔥 Hot functions:")
            for func in code_metrics['hot_functions'][:3]:
                print(f"      • {func}")
        else:
            print(f"   ❌ Error: {latest_metrics['error']}")
    else:
        print("⚠️ No metrics collected yet")
    
    print()
    show_health_status()
    
    print()
    print("💡 Commands:")
    print("   ./monitoring_health_check.sh    - Run health check now")
    print("   python3 collect_monitoring_metrics.py  - Collect metrics now")
    print("   python3 setup_48hour_monitoring.py     - View this dashboard")

if __name__ == "__main__":
    main()
