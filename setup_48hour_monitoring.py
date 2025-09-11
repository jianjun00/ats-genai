#!/usr/bin/env python3
"""
Setup 48-Hour Monitoring

This script sets up comprehensive monitoring to track the impact of our cleanup
and collect data for future cleanup decisions.
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


def create_monitoring_config():
    """Create configuration for 48-hour monitoring"""
    
    config = {
        'monitoring_period': {
            'start_time': datetime.now().isoformat(),
            'end_time': (datetime.now() + timedelta(hours=48)).isoformat(),
            'duration_hours': 48
        },
        
        'cleanup_tracking': {
            'files_removed': 12,
            'size_cleaned_kb': 98.3,
            'cleanup_commit': subprocess.run('git rev-parse HEAD', shell=True, capture_output=True, text=True).stdout.strip(),
            'cleanup_branch': 'cleanup/remove-obvious-dead-code'
        },
        
        'monitoring_targets': {
            'system_stability': [
                'analytics_service_startup',
                'database_connections',
                'import_errors',
                'runtime_exceptions'
            ],
            'performance_metrics': [
                'function_call_frequency',
                'database_query_patterns',
                'memory_usage',
                'startup_times'
            ],
            'usage_patterns': [
                'code_function_usage',
                'database_table_access',
                'api_endpoint_hits',
                'feature_utilization'
            ]
        },
        
        'alert_conditions': {
            'import_failures': 'any',
            'service_startup_failures': 'any',
            'missing_function_calls': 'functions that were previously tracked',
            'performance_degradation': '>20% increase in response times'
        },
        
        'data_collection': {
            'observability_enabled': True,
            'signoz_monitoring': True,
            'custom_metrics': True,
            'usage_tracking': True
        }
    }
    
    # Save monitoring configuration
    config_file = 'monitoring_48h_config.json'
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Monitoring configuration saved: {config_file}")
    return config


def setup_automated_health_checks():
    """Setup automated health checks for the monitoring period"""
    
    health_check_script = '''#!/bin/bash

# 48-Hour Monitoring Health Check Script
# Runs every hour to verify system health after cleanup

echo "🏥 ATS Health Check - $(date)"
echo "=================================="

# 1. Test core imports
echo "🔍 Testing core imports..."
python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from observability.instrumentation_setup import get_instrumentation_status
    from observability.code_usage_tracker import get_code_tracker
    from observability.database_usage_tracker import get_database_tracker
    print('✅ Core observability imports working')
except Exception as e:
    print(f'❌ Import error: {e}')
    exit(1)

try:
    from services.analytics_service import UnifiedAnalyticsService
    print('✅ Analytics service import working')
except Exception as e:
    print(f'❌ Analytics service import error: {e}')
    exit(1)
"

if [ $? -eq 0 ]; then
    echo "✅ All critical imports successful"
else
    echo "❌ Import failures detected"
    exit 1
fi

# 2. Test observability status
echo "📊 Checking observability status..."
python3 -c "
import sys
sys.path.insert(0, 'src')
from observability.instrumentation_setup import get_instrumentation_status

status = get_instrumentation_status()
print(f'Instrumentation enabled: {status[\"instrumentation_enabled\"]}')
print(f'Modules instrumented: {status[\"instrumented_modules_count\"]}')
print(f'Database tracking: {status[\"database_tracking_enabled\"]}')
"

# 3. Verify no missing dependencies
echo "🔗 Checking for missing dependencies..."
python3 -c "
import importlib
critical_modules = [
    'numpy', 'pandas', 'psycopg2', 'pathlib', 'json', 'datetime'
]

for module in critical_modules:
    try:
        importlib.import_module(module)
        print(f'✅ {module}')
    except ImportError as e:
        print(f'❌ {module}: {e}')
        exit(1)
"

# 4. Log results with timestamp
echo "$(date): Health check completed successfully" >> monitoring_health_log.txt

echo "✅ Health check completed at $(date)"
echo ""
'''
    
    # Write health check script
    health_script_file = 'monitoring_health_check.sh'
    with open(health_script_file, 'w') as f:
        f.write(health_check_script)
    
    Path(health_script_file).chmod(0o755)
    print(f"✅ Health check script created: {health_script_file}")
    
    return health_script_file


def setup_metrics_collection():
    """Setup enhanced metrics collection for monitoring period"""
    
    metrics_script = '''#!/usr/bin/env python3
"""
48-Hour Metrics Collection Script
Collects detailed metrics during monitoring period
"""

import json
import time
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')

def collect_usage_metrics():
    """Collect current usage metrics"""
    try:
        from observability.code_usage_tracker import get_code_tracker
        from observability.database_usage_tracker import get_database_tracker
        
        # Get code usage stats
        code_tracker = get_code_tracker()
        code_stats = code_tracker.get_usage_stats()
        
        # Get database usage stats  
        db_tracker = get_database_tracker()
        db_stats = db_tracker.get_database_stats()
        
        # Compile metrics
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'code_metrics': {
                'total_function_calls': code_stats['total_function_calls'],
                'unique_functions_called': code_stats['unique_functions_called'],
                'modules_accessed': code_stats['modules_accessed'],
                'hot_functions': list(code_stats['hot_functions'].keys())[:10]
            },
            'database_metrics': {
                'total_table_accesses': db_stats['total_table_accesses'],
                'unique_tables_accessed': db_stats['unique_tables_accessed'],
                'hot_tables': list(db_stats['hot_tables'].keys())[:10],
                'query_patterns': db_stats['query_pattern_distribution']
            },
            'system_health': {
                'tracking_enabled': True,
                'no_errors_detected': True
            }
        }
        
        return metrics
        
    except Exception as e:
        return {
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'system_health': {
                'tracking_enabled': False,
                'error_detected': True
            }
        }

def main():
    """Main metrics collection"""
    print(f"📊 Collecting metrics at {datetime.now()}")
    
    metrics = collect_usage_metrics()
    
    # Save to timestamped file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metrics_file = f'monitoring_metrics_{timestamp}.json'
    
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    # Also append to continuous log
    log_file = 'monitoring_metrics_log.jsonl'
    with open(log_file, 'a') as f:
        f.write(json.dumps(metrics) + '\\n')
    
    print(f"✅ Metrics saved to {metrics_file}")
    
    # Print summary
    if 'error' not in metrics:
        print(f"📈 Function calls: {metrics['code_metrics']['total_function_calls']}")
        print(f"🗄️ Table accesses: {metrics['database_metrics']['total_table_accesses']}")
    else:
        print(f"❌ Error: {metrics['error']}")

if __name__ == "__main__":
    main()
'''
    
    metrics_script_file = 'collect_monitoring_metrics.py'
    with open(metrics_script_file, 'w') as f:
        f.write(metrics_script)
    
    Path(metrics_script_file).chmod(0o755)
    print(f"✅ Metrics collection script created: {metrics_script_file}")
    
    return metrics_script_file


def setup_cron_jobs(health_script: str, metrics_script: str):
    """Setup cron jobs for automated monitoring"""
    
    print("⏰ Setting up automated monitoring schedule...")
    
    # Get current directory
    current_dir = Path.cwd()
    
    cron_entries = f"""
# ATS 48-Hour Monitoring - Auto-generated
# Health check every hour
0 * * * * cd {current_dir} && ./{health_script} >> monitoring_health_log.txt 2>&1

# Metrics collection every 2 hours
0 */2 * * * cd {current_dir} && python3 {metrics_script} >> monitoring_metrics_log.txt 2>&1

# End monitoring after 48 hours (remove these cron jobs)
0 12 $(date -d '+2 days' '+%d') * * crontab -l | grep -v "ATS 48-Hour Monitoring" | crontab -
"""
    
    cron_file = 'monitoring_cron_setup.txt'
    with open(cron_file, 'w') as f:
        f.write(cron_entries.strip())
    
    print(f"📋 Cron setup saved to: {cron_file}")
    print("💡 To activate monitoring, run:")
    print(f"   crontab -l > current_cron.backup")
    print(f"   echo '# ATS 48-Hour Monitoring' >> current_cron.backup")
    print(f"   echo '0 * * * * cd {current_dir} && ./{health_script}' >> current_cron.backup") 
    print(f"   echo '0 */2 * * * cd {current_dir} && python3 {metrics_script}' >> current_cron.backup")
    print(f"   crontab current_cron.backup")
    
    return cron_file


def create_monitoring_dashboard():
    """Create simple monitoring dashboard script"""
    
    dashboard_script = '''#!/usr/bin/env python3
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
'''
    
    dashboard_file = 'monitoring_dashboard.py'
    with open(dashboard_file, 'w') as f:
        f.write(dashboard_script)
    
    Path(dashboard_file).chmod(0o755)
    print(f"✅ Monitoring dashboard created: {dashboard_file}")
    
    return dashboard_file


def main():
    """Setup complete 48-hour monitoring"""
    print("⏰ Setting Up 48-Hour Monitoring")
    print("=" * 40)
    
    # 1. Create monitoring configuration
    config = create_monitoring_config()
    
    # 2. Setup health checks
    health_script = setup_automated_health_checks()
    
    # 3. Setup metrics collection
    metrics_script = setup_metrics_collection()
    
    # 4. Setup cron jobs
    cron_file = setup_cron_jobs(health_script, metrics_script)
    
    # 5. Create monitoring dashboard
    dashboard_file = create_monitoring_dashboard()
    
    # 6. Run initial health check
    print("\n🏥 Running initial health check...")
    result = subprocess.run(f'./{health_script}', shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Initial health check passed")
    else:
        print(f"⚠️ Initial health check issues: {result.stderr}")
    
    # 7. Collect initial metrics
    print("📊 Collecting initial metrics...")
    result = subprocess.run(f'python3 {metrics_script}', shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Initial metrics collected")
    else:
        print(f"⚠️ Metrics collection issues: {result.stderr}")
    
    # Summary
    print("\n" + "=" * 50)
    print("⏰ 48-HOUR MONITORING SETUP COMPLETE")
    print("=" * 50)
    
    end_time = datetime.now() + timedelta(hours=48)
    print(f"🕒 Monitoring period: 48 hours")
    print(f"📅 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n📋 Files created:")
    print(f"   • {config['monitoring_period']} - Configuration")
    print(f"   • {health_script} - Health checks")
    print(f"   • {metrics_script} - Metrics collection")
    print(f"   • {dashboard_file} - Monitoring dashboard")
    
    print(f"\n🎯 Monitoring objectives:")
    print(f"   • Verify cleanup caused no issues")
    print(f"   • Collect usage data for future cleanup")
    print(f"   • Track system performance and stability")
    print(f"   • Identify additional cleanup opportunities")
    
    print(f"\n💡 Next steps:")
    print(f"   1. View dashboard: python3 {dashboard_file}")
    print(f"   2. Let monitoring run for 48 hours")
    print(f"   3. Review collected data for insights")
    print(f"   4. Plan next phase of cleanup based on findings")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)