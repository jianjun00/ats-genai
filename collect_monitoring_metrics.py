#!/usr/bin/env python3
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
        f.write(json.dumps(metrics) + '\n')
    
    print(f"✅ Metrics saved to {metrics_file}")
    
    # Print summary
    if 'error' not in metrics:
        print(f"📈 Function calls: {metrics['code_metrics']['total_function_calls']}")
        print(f"🗄️ Table accesses: {metrics['database_metrics']['total_table_accesses']}")
    else:
        print(f"❌ Error: {metrics['error']}")

if __name__ == "__main__":
    main()
