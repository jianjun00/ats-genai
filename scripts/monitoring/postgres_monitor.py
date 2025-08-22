#!/usr/bin/env python3
"""
PostgreSQL Monitoring Dashboard

Monitors PostgreSQL database performance, connections, load, and worker processes.
Works without requiring database authentication - uses system-level monitoring.
"""

import subprocess
import psutil
import time
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

@dataclass
class PostgreSQLMetrics:
    """PostgreSQL performance metrics"""
    timestamp: datetime
    
    # Process metrics
    total_processes: int
    worker_processes: int
    connection_processes: int
    background_processes: int
    
    # System resource usage
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    
    # Connection metrics  
    max_connections: Optional[int]
    active_connections: int
    idle_connections: int
    
    # Database activity
    databases_count: int
    postgres_uptime_seconds: float
    
    # Load metrics
    load_1min: float
    load_5min: float
    load_15min: float
    
    # Disk usage
    data_directory_size_mb: float
    disk_usage_percent: float

class PostgreSQLMonitor:
    """Monitor PostgreSQL without requiring database authentication"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.postgres_processes = []
        self.main_postgres_pid = None
        self._find_postgres_processes()
    
    def _find_postgres_processes(self):
        """Find all PostgreSQL processes"""
        try:
            self.postgres_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    if 'postgres' in proc.info['name'].lower():
                        self.postgres_processes.append(proc)
                        
                        # Identify main postgres process
                        if proc.info['cmdline'] and any('postgres' in arg for arg in proc.info['cmdline']):
                            if not any('background' in arg for arg in proc.info['cmdline']):
                                if self.main_postgres_pid is None:
                                    self.main_postgres_pid = proc.info['pid']
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            self.logger.info(f"Found {len(self.postgres_processes)} PostgreSQL processes")
        except Exception as e:
            self.logger.error(f"Error finding PostgreSQL processes: {e}")
    
    def get_current_metrics(self) -> PostgreSQLMetrics:
        """Get current PostgreSQL metrics"""
        
        # Refresh process list
        self._find_postgres_processes()
        
        # Process metrics
        total_processes = len(self.postgres_processes)
        worker_processes = self._count_worker_processes()
        connection_processes = self._count_connection_processes()
        background_processes = self._count_background_processes()
        
        # System resource usage
        cpu_percent, memory_mb, memory_percent = self._get_resource_usage()
        
        # Connection metrics (estimated)
        active_connections, idle_connections = self._estimate_connections()
        
        # System load
        load_1min, load_5min, load_15min = self._get_system_load()
        
        # Uptime
        uptime_seconds = self._get_postgres_uptime()
        
        # Disk usage
        data_dir_size, disk_usage_pct = self._get_disk_usage()
        
        return PostgreSQLMetrics(
            timestamp=datetime.now(),
            total_processes=total_processes,
            worker_processes=worker_processes,
            connection_processes=connection_processes,
            background_processes=background_processes,
            cpu_percent=cpu_percent,
            memory_mb=memory_mb,
            memory_percent=memory_percent,
            max_connections=None,  # Would need DB access to get this
            active_connections=active_connections,
            idle_connections=idle_connections,
            databases_count=0,  # Would need DB access
            postgres_uptime_seconds=uptime_seconds,
            load_1min=load_1min,
            load_5min=load_5min,
            load_15min=load_15min,
            data_directory_size_mb=data_dir_size,
            disk_usage_percent=disk_usage_pct
        )
    
    def _count_worker_processes(self) -> int:
        """Count PostgreSQL worker processes"""
        worker_count = 0
        worker_keywords = ['background writer', 'checkpointer', 'walwriter', 'autovacuum', 'scheduler']
        
        for proc in self.postgres_processes:
            try:
                cmdline = ' '.join(proc.cmdline())
                if any(keyword in cmdline.lower() for keyword in worker_keywords):
                    worker_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return worker_count
    
    def _count_connection_processes(self) -> int:
        """Count client connection processes"""
        connection_count = 0
        
        for proc in self.postgres_processes:
            try:
                cmdline = ' '.join(proc.cmdline())
                # Look for client connections (usually have database name and user)
                if '@' in cmdline or 'postgres:' in cmdline:
                    if not any(bg in cmdline.lower() for bg in ['background', 'launcher', 'writer', 'scheduler']):
                        connection_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return connection_count
    
    def _count_background_processes(self) -> int:
        """Count background PostgreSQL processes"""
        background_count = 0
        background_keywords = ['timescaledb background worker', 'logical replication']
        
        for proc in self.postgres_processes:
            try:
                cmdline = ' '.join(proc.cmdline()).lower()
                if any(keyword in cmdline for keyword in background_keywords):
                    background_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return background_count
    
    def _get_resource_usage(self) -> tuple[float, float, float]:
        """Get CPU and memory usage for all PostgreSQL processes"""
        total_cpu = 0.0
        total_memory_mb = 0.0
        
        for proc in self.postgres_processes:
            try:
                proc_obj = psutil.Process(proc.pid)
                total_cpu += proc_obj.cpu_percent()
                memory_info = proc_obj.memory_info()
                total_memory_mb += memory_info.rss / (1024 * 1024)  # Convert to MB
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # Calculate memory percentage of total system memory
        total_system_memory = psutil.virtual_memory().total / (1024 * 1024)  # MB
        memory_percent = (total_memory_mb / total_system_memory) * 100 if total_system_memory > 0 else 0
        
        return total_cpu, total_memory_mb, memory_percent
    
    def _estimate_connections(self) -> tuple[int, int]:
        """Estimate active vs idle connections based on CPU usage"""
        active_connections = 0
        idle_connections = 0
        
        for proc in self.postgres_processes:
            try:
                cmdline = ' '.join(proc.cmdline())
                # Skip if it's a background process
                if any(bg in cmdline.lower() for bg in ['background', 'launcher', 'writer', 'scheduler', 'autovacuum']):
                    continue
                
                # If it looks like a client connection
                if '@' in cmdline or 'postgres:' in cmdline:
                    proc_obj = psutil.Process(proc.pid)
                    cpu_percent = proc_obj.cpu_percent()
                    
                    # If using CPU, probably active; otherwise idle
                    if cpu_percent > 0.1:
                        active_connections += 1
                    else:
                        idle_connections += 1
                        
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return active_connections, idle_connections
    
    def _get_system_load(self) -> tuple[float, float, float]:
        """Get system load averages"""
        try:
            load_avg = psutil.getloadavg()
            return load_avg[0], load_avg[1], load_avg[2]
        except AttributeError:
            # getloadavg not available on all platforms
            return 0.0, 0.0, 0.0
    
    def _get_postgres_uptime(self) -> float:
        """Get PostgreSQL uptime in seconds"""
        if self.main_postgres_pid:
            try:
                proc = psutil.Process(self.main_postgres_pid)
                create_time = proc.create_time()
                return time.time() - create_time
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Fallback: use oldest postgres process
        oldest_time = None
        for proc in self.postgres_processes:
            try:
                proc_obj = psutil.Process(proc.pid)
                create_time = proc_obj.create_time()
                if oldest_time is None or create_time < oldest_time:
                    oldest_time = create_time
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return time.time() - oldest_time if oldest_time else 0.0
    
    def _get_disk_usage(self) -> tuple[float, float]:
        """Get PostgreSQL data directory disk usage"""
        # Common PostgreSQL data directory locations
        possible_data_dirs = [
            "/var/lib/postgresql/16/main",
            "/var/lib/postgresql/15/main", 
            "/var/lib/postgresql/14/main",
            "/var/lib/postgresql/data",
            "/usr/local/var/postgres"
        ]
        
        data_dir_size_mb = 0.0
        disk_usage_percent = 0.0
        
        for data_dir in possible_data_dirs:
            try:
                if psutil.os.path.exists(data_dir):
                    # Get directory size
                    total_size = 0
                    for dirpath, dirnames, filenames in psutil.os.walk(data_dir):
                        for filename in filenames:
                            filepath = psutil.os.path.join(dirpath, filename)
                            try:
                                total_size += psutil.os.path.getsize(filepath)
                            except (OSError, IOError):
                                continue
                    
                    data_dir_size_mb = total_size / (1024 * 1024)  # Convert to MB
                    
                    # Get disk usage percentage for the filesystem containing the data directory
                    disk_usage = psutil.disk_usage(data_dir)
                    disk_usage_percent = (disk_usage.used / disk_usage.total) * 100
                    break
                    
            except (OSError, IOError, psutil.AccessDenied):
                continue
        
        return data_dir_size_mb, disk_usage_percent
    
    def get_process_details(self) -> List[Dict[str, Any]]:
        """Get detailed information about each PostgreSQL process"""
        process_details = []
        
        for proc in self.postgres_processes:
            try:
                proc_obj = psutil.Process(proc.pid)
                
                # Categorize process type
                cmdline = ' '.join(proc_obj.cmdline())
                process_type = self._categorize_process(cmdline)
                
                details = {
                    'pid': proc.pid,
                    'type': process_type,
                    'cmdline': cmdline,
                    'cpu_percent': proc_obj.cpu_percent(),
                    'memory_mb': proc_obj.memory_info().rss / (1024 * 1024),
                    'create_time': datetime.fromtimestamp(proc_obj.create_time()).isoformat(),
                    'status': proc_obj.status(),
                    'num_threads': proc_obj.num_threads()
                }
                
                process_details.append(details)
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return sorted(process_details, key=lambda x: x['cpu_percent'], reverse=True)
    
    def _categorize_process(self, cmdline: str) -> str:
        """Categorize PostgreSQL process type"""
        cmdline_lower = cmdline.lower()
        
        if 'checkpointer' in cmdline_lower:
            return 'checkpointer'
        elif 'background writer' in cmdline_lower:
            return 'background_writer'
        elif 'walwriter' in cmdline_lower:
            return 'wal_writer'
        elif 'autovacuum launcher' in cmdline_lower:
            return 'autovacuum_launcher'
        elif 'autovacuum worker' in cmdline_lower:
            return 'autovacuum_worker'
        elif 'timescaledb background worker launcher' in cmdline_lower:
            return 'timescaledb_launcher'
        elif 'timescaledb background worker scheduler' in cmdline_lower:
            return 'timescaledb_scheduler'
        elif 'logical replication launcher' in cmdline_lower:
            return 'logical_replication'
        elif '@' in cmdline or 'postgres:' in cmdline:
            return 'client_connection'
        elif '/postgres' in cmdline or 'postgres -d' in cmdline:
            return 'main_server'
        else:
            return 'unknown'
    
    def generate_monitoring_report(self, include_processes: bool = True) -> Dict[str, Any]:
        """Generate comprehensive monitoring report"""
        metrics = self.get_current_metrics()
        
        report = {
            'timestamp': metrics.timestamp.isoformat(),
            'summary': {
                'status': 'healthy' if metrics.total_processes > 0 else 'down',
                'uptime_hours': metrics.postgres_uptime_seconds / 3600,
                'total_processes': metrics.total_processes,
                'cpu_usage_percent': metrics.cpu_percent,
                'memory_usage_mb': metrics.memory_mb,
                'memory_usage_percent': metrics.memory_percent
            },
            'processes': {
                'total': metrics.total_processes,
                'worker_processes': metrics.worker_processes,
                'connection_processes': metrics.connection_processes,
                'background_processes': metrics.background_processes,
                'estimated_active_connections': metrics.active_connections,
                'estimated_idle_connections': metrics.idle_connections
            },
            'system_load': {
                'load_1min': metrics.load_1min,
                'load_5min': metrics.load_5min,
                'load_15min': metrics.load_15min
            },
            'disk': {
                'data_directory_size_mb': metrics.data_directory_size_mb,
                'disk_usage_percent': metrics.disk_usage_percent
            },
            'raw_metrics': asdict(metrics)
        }
        
        if include_processes:
            report['process_details'] = self.get_process_details()
        
        # Add health assessment
        report['health_assessment'] = self._assess_health(metrics)
        
        return report
    
    def _assess_health(self, metrics: PostgreSQLMetrics) -> Dict[str, Any]:
        """Assess PostgreSQL health based on metrics"""
        issues = []
        warnings = []
        
        # Check if PostgreSQL is running
        if metrics.total_processes == 0:
            issues.append("PostgreSQL appears to be down - no processes found")
            return {'status': 'critical', 'issues': issues, 'warnings': warnings}
        
        # Check resource usage
        if metrics.cpu_percent > 80:
            issues.append(f"High CPU usage: {metrics.cpu_percent:.1f}%")
        elif metrics.cpu_percent > 60:
            warnings.append(f"Elevated CPU usage: {metrics.cpu_percent:.1f}%")
        
        if metrics.memory_percent > 80:
            issues.append(f"High memory usage: {metrics.memory_percent:.1f}%")
        elif metrics.memory_percent > 60:
            warnings.append(f"Elevated memory usage: {metrics.memory_percent:.1f}%")
        
        # Check system load
        if metrics.load_1min > 8:
            issues.append(f"High system load: {metrics.load_1min:.2f}")
        elif metrics.load_1min > 4:
            warnings.append(f"Elevated system load: {metrics.load_1min:.2f}")
        
        # Check disk usage
        if metrics.disk_usage_percent > 90:
            issues.append(f"Disk space critical: {metrics.disk_usage_percent:.1f}% used")
        elif metrics.disk_usage_percent > 80:
            warnings.append(f"Disk space getting low: {metrics.disk_usage_percent:.1f}% used")
        
        # Check process counts
        if metrics.worker_processes < 3:
            warnings.append(f"Few worker processes: {metrics.worker_processes}")
        
        # Determine overall status
        if issues:
            status = 'critical'
        elif warnings:
            status = 'warning'
        else:
            status = 'healthy'
        
        return {
            'status': status,
            'issues': issues,
            'warnings': warnings,
            'recommendations': self._get_recommendations(metrics, issues, warnings)
        }
    
    def _get_recommendations(self, metrics: PostgreSQLMetrics, issues: List[str], warnings: List[str]) -> List[str]:
        """Generate recommendations based on health assessment"""
        recommendations = []
        
        if metrics.cpu_percent > 60:
            recommendations.append("Monitor query performance and consider query optimization")
            recommendations.append("Check for long-running queries or blocking processes")
        
        if metrics.memory_percent > 60:
            recommendations.append("Review PostgreSQL memory configuration (shared_buffers, work_mem)")
            recommendations.append("Consider increasing system memory or optimizing memory usage")
        
        if metrics.load_1min > 4:
            recommendations.append("Investigate system load - check for I/O bottlenecks")
            recommendations.append("Consider distributing load or optimizing queries")
        
        if metrics.disk_usage_percent > 80:
            recommendations.append("Plan for disk space expansion")
            recommendations.append("Consider archiving old data or implementing data retention policies")
        
        if metrics.connection_processes > 100:
            recommendations.append("Monitor connection pool usage")
            recommendations.append("Consider connection pooling with pgbouncer if not already implemented")
        
        if not recommendations:
            recommendations.append("System appears healthy - continue regular monitoring")
        
        return recommendations

def main():
    """Main monitoring function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor PostgreSQL without database authentication")
    parser.add_argument('--format', choices=['json', 'text'], default='text', 
                       help='Output format')
    parser.add_argument('--include-processes', action='store_true',
                       help='Include detailed process information')
    parser.add_argument('--watch', type=int, metavar='SECONDS',
                       help='Continuously monitor with specified interval')
    parser.add_argument('--output-file', help='Save output to file')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    monitor = PostgreSQLMonitor()
    
    def print_report():
        report = monitor.generate_monitoring_report(include_processes=args.include_processes)
        
        if args.format == 'json':
            output = json.dumps(report, indent=2, default=str)
        else:
            output = format_text_report(report)
        
        if args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(output)
            print(f"Report saved to {args.output_file}")
        else:
            print(output)
    
    # Single report or continuous monitoring
    if args.watch:
        try:
            while True:
                print_report()
                if not args.output_file:
                    print(f"\n{'='*60}\nNext update in {args.watch} seconds...\n")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
    else:
        print_report()

def format_text_report(report: Dict[str, Any]) -> str:
    """Format report as human-readable text"""
    lines = [
        "🐘 PostgreSQL Monitoring Report",
        "="*50,
        f"Timestamp: {report['timestamp']}",
        f"Status: {report['summary']['status'].upper()}",
        f"Uptime: {report['summary']['uptime_hours']:.1f} hours",
        "",
        "📊 SUMMARY:",
        f"  Processes: {report['summary']['total_processes']}",
        f"  CPU Usage: {report['summary']['cpu_usage_percent']:.1f}%",
        f"  Memory Usage: {report['summary']['memory_usage_mb']:.1f} MB ({report['summary']['memory_usage_percent']:.1f}%)",
        "",
        "⚙️  PROCESSES:",
        f"  Worker Processes: {report['processes']['worker_processes']}",
        f"  Connection Processes: {report['processes']['connection_processes']}",
        f"  Background Processes: {report['processes']['background_processes']}",
        f"  Active Connections: {report['processes']['estimated_active_connections']}",
        f"  Idle Connections: {report['processes']['estimated_idle_connections']}",
        "",
        "💻 SYSTEM LOAD:",
        f"  1 min: {report['system_load']['load_1min']:.2f}",
        f"  5 min: {report['system_load']['load_5min']:.2f}",
        f"  15 min: {report['system_load']['load_15min']:.2f}",
        "",
        "💾 DISK:",
        f"  Data Directory: {report['disk']['data_directory_size_mb']:.1f} MB",
        f"  Disk Usage: {report['disk']['disk_usage_percent']:.1f}%",
        ""
    ]
    
    # Health assessment
    health = report['health_assessment']
    status_emoji = {'healthy': '✅', 'warning': '⚠️', 'critical': '❌'}
    
    lines.extend([
        f"🏥 HEALTH: {status_emoji.get(health['status'], '❓')} {health['status'].upper()}",
    ])
    
    if health['issues']:
        lines.append("  ISSUES:")
        for issue in health['issues']:
            lines.append(f"    ❌ {issue}")
    
    if health['warnings']:
        lines.append("  WARNINGS:")
        for warning in health['warnings']:
            lines.append(f"    ⚠️  {warning}")
    
    if health['recommendations']:
        lines.append("  RECOMMENDATIONS:")
        for rec in health['recommendations']:
            lines.append(f"    💡 {rec}")
    
    # Process details if included
    if 'process_details' in report and report['process_details']:
        lines.extend([
            "",
            "🔍 PROCESS DETAILS:",
            f"{'PID':<8} {'Type':<20} {'CPU%':<6} {'Memory(MB)':<12} {'Status':<10}"
        ])
        lines.append("-" * 70)
        
        for proc in report['process_details'][:10]:  # Show top 10
            lines.append(f"{proc['pid']:<8} {proc['type']:<20} {proc['cpu_percent']:<6.1f} {proc['memory_mb']:<12.1f} {proc['status']:<10}")
    
    return "\n".join(lines)

if __name__ == "__main__":
    main()