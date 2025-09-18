"""
System Monitor with Fail-Fast Exception Handling

This refactored version eliminates exception masking to reveal real issues.
Exceptions are handled specifically with actionable error messages.

BEFORE: Generic exception handling masked real issues
AFTER: Specific exception handling with clear error propagation
"""

import asyncio
import logging
import psutil
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Optional
import asyncpg
from pathlib import Path

# Custom exceptions for specific error scenarios
class SystemMonitorError(Exception):
    """Base exception for system monitor issues"""
    pass

class DatabaseConnectionError(SystemMonitorError):
    """Specific exception for database connection issues"""
    pass

class MetricsStorageError(SystemMonitorError):
    """Specific exception for metrics storage issues"""
    pass

class ProcessMonitoringError(SystemMonitorError):
    """Specific exception for process monitoring issues"""
    pass

class AlertSystemError(SystemMonitorError):
    """Specific exception for alert system issues"""
    pass


@dataclass
class SystemMetrics:
    """System metrics data structure"""
    timestamp: str
    cpu_percent: float
    memory_percent: float
    memory_total_gb: float
    disk_usage_percent: float
    disk_total_gb: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    agent_process_memory_mb: float
    database_connections: int


class SystemMonitorFailFast:
    """System monitor with fail-fast exception handling"""
    
    def __init__(self, agent_id: str, environment: str = "dev"):
        self.agent_id = agent_id
        self.environment = environment
        self.logger = logging.getLogger(__name__)
        self.monitoring_active = False
        self.metrics_history: List[SystemMetrics] = []
        
        # Set up metrics storage path
        self.metrics_file = Path(f"/tmp/system_metrics_{agent_id}_{environment}.jsonl")
        
    async def start_monitoring(self, interval_seconds: int = 60):
        """Start system monitoring - FAIL FAST ON CRITICAL ISSUES"""
        if self.monitoring_active:
            raise SystemMonitorError("System monitoring is already active")
            
        self.monitoring_active = True
        self.logger.info(f"Starting system health monitoring (interval: {interval_seconds}s)")
        
        # NO generic exception handling - let critical monitoring failures propagate
        while self.monitoring_active:
            # Collect metrics - let collection failures propagate immediately
            metrics = await self._collect_system_metrics()
            
            # Store metrics - let storage failures propagate (critical for monitoring)
            await self._store_metrics(metrics)
            
            # Analyze health - let analysis failures propagate (critical alerts)
            await self._analyze_health(metrics)
            
            # Cleanup old data - let cleanup failures propagate (disk space issues)
            await self._cleanup_old_data()
            
            await asyncio.sleep(interval_seconds)
    
    async def stop_monitoring(self):
        """Stop system monitoring"""
        self.monitoring_active = False
        self.logger.info("System health monitoring stopped")
    
    async def _collect_system_metrics(self) -> SystemMetrics:
        """Collect system metrics - FAIL FAST ON SYSTEM ISSUES"""
        try:
            # CPU and Memory - let psutil errors propagate
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Disk usage - let disk access errors propagate
            disk = psutil.disk_usage('/')
            
            # Network - let network monitoring errors propagate
            net_io = psutil.net_io_counters()
            
            # Process count - let process enumeration errors propagate
            process_count = len(psutil.pids())
            
        except psutil.Error as e:
            # Specific handling for psutil errors with actionable message
            raise ProcessMonitoringError(
                f"Failed to collect system metrics via psutil: {e}. "
                f"System may have permission issues or be under extreme load."
            )
        
        # Agent process memory with specific error handling
        agent_memory_mb = await self._get_agent_memory_usage()
        
        # Database connections with specific error handling  
        db_connections = await self._count_database_connections()
        
        return SystemMetrics(
            timestamp=datetime.now().isoformat(),
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_total_gb=memory.total / (1024**3),
            disk_usage_percent=(disk.used / disk.total) * 100,
            disk_total_gb=disk.total / (1024**3),
            network_bytes_sent=net_io.bytes_sent,
            network_bytes_recv=net_io.bytes_recv,
            process_count=process_count,
            agent_process_memory_mb=agent_memory_mb,
            database_connections=db_connections
        )
    
    async def _get_agent_memory_usage(self) -> float:
        """Get agent process memory usage - SPECIFIC ERROR HANDLING"""
        try:
            current_process = psutil.Process()
            return current_process.memory_info().rss / 1024 / 1024
            
        except psutil.NoSuchProcess:
            # Current process doesn't exist - this is a critical issue
            raise ProcessMonitoringError(
                "Current process no longer exists. Agent may be terminating."
            )
        except psutil.AccessDenied:
            # Permission issue - actionable error message
            raise ProcessMonitoringError(
                "Permission denied accessing process memory information. "
                "Agent may need elevated privileges."
            )
        except psutil.Error as e:
            # Other psutil errors with context
            raise ProcessMonitoringError(
                f"Failed to get agent memory usage: {e}"
            )
    
    async def _count_database_connections(self) -> int:
        """Count database connections - FAIL FAST ON DATABASE ISSUES"""
        try:
            # This would normally use the environment's database connection
            # For demo purposes, showing the pattern
            connection_query = """
                SELECT count(*) 
                FROM pg_stat_activity 
                WHERE state = 'active'
            """
            
            # Connection timeout should be explicit, not hidden
            conn = await asyncpg.connect(
                host="localhost", 
                port=5432, 
                user="postgres", 
                password="password",
                database="monitoring",
                timeout=10  # Explicit timeout
            )
            
            try:
                result = await conn.fetchval(connection_query)
                return int(result)
            finally:
                await conn.close()
                
        except asyncio.TimeoutError:
            # FAIL FAST - Don't mask timeout errors
            raise DatabaseConnectionError(
                "Database connection timeout after 10s. "
                "Database may be down or overloaded."
            )
        except asyncpg.PostgresConnectionError as e:
            # Specific connection error with actionable message
            raise DatabaseConnectionError(
                f"Cannot connect to PostgreSQL database: {e}. "
                f"Check database server status and connection parameters."
            )
        except asyncpg.PostgresError as e:
            # Database query error with context
            raise DatabaseConnectionError(
                f"PostgreSQL query error while counting connections: {e}. "
                f"Database may have permission or schema issues."
            )
    
    async def _store_metrics(self, metrics: SystemMetrics):
        """Store metrics - FAIL FAST ON STORAGE ISSUES"""
        # Add to memory history
        self.metrics_history.append(metrics)
        
        # Keep only last 24 hours (assuming 1 minute intervals)
        if len(self.metrics_history) > 1440:
            self.metrics_history.pop(0)
        
        # Store to file with specific error handling
        try:
            # Ensure directory exists
            self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.metrics_file, 'a') as f:
                f.write(json.dumps(asdict(metrics)) + '\n')
                
        except PermissionError:
            # Specific permission error with actionable message
            raise MetricsStorageError(
                f"Permission denied writing to metrics file: {self.metrics_file}. "
                f"Check file permissions and disk space."
            )
        except OSError as e:
            # Disk space or filesystem errors
            raise MetricsStorageError(
                f"Failed to write metrics to {self.metrics_file}: {e}. "
                f"Check disk space and filesystem health."
            )
        except json.JSONEncodeError as e:
            # Data serialization error
            raise MetricsStorageError(
                f"Failed to serialize metrics data: {e}. "
                f"Metrics data may be corrupted: {metrics}"
            )
    
    async def _analyze_health(self, metrics: SystemMetrics):
        """Analyze system health - FAIL FAST ON ALERT SYSTEM ISSUES"""
        alert_data = {
            "cpu_percent": metrics.cpu_percent,
            "memory_percent": metrics.memory_percent,
            "disk_usage_percent": metrics.disk_usage_percent,
            "agent_memory_mb": metrics.agent_process_memory_mb,
            "db_connections": metrics.database_connections,
            "timestamp": metrics.timestamp
        }
        
        # Alert system integration with specific error handling
        try:
            from agents.alert_manager import get_alert_manager
            alert_manager = get_alert_manager(self.agent_id)
            await alert_manager.evaluate_alert_rules(alert_data, "system_monitor")
            
        except ImportError:
            # Missing alert system dependency
            raise AlertSystemError(
                "Alert manager module not available. "
                "Install alert system dependencies or disable alert evaluation."
            )
        except AttributeError as e:
            # Alert manager API mismatch
            raise AlertSystemError(
                f"Alert manager API incompatible: {e}. "
                f"Update alert manager or system monitor integration."
            )
        except Exception as e:
            # Unexpected alert system error - but with context
            raise AlertSystemError(
                f"Alert evaluation failed unexpectedly: {e}. "
                f"Alert system may have internal issues."
            )
    
    async def _cleanup_old_data(self):
        """Cleanup old metrics data - FAIL FAST ON CLEANUP ISSUES"""
        if not self.metrics_file.exists():
            return
            
        try:
            # Read all lines
            with open(self.metrics_file, 'r') as f:
                lines = f.readlines()
            
            # Filter out entries older than 7 days
            cutoff_time = datetime.now() - timedelta(days=7)
            recent_lines = []
            
            for line in lines:
                try:
                    data = json.loads(line.strip())
                    metric_time = datetime.fromisoformat(data['timestamp'])
                    if metric_time > cutoff_time:
                        recent_lines.append(line)
                except (json.JSONDecodeError, KeyError, ValueError):
                    # Skip malformed lines but continue processing
                    self.logger.warning(f"Skipping malformed metrics line: {line.strip()}")
                    continue
            
            # Write back only recent data
            with open(self.metrics_file, 'w') as f:
                f.writelines(recent_lines)
                
        except PermissionError:
            # Specific permission error
            raise MetricsStorageError(
                f"Permission denied during metrics cleanup: {self.metrics_file}"
            )
        except OSError as e:
            # Filesystem errors during cleanup
            raise MetricsStorageError(
                f"Filesystem error during metrics cleanup: {e}"
            )
    
    async def get_recent_metrics(self, hours: int = 24) -> List[SystemMetrics]:
        """Get recent metrics - FAIL FAST ON DATA ACCESS ISSUES"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_metrics = [
            m for m in self.metrics_history 
            if datetime.fromisoformat(m.timestamp) > cutoff_time
        ]
        
        if not recent_metrics:
            # No recent metrics is a monitoring issue
            raise SystemMonitorError(
                f"No system metrics available for last {hours} hours. "
                f"System monitoring may not be working properly."
            )
        
        return recent_metrics
    
    async def get_health_summary(self) -> dict:
        """Get health summary - FAIL FAST ON HEALTH CHECK ISSUES"""
        if not self.metrics_history:
            raise SystemMonitorError(
                "No metrics available for health summary. "
                "System monitoring may not be initialized."
            )
        
        latest_metrics = self.metrics_history[-1]
        
        # Health thresholds (configurable in production)
        health_status = {
            "overall_status": "healthy",
            "timestamp": latest_metrics.timestamp,
            "issues": []
        }
        
        # Check specific health indicators
        if latest_metrics.cpu_percent > 80:
            health_status["issues"].append(f"High CPU usage: {latest_metrics.cpu_percent:.1f}%")
            health_status["overall_status"] = "warning"
        
        if latest_metrics.memory_percent > 85:
            health_status["issues"].append(f"High memory usage: {latest_metrics.memory_percent:.1f}%")
            health_status["overall_status"] = "warning"
        
        if latest_metrics.disk_usage_percent > 90:
            health_status["issues"].append(f"High disk usage: {latest_metrics.disk_usage_percent:.1f}%")
            health_status["overall_status"] = "critical"
        
        if latest_metrics.database_connections > 100:
            health_status["issues"].append(f"High DB connections: {latest_metrics.database_connections}")
            health_status["overall_status"] = "warning"
        
        return health_status