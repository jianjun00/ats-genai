"""
System Health Monitor
====================

Real-time system health monitoring for the Data Quality Agent ecosystem.
Provides operational intelligence, alerting, and system diagnostics.
"""

import asyncio
import psutil
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import json

@dataclass
class SystemMetrics:
    """System resource metrics"""
    timestamp: str
    cpu_percent: float
    memory_percent: float
    memory_available_gb: float
    disk_usage_percent: float
    disk_free_gb: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    agent_process_memory_mb: float
    database_connections: int

@dataclass
class HealthAlert:
    """System health alert"""
    alert_id: str
    severity: str  # critical, warning, info
    component: str
    message: str
    timestamp: str
    resolved: bool = False
    metadata: Optional[Dict[str, Any]] = None

class SystemHealthMonitor:
    """Monitors system health and provides operational intelligence"""
    
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.logger = logging.getLogger(f"system_monitor.{agent_id}")
        
        # Health thresholds
        self.thresholds = {
            "cpu_warning": 70.0,
            "cpu_critical": 85.0,
            "memory_warning": 80.0,
            "memory_critical": 90.0,
            "disk_warning": 85.0,
            "disk_critical": 95.0,
            "agent_memory_warning": 512.0,  # MB
            "agent_memory_critical": 1024.0  # MB
        }
        
        # State tracking
        self.monitoring_active = False
        self.metrics_history: List[SystemMetrics] = []
        self.active_alerts: Dict[str, HealthAlert] = {}
        self.alert_history: List[HealthAlert] = []
        
        # Performance baselines
        self.baseline_metrics: Optional[SystemMetrics] = None
        self.metrics_file = Path(f"logs/system/system_metrics_{agent_id}.jsonl")
        self.alerts_file = Path(f"logs/system/alerts_{agent_id}.jsonl")
        
        # Ensure directories exist
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
    
    async def start_monitoring(self, interval_seconds: int = 60):
        """Start continuous system monitoring"""
        if self.monitoring_active:
            return
        
        self.monitoring_active = True
        self.logger.info(f"Starting system health monitoring (interval: {interval_seconds}s)")
        
        try:
            while self.monitoring_active:
                # Collect system metrics
                metrics = await self._collect_system_metrics()
                
                # Store metrics
                await self._store_metrics(metrics)
                
                # Analyze health and generate alerts
                await self._analyze_health(metrics)
                
                # Cleanup old data
                await self._cleanup_old_data()
                
                await asyncio.sleep(interval_seconds)
        
        except Exception as e:
            self.logger.error(f"System monitoring failed: {e}", exc_info=True)
            self.monitoring_active = False
    
    async def stop_monitoring(self):
        """Stop system monitoring"""
        self.monitoring_active = False
        self.logger.info("System health monitoring stopped")
    
    async def _collect_system_metrics(self) -> SystemMetrics:
        """Collect comprehensive system metrics"""
        try:
            # CPU and Memory
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Network
            net_io = psutil.net_io_counters()
            
            # Process count
            process_count = len(psutil.pids())
            
            # Agent process memory (if we can find it)
            agent_memory_mb = 0.0
            try:
                current_process = psutil.Process()
                agent_memory_mb = current_process.memory_info().rss / 1024 / 1024
            except:
                pass
            
            # Database connections (estimate)
            db_connections = await self._count_database_connections()
            
            return SystemMetrics(
                timestamp=datetime.now().isoformat(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                memory_available_gb=memory.available / 1024 / 1024 / 1024,
                disk_usage_percent=disk.percent,
                disk_free_gb=disk.free / 1024 / 1024 / 1024,
                network_bytes_sent=net_io.bytes_sent,
                network_bytes_recv=net_io.bytes_recv,
                process_count=process_count,
                agent_process_memory_mb=agent_memory_mb,
                database_connections=db_connections
            )
            
        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {e}")
            # Return empty metrics
            return SystemMetrics(
                timestamp=datetime.now().isoformat(),
                cpu_percent=0.0, memory_percent=0.0, memory_available_gb=0.0,
                disk_usage_percent=0.0, disk_free_gb=0.0,
                network_bytes_sent=0, network_bytes_recv=0,
                process_count=0, agent_process_memory_mb=0.0,
                database_connections=0
            )
    
    async def _count_database_connections(self) -> int:
        """Estimate database connection count"""
        try:
            import asyncpg
            # Try to connect and check active connections
            conn = await asyncpg.connect(
                host='ats-intg-postgres', port=5432,
                user='postgres', password='intg_password', database='intg_db'
            )
            
            result = await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'"
            )
            await conn.close()
            return result or 0
            
        except Exception:
            return 0
    
    async def _store_metrics(self, metrics: SystemMetrics):
        """Store metrics to file and memory"""
        # Add to history
        self.metrics_history.append(metrics)
        
        # Keep only last 24 hours of metrics (assuming 1 minute intervals)
        if len(self.metrics_history) > 1440:
            self.metrics_history.pop(0)
        
        # Store to file
        try:
            with open(self.metrics_file, 'a') as f:
                f.write(json.dumps(asdict(metrics)) + '\n')
        except Exception as e:
            self.logger.error(f"Failed to store metrics: {e}")
    
    async def _analyze_health(self, metrics: SystemMetrics):
        """Analyze system health and generate alerts"""
        current_time = datetime.now().isoformat()
        
        # Prepare data for alert evaluation
        alert_data = {
            "cpu_percent": metrics.cpu_percent,
            "memory_percent": metrics.memory_percent,
            "disk_usage_percent": metrics.disk_usage_percent,
            "agent_memory_mb": metrics.agent_process_memory_mb,
            "db_connections": metrics.database_connections,
            "timestamp": metrics.timestamp
        }
        
        # Trigger alert manager evaluation
        try:
            from agents.alert_manager import get_alert_manager
            alert_manager = get_alert_manager(self.agent_id)
            await alert_manager.evaluate_alert_rules(alert_data, "system_monitor")
        except Exception as e:
            self.logger.error(f"Failed to evaluate alert rules: {e}")
        
        # Legacy alert handling (keep for backward compatibility)
        # Check CPU
        if metrics.cpu_percent > self.thresholds["cpu_critical"]:
            await self._create_alert("cpu_critical", "system", 
                                   f"Critical CPU usage: {metrics.cpu_percent:.1f}%", 
                                   "critical", {"cpu_percent": metrics.cpu_percent})
        elif metrics.cpu_percent > self.thresholds["cpu_warning"]:
            await self._create_alert("cpu_warning", "system",
                                   f"High CPU usage: {metrics.cpu_percent:.1f}%",
                                   "warning", {"cpu_percent": metrics.cpu_percent})
        else:
            await self._resolve_alert("cpu_warning")
            await self._resolve_alert("cpu_critical")
        
        # Check Memory
        if metrics.memory_percent > self.thresholds["memory_critical"]:
            await self._create_alert("memory_critical", "system",
                                   f"Critical memory usage: {metrics.memory_percent:.1f}%",
                                   "critical", {"memory_percent": metrics.memory_percent})
        elif metrics.memory_percent > self.thresholds["memory_warning"]:
            await self._create_alert("memory_warning", "system",
                                   f"High memory usage: {metrics.memory_percent:.1f}%",
                                   "warning", {"memory_percent": metrics.memory_percent})
        else:
            await self._resolve_alert("memory_warning")
            await self._resolve_alert("memory_critical")
        
        # Check Disk
        if metrics.disk_usage_percent > self.thresholds["disk_critical"]:
            await self._create_alert("disk_critical", "system",
                                   f"Critical disk usage: {metrics.disk_usage_percent:.1f}%",
                                   "critical", {"disk_usage_percent": metrics.disk_usage_percent})
        elif metrics.disk_usage_percent > self.thresholds["disk_warning"]:
            await self._create_alert("disk_warning", "system",
                                   f"High disk usage: {metrics.disk_usage_percent:.1f}%",
                                   "warning", {"disk_usage_percent": metrics.disk_usage_percent})
        else:
            await self._resolve_alert("disk_warning")
            await self._resolve_alert("disk_critical")
        
        # Check Agent Memory
        if metrics.agent_process_memory_mb > self.thresholds["agent_memory_critical"]:
            await self._create_alert("agent_memory_critical", "agent",
                                   f"Critical agent memory usage: {metrics.agent_process_memory_mb:.1f}MB",
                                   "critical", {"agent_memory_mb": metrics.agent_process_memory_mb})
        elif metrics.agent_process_memory_mb > self.thresholds["agent_memory_warning"]:
            await self._create_alert("agent_memory_warning", "agent",
                                   f"High agent memory usage: {metrics.agent_process_memory_mb:.1f}MB",
                                   "warning", {"agent_memory_mb": metrics.agent_process_memory_mb})
        else:
            await self._resolve_alert("agent_memory_warning")
            await self._resolve_alert("agent_memory_critical")
    
    async def _create_alert(self, alert_id: str, component: str, message: str, 
                           severity: str, metadata: Dict[str, Any] = None):
        """Create or update an alert"""
        if alert_id in self.active_alerts:
            # Update existing alert
            self.active_alerts[alert_id].timestamp = datetime.now().isoformat()
            self.active_alerts[alert_id].metadata = metadata
            return
        
        # Create new alert
        alert = HealthAlert(
            alert_id=alert_id,
            severity=severity,
            component=component,
            message=message,
            timestamp=datetime.now().isoformat(),
            metadata=metadata
        )
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Log alert
        log_method = getattr(self.logger, severity.lower() if severity != "critical" else "error")
        log_method(f"ALERT: {message}")
        
        # Store alert to file
        try:
            with open(self.alerts_file, 'a') as f:
                f.write(json.dumps(asdict(alert)) + '\n')
        except Exception as e:
            self.logger.error(f"Failed to store alert: {e}")
    
    async def _resolve_alert(self, alert_id: str):
        """Resolve an active alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            del self.active_alerts[alert_id]
            
            self.logger.info(f"RESOLVED: {alert.message}")
    
    async def _cleanup_old_data(self):
        """Clean up old metrics and alerts"""
        # Keep only recent alerts (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        self.alert_history = [
            alert for alert in self.alert_history
            if datetime.fromisoformat(alert.timestamp) > cutoff_time
        ]
    
    async def get_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive health summary"""
        if not self.metrics_history:
            return {"status": "no_data", "message": "No metrics available"}
        
        latest_metrics = self.metrics_history[-1]
        
        # Calculate health score (0-100)
        health_score = 100.0
        
        # Deduct points for resource usage
        if latest_metrics.cpu_percent > self.thresholds["cpu_warning"]:
            health_score -= min(30, (latest_metrics.cpu_percent - self.thresholds["cpu_warning"]) * 2)
        
        if latest_metrics.memory_percent > self.thresholds["memory_warning"]:
            health_score -= min(30, (latest_metrics.memory_percent - self.thresholds["memory_warning"]) * 3)
        
        if latest_metrics.disk_usage_percent > self.thresholds["disk_warning"]:
            health_score -= min(20, (latest_metrics.disk_usage_percent - self.thresholds["disk_warning"]) * 2)
        
        # Deduct points for active alerts
        health_score -= len([a for a in self.active_alerts.values() if a.severity == "critical"]) * 20
        health_score -= len([a for a in self.active_alerts.values() if a.severity == "warning"]) * 10
        
        health_score = max(0, health_score)
        
        # Determine overall status
        if health_score >= 90:
            status = "excellent"
        elif health_score >= 75:
            status = "good"
        elif health_score >= 50:
            status = "warning"
        else:
            status = "critical"
        
        return {
            "status": status,
            "health_score": round(health_score, 1),
            "latest_metrics": asdict(latest_metrics),
            "active_alerts": [asdict(alert) for alert in self.active_alerts.values()],
            "alert_summary": {
                "critical": len([a for a in self.active_alerts.values() if a.severity == "critical"]),
                "warning": len([a for a in self.active_alerts.values() if a.severity == "warning"]),
                "info": len([a for a in self.active_alerts.values() if a.severity == "info"])
            },
            "trends": await self._calculate_trends(),
            "recommendations": await self._generate_recommendations()
        }
    
    async def _calculate_trends(self) -> Dict[str, Any]:
        """Calculate performance trends over time"""
        if len(self.metrics_history) < 10:
            return {"status": "insufficient_data"}
        
        recent_metrics = self.metrics_history[-10:]
        
        # Calculate averages for trend analysis
        cpu_trend = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics)
        memory_trend = sum(m.memory_percent for m in recent_metrics) / len(recent_metrics)
        
        return {
            "cpu_avg_10min": round(cpu_trend, 1),
            "memory_avg_10min": round(memory_trend, 1),
            "metrics_collected": len(self.metrics_history),
            "monitoring_duration_hours": len(self.metrics_history) / 60.0  # Assuming 1min intervals
        }
    
    async def _generate_recommendations(self) -> List[str]:
        """Generate operational recommendations"""
        recommendations = []
        
        if not self.metrics_history:
            return ["Start system monitoring to get health recommendations"]
        
        latest = self.metrics_history[-1]
        
        if latest.cpu_percent > 80:
            recommendations.append("High CPU usage detected - consider reducing monitoring frequency or scaling resources")
        
        if latest.memory_percent > 85:
            recommendations.append("High memory usage - monitor for memory leaks and consider increasing available RAM")
        
        if latest.disk_usage_percent > 90:
            recommendations.append("Disk space critically low - clean up old logs and data files")
        
        if latest.agent_process_memory_mb > 800:
            recommendations.append("Agent memory usage is high - consider restarting the agent process")
        
        if len(self.active_alerts) > 5:
            recommendations.append("Multiple active alerts - review system configuration and resource allocation")
        
        if not recommendations:
            recommendations.append("System is operating within normal parameters")
        
        return recommendations

# Global system monitor instance
_system_monitor: Optional[SystemHealthMonitor] = None

def get_system_monitor(agent_id: str) -> SystemHealthMonitor:
    """Get or create system monitor instance"""
    global _system_monitor
    
    if _system_monitor is None:
        _system_monitor = SystemHealthMonitor(agent_id)
    
    return _system_monitor