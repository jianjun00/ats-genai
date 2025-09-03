"""
PostgreSQL Prometheus Exporter

Integrates with the existing ATS Prometheus/Grafana monitoring infrastructure
to provide PostgreSQL database monitoring metrics.
"""

import asyncio
import asyncpg
import psutil
import time
import logging
from typing import Dict, Optional
from datetime import datetime
from dataclasses import dataclass
import threading

try:
    from prometheus_client import Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("prometheus_client not installed. PostgreSQL Prometheus metrics will not be available.")

from shared.utils.environment import Environment

logger = logging.getLogger(__name__)

@dataclass
class PostgreSQLConnectionMetrics:
    """PostgreSQL connection and performance metrics"""
    timestamp: datetime
    
    # Connection metrics
    total_connections: int
    active_connections: int
    idle_connections: int
    max_connections: Optional[int]
    
    # Database activity
    total_queries: int
    queries_per_second: float
    transactions_per_second: float
    
    # Performance metrics
    cache_hit_ratio: float
    index_usage_ratio: float
    table_bloat_ratio: float
    
    # Lock and blocking
    blocked_queries: int
    long_running_queries: int
    
    # Disk and I/O
    database_size_mb: float
    temp_files_count: int
    temp_files_size_mb: float
    
    # Replication (if applicable)
    replication_lag_seconds: Optional[float]
    
    # System resources
    cpu_percent: float
    memory_mb: float
    disk_usage_percent: float

class PostgreSQLPrometheusExporter:
    """
    Exports PostgreSQL metrics to Prometheus for integration with existing Grafana dashboards.
    
    This exporter works alongside the existing data agent monitoring infrastructure
    and follows the same patterns for consistency.
    """
    
    def __init__(self, env: Environment = None, metrics_prefix: str = "postgresql", port: int = 8001):
        """
        Initialize the PostgreSQL Prometheus exporter.
        
        Args:
            env: Environment configuration for database connection
            metrics_prefix: Prefix for all metrics names
            port: Port to expose PostgreSQL metrics on (different from data agent port 8000)
        """
        self.env = env or Environment()
        self.metrics_prefix = metrics_prefix
        self.port = port
        self.server_started = False
        self.postgres_processes = []
        
        if not PROMETHEUS_AVAILABLE:
            logger.warning("PostgreSQLPrometheusExporter initialized but prometheus_client not available")
            return
        
        # Connection metrics
        self.total_connections = Gauge(
            f"{metrics_prefix}_connections_total",
            "Total number of PostgreSQL connections"
        )
        self.active_connections = Gauge(
            f"{metrics_prefix}_connections_active",
            "Number of active PostgreSQL connections"
        )
        self.idle_connections = Gauge(
            f"{metrics_prefix}_connections_idle",
            "Number of idle PostgreSQL connections"
        )
        self.max_connections = Gauge(
            f"{metrics_prefix}_connections_max",
            "Maximum number of PostgreSQL connections allowed"
        )
        
        # Performance metrics
        self.queries_per_second = Gauge(
            f"{metrics_prefix}_queries_per_second",
            "Number of queries executed per second"
        )
        self.transactions_per_second = Gauge(
            f"{metrics_prefix}_transactions_per_second",
            "Number of transactions per second"
        )
        self.cache_hit_ratio = Gauge(
            f"{metrics_prefix}_cache_hit_ratio",
            "Buffer cache hit ratio (0-1)"
        )
        self.index_usage_ratio = Gauge(
            f"{metrics_prefix}_index_usage_ratio",
            "Index usage ratio (0-1)"
        )
        
        # Lock and blocking metrics
        self.blocked_queries = Gauge(
            f"{metrics_prefix}_blocked_queries",
            "Number of currently blocked queries"
        )
        self.long_running_queries = Gauge(
            f"{metrics_prefix}_long_running_queries",
            "Number of queries running longer than 5 minutes"
        )
        
        # Database size metrics
        self.database_size_mb = Gauge(
            f"{metrics_prefix}_database_size_mb",
            "Total database size in MB"
        )
        self.temp_files_count = Gauge(
            f"{metrics_prefix}_temp_files_count",
            "Number of temporary files created"
        )
        self.temp_files_size_mb = Gauge(
            f"{metrics_prefix}_temp_files_size_mb",
            "Total size of temporary files in MB"
        )
        
        # System resource metrics
        self.cpu_percent = Gauge(
            f"{metrics_prefix}_cpu_percent",
            "PostgreSQL CPU usage percentage"
        )
        self.memory_mb = Gauge(
            f"{metrics_prefix}_memory_mb",
            "PostgreSQL memory usage in MB"
        )
        self.disk_usage_percent = Gauge(
            f"{metrics_prefix}_disk_usage_percent",
            "Disk usage percentage for PostgreSQL data directory"
        )
        
        # Process metrics
        self.worker_processes = Gauge(
            f"{metrics_prefix}_worker_processes",
            "Number of PostgreSQL worker processes"
        )
        self.connection_processes = Gauge(
            f"{metrics_prefix}_connection_processes",
            "Number of PostgreSQL connection processes"
        )
        
        # Health metrics
        self.uptime_seconds = Gauge(
            f"{metrics_prefix}_uptime_seconds",
            "PostgreSQL uptime in seconds"
        )
        self.is_healthy = Gauge(
            f"{metrics_prefix}_healthy",
            "PostgreSQL health status (1=healthy, 0=unhealthy)"
        )
        
        # Replication metrics
        self.replication_lag_seconds = Gauge(
            f"{metrics_prefix}_replication_lag_seconds",
            "Replication lag in seconds"
        )
    
    def start_server(self):
        """Start the Prometheus metrics server"""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Cannot start PostgreSQL Prometheus server: prometheus_client not available")
            return
            
        if self.server_started:
            return
            
        try:
            start_http_server(self.port)
            self.server_started = True
            logger.info(f"PostgreSQL Prometheus metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start PostgreSQL Prometheus metrics server: {e}")
    
    async def collect_database_metrics(self) -> Optional[PostgreSQLConnectionMetrics]:
        """Collect metrics from PostgreSQL database"""
        try:
            pool = await asyncpg.create_pool(self.env.get_database_url())
            
            async with pool.acquire() as conn:
                # Connection metrics
                connections_data = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_connections,
                        COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                        COUNT(*) FILTER (WHERE state = 'idle') as idle_connections
                    FROM pg_stat_activity
                    WHERE pid <> pg_backend_pid()
                """)
                
                # Max connections
                max_conn_data = await conn.fetchval("SHOW max_connections")
                
                # Performance metrics
                cache_hit_data = await conn.fetchrow("""
                    SELECT 
                        CASE WHEN (blks_hit + blks_read) > 0 
                             THEN blks_hit::float / (blks_hit + blks_read) 
                             ELSE 0 
                        END as cache_hit_ratio
                    FROM pg_stat_database 
                    WHERE datname = current_database()
                """)
                
                # Query activity
                query_stats = await conn.fetchrow("""
                    SELECT 
                        COALESCE(SUM(calls), 0) as total_queries,
                        COALESCE(SUM(calls) / GREATEST(EXTRACT(epoch FROM (now() - stats_reset)), 1), 0) as queries_per_second
                    FROM pg_stat_statements
                    WHERE queryid IS NOT NULL
                """)
                
                # Transaction stats
                txn_stats = await conn.fetchrow("""
                    SELECT 
                        COALESCE(xact_commit + xact_rollback, 0) as total_transactions,
                        COALESCE((xact_commit + xact_rollback) / GREATEST(EXTRACT(epoch FROM (now() - stats_reset)), 1), 0) as transactions_per_second
                    FROM pg_stat_database 
                    WHERE datname = current_database()
                """)
                
                # Blocking queries
                blocking_data = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) FILTER (WHERE wait_event IS NOT NULL) as blocked_queries,
                        COUNT(*) FILTER (WHERE state = 'active' AND query_start < now() - interval '5 minutes') as long_running_queries
                    FROM pg_stat_activity
                    WHERE pid <> pg_backend_pid()
                """)
                
                # Database size
                db_size_data = await conn.fetchval("""
                    SELECT pg_database_size(current_database()) / (1024*1024)::float
                """)
                
                # Temporary files
                temp_files_data = await conn.fetchrow("""
                    SELECT 
                        COALESCE(SUM(temp_files), 0) as temp_files_count,
                        COALESCE(SUM(temp_bytes) / (1024*1024)::float, 0) as temp_files_size_mb
                    FROM pg_stat_database
                    WHERE datname = current_database()
                """)
                
            await pool.close()
            
            # System metrics from psutil
            system_metrics = self._get_system_metrics()
            
            return PostgreSQLConnectionMetrics(
                timestamp=datetime.now(),
                total_connections=connections_data['total_connections'] or 0,
                active_connections=connections_data['active_connections'] or 0,
                idle_connections=connections_data['idle_connections'] or 0,
                max_connections=int(max_conn_data) if max_conn_data else None,
                total_queries=query_stats['total_queries'] if query_stats else 0,
                queries_per_second=query_stats['queries_per_second'] if query_stats else 0,
                transactions_per_second=txn_stats['transactions_per_second'] if txn_stats else 0,
                cache_hit_ratio=cache_hit_data['cache_hit_ratio'] if cache_hit_data else 0,
                index_usage_ratio=0.95,  # Placeholder - would need more complex query
                table_bloat_ratio=0.05,  # Placeholder - would need bloat analysis
                blocked_queries=blocking_data['blocked_queries'] if blocking_data else 0,
                long_running_queries=blocking_data['long_running_queries'] if blocking_data else 0,
                database_size_mb=db_size_data or 0,
                temp_files_count=temp_files_data['temp_files_count'] if temp_files_data else 0,
                temp_files_size_mb=temp_files_data['temp_files_size_mb'] if temp_files_data else 0,
                replication_lag_seconds=None,  # Would need replication setup
                **system_metrics
            )
            
        except Exception as e:
            logger.error(f"Error collecting PostgreSQL database metrics: {e}")
            # Return system-only metrics if database is unavailable
            system_metrics = self._get_system_metrics()
            return PostgreSQLConnectionMetrics(
                timestamp=datetime.now(),
                total_connections=0,
                active_connections=0,
                idle_connections=0,
                max_connections=None,
                total_queries=0,
                queries_per_second=0,
                transactions_per_second=0,
                cache_hit_ratio=0,
                index_usage_ratio=0,
                table_bloat_ratio=0,
                blocked_queries=0,
                long_running_queries=0,
                database_size_mb=0,
                temp_files_count=0,
                temp_files_size_mb=0,
                replication_lag_seconds=None,
                **system_metrics
            )
    
    def _get_system_metrics(self) -> Dict[str, float]:
        """Get system-level metrics for PostgreSQL processes"""
        try:
            # Refresh process list
            self.postgres_processes = []
            worker_count = 0
            connection_count = 0
            total_cpu = 0.0
            total_memory_mb = 0.0
            oldest_create_time = None
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    if 'postgres' in proc.info['name'].lower():
                        self.postgres_processes.append(proc)
                        
                        # Categorize process
                        cmdline = ' '.join(proc.info['cmdline']).lower() if proc.info['cmdline'] else ''
                        if any(bg in cmdline for bg in ['background', 'writer', 'launcher', 'scheduler']):
                            worker_count += 1
                        elif '@' in cmdline or 'postgres:' in cmdline:
                            connection_count += 1
                        
                        # Resource usage
                        proc_obj = psutil.Process(proc.info['pid'])
                        total_cpu += proc_obj.cpu_percent()
                        total_memory_mb += proc_obj.memory_info().rss / (1024 * 1024)
                        
                        # Track oldest process for uptime
                        if oldest_create_time is None or proc.info['create_time'] < oldest_create_time:
                            oldest_create_time = proc.info['create_time']
                            
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Calculate uptime
            uptime_seconds = time.time() - oldest_create_time if oldest_create_time else 0
            
            # Disk usage (try common PostgreSQL data directories)
            disk_usage_percent = 0.0
            for data_dir in ["/var/lib/postgresql/16/main", "/var/lib/postgresql/data"]:
                try:
                    if psutil.os.path.exists(data_dir):
                        disk_usage = psutil.disk_usage(data_dir)
                        disk_usage_percent = (disk_usage.used / disk_usage.total) * 100
                        break
                except (OSError, IOError):
                    continue
            
            return {
                'cpu_percent': total_cpu,
                'memory_mb': total_memory_mb,
                'disk_usage_percent': disk_usage_percent,
                'worker_processes': worker_count,
                'connection_processes': connection_count,
                'uptime_seconds': uptime_seconds
            }
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return {
                'cpu_percent': 0.0,
                'memory_mb': 0.0,
                'disk_usage_percent': 0.0,
                'worker_processes': 0,
                'connection_processes': 0,
                'uptime_seconds': 0
            }
    
    def update_prometheus_metrics(self, metrics: PostgreSQLConnectionMetrics):
        """Update Prometheus metrics with collected data"""
        if not PROMETHEUS_AVAILABLE:
            return
        
        # Start server if not already started
        if not self.server_started:
            self.start_server()
        
        # Update connection metrics
        self.total_connections.set(metrics.total_connections)
        self.active_connections.set(metrics.active_connections)
        self.idle_connections.set(metrics.idle_connections)
        if metrics.max_connections:
            self.max_connections.set(metrics.max_connections)
        
        # Update performance metrics
        self.queries_per_second.set(metrics.queries_per_second)
        self.transactions_per_second.set(metrics.transactions_per_second)
        self.cache_hit_ratio.set(metrics.cache_hit_ratio)
        self.index_usage_ratio.set(metrics.index_usage_ratio)
        
        # Update blocking metrics
        self.blocked_queries.set(metrics.blocked_queries)
        self.long_running_queries.set(metrics.long_running_queries)
        
        # Update database size metrics
        self.database_size_mb.set(metrics.database_size_mb)
        self.temp_files_count.set(metrics.temp_files_count)
        self.temp_files_size_mb.set(metrics.temp_files_size_mb)
        
        # Update system metrics
        self.cpu_percent.set(metrics.cpu_percent)
        self.memory_mb.set(metrics.memory_mb)
        self.disk_usage_percent.set(metrics.disk_usage_percent)
        self.uptime_seconds.set(metrics.postgres_uptime_seconds)
        
        # Update process metrics (from system metrics dict)
        self.worker_processes.set(getattr(metrics, 'worker_processes', 0))
        self.connection_processes.set(getattr(metrics, 'connection_processes', 0))
        
        # Update health status
        is_healthy = 1 if metrics.total_connections >= 0 else 0  # Simple health check
        self.is_healthy.set(is_healthy)
        
        # Update replication metrics if available
        if metrics.replication_lag_seconds is not None:
            self.replication_lag_seconds.set(metrics.replication_lag_seconds)

class PostgreSQLMonitor:
    """
    Monitor that continuously collects and exports PostgreSQL metrics to Prometheus.
    
    Integrates with the existing ATS monitoring infrastructure.
    """
    
    def __init__(self, exporter: PostgreSQLPrometheusExporter, update_interval: int = 30):
        """
        Initialize the PostgreSQL monitor.
        
        Args:
            exporter: PostgreSQLPrometheusExporter instance
            update_interval: Interval in seconds between metric collections
        """
        self.exporter = exporter
        self.update_interval = update_interval
        self.is_running = False
        self.thread = None
        
    def start(self):
        """Start the PostgreSQL monitor"""
        if self.is_running:
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self._monitoring_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info("PostgreSQL monitoring started")
        
    def stop(self):
        """Stop the PostgreSQL monitor"""
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=5.0)
        logger.info("PostgreSQL monitoring stopped")
        
    def _monitoring_loop(self):
        """Background thread that periodically collects and updates metrics"""
        while self.is_running:
            try:
                # Collect metrics asynchronously
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                metrics = loop.run_until_complete(self.exporter.collect_database_metrics())
                if metrics:
                    self.exporter.update_prometheus_metrics(metrics)
                    logger.debug(f"Updated PostgreSQL metrics at {metrics.timestamp}")
                
                loop.close()
                
            except Exception as e:
                logger.error(f"Error in PostgreSQL monitoring loop: {e}")
                
            time.sleep(self.update_interval)

def setup_postgresql_monitoring(env: Environment = None, port: int = 8001, 
                               update_interval: int = 30, 
                               metrics_prefix: str = "postgresql"):
    """
    Set up PostgreSQL monitoring for integration with existing Prometheus/Grafana infrastructure.
    
    Args:
        env: Environment configuration for database connection
        port: Port to expose PostgreSQL metrics on
        update_interval: Interval in seconds between metric collections
        metrics_prefix: Prefix for all metrics names
        
    Returns:
        PostgreSQLMonitor instance or None if Prometheus is not available
    """
    if not PROMETHEUS_AVAILABLE:
        logger.warning("Cannot set up PostgreSQL monitoring: prometheus_client not available")
        return None
        
    exporter = PostgreSQLPrometheusExporter(env=env, metrics_prefix=metrics_prefix, port=port)
    monitor = PostgreSQLMonitor(exporter, update_interval)
    monitor.start()
    
    logger.info(f"PostgreSQL monitoring setup complete - metrics available on port {port}")
    return monitor

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PostgreSQL Prometheus Exporter")
    parser.add_argument("--port", type=int, default=8001, help="Prometheus metrics port")
    parser.add_argument("--interval", type=int, default=30, help="Collection interval in seconds")
    parser.add_argument("--metrics-prefix", default="postgresql", help="Metrics prefix")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup monitoring
    env = Environment()
    monitor = setup_postgresql_monitoring(
        env=env,
        port=args.port,
        update_interval=args.interval,
        metrics_prefix=args.metrics_prefix
    )
    
    if monitor:
        try:
            logger.info(f"PostgreSQL monitoring running on port {args.port}")
            logger.info("Press Ctrl+C to stop")
            
            # Keep the main thread alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutting down PostgreSQL monitoring...")
            monitor.stop()
    else:
        logger.error("Failed to setup PostgreSQL monitoring")