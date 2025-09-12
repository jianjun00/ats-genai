"""
Service Monitoring Dashboard

Real-time dashboard for service performance monitoring and metrics visualization.
Provides web-based interface for:
1. Service performance metrics and trends
2. Health status monitoring
3. Resource utilization tracking
4. Alert management and notifications
5. Performance benchmarking
"""

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import asyncio
import logging
from pathlib import Path

from infrastructure.monitoring.service_metrics import (
    ServiceMetricsCollector,
    ServiceHealthMonitor,
    ResourceMonitor,
    get_global_metrics_collector,
    setup_default_benchmarks,
    setup_default_alerts
)

logger = logging.getLogger(__name__)

# Dashboard configuration
DASHBOARD_DIR = Path(__file__).parent
TEMPLATES_DIR = DASHBOARD_DIR / "templates"
STATIC_DIR = DASHBOARD_DIR / "static"

# Create directories if they don't exist
TEMPLATES_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# FastAPI app for dashboard
dashboard_app = FastAPI(title="Service Monitoring Dashboard", version="1.0.0")

# Templates and static files
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
if STATIC_DIR.exists():
    dashboard_app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ========================================================================================
# DASHBOARD DEPENDENCIES
# ========================================================================================

def get_metrics_collector() -> ServiceMetricsCollector:
    """Get metrics collector instance"""
    return get_global_metrics_collector()


# Global monitors (will be initialized by startup event)
health_monitor: Optional[ServiceHealthMonitor] = None
resource_monitor: Optional[ResourceMonitor] = None


# ========================================================================================
# DASHBOARD ROUTES
# ========================================================================================

@dashboard_app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    """Main dashboard page"""
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "title": "Service Monitoring Dashboard",
        "current_time": datetime.utcnow().isoformat()
    })


@dashboard_app.get("/api/services/stats")
async def get_all_service_stats(collector: ServiceMetricsCollector = Depends(get_metrics_collector)):
    """Get statistics for all monitored services"""
    try:
        all_stats = {}
        
        # Get stats for all services that have recorded metrics
        for service_name in set(metric.service_name for metric in collector.metrics):
            stats = collector.get_service_stats(service_name)
            if stats:
                all_stats[service_name] = stats
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "services": all_stats,
            "total_services": len(all_stats)
        }
        
    except Exception as e:
        logger.error(f"Error getting service stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/services/{service_name}/stats")
async def get_service_stats(
    service_name: str,
    operation: Optional[str] = None,
    collector: ServiceMetricsCollector = Depends(get_metrics_collector)
):
    """Get detailed statistics for a specific service"""
    try:
        stats = collector.get_service_stats(service_name, operation)
        if not stats:
            raise HTTPException(status_code=404, detail=f"No stats found for service: {service_name}")
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "service_stats": stats
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stats for service {service_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/services/{service_name}/benchmarks")
async def get_service_benchmarks(
    service_name: str,
    operation: Optional[str] = None,
    collector: ServiceMetricsCollector = Depends(get_metrics_collector)
):
    """Get benchmark violations for a service"""
    try:
        if operation:
            violations = collector.get_benchmark_violations(service_name, operation)
            return {
                "service_name": service_name,
                "operation": operation,
                "violations": violations,
                "violation_count": len(violations)
            }
        else:
            # Get violations for all operations
            all_violations = {}
            stats = collector.get_service_stats(service_name)
            
            if stats and 'operation_counts' in stats:
                for op in stats['operation_counts'].keys():
                    violations = collector.get_benchmark_violations(service_name, op)
                    if violations:
                        all_violations[op] = violations
            
            return {
                "service_name": service_name,
                "violations_by_operation": all_violations,
                "total_violation_count": sum(len(v) for v in all_violations.values())
            }
            
    except Exception as e:
        logger.error(f"Error getting benchmarks for {service_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/health/overall")
async def get_overall_health():
    """Get overall system health status"""
    try:
        if health_monitor is None:
            return {"error": "Health monitoring not initialized"}
        
        overall_health = await health_monitor.get_overall_health()
        return overall_health
        
    except Exception as e:
        logger.error(f"Error getting overall health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/health/{service_name}")
async def get_service_health(service_name: str):
    """Get health status for a specific service"""
    try:
        if health_monitor is None:
            return {"error": "Health monitoring not initialized"}
        
        health = await health_monitor.check_service_health(service_name)
        
        return {
            "service_name": health.service_name,
            "status": health.status,
            "last_check": health.last_check.isoformat(),
            "response_time_ms": health.response_time_ms,
            "error_count": health.error_count,
            "uptime_seconds": health.uptime_seconds,
            "details": health.details
        }
        
    except Exception as e:
        logger.error(f"Error getting health for service {service_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/resources/current")
async def get_current_resources():
    """Get current system resource usage"""
    try:
        if resource_monitor is None:
            return {"error": "Resource monitoring not initialized"}
        
        system_resources = resource_monitor.get_current_resource_usage()
        process_resources = resource_monitor.get_process_resource_usage()
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "system_resources": system_resources,
            "process_resources": process_resources
        }
        
    except Exception as e:
        logger.error(f"Error getting current resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/alerts/active")
async def get_active_alerts(collector: ServiceMetricsCollector = Depends(get_metrics_collector)):
    """Get currently active alerts"""
    try:
        alerts = collector.evaluate_alerts()
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "active_alerts": alerts,
            "alert_count": len(alerts),
            "severity_breakdown": {
                "critical": len([a for a in alerts if a.get('severity') == 'critical']),
                "error": len([a for a in alerts if a.get('severity') == 'error']),
                "warning": len([a for a in alerts if a.get('severity') == 'warning']),
                "info": len([a for a in alerts if a.get('severity') == 'info'])
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting active alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/metrics/recent")
async def get_recent_metrics(
    service_name: Optional[str] = None,
    operation: Optional[str] = None,
    metric_type: Optional[str] = None,
    limit: int = 100,
    collector: ServiceMetricsCollector = Depends(get_metrics_collector)
):
    """Get recent metrics with optional filtering"""
    try:
        # Filter metrics based on parameters
        filtered_metrics = []
        
        for metric in reversed(list(collector.metrics)):
            if len(filtered_metrics) >= limit:
                break
                
            # Apply filters
            if service_name and metric.service_name != service_name:
                continue
            if operation and metric.operation != operation:
                continue
            if metric_type and metric.metric_type != metric_type:
                continue
                
            filtered_metrics.append({
                "service_name": metric.service_name,
                "operation": metric.operation,
                "metric_type": metric.metric_type,
                "value": metric.value,
                "timestamp": metric.timestamp.isoformat(),
                "labels": metric.labels,
                "metadata": metric.metadata
            })
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": filtered_metrics,
            "total_count": len(filtered_metrics),
            "filters": {
                "service_name": service_name,
                "operation": operation,
                "metric_type": metric_type,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting recent metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.get("/api/summary")
async def get_dashboard_summary(collector: ServiceMetricsCollector = Depends(get_metrics_collector)):
    """Get dashboard summary with key metrics"""
    try:
        # Get service statistics
        services = set(metric.service_name for metric in collector.metrics)
        service_stats = {}
        total_requests = 0
        total_errors = 0
        
        for service_name in services:
            stats = collector.get_service_stats(service_name)
            if stats:
                service_stats[service_name] = stats
                total_requests += stats.get('total_requests', 0)
                total_errors += stats.get('total_errors', 0)
        
        # Calculate overall error rate
        overall_error_rate = (total_errors / total_requests) if total_requests > 0 else 0
        
        # Get active alerts
        alerts = collector.evaluate_alerts()
        critical_alerts = [a for a in alerts if a.get('severity') == 'critical']
        
        # Get system health
        overall_health_status = "unknown"
        if health_monitor:
            try:
                health_data = await health_monitor.get_overall_health()
                overall_health_status = health_data.get('overall_status', 'unknown')
            except Exception:
                pass
        
        # Get resource usage
        resource_usage = {}
        if resource_monitor:
            try:
                resource_usage = resource_monitor.get_current_resource_usage()
            except Exception:
                pass
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_services": len(services),
                "total_requests": total_requests,
                "total_errors": total_errors,
                "overall_error_rate": overall_error_rate,
                "active_alerts": len(alerts),
                "critical_alerts": len(critical_alerts),
                "health_status": overall_health_status,
                "uptime_services": len([s for s in service_stats.values() if s.get('uptime_status') == 'active'])
            },
            "top_services": {
                name: {
                    "total_requests": stats.get('total_requests', 0),
                    "error_rate": stats.get('overall_error_rate', 0),
                    "uptime_status": stats.get('uptime_status', 'unknown')
                }
                for name, stats in sorted(
                    service_stats.items(), 
                    key=lambda x: x[1].get('total_requests', 0), 
                    reverse=True
                )[:5]
            },
            "resource_usage": {
                "cpu_percent": resource_usage.get('cpu_percent', 0),
                "memory_percent": resource_usage.get('memory_percent', 0),
                "disk_percent": resource_usage.get('disk_percent', 0)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting dashboard summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================================================================
# DASHBOARD STARTUP AND SHUTDOWN
# ========================================================================================

@dashboard_app.on_event("startup")
async def startup_event():
    """Initialize monitoring components on startup"""
    global health_monitor, resource_monitor
    
    logger.info("Starting Service Monitoring Dashboard...")
    
    # Setup default benchmarks and alerts
    setup_default_benchmarks()
    setup_default_alerts()
    
    # Initialize health monitor
    health_monitor = ServiceHealthMonitor(check_interval_seconds=30)
    
    # Initialize resource monitor
    resource_monitor = ResourceMonitor()
    await resource_monitor.start_monitoring(interval_seconds=60)
    
    logger.info("Service Monitoring Dashboard started successfully")


@dashboard_app.on_event("shutdown")
async def shutdown_event():
    """Cleanup monitoring components on shutdown"""
    global health_monitor, resource_monitor
    
    logger.info("Shutting down Service Monitoring Dashboard...")
    
    if health_monitor:
        await health_monitor.stop_monitoring()
    
    if resource_monitor:
        await resource_monitor.stop_monitoring()
    
    logger.info("Service Monitoring Dashboard shutdown complete")


# ========================================================================================
# DASHBOARD HTML TEMPLATE
# ========================================================================================

def create_dashboard_template():
    """Create the main dashboard HTML template"""
    template_content = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .dashboard-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            border-left: 4px solid #667eea;
        }
        .card h3 {
            margin-top: 0;
            color: #333;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        .status-healthy { color: #28a745; }
        .status-warning { color: #ffc107; }
        .status-error { color: #dc3545; }
        .status-critical { color: #dc3545; font-weight: bold; }
        
        .refresh-button {
            background: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            margin: 10px 5px;
        }
        .refresh-button:hover {
            background: #5a6fd8;
        }
        
        .loading {
            text-align: center;
            padding: 20px;
            color: #666;
        }
        
        .error {
            color: #dc3545;
            padding: 10px;
            background: #f8d7da;
            border-radius: 5px;
            margin: 10px 0;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }
        th, td {
            text-align: left;
            padding: 8px;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f2f2f2;
        }
        
        .chart-container {
            width: 100%;
            height: 300px;
            background: #f8f9fa;
            border-radius: 5px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #666;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <div class="dashboard-header">
        <h1>{{ title }}</h1>
        <p>Real-time service performance monitoring and metrics</p>
        <p>Last updated: <span id="last-updated">{{ current_time }}</span></p>
        <button class="refresh-button" onclick="refreshDashboard()">Refresh All</button>
        <button class="refresh-button" onclick="toggleAutoRefresh()">Auto Refresh: <span id="auto-refresh-status">OFF</span></button>
    </div>

    <!-- Summary Cards -->
    <div class="dashboard-grid">
        <div class="card">
            <h3>System Overview</h3>
            <div id="system-overview" class="loading">Loading...</div>
        </div>
        
        <div class="card">
            <h3>Active Alerts</h3>
            <div id="active-alerts" class="loading">Loading...</div>
        </div>
        
        <div class="card">
            <h3>Resource Usage</h3>
            <div id="resource-usage" class="loading">Loading...</div>
        </div>
        
        <div class="card">
            <h3>Service Health</h3>
            <div id="service-health" class="loading">Loading...</div>
        </div>
    </div>

    <!-- Detailed Sections -->
    <div class="dashboard-grid">
        <div class="card">
            <h3>Service Performance</h3>
            <div id="service-performance" class="loading">Loading...</div>
        </div>
        
        <div class="card">
            <h3>Recent Metrics</h3>
            <div id="recent-metrics" class="loading">Loading...</div>
        </div>
    </div>

    <script>
        let autoRefreshInterval;
        let autoRefreshEnabled = false;
        
        // Fetch dashboard data
        async function fetchDashboardData() {
            try {
                // Update timestamp
                document.getElementById('last-updated').textContent = new Date().toISOString();
                
                // Fetch summary data
                const summaryResponse = await fetch('/api/summary');
                const summaryData = await summaryResponse.json();
                updateSystemOverview(summaryData.summary);
                
                // Fetch alerts
                const alertsResponse = await fetch('/api/alerts/active');
                const alertsData = await alertsResponse.json();
                updateActiveAlerts(alertsData);
                
                // Fetch resource usage
                const resourcesResponse = await fetch('/api/resources/current');
                const resourcesData = await resourcesResponse.json();
                updateResourceUsage(resourcesData);
                
                // Fetch health status
                const healthResponse = await fetch('/api/health/overall');
                const healthData = await healthResponse.json();
                updateServiceHealth(healthData);
                
                // Fetch service performance
                const servicesResponse = await fetch('/api/services/stats');
                const servicesData = await servicesResponse.json();
                updateServicePerformance(servicesData);
                
                // Fetch recent metrics
                const metricsResponse = await fetch('/api/metrics/recent?limit=20');
                const metricsData = await metricsResponse.json();
                updateRecentMetrics(metricsData);
                
            } catch (error) {
                console.error('Error fetching dashboard data:', error);
                showError('Failed to fetch dashboard data: ' + error.message);
            }
        }
        
        function updateSystemOverview(summary) {
            const html = `
                <div class="metric-value">${summary.total_services}</div>
                <div>Total Services</div>
                <hr>
                <div>Requests: <strong>${summary.total_requests.toLocaleString()}</strong></div>
                <div>Error Rate: <strong class="${getErrorRateClass(summary.overall_error_rate)}">${(summary.overall_error_rate * 100).toFixed(2)}%</strong></div>
                <div>Health: <strong class="status-${summary.health_status}">${summary.health_status.toUpperCase()}</strong></div>
            `;
            document.getElementById('system-overview').innerHTML = html;
        }
        
        function updateActiveAlerts(alertsData) {
            const alerts = alertsData.active_alerts;
            if (alerts.length === 0) {
                document.getElementById('active-alerts').innerHTML = '<div class="status-healthy">No active alerts</div>';
                return;
            }
            
            let html = `<div class="metric-value status-${getSeverityClass(alerts)}">${alerts.length}</div><div>Active Alerts</div><hr>`;
            alerts.slice(0, 5).forEach(alert => {
                html += `<div class="status-${alert.severity}">${alert.service_name}: ${alert.message}</div>`;
            });
            
            document.getElementById('active-alerts').innerHTML = html;
        }
        
        function updateResourceUsage(resourceData) {
            const resources = resourceData.system_resources;
            const html = `
                <div>CPU: <strong class="${getUsageClass(resources.cpu_percent)}">${resources.cpu_percent.toFixed(1)}%</strong></div>
                <div>Memory: <strong class="${getUsageClass(resources.memory_percent)}">${resources.memory_percent.toFixed(1)}%</strong></div>
                <div>Disk: <strong class="${getUsageClass(resources.disk_percent)}">${resources.disk_percent.toFixed(1)}%</strong></div>
                <div class="chart-container">Resource usage visualization would go here</div>
            `;
            document.getElementById('resource-usage').innerHTML = html;
        }
        
        function updateServiceHealth(healthData) {
            if (healthData.error) {
                document.getElementById('service-health').innerHTML = `<div class="error">${healthData.error}</div>`;
                return;
            }
            
            const summary = healthData.summary;
            const html = `
                <div>Total: <strong>${summary.total_services}</strong></div>
                <div class="status-healthy">Healthy: ${summary.healthy_services}</div>
                <div class="status-warning">Degraded: ${summary.degraded_services}</div>
                <div class="status-error">Unhealthy: ${summary.unhealthy_services}</div>
            `;
            document.getElementById('service-health').innerHTML = html;
        }
        
        function updateServicePerformance(servicesData) {
            const services = servicesData.services;
            let html = '<table><tr><th>Service</th><th>Requests</th><th>Error Rate</th><th>Status</th></tr>';
            
            Object.entries(services).forEach(([name, stats]) => {
                html += `
                    <tr>
                        <td>${name}</td>
                        <td>${stats.total_requests}</td>
                        <td class="${getErrorRateClass(stats.overall_error_rate)}">${(stats.overall_error_rate * 100).toFixed(2)}%</td>
                        <td class="status-${stats.uptime_status === 'active' ? 'healthy' : 'error'}">${stats.uptime_status}</td>
                    </tr>
                `;
            });
            
            html += '</table>';
            document.getElementById('service-performance').innerHTML = html;
        }
        
        function updateRecentMetrics(metricsData) {
            const metrics = metricsData.metrics;
            let html = '<table><tr><th>Time</th><th>Service</th><th>Operation</th><th>Type</th><th>Value</th></tr>';
            
            metrics.slice(0, 10).forEach(metric => {
                const time = new Date(metric.timestamp).toLocaleTimeString();
                html += `
                    <tr>
                        <td>${time}</td>
                        <td>${metric.service_name}</td>
                        <td>${metric.operation}</td>
                        <td>${metric.metric_type}</td>
                        <td>${formatMetricValue(metric.value, metric.metric_type)}</td>
                    </tr>
                `;
            });
            
            html += '</table>';
            document.getElementById('recent-metrics').innerHTML = html;
        }
        
        // Helper functions
        function getErrorRateClass(rate) {
            if (rate > 0.05) return 'status-critical';
            if (rate > 0.01) return 'status-error';
            if (rate > 0.005) return 'status-warning';
            return 'status-healthy';
        }
        
        function getUsageClass(percent) {
            if (percent > 90) return 'status-critical';
            if (percent > 80) return 'status-error';
            if (percent > 70) return 'status-warning';
            return 'status-healthy';
        }
        
        function getSeverityClass(alerts) {
            if (alerts.some(a => a.severity === 'critical')) return 'critical';
            if (alerts.some(a => a.severity === 'error')) return 'error';
            return 'warning';
        }
        
        function formatMetricValue(value, type) {
            if (type === 'latency') return `${value.toFixed(1)}ms`;
            if (type.includes('percent')) return `${value.toFixed(1)}%`;
            if (type.includes('memory')) return `${(value/1024/1024).toFixed(1)}MB`;
            return value.toFixed(2);
        }
        
        function showError(message) {
            console.error(message);
            // Could show error notifications here
        }
        
        function refreshDashboard() {
            fetchDashboardData();
        }
        
        function toggleAutoRefresh() {
            autoRefreshEnabled = !autoRefreshEnabled;
            const statusElement = document.getElementById('auto-refresh-status');
            
            if (autoRefreshEnabled) {
                statusElement.textContent = 'ON';
                autoRefreshInterval = setInterval(fetchDashboardData, 30000); // Refresh every 30 seconds
            } else {
                statusElement.textContent = 'OFF';
                if (autoRefreshInterval) {
                    clearInterval(autoRefreshInterval);
                }
            }
        }
        
        // Initial load
        fetchDashboardData();
    </script>
</body>
</html>
'''
    
    # Write template file
    template_file = TEMPLATES_DIR / "dashboard.html"
    with open(template_file, 'w') as f:
        f.write(template_content)
    
    logger.info(f"Created dashboard template: {template_file}")


# ========================================================================================
# DASHBOARD INITIALIZATION
# ========================================================================================

def initialize_dashboard():
    """Initialize dashboard templates and static files"""
    try:
        # Create dashboard template
        create_dashboard_template()
        
        # Create a simple CSS file
        css_content = """
        /* Additional dashboard styles can go here */
        .dashboard-container {
            max-width: 1200px;
            margin: 0 auto;
        }
        """
        
        css_file = STATIC_DIR / "dashboard.css"
        with open(css_file, 'w') as f:
            f.write(css_content)
        
        logger.info("Dashboard initialization complete")
        
    except Exception as e:
        logger.error(f"Error initializing dashboard: {e}")


# Run initialization when module is imported
initialize_dashboard()


# ========================================================================================
# USAGE EXAMPLES
# ========================================================================================

"""
USAGE EXAMPLES:

1. Running the Dashboard:
    import uvicorn
    from infrastructure.monitoring.dashboard import dashboard_app
    
    uvicorn.run(dashboard_app, host="0.0.0.0", port=8080)

2. Accessing the Dashboard:
    # Web interface
    http://localhost:8080/
    
    # API endpoints
    http://localhost:8080/api/summary
    http://localhost:8080/api/services/stats
    http://localhost:8080/api/health/overall
    http://localhost:8080/api/alerts/active

3. Integrating with Services:
    from infrastructure.monitoring.service_metrics import monitor_performance
    
    @monitor_performance('MyService', 'my_operation')
    async def my_service_method(self):
        # Service implementation
        pass

4. Custom Health Checks:
    from infrastructure.monitoring.dashboard import health_monitor
    
    async def my_health_check():
        # Check service health
        return {"status": "healthy", "details": {...}}
    
    health_monitor.register_health_check('MyService', my_health_check)

5. Custom Benchmarks:
    from infrastructure.monitoring.service_metrics import get_global_metrics_collector, PerformanceBenchmark
    
    collector = get_global_metrics_collector()
    collector.add_benchmark(PerformanceBenchmark(
        service_name='MyService',
        operation='critical_operation',
        latency_p95_ms=50.0,
        error_rate_threshold=0.005
    ))
"""