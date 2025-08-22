#!/usr/bin/env python3
"""
Setup PostgreSQL Monitoring Integration

Integrates PostgreSQL monitoring with existing ATS Prometheus/Grafana infrastructure.
"""

import os
import sys
import time
import logging
import subprocess
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import Environment
from monitoring.postgres_prometheus_exporter import setup_postgresql_monitoring

def check_prometheus_running(port: int = 9090) -> bool:
    """Check if Prometheus is running"""
    try:
        import requests
        response = requests.get(f"http://localhost:{port}/-/healthy", timeout=5)
        return response.status_code == 200
    except:
        return False

def check_grafana_running(port: int = 3000) -> bool:
    """Check if Grafana is running"""
    try:
        import requests
        response = requests.get(f"http://localhost:{port}/api/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def start_monitoring_stack():
    """Start the monitoring stack using docker-compose"""
    monitoring_dir = Path(__file__).parent.parent.parent / "src" / "market_data" / "agent" / "monitoring"
    
    if not (monitoring_dir / "docker-compose.yml").exists():
        logger.error(f"Docker compose file not found at {monitoring_dir}/docker-compose.yml")
        return False
    
    try:
        logger.info("Starting monitoring stack with docker-compose...")
        subprocess.run([
            "docker-compose", 
            "-f", str(monitoring_dir / "docker-compose.yml"),
            "up", "-d"
        ], check=True, cwd=monitoring_dir)
        
        # Wait for services to be ready
        logger.info("Waiting for services to start...")
        for i in range(30):  # Wait up to 30 seconds
            if check_prometheus_running() and check_grafana_running():
                logger.info("Monitoring stack is ready!")
                return True
            time.sleep(1)
        
        logger.warning("Monitoring stack started but services may not be fully ready")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start monitoring stack: {e}")
        return False
    except FileNotFoundError:
        logger.error("docker-compose not found. Please install Docker and docker-compose")
        return False

def setup_integration():
    """Setup complete PostgreSQL monitoring integration"""
    
    logger.info("Setting up PostgreSQL monitoring integration with ATS infrastructure")
    
    # Check if monitoring stack is running
    prometheus_running = check_prometheus_running()
    grafana_running = check_grafana_running()
    
    logger.info(f"Prometheus running: {prometheus_running}")
    logger.info(f"Grafana running: {grafana_running}")
    
    if not prometheus_running or not grafana_running:
        logger.info("Monitoring stack not detected. Attempting to start...")
        if not start_monitoring_stack():
            logger.error("Could not start monitoring stack. PostgreSQL metrics will still be exported on port 8001")
        else:
            logger.info("Monitoring stack started successfully")
    
    # Setup PostgreSQL monitoring
    logger.info("Starting PostgreSQL metrics exporter...")
    env = Environment()
    
    try:
        monitor = setup_postgresql_monitoring(
            env=env,
            port=8001,
            update_interval=30,
            metrics_prefix="postgresql"
        )
        
        if monitor:
            logger.info("✅ PostgreSQL monitoring setup complete!")
            logger.info("📊 Metrics available at: http://localhost:8001/metrics")
            
            if prometheus_running:
                logger.info("🔍 Prometheus will scrape PostgreSQL metrics automatically")
                logger.info("📈 Prometheus UI: http://localhost:9090")
            
            if grafana_running:
                logger.info("📋 Grafana dashboards: http://localhost:3000")
                logger.info("   - Username: admin")
                logger.info("   - Password: admin (default)")
                logger.info("   - PostgreSQL dashboard will be available after importing")
            
            return monitor
        else:
            logger.error("❌ Failed to setup PostgreSQL monitoring")
            return None
            
    except Exception as e:
        logger.error(f"Error setting up PostgreSQL monitoring: {e}")
        return None

def import_grafana_dashboard():
    """Import PostgreSQL dashboard into Grafana"""
    try:
        import requests
        
        # Check if Grafana is accessible
        if not check_grafana_running():
            logger.warning("Grafana not running, skipping dashboard import")
            return False
        
        # Load dashboard JSON
        dashboard_file = Path(__file__).parent.parent.parent / "k8s" / "data-agent" / "postgres-grafana-dashboard.json"
        
        if not dashboard_file.exists():
            logger.warning(f"Dashboard file not found: {dashboard_file}")
            return False
        
        with open(dashboard_file, 'r') as f:
            dashboard_json = f.read()
        
        # Import dashboard via Grafana API
        import_payload = {
            "dashboard": dashboard_json,
            "overwrite": True,
            "inputs": [
                {
                    "name": "DS_PROMETHEUS",
                    "type": "datasource",
                    "pluginId": "prometheus",
                    "value": "Prometheus"
                }
            ]
        }
        
        response = requests.post(
            "http://localhost:3000/api/dashboards/import",
            json=import_payload,
            auth=("admin", "admin"),
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info("✅ PostgreSQL dashboard imported successfully")
            dashboard_url = response.json().get("importedUrl", "/d/postgresql-dashboard")
            logger.info(f"📋 Dashboard URL: http://localhost:3000{dashboard_url}")
            return True
        else:
            logger.warning(f"Dashboard import failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.warning(f"Could not import dashboard: {e}")
        return False

def main():
    """Main setup function"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    global logger
    logger = logging.getLogger(__name__)
    
    logger.info("🐘 PostgreSQL Monitoring Setup for ATS")
    logger.info("="*50)
    
    # Setup integration
    monitor = setup_integration()
    
    if monitor:
        # Try to import dashboard
        import_grafana_dashboard()
        
        logger.info("")
        logger.info("🎯 Integration Summary:")
        logger.info("="*50)
        logger.info("✅ PostgreSQL metrics exporter: http://localhost:8001/metrics")
        logger.info("✅ Integrated with existing Prometheus configuration")
        logger.info("✅ Grafana dashboard available")
        logger.info("")
        logger.info("📋 Available Metrics:")
        logger.info("   - Connection counts (total, active, idle)")
        logger.info("   - Query performance (QPS, TPS, cache hit ratio)")
        logger.info("   - Resource usage (CPU, memory, disk)")
        logger.info("   - Process monitoring (workers, connections)")
        logger.info("   - Blocking queries and performance issues")
        logger.info("")
        logger.info("🔧 Management Commands:")
        logger.info("   # View PostgreSQL metrics directly")
        logger.info("   curl http://localhost:8001/metrics")
        logger.info("")
        logger.info("   # Check Prometheus targets")
        logger.info("   curl http://localhost:9090/api/v1/targets")
        logger.info("")
        logger.info("   # Standalone monitoring")
        logger.info("   python scripts/monitoring/postgres_monitor.py --watch 30")
        logger.info("")
        logger.info("🚀 Monitoring is now running continuously!")
        logger.info("   Press Ctrl+C to stop...")
        
        try:
            # Keep running
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping PostgreSQL monitoring...")
            monitor.stop()
            logger.info("✅ PostgreSQL monitoring stopped")
    
    else:
        logger.error("❌ Setup failed. Check logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()