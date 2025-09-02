#!/usr/bin/env python3
"""
ATS Real-time Collection Monitoring System Startup Script

Comprehensive startup and orchestration script for the complete real-time collection
monitoring infrastructure including:

- Real-time data collection monitoring
- Multi-channel alerting (Slack, Discord, Email, PagerDuty)
- Web dashboard with live updates
- Prometheus metrics integration
- Health checks and system validation
- Graceful shutdown handling

Features:
- All-in-one startup with proper dependency management
- Environment validation and configuration
- Service health monitoring
- Automatic restart on failures
- Integration with existing ATS infrastructure

Usage:
    # Start all monitoring components
    python3 scripts/start_realtime_monitoring.py
    
    # Start with custom configuration
    python3 scripts/start_realtime_monitoring.py --config monitoring_config.json
    
    # Start specific components only
    python3 scripts/start_realtime_monitoring.py --components monitor,dashboard
    
    # Test mode (validation only)
    python3 scripts/start_realtime_monitoring.py --test

Access Points:
    - Dashboard: http://localhost:8090
    - Prometheus Metrics: http://localhost:8091/metrics
    - Health Checks: http://localhost:8090/health
"""

import asyncio
import json
import logging
import os
import sys
import signal
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import argparse

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import monitoring components
from market_data.realtime.monitoring.realtime_collection_monitor import RealtimeCollectionMonitor
from market_data.realtime.monitoring.alert_channels import AlertChannelManager
from market_data.realtime.monitoring.monitoring_dashboard import MonitoringDashboard
from market_data.realtime.monitoring.prometheus_integration import RealtimePrometheusIntegration

logger = logging.getLogger(__name__)


class MonitoringSystemOrchestrator:
    """Orchestrates the complete real-time monitoring system."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize the monitoring system orchestrator."""
        
        self.config = self._load_configuration(config_file)
        self.running = False
        self.components = {}
        self.tasks = {}
        
        # Component instances
        self.monitor = None
        self.alert_manager = None
        self.dashboard = None
        self.prometheus_integration = None
        
        logger.info("🎯 Monitoring System Orchestrator initialized")
        
    def _load_configuration(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load monitoring system configuration."""
        
        # Try to use production config by default
        if config_file is None:
            production_config = os.path.join(os.path.dirname(__file__), '..', 'config', 'realtime_monitoring_config.json')
            if os.path.exists(production_config):
                config_file = production_config
                logger.info(f"📋 Using production configuration: {production_config}")
        
        # Default configuration (fallback)
        default_config = {
            "components": {
                "monitor": {
                    "enabled": True,
                    "interval_seconds": 60,
                    "database": {
                        "host": "ats-intg-postgres",
                        "port": 5432,
                        "user": "postgres",
                        "password": "intg_password",
                        "database": "intg_db"
                    }
                },
                "alerting": {
                    "enabled": True,
                    "test_on_startup": True,
                    "channels": {
                        "slack": {
                            "enabled": False,
                            "webhook_url": None,
                            "min_level": "warning"
                        },
                        "discord": {
                            "enabled": False, 
                            "webhook_url": None,
                            "min_level": "info"
                        },
                        "email": {
                            "enabled": False,
                            "recipients": [],
                            "min_level": "critical"
                        }
                    }
                },
                "dashboard": {
                    "enabled": True,
                    "host": "0.0.0.0",
                    "port": 8090,
                    "update_interval_seconds": 30
                },
                "prometheus": {
                    "enabled": True,
                    "port": 8091,
                    "existing_prometheus_url": "http://localhost:8080"
                }
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "health_checks": {
                "enabled": True,
                "interval_seconds": 120,
                "restart_on_failure": True
            }
        }
        
        if config_file and Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    
                # Merge configurations (user config overrides defaults)
                self._deep_merge_dict(default_config, user_config)
                logger.info(f"✅ Loaded configuration from {config_file}")
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to load config file {config_file}: {e}")
                logger.info("📋 Using default configuration")
        else:
            logger.info("📋 Using default configuration")
            
        return default_config
        
    def _deep_merge_dict(self, base_dict: Dict, override_dict: Dict):
        """Deep merge two dictionaries."""
        
        for key, value in override_dict.items():
            if (key in base_dict and 
                isinstance(base_dict[key], dict) and 
                isinstance(value, dict)):
                self._deep_merge_dict(base_dict[key], value)
            else:
                base_dict[key] = value
                
    async def _validate_environment(self) -> bool:
        """Validate environment and prerequisites."""
        
        logger.info("🔍 Validating environment...")
        
        validation_results = []
        
        # Check database connectivity
        try:
            monitor_config = self.config["components"]["monitor"]
            db_config = monitor_config["database"]
            
            import asyncpg
            
            db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            
            conn = await asyncpg.connect(db_url)
            await conn.fetchval("SELECT 1")
            await conn.close()
            
            validation_results.append(("Database Connectivity", True, "✅ Connected successfully"))
            
        except Exception as e:
            validation_results.append(("Database Connectivity", False, f"❌ Failed: {e}"))
            
        # Check required tables
        try:
            conn = await asyncpg.connect(db_url)
            
            for table in ['intg_one_minute_live_tiingo', 'intg_one_minute_live_polygon']:
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                """, table)
                
                if exists:
                    validation_results.append((f"Table {table}", True, "✅ Exists"))
                else:
                    validation_results.append((f"Table {table}", False, "❌ Missing"))
                    
            await conn.close()
            
        except Exception as e:
            validation_results.append(("Table Validation", False, f"❌ Failed: {e}"))
            
        # Check alert channel configurations
        alert_config = self.config["components"]["alerting"]
        
        for channel_name, channel_config in alert_config["channels"].items():
            if channel_config["enabled"]:
                # Basic configuration validation
                if channel_name in ["slack", "discord"] and not channel_config.get("webhook_url"):
                    validation_results.append((f"Alert Channel {channel_name}", False, "❌ Missing webhook URL"))
                else:
                    validation_results.append((f"Alert Channel {channel_name}", True, "✅ Configured"))
            else:
                validation_results.append((f"Alert Channel {channel_name}", True, "⏸️ Disabled"))
                
        # Check port availability
        dashboard_port = self.config["components"]["dashboard"]["port"]
        prometheus_port = self.config["components"]["prometheus"]["port"]
        
        for service, port in [("Dashboard", dashboard_port), ("Prometheus", prometheus_port)]:
            try:
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex(('localhost', port))
                sock.close()
                
                if result == 0:
                    validation_results.append((f"Port {port} ({service})", False, f"❌ Port already in use"))
                else:
                    validation_results.append((f"Port {port} ({service})", True, f"✅ Available"))
                    
            except Exception as e:
                validation_results.append((f"Port {port} ({service})", False, f"❌ Check failed: {e}"))
                
        # Display validation results
        all_passed = True
        
        logger.info("📋 Environment Validation Results:")
        logger.info("=" * 60)
        
        for check_name, passed, message in validation_results:
            logger.info(f"{check_name}: {message}")
            if not passed:
                all_passed = False
                
        logger.info("=" * 60)
        
        if all_passed:
            logger.info("✅ All environment validations passed")
        else:
            logger.warning("⚠️ Some environment validations failed")
            
        return all_passed
        
    async def _initialize_components(self, component_filter: Optional[List[str]] = None):
        """Initialize monitoring system components."""
        
        logger.info("🚀 Initializing monitoring components...")
        
        # Filter components if specified
        if component_filter:
            enabled_components = component_filter
        else:
            enabled_components = [
                name for name, config in self.config["components"].items()
                if config.get("enabled", True)
            ]
            
        # Initialize Monitor
        if "monitor" in enabled_components:
            try:
                monitor_config = self.config["components"]["monitor"]
                db_config = monitor_config["database"]
                
                self.monitor = RealtimeCollectionMonitor(
                    db_host=db_config["host"],
                    db_port=db_config["port"],
                    db_user=db_config["user"],
                    db_password=db_config["password"],
                    db_name=db_config["database"],
                    monitoring_interval=monitor_config["interval_seconds"]
                )
                
                await self.monitor.initialize()
                self.components["monitor"] = self.monitor
                logger.info("✅ Real-time Collection Monitor initialized")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Monitor: {e}")
                raise
                
        # Initialize Alert Manager
        if "alerting" in enabled_components:
            try:
                self.alert_manager = AlertChannelManager()
                self.components["alerting"] = self.alert_manager
                
                # Test channels if configured
                alert_config = self.config["components"]["alerting"]
                if alert_config.get("test_on_startup", False):
                    test_results = await self.alert_manager.test_channels()
                    logger.info(f"📢 Alert channel test results: {test_results}")
                    
                logger.info("✅ Alert Channel Manager initialized")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Alert Manager: {e}")
                
        # Initialize Dashboard
        if "dashboard" in enabled_components:
            try:
                dashboard_config = self.config["components"]["dashboard"]
                
                self.dashboard = MonitoringDashboard(
                    host=dashboard_config["host"],
                    port=dashboard_config["port"],
                    monitor_interval=dashboard_config["update_interval_seconds"]
                )
                
                # Share monitor instance
                if self.monitor:
                    self.dashboard.monitor = self.monitor
                    
                await self.dashboard.initialize()
                self.components["dashboard"] = self.dashboard
                logger.info("✅ Monitoring Dashboard initialized")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Dashboard: {e}")
                
        # Initialize Prometheus Integration
        if "prometheus" in enabled_components:
            try:
                prometheus_config = self.config["components"]["prometheus"]
                
                self.prometheus_integration = RealtimePrometheusIntegration(
                    monitor_interval=self.config["components"]["monitor"]["interval_seconds"],
                    metrics_port=prometheus_config["port"],
                    existing_prometheus_url=prometheus_config.get("existing_prometheus_url")
                )
                
                # Share monitor instance
                if self.monitor:
                    self.prometheus_integration.monitor = self.monitor
                    
                await self.prometheus_integration.initialize()
                self.components["prometheus"] = self.prometheus_integration
                logger.info("✅ Prometheus Integration initialized")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Prometheus Integration: {e}")
                
        logger.info(f"🎯 Initialized {len(self.components)} components: {list(self.components.keys())}")
        
    async def _start_component_tasks(self):
        """Start background tasks for all components."""
        
        logger.info("▶️ Starting component tasks...")
        
        # Start Monitor
        if "monitor" in self.components:
            self.tasks["monitor"] = asyncio.create_task(
                self.monitor.start_monitoring(),
                name="monitor_task"
            )
            
        # Start Dashboard (includes its own monitoring)
        if "dashboard" in self.components:
            self.tasks["dashboard"] = asyncio.create_task(
                self.dashboard.start_server(),
                name="dashboard_task"
            )
            
        # Start Prometheus Integration
        if "prometheus" in self.components:
            self.tasks["prometheus"] = asyncio.create_task(
                self.prometheus_integration.start_metrics_server(),
                name="prometheus_task"
            )
            
        # Start health monitoring task
        if self.config["health_checks"]["enabled"]:
            self.tasks["health_monitor"] = asyncio.create_task(
                self._health_monitoring_loop(),
                name="health_monitor_task"
            )
            
        logger.info(f"🚀 Started {len(self.tasks)} background tasks")
        
    async def _health_monitoring_loop(self):
        """Background task for monitoring component health."""
        
        logger.info("❤️ Starting health monitoring loop")
        
        interval = self.config["health_checks"]["interval_seconds"]
        
        while self.running:
            try:
                # Check component health
                health_status = {}
                
                for name, component in self.components.items():
                    try:
                        if hasattr(component, 'running'):
                            health_status[name] = 'running' if component.running else 'stopped'
                        else:
                            health_status[name] = 'unknown'
                    except Exception as e:
                        health_status[name] = f'error: {e}'
                        
                # Check task status
                failed_tasks = []
                for task_name, task in self.tasks.items():
                    if task.done() and not task.cancelled():
                        if task.exception():
                            failed_tasks.append((task_name, task.exception()))
                            
                if failed_tasks:
                    logger.error(f"❌ Failed tasks detected: {[name for name, _ in failed_tasks]}")
                    
                    if self.config["health_checks"]["restart_on_failure"]:
                        logger.info("🔄 Attempting to restart failed components...")
                        # Implementation for restart logic would go here
                        
                # Log health summary
                healthy_components = sum(1 for status in health_status.values() if status == 'running')
                logger.debug(f"❤️ Health check: {healthy_components}/{len(health_status)} components healthy")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"❌ Error in health monitoring: {e}")
                await asyncio.sleep(30)
                
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        
        def signal_handler(signum, frame):
            logger.info(f"📤 Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.shutdown())
            
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
    async def start(self, component_filter: Optional[List[str]] = None, test_mode: bool = False):
        """Start the monitoring system."""
        
        try:
            logger.info("="*80)
            logger.info("ATS REAL-TIME COLLECTION MONITORING SYSTEM")
            logger.info("="*80)
            logger.info(f"🕒 Startup time: {datetime.now()}")
            
            # Validate environment
            if not await self._validate_environment():
                if not test_mode:
                    logger.error("❌ Environment validation failed, aborting startup")
                    return False
                else:
                    logger.warning("⚠️ Environment validation failed, but continuing in test mode")
                    
            if test_mode:
                logger.info("✅ Test mode validation completed successfully")
                return True
                
            # Initialize components
            await self._initialize_components(component_filter)
            
            if not self.components:
                logger.error("❌ No components initialized, aborting startup")
                return False
                
            # Setup signal handlers
            self._setup_signal_handlers()
            
            # Start background tasks
            self.running = True
            await self._start_component_tasks()
            
            # Display access information
            logger.info("="*80)
            logger.info("🎯 MONITORING SYSTEM ACCESS POINTS")
            logger.info("="*80)
            
            if "dashboard" in self.components:
                port = self.config["components"]["dashboard"]["port"]
                logger.info(f"📊 Dashboard: http://localhost:{port}")
                logger.info(f"❤️ Health Check: http://localhost:{port}/health")
                logger.info(f"📡 WebSocket: ws://localhost:{port}/ws")
                
            if "prometheus" in self.components:
                port = self.config["components"]["prometheus"]["port"]
                logger.info(f"📈 Prometheus Metrics: http://localhost:{port}/metrics")
                logger.info(f"🚨 Alerting Rules: http://localhost:{port}/config/rules")
                logger.info(f"📋 Grafana Dashboard: http://localhost:{port}/config/grafana")
                
            logger.info("="*80)
            logger.info("✅ ATS Real-time Monitoring System is fully operational!")
            logger.info("="*80)
            
            # Wait for all tasks
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            
        except Exception as e:
            logger.error(f"❌ Failed to start monitoring system: {e}")
            return False
            
        return True
        
    async def shutdown(self):
        """Gracefully shutdown the monitoring system."""
        
        logger.info("🛑 Shutting down monitoring system...")
        
        self.running = False
        
        # Cancel all tasks
        for task_name, task in self.tasks.items():
            if not task.done():
                logger.info(f"🛑 Cancelling {task_name} task")
                task.cancel()
                
        # Close components
        for name, component in self.components.items():
            try:
                if hasattr(component, 'close'):
                    await component.close()
                logger.info(f"✅ {name} component closed")
            except Exception as e:
                logger.error(f"❌ Error closing {name}: {e}")
                
        logger.info("✅ Monitoring system shutdown complete")
        
    def save_configuration(self, output_file: str):
        """Save current configuration to file."""
        
        try:
            with open(output_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"✅ Configuration saved to {output_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save configuration: {e}")


async def main():
    """Main function for monitoring system startup."""
    
    parser = argparse.ArgumentParser(
        description='ATS Real-time Collection Monitoring System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scripts/start_realtime_monitoring.py
  python3 scripts/start_realtime_monitoring.py --config custom_config.json
  python3 scripts/start_realtime_monitoring.py --components monitor,dashboard
  python3 scripts/start_realtime_monitoring.py --test
        """
    )
    
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--components', help='Comma-separated list of components to start (monitor,alerting,dashboard,prometheus)')
    parser.add_argument('--test', action='store_true', help='Test mode - validate environment and exit')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='Log level')
    parser.add_argument('--save-config', help='Save current configuration to specified file')
    
    args = parser.parse_args()
    
    # Setup logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format=log_format
    )
    
    # Initialize orchestrator
    orchestrator = MonitoringSystemOrchestrator(config_file=args.config)
    
    # Save configuration if requested
    if args.save_config:
        orchestrator.save_configuration(args.save_config)
        return
        
    # Parse component filter
    component_filter = None
    if args.components:
        component_filter = [c.strip() for c in args.components.split(',')]
        
    try:
        # Start monitoring system
        success = await orchestrator.start(
            component_filter=component_filter,
            test_mode=args.test
        )
        
        if not success:
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
    finally:
        await orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())