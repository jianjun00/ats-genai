#!/usr/bin/env python3
"""
Comprehensive Monitoring System Diagnostic and Debugging Script

This script performs extensive diagnostics to identify and debug any issues
with the ATS real-time collection monitoring system.

Features:
- System environment validation
- Database connectivity testing
- Dependency checking
- Configuration validation
- Service health checks
- Performance diagnostics
- Error reproduction
- Automated issue resolution

Usage:
    python3 scripts/debug_monitoring_system.py
    python3 scripts/debug_monitoring_system.py --verbose
    python3 scripts/debug_monitoring_system.py --fix-issues
"""

import asyncio
import json
import logging
import os
import sys
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

logger = logging.getLogger(__name__)


class DiagnosticResult:
    """Container for diagnostic test results."""
    
    def __init__(self, name: str, passed: bool, message: str, details: Optional[Dict] = None, 
                 fix_suggestion: Optional[str] = None):
        self.name = name
        self.passed = passed
        self.message = message
        self.details = details or {}
        self.fix_suggestion = fix_suggestion
        self.timestamp = datetime.now()


class MonitoringSystemDebugger:
    """Comprehensive monitoring system debugger."""
    
    def __init__(self, verbose: bool = False, auto_fix: bool = False):
        self.verbose = verbose
        self.auto_fix = auto_fix
        self.results: List[DiagnosticResult] = []
        
        # Paths
        self.project_root = Path(__file__).parent.parent
        self.config_file = self.project_root / "config" / "realtime_monitoring_config.json"
        
        logger.info("🔧 Monitoring System Debugger initialized")
        
    def log_result(self, result: DiagnosticResult):
        """Log and store diagnostic result."""
        self.results.append(result)
        
        status = "✅ PASS" if result.passed else "❌ FAIL"
        logger.info(f"{status} {result.name}: {result.message}")
        
        if self.verbose and result.details:
            for key, value in result.details.items():
                logger.info(f"   {key}: {value}")
                
        if not result.passed and result.fix_suggestion:
            logger.warning(f"   💡 Fix: {result.fix_suggestion}")
            
    async def test_python_environment(self):
        """Test Python environment and required modules."""
        
        try:
            # Check Python version
            python_version = sys.version_info
            if python_version.major >= 3 and python_version.minor >= 8:
                self.log_result(DiagnosticResult(
                    "Python Version",
                    True,
                    f"Python {python_version.major}.{python_version.minor}.{python_version.micro}",
                    {"version": f"{python_version.major}.{python_version.minor}.{python_version.micro}"}
                ))
            else:
                self.log_result(DiagnosticResult(
                    "Python Version",
                    False,
                    f"Python {python_version.major}.{python_version.minor} too old",
                    {"required": ">=3.8", "current": f"{python_version.major}.{python_version.minor}"},
                    "Upgrade to Python 3.8 or higher"
                ))
                
        except Exception as e:
            self.log_result(DiagnosticResult(
                "Python Environment",
                False,
                f"Error checking Python: {e}",
                fix_suggestion="Check Python installation"
            ))
            
    async def test_required_dependencies(self):
        """Test required Python dependencies."""
        
        # Core dependencies for monitoring system
        dependencies = [
            ("asyncio", "Built-in async support"),
            ("json", "JSON processing"),
            ("logging", "Logging support"),
            ("datetime", "Date/time handling"),
            ("dataclasses", "Data structures"),
            ("pathlib", "Path handling"),
            ("asyncpg", "PostgreSQL async driver"),
            ("aiohttp", "HTTP client/server"),
            ("jinja2", "Template engine"),
            ("yaml", "YAML processing")
        ]
        
        available_deps = []
        missing_deps = []
        
        for dep_name, description in dependencies:
            try:
                if dep_name in ['asyncio', 'json', 'logging', 'datetime', 'dataclasses', 'pathlib']:
                    # Built-in modules
                    __import__(dep_name)
                    available_deps.append((dep_name, description))
                else:
                    # External dependencies
                    __import__(dep_name)
                    available_deps.append((dep_name, description))
            except ImportError:
                missing_deps.append((dep_name, description))
                
        if not missing_deps:
            self.log_result(DiagnosticResult(
                "Python Dependencies",
                True,
                f"All {len(available_deps)} dependencies available",
                {"available": [dep[0] for dep in available_deps]}
            ))
        else:
            self.log_result(DiagnosticResult(
                "Python Dependencies", 
                False,
                f"{len(missing_deps)} dependencies missing",
                {
                    "available": [dep[0] for dep in available_deps],
                    "missing": [dep[0] for dep in missing_deps]
                },
                f"Install missing dependencies: pip install {' '.join([dep[0] for dep in missing_deps])}"
            ))
            
    async def test_database_connectivity(self):
        """Test database connectivity and health."""
        
        db_configs = [
            {
                "name": "ATS-INTG PostgreSQL (Container)",
                "host": "ats-intg-postgres", 
                "port": 5432,
                "user": "postgres",
                "password": "intg_password",
                "database": "intg_db"
            },
            {
                "name": "ATS-INTG PostgreSQL (Localhost)",
                "host": "localhost",
                "port": 4432,
                "user": "postgres", 
                "password": "intg_password",
                "database": "intg_db"
            },
            {
                "name": "ATS-DEV PostgreSQL",
                "host": "localhost",
                "port": 3432,
                "user": "postgres",
                "password": "dev_password",
                "database": "dev_db"
            }
        ]
        
        for config in db_configs:
            try:
                # Test with psql command first (faster)
                cmd = [
                    "psql",
                    f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}",
                    "-c", "SELECT version();",
                    "-t"  # tuples only
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                
                if result.returncode == 0:
                    version = result.stdout.strip()
                    self.log_result(DiagnosticResult(
                        f"Database: {config['name']}",
                        True,
                        "Connection successful",
                        {
                            "host": f"{config['host']}:{config['port']}",
                            "database": config['database'],
                            "version": version[:50] + "..." if len(version) > 50 else version
                        }
                    ))
                    
                    # Test required tables
                    await self._test_database_tables(config)
                    
                else:
                    self.log_result(DiagnosticResult(
                        f"Database: {config['name']}",
                        False,
                        "Connection failed",
                        {
                            "host": f"{config['host']}:{config['port']}",
                            "error": result.stderr.strip()
                        },
                        f"Check if database is running and accessible at {config['host']}:{config['port']}"
                    ))
                    
            except subprocess.TimeoutExpired:
                self.log_result(DiagnosticResult(
                    f"Database: {config['name']}",
                    False,
                    "Connection timeout",
                    {"host": f"{config['host']}:{config['port']}"},
                    "Check network connectivity and database status"
                ))
            except FileNotFoundError:
                self.log_result(DiagnosticResult(
                    f"Database: {config['name']}",
                    False,
                    "psql command not found",
                    {"host": f"{config['host']}:{config['port']}"},
                    "Install PostgreSQL client tools"
                ))
            except Exception as e:
                self.log_result(DiagnosticResult(
                    f"Database: {config['name']}",
                    False,
                    f"Error: {str(e)}",
                    {"host": f"{config['host']}:{config['port']}"}
                ))
                
    async def _test_database_tables(self, config: Dict):
        """Test required database tables exist."""
        
        required_tables = [
            "intg_one_minute_live_tiingo",
            "intg_one_minute_live_polygon"
        ]
        
        for table in required_tables:
            try:
                cmd = [
                    "psql",
                    f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}",
                    "-c", f"SELECT COUNT(*) FROM {table} LIMIT 1;",
                    "-t"
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
                
                if result.returncode == 0:
                    count = result.stdout.strip()
                    self.log_result(DiagnosticResult(
                        f"Table: {table}",
                        True,
                        f"Exists with {count} records",
                        {"table": table, "records": count, "database": config['name']}
                    ))
                else:
                    self.log_result(DiagnosticResult(
                        f"Table: {table}",
                        False,
                        "Table missing or inaccessible",
                        {"table": table, "database": config['name'], "error": result.stderr.strip()},
                        f"Create table {table} or check permissions"
                    ))
                    
            except Exception as e:
                self.log_result(DiagnosticResult(
                    f"Table: {table}",
                    False,
                    f"Error checking table: {str(e)}",
                    {"table": table, "database": config['name']}
                ))
                
    async def test_configuration_files(self):
        """Test monitoring system configuration files."""
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    
                # Validate configuration structure
                required_sections = ['components', 'logging']
                missing_sections = [section for section in required_sections if section not in config]
                
                if not missing_sections:
                    self.log_result(DiagnosticResult(
                        "Configuration File",
                        True,
                        f"Valid configuration loaded",
                        {
                            "file": str(self.config_file),
                            "sections": list(config.keys()),
                            "size_kb": round(self.config_file.stat().st_size / 1024, 2)
                        }
                    ))
                    
                    # Test individual components configuration
                    await self._test_component_configs(config)
                    
                else:
                    self.log_result(DiagnosticResult(
                        "Configuration File",
                        False,
                        f"Missing required sections: {missing_sections}",
                        {"file": str(self.config_file), "missing": missing_sections},
                        "Add missing configuration sections"
                    ))
                    
            except json.JSONDecodeError as e:
                self.log_result(DiagnosticResult(
                    "Configuration File",
                    False,
                    f"Invalid JSON: {str(e)}",
                    {"file": str(self.config_file)},
                    "Fix JSON syntax errors in configuration file"
                ))
            except Exception as e:
                self.log_result(DiagnosticResult(
                    "Configuration File",
                    False,
                    f"Error reading config: {str(e)}",
                    {"file": str(self.config_file)}
                ))
        else:
            self.log_result(DiagnosticResult(
                "Configuration File",
                False,
                "Configuration file not found",
                {"expected_path": str(self.config_file)},
                f"Create configuration file at {self.config_file}"
            ))
            
    async def _test_component_configs(self, config: Dict):
        """Test individual component configurations."""
        
        components = config.get('components', {})
        
        for comp_name, comp_config in components.items():
            enabled = comp_config.get('enabled', False)
            
            if enabled:
                # Component-specific validation
                if comp_name == 'alerting':
                    channels = comp_config.get('channels', {})
                    active_channels = [name for name, ch_config in channels.items() if ch_config.get('enabled', False)]
                    
                    if active_channels:
                        self.log_result(DiagnosticResult(
                            f"Component: {comp_name}",
                            True,
                            f"Configured with {len(active_channels)} active channels",
                            {"enabled": enabled, "channels": active_channels}
                        ))
                    else:
                        self.log_result(DiagnosticResult(
                            f"Component: {comp_name}",
                            False,
                            "No active alert channels configured",
                            {"enabled": enabled, "channels": list(channels.keys())},
                            "Enable at least one alert channel (Slack, Email, etc.)"
                        ))
                        
                elif comp_name == 'dashboard':
                    port = comp_config.get('port', 4008)
                    host = comp_config.get('host', '0.0.0.0')
                    
                    self.log_result(DiagnosticResult(
                        f"Component: {comp_name}",
                        True,
                        f"Configured for {host}:{port}",
                        {"enabled": enabled, "host": host, "port": port}
                    ))
                    
                elif comp_name == 'prometheus':
                    port = comp_config.get('port', 8091)
                    
                    self.log_result(DiagnosticResult(
                        f"Component: {comp_name}",
                        True,
                        f"Configured on port {port}",
                        {"enabled": enabled, "port": port}
                    ))
                    
                else:
                    self.log_result(DiagnosticResult(
                        f"Component: {comp_name}",
                        True,
                        "Enabled",
                        {"enabled": enabled}
                    ))
            else:
                self.log_result(DiagnosticResult(
                    f"Component: {comp_name}",
                    True,
                    "Disabled",
                    {"enabled": enabled}
                ))
                
    async def test_network_ports(self):
        """Test network port availability."""
        
        ports_to_test = [
            (4008, "Monitoring Dashboard"),
            (8091, "Prometheus Metrics"),
            (3000, "ATS-DEV Analytics"),
            (4000, "ATS-INTG Dashboard"),
            (4432, "ATS-INTG PostgreSQL"),
            (3432, "ATS-DEV PostgreSQL")
        ]
        
        for port, service in ports_to_test:
            try:
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('localhost', port))
                sock.close()
                
                if result == 0:
                    # Port is in use
                    self.log_result(DiagnosticResult(
                        f"Port {port} ({service})",
                        True,
                        "Port in use (service may be running)",
                        {"port": port, "service": service, "status": "in_use"}
                    ))
                else:
                    # Port is available
                    self.log_result(DiagnosticResult(
                        f"Port {port} ({service})",
                        True,
                        "Port available",
                        {"port": port, "service": service, "status": "available"}
                    ))
                    
            except Exception as e:
                self.log_result(DiagnosticResult(
                    f"Port {port} ({service})",
                    False,
                    f"Error checking port: {str(e)}",
                    {"port": port, "service": service}
                ))
                
    async def test_docker_environment(self):
        """Test Docker environment and containers."""
        
        try:
            # Check if Docker is available
            result = subprocess.run(['docker', '--version'], capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                docker_version = result.stdout.strip()
                self.log_result(DiagnosticResult(
                    "Docker Installation",
                    True,
                    "Docker available",
                    {"version": docker_version}
                ))
                
                # Check ATS containers
                await self._test_docker_containers()
                
            else:
                self.log_result(DiagnosticResult(
                    "Docker Installation",
                    False,
                    "Docker not responding",
                    {"error": result.stderr.strip()},
                    "Check Docker installation and service status"
                ))
                
        except FileNotFoundError:
            self.log_result(DiagnosticResult(
                "Docker Installation",
                False,
                "Docker command not found",
                {},
                "Install Docker or check PATH"
            ))
        except Exception as e:
            self.log_result(DiagnosticResult(
                "Docker Installation",
                False,
                f"Error checking Docker: {str(e)}"
            ))
            
    async def _test_docker_containers(self):
        """Test ATS Docker containers status."""
        
        try:
            result = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'], 
                                  capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                containers = result.stdout.strip().split('\n')[1:]  # Skip header
                ats_containers = [c for c in containers if 'ats-' in c.lower() or 'intg' in c.lower()]
                
                if ats_containers:
                    healthy_count = sum(1 for c in ats_containers if 'healthy' in c.lower() or 'up' in c.lower())
                    
                    self.log_result(DiagnosticResult(
                        "Docker Containers",
                        True,
                        f"Found {len(ats_containers)} ATS containers, {healthy_count} healthy",
                        {
                            "total_containers": len(ats_containers),
                            "healthy_containers": healthy_count,
                            "containers": [c.split('\t')[0] for c in ats_containers]
                        }
                    ))
                else:
                    self.log_result(DiagnosticResult(
                        "Docker Containers",
                        False,
                        "No ATS containers found",
                        {"total_containers": len(containers)},
                        "Start ATS containers using Docker Compose"
                    ))
            else:
                self.log_result(DiagnosticResult(
                    "Docker Containers",
                    False,
                    "Failed to list containers",
                    {"error": result.stderr.strip()}
                ))
                
        except Exception as e:
            self.log_result(DiagnosticResult(
                "Docker Containers",
                False,
                f"Error checking containers: {str(e)}"
            ))
            
    async def test_monitoring_processes(self):
        """Test existing monitoring processes."""
        
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=5)
            
            if result.returncode == 0:
                processes = result.stdout.split('\n')
                monitoring_processes = [
                    p for p in processes 
                    if any(keyword in p.lower() for keyword in ['monitoring', 'simple_wsl_monitor', 'realtime_collector'])
                ]
                
                if monitoring_processes:
                    self.log_result(DiagnosticResult(
                        "Monitoring Processes",
                        True,
                        f"Found {len(monitoring_processes)} monitoring processes",
                        {"process_count": len(monitoring_processes)}
                    ))
                    
                    for proc in monitoring_processes[:3]:  # Show first 3
                        parts = proc.split()
                        if len(parts) >= 11:
                            cmd = ' '.join(parts[10:])[:60] + "..." if len(' '.join(parts[10:])) > 60 else ' '.join(parts[10:])
                            logger.info(f"   Process: {cmd}")
                else:
                    self.log_result(DiagnosticResult(
                        "Monitoring Processes",
                        True,
                        "No monitoring processes currently running",
                        {"process_count": 0}
                    ))
            else:
                self.log_result(DiagnosticResult(
                    "Monitoring Processes",
                    False,
                    "Failed to list processes",
                    {"error": result.stderr.strip()}
                ))
                
        except Exception as e:
            self.log_result(DiagnosticResult(
                "Monitoring Processes",
                False,
                f"Error checking processes: {str(e)}"
            ))
            
    async def test_real_time_data_health(self):
        """Test real-time data collection health."""
        
        try:
            # Test current data freshness
            cmd = [
                "psql",
                "postgresql://postgres:intg_password@localhost:4432/intg_db",
                "-c", """
                SELECT 
                    vendor,
                    symbol,
                    COUNT(*) as records_last_hour,
                    EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutes_old,
                    ROUND(AVG(quality_score), 3) as avg_quality
                FROM (
                    SELECT 'Tiingo' as vendor, symbol, timestamp, quality_score 
                    FROM intg_one_minute_live_tiingo 
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    UNION ALL
                    SELECT 'Polygon' as vendor, symbol, timestamp, quality_score 
                    FROM intg_one_minute_live_polygon 
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                ) combined
                GROUP BY vendor, symbol
                ORDER BY vendor, symbol;
                """,
                "-t"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                lines = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
                
                if lines:
                    data_health = {}
                    stale_data_count = 0
                    
                    for line in lines:
                        parts = line.split('|')
                        if len(parts) >= 5:
                            vendor = parts[0].strip()
                            symbol = parts[1].strip()
                            records = int(parts[2].strip()) if parts[2].strip().isdigit() else 0
                            minutes_old = float(parts[3].strip()) if parts[3].strip().replace('.', '').isdigit() else 999
                            avg_quality = float(parts[4].strip()) if parts[4].strip().replace('.', '').isdigit() else 0
                            
                            data_health[f"{vendor}_{symbol}"] = {
                                "records": records,
                                "minutes_old": minutes_old,
                                "quality": avg_quality
                            }
                            
                            if minutes_old > 5:  # Stale if > 5 minutes
                                stale_data_count += 1
                    
                    if stale_data_count == 0:
                        self.log_result(DiagnosticResult(
                            "Real-time Data Health",
                            True,
                            f"All data streams fresh ({len(data_health)} streams)",
                            {"streams": len(data_health), "stale_streams": stale_data_count}
                        ))
                    else:
                        self.log_result(DiagnosticResult(
                            "Real-time Data Health",
                            False,
                            f"{stale_data_count} stale data streams detected",
                            {"streams": len(data_health), "stale_streams": stale_data_count},
                            "Check real-time data collectors and database connectivity"
                        ))
                        
                    if self.verbose:
                        for stream, health in data_health.items():
                            logger.info(f"   {stream}: {health['records']} records, {health['minutes_old']:.1f}min old, {health['quality']:.3f} quality")
                else:
                    self.log_result(DiagnosticResult(
                        "Real-time Data Health",
                        False,
                        "No real-time data found",
                        {},
                        "Check if real-time data collectors are running"
                    ))
            else:
                self.log_result(DiagnosticResult(
                    "Real-time Data Health",
                    False,
                    "Failed to query real-time data",
                    {"error": result.stderr.strip()},
                    "Check database connectivity and table permissions"
                ))
                
        except Exception as e:
            self.log_result(DiagnosticResult(
                "Real-time Data Health",
                False,
                f"Error checking data health: {str(e)}"
            ))
            
    async def attempt_fixes(self):
        """Attempt automatic fixes for identified issues."""
        
        if not self.auto_fix:
            return
            
        logger.info("🔧 Attempting automatic fixes...")
        
        failed_results = [r for r in self.results if not r.passed and r.fix_suggestion]
        
        if not failed_results:
            logger.info("✅ No issues found that can be automatically fixed")
            return
            
        for result in failed_results:
            logger.info(f"🔧 Attempting to fix: {result.name}")
            logger.info(f"   Issue: {result.message}")
            logger.info(f"   Fix: {result.fix_suggestion}")
            
            # Implement specific fixes here
            if "missing dependencies" in result.message.lower():
                await self._fix_missing_dependencies(result)
            elif "configuration file not found" in result.message.lower():
                await self._fix_missing_config()
            elif "port" in result.name.lower() and "in use" in result.message.lower():
                await self._fix_port_conflicts(result)
                
    async def _fix_missing_dependencies(self, result: DiagnosticResult):
        """Fix missing Python dependencies."""
        
        missing = result.details.get('missing', [])
        if missing:
            try:
                cmd = [sys.executable, '-m', 'pip', 'install'] + missing
                logger.info(f"   Running: {' '.join(cmd)}")
                
                install_result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                
                if install_result.returncode == 0:
                    logger.info(f"   ✅ Successfully installed dependencies: {missing}")
                else:
                    logger.error(f"   ❌ Failed to install dependencies: {install_result.stderr.strip()}")
                    
            except Exception as e:
                logger.error(f"   ❌ Error installing dependencies: {str(e)}")
                
    async def _fix_missing_config(self):
        """Create default configuration file if missing."""
        
        try:
            self.config_file.parent.mkdir(exist_ok=True)
            
            # Create basic configuration
            default_config = {
                "description": "Auto-generated ATS Real-time Monitoring Configuration",
                "version": "1.0",
                "last_updated": datetime.now().isoformat(),
                "components": {
                    "monitor": {"enabled": True, "interval_seconds": 60},
                    "alerting": {"enabled": True, "test_on_startup": False},
                    "dashboard": {"enabled": True, "port": 4008},
                    "prometheus": {"enabled": True, "port": 8091}
                },
                "logging": {"level": "INFO"}
            }
            
            with open(self.config_file, 'w') as f:
                json.dump(default_config, f, indent=2)
                
            logger.info(f"   ✅ Created default configuration at {self.config_file}")
            
        except Exception as e:
            logger.error(f"   ❌ Failed to create configuration file: {str(e)}")
            
    async def _fix_port_conflicts(self, result: DiagnosticResult):
        """Handle port conflicts."""
        
        port = result.details.get('port')
        if port:
            logger.info(f"   ℹ️ Port {port} is in use - this may be normal if services are already running")
            
            # Could implement more sophisticated port conflict resolution here
            
    def generate_report(self) -> str:
        """Generate comprehensive diagnostic report."""
        
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r.passed)
        failed_tests = total_tests - passed_tests
        
        report = []
        report.append("=" * 80)
        report.append("ATS REAL-TIME COLLECTION MONITORING SYSTEM - DIAGNOSTIC REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Tests: {total_tests}")
        report.append(f"Passed: {passed_tests}")
        report.append(f"Failed: {failed_tests}")
        report.append(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")
        report.append("")
        
        # Group results by category
        categories = {}
        for result in self.results:
            category = result.name.split(':')[0] if ':' in result.name else "General"
            if category not in categories:
                categories[category] = []
            categories[category].append(result)
            
        for category, category_results in categories.items():
            report.append(f"📋 {category.upper()}")
            report.append("-" * 40)
            
            for result in category_results:
                status = "✅" if result.passed else "❌"
                report.append(f"{status} {result.name}: {result.message}")
                
                if not result.passed and result.fix_suggestion:
                    report.append(f"   💡 Fix: {result.fix_suggestion}")
                    
            report.append("")
            
        # Summary and recommendations
        if failed_tests > 0:
            report.append("🚨 ISSUES IDENTIFIED")
            report.append("-" * 40)
            
            critical_issues = [r for r in self.results if not r.passed and any(keyword in r.name.lower() for keyword in ['database', 'dependencies', 'configuration'])]
            
            if critical_issues:
                report.append("Critical issues that must be resolved:")
                for issue in critical_issues:
                    report.append(f"  ❌ {issue.name}: {issue.message}")
                    if issue.fix_suggestion:
                        report.append(f"     💡 {issue.fix_suggestion}")
                        
            report.append("")
            report.append("🔧 RECOMMENDED ACTIONS")
            report.append("-" * 40)
            report.append("1. Review and resolve critical issues above")
            report.append("2. Install missing dependencies if any")
            report.append("3. Check database connectivity")
            report.append("4. Verify configuration files")
            report.append("5. Re-run diagnostics after fixes")
            
        else:
            report.append("🎉 ALL DIAGNOSTICS PASSED!")
            report.append("-" * 40)
            report.append("✅ System appears healthy and ready for monitoring")
            report.append("✅ All dependencies available")
            report.append("✅ Database connectivity working")
            report.append("✅ Configuration files valid")
            report.append("")
            report.append("🚀 READY TO START MONITORING")
            report.append("Run: ./scripts/start_monitoring.sh")
            
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
        
    async def run_diagnostics(self):
        """Run all diagnostic tests."""
        
        logger.info("🔧 Starting comprehensive monitoring system diagnostics...")
        logger.info("=" * 80)
        
        # Run all diagnostic tests
        diagnostic_tasks = [
            self.test_python_environment(),
            self.test_required_dependencies(), 
            self.test_configuration_files(),
            self.test_database_connectivity(),
            self.test_network_ports(),
            self.test_docker_environment(),
            self.test_monitoring_processes(),
            self.test_real_time_data_health()
        ]
        
        await asyncio.gather(*diagnostic_tasks, return_exceptions=True)
        
        # Attempt fixes if requested
        if self.auto_fix:
            await self.attempt_fixes()
            
        # Generate and display report
        report = self.generate_report()
        
        logger.info("\n" + report)
        
        return len([r for r in self.results if not r.passed]) == 0


async def main():
    """Main diagnostic function."""
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ATS Real-time Collection Monitoring System Diagnostics',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output with detailed information')
    parser.add_argument('--fix-issues', '-f', action='store_true', help='Attempt automatic fixes for identified issues')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='Log level')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run diagnostics
    debugger = MonitoringSystemDebugger(verbose=args.verbose, auto_fix=args.fix_issues)
    
    try:
        success = await debugger.run_diagnostics()
        
        if success:
            logger.info("\n🎉 All diagnostics passed! System is ready for monitoring.")
            sys.exit(0)
        else:
            logger.error("\n❌ Some diagnostics failed. Please review and fix the issues above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n📤 Diagnostics interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Diagnostic error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())