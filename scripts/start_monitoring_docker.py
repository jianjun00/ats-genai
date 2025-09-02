#!/usr/bin/env python3
"""
ATS Real-time Collection Monitoring System - Docker-based Startup

This script fixes the aiohttp dependency issue by running the monitoring system
within the Docker environment where all dependencies are available.

Usage:
    python3 scripts/start_monitoring_docker.py
    
This addresses the user request to "fix the issues" by providing a working
startup method for the monitoring system.
"""

import os
import sys
import json
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_prerequisites():
    """Check that prerequisites are met."""
    logger.info("🔍 Checking prerequisites...")
    
    # Check config file exists
    config_path = Path(__file__).parent.parent / "config" / "realtime_monitoring_config.json"
    if not config_path.exists():
        logger.error(f"❌ Configuration file not found: {config_path}")
        return False
    
    # Check Docker is available
    try:
        result = subprocess.run(["docker", "--version"], 
                              capture_output=True, text=True, check=True)
        logger.info(f"✅ Docker available: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("❌ Docker not available or not installed")
        return False
    
    # Check ATS Docker image is available
    try:
        result = subprocess.run([
            "docker", "run", "--rm", "dragonflyer762/ats-genai:latest",
            "python3", "-c", "import aiohttp; print('Dependencies OK')"
        ], capture_output=True, text=True, check=True)
        logger.info("✅ ATS Docker image with dependencies available")
    except subprocess.CalledProcessError:
        logger.error("❌ ATS Docker image or dependencies not available")
        return False
    
    # Check database connection
    try:
        result = subprocess.run([
            "PGPASSWORD=intg_password", "pg_isready", 
            "-h", "localhost", "-p", "4432", "-U", "postgres", "-d", "intg_db"
        ], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✅ ATS-INTG database connection available")
        else:
            logger.warning("⚠️ ATS-INTG database not available - monitoring will use mock data")
    except FileNotFoundError:
        logger.warning("⚠️ pg_isready not available - skipping database check")
    
    return True


def start_monitoring_in_docker():
    """Start the monitoring system within Docker container."""
    logger.info("🚀 Starting ATS Real-time Monitoring System in Docker...")
    
    project_root = Path(__file__).parent.parent
    config_path = project_root / "config" / "realtime_monitoring_config.json"
    
    # Build Docker command
    docker_cmd = [
        "docker", "run",
        "--rm",
        "--name", "ats-realtime-monitoring",
        "--network", "ats-network",  # Connect to ATS network for database access
        "-p", "8090:8090",  # Dashboard port
        "-p", "8091:8091",  # Metrics port
        "-v", f"{project_root}:/workspace",
        "-w", "/workspace",
        "-e", "PYTHONPATH=src",
        "-e", "ALERT_EMAIL_RECIPIENTS=jianjun00@gmail.com",
        "-e", f"CONFIG_FILE=/workspace/config/realtime_monitoring_config.json"
    ]
    
    # Add email configuration if available
    smtp_password = os.getenv("SMTP_PASSWORD")
    if smtp_password:
        docker_cmd.extend([
            "-e", f"SMTP_PASSWORD={smtp_password}",
            "-e", "SMTP_SERVER=smtp.gmail.com",
            "-e", "SMTP_PORT=587",
            "-e", "SMTP_USERNAME=jianjun00@gmail.com",
            "-e", "SMTP_USE_TLS=true"
        ])
        logger.info("✅ Email alerts configured")
    else:
        logger.warning("⚠️ SMTP_PASSWORD not set - email alerts disabled")
        logger.info("   To enable: export SMTP_PASSWORD=your_gmail_app_password")
    
    # Complete Docker command - use standalone version that works with basic dependencies
    docker_cmd.extend([
        "dragonflyer762/ats-genai:latest",
        "python3", "scripts/start_standalone_monitoring.py"
    ])
    
    logger.info("🔧 Docker command configured")
    logger.info("📊 Access points will be:")
    logger.info("   Dashboard:  http://localhost:8090")
    logger.info("   Metrics:    http://localhost:8091/metrics")
    logger.info("   Health:     http://localhost:8090/health")
    logger.info("")
    logger.info("🎯 Starting monitoring container...")
    
    try:
        # Start the monitoring system
        subprocess.run(docker_cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to start monitoring system: {e}")
        return False
    except KeyboardInterrupt:
        logger.info("\n⏹️ Monitoring system stopped by user")
        return True
    
    return True


def main():
    """Main function."""
    logger.info("🎯 ATS Real-time Collection Monitoring System")
    logger.info("🐳 Docker-based Startup (Fixed Version)")
    logger.info("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        logger.error("❌ Prerequisites not met - cannot start monitoring")
        return False
    
    logger.info("✅ All prerequisites met")
    logger.info("")
    
    # Start monitoring
    success = start_monitoring_in_docker()
    
    if success:
        logger.info("✅ Monitoring system startup completed")
    else:
        logger.error("❌ Monitoring system startup failed")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)