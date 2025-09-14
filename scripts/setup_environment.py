#!/usr/bin/env python3
"""
Environment Setup Script
========================

Sets up the Data Quality Agent environment with proper configuration,
directories, and initial setup for production deployment.
"""

import os
import sys
import json
import shutil
from pathlib import Path
from typing import Dict, Any, Optional

class EnvironmentSetup:
    """Setup environment for Data Quality Agent"""

    def __init__(self, environment: str = "production"):
        self.environment = environment
        self.project_root = Path(__file__).parent.parent
        self.config_dir = self.project_root / "config"
        self.logs_dir = self.project_root / "logs"

    def setup_directories(self):
        """Create necessary directories"""
        print("📁 Setting up directories...")

        directories = [
            "logs/agent",
            "logs/system",
            "logs/alerts",
            "logs/validation",
            "logs/api_tests",
            "logs/database_tests",
            "config/dashboards",
            "data/training_data",
            "data/checkpoints",
            "backup"
        ]

        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"  ✅ Created {directory}")

        # Set permissions for log directories
        logs_path = self.project_root / "logs"
        if logs_path.exists():
            os.chmod(logs_path, 0o755)

    def setup_configuration(self):
        """Setup configuration files"""
        print(f"⚙️ Setting up {self.environment} configuration...")

        # Copy template files
        template_file = self.config_dir / f"{self.environment}.env.template"
        env_file = self.config_dir / f"{self.environment}.env"

        if template_file.exists():
            if not env_file.exists():
                shutil.copy(template_file, env_file)
                print(f"  ✅ Created {env_file}")
                print(f"  ⚠️  Please edit {env_file} with your actual configuration values")
            else:
                print(f"  ✅ Configuration file {env_file} already exists")
        else:
            print(f"  ❌ Template file {template_file} not found")

    def setup_agent_config(self):
        """Setup agent-specific configuration"""
        print("🤖 Setting up agent configuration...")

        # Create default agent configuration
        config = {
            "monitoring": {
                "cycle_interval_seconds": 300 if self.environment == "production" else 60,
                "stall_threshold_minutes": 60,
                "max_concurrent_workflows": 20 if self.environment == "production" else 5,
                "health_check_interval_seconds": 60,
                "cleanup_old_workflows_days": 30
            },
            "issue_thresholds": {
                "extreme_volume_multiplier": 50.0,
                "extreme_price_change_percent": 20.0,
                "data_staleness_hours": 24,
                "max_missing_consecutive_days": 3,
                "quality_score_critical_threshold": 50,
                "quality_score_warning_threshold": 75
            },
            "action_thresholds": {
                "auto_resolve_confidence_threshold": 0.85,
                "escalation_confidence_threshold": 0.3,
                "backfill_auto_trigger_threshold": 5,
                "cross_validation_vendor_count": 2,
                "max_retry_attempts": 3
            },
            "vendor_config": {
                "primary_vendors": ["polygon", "tiingo", "eodhd"],
                "secondary_vendors": ["eodhd", "tiingo"],
                "vendor_priorities": {
                    "polygon": 1,
                    "tiingo": 2,
                    "eodhd": 3
                },
                "rate_limits": {
                    "polygon": {"requests_per_minute": 5, "requests_per_day": 100},
                    "tiingo": {"requests_per_minute": 10, "requests_per_day": 500},
                    "eodhd": {"requests_per_minute": 20, "requests_per_day": 1000}
                }
            },
            "notifications": {
                "enable_email_notifications": self.environment == "production",
                "enable_slack_notifications": self.environment == "production",
                "email_recipients": [],
                "slack_webhook_url": None,
                "notification_severity_threshold": "high",
                "max_notifications_per_hour": 10
            },
            "enable_autonomous_mode": True,
            "enable_learning_mode": True,
            "enable_reflection": True,
            "log_level": "INFO" if self.environment == "production" else "DEBUG",
            "max_memory_mb": 1024,
            "max_cpu_percent": 50.0,
            "enable_metrics_collection": True
        }

        config_file = self.config_dir / "agent_config.json"

        if not config_file.exists():
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            print(f"  ✅ Created {config_file}")
        else:
            print(f"  ✅ Agent configuration {config_file} already exists")

    def setup_docker_compose(self):
        """Setup Docker Compose configuration"""
        print("🐳 Setting up Docker Compose configuration...")

        if self.environment == "production":
            compose_content = """version: '3.8'

services:
  ats-prod-postgres:
    image: timescale/timescaledb:latest-pg13
    container_name: ats-prod-postgres
    environment:
      POSTGRES_DB: prod_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "5433:5432"
    volumes:
      - ats_prod_data:/var/lib/postgresql/data
      - ./backup:/backup
    networks:
      - ats-network
    restart: unless-stopped

  ats-prod-analytics:
    image: dragonflyer762/ats-genai:latest
    container_name: ats-prod-analytics
    environment:
      PYTHONPATH: /workspace/src
      DB_HOST: ats-prod-postgres
      DB_PORT: 5432
      DB_USER: postgres
      DB_PASSWORD: ${DB_PASSWORD}
      DB_NAME: prod_db
      ENVIRONMENT: production
    ports:
      - "4000:3000"
    volumes:
      - .:/workspace
      - ./logs:/workspace/logs
      - ./data:/data
    networks:
      - ats-network
    depends_on:
      - ats-prod-postgres
    restart: unless-stopped
    command: python src/services/analytics_service.py

volumes:
  ats_prod_data:

networks:
  ats-network:
    external: true
"""
        else:
            compose_content = """version: '3.8'

services:
  ats-dev-postgres:
    image: timescale/timescaledb:latest-pg13
    container_name: ats-dev-postgres
    environment:
      POSTGRES_DB: dev_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: dev_password
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "5432:5432"
    volumes:
      - ats_dev_data:/var/lib/postgresql/data
    networks:
      - ats-network

  ats-dev-analytics:
    image: dragonflyer762/ats-genai:latest
    container_name: ats-dev-analytics
    environment:
      PYTHONPATH: /workspace/src
      DB_HOST: ats-dev-postgres
      DB_PORT: 5432
      DB_USER: postgres
      DB_PASSWORD: dev_password
      DB_NAME: dev_db
      ENVIRONMENT: development
    ports:
      - "3000:3000"
    volumes:
      - .:/workspace
      - ./logs:/workspace/logs
      - ./data:/data
    networks:
      - ats-network
    depends_on:
      - ats-dev-postgres
    command: python src/services/analytics_service.py

volumes:
  ats_dev_data:

networks:
  ats-network:
    external: true
"""

        compose_file = self.project_root / f"docker-compose.{self.environment}.yml"

        if not compose_file.exists():
            with open(compose_file, 'w') as f:
                f.write(compose_content)
            print(f"  ✅ Created {compose_file}")
        else:
            print(f"  ✅ Docker Compose file {compose_file} already exists")

    def setup_systemd_service(self):
        """Setup systemd service for production"""
        if self.environment != "production":
            return

        print("🔧 Setting up systemd service...")

        service_content = f"""[Unit]
Description=ATS Data Quality Agent
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory={self.project_root}
ExecStart=/usr/bin/docker-compose -f docker-compose.production.yml up -d
ExecStop=/usr/bin/docker-compose -f docker-compose.production.yml down
ExecReload=/usr/bin/docker-compose -f docker-compose.production.yml restart
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
"""

        service_file = self.project_root / "ats-data-quality-agent.service"

        with open(service_file, 'w') as f:
            f.write(service_content)

        print(f"  ✅ Created {service_file}")
        print("  📋 To install system service:")
        print(f"     sudo cp {service_file} /etc/systemd/system/")
        print("     sudo systemctl daemon-reload")
        print("     sudo systemctl enable ats-data-quality-agent")
        print("     sudo systemctl start ats-data-quality-agent")

    def setup_cron_jobs(self):
        """Setup cron jobs for maintenance"""
        print("⏰ Setting up cron jobs...")

        cron_script_content = f"""#!/bin/bash
# ATS Data Quality Agent Maintenance Scripts

# Log cleanup (daily at 3 AM)
0 3 * * * cd {self.project_root} && find logs/ -name "*.log" -mtime +30 -delete

# System health check (every 6 hours)
0 */6 * * * cd {self.project_root} && python scripts/quick_health_check.py >> logs/system/health_check.log 2>&1

# Weekly system validation (Sundays at 2 AM)
0 2 * * 0 cd {self.project_root} && python scripts/validate_system.py >> logs/system/weekly_validation.log 2>&1

# Database backup (daily at 1 AM for production)
{"0 1 * * * cd " + str(self.project_root) + " && ./scripts/backup_database.sh" if self.environment == "production" else "# Database backup disabled for development"}
"""

        cron_file = self.project_root / "scripts" / f"crontab.{self.environment}"

        with open(cron_file, 'w') as f:
            f.write(cron_script_content)

        print(f"  ✅ Created {cron_file}")
        print("  📋 To install cron jobs:")
        print(f"     crontab {cron_file}")

    def create_startup_script(self):
        """Create startup script"""
        print("🚀 Creating startup script...")

        startup_content = f"""#!/bin/bash
# ATS Data Quality Agent Startup Script

set -e

echo "🚀 Starting ATS Data Quality Agent ({self.environment})"

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed"
    exit 1
fi

# Create network if it doesn't exist
docker network create ats-network 2>/dev/null || true

# Load environment variables
if [ -f config/{self.environment}.env ]; then
    export $(cat config/{self.environment}.env | grep -v '^#' | xargs)
    echo "✅ Loaded environment configuration"
else
    echo "⚠️  Environment file config/{self.environment}.env not found"
fi

# Start services
echo "🐳 Starting Docker services..."
docker-compose -f docker-compose.{self.environment}.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Health check
echo "🩺 Running health check..."
python scripts/quick_health_check.py

if [ $? -eq 0 ]; then
    echo "✅ ATS Data Quality Agent started successfully!"
    echo "📊 Dashboard: http://localhost:{'4000' if self.environment == 'production' else '3000'}/data-quality/dashboard"
else
    echo "❌ Health check failed - please check logs"
    exit 1
fi
"""

        startup_script = self.project_root / "scripts" / f"start_{self.environment}.sh"

        with open(startup_script, 'w') as f:
            f.write(startup_content)

        # Make executable
        os.chmod(startup_script, 0o755)

        print(f"  ✅ Created {startup_script}")

    def create_monitoring_script(self):
        """Create monitoring and alerting script"""
        print("📊 Creating monitoring script...")

        monitoring_content = f"""#!/bin/bash
# ATS Data Quality Agent Monitoring Script

# Function to send alert
send_alert() {{
    local message="$1"
    local severity="$2"

    echo "$(date): [$severity] $message" >> logs/system/monitoring.log

    # Send Slack notification if configured
    if [ ! -z "$SLACK_WEBHOOK_URL" ]; then
        curl -X POST -H 'Content-type: application/json' \\
            --data "{{\\"text\\":\\"[$severity] ATS Agent Alert: $message\\"}}" \\
            "$SLACK_WEBHOOK_URL" 2>/dev/null || true
    fi
}}

# Check if services are running
check_services() {{
    local failed=0

    if ! docker ps | grep -q "ats-{self.environment}-postgres"; then
        send_alert "PostgreSQL service not running" "CRITICAL"
        failed=1
    fi

    if ! docker ps | grep -q "ats-{self.environment}-analytics"; then
        send_alert "Analytics service not running" "CRITICAL"
        failed=1
    fi

    return $failed
}}

# Check API health
check_api_health() {{
    local api_url="http://localhost:{'4000' if self.environment == 'production' else '3000'}/health"

    if ! curl -f "$api_url" >/dev/null 2>&1; then
        send_alert "API health check failed" "HIGH"
        return 1
    fi

    return 0
}}

# Check disk space
check_disk_space() {{
    local usage=$(df / | awk 'NR==2 {{print $5}}' | sed 's/%//')

    if [ $usage -gt 90 ]; then
        send_alert "Disk usage critical: ${{usage}}%" "CRITICAL"
        return 1
    elif [ $usage -gt 80 ]; then
        send_alert "Disk usage warning: ${{usage}}%" "WARNING"
        return 1
    fi

    return 0
}}

# Main monitoring loop
main() {{
    echo "🔍 Starting monitoring check..."

    local errors=0

    check_services || ((errors++))
    check_api_health || ((errors++))
    check_disk_space || ((errors++))

    if [ $errors -eq 0 ]; then
        echo "✅ All checks passed"
    else
        echo "❌ $errors checks failed"
        exit 1
    fi
}}

# Load environment if available
if [ -f config/{self.environment}.env ]; then
    export $(cat config/{self.environment}.env | grep -v '^#' | xargs)
fi

main "$@"
"""

        monitoring_script = self.project_root / "scripts" / f"monitor_{self.environment}.sh"

        with open(monitoring_script, 'w') as f:
            f.write(monitoring_content)

        # Make executable
        os.chmod(monitoring_script, 0o755)

        print(f"  ✅ Created {monitoring_script}")

    def run_setup(self):
        """Run complete environment setup"""
        print(f"🔧 Setting up {self.environment} environment for ATS Data Quality Agent")
        print("=" * 70)

        try:
            self.setup_directories()
            self.setup_configuration()
            self.setup_agent_config()
            self.setup_docker_compose()
            self.setup_systemd_service()
            self.setup_cron_jobs()
            self.create_startup_script()
            self.create_monitoring_script()

            print("\n" + "=" * 70)
            print("✅ Environment setup completed successfully!")
            print("=" * 70)

            print(f"\n📋 Next steps for {self.environment}:")
            print(f"1. Edit config/{self.environment}.env with your actual configuration")
            print("2. Update config/agent_config.json if needed")
            print(f"3. Run: ./scripts/start_{self.environment}.sh")
            print("4. Access dashboard: http://localhost:{}/data-quality/dashboard".format(
                "4000" if self.environment == "production" else "3000"))

            if self.environment == "production":
                print("\n🔒 Production-specific steps:")
                print("1. Set up SSL/TLS certificates")
                print("2. Configure firewall rules")
                print("3. Set up monitoring and alerting")
                print("4. Install systemd service")
                print("5. Set up automated backups")

        except Exception as e:
            print(f"\n❌ Setup failed: {str(e)}")
            sys.exit(1)

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Setup ATS Data Quality Agent environment")
    parser.add_argument("environment", choices=["development", "production"],
                       help="Environment to setup")

    args = parser.parse_args()

    setup = EnvironmentSetup(args.environment)
    setup.run_setup()

if __name__ == "__main__":
    main()