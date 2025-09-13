#!/bin/bash
"""
Setup Cron Scheduling for ATS Data Coverage Monitoring
Configures automated daily monitoring, alerting, and dashboard updates
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔧 ATS Data Coverage Monitoring - Cron Setup${NC}"
echo "=============================================================="

# Project root directory
PROJECT_ROOT="/home/jianjun/ats-genai-pm"
SCRIPTS_DIR="$PROJECT_ROOT/scripts"
LOGS_DIR="$PROJECT_ROOT/logs/monitoring"

# Create logs directory
echo -e "${YELLOW}📁 Creating monitoring logs directory...${NC}"
mkdir -p "$LOGS_DIR"

# Environment variables for monitoring
ENV_FILE="$PROJECT_ROOT/.env.monitoring"

echo -e "${YELLOW}⚙️ Creating monitoring environment file...${NC}"
cat > "$ENV_FILE" << 'EOF'
# ATS Data Coverage Monitoring Configuration

# Database Configuration
DB_HOST=localhost
DB_PORT=4432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db

# Dashboard Configuration
DASHBOARD_PORT=8080
DASHBOARD_HOST=localhost

# Alert Configuration
ALERT_EMAIL_ENABLED=false
ALERT_SLACK_ENABLED=true

# Slack Configuration (Set your webhook URL)
SLACK_WEBHOOK_URL=
SLACK_CHANNEL=#ats-data-alerts
SLACK_USERNAME=ATS Coverage Monitor
SLACK_ICON=:chart_with_upwards_trend:

# Grafana Configuration
GRAFANA_API_URL=http://localhost:3000
GRAFANA_API_KEY=
GRAFANA_ORG_ID=1

# Python environment
PYTHONPATH=/home/jianjun/ats-genai-pm/src
EOF

echo -e "${GREEN}✅ Created environment file: $ENV_FILE${NC}"

# Create monitoring scripts
echo -e "${YELLOW}📝 Creating monitoring scripts...${NC}"

# 1. Daily Coverage Monitoring Script
cat > "$SCRIPTS_DIR/run_daily_coverage_monitoring.sh" << 'EOF'
#!/bin/bash
# Daily Coverage Monitoring Script
# Runs comprehensive coverage analysis and alerting

set -e

# Load environment
cd /home/jianjun/ats-genai-pm
source .env.monitoring 2>/dev/null || true

# Set required environment variables
export PYTHONPATH="/home/jianjun/ats-genai-pm/src"
export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-4432}"
export DB_USER="${DB_USER:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-intg_password}"
export DB_NAME="${DB_NAME:-intg_db}"

# Logging
LOG_FILE="/home/jianjun/ats-genai-pm/logs/monitoring/daily_monitoring_$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

echo "$(date): Starting daily coverage monitoring" >> "$LOG_FILE"

# Run coverage monitoring
python3 src/monitoring/coverage_monitor.py >> "$LOG_FILE" 2>&1

# Run alert check
python3 src/monitoring/alert_system.py >> "$LOG_FILE" 2>&1

echo "$(date): Daily coverage monitoring completed" >> "$LOG_FILE"
EOF

# 2. Hourly Alert Check Script
cat > "$SCRIPTS_DIR/run_hourly_alert_check.sh" << 'EOF'
#!/bin/bash
# Hourly Alert Check Script
# Quick alert check for critical issues

set -e

# Load environment
cd /home/jianjun/ats-genai-pm
source .env.monitoring 2>/dev/null || true

# Set required environment variables
export PYTHONPATH="/home/jianjun/ats-genai-pm/src"
export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-4432}"
export DB_USER="${DB_USER:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-intg_password}"
export DB_NAME="${DB_NAME:-intg_db}"

# Logging
LOG_FILE="/home/jianjun/ats-genai-pm/logs/monitoring/hourly_alerts_$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

echo "$(date): Starting hourly alert check" >> "$LOG_FILE"

# Run alert check only
python3 src/monitoring/alert_system.py >> "$LOG_FILE" 2>&1

echo "$(date): Hourly alert check completed" >> "$LOG_FILE"
EOF

# 3. Dashboard Health Check Script
cat > "$SCRIPTS_DIR/check_dashboard_health.sh" << 'EOF'
#!/bin/bash
# Dashboard Health Check Script
# Ensures dashboard is running and responsive

set -e

# Load environment
cd /home/jianjun/ats-genai-pm
source .env.monitoring 2>/dev/null || true

DASHBOARD_URL="http://${DASHBOARD_HOST:-localhost}:${DASHBOARD_PORT:-8080}/health"
LOG_FILE="/home/jianjun/ats-genai-pm/logs/monitoring/dashboard_health_$(date +%Y%m%d).log"

echo "$(date): Checking dashboard health at $DASHBOARD_URL" >> "$LOG_FILE"

# Check if dashboard is responding
if curl -f -s -m 10 "$DASHBOARD_URL" > /dev/null 2>&1; then
    echo "$(date): ✅ Dashboard is healthy" >> "$LOG_FILE"
else
    echo "$(date): ❌ Dashboard health check failed" >> "$LOG_FILE"
    
    # Try to restart dashboard if it's not running
    if ! pgrep -f "coverage_dashboard_fixed.py" > /dev/null; then
        echo "$(date): 🔄 Starting dashboard..." >> "$LOG_FILE"
        nohup python3 coverage_dashboard_fixed.py >> "$LOG_FILE" 2>&1 &
        sleep 5
        
        # Verify restart
        if curl -f -s -m 10 "$DASHBOARD_URL" > /dev/null 2>&1; then
            echo "$(date): ✅ Dashboard restarted successfully" >> "$LOG_FILE"
        else
            echo "$(date): ❌ Dashboard restart failed" >> "$LOG_FILE"
        fi
    fi
fi
EOF

# Make scripts executable
chmod +x "$SCRIPTS_DIR/run_daily_coverage_monitoring.sh"
chmod +x "$SCRIPTS_DIR/run_hourly_alert_check.sh"
chmod +x "$SCRIPTS_DIR/check_dashboard_health.sh"

echo -e "${GREEN}✅ Created monitoring scripts${NC}"

# Setup cron jobs
echo -e "${YELLOW}⏰ Setting up cron jobs...${NC}"

# Create temporary cron file
TEMP_CRON="/tmp/ats_monitoring_cron"

# Get existing crontab (if any) and filter out our jobs
(crontab -l 2>/dev/null | grep -v "ATS Data Coverage Monitoring" || true) > "$TEMP_CRON"

# Add our cron jobs
cat >> "$TEMP_CRON" << EOF

# ATS Data Coverage Monitoring
# Daily comprehensive monitoring at 6:00 AM
0 6 * * * $SCRIPTS_DIR/run_daily_coverage_monitoring.sh

# Hourly alert checks (every hour during business hours 8 AM - 6 PM)
0 8-18 * * * $SCRIPTS_DIR/run_hourly_alert_check.sh

# Dashboard health check every 15 minutes
*/15 * * * * $SCRIPTS_DIR/check_dashboard_health.sh

# Export Prometheus metrics every 5 minutes
*/5 * * * * PYTHONPATH=$PROJECT_ROOT/src python3 $PROJECT_ROOT/src/monitoring/prometheus_exporter.py

# Weekly log cleanup (Sunday at 2 AM)
0 2 * * 0 find $LOGS_DIR -name "*.log" -mtime +7 -delete
EOF

# Install the new crontab
crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo -e "${GREEN}✅ Cron jobs installed${NC}"

# Display installed cron jobs
echo -e "${BLUE}📋 Installed cron jobs:${NC}"
crontab -l | grep -A 10 "ATS Data Coverage Monitoring"

echo ""
echo -e "${YELLOW}⚙️ Configuration Steps:${NC}"
echo "1. Set your Slack webhook URL in: $ENV_FILE"
echo "   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
echo ""
echo "2. Optionally configure Grafana integration:"
echo "   GRAFANA_API_URL=http://your-grafana-server:3000"
echo "   GRAFANA_API_KEY=your-grafana-api-key"
echo ""
echo "3. Test the monitoring manually:"
echo "   $SCRIPTS_DIR/run_hourly_alert_check.sh"
echo ""
echo "4. View logs:"
echo "   tail -f $LOGS_DIR/daily_monitoring_$(date +%Y%m%d).log"

echo ""
echo -e "${GREEN}✅ ATS Data Coverage Monitoring cron setup complete!${NC}"
echo -e "${BLUE}📊 Dashboard running at: http://localhost:8080${NC}"
echo -e "${BLUE}📂 Logs directory: $LOGS_DIR${NC}"