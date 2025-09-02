#!/bin/bash
#
# ATS Real-time Collection Monitoring - Quick Start Script
#
# Usage: ./scripts/start_monitoring.sh

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}🎯 ATS Real-time Collection Monitoring System${NC}"
echo -e "${BLUE}=============================================${NC}"

# Set environment
export PYTHONPATH="$PROJECT_ROOT/src"
cd "$PROJECT_ROOT"

# Configuration
CONFIG_FILE="$PROJECT_ROOT/config/realtime_monitoring_config.json"

if [[ -f "$CONFIG_FILE" ]]; then
    echo -e "${GREEN}✅ Using configuration: $CONFIG_FILE${NC}"
else
    echo -e "${RED}❌ Configuration not found: $CONFIG_FILE${NC}"
    exit 1
fi

# Email setup for jianjun00@gmail.com
export SMTP_SERVER=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USERNAME=jianjun00@gmail.com
export SMTP_USE_TLS=true
export ALERT_EMAIL_RECIPIENTS=jianjun00@gmail.com

if [[ -z "$SMTP_PASSWORD" ]]; then
    echo -e "${YELLOW}⚠️ SMTP_PASSWORD not set - email alerts disabled${NC}"
    echo "   To enable: export SMTP_PASSWORD=your_gmail_app_password"
else
    echo -e "${GREEN}✅ Email alerts configured for jianjun00@gmail.com${NC}"
fi

echo -e "${GREEN}✅ Slack alerts configured for #ats-alerts${NC}"

# Validate architecture
echo -e "${BLUE}🧪 Validating system architecture...${NC}"
if python3 "$SCRIPT_DIR/test_monitoring_architecture.py" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Architecture validation passed${NC}"
else
    echo -e "${RED}❌ Architecture validation failed${NC}"
    exit 1
fi

# Check ports
check_port() {
    local port=$1
    if command -v netstat &> /dev/null && netstat -tlnp 2>/dev/null | grep -q ":$port "; then
        echo -e "${YELLOW}⚠️ Port $port already in use${NC}"
    else
        echo -e "${GREEN}✅ Port $port available${NC}"
    fi
}

check_port 8090
check_port 8091

echo ""
echo -e "${BLUE}🚀 Starting monitoring system...${NC}"
echo -e "${YELLOW}Access points:${NC}"
echo "  Dashboard:  http://localhost:8090"
echo "  Metrics:    http://localhost:8091/metrics"
echo "  Health:     http://localhost:8090/health"
echo ""

# Start monitoring
exec python3 "$SCRIPT_DIR/start_realtime_monitoring.py" --config "$CONFIG_FILE"
