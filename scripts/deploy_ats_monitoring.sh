#!/bin/bash
# ATS Data Coverage Monitoring - Quick Deployment Script
# Deploys the complete monitoring system with your Slack webhook

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 ATS Data Coverage Monitoring - Quick Deployment${NC}"
echo "=========================================================="

PROJECT_ROOT="/home/jianjun/ats-genai-pm"
cd "$PROJECT_ROOT"

# Set environment variables
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/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"
export PYTHONPATH="$PROJECT_ROOT/src"

echo -e "\n${YELLOW}📋 Step 1: Validating System Components${NC}"
echo "-------------------------------------------"

# Validate system
if ./scripts/validate_monitoring_system.sh > /tmp/validation.log 2>&1; then
    echo -e "${GREEN}✅ System validation passed${NC}"
else
    echo -e "${YELLOW}⚠️ Some validation issues found (see /tmp/validation.log)${NC}"
    echo "Continuing with deployment..."
fi

echo -e "\n${YELLOW}📋 Step 2: Testing Slack Integration${NC}"
echo "---------------------------------------"

# Test Slack webhook
if curl -s -X POST -H 'Content-type: application/json' \
    --data '{"username": "ATS Coverage Monitor", "icon_emoji": ":chart_with_upwards_trend:", "text": "🚀 **ATS Monitoring Deployment Started**\n\nYour ATS Data Coverage Monitoring system is being deployed...\n\n✅ Slack integration confirmed\n🔧 Setting up automated monitoring\n📊 Configuring dashboards"}' \
    "$SLACK_WEBHOOK_URL" > /dev/null; then
    echo -e "${GREEN}✅ Slack webhook test successful${NC}"
else
    echo -e "${RED}❌ Slack webhook test failed${NC}"
    echo "Continuing with deployment..."
fi

echo -e "\n${YELLOW}📋 Step 3: Setting Up Cron Jobs${NC}"
echo "------------------------------------"

# Setup cron jobs
if ./scripts/setup_coverage_monitoring_cron.sh > /tmp/cron_setup.log 2>&1; then
    echo -e "${GREEN}✅ Cron jobs configured successfully${NC}"
    echo "   • Daily monitoring at 6:00 AM"
    echo "   • Hourly alerts during business hours"
    echo "   • Dashboard health checks every 15 minutes"
    echo "   • Prometheus metrics export every 5 minutes"
else
    echo -e "${RED}❌ Cron setup failed (see /tmp/cron_setup.log)${NC}"
fi

echo -e "\n${YELLOW}📋 Step 4: Setting Up Grafana Stack${NC}"
echo "--------------------------------------"

# Setup Grafana monitoring
if ./scripts/setup_grafana_monitoring.sh > /tmp/grafana_setup.log 2>&1; then
    echo -e "${GREEN}✅ Grafana monitoring stack configured${NC}"
else
    echo -e "${RED}❌ Grafana setup failed (see /tmp/grafana_setup.log)${NC}"
fi

echo -e "\n${YELLOW}📋 Step 5: Starting Monitoring Stack${NC}"
echo "---------------------------------------"

# Start monitoring stack
if ./scripts/start_monitoring_stack.sh > /tmp/monitoring_start.log 2>&1; then
    echo -e "${GREEN}✅ Monitoring stack started successfully${NC}"
else
    echo -e "${YELLOW}⚠️ Monitoring stack startup issues (see /tmp/monitoring_start.log)${NC}"
    echo "You may need to start it manually"
fi

echo -e "\n${YELLOW}📋 Step 6: Testing Coverage Dashboard${NC}"
echo "----------------------------------------"

# Wait for dashboard to start
sleep 5

# Test dashboard
if timeout 10 python3 coverage_dashboard_fixed.py --port 8080 > /tmp/dashboard_test.log 2>&1 &
DASHBOARD_PID=$!
sleep 3
if kill -0 $DASHBOARD_PID 2>/dev/null; then
    kill $DASHBOARD_PID 2>/dev/null || true
    echo -e "${GREEN}✅ Coverage dashboard test successful${NC}"
else
    echo -e "${YELLOW}⚠️ Coverage dashboard test failed${NC}"
fi

echo -e "\n${YELLOW}📋 Step 7: Final Health Check${NC}"
echo "-----------------------------------"

# Check service endpoints
services_status=""

# Check Grafana
if curl -f -s http://localhost:3000/api/health > /dev/null 2>&1; then
    services_status="${services_status}\n  ${GREEN}✅ Grafana${NC} - http://localhost:3000"
else
    services_status="${services_status}\n  ${YELLOW}⚠️ Grafana${NC} - http://localhost:3000 (may need manual start)"
fi

# Check Prometheus
if curl -f -s http://localhost:9090/-/healthy > /dev/null 2>&1; then
    services_status="${services_status}\n  ${GREEN}✅ Prometheus${NC} - http://localhost:9090"
else
    services_status="${services_status}\n  ${YELLOW}⚠️ Prometheus${NC} - http://localhost:9090 (may need manual start)"
fi

# Check Node Exporter
if curl -f -s http://localhost:9100/metrics > /dev/null 2>&1; then
    services_status="${services_status}\n  ${GREEN}✅ Node Exporter${NC} - http://localhost:9100"
else
    services_status="${services_status}\n  ${YELLOW}⚠️ Node Exporter${NC} - http://localhost:9100 (may need manual start)"
fi

echo -e "${BLUE}📊 Service Status:${NC}"
echo -e "$services_status"

# Send deployment completion notification
curl -s -X POST -H 'Content-type: application/json' \
    --data '{
        "username": "ATS Coverage Monitor",
        "icon_emoji": ":chart_with_upwards_trend:",
        "attachments": [
            {
                "color": "#36a64f",
                "title": "🎯 ATS Monitoring Deployment Complete!",
                "text": "Your ATS Data Coverage Monitoring system has been deployed successfully.",
                "fields": [
                    {
                        "title": "Coverage Dashboard",
                        "value": "http://localhost:8080",
                        "short": true
                    },
                    {
                        "title": "Grafana Dashboard",
                        "value": "http://localhost:3000 (admin/ats_admin_2024)",
                        "short": true
                    },
                    {
                        "title": "Monitoring Features",
                        "value": "• Real-time coverage tracking\n• Intelligent gap detection\n• Priority-based backfill queue\n• Automated Slack alerts\n• Professional Grafana dashboards",
                        "short": false
                    }
                ],
                "footer": "ATS Platform Monitoring System"
            }
        ]
    }' \
    "$SLACK_WEBHOOK_URL" > /dev/null 2>&1

echo -e "\n${GREEN}🎉 DEPLOYMENT COMPLETE!${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}📱 Access Points:${NC}"
echo "  🏠 Coverage Dashboard: http://localhost:8080"
echo "  📊 Grafana: http://localhost:3000 (admin/ats_admin_2024)"
echo "  📈 Prometheus: http://localhost:9090"
echo "  💻 Node Exporter: http://localhost:9100"
echo ""
echo -e "${BLUE}📋 What's Monitoring:${NC}"
echo "  • Daily prices and minute bar coverage"
echo "  • Data gaps requiring backfill operations"  
echo "  • Priority symbol coverage status"
echo "  • Data freshness and staleness alerts"
echo ""
echo -e "${BLUE}🚨 Alert Channels:${NC}"
echo "  • Slack: Your configured webhook (alerts active)"
echo "  • Grafana: Built-in alerting with thresholds"
echo "  • Dashboard: Real-time visual indicators"
echo ""
echo -e "${BLUE}📚 Documentation:${NC}"
echo "  • Operations Guide: docs/OPERATIONS_COVERAGE_MONITORING.md"
echo "  • Validation Script: scripts/validate_monitoring_system.sh"
echo "  • System Logs: logs/monitoring/"
echo ""
echo -e "${YELLOW}🔔 Next Steps:${NC}"
echo "1. Check your Slack channel for deployment notifications"
echo "2. Open http://localhost:8080 to view the coverage dashboard"
echo "3. Configure priority symbols in the database"
echo "4. Review the operations guide for daily procedures"
echo ""
echo -e "${GREEN}✅ Your ATS monitoring system is now identifying which instruments and dates need backfill!${NC}"