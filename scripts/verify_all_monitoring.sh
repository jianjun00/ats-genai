#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${CYAN}🔍 ATS Platform - Complete Monitoring Status${NC}"
echo "=================================================================="
echo ""

# Function to check monitoring status
check_monitoring() {
    local env=$1
    local namespace=$2
    local channel=$3
    local interval=$4
    
    echo -e "${YELLOW}📊 $env Environment Monitoring${NC}"
    
    # Check if monitoring pod exists and is running
    pod_status=$(kubectl get pods -n $namespace -l app=postgres-slack-alerts --no-headers 2>/dev/null)
    
    if [ -n "$pod_status" ]; then
        if echo "$pod_status" | grep -q "Running"; then
            echo -e "  ${GREEN}✅ PostgreSQL Slack Alerts: ACTIVE${NC}"
            pod_name=$(echo "$pod_status" | awk '{print $1}')
            
            # Get recent log to show it's working
            recent_log=$(kubectl logs -n $namespace $pod_name --tail=1 2>/dev/null)
            if [ -n "$recent_log" ]; then
                echo -e "  ${BLUE}📋 Latest Status:${NC} $(echo "$recent_log" | sed 's/.*INFO - //')"
            fi
            
            # Check database connection
            table_count=$(kubectl exec -n $namespace deployment/postgres -- psql -U postgres -d $(echo $namespace | sed 's/ats-//')_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '$(echo $namespace | sed 's/ats-//')_%';" 2>/dev/null | tr -d ' ')
            if [ -n "$table_count" ] && [ "$table_count" -gt 0 ]; then
                echo -e "  ${GREEN}🗄️  Database: Connected (${table_count} tables)${NC}"
            else
                echo -e "  ${RED}🗄️  Database: Connection issues${NC}"
            fi
            
        else
            echo -e "  ${RED}❌ PostgreSQL Slack Alerts: NOT RUNNING${NC}"
            echo -e "  ${BLUE}   Status: $(echo "$pod_status" | awk '{print $3}')${NC}"
        fi
        
        echo -e "  ${PURPLE}📢 Alert Channel: $channel${NC}"
        echo -e "  ${PURPLE}⏱️  Check Interval: $interval${NC}"
        
    else
        echo -e "  ${RED}❌ No monitoring deployment found${NC}"
    fi
    
    echo ""
}

# Check all environments
check_monitoring "Development" "ats-dev" "#ats-dev-alerts" "5 minutes"
check_monitoring "Integration" "ats-intg" "#ats-intg-alerts" "5 minutes" 
check_monitoring "Production" "ats-prod" "#ats-prod-alerts" "3 minutes"

# Overall summary
echo -e "${CYAN}📋 Monitoring Summary:${NC}"

# Count running monitoring services
dev_running=$(kubectl get pods -n ats-dev -l app=postgres-slack-alerts --no-headers 2>/dev/null | grep -c "Running" || echo "0")
intg_running=$(kubectl get pods -n ats-intg -l app=postgres-slack-alerts --no-headers 2>/dev/null | grep -c "Running" || echo "0")
prod_running=$(kubectl get pods -n ats-prod -l app=postgres-slack-alerts --no-headers 2>/dev/null | grep -c "Running" || echo "0")

total_running=$((dev_running + intg_running + prod_running))

echo -e "  🚀 Active Monitors: $total_running/3 environments"
echo -e "  📢 Alert Channels: #ats-dev-alerts, #ats-intg-alerts, #ats-prod-alerts"
echo -e "  🔗 Webhook: https://hooks.slack.com/services/T09ANHQAF0D/B09AX7TTTHT/..."

echo ""
echo -e "${GREEN}🔧 Monitoring Features:${NC}"
echo "  • Real-time PostgreSQL health monitoring"
echo "  • Environment-specific alert channels"
echo "  • Long-running query detection"
echo "  • Connection usage monitoring" 
echo "  • Database connectivity checks"
echo "  • Production-grade enhanced monitoring"

echo ""
echo -e "${BLUE}📚 Commands to check logs:${NC}"
echo "  kubectl logs -n ats-dev deployment/postgres-slack-alerts"
echo "  kubectl logs -n ats-intg deployment/postgres-slack-alerts" 
echo "  kubectl logs -n ats-prod deployment/postgres-slack-alerts"

if [ "$total_running" -eq 3 ]; then
    echo ""
    echo -e "${GREEN}🎉 All monitoring services are operational!${NC}"
else
    echo ""
    echo -e "${YELLOW}⚠️  Some monitoring services may need attention.${NC}"
fi

echo ""