#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}🏢 ATS Platform - Multi-Environment Overview${NC}"
echo "=============================================================="
echo ""

# Function to check environment
check_environment() {
    local env=$1
    local namespace=$2
    local port=$3
    local db_name=$4
    
    echo -e "${YELLOW}🔧 Environment: $env (Namespace: $namespace)${NC}"
    
    # Check if namespace exists
    if ! kubectl get namespace $namespace >/dev/null 2>&1; then
        echo -e "  ${RED}❌ Namespace does not exist${NC}"
        echo ""
        return
    fi
    
    # Check pods
    pod_count=$(kubectl get pods -n $namespace --no-headers 2>/dev/null | wc -l)
    running_pods=$(kubectl get pods -n $namespace --no-headers 2>/dev/null | grep "Running" | wc -l)
    
    echo -e "  📊 Pods: $running_pods/$pod_count running"
    
    # Check services
    service_count=$(kubectl get services -n $namespace --no-headers 2>/dev/null | wc -l)
    echo -e "  🌐 Services: $service_count deployed"
    
    # Check if analytics service is available
    if kubectl get service unified-analytics-service -n $namespace >/dev/null 2>&1; then
        echo -e "  ✅ Analytics service: Available (NodePort: $port)"
        
        # Test database if possible
        if kubectl get pod -n $namespace -l app=postgres >/dev/null 2>&1; then
            table_count=$(kubectl exec -n $namespace deployment/postgres -- psql -U postgres -d $db_name -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '${env}__%';" 2>/dev/null | tr -d ' ')
            if [ -n "$table_count" ] && [ "$table_count" -gt 0 ]; then
                echo -e "  🗄️  Database: $db_name ($table_count ${env}_ tables)"
            else
                echo -e "  🗄️  Database: $db_name (connection issue or no ${env}_ tables)"
            fi
        else
            echo -e "  🗄️  Database: No postgres deployment found"
        fi
    else
        echo -e "  ❌ Analytics service: Not available"
        
        # List available services
        services=$(kubectl get services -n $namespace --no-headers 2>/dev/null | awk '{print $1}' | tr '\n' ', ' | sed 's/,$//')
        if [ -n "$services" ]; then
            echo -e "  📝 Available services: $services"
        fi
    fi
    
    echo ""
}

# Check all environments
check_environment "dev" "ats-dev" "30001" "dev_db"
check_environment "intg" "ats-intg" "30004" "intg_db" 
check_environment "prod" "ats-prod" "30005" "prod_db"

# Overall cluster status
echo -e "${CYAN}🎯 Cluster Overview:${NC}"
echo -e "  📦 Total namespaces: $(kubectl get namespaces | grep ats- | wc -l)"
echo -e "  🚀 Total pods: $(kubectl get pods --all-namespaces | grep ats- | wc -l)"
echo -e "  🌐 Total services: $(kubectl get services --all-namespaces | grep ats- | wc -l)"

echo ""
echo -e "${GREEN}🔗 Access URLs (if port-forwarding is active):${NC}"
echo -e "  Development:  http://localhost:8081  (ats-dev)"
echo -e "  Integration:  http://localhost:30004 (ats-intg)"  
echo -e "  Production:   http://localhost:30005 (ats-prod)"

echo ""
echo -e "${BLUE}📋 Quick Commands:${NC}"
echo "  kubectl get all -n ats-dev"
echo "  kubectl get all -n ats-intg"  
echo "  kubectl get all -n ats-prod"
echo ""