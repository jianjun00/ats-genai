#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🏭 Verifying ATS Production Environment...${NC}"
echo ""

# Function to test endpoint
test_endpoint() {
    local url=$1
    local description=$2
    
    echo -e "${BLUE}📊 Testing: $description${NC}"
    response=$(curl -s "$url" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$response" ]; then
        echo -e "  ${GREEN}✅ Success${NC}"
        if [[ "$response" == *"status"* ]]; then
            echo "$response" | python3 -c "import sys, json; print('  Response:', json.load(sys.stdin).get('message', 'OK'))" 2>/dev/null || echo "  Response: API endpoint working"
        fi
    else
        echo -e "  ${RED}❌ Failed${NC}"
    fi
    echo ""
}

# Test NodePort access
PROD_PORT=30005
NODE_IP="192.168.49.2"

echo -e "${GREEN}🌐 Production Environment Status:${NC}"
kubectl get all -n ats-prod
echo ""

echo -e "${GREEN}🔍 Service Testing (NodePort: $PROD_PORT):${NC}"

# Test via port-forward if available
if netstat -tuln 2>/dev/null | grep -q ":$PROD_PORT "; then
    BASE_URL="http://localhost:$PROD_PORT"
    echo -e "${BLUE}Using port-forward: $BASE_URL${NC}"
else
    BASE_URL="http://$NODE_IP:$PROD_PORT"
    echo -e "${BLUE}Using NodePort: $BASE_URL${NC}"
fi

test_endpoint "$BASE_URL/api/health" "Production Health Check"
test_endpoint "$BASE_URL/api/tables" "Production Tables List"
test_endpoint "$BASE_URL/api/analytics/summary" "Production Analytics Summary"
test_endpoint "$BASE_URL/api/tables/prod_users" "Production Users Data"

# Check pod status
echo -e "${GREEN}🚀 Pod Status:${NC}"
kubectl get pods -n ats-prod -o custom-columns="NAME:.metadata.name,STATUS:.status.phase,READY:.status.conditions[?(@.type=='Ready')].status,RESTARTS:.status.containerStatuses[0].restartCount"
echo ""

# Check service endpoints
echo -e "${GREEN}🔗 Service Endpoints:${NC}"
kubectl get endpoints -n ats-prod
echo ""

# Database connectivity test
echo -e "${GREEN}🗄️  Database Test:${NC}"
kubectl exec -n ats-prod deployment/postgres -- psql -U postgres -d prod_db -c "SELECT 'Production DB Connected' as status, COUNT(*) as tables FROM information_schema.tables WHERE table_name LIKE 'prod_%';" 2>/dev/null
echo ""

echo -e "${GREEN}✅ Production Environment Verification Complete!${NC}"