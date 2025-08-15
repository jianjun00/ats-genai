#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kubernetes Job Status Check ===${NC}"

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed or not in PATH${NC}"
    echo -e "${YELLOW}This script requires kubectl to check Kubernetes job status.${NC}"
    exit 1
fi

# Check if we can access the cluster
echo -e "${YELLOW}Checking cluster access...${NC}"
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to Kubernetes cluster${NC}"
    echo -e "${YELLOW}Please check your kubeconfig and cluster connection.${NC}"
    exit 1
fi

# Check job status
echo -e "\n${YELLOW}Checking job status in ats-dev namespace...${NC}"
kubectl get jobs -n ats-dev | grep instrument-polygon || echo -e "${YELLOW}No instrument-polygon jobs found${NC}"

# Check pod status
echo -e "\n${YELLOW}Checking pod status in ats-dev namespace...${NC}"
kubectl get pods -n ats-dev | grep instrument-polygon || echo -e "${YELLOW}No instrument-polygon pods found${NC}"

# Get logs from the most recent pod
echo -e "\n${YELLOW}Attempting to get logs from the most recent instrument-polygon pod...${NC}"
LATEST_POD=$(kubectl get pods -n ats-dev -l job-name=instrument-polygon-aapl --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)

if [ -n "$LATEST_POD" ]; then
    echo -e "${GREEN}Found pod: $LATEST_POD${NC}"
    echo -e "${YELLOW}Last 50 lines of logs:${NC}"
    kubectl logs -n ats-dev $LATEST_POD --tail=50
else
    echo -e "${YELLOW}No instrument-polygon-aapl pods found${NC}"
    
    # Try backfill job
    BACKFILL_POD=$(kubectl get pods -n ats-dev -l job-name=instrument-polygon-backfill --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
    
    if [ -n "$BACKFILL_POD" ]; then
        echo -e "${GREEN}Found backfill pod: $BACKFILL_POD${NC}"
        echo -e "${YELLOW}Last 50 lines of logs:${NC}"
        kubectl logs -n ats-dev $BACKFILL_POD --tail=50
    else
        echo -e "${YELLOW}No instrument-polygon-backfill pods found either${NC}"
    fi
fi

echo -e "\n${BLUE}=== Status Check Complete ===${NC}"
echo -e "${YELLOW}If you're having issues with kubectl access, ensure:${NC}"
echo -e "1. You're connected to the right cluster"
echo -e "2. You have permissions to access the ats-dev namespace"
echo -e "3. The jobs have been deployed with: kubectl apply -f k8s/dev/instrument-polygon-job.yaml"

exit 0
