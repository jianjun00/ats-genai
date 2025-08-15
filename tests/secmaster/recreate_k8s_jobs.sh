#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kubernetes Job Recreation Script ===${NC}"

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed or not in PATH${NC}"
    exit 1
fi

# Check if we can access the cluster
echo -e "${YELLOW}Checking cluster access...${NC}"
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to Kubernetes cluster${NC}"
    exit 1
fi

# Delete existing jobs
echo -e "\n${YELLOW}Step 1: Deleting existing jobs...${NC}"
kubectl delete job instrument-polygon-aapl -n ats-dev --ignore-not-found
kubectl delete job instrument-polygon-backfill -n ats-dev --ignore-not-found

# Wait for jobs to be deleted
echo -e "\n${YELLOW}Waiting for jobs to be deleted...${NC}"
sleep 5

# Apply new job configurations
echo -e "\n${YELLOW}Step 2: Creating new jobs...${NC}"
kubectl apply -f /home/jianjun/ats-genai/k8s/dev/instrument-polygon-job.yaml
kubectl apply -f /home/jianjun/ats-genai/k8s/dev/instrument-polygon-backfill-job.yaml

# Check job status
echo -e "\n${YELLOW}Step 3: Checking job status...${NC}"
kubectl get jobs -n ats-dev | grep instrument-polygon

# Wait for jobs to start
echo -e "\n${YELLOW}Step 4: Waiting for jobs to start...${NC}"
sleep 10

# Get the pod name for the AAPL job
echo -e "\n${YELLOW}Step 5: Checking database connection in job pod...${NC}"
POD_NAME=$(kubectl get pods -n ats-dev -l job-name=instrument-polygon-aapl --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}')

if [ -n "$POD_NAME" ]; then
    echo -e "${GREEN}Found pod: $POD_NAME${NC}"
    echo -e "${YELLOW}Checking logs for database connection...${NC}"
    kubectl logs -n ats-dev $POD_NAME | grep -E 'DATABASE|database|connection|timescaledb'
    
    # Check if the logs contain the database connection information
    if kubectl logs -n ats-dev $POD_NAME | grep -q "timescaledb.ats-dev.svc.cluster.local"; then
        echo -e "\n${GREEN}✓ Database connection configured correctly!${NC}"
    else
        echo -e "\n${RED}✗ Database connection may not be configured correctly. Check the logs.${NC}"
    fi
else
    echo -e "${RED}No pod found for job instrument-polygon-aapl${NC}"
fi

echo -e "\n${GREEN}Jobs have been recreated successfully!${NC}"
echo -e "${YELLOW}To check job status and logs, run:${NC}"
echo -e "./check_k8s_jobs.sh"

exit 0
