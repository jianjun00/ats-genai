#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kubernetes Pod Logs Checker ===${NC}"

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed or not in PATH${NC}"
    exit 1
fi

# Get all pods with errors
echo -e "${YELLOW}Finding pods with errors in ats-dev namespace...${NC}"
ERROR_PODS=$(kubectl get pods -n ats-dev | grep Error | awk '{print $1}')

if [ -z "$ERROR_PODS" ]; then
    echo -e "${GREEN}No pods with errors found.${NC}"

    # Check for running pods instead
    RUNNING_PODS=$(kubectl get pods -n ats-dev | grep Running | awk '{print $1}')
    if [ -z "$RUNNING_PODS" ]; then
        echo -e "${YELLOW}No running pods found either. Checking all pods:${NC}"
        kubectl get pods -n ats-dev
    else
        echo -e "${GREEN}Found running pods:${NC}"
        kubectl get pods -n ats-dev | grep Running
    fi
else
    echo -e "${RED}Found pods with errors:${NC}"
    kubectl get pods -n ats-dev | grep Error

    # Loop through each error pod and get its logs
    echo -e "\n${YELLOW}Fetching logs from error pods:${NC}"
    for POD in $ERROR_PODS; do
        echo -e "\n${BLUE}=== Logs for pod: $POD ===${NC}"
        kubectl logs -n ats-dev $POD || echo -e "${RED}Failed to get logs for $POD${NC}"

        # Get pod description for more details
        echo -e "\n${BLUE}=== Description for pod: $POD ===${NC}"
        kubectl describe pod -n ats-dev $POD | grep -A 10 "Events:" || echo -e "${RED}Failed to get description for $POD${NC}"
    done
fi

# Check for pods in other states
echo -e "\n${YELLOW}Checking for pods in other states:${NC}"
PENDING_PODS=$(kubectl get pods -n ats-dev | grep Pending | awk '{print $1}')
if [ -n "$PENDING_PODS" ]; then
    echo -e "${YELLOW}Found pending pods:${NC}"
    kubectl get pods -n ats-dev | grep Pending

    # Get events for pending pods
    for POD in $PENDING_PODS; do
        echo -e "\n${BLUE}=== Events for pending pod: $POD ===${NC}"
        kubectl describe pod -n ats-dev $POD | grep -A 10 "Events:" || echo -e "${RED}Failed to get events for $POD${NC}"
    done
fi

CREATING_PODS=$(kubectl get pods -n ats-dev | grep ContainerCreating | awk '{print $1}')
if [ -n "$CREATING_PODS" ]; then
    echo -e "${YELLOW}Found pods in ContainerCreating state:${NC}"
    kubectl get pods -n ats-dev | grep ContainerCreating

    # Wait a bit and check if they're still creating
    echo -e "${YELLOW}Waiting 10 seconds to see if they complete...${NC}"
    sleep 10

    for POD in $CREATING_PODS; do
        STATUS=$(kubectl get pod -n ats-dev $POD -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
        if [ "$STATUS" == "Running" ]; then
            echo -e "${GREEN}Pod $POD is now running${NC}"
        elif [ "$STATUS" == "NotFound" ]; then
            echo -e "${RED}Pod $POD no longer exists${NC}"
        else
            echo -e "${YELLOW}Pod $POD is in state: $STATUS${NC}"
            kubectl describe pod -n ats-dev $POD | grep -A 10 "Events:" || echo -e "${RED}Failed to get events for $POD${NC}"
        fi
    done
fi

echo -e "\n${BLUE}=== Pod Log Check Complete ===${NC}"
echo -e "${YELLOW}If you're seeing image pull errors, check:${NC}"
echo -e "1. Image name and tag are correct"
echo -e "2. Registry credentials are properly configured"
echo -e "3. Network connectivity to the registry"
echo -e "\n${YELLOW}If you're seeing application errors:${NC}"
echo -e "1. Check that environment variables are set correctly"
echo -e "2. Verify the Gin config path is correct"
echo -e "3. Ensure the 'dev' environment is properly supported in the code"

exit 0
