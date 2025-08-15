#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kubernetes Job Verification Script ===${NC}"

# Step 1: Verify YAML files exist
echo -e "\n${YELLOW}Step 1: Verifying YAML files...${NC}"
for file in "/home/jianjun/ats-genai/k8s/dev/instrument-polygon-job.yaml" "/home/jianjun/ats-genai/k8s/dev/instrument-polygon-backfill-job.yaml"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓ File exists: $file${NC}"
    else
        echo -e "${RED}✗ File missing: $file${NC}"
        exit 1
    fi
done

# Step 2: Verify environment parameter in YAML files
echo -e "\n${YELLOW}Step 2: Verifying environment parameter in YAML files...${NC}"
for file in "/home/jianjun/ats-genai/k8s/dev/instrument-polygon-job.yaml" "/home/jianjun/ats-genai/k8s/dev/instrument-polygon-backfill-job.yaml"; do
    ENV_PARAM=$(grep -A1 "\-\-environment" "$file" | grep -o '"dev"' | tr -d '"')
    if [ "$ENV_PARAM" == "dev" ]; then
        echo -e "${GREEN}✓ Correct environment parameter in $file: $ENV_PARAM${NC}"
    else
        echo -e "${RED}✗ Incorrect environment parameter in $file: $ENV_PARAM (should be 'dev')${NC}"
        exit 1
    fi
done

# Step 3: Verify appropriate Gin config file exists
echo -e "\n${YELLOW}Step 3: Verifying Gin config file...${NC}"
if [ -f "/home/jianjun/ats-genai/config/app_docker.gin" ]; then
    echo -e "${GREEN}✓ Gin config file exists: /home/jianjun/ats-genai/config/app_docker.gin${NC}"
    echo -e "${YELLOW}Note: The 'dev' environment likely uses app_docker.gin for container execution${NC}"
else
    echo -e "${RED}✗ Gin config file missing: /home/jianjun/ats-genai/config/app_docker.gin${NC}"
    exit 1
fi

# Step 4: Verify no UniverseStateIntervalBuilder in docker config
echo -e "\n${YELLOW}Step 4: Verifying no UniverseStateIntervalBuilder in docker config...${NC}"
if grep -q "UniverseStateIntervalBuilder" "/home/jianjun/ats-genai/config/app_docker.gin"; then
    echo -e "${RED}✗ Found UniverseStateIntervalBuilder in docker config${NC}"
    exit 1
else
    echo -e "${GREEN}✓ No UniverseStateIntervalBuilder in docker config${NC}"
fi

# Step 5: Dry run kubectl apply
echo -e "\n${YELLOW}Step 5: Performing dry run of kubectl apply...${NC}"
kubectl apply -f /home/jianjun/ats-genai/k8s/dev/instrument-polygon-job.yaml --dry-run=client
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dry run successful for instrument-polygon-job.yaml${NC}"
else
    echo -e "${RED}✗ Dry run failed for instrument-polygon-job.yaml${NC}"
    exit 1
fi

kubectl apply -f /home/jianjun/ats-genai/k8s/dev/instrument-polygon-backfill-job.yaml --dry-run=client
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dry run successful for instrument-polygon-backfill-job.yaml${NC}"
else
    echo -e "${RED}✗ Dry run failed for instrument-polygon-backfill-job.yaml${NC}"
    exit 1
fi

# Final verification
echo -e "\n${BLUE}=== Verification Complete ===${NC}"
echo -e "${GREEN}All checks passed! The Kubernetes jobs are ready to be deployed.${NC}"
echo -e "${YELLOW}Run the following commands to deploy:${NC}"
echo -e "kubectl apply -f /home/jianjun/ats-genai/k8s/dev/instrument-polygon-job.yaml"
echo -e "kubectl apply -f /home/jianjun/ats-genai/k8s/dev/instrument-polygon-backfill-job.yaml"

exit 0
