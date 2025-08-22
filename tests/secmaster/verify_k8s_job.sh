#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kubernetes Job Verification Script ===${NC}"

# Step 1: Verify Flyte workflow script exists
echo -e "\n${YELLOW}Step 1: Verifying Flyte workflow script...${NC}"
FLYTE_SCRIPT="/home/jianjun/ats-genai/scripts/flyte_instrument_polygon_workflow.py"
if [ -f "$FLYTE_SCRIPT" ]; then
    echo -e "${GREEN}✓ Flyte workflow script exists: $FLYTE_SCRIPT${NC}"
else
    echo -e "${RED}✗ Flyte workflow script not found: $FLYTE_SCRIPT${NC}"
    exit 1
fi

# Step 2: Verify Flyte workflow can generate job configurations
echo -e "\n${YELLOW}Step 2: Verifying Flyte workflow can generate job configurations...${NC}"

# Generate test job YAML to a temporary directory
TMP_DIR=$(mktemp -d)
echo -e "Generating test job YAML to $TMP_DIR..."
python $FLYTE_SCRIPT --job-type test --tickers "AAPL" --output-dir "$TMP_DIR"
if [ $? -eq 0 ] && [ -n "$(ls -A $TMP_DIR)" ]; then
    echo -e "${GREEN}✓ Successfully generated test job YAML${NC}"
else
    echo -e "${RED}✗ Failed to generate test job YAML${NC}"
    rm -rf "$TMP_DIR"
    exit 1
fi

# Check if environment parameter is set correctly
TEST_JOB_YAML=$(ls $TMP_DIR/*.yaml | head -n 1)
ENV_PARAM=$(grep -A1 "\-\-environment" "$TEST_JOB_YAML" | grep -o '"dev"' | tr -d '"')
if [ "$ENV_PARAM" == "dev" ]; then
    echo -e "${GREEN}✓ Correct environment parameter in generated YAML: $ENV_PARAM${NC}"
else
    echo -e "${RED}✗ Incorrect or missing environment parameter in generated YAML. Expected 'dev', got '$ENV_PARAM'${NC}"
    rm -rf "$TMP_DIR"
    exit 1
fi

# Clean up
rm -rf "$TMP_DIR"

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

# Step 5: Dry run kubectl apply using generated YAML
echo -e "\n${YELLOW}Step 5: Performing dry run of kubectl apply with generated YAML...${NC}"

# Generate test job YAML to a temporary directory
TMP_DIR=$(mktemp -d)

# Generate test job YAML
python $FLYTE_SCRIPT --job-type test --tickers "AAPL" --output-dir "$TMP_DIR"
TEST_JOB_YAML=$(ls $TMP_DIR/*.yaml | head -n 1)

# Dry run test job
echo -e "Dry running test job from $TEST_JOB_YAML..."
kubectl apply -f "$TEST_JOB_YAML" --dry-run=client
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dry run successful for test job${NC}"
else
    echo -e "${RED}✗ Dry run failed for test job${NC}"
    rm -rf "$TMP_DIR"
    exit 1
fi

# Generate backfill job YAML
python $FLYTE_SCRIPT --job-type backfill --output-dir "$TMP_DIR"
BACKFILL_JOB_YAML=$(ls $TMP_DIR/*.yaml | grep -v $(basename "$TEST_JOB_YAML") | head -n 1)

# Dry run backfill job
echo -e "Dry running backfill job from $BACKFILL_JOB_YAML..."
kubectl apply -f "$BACKFILL_JOB_YAML" --dry-run=client
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dry run successful for backfill job${NC}"
else
    echo -e "${RED}✗ Dry run failed for backfill job${NC}"
    rm -rf "$TMP_DIR"
    exit 1
fi

# Clean up
rm -rf "$TMP_DIR"

# Final verification
echo -e "\n${BLUE}=== Verification Complete ===${NC}"
echo -e "${GREEN}All checks passed! The Flyte workflow is ready to generate and deploy jobs.${NC}"
echo -e "${YELLOW}Run the following commands to deploy:${NC}"
echo -e "python /home/jianjun/ats-genai/scripts/flyte_instrument_polygon_workflow.py --job-type test --tickers \"AAPL,MSFT,GOOG\" --apply"
echo -e "python /home/jianjun/ats-genai/scripts/flyte_instrument_polygon_workflow.py --job-type backfill --apply"

exit 0
