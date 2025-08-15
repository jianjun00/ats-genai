#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Testing Kubernetes environment for instrument-polygon jobs${NC}"

# Set up environment variables as they would be in Kubernetes
export PYTHONPATH=/app/src
export LOG_LEVEL=INFO

# Create a test directory to simulate the container environment
TEST_DIR=$(mktemp -d)
echo -e "${YELLOW}Created test directory: ${TEST_DIR}${NC}"

# Copy necessary files to test directory
mkdir -p ${TEST_DIR}/src/secmaster
mkdir -p ${TEST_DIR}/config
cp -r /home/jianjun/ats-genai/src/secmaster/populate_instrument_polygon.py ${TEST_DIR}/src/secmaster/
cp -r /home/jianjun/ats-genai/src/config ${TEST_DIR}/src/
cp -r /home/jianjun/ats-genai/config/app_docker.gin ${TEST_DIR}/config/

# Install required dependencies in the test environment
echo -e "${YELLOW}Installing required dependencies...${NC}"
cd ${TEST_DIR}
uv pip install asyncpg ray requests

# Test the exact command that will run in Kubernetes
echo -e "${YELLOW}Testing with dev environment and AAPL ticker...${NC}"
PYTHONPATH=${TEST_DIR} python -m src.secmaster.populate_instrument_polygon --environment dev --ticker AAPL

# Capture the exit code
EXIT_CODE=$?

# Clean up
echo -e "${YELLOW}Cleaning up test directory...${NC}"
rm -rf ${TEST_DIR}

# Report results
if [ ${EXIT_CODE} -eq 0 ]; then
    echo -e "${GREEN}✅ Test passed! The command works as expected.${NC}"
    echo -e "${GREEN}✅ Safe to deploy to Kubernetes with --environment dev${NC}"
    
    echo -e "\n${YELLOW}Kubernetes Job Command:${NC}"
    echo "command: [\"python\", \"-m\", \"src.secmaster.populate_instrument_polygon\", \"--environment\", \"dev\", \"--ticker\", \"AAPL\"]"
    
    exit 0
else
    echo -e "${RED}❌ Test failed! The command would fail in Kubernetes.${NC}"
    echo -e "${RED}❌ Fix issues before deploying.${NC}"
    exit 1
fi
