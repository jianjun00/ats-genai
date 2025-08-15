#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Running diagnostic tests on Docker image...${NC}"

# Get the Docker image name
IMAGE="dragonflyer762/ats-genai:dev-latest"
echo -e "${YELLOW}Using image: ${IMAGE}${NC}"

# Run diagnostic commands in the container
echo -e "${YELLOW}1. Checking Python version:${NC}"
docker run --rm $IMAGE python --version

echo -e "\n${YELLOW}2. Checking PYTHONPATH:${NC}"
docker run --rm -e PYTHONPATH=/app/src $IMAGE bash -c 'echo $PYTHONPATH'

echo -e "\n${YELLOW}3. Listing /app directory:${NC}"
docker run --rm $IMAGE ls -la /app

echo -e "\n${YELLOW}4. Checking Gin config files:${NC}"
docker run --rm $IMAGE ls -la /app/config/

echo -e "\n${YELLOW}5. Checking if app_docker.gin exists:${NC}"
docker run --rm $IMAGE bash -c 'if [ -f "/app/config/app_docker.gin" ]; then echo "app_docker.gin exists"; cat /app/config/app_docker.gin | head -10; else echo "app_docker.gin does not exist"; fi'

echo -e "\n${YELLOW}6. Testing module imports:${NC}"
docker run --rm -e PYTHONPATH=/app/src $IMAGE python -c "import sys; print(sys.path); import src.secmaster.populate_instrument_polygon; print('Import successful')" || echo "Import failed"

echo -e "\n${YELLOW}7. Checking command help:${NC}"
docker run --rm -e PYTHONPATH=/app/src $IMAGE python -m src.secmaster.populate_instrument_polygon --help

echo -e "\n${YELLOW}8. Checking available environments:${NC}"
docker run --rm -e PYTHONPATH=/app/src $IMAGE python -c "from src.secmaster.populate_instrument_polygon import main; print('Available environments:', main.get_parser().parse_args(['--help']).environment)" || echo "Failed to check environments"

echo -e "\n${GREEN}Diagnostic tests completed.${NC}"
