#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Creating a test container to check database connectivity${NC}"

# Create a container from the image
echo -e "${GREEN}Creating container from image...${NC}"
CONTAINER_ID=$(docker create dragonflyer762/ats-genai:dev-latest)

# Copy the test script into the container
echo -e "${GREEN}Copying test script into container...${NC}"
docker cp /home/jianjun/ats-genai/tests/secmaster/test_db_connection.py $CONTAINER_ID:/app/test_db_connection.py

# Make the script executable
docker exec $CONTAINER_ID chmod +x /app/test_db_connection.py

# Run the test script
echo -e "${GREEN}Running test script...${NC}"
docker start -a $CONTAINER_ID

# Remove the container
echo -e "${GREEN}Removing container...${NC}"
docker rm $CONTAINER_ID
