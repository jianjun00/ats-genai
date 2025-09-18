#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Updating Docker image with simplified database connection${NC}"

# Pull the latest image
echo -e "${GREEN}Pulling latest image...${NC}"
docker pull dragonflyer762/ats-genai:dev-latest

# Create a container from the image
echo -e "${GREEN}Creating container from image...${NC}"
CONTAINER_ID=$(docker create dragonflyer762/ats-genai:dev-latest)

# Copy the updated database.py file into the container
echo -e "${GREEN}Copying updated database.py file into container...${NC}"
docker cp /home/jianjun/ats-genai/src/config/database.py $CONTAINER_ID:/app/src/config/database.py

# Commit the changes to a new image
echo -e "${GREEN}Committing changes to new image...${NC}"
docker commit $CONTAINER_ID dragonflyer762/ats-genai:dev-latest

# Remove the container
echo -e "${GREEN}Removing container...${NC}"
docker rm $CONTAINER_ID

# Verify the changes
echo -e "${GREEN}Verifying changes...${NC}"
docker run --rm dragonflyer762/ats-genai:dev-latest cat /app/src/config/database.py | grep "simplified Kubernetes"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Verification successful! The updated database.py file is in the image.${NC}"
else
    echo -e "${RED}Verification failed! The updated database.py file is not in the image.${NC}"
    exit 1
fi

# Push the updated image to Docker Hub
echo -e "${GREEN}Pushing updated image to Docker Hub...${NC}"
docker push dragonflyer762/ats-genai:dev-latest

echo -e "${GREEN}Done! The Docker image has been updated with the simplified database connection.${NC}"
