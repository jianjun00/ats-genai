#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Docker Image Environment Update Script ===${NC}"

# Step 1: Create a container from the existing image
echo -e "\n${YELLOW}Step 1: Creating container from existing image...${NC}"
CONTAINER_ID=$(docker create dragonflyer762/ats-genai:dev-latest)

# Step 2: Copy the updated environment.py file into the container
echo -e "\n${YELLOW}Step 2: Copying updated environment.py into the container...${NC}"
docker cp /home/jianjun/ats-genai/src/config/environment.py $CONTAINER_ID:/app/src/config/environment.py

# Step 3: Verify the updated file
echo -e "\n${YELLOW}Step 3: Verifying updated environment.py in container...${NC}"
docker start $CONTAINER_ID
docker exec $CONTAINER_ID bash -c "cat /app/src/config/environment.py | grep -A 5 'class EnvironmentType'"

# Step 4: Commit the changes to create a new image
echo -e "\n${YELLOW}Step 4: Committing changes to create updated image...${NC}"
docker commit -m "Updated environment.py to support 'dev' environment" $CONTAINER_ID dragonflyer762/ats-genai:dev-latest
docker stop $CONTAINER_ID
docker rm $CONTAINER_ID

# Step 5: Test the updated image
echo -e "\n${YELLOW}Step 5: Testing updated image with 'dev' environment...${NC}"
docker run --rm dragonflyer762/ats-genai:dev-latest \
  python -c "from config.environment import EnvironmentType; print(f'DEV in EnvironmentType: {\"DEV\" in EnvironmentType.__members__}')"

# Step 6: Push the updated image to Docker Hub
echo -e "\n${YELLOW}Step 6: Pushing updated image to Docker Hub...${NC}"
docker push dragonflyer762/ats-genai:dev-latest

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Image successfully pushed to Docker Hub${NC}"
else
    echo -e "${RED}❌ Failed to push image to Docker Hub. Check your Docker credentials.${NC}"
    exit 1
fi

echo -e "\n${BLUE}=== Docker Image Update Complete ===${NC}"
echo -e "${GREEN}The updated Docker image now supports the 'dev' environment.${NC}"
echo -e "${YELLOW}Next step: Recreate the Kubernetes jobs to use the updated image.${NC}"

exit 0
