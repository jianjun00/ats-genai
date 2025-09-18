#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Docker Image Update Script ===${NC}"

# Step 1: Build updated Docker image with 'dev' environment support
echo -e "\n${YELLOW}Step 1: Building updated Docker image...${NC}"
docker build -t dragonflyer762/ats-genai:dev-latest-updated -f Dockerfile.update .

# Step 1a: Copy updated environment.py into the image
echo -e "\n${YELLOW}Step 1a: Copying updated environment.py into the image...${NC}"
CONTAINER_ID=$(docker create dragonflyer762/ats-genai:dev-latest-updated)
docker cp src/config/environment.py $CONTAINER_ID:/app/src/config/environment.py
docker start $CONTAINER_ID
docker exec $CONTAINER_ID bash -c "cat /app/src/config/environment.py | grep -A 5 'class EnvironmentType'"
docker stop $CONTAINER_ID

# Step 1b: Commit the changes to the image
echo -e "\n${YELLOW}Step 1b: Committing changes to the image...${NC}"
CONTAINER_ID=$(docker create dragonflyer762/ats-genai:dev-latest-updated)
docker start $CONTAINER_ID
docker exec $CONTAINER_ID bash -c "cat /app/src/secmaster/populate_instrument_polygon.py | grep -A 3 'choices='"
docker exec $CONTAINER_ID bash -c "cat /app/src/config/environment.py | grep -A 5 'class EnvironmentType'"
docker commit -m "Added 'dev' environment support to populate_instrument_polygon.py and environment.py" $CONTAINER_ID dragonflyer762/ats-genai:dev-latest-updated
docker stop $CONTAINER_ID
docker rm $CONTAINER_ID
echo -e "${GREEN}✅ Changes committed to the image${NC}"

# Step 2: Test the updated image
echo -e "\n${YELLOW}Step 2: Testing updated image with 'dev' environment...${NC}"

# Test that populate_instrument_polygon.py accepts 'dev' as an environment
docker run --rm dragonflyer762/ats-genai:dev-latest-updated \
  python -m src.secmaster.populate_instrument_polygon --environment dev --gin_config /app/config/app_docker.gin --help

# Test that environment.py includes 'dev' in EnvironmentType
echo -e "\n${YELLOW}Verifying EnvironmentType enum includes 'dev'...${NC}"
docker run --rm dragonflyer762/ats-genai:dev-latest-updated \
  python -c "from config.environment import EnvironmentType; print('DEV' in EnvironmentType.__members__)"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Updated image successfully supports 'dev' environment!${NC}"
else
    echo -e "${RED}❌ Updated image test failed!${NC}"
    exit 1
fi

# Step 3: Tag and push the image
echo -e "\n${YELLOW}Step 3: Tag and push the updated image...${NC}"
echo -e "${YELLOW}Tagging image as dragonflyer762/ats-genai:dev-latest${NC}"
docker tag dragonflyer762/ats-genai:dev-latest-updated dragonflyer762/ats-genai:dev-latest

echo -e "${YELLOW}Pushing image to Docker Hub...${NC}"
docker push dragonflyer762/ats-genai:dev-latest

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Image successfully pushed to Docker Hub${NC}"
else
    echo -e "${RED}❌ Failed to push image to Docker Hub. Check your Docker credentials.${NC}"
    echo -e "${YELLOW}You may need to run 'docker login' first.${NC}"
    exit 1
fi

echo -e "\n${BLUE}=== Docker Image Update Complete ===${NC}"
echo -e "${GREEN}The updated Docker image now supports the 'dev' environment.${NC}"
echo -e "${YELLOW}After pushing the image, redeploy your Kubernetes jobs to use it.${NC}"

exit 0
