#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Docker Image Content Verification ===${NC}"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    exit 1
fi

IMAGE_NAME="dragonflyer762/ats-genai:dev-latest"
UPDATED_IMAGE_NAME="dragonflyer762/ats-genai:dev-latest-updated"

echo -e "${YELLOW}Step 1: Checking original image content...${NC}"
echo -e "Running: docker run --rm $IMAGE_NAME cat /app/src/secmaster/populate_instrument_polygon.py | grep -A 3 'choices='"
docker run --rm $IMAGE_NAME cat /app/src/secmaster/populate_instrument_polygon.py | grep -A 3 'choices='

echo -e "\n${YELLOW}Step 2: Checking updated image content...${NC}"
echo -e "Running: docker run --rm $UPDATED_IMAGE_NAME cat /app/src/secmaster/populate_instrument_polygon.py | grep -A 3 'choices='"
docker run --rm $UPDATED_IMAGE_NAME cat /app/src/secmaster/populate_instrument_polygon.py | grep -A 3 'choices='

echo -e "\n${YELLOW}Step 3: Checking help output from both images...${NC}"
echo -e "${BLUE}Original image help:${NC}"
docker run --rm $IMAGE_NAME python -m src.secmaster.populate_instrument_polygon --help

echo -e "\n${BLUE}Updated image help:${NC}"
docker run --rm $UPDATED_IMAGE_NAME python -m src.secmaster.populate_instrument_polygon --help

echo -e "\n${YELLOW}Step 4: Checking if the updated image was properly tagged...${NC}"
ORIGINAL_ID=$(docker inspect --format='{{.Id}}' $IMAGE_NAME)
UPDATED_ID=$(docker inspect --format='{{.Id}}' $UPDATED_IMAGE_NAME)

echo -e "Original image ID: $ORIGINAL_ID"
echo -e "Updated image ID: $UPDATED_ID"

if [ "$ORIGINAL_ID" == "$UPDATED_ID" ]; then
    echo -e "${RED}ERROR: The updated image was not properly tagged to replace the original image.${NC}"
    echo -e "The tag command may have failed: docker tag $UPDATED_IMAGE_NAME $IMAGE_NAME"
else
    echo -e "${GREEN}The images have different IDs, which is expected.${NC}"
fi

echo -e "\n${BLUE}=== Verification Complete ===${NC}"
echo -e "${YELLOW}If the updated image shows 'dev' in choices but the original doesn't,${NC}"
echo -e "${YELLOW}you need to properly tag and push the updated image:${NC}"
echo -e "docker tag $UPDATED_IMAGE_NAME $IMAGE_NAME"
echo -e "docker push $IMAGE_NAME"

exit 0
