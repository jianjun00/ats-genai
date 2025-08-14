#!/bin/bash
# Script to prepare base64-encoded secrets for Kubernetes

# Set your actual values here
DB_USER="postgres"
DB_PASSWORD="your_actual_password"
POLYGON_API_KEY="your_polygon_api_key"

# Encode values
DB_USER_ENCODED=$(echo -n "$DB_USER" | base64)
DB_PASSWORD_ENCODED=$(echo -n "$DB_PASSWORD" | base64)
POLYGON_API_KEY_ENCODED=$(echo -n "$POLYGON_API_KEY" | base64)

# Output encoded values
echo "Encoded values for Kubernetes secrets:"
echo "------------------------------------"
echo "DB_USER: $DB_USER_ENCODED"
echo "DB_PASSWORD: $DB_PASSWORD_ENCODED"
echo "POLYGON_API_KEY: $POLYGON_API_KEY_ENCODED"
echo ""
echo "Update your Kubernetes Secret manifest with these values."
