#!/bin/bash
# Script to set up database environment variables for CI/CD

# Default values
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"password"}
DB_NAME=${DB_NAME:-"test_db"}

# Print current settings
echo "Current database configuration:"
echo "DB_HOST=$DB_HOST"
echo "DB_PORT=$DB_PORT"
echo "DB_USER=$DB_USER"
echo "DB_PASSWORD=******"
echo "DB_NAME=$DB_NAME"

# Export variables for use in the current shell
export DB_HOST
export DB_PORT
export DB_USER
export DB_PASSWORD
export DB_NAME

# Generate commands for setting these variables in CI/CD
echo ""
echo "To set these variables in your CI/CD environment, use:"
echo "export DB_HOST=\"$DB_HOST\""
echo "export DB_PORT=\"$DB_PORT\""
echo "export DB_USER=\"$DB_USER\""
echo "export DB_PASSWORD=\"your_secure_password\""
echo "export DB_NAME=\"$DB_NAME\""

echo ""
echo "For GitHub Actions, add these to your workflow file:"
echo "env:"
echo "  DB_HOST: $DB_HOST"
echo "  DB_PORT: $DB_PORT"
echo "  DB_USER: $DB_USER"
echo "  DB_PASSWORD: \${{ secrets.DB_PASSWORD }}"
echo "  DB_NAME: $DB_NAME"

echo ""
echo "Remember to set the DB_PASSWORD secret in your CI/CD environment!"
