#!/bin/bash
# Script to test database connection with environment variables

# Set default values
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-postgres}
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
ENVIRONMENTS=${ENVIRONMENTS:-local}

echo "Running database connection test with:"
echo "  DB_USER: $DB_USER"
echo "  DB_PASSWORD: ********"
echo "  DB_HOST: $DB_HOST"
echo "  DB_PORT: $DB_PORT"
echo "  ENVIRONMENTS: $ENVIRONMENTS"
echo

# Run the Python test script with environment variables
uv run python /home/jianjun/ats-genai/tests/secmaster/test_db_connection_local.py \
  --db-user "$DB_USER" \
  --db-password "$DB_PASSWORD" \
  --db-host "$DB_HOST" \
  --db-port "$DB_PORT" \
  --environments "$ENVIRONMENTS"
