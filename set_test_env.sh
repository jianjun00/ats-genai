#!/bin/bash
# Set environment variables for test database connection
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=test_user
export POSTGRES_PASSWORD=test_password
export POSTGRES_POOL_MIN=1
export POSTGRES_POOL_MAX=10
export POSTGRES_CMD_TIMEOUT=60

# Also update the legacy TSDB_URL for backward compatibility
export TSDB_URL=postgresql://test_user:test_password@localhost:5432/trading_db

echo "Test database environment variables set:"
echo "POSTGRES_USER=$POSTGRES_USER"
echo "POSTGRES_HOST=$POSTGRES_HOST"
echo "POSTGRES_PORT=$POSTGRES_PORT"

# Run the command passed as arguments with the environment variables set
if [ $# -gt 0 ]; then
    exec "$@"
fi
