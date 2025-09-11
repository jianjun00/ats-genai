#!/bin/bash
# Health monitoring endpoint for external monitoring systems

ENVIRONMENT="${1:-intg}"

# Run health check and return JSON
docker run --rm \
    --network ats-${ENVIRONMENT}-network \
    -e PYTHONPATH="/workspace/src" \
    -e DB_HOST="ats-${ENVIRONMENT}-postgres" \
    -e DB_PORT="5432" \
    -e DB_USER="postgres" \
    -e DB_PASSWORD="${ENVIRONMENT}_password" \
    -e DB_NAME="${ENVIRONMENT}_db" \
    -v /home/jianjun/ats-genai-data:/workspace \
    -w /workspace \
    dragonflyer762/ats-genai:latest \
    python3 tests/monitoring/test_news_data_monitoring.py \
    --environment "$ENVIRONMENT" \
    --output json 2>/dev/null | sed -n '/^{/,$p'