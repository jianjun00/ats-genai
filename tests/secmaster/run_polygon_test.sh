#!/bin/bash
set -e

# Set up environment
export PYTHONPATH=/home/jianjun/ats-genai/src

# Run the unit test
cd /home/jianjun/ats-genai
uv run pytest tests/secmaster/test_populate_instrument_polygon.py -v

# Ensure dependencies are installed
echo "Installing required dependencies..."
uv pip install asyncpg ray requests

# If tests pass, run a direct test with the script
echo "Running direct test with the script..."
uv run python -m src.secmaster.populate_instrument_polygon --environment dev --ticker AAPL

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ Test passed! Safe to deploy to Kubernetes."
    exit 0
else
    echo "❌ Test failed! Fix issues before deploying to Kubernetes."
    exit 1
fi
