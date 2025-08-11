#!/bin/bash

# ATS GenAI Environment Setup Script
# This script sets up the proper PYTHONPATH and loads environment variables

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "✅ Environment variables loaded from .env"
else
    echo "⚠️  .env file not found"
fi

# Set PYTHONPATH to include src directory
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
echo "✅ PYTHONPATH set to include src directory"

# Print current PYTHONPATH
echo "📁 Current PYTHONPATH: $PYTHONPATH"

# Test if the setup works
echo
echo "🧪 Testing Python module imports..."
if uv run python -c "from config.database import *; print('✅ Database config loaded successfully')" 2>/dev/null; then
    echo "✅ Module imports working correctly"
else
    echo "❌ Module imports still have issues"
fi

echo
echo "🚀 Environment setup complete!"
echo
echo "Now you can run:"
echo "  uv run python -c \"from config.database import *; print('Database config loaded')\""
echo "  uv run python -c \"from config.environment import Environment; print('Environment config loaded')\""
echo "  uv run uvicorn src.simple_main:app --host 0.0.0.0 --port 8080"
echo
echo "Or start an interactive Python session:"
echo "  uv run python"
