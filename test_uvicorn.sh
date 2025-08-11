#!/bin/bash
set -e

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Install uvicorn if not found
if ! command -v uvicorn &> /dev/null; then
    echo "uvicorn is not installed. Installing..."
    if command -v uv &> /dev/null; then
        uv pip install uvicorn
    else
        pip install uvicorn
    fi
fi

echo "Testing uvicorn installation..."
python -m uvicorn --version

echo "Creating a simple FastAPI test app..."
cat > test_app.py << 'EOL'
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello, World!"}
EOL

echo "Starting test server..."
echo "Open http://localhost:8000 in your browser"
uvicorn test_app:app --reload
