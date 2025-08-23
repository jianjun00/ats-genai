#!/bin/bash

# Claude Context MCP Server Startup Script
# This script starts the local Milvus vector database and Claude Context MCP server

set -e

echo "🚀 Starting Claude Context MCP Setup..."

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MILVUS_ADDRESS="localhost:19530"
EMBEDDING_MODEL="text-embedding-3-small"

# Load OpenAI API key from .env.test
if [ -f "$REPO_ROOT/.env.test" ]; then
    echo "📄 Loading OpenAI API key from .env.test..."
    export OPENAI_API_KEY=$(grep "OPENAI_API_KEY=" "$REPO_ROOT/.env.test" | cut -d'=' -f2-)
    if [ -z "$OPENAI_API_KEY" ]; then
        echo "❌ Error: OPENAI_API_KEY not found in .env.test"
        exit 1
    fi
    echo "✅ OpenAI API key loaded (length: ${#OPENAI_API_KEY})"
else
    echo "❌ Error: .env.test file not found at $REPO_ROOT/.env.test"
    exit 1
fi

# Check if Milvus is running
echo "🔍 Checking Milvus vector database status..."
if curl -s -f http://localhost:9091/healthz > /dev/null; then
    echo "✅ Milvus is running and healthy"
else
    echo "🚀 Starting Milvus vector database..."
    cd "$REPO_ROOT"
    if [ ! -f "docker-compose.yml" ]; then
        echo "📥 Downloading Milvus Docker Compose configuration..."
        wget https://github.com/milvus-io/milvus/releases/download/v2.5.4/milvus-standalone-docker-compose.yml -O docker-compose.yml
    fi
    
    docker-compose up -d
    echo "⏳ Waiting for Milvus to become healthy..."
    
    # Wait up to 60 seconds for Milvus to be healthy
    for i in {1..12}; do
        if curl -s -f http://localhost:9091/healthz > /dev/null; then
            echo "✅ Milvus is now running and healthy"
            break
        fi
        echo "   Attempt $i/12: Waiting 5 seconds..."
        sleep 5
    done
    
    if ! curl -s -f http://localhost:9091/healthz > /dev/null; then
        echo "❌ Error: Milvus failed to start or become healthy"
        exit 1
    fi
fi

# Test Claude Context MCP connection
echo "🔧 Testing Claude Context MCP server..."
export MILVUS_ADDRESS="$MILVUS_ADDRESS"
export EMBEDDING_MODEL="$EMBEDDING_MODEL"

# Test with timeout
if timeout 10 npx @zilliz/claude-context-mcp@latest --help > /dev/null 2>&1; then
    echo "✅ Claude Context MCP server is working"
else
    echo "❌ Error: Claude Context MCP server test failed"
    exit 1
fi

echo ""
echo "🎉 Claude Context MCP Setup Complete!"
echo ""
echo "📋 Configuration Summary:"
echo "   • Milvus Address: $MILVUS_ADDRESS"
echo "   • Embedding Model: $EMBEDDING_MODEL"
echo "   • OpenAI API Key: Loaded from .env.test"
echo "   • Repository: $REPO_ROOT"
echo ""
echo "💡 Usage:"
echo "   • Claude Code now has semantic search capabilities"
echo "   • Ask questions like: 'Find database connection code' or 'Show authentication functions'"
echo "   • The MCP server will automatically index your codebase as you use it"
echo ""
echo "🔗 Access Points:"
echo "   • Milvus UI: http://localhost:9000 (admin/minioadmin)"
echo "   • Milvus API: http://localhost:19530"
echo "   • Milvus Health: http://localhost:9091/healthz"
echo ""
echo "🛠️ Management Commands:"
echo "   • Status: docker ps | grep milvus"
echo "   • Logs: docker logs milvus-standalone"
echo "   • Stop: docker-compose down"
echo "   • Restart: docker-compose restart"