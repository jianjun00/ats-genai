#!/bin/bash

# Claude Context MCP Status Check Script

echo "🔍 Claude Context MCP Status Check"
echo "=================================="

# Check Milvus containers
echo "📊 Milvus Container Status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep milvus || echo "❌ No Milvus containers found"

echo ""

# Check Milvus health
echo "🏥 Milvus Health Check:"
if curl -s -f http://localhost:9091/healthz > /dev/null; then
    echo "✅ Milvus is healthy (http://localhost:9091/healthz)"
else
    echo "❌ Milvus health check failed"
fi

echo ""

# Check Milvus API connectivity
echo "🔌 Milvus API Connectivity:"
if nc -z localhost 19530 2>/dev/null; then
    echo "✅ Milvus API is accessible on port 19530"
else
    echo "❌ Milvus API is not accessible on port 19530"
fi

echo ""

# Check Claude MCP configuration
echo "⚙️  Claude MCP Configuration:"
if [ -f "$HOME/.claude.json" ]; then
    if grep -q "claude-context" "$HOME/.claude.json"; then
        echo "✅ Claude Context MCP server is configured"
        # Show MCP server config (without exposing API key)
        grep -A 10 '"claude-context"' "$HOME/.claude.json" | sed 's/sk-[^"]*/"[HIDDEN]"/g'
    else
        echo "❌ Claude Context MCP server not found in configuration"
    fi
else
    echo "❌ Claude configuration file not found"
fi

echo ""

# Check OpenAI API key availability
echo "🔑 API Key Status:"
if [ -f "$(pwd)/.env.test" ]; then
    if grep -q "OPENAI_API_KEY=" "$(pwd)/.env.test"; then
        echo "✅ OpenAI API key found in .env.test"
    else
        echo "❌ OpenAI API key not found in .env.test"
    fi
else
    echo "❌ .env.test file not found"
fi

echo ""

# Test MCP server responsiveness
echo "🧪 MCP Server Test:"
export OPENAI_API_KEY=$(grep "OPENAI_API_KEY=" .env.test 2>/dev/null | cut -d'=' -f2- || echo "")
export MILVUS_ADDRESS="localhost:19530"
export EMBEDDING_MODEL="text-embedding-3-small"

if [ -n "$OPENAI_API_KEY" ]; then
    if timeout 5 npx @zilliz/claude-context-mcp@latest --help >/dev/null 2>&1; then
        echo "✅ Claude Context MCP server responds correctly"
    else
        echo "❌ Claude Context MCP server test failed"
    fi
else
    echo "⚠️  Cannot test MCP server - OpenAI API key not found"
fi

echo ""
echo "🎯 Next Steps:"
echo "   • If all checks pass, you can use semantic search in Claude Code"
echo "   • Ask questions like: 'Find all database connection code'"
echo "   • The MCP server will automatically index your codebase"
echo ""
echo "🔧 Troubleshooting:"
echo "   • Start Milvus: docker-compose up -d"
echo "   • Restart Claude Code to reload MCP configuration"
echo "   • Check logs: docker logs milvus-standalone"