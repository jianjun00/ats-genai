#!/bin/bash
# Fix WSL2 networking for ATS services
# This script ensures Windows can access WSL2 services on localhost

set -e

echo "🔧 Fixing WSL2 networking for ATS services..."

# Get WSL IP address
WSL_IP=$(hostname -I | awk '{print $1}')
echo "📍 WSL IP: $WSL_IP"

echo "🚀 Setting up networking..."

# Alternative approach: Use socat for port forwarding within WSL
if command -v socat >/dev/null 2>&1; then
    echo "✅ Using socat for internal port forwarding"

    # Kill existing socat processes
    pkill -f "socat.*:3000" 2>/dev/null || true
    pkill -f "socat.*:4000" 2>/dev/null || true

    # Start port forwarding in background
    nohup socat TCP-LISTEN:3000,fork,reuseaddr TCP:localhost:3000 > /dev/null 2>&1 &
    nohup socat TCP-LISTEN:4000,fork,reuseaddr TCP:localhost:4000 > /dev/null 2>&1 &

    echo "✅ Internal port forwarding started"
else
    echo "⚠️ socat not available, will need Windows-side port forwarding"
fi

# Test connectivity
echo "🧪 Testing connectivity..."
if curl -s http://localhost:3000/health > /dev/null; then
    echo "✅ ATS-DEV (port 3000) accessible"
else
    echo "❌ ATS-DEV (port 3000) not accessible"
fi

if curl -s http://localhost:4000/health > /dev/null; then
    echo "✅ ATS-INTG (port 4000) accessible"
else
    echo "❌ ATS-INTG (port 4000) not accessible"
fi

echo ""
echo "🎯 SOLUTION STEPS:"
echo "1. From Windows Command Prompt (Run as Administrator):"
echo "   cd C:\temp"
echo "   setup_wsl_ports.bat $WSL_IP"
echo ""
echo "2. Or manually run these commands in Windows Admin Command Prompt:"
echo "   netsh interface portproxy add v4tov4 listenport=3000 listenaddress=0.0.0.0 connectport=3000 connectaddress=$WSL_IP"
echo "   netsh interface portproxy add v4tov4 listenport=4000 listenaddress=0.0.0.0 connectport=4000 connectaddress=$WSL_IP"
echo ""
echo "3. Then test from Windows:"
echo "   http://localhost:3000 (ATS-DEV)"
echo "   http://localhost:4000 (ATS-INTG)"

echo ""
echo "✅ WSL networking fix script completed!"