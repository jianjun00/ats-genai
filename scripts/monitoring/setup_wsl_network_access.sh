#!/bin/bash
# WSL Network Access Setup for ATS Monitoring
# Run this script inside WSL to configure services for external access

echo "🌐 Setting up WSL network access for ATS monitoring services..."

# Get WSL IP address
WSL_IP=$(hostname -I | awk '{print $1}')
echo "WSL IP Address: $WSL_IP"

# Check if services are binding to localhost only
echo ""
echo "🔍 Checking current service bindings..."

# Check if processes are listening on specific ports
check_port_binding() {
    local port=$1
    local service=$2
    
    # Check if port is listening
    if netstat -tln | grep -q ":$port "; then
        local binding=$(netstat -tln | grep ":$port " | awk '{print $4}')
        echo "  $service (port $port): $binding"
        
        # Check if it's only localhost
        if echo "$binding" | grep -q "127.0.0.1:$port\|localhost:$port"; then
            echo "    ⚠️  WARNING: Only listening on localhost"
            return 1
        elif echo "$binding" | grep -q "0.0.0.0:$port\|:::$port"; then
            echo "    ✅ Listening on all interfaces"
            return 0
        else
            echo "    ❓ Unknown binding pattern"
            return 1
        fi
    else
        echo "  $service (port $port): Not running"
        return 2
    fi
}

# Check each service
services_status=()
check_port_binding 3000 "Grafana" && services_status+=(0) || services_status+=($?)
check_port_binding 8000 "Data Agent" && services_status+=(0) || services_status+=($?)
check_port_binding 8001 "PostgreSQL Metrics" && services_status+=(0) || services_status+=($?)
check_port_binding 9090 "Prometheus" && services_status+=(0) || services_status+=($?)

# Install tools if needed
echo ""
echo "🔧 Installing network tools..."
if ! command -v netstat &> /dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y net-tools
fi

# Create configuration files for services to bind to all interfaces
echo ""
echo "📝 Creating service configuration recommendations..."

# PostgreSQL metrics configuration
cat > /tmp/postgres_monitoring_external.py << 'EOF'
#!/usr/bin/env python3
"""
PostgreSQL monitoring with external network access
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from monitoring.postgres_prometheus_exporter import setup_postgresql_monitoring
from config.environment import Environment
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Setup monitoring with external access
env = Environment()
monitor = setup_postgresql_monitoring(env=env, port=8001, update_interval=30)

if monitor:
    print("✅ PostgreSQL monitoring running on all interfaces (port 8001)")
    print("   Accessible externally via WSL port forwarding")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping PostgreSQL monitoring...")
        monitor.stop()
else:
    print("❌ Failed to start PostgreSQL monitoring")
EOF

chmod +x /tmp/postgres_monitoring_external.py

# Create systemd service file for PostgreSQL monitoring
cat > /tmp/postgres-monitoring.service << EOF
[Unit]
Description=PostgreSQL Monitoring for ATS
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/ats-genai
Environment=PYTHONPATH=/home/$USER/ats-genai/src
ExecStart=/usr/bin/python3 /tmp/postgres_monitoring_external.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo ""
echo "🚀 Service startup scripts created:"
echo "  PostgreSQL Monitoring: /tmp/postgres_monitoring_external.py"
echo "  Systemd Service: /tmp/postgres-monitoring.service"

# Show network configuration
echo ""
echo "🌐 Network Configuration:"
echo "WSL IP: $WSL_IP"
echo "Network interfaces:"
ip addr show | grep -E "inet.*scope global" | awk '{print "  " $2 " (" $NF ")"}'

# Create helper scripts
cat > /tmp/start_monitoring_external.sh << 'EOF'
#!/bin/bash
echo "Starting ATS monitoring services for external access..."

# Kill any existing monitoring processes
pkill -f "postgres_monitoring_external.py" 2>/dev/null
pkill -f "prometheus" 2>/dev/null
pkill -f "grafana" 2>/dev/null

# Start PostgreSQL monitoring
echo "Starting PostgreSQL monitoring..."
nohup python3 /tmp/postgres_monitoring_external.py > /tmp/postgres_monitoring.log 2>&1 &

# Start other services if available
if command -v docker-compose &> /dev/null; then
    echo "Starting monitoring stack with docker-compose..."
    cd ~/ats-genai/src/market_data/agent/monitoring
    docker-compose up -d
fi

echo "✅ Monitoring services started"
echo "Check logs: tail -f /tmp/postgres_monitoring.log"
EOF

chmod +x /tmp/start_monitoring_external.sh

cat > /tmp/stop_monitoring_external.sh << 'EOF'
#!/bin/bash
echo "Stopping ATS monitoring services..."

# Kill monitoring processes
pkill -f "postgres_monitoring_external.py" 2>/dev/null
pkill -f "prometheus" 2>/dev/null
pkill -f "grafana" 2>/dev/null

# Stop docker services if running
if command -v docker-compose &> /dev/null; then
    cd ~/ats-genai/src/market_data/agent/monitoring 2>/dev/null
    docker-compose down 2>/dev/null
fi

echo "✅ Monitoring services stopped"
EOF

chmod +x /tmp/stop_monitoring_external.sh

echo ""
echo "🎯 External Access Setup Summary:"
echo "================================"
echo ""
echo "📋 Steps to enable external access:"
echo ""
echo "1. 🪟 On Windows host (run as Administrator):"
echo "   PowerShell -ExecutionPolicy Bypass -File scripts/monitoring/setup_wsl_port_forwarding.ps1"
echo ""
echo "2. 🐧 In WSL (run this):"
echo "   /tmp/start_monitoring_external.sh"
echo ""
echo "3. 🌐 Access from other machines:"
echo "   - Get Windows host IP from the PowerShell script output"
echo "   - Use URLs like: http://WINDOWS_HOST_IP:3000 (Grafana)"
echo ""
echo "🔧 Management Commands:"
echo "  Start:  /tmp/start_monitoring_external.sh"
echo "  Stop:   /tmp/stop_monitoring_external.sh"
echo "  Logs:   tail -f /tmp/postgres_monitoring.log"
echo ""
echo "📝 Notes:"
echo "- WSL IP changes when WSL restarts"
echo "- Re-run Windows PowerShell script after WSL restart"
echo "- Services will bind to 0.0.0.0 (all interfaces)"
echo ""

# Test current network accessibility
echo "🧪 Testing current network accessibility:"
for port in 3000 8000 8001 9090; do
    if netstat -tln | grep -q ":$port "; then
        if netstat -tln | grep ":$port " | grep -q "0.0.0.0\|:::"; then
            echo "  Port $port: ✅ Accessible externally"
        else
            echo "  Port $port: ⚠️  Localhost only"
        fi
    else
        echo "  Port $port: ❌ Not running"
    fi
done

echo ""
echo "✅ WSL network access setup complete!"