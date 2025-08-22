#!/bin/bash
# Start ATS monitoring services with external network access
# This script ensures all services bind to 0.0.0.0 (all interfaces) for WSL external access

echo "🚀 Starting ATS monitoring services for external access..."

# Get WSL IP
WSL_IP=$(hostname -I | awk '{print $1}')
echo "WSL IP: $WSL_IP"

# Kill existing monitoring processes
echo "🛑 Stopping existing monitoring processes..."
pkill -f "postgres_.*monitoring" 2>/dev/null
pkill -f "prometheus" 2>/dev/null
pkill -f "grafana" 2>/dev/null
pkill -f "setup_postgres_monitoring" 2>/dev/null

# Wait for processes to stop
sleep 2

# Create monitoring directory
mkdir -p /tmp/ats-monitoring
cd /tmp/ats-monitoring

# 1. Start PostgreSQL monitoring (external access)
echo "📊 Starting PostgreSQL monitoring (port 8001)..."
cat > postgres_monitoring.py << 'EOF'
#!/usr/bin/env python3
import sys
import os
from pathlib import Path

# Add ATS src to path
ats_src = Path.home() / "ats-genai" / "src"
sys.path.insert(0, str(ats_src))

os.chdir(Path.home() / "ats-genai")

from monitoring.postgres_prometheus_exporter import setup_postgresql_monitoring
from config.environment import Environment
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/postgres_monitoring.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

try:
    # Setup monitoring with external access
    env = Environment()
    monitor = setup_postgresql_monitoring(env=env, port=8001, update_interval=30)
    
    if monitor:
        logger.info("✅ PostgreSQL monitoring running on all interfaces (port 8001)")
        logger.info(f"   Accessible at: http://{os.environ.get('WSL_IP', 'localhost')}:8001/metrics")
        
        # Keep running
        while True:
            time.sleep(1)
    else:
        logger.error("❌ Failed to start PostgreSQL monitoring")
        sys.exit(1)
        
except KeyboardInterrupt:
    logger.info("Stopping PostgreSQL monitoring...")
    if 'monitor' in locals():
        monitor.stop()
except Exception as e:
    logger.error(f"Error in PostgreSQL monitoring: {e}")
    sys.exit(1)
EOF

# Set environment variables
export WSL_IP=$WSL_IP
export PYTHONPATH="/home/$(whoami)/ats-genai/src"

# Start PostgreSQL monitoring in background
echo "  Starting PostgreSQL metrics exporter..."
nohup python3 postgres_monitoring.py > /tmp/postgres_monitoring.log 2>&1 &
POSTGRES_PID=$!

# Wait a moment for it to start
sleep 3

# Check if PostgreSQL monitoring started successfully
if kill -0 $POSTGRES_PID 2>/dev/null; then
    echo "  ✅ PostgreSQL monitoring started (PID: $POSTGRES_PID)"
else
    echo "  ❌ PostgreSQL monitoring failed to start"
    echo "  📋 Check logs: tail /tmp/postgres_monitoring.log"
fi

# 2. Start Prometheus (if docker is available)
echo "📈 Starting Prometheus (port 9090)..."
if command -v docker &> /dev/null; then
    # Use Docker to run Prometheus with external access
    docker run -d --name ats-prometheus \
        -p 0.0.0.0:9090:9090 \
        -v /home/$(whoami)/ats-genai/src/market_data/agent/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml \
        --network host \
        prom/prometheus:v2.45.0 \
        --config.file=/etc/prometheus/prometheus.yml \
        --web.listen-address=0.0.0.0:9090 > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Prometheus started with Docker"
    else
        echo "  ⚠️  Prometheus Docker start failed, trying existing setup..."
    fi
else
    echo "  ⚠️  Docker not available for Prometheus"
fi

# 3. Start Grafana (if docker is available)
echo "📊 Starting Grafana (port 3000)..."
if command -v docker &> /dev/null; then
    # Use Docker to run Grafana with external access
    docker run -d --name ats-grafana \
        -p 0.0.0.0:3000:3000 \
        -e GF_SECURITY_ADMIN_PASSWORD=admin \
        -e GF_SERVER_HTTP_ADDR=0.0.0.0 \
        --network host \
        grafana/grafana:10.0.3 > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Grafana started with Docker"
    else
        echo "  ⚠️  Grafana Docker start failed"
    fi
else
    echo "  ⚠️  Docker not available for Grafana"
fi

# 4. Try docker-compose if available
if [ -f "/home/$(whoami)/ats-genai/src/market_data/agent/monitoring/docker-compose.yml" ] && command -v docker-compose &> /dev/null; then
    echo "🐳 Starting monitoring stack with docker-compose..."
    cd "/home/$(whoami)/ats-genai/src/market_data/agent/monitoring"
    
    # Modify docker-compose to bind to all interfaces
    cp docker-compose.yml docker-compose-external.yml
    sed -i 's/127\.0\.0\.1:/0.0.0.0:/g' docker-compose-external.yml 2>/dev/null || true
    
    docker-compose -f docker-compose-external.yml up -d > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "  ✅ Docker-compose monitoring stack started"
    fi
fi

# Wait for services to start
echo ""
echo "⏳ Waiting for services to start..."
sleep 5

# Check service status
echo ""
echo "🔍 Checking service status:"

check_service() {
    local port=$1
    local service=$2
    
    if netstat -tln 2>/dev/null | grep -q ":$port "; then
        local binding=$(netstat -tln | grep ":$port " | head -1 | awk '{print $4}')
        if echo "$binding" | grep -q "0.0.0.0:$port\|:::$port"; then
            echo "  ✅ $service (port $port): Running and externally accessible"
            return 0
        else
            echo "  ⚠️  $service (port $port): Running but localhost only ($binding)"
            return 1
        fi
    else
        echo "  ❌ $service (port $port): Not running"
        return 2
    fi
}

# Check each service
check_service 8001 "PostgreSQL Metrics"
check_service 9090 "Prometheus"
check_service 3000 "Grafana"
check_service 8000 "Data Agent"

# Get Windows host IP for display
echo ""
echo "🌐 Network Access Information:"
echo "================================"
echo "WSL IP: $WSL_IP"

# Try to get Windows host IP using multiple methods (robust detection)
get_windows_ip() {
    # Check for manual override via environment variable
    if [ -n "$EXTERNAL_IP" ]; then
        echo "$EXTERNAL_IP"
        return 0
    fi
    
    # Method 1: Use PowerShell to get actual Windows network IP (most reliable for external access)
    if command -v powershell.exe &>/dev/null; then
        # Get Windows IP that's not a virtual/cluster IP
        PS_IP=$(powershell.exe -Command "
            \$ips = Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
                \$_.InterfaceAlias -like '*Wi-Fi*' -or \$_.InterfaceAlias -like '*Ethernet*'
            } | Where-Object {
                \$_.AddressState -eq 'Preferred' -and 
                \$_.IPAddress -notlike '172.*' -and 
                \$_.IPAddress -notlike '169.254.*'
            } | Select-Object -ExpandProperty IPAddress
            \$ips[0]
        " 2>/dev/null | tr -d '\r\n')
        
        if [ -n "$PS_IP" ] && [ "$PS_IP" != "" ] && [ "$PS_IP" != "System.Object[]" ]; then
            echo "$PS_IP"
            return 0
        fi
    fi
    
    # Method 2: Default gateway (for local network access)
    DEFAULT_GW=$(ip route | grep default | awk '{print $3}' | head -1)
    if [ -n "$DEFAULT_GW" ]; then
        # Skip if it's a k8s/docker IP range
        if [[ "$DEFAULT_GW" != 172.* ]] && ping -c 1 -W 2 "$DEFAULT_GW" &>/dev/null; then
            echo "$DEFAULT_GW"
            return 0
        fi
    fi
    
    # Method 3: Try common external network IPs
    for ip in "10.0.0.1" "192.168.1.1" "192.168.0.1"; do
        if ping -c 1 -W 1 "$ip" &>/dev/null; then
            echo "$ip"
            return 0
        fi
    done
    
    # Method 4: Fallback to detected gateway with warning
    if [ -n "$DEFAULT_GW" ]; then
        echo "$DEFAULT_GW"
        return 0
    fi
    
    # Final fallback
    echo "<YOUR_EXTERNAL_IP>"
}

WINDOWS_IP=$(get_windows_ip)

echo ""
echo "🎯 Access URLs:"
echo ""
echo "📊 Local WSL Access:"
echo "  PostgreSQL Metrics: http://localhost:8001/metrics"
echo "  Prometheus:         http://localhost:9090"
echo "  Grafana:           http://localhost:3000 (admin/admin)"
echo ""
if [ "$WINDOWS_IP" != "<YOUR_EXTERNAL_IP>" ]; then
    echo "🌍 External Network Access (after Windows port forwarding):"
    echo "  PostgreSQL Metrics: http://$WINDOWS_IP:8001/metrics"
    echo "  Prometheus:         http://$WINDOWS_IP:9090"
    echo "  Grafana:           http://$WINDOWS_IP:3000 (admin/admin)"
    echo ""
    
    # Check if detected IP might be a K8s/Docker IP
    if [[ "$WINDOWS_IP" == 172.* ]]; then
        echo "⚠️  Detected IP ($WINDOWS_IP) appears to be a K8s/Docker cluster IP"
        echo "    For external access, use your actual network IP instead."
        echo ""
        echo "🔧 To override with correct external IP:"
        echo "    EXTERNAL_IP=10.0.0.79 scripts/monitoring/start_external_monitoring.sh"
        echo ""
        echo "💡 Find your external IP on Windows with: ipconfig | findstr IPv4"
    else
        echo "✅ Windows Host IP detected: $WINDOWS_IP"
    fi
else
    echo "🌍 External Network Access (after Windows port forwarding):"
    echo "  PostgreSQL Metrics: http://YOUR_EXTERNAL_IP:8001/metrics"
    echo "  Prometheus:         http://YOUR_EXTERNAL_IP:9090"
    echo "  Grafana:           http://YOUR_EXTERNAL_IP:3000 (admin/admin)"
    echo ""
    echo "⚠️  Could not auto-detect external IP. Use manual override:"
    echo "    EXTERNAL_IP=10.0.0.79 scripts/monitoring/start_external_monitoring.sh"
    echo ""
    echo "💡 Find your external IP on Windows with: ipconfig | findstr IPv4"
fi
echo ""
echo "📝 Next Steps:"
echo "1. 🪟 On Windows host, run as Administrator:"
echo "   PowerShell -ExecutionPolicy Bypass -File scripts/monitoring/setup_wsl_port_forwarding.ps1"
echo ""
echo "2. 🌐 Access monitoring from other machines using Windows host IP"
echo ""
echo "🔧 Management:"
echo "  Stop all:    scripts/monitoring/stop_external_monitoring.sh"
echo "  View logs:   tail -f /tmp/postgres_monitoring.log"
echo "  Check ports: netstat -tln | grep -E ':(3000|8000|8001|9090) '"
echo ""

# Save process info
echo "PostgreSQL Monitoring PID: $POSTGRES_PID" > /tmp/ats_monitoring_pids.txt
docker ps --filter "name=ats-" --format "{{.Names}}: {{.ID}}" >> /tmp/ats_monitoring_pids.txt 2>/dev/null

echo "✅ External monitoring setup complete!"
echo "   Process info saved to: /tmp/ats_monitoring_pids.txt"