#!/bin/bash
# Stop ATS external monitoring services

echo "🛑 Stopping ATS external monitoring services..."

# Stop PostgreSQL monitoring processes
echo "📊 Stopping PostgreSQL monitoring..."
pkill -f "postgres.*monitoring" 2>/dev/null
pkill -f "setup_postgres_monitoring" 2>/dev/null

# Stop Docker containers
echo "🐳 Stopping Docker monitoring containers..."
docker stop ats-prometheus ats-grafana 2>/dev/null
docker rm ats-prometheus ats-grafana 2>/dev/null

# Stop docker-compose services
if [ -f "/home/$(whoami)/ats-genai/src/market_data/agent/monitoring/docker-compose.yml" ]; then
    echo "🐳 Stopping docker-compose monitoring stack..."
    cd "/home/$(whoami)/ats-genai/src/market_data/agent/monitoring"
    docker-compose down 2>/dev/null
    docker-compose -f docker-compose-external.yml down 2>/dev/null
fi

# Clean up temporary files
echo "🧹 Cleaning up temporary files..."
rm -f /tmp/postgres_monitoring.log
rm -f /tmp/ats_monitoring_pids.txt
rm -rf /tmp/ats-monitoring

# Wait for processes to stop
sleep 2

# Check if anything is still running
echo ""
echo "🔍 Checking remaining processes:"
remaining_ports=()
for port in 3000 8000 8001 9090; do
    if netstat -tln 2>/dev/null | grep -q ":$port "; then
        echo "  ⚠️  Port $port still in use"
        remaining_ports+=($port)
    else
        echo "  ✅ Port $port freed"
    fi
done

if [ ${#remaining_ports[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  Some ports still in use. You may need to manually kill processes:"
    for port in "${remaining_ports[@]}"; do
        echo "    sudo fuser -k $port/tcp 2>/dev/null"
    done
else
    echo ""
    echo "✅ All monitoring services stopped successfully"
fi

echo ""
echo "📝 To restart monitoring:"
echo "  scripts/monitoring/start_external_monitoring.sh"