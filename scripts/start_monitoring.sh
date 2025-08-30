#!/bin/bash
# Start ATS Monitoring Stack with Docker Compose
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

echo "🚀 Starting ATS Monitoring Stack"
echo "================================="

cd "$PROJECT_DIR"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install docker-compose."
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker."
    exit 1
fi

echo "📦 Starting monitoring services..."
docker-compose -f docker-compose.monitoring.yml up -d

echo "⏳ Waiting for services to start..."
sleep 10

# Check service health
echo "🔍 Checking service status..."
docker-compose -f docker-compose.monitoring.yml ps

# Get external URLs
echo ""
echo "🔗 Monitoring Access URLs:"
echo "========================="
echo "📊 Grafana:              http://localhost:3001"
echo "   Login: admin / ats-monitoring-password"
echo ""
echo "📈 Prometheus:           http://localhost:9090"
echo "🚨 AlertManager:         http://localhost:9093"
echo "📊 Data Quality Metrics: http://localhost:8080/metrics"
echo "⚙️  Node Exporter:       http://localhost:9100/metrics"
echo ""

# Test if services are accessible
echo "🧪 Testing service endpoints..."

test_endpoint() {
    local url=$1
    local name=$2
    
    if curl -s "$url" > /dev/null 2>&1; then
        echo "✅ $name is accessible"
        return 0
    else
        echo "❌ $name is not accessible"
        return 1
    fi
}

test_endpoint "http://localhost:3001/api/health" "Grafana"
test_endpoint "http://localhost:9090/-/ready" "Prometheus"
test_endpoint "http://localhost:9093/-/ready" "AlertManager"
test_endpoint "http://localhost:8080/metrics" "Data Quality Exporter"
test_endpoint "http://localhost:9100/metrics" "Node Exporter"

echo ""
echo "📋 Next Steps:"
echo "1. Open Grafana at http://localhost:3001 (admin/ats-monitoring-password)"
echo "2. Configure Slack webhook URL in monitoring/alertmanager/alertmanager.yml"
echo "3. Create Slack channels: #ats-alerts, #ats-critical, #ats-data-quality"
echo "4. Monitor the ATS Data Quality Dashboard"
echo ""
echo "🛠️  Management Commands:"
echo "Stop monitoring:  docker-compose -f docker-compose.monitoring.yml down"
echo "View logs:        docker-compose -f docker-compose.monitoring.yml logs -f"
echo "Restart services: docker-compose -f docker-compose.monitoring.yml restart"