#!/bin/bash
# Start ATS-INTG Monitoring Stack with Docker Compose
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

echo "🚀 Starting ATS-INTG Monitoring Stack"
echo "====================================="

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

echo "📦 Starting INTG monitoring services..."
docker-compose -f deployment/docker-compose.monitoring-intg.yml up -d

echo "⏳ Waiting for services to start..."
sleep 10

# Check service health
echo "🔍 Checking service status..."
docker-compose -f deployment/docker-compose.monitoring-intg.yml ps

# Get external URLs
echo ""
echo "🔗 ATS-INTG Monitoring Access URLs:"
echo "=================================="
echo "📊 Grafana:              http://localhost:4000"
echo "   Login: admin / ats-intg-monitoring-password"
echo ""
echo "📈 Prometheus:           http://localhost:9091"
echo "🚨 AlertManager:         http://localhost:9094"
echo "📊 Data Quality Metrics: http://localhost:8081/metrics"
echo "⚙️  Node Exporter:       http://localhost:9101/metrics"
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

test_endpoint "http://localhost:4000/api/health" "Grafana INTG"
test_endpoint "http://localhost:9091/-/ready" "Prometheus INTG"
test_endpoint "http://localhost:9094/-/ready" "AlertManager INTG"
test_endpoint "http://localhost:8081/metrics" "Data Quality Exporter INTG"
test_endpoint "http://localhost:9101/metrics" "Node Exporter INTG"

echo ""
echo "📋 Next Steps:"
echo "1. Open Grafana at http://localhost:4000 (admin/ats-intg-monitoring-password)"
echo "2. Configure Slack webhook URL in monitoring/alertmanager/alertmanager-intg.yml"
echo "3. Create Slack channels: #ats-intg-alerts, #ats-intg-critical, #ats-intg-data-quality"
echo "4. Monitor the ATS Data Quality Dashboard"
echo ""
echo "🛠️  Management Commands:"
echo "Stop monitoring:  docker-compose -f deployment/docker-compose.monitoring-intg.yml down"
echo "View logs:        docker-compose -f deployment/docker-compose.monitoring-intg.yml logs -f"
echo "Restart services: docker-compose -f deployment/docker-compose.monitoring-intg.yml restart"