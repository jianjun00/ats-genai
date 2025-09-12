#!/bin/bash
# Start News Collection Service with OpenTelemetry Metrics for SigNoz

set -e

# Configuration
ENVIRONMENT="${1:-intg}"
POLYGON_API_KEY="${POLYGON_API_KEY}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}📊 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Header
echo -e "${BLUE}🚀 Starting ATS News Collection Service with OpenTelemetry Metrics${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo ""

# Check prerequisites
print_status "Checking prerequisites..."

# Check API key
if [ -z "$POLYGON_API_KEY" ]; then
    print_error "POLYGON_API_KEY environment variable not set"
    echo "Usage: POLYGON_API_KEY=\"your_key\" $0 [environment]"
    exit 1
fi

# Check Docker networks
if ! docker network inspect ats-${ENVIRONMENT}-network >/dev/null 2>&1; then
    print_error "Docker network ats-${ENVIRONMENT}-network not found"
    echo "Please ensure ATS environment is running"
    exit 1
fi

if ! docker network inspect signoz-network >/dev/null 2>&1; then
    print_warning "SigNoz network not found, creating..."
    docker network create signoz-network
fi

print_success "Prerequisites check passed"

# Stop existing service if running
if docker ps -q --filter "name=ats-${ENVIRONMENT}-news-metrics" | grep -q .; then
    print_status "Stopping existing news metrics service..."
    docker stop ats-${ENVIRONMENT}-news-metrics
    docker rm ats-${ENVIRONMENT}-news-metrics
fi

# Update docker-compose environment
print_status "Configuring service for environment: $ENVIRONMENT"

# Create environment-specific compose file
cat > docker-compose.news-metrics-${ENVIRONMENT}.yml <<EOF
version: '3.8'

services:
  ats-${ENVIRONMENT}-news-metrics:
    image: dragonflyer762/ats-genai:latest
    container_name: ats-${ENVIRONMENT}-news-metrics
    networks:
      - ats-${ENVIRONMENT}-network
      - ats-network
    ports:
      - "8082:8082"
    environment:
      # Service Configuration
      - ENVIRONMENT=${ENVIRONMENT}
      - SERVICE_PORT=8082
      - PYTHONPATH=/workspace/src

      # Database Configuration
      - DB_HOST=ats-${ENVIRONMENT}-postgres
      - DB_PORT=5432
      - DB_USER=postgres
      - DB_PASSWORD=${ENVIRONMENT}_password
      - DB_NAME=${ENVIRONMENT}_db

      # API Keys
      - POLYGON_API_KEY=${POLYGON_API_KEY}

      # OpenTelemetry Configuration for SigNoz
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://signoz-otel-collector:4318
      - OTEL_SERVICE_NAME=ats-${ENVIRONMENT}-news-collection
      - OTEL_RESOURCE_ATTRIBUTES=environment=${ENVIRONMENT},ats.component=news-ingestion

      # Python OpenTelemetry Configuration
      - OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
      - OTEL_PYTHON_LOG_CORRELATION=true

    volumes:
      - /home/jianjun/ats-genai-data:/workspace
      - /mnt/d/ats-data:/data
      - /mnt/d/ats-logs:/logs

    working_dir: /workspace

    command: >
      bash -c "
        echo '📦 Installing OpenTelemetry packages...' &&
        pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp-proto-http opentelemetry-instrumentation-asyncpg opentelemetry-instrumentation-aiohttp-client opentelemetry-instrumentation-logging &&
        echo '🚀 Starting news collection service with metrics...' &&
        python3 scripts/news_collection_with_metrics.py
      "

    restart: unless-stopped

    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8082/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s


networks:
  ats-${ENVIRONMENT}-network:
    external: true
  ats-network:
    external: true
EOF

# Start the service
print_status "Starting news metrics service..."
POLYGON_API_KEY="$POLYGON_API_KEY" docker-compose -f docker-compose.news-metrics-${ENVIRONMENT}.yml up -d

# Wait for service to start
print_status "Waiting for service to be ready..."
sleep 10

# Check service status
if docker ps --filter "name=ats-${ENVIRONMENT}-news-metrics" --filter "status=running" | grep -q .; then
    print_success "News metrics service started successfully!"
    echo ""
    print_status "Service endpoints:"
    echo "   Health Check: http://localhost:8082/health"
    echo "   Metrics: http://localhost:8082/metrics"
    echo "   Manual Collection: POST http://localhost:8082/collect"
    echo ""
    print_status "Monitoring:"
    echo "   SigNoz Dashboard: http://localhost:8080"
    echo "   Service Name: ats-${ENVIRONMENT}-news-collection"
    echo ""
    print_status "Testing endpoints:"

    # Test health endpoint
    echo -n "   Health endpoint: "
    if curl -s -f http://localhost:8082/health >/dev/null; then
        print_success "OK"
    else
        print_warning "Not ready yet (may take 1-2 minutes)"
    fi

    # Test metrics endpoint
    echo -n "   Metrics endpoint: "
    if curl -s -f http://localhost:8082/metrics >/dev/null; then
        print_success "OK"
    else
        print_warning "Not ready yet (may take 1-2 minutes)"
    fi

    echo ""
    print_status "View logs with: docker logs ats-${ENVIRONMENT}-news-metrics -f"
    echo ""
    print_success "🎉 News collection service with OpenTelemetry metrics is now running!"
    print_status "📊 Check SigNoz dashboard in 2-3 minutes for service to appear"

else
    print_error "Failed to start service"
    echo ""
    print_status "Check logs with: docker logs ats-${ENVIRONMENT}-news-metrics"
    exit 1
fi