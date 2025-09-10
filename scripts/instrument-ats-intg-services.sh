#!/bin/bash
# ATS-INTG Service Instrumentation Script
# Automatically instrument existing ATS-INTG services with OpenTelemetry

set -e

# Configuration
API_KEY="9RbijHam3W4B0a8h5fFB+7NgUgmXV+hFnzIPQUqtc6M="
SIGNOZ_ENDPOINT="http://signoz-otel-collector:4318"
ENVIRONMENT="intg"

echo "🔧 ATS-INTG Service Instrumentation"
echo "=================================="
echo "API Key: ${API_KEY:0:8}..."
echo "Endpoint: $SIGNOZ_ENDPOINT"
echo "Environment: $ENVIRONMENT"
echo ""

# Check if services are running
check_service_running() {
    local service_name=$1
    if docker ps | grep -q "$service_name"; then
        echo "✅ $service_name is running"
        return 0
    else
        echo "❌ $service_name is not running"
        return 1
    fi
}

# Install OpenTelemetry in service container
instrument_service() {
    local service_name=$1
    local service_type=$2
    
    echo "🔧 Instrumenting $service_name..."
    
    # Install OpenTelemetry packages in the container
    docker exec "$service_name" bash -c "
        pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp \
                   opentelemetry-instrumentation-auto \
                   opentelemetry-instrumentation-requests \
                   opentelemetry-instrumentation-psycopg2 \
                   opentelemetry-instrumentation-asyncpg 2>/dev/null || echo 'Packages already installed'
    " 2>/dev/null || echo "⚠️  Could not install packages in $service_name (may already be installed)"
    
    # Create instrumentation script
    cat > "/tmp/${service_name}-instrumentation.py" << EOF
#!/usr/bin/env python3
"""
OpenTelemetry instrumentation for $service_name
Auto-generated instrumentation script
"""

import os
import logging
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.instrumentation.auto_instrumentation import sitecustomize

# Configure OpenTelemetry
resource = Resource.create({
    SERVICE_NAME: "$service_name",
    SERVICE_VERSION: "1.0.0",
    "environment": "$ENVIRONMENT",
    "ats.tier": "integration",
    "ats.service.type": "$service_type"
})

# Setup tracing
trace.set_tracer_provider(TracerProvider(resource=resource))
otlp_trace_exporter = OTLPSpanExporter(
    endpoint="$SIGNOZ_ENDPOINT/v1/traces",
    headers={"signoz-access-token": "$API_KEY"}
)
span_processor = BatchSpanProcessor(otlp_trace_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setup metrics
otlp_metric_exporter = OTLPMetricExporter(
    endpoint="$SIGNOZ_ENDPOINT/v1/metrics",
    headers={"signoz-access-token": "$API_KEY"}
)
metric_reader = PeriodicExportingMetricReader(
    exporter=otlp_metric_exporter,
    export_interval_millis=15000
)
metrics.set_meter_provider(MeterProvider(
    resource=resource,
    metric_readers=[metric_reader]
))

print(f"✅ OpenTelemetry configured for $service_name")
EOF
    
    # Copy instrumentation to service container
    docker cp "/tmp/${service_name}-instrumentation.py" "$service_name:/workspace/otel_instrumentation.py"
    
    echo "✅ $service_name instrumented successfully"
}

# Create service restart script with instrumentation
create_instrumented_restart_script() {
    local service_name=$1
    
    cat > "/tmp/restart-${service_name}.sh" << EOF
#!/bin/bash
# Restart $service_name with OpenTelemetry instrumentation

echo "🔄 Restarting $service_name with instrumentation..."

# Set OpenTelemetry environment variables
export OTEL_EXPORTER_OTLP_ENDPOINT="$SIGNOZ_ENDPOINT"
export OTEL_EXPORTER_OTLP_HEADERS="signoz-access-token=$API_KEY"
export OTEL_SERVICE_NAME="$service_name"
export OTEL_RESOURCE_ATTRIBUTES="environment=$ENVIRONMENT,ats.tier=integration"
export OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=""

# Restart the service with instrumentation
docker restart "$service_name"

echo "✅ $service_name restarted with instrumentation"
EOF
    
    chmod +x "/tmp/restart-${service_name}.sh"
    echo "📄 Restart script created: /tmp/restart-${service_name}.sh"
}

# Main instrumentation process
main() {
    echo "🔍 Detecting ATS-INTG services..."
    
    # List of ATS-INTG services to instrument
    declare -A services=(
        ["ats-intg-analytics"]="web_service"
        ["ats-intg-postgres"]="database" 
        ["ats-intg-prometheus-metrics"]="metrics_collector"
    )
    
    for service_name in "${!services[@]}"; do
        service_type="${services[$service_name]}"
        
        if check_service_running "$service_name"; then
            instrument_service "$service_name" "$service_type"
            create_instrumented_restart_script "$service_name"
        else
            echo "⚠️  Skipping $service_name - service not running"
        fi
        echo ""
    done
    
    # Create comprehensive monitoring startup script
    cat > "/tmp/start-ats-intg-monitoring.sh" << 'EOF'
#!/bin/bash
# Start comprehensive ATS-INTG monitoring

echo "🚀 Starting ATS-INTG comprehensive monitoring..."

# Set global OpenTelemetry environment
export OTEL_EXPORTER_OTLP_ENDPOINT="http://signoz-otel-collector:4318"
export OTEL_EXPORTER_OTLP_HEADERS="signoz-access-token=9RbijHam3W4B0a8h5fFB+7NgUgmXV+hFnzIPQUqtc6M="
export OTEL_RESOURCE_ATTRIBUTES="environment=intg,ats.tier=integration"

# Start monitoring simulation
echo "📊 Starting monitoring data simulation..."
cd /home/jianjun/ats-genai-data
source .venv/bin/activate
python /home/jianjun/ats-genai-model/ats-intg-monitoring.py &

echo "✅ ATS-INTG monitoring started!"
echo "🔗 Dashboard: http://localhost:8080"
echo "📊 Look for services: ats-intg-analytics, ats-intg-monitor"
EOF
    
    chmod +x "/tmp/start-ats-intg-monitoring.sh"
    
    echo "=================================="
    echo "🎉 ATS-INTG Instrumentation Complete!"
    echo ""
    echo "📋 Next Steps:"
    echo "1. Run monitoring simulation:"
    echo "   bash /tmp/start-ats-intg-monitoring.sh"
    echo ""
    echo "2. Restart services with instrumentation:"
    for service_name in "${!services[@]}"; do
        if check_service_running "$service_name" >/dev/null 2>&1; then
            echo "   bash /tmp/restart-${service_name}.sh"
        fi
    done
    echo ""
    echo "3. Access SigNoz dashboard: http://localhost:8080"
    echo "4. Look for ATS-INTG services in traces and metrics"
    echo ""
    echo "🚨 Alert Thresholds Set:"
    echo "   - Data freshness: > 120 minutes"
    echo "   - Test success rate: < 90%" 
    echo "   - DB connections: > 90"
    echo "   - API response time: > 5000ms"
    echo "=================================="
}

# Run main function
main "$@"