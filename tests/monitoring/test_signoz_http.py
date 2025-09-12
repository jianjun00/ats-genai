#!/usr/bin/env python3
"""
Test SigNoz integration using HTTP endpoint instead of gRPC
"""

import time
import logging
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_tracing():
    """Setup OpenTelemetry tracing using HTTP endpoint"""

    resource = Resource.create({
        SERVICE_NAME: "ats-analytics-service",
        SERVICE_VERSION: "1.0.0",
        "environment": "dev",
        "ats.component": "analytics"
    })

    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Use HTTP endpoint instead of gRPC
    otlp_exporter = OTLPSpanExporter(
        endpoint="http://localhost:4318/v1/traces"  # SigNoz OTLP HTTP endpoint
    )

    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    logger.info("✅ OpenTelemetry HTTP tracing configured")
    return trace.get_tracer(__name__)

def test_simple_trace(tracer):
    """Create a simple trace"""
    with tracer.start_as_current_span("test_operation") as span:
        span.set_attribute("test.id", "ats-001")
        span.set_attribute("test.status", "success")
        time.sleep(0.1)
        logger.info("📋 Test operation completed")

if __name__ == "__main__":
    print("🧪 Testing SigNoz HTTP integration...")

    tracer = setup_tracing()
    test_simple_trace(tracer)

    # Force flush
    trace.get_tracer_provider().force_flush(timeout_millis=5000)
    print("✅ Test sent to SigNoz!")