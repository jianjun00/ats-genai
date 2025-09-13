#!/usr/bin/env python3
"""
Test script to verify SigNoz integration with ATS services
This script demonstrates how to send telemetry data to SigNoz
"""

import time
import logging
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_tracing():
    """Setup OpenTelemetry tracing for ATS services"""

    # Create resource with service information
    resource = Resource.create({
        SERVICE_NAME: "ats-test-service",
        SERVICE_VERSION: "1.0.0",
        "environment": "dev",
        "ats.component": "test"
    })

    # Set up tracer provider
    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Configure OTLP exporter (pointing to SigNoz)
    otlp_exporter = OTLPSpanExporter(
        endpoint="http://localhost:4317",  # SigNoz OTLP gRPC endpoint
        insecure=True
    )

    # Add batch span processor
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    logger.info("✅ OpenTelemetry tracing configured for SigNoz")
    return trace.get_tracer(__name__)

def simulate_financial_operations(tracer):
    """Simulate typical ATS financial operations with tracing"""

    with tracer.start_as_current_span("market_data_fetch") as span:
        # Simulate market data fetching
        span.set_attribute("data.vendor", "firstrate")
        span.set_attribute("data.symbol", "AAPL")
        span.set_attribute("data.timeframe", "1m")

        time.sleep(0.1)  # Simulate processing time
        logger.info("📊 Fetched market data for AAPL")

        with tracer.start_as_current_span("data_validation") as child_span:
            child_span.set_attribute("validation.type", "ohlcv_completeness")
            child_span.set_attribute("validation.passed", True)
            time.sleep(0.05)
            logger.info("✅ Data validation passed")

    with tracer.start_as_current_span("portfolio_calculation") as span:
        span.set_attribute("portfolio.total_value", 150000.25)
        span.set_attribute("portfolio.positions", 12)
        span.set_attribute("calculation.type", "mark_to_market")

        time.sleep(0.2)  # Simulate calculation
        logger.info("💰 Portfolio calculation completed")

        # Simulate error scenario
        with tracer.start_as_current_span("risk_calculation") as risk_span:
            risk_span.set_attribute("risk.model", "var")
            risk_span.set_attribute("risk.confidence", 0.95)

            # Simulate occasional error
            import random
            if random.random() < 0.3:
                risk_span.record_exception(Exception("Risk calculation timeout"))
                risk_span.set_status(trace.Status(trace.StatusCode.ERROR, "Calculation timeout"))
                logger.error("❌ Risk calculation failed")
            else:
                risk_span.set_attribute("risk.var_95", 5000.0)
                logger.info("📈 Risk calculation completed")

def test_database_operations(tracer):
    """Simulate database operations typical in ATS"""

    with tracer.start_as_current_span("database_query") as span:
        span.set_attribute("db.system", "postgresql")
        span.set_attribute("db.name", "ats_dev")
        span.set_attribute("db.statement", "SELECT * FROM dev_instrument WHERE symbol = ?")
        span.set_attribute("db.operation", "select")
        span.set_attribute("db.table", "dev_instrument")

        time.sleep(0.3)  # Simulate query execution
        span.set_attribute("db.rows_affected", 150)
        logger.info("🗄️ Database query completed")

def main():
    """Main test function"""
    print("🚀 Starting SigNoz Integration Test for ATS Platform")
    print("=" * 60)

    # Setup tracing
    tracer = setup_tracing()

    # Run test operations
    for i in range(3):
        print(f"\n📋 Running test iteration {i+1}/3")

        simulate_financial_operations(tracer)
        test_database_operations(tracer)

        time.sleep(1)  # Brief pause between iterations

    # Force flush spans
    trace.get_tracer_provider().force_flush(timeout_millis=5000)

    print("\n" + "=" * 60)
    print("✅ Test completed! Check SigNoz dashboard for traces:")
    print("🔗 SigNoz Dashboard: http://localhost:8080")
    print("📊 Go to: Services -> ats-test-service -> Traces")
    print("=" * 60)

if __name__ == "__main__":
    main()