#!/usr/bin/env python3
"""
ATS-INTG Comprehensive Monitoring Setup
Ultra-comprehensive monitoring for integration environment
"""

import time
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class INTGMonitoringConfig:
    """Configuration for ATS-INTG monitoring"""
    signoz_endpoint: str = "http://localhost:4318"
    api_key: str = "9RbijHam3W4B0a8h5fFB+7NgUgmXV+hFnzIPQUqtc6M="
    environment: str = "intg"
    retention_days: int = 90
    sampling_rate: float = 1.0  # 100% sampling for integration

class ATSINTGMonitor:
    """Comprehensive ATS Integration Environment Monitor"""
    
    def __init__(self, config: INTGMonitoringConfig):
        self.config = config
        self.setup_telemetry()
        self.tracer = trace.get_tracer(__name__)
        self.meter = metrics.get_meter(__name__)
        self.setup_custom_metrics()
        
    def setup_telemetry(self):
        """Setup OpenTelemetry for ATS-INTG"""
        
        # Resource with ATS-INTG specific attributes
        resource = Resource.create({
            SERVICE_NAME: "ats-intg-monitor",
            SERVICE_VERSION: "1.0.0",
            "environment": self.config.environment,
            "ats.tier": "integration",
            "ats.criticality": "high",
            "deployment.environment": "integration"
        })
        
        # Setup tracing
        trace.set_tracer_provider(TracerProvider(resource=resource))
        
        otlp_trace_exporter = OTLPSpanExporter(
            endpoint=f"{self.config.signoz_endpoint}/v1/traces",
            headers={"signoz-access-token": self.config.api_key}
        )
        
        span_processor = BatchSpanProcessor(otlp_trace_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)
        
        # Setup metrics
        otlp_metric_exporter = OTLPMetricExporter(
            endpoint=f"{self.config.signoz_endpoint}/v1/metrics", 
            headers={"signoz-access-token": self.config.api_key}
        )
        
        metric_reader = PeriodicExportingMetricReader(
            exporter=otlp_metric_exporter,
            export_interval_millis=15000  # Export every 15 seconds
        )
        
        metrics.set_meter_provider(MeterProvider(
            resource=resource,
            metric_readers=[metric_reader]
        ))
        
        logger.info("✅ ATS-INTG telemetry configured")
    
    def setup_custom_metrics(self):
        """Setup ATS-INTG specific metrics"""
        
        # Integration Testing Metrics
        self.integration_test_counter = self.meter.create_counter(
            "ats_intg_tests_total",
            description="Total integration tests executed"
        )
        
        self.integration_test_success_counter = self.meter.create_counter(
            "ats_intg_tests_passed_total", 
            description="Integration tests passed"
        )
        
        self.integration_test_duration = self.meter.create_histogram(
            "ats_intg_test_duration_ms",
            description="Integration test execution duration",
            unit="ms"
        )
        
        # Data Pipeline Health Metrics
        self.data_freshness_gauge = self.meter.create_gauge(
            "ats_intg_data_freshness_minutes",
            description="Data age in minutes"
        )
        
        self.data_quality_score = self.meter.create_gauge(
            "ats_intg_data_quality_score", 
            description="Data quality score 0-1"
        )
        
        self.pipeline_success_rate = self.meter.create_gauge(
            "ats_intg_pipeline_success_rate",
            description="Data pipeline success rate"
        )
        
        # External API Health Metrics
        self.external_api_response_time = self.meter.create_histogram(
            "ats_intg_external_api_duration_ms",
            description="External API response time",
            unit="ms" 
        )
        
        self.external_api_success_rate = self.meter.create_gauge(
            "ats_intg_external_api_success_rate",
            description="External API success rate"
        )
        
        # Database Performance Metrics
        self.db_connection_pool_usage = self.meter.create_gauge(
            "ats_intg_db_connections_active",
            description="Active database connections"
        )
        
        self.db_query_duration = self.meter.create_histogram(
            "ats_intg_db_query_duration_ms", 
            description="Database query duration",
            unit="ms"
        )
        
        # ML Pipeline Metrics
        self.ml_training_duration = self.meter.create_histogram(
            "ats_intg_ml_training_duration_ms",
            description="ML training pipeline duration",
            unit="ms"
        )
        
        self.feature_completeness = self.meter.create_gauge(
            "ats_intg_feature_completeness_percent",
            description="Feature completeness percentage"
        )
        
        logger.info("📊 ATS-INTG custom metrics initialized")
    
    def monitor_integration_tests(self, test_suite: str, test_results: Dict):
        """Monitor integration test execution"""
        
        with self.tracer.start_as_current_span("integration_test_suite") as span:
            span.set_attribute("test.suite", test_suite)
            span.set_attribute("test.environment", "intg")
            
            total_tests = test_results.get("total", 0)
            passed_tests = test_results.get("passed", 0)
            duration_ms = test_results.get("duration_ms", 0)
            
            # Record metrics
            self.integration_test_counter.add(
                total_tests, 
                {"test_suite": test_suite, "environment": "intg"}
            )
            
            self.integration_test_success_counter.add(
                passed_tests,
                {"test_suite": test_suite, "environment": "intg"} 
            )
            
            self.integration_test_duration.record(
                duration_ms,
                {"test_suite": test_suite, "environment": "intg"}
            )
            
            span.set_attribute("test.total", total_tests)
            span.set_attribute("test.passed", passed_tests)
            span.set_attribute("test.success_rate", passed_tests / total_tests if total_tests > 0 else 0)
            
            if passed_tests < total_tests:
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Some tests failed"))
                
            logger.info(f"🧪 Integration tests - {test_suite}: {passed_tests}/{total_tests} passed")
    
    def monitor_data_pipeline_health(self, vendor: str, pipeline_data: Dict):
        """Monitor data pipeline health and quality"""
        
        with self.tracer.start_as_current_span("data_pipeline_health") as span:
            span.set_attribute("data.vendor", vendor)
            span.set_attribute("data.environment", "intg")
            
            freshness_minutes = pipeline_data.get("freshness_minutes", 0)
            quality_score = pipeline_data.get("quality_score", 0.0)
            success_rate = pipeline_data.get("success_rate", 0.0)
            records_processed = pipeline_data.get("records_processed", 0)
            
            # Record metrics
            self.data_freshness_gauge.set(
                freshness_minutes,
                {"vendor": vendor, "environment": "intg"}
            )
            
            self.data_quality_score.set(
                quality_score,
                {"vendor": vendor, "environment": "intg"}
            )
            
            self.pipeline_success_rate.set(
                success_rate, 
                {"vendor": vendor, "environment": "intg"}
            )
            
            span.set_attribute("data.freshness_minutes", freshness_minutes)
            span.set_attribute("data.quality_score", quality_score)
            span.set_attribute("data.success_rate", success_rate)
            span.set_attribute("data.records_processed", records_processed)
            
            # Alert conditions
            if freshness_minutes > 120:  # 2 hours
                span.record_exception(Exception(f"Data freshness alert: {freshness_minutes} minutes"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Data too stale"))
                
            if quality_score < 0.95:
                span.record_exception(Exception(f"Data quality alert: {quality_score}"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Data quality below threshold"))
                
            logger.info(f"📊 Data pipeline - {vendor}: quality={quality_score:.3f}, freshness={freshness_minutes}min")
    
    def monitor_external_api_health(self, api_name: str, api_metrics: Dict):
        """Monitor external API health and performance"""
        
        with self.tracer.start_as_current_span("external_api_health") as span:
            span.set_attribute("api.name", api_name)
            span.set_attribute("api.environment", "intg")
            
            response_time_ms = api_metrics.get("response_time_ms", 0)
            success_rate = api_metrics.get("success_rate", 0.0)
            error_count = api_metrics.get("error_count", 0)
            
            # Record metrics
            self.external_api_response_time.record(
                response_time_ms,
                {"api_name": api_name, "environment": "intg"}
            )
            
            self.external_api_success_rate.set(
                success_rate,
                {"api_name": api_name, "environment": "intg"}
            )
            
            span.set_attribute("api.response_time_ms", response_time_ms)
            span.set_attribute("api.success_rate", success_rate)
            span.set_attribute("api.error_count", error_count)
            
            # Performance and reliability checks
            if response_time_ms > 5000:  # 5 seconds
                span.record_exception(Exception(f"API performance alert: {response_time_ms}ms"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "API response time too high"))
                
            if success_rate < 0.95:
                span.record_exception(Exception(f"API reliability alert: {success_rate}"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "API success rate too low"))
                
            logger.info(f"🌐 External API - {api_name}: {response_time_ms}ms, success={success_rate:.3f}")
    
    def monitor_database_performance(self, db_metrics: Dict):
        """Monitor integration database performance"""
        
        with self.tracer.start_as_current_span("database_performance") as span:
            span.set_attribute("db.system", "postgresql")
            span.set_attribute("db.name", "intg_db")
            span.set_attribute("db.environment", "intg")
            
            active_connections = db_metrics.get("active_connections", 0)
            avg_query_time_ms = db_metrics.get("avg_query_time_ms", 0)
            slow_queries_count = db_metrics.get("slow_queries_count", 0)
            
            # Record metrics
            self.db_connection_pool_usage.set(
                active_connections,
                {"db_name": "intg_db", "environment": "intg"}
            )
            
            self.db_query_duration.record(
                avg_query_time_ms,
                {"db_name": "intg_db", "environment": "intg"}
            )
            
            span.set_attribute("db.active_connections", active_connections)
            span.set_attribute("db.avg_query_time_ms", avg_query_time_ms)
            span.set_attribute("db.slow_queries_count", slow_queries_count)
            
            # Performance alerts
            if active_connections > 90:  # Near connection limit
                span.record_exception(Exception(f"DB connection pool alert: {active_connections}"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Connection pool near exhaustion"))
                
            if avg_query_time_ms > 1000:  # 1 second
                span.record_exception(Exception(f"DB performance alert: {avg_query_time_ms}ms"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Database queries too slow"))
                
            logger.info(f"🗄️ Database - intg_db: {active_connections} conn, {avg_query_time_ms}ms avg")
    
    def monitor_ml_pipeline_integration(self, ml_metrics: Dict):
        """Monitor ML pipeline integration performance"""
        
        with self.tracer.start_as_current_span("ml_pipeline_integration") as span:
            span.set_attribute("ml.environment", "intg")
            span.set_attribute("ml.pipeline_type", "training_data_generation")
            
            training_duration_ms = ml_metrics.get("training_duration_ms", 0)
            feature_completeness = ml_metrics.get("feature_completeness", 0.0)
            model_accuracy = ml_metrics.get("model_accuracy", 0.0)
            data_drift_score = ml_metrics.get("data_drift_score", 0.0)
            
            # Record metrics
            self.ml_training_duration.record(
                training_duration_ms,
                {"pipeline_type": "training_data", "environment": "intg"}
            )
            
            self.feature_completeness.set(
                feature_completeness * 100,  # Convert to percentage
                {"environment": "intg"}
            )
            
            span.set_attribute("ml.training_duration_ms", training_duration_ms)
            span.set_attribute("ml.feature_completeness", feature_completeness)
            span.set_attribute("ml.model_accuracy", model_accuracy)
            span.set_attribute("ml.data_drift_score", data_drift_score)
            
            # ML-specific alerts
            if feature_completeness < 0.95:
                span.record_exception(Exception(f"Feature completeness alert: {feature_completeness}"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Incomplete features"))
                
            if data_drift_score > 0.3:
                span.record_exception(Exception(f"Data drift alert: {data_drift_score}"))
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Significant data drift detected"))
                
            logger.info(f"🤖 ML Pipeline: features={feature_completeness:.3f}, drift={data_drift_score:.3f}")

def simulate_ats_intg_monitoring():
    """Simulate comprehensive ATS-INTG monitoring"""
    
    config = INTGMonitoringConfig()
    monitor = ATSINTGMonitor(config)
    
    print("🚀 Starting ATS-INTG monitoring simulation...")
    print("=" * 60)
    
    # Simulate integration test monitoring
    test_results = {
        "total": 25,
        "passed": 23, 
        "duration_ms": 45000
    }
    monitor.monitor_integration_tests("service_integration_tests", test_results)
    
    # Simulate data pipeline monitoring
    pipeline_data = {
        "freshness_minutes": 15,
        "quality_score": 0.98,
        "success_rate": 0.97,
        "records_processed": 150000
    }
    monitor.monitor_data_pipeline_health("firstrate", pipeline_data)
    
    # Simulate external API monitoring
    api_metrics = {
        "response_time_ms": 850,
        "success_rate": 0.995,
        "error_count": 2
    }
    monitor.monitor_external_api_health("tiingo_api", api_metrics)
    
    # Simulate database performance monitoring
    db_metrics = {
        "active_connections": 45,
        "avg_query_time_ms": 120,
        "slow_queries_count": 3
    }
    monitor.monitor_database_performance(db_metrics)
    
    # Simulate ML pipeline monitoring
    ml_metrics = {
        "training_duration_ms": 180000,
        "feature_completeness": 0.96,
        "model_accuracy": 0.94,
        "data_drift_score": 0.15
    }
    monitor.monitor_ml_pipeline_integration(ml_metrics)
    
    # Force flush all telemetry
    trace.get_tracer_provider().force_flush(timeout_millis=5000)
    
    print("\n" + "=" * 60)
    print("✅ ATS-INTG monitoring simulation completed!")
    print("🔗 Check SigNoz dashboard: http://localhost:8080")
    print("📊 Look for 'ats-intg-monitor' service traces and metrics")
    print("=" * 60)

if __name__ == "__main__":
    simulate_ats_intg_monitoring()