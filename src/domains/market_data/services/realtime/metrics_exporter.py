#!/usr/bin/env python3
"""
Prometheus Metrics Exporter for Real-Time Data Collection

Exports comprehensive metrics about real-time data collection performance,
quality, and system health for monitoring and alerting.
"""

import asyncio
import asyncpg
import logging
import os
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Counter, Histogram, Info
import time

from shared.utils.environment import Environment

logger = logging.getLogger(__name__)

class HealthcheckHandler:
    """Simple health check handler for tests"""
    
    def __init__(self):
        pass
    
    async def health_check(self, request):
        """Health check endpoint"""
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        }

class RealTimeMetricsExporter:
    """
    Prometheus metrics exporter for real-time data collection system.
    """
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        self.running = False
        
        # Configuration
        self.metrics_port = int(os.getenv('METRICS_PORT', '9090'))
        self.collection_interval = int(os.getenv('METRICS_COLLECTION_INTERVAL', '30'))
        
        # Initialize Prometheus metrics
        self._init_prometheus_metrics()
        
    def _init_prometheus_metrics(self):
        """Initialize all Prometheus metrics"""
        
        # System Info
        self.system_info = Info('realtime_collector_info', 'Real-time collector system information')
        self.system_info.info({
            'version': '1.0.0',
            'environment': os.getenv('ENVIRONMENT', 'dev'),
            'deployment': 'kubernetes'
        })
        
        # Data Collection Metrics
        self.bars_collected_total = Counter(
            'realtime_bars_collected_total',
            'Total number of minute bars collected',
            ['vendor', 'symbol']
        )
        
        self.collection_latency_seconds = Histogram(
            'realtime_collection_latency_seconds',
            'Data collection latency in seconds',
            ['vendor'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        )
        
        self.data_quality_score = Gauge(
            'realtime_data_quality_score',
            'Real-time data quality score (0-1)',
            ['vendor', 'symbol']
        )
        
        # Connection Health
        self.connection_status = Gauge(
            'realtime_connection_status',
            'Connection status (1=connected, 0=disconnected)',
            ['vendor']
        )
        
        self.connection_errors_total = Counter(
            'realtime_connection_errors_total',
            'Total connection errors',
            ['vendor', 'error_type']
        )
        
        # Data Gaps and Issues
        self.gaps_detected_total = Counter(
            'realtime_gaps_detected_total',
            'Total data gaps detected',
            ['vendor', 'symbol', 'severity']
        )
        
        self.missing_bars_total = Counter(
            'realtime_missing_bars_total',
            'Total missing minute bars',
            ['vendor', 'symbol']
        )
        
        self.late_bars_total = Counter(
            'realtime_late_bars_total',
            'Total late minute bars (>5min delay)',
            ['vendor', 'symbol']
        )
        
        # Collection Status
        self.active_symbols = Gauge(
            'realtime_active_symbols',
            'Number of actively collected symbols',
            ['vendor']
        )
        
        self.collection_health_score = Gauge(
            'realtime_collection_health_score',
            'Overall collection health score per vendor/symbol',
            ['vendor', 'symbol']
        )
        
        self.bars_per_hour = Gauge(
            'realtime_bars_per_hour',
            'Bars collected in the last hour',
            ['vendor']
        )
        
        # Validation Metrics
        self.validation_accuracy = Gauge(
            'realtime_validation_accuracy',
            'Validation accuracy vs batch data',
            ['vendor', 'symbol']
        )
        
        self.price_discrepancies_total = Counter(
            'realtime_price_discrepancies_total',
            'Total price discrepancies detected',
            ['vendor', 'symbol']
        )
        
        # Backfill Metrics
        self.backfill_requests_total = Counter(
            'realtime_backfill_requests_total',
            'Total backfill requests triggered',
            ['vendor', 'symbol', 'status']
        )
        
        self.backfilled_bars_total = Counter(
            'realtime_backfilled_bars_total',
            'Total bars backfilled',
            ['vendor', 'symbol']
        )
        
        # System Performance
        self.database_query_duration_seconds = Histogram(
            'realtime_database_query_duration_seconds',
            'Database query duration',
            ['operation'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
        )
        
        self.api_request_duration_seconds = Histogram(
            'realtime_api_request_duration_seconds',
            'Vendor API request duration',
            ['vendor', 'endpoint'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
        )
        
        # Market Hours Status
        self.market_hours_status = Gauge(
            'realtime_market_hours_status',
            'Market hours status (1=open, 0=closed)'
        )
        
        self.trading_day_bars_expected = Gauge(
            'realtime_trading_day_bars_expected',
            'Expected bars for current trading day',
            ['vendor']
        )
        
        self.trading_day_bars_collected = Gauge(
            'realtime_trading_day_bars_collected',
            'Bars collected for current trading day',
            ['vendor']
        )
        
    async def initialize(self):
        """Initialize metrics exporter"""
        # Start Prometheus HTTP server
        start_http_server(self.metrics_port)
        logger.info(f"📊 Prometheus metrics server started on port {self.metrics_port}")
        
        # Connect to database
        self.pool = await asyncpg.create_pool(self.env.get_database_url())
        logger.info("✅ Connected to database for metrics collection")
        
    async def start_metrics_collection(self):
        """Start continuous metrics collection"""
        logger.info("🚀 Starting real-time metrics collection")
        self.running = True
        
        try:
            while self.running:
                start_time = time.time()
                
                try:
                    # Collect all metrics
                    await self._collect_collection_metrics()
                    await self._collect_health_metrics()
                    await self._collect_gap_metrics()
                    await self._collect_validation_metrics()
                    await self._collect_backfill_metrics()
                    await self._collect_system_metrics()
                    
                except Exception as e:
                    logger.error(f"Error collecting metrics: {e}")
                    
                # Wait for next collection interval
                collection_time = time.time() - start_time
                sleep_time = max(0, self.collection_interval - collection_time)
                await asyncio.sleep(sleep_time)
                
        except Exception as e:
            logger.error(f"💥 Metrics collection failed: {e}")
            raise
        finally:
            self.running = False
            
    async def _collect_collection_metrics(self):
        """Collect data collection metrics"""
        # Get bars collected in last hour per vendor
        query = """
            SELECT 
                'polygon' as vendor,
                COUNT(*) as bar_count,
                AVG(data_latency_ms) / 1000.0 as avg_latency_seconds,
                AVG(quality_score) as avg_quality_score
            FROM dev_one_minute_live_polygon
            WHERE received_at >= now() - INTERVAL '1 hour'
            
            UNION ALL
            
            SELECT 
                'tiingo' as vendor,
                COUNT(*) as bar_count,
                AVG(data_latency_ms) / 1000.0 as avg_latency_seconds,
                AVG(quality_score) as avg_quality_score
            FROM dev_one_minute_live_tiingo
            WHERE received_at >= now() - INTERVAL '1 hour'
            
            UNION ALL
            
            SELECT 
                'fmp' as vendor,
                COUNT(*) as bar_count,
                AVG(data_latency_ms) / 1000.0 as avg_latency_seconds,
                AVG(quality_score) as avg_quality_score
            FROM dev_one_minute_live_fmp
            WHERE received_at >= now() - INTERVAL '1 hour'
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            for row in rows:
                vendor = row['vendor']
                bar_count = row['bar_count'] or 0
                avg_latency = row['avg_latency_seconds'] or 0
                row['avg_quality_score'] or 0
                
                # Update metrics
                self.bars_per_hour.labels(vendor=vendor).set(bar_count)
                
                if avg_latency > 0:
                    self.collection_latency_seconds.labels(vendor=vendor).observe(avg_latency)
                    
    async def _collect_health_metrics(self):
        """Collect connection and health metrics"""
        query = """
            SELECT 
                vendor,
                symbol,
                collection_health_score,
                CASE WHEN last_received_timestamp > now() - INTERVAL '10 minutes' THEN 1 ELSE 0 END as is_connected,
                consecutive_missing_bars,
                data_delay_minutes
            FROM dev_realtime_collection_status
            WHERE is_active = true
        """
        
        vendor_connections = {}
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            for row in rows:
                vendor = row['vendor']
                symbol = row['symbol']
                health_score = row['collection_health_score'] or 0
                is_connected = row['is_connected']
                missing_bars = row['consecutive_missing_bars'] or 0
                delay_minutes = row['data_delay_minutes'] or 0
                
                # Update health metrics
                self.collection_health_score.labels(vendor=vendor, symbol=symbol).set(health_score)
                
                # Track vendor connection status
                if vendor not in vendor_connections:
                    vendor_connections[vendor] = []
                vendor_connections[vendor].append(is_connected)
                
                # Track missing and late bars
                if missing_bars > 0:
                    self.missing_bars_total.labels(vendor=vendor, symbol=symbol).inc(missing_bars)
                    
                if delay_minutes > 5:
                    self.late_bars_total.labels(vendor=vendor, symbol=symbol).inc()
                    
        # Set vendor connection status (connected if any symbol is connected)
        for vendor, connections in vendor_connections.items():
            connection_status = 1 if any(connections) else 0
            self.connection_status.labels(vendor=vendor).set(connection_status)
            
        # Count active symbols per vendor
        symbol_counts = {}
        for row in rows:
            vendor = row['vendor']
            symbol_counts[vendor] = symbol_counts.get(vendor, 0) + 1
            
        for vendor, count in symbol_counts.items():
            self.active_symbols.labels(vendor=vendor).set(count)
            
    async def _collect_gap_metrics(self):
        """Collect gap detection metrics"""
        query = """
            SELECT 
                vendor,
                symbol,
                gap_severity,
                gap_type,
                COUNT(*) as gap_count,
                SUM(missing_bars_count) as total_missing_bars
            FROM dev_realtime_gaps
            WHERE detected_at >= now() - INTERVAL '1 hour'
              AND backfill_status != 'completed'
            GROUP BY vendor, symbol, gap_severity, gap_type
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            for row in rows:
                vendor = row['vendor']
                symbol = row['symbol']
                severity = row['gap_severity']
                row['gap_type']
                gap_count = row['gap_count']
                missing_bars = row['total_missing_bars']
                
                # Update gap metrics
                self.gaps_detected_total.labels(
                    vendor=vendor, symbol=symbol, severity=severity
                ).inc(gap_count)
                
                self.missing_bars_total.labels(
                    vendor=vendor, symbol=symbol
                ).inc(missing_bars)
                
    async def _collect_validation_metrics(self):
        """Collect validation accuracy metrics"""
        query = """
            SELECT 
                vendor,
                symbol,
                overall_accuracy_score,
                discrepant_prices,
                avg_price_difference
            FROM dev_realtime_batch_validation
            WHERE validation_date >= CURRENT_DATE - INTERVAL '7 days'
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            for row in rows:
                vendor = row['vendor']
                symbol = row['symbol']
                accuracy = row['overall_accuracy_score'] or 0
                discrepant_prices = row['discrepant_prices'] or 0
                
                # Update validation metrics
                self.validation_accuracy.labels(vendor=vendor, symbol=symbol).set(accuracy)
                
                if discrepant_prices > 0:
                    self.price_discrepancies_total.labels(
                        vendor=vendor, symbol=symbol
                    ).inc(discrepant_prices)
                    
    async def _collect_backfill_metrics(self):
        """Collect backfill metrics"""
        query = """
            SELECT 
                vendor,
                symbol,
                backfill_status,
                COUNT(*) as request_count,
                SUM(COALESCE(backfilled_bars_count, 0)) as total_backfilled_bars
            FROM dev_realtime_gaps
            WHERE backfill_started_at >= now() - INTERVAL '24 hours'
            GROUP BY vendor, symbol, backfill_status
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            for row in rows:
                vendor = row['vendor']
                symbol = row['symbol']
                status = row['backfill_status']
                request_count = row['request_count']
                backfilled_bars = row['total_backfilled_bars']
                
                # Update backfill metrics
                self.backfill_requests_total.labels(
                    vendor=vendor, symbol=symbol, status=status
                ).inc(request_count)
                
                if backfilled_bars > 0:
                    self.backfilled_bars_total.labels(
                        vendor=vendor, symbol=symbol
                    ).inc(backfilled_bars)
                    
    async def _collect_system_metrics(self):
        """Collect system and market status metrics"""
        # Market hours status (simplified - would use actual market calendar)
        now = datetime.now()
        is_market_hours = 9 <= now.hour < 16 and now.weekday() < 5
        self.market_hours_status.set(1 if is_market_hours else 0)
        
        # Expected vs collected bars for today
        if is_market_hours:
            minutes_since_open = max(0, (now.hour - 9) * 60 + (now.minute - 30))
            expected_bars_per_symbol = minutes_since_open
            
            query = """
                SELECT 
                    'polygon' as vendor,
                    COUNT(DISTINCT symbol) as symbol_count,
                    COUNT(*) as bars_today
                FROM dev_one_minute_live_polygon
                WHERE DATE(timestamp) = CURRENT_DATE
                
                UNION ALL
                
                SELECT 
                    'tiingo' as vendor,
                    COUNT(DISTINCT symbol) as symbol_count,
                    COUNT(*) as bars_today
                FROM dev_one_minute_live_tiingo
                WHERE DATE(timestamp) = CURRENT_DATE
                
                UNION ALL
                
                SELECT 
                    'fmp' as vendor,
                    COUNT(DISTINCT symbol) as symbol_count,
                    COUNT(*) as bars_today
                FROM dev_one_minute_live_fmp
                WHERE DATE(timestamp) = CURRENT_DATE
            """
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                
                for row in rows:
                    vendor = row['vendor']
                    symbol_count = row['symbol_count'] or 0
                    bars_today = row['bars_today'] or 0
                    
                    expected_total = symbol_count * expected_bars_per_symbol
                    
                    self.trading_day_bars_expected.labels(vendor=vendor).set(expected_total)
                    self.trading_day_bars_collected.labels(vendor=vendor).set(bars_today)
                    
    async def shutdown(self):
        """Shutdown metrics exporter"""
        logger.info("🛑 Shutting down metrics exporter")
        self.running = False
        
        if self.pool:
            await self.pool.close()

async def main():
    """Main entry point for metrics exporter"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    exporter = RealTimeMetricsExporter()
    
    try:
        await exporter.initialize()
        await exporter.start_metrics_collection()
    except KeyboardInterrupt:
        logger.info("👋 Received shutdown signal")
    except Exception as e:
        logger.error(f"💥 Metrics exporter failed: {e}")
        raise
    finally:
        await exporter.shutdown()

# Aliases for test compatibility
RealtimeMetricsExporter = RealTimeMetricsExporter
MetricsCollector = RealTimeMetricsExporter

if __name__ == "__main__":
    asyncio.run(main())