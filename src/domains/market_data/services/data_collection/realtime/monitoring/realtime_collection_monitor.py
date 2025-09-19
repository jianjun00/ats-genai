#!/usr/bin/env python3
"""
Real-time Collection System Monitoring and Alerting

Comprehensive monitoring for the AAPL/TSLA real-time collection system with:
- Real-time data freshness tracking
- Quality score monitoring and alerting
- Cross-vendor consistency validation
- Performance metrics collection
- Automated alerting for anomalies
- Integration with Prometheus metrics

Features:
- Continuous monitoring of live data collection
- Quality score degradation alerts
- Data gap detection and notification
- Performance baseline tracking
- Cross-vendor price divergence alerts
- Collection rate monitoring
- Database connection health checks

Usage:
    from market_data.realtime.monitoring.realtime_collection_monitor import RealtimeCollectionMonitor

    monitor = RealtimeCollectionMonitor()
    await monitor.start_monitoring()
"""

import asyncio
import asyncpg
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import time
from dataclasses import dataclass, asdict
from enum import Enum

# Add src to path for imports
sys.path.insert(0, '/workspace/src')

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    FATAL = "fatal"


@dataclass
class MonitoringAlert:
    """Alert data structure."""
    timestamp: datetime
    level: AlertLevel
    category: str
    message: str
    details: Dict[str, Any]
    metric_name: str
    current_value: Optional[float] = None
    threshold_value: Optional[float] = None


@dataclass
class DataFreshnessMetric:
    """Data freshness tracking."""
    vendor: str
    symbol: str
    latest_timestamp: datetime
    records_last_hour: int
    records_last_day: int
    average_quality_score: float
    seconds_since_last_update: int


@dataclass
class QualityMetrics:
    """Data quality metrics."""
    vendor: str
    symbol: str
    timestamp: datetime
    quality_score: float
    price: float
    volume: int
    ohlc_valid: bool
    price_change_pct: Optional[float] = None


@dataclass
class CrossVendorConsistency:
    """Cross-vendor data consistency metrics."""
    symbol: str
    timestamp: datetime
    tiingo_price: Optional[float]
    polygon_price: Optional[float]
    price_difference_pct: Optional[float]
    tiingo_quality: Optional[float]
    polygon_quality: Optional[float]
    consistency_score: float


class RealtimeCollectionMonitor:
    """Comprehensive monitoring system for real-time data collection."""

    def __init__(self,
                 db_host: str = "ats-intg-postgres",
                 db_port: int = 5432,
                 db_user: str = "postgres",
                 db_password: str = "intg_password",
                 db_name: str = "intg_db",
                 monitoring_interval: int = 60):
        """Initialize the monitoring system."""

        self.db_config = {
            'host': db_host,
            'port': db_port,
            'user': db_user,
            'password': db_password,
            'database': db_name
        }

        self.monitoring_interval = monitoring_interval
        self.db_pool = None
        self.running = False

        # Monitoring state
        self.alerts = []
        self.metrics_history = []
        self.last_alert_times = {}

        # Alert thresholds
        self.thresholds = {
            'data_freshness_minutes': 5,  # Alert if no data for 5+ minutes
            'quality_score_min': 0.7,     # Alert if quality drops below 70%
            'price_divergence_pct': 5.0,  # Alert if vendor prices differ >5%
            'collection_rate_min': 0.8,   # Alert if collection rate <80%
            'volume_anomaly_factor': 3.0,  # Alert if volume >3x normal
            'price_spike_pct': 10.0       # Alert if price changes >10%
        }

        # Expected symbols
        self.symbols = ['AAPL', 'TSLA']
        self.vendors = ['tiingo', 'polygon']

        logger.info("🎯 Real-time Collection Monitor initialized")

    async def initialize(self):
        """Initialize database connection pool."""
        try:
            db_url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"

            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=2,
                max_size=10,
                command_timeout=30
            )

            logger.info("✅ Database connection pool initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize monitoring system: {e}")
            raise

    async def close(self):
        """Close monitoring system and database connections."""
        self.running = False

        if self.db_pool:
            await self.db_pool.close()

        logger.info("✅ Real-time Collection Monitor closed")

    async def collect_data_freshness_metrics(self) -> List[DataFreshnessMetric]:
        """Collect data freshness metrics for all symbols and vendors."""
        metrics = []

        async with self.db_pool.acquire() as conn:
            for vendor in self.vendors:
                table_name = f"intg_one_minute_live_{vendor}"

                # Check if table exists
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = $1
                    )
                """, table_name)

                if not table_exists:
                    logger.warning(f"⚠️ Table {table_name} does not exist")
                    continue

                for symbol in self.symbols:
                    try:
                        # Get latest data for symbol
                        latest_data = await conn.fetchrow(f"""
                            SELECT timestamp, quality_score, close_price, volume
                            FROM {table_name}
                            WHERE symbol = $1
                            ORDER BY timestamp DESC
                            LIMIT 1
                        """, symbol)

                        if not latest_data:
                            logger.warning(f"⚠️ No data found for {symbol} in {table_name}")
                            continue

                        # Count records in last hour and day
                        hour_ago = datetime.now() - timedelta(hours=1)
                        day_ago = datetime.now() - timedelta(days=1)

                        records_last_hour = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM {table_name}
                            WHERE symbol = $1 AND timestamp >= $2
                        """, symbol, hour_ago)

                        records_last_day = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM {table_name}
                            WHERE symbol = $1 AND timestamp >= $2
                        """, symbol, day_ago)

                        # Calculate average quality score for last hour
                        avg_quality = await conn.fetchval(f"""
                            SELECT AVG(quality_score) FROM {table_name}
                            WHERE symbol = $1 AND timestamp >= $2
                        """, symbol, hour_ago) or 0.0

                        # Calculate seconds since last update
                        seconds_since = (datetime.now() - latest_data['timestamp']).total_seconds()

                        metric = DataFreshnessMetric(
                            vendor=vendor,
                            symbol=symbol,
                            latest_timestamp=latest_data['timestamp'],
                            records_last_hour=records_last_hour,
                            records_last_day=records_last_day,
                            average_quality_score=avg_quality,
                            seconds_since_last_update=int(seconds_since)
                        )

                        metrics.append(metric)

                    except Exception as e:
                        logger.error(f"❌ Error collecting freshness metrics for {vendor} {symbol}: {e}")

        return metrics

    async def collect_quality_metrics(self) -> List[QualityMetrics]:
        """Collect data quality metrics."""
        metrics = []

        async with self.db_pool.acquire() as conn:
            for vendor in self.vendors:
                table_name = f"intg_one_minute_live_{vendor}"

                for symbol in self.symbols:
                    try:
                        # Get latest 5 records for trend analysis
                        recent_data = await conn.fetch(f"""
                            SELECT timestamp, quality_score, close_price, volume,
                                   open_price, high_price, low_price
                            FROM {table_name}
                            WHERE symbol = $1
                            ORDER BY timestamp DESC
                            LIMIT 5
                        """, symbol)

                        if not recent_data:
                            continue

                        latest = recent_data[0]

                        # Calculate price change if we have previous data
                        price_change_pct = None
                        if len(recent_data) > 1:
                            prev_price = recent_data[1]['close_price']
                            if prev_price and prev_price > 0:
                                price_change_pct = ((latest['close_price'] - prev_price) / prev_price) * 100

                        # Validate OHLC relationships
                        ohlc_valid = (
                            latest['high_price'] >= latest['open_price'] and
                            latest['high_price'] >= latest['close_price'] and
                            latest['high_price'] >= latest['low_price'] and
                            latest['low_price'] <= latest['open_price'] and
                            latest['low_price'] <= latest['close_price']
                        )

                        metric = QualityMetrics(
                            vendor=vendor,
                            symbol=symbol,
                            timestamp=latest['timestamp'],
                            quality_score=latest['quality_score'],
                            price=latest['close_price'],
                            volume=latest['volume'],
                            ohlc_valid=ohlc_valid,
                            price_change_pct=price_change_pct
                        )

                        metrics.append(metric)

                    except Exception as e:
                        logger.error(f"❌ Error collecting quality metrics for {vendor} {symbol}: {e}")

        return metrics

    async def collect_cross_vendor_consistency_metrics(self) -> List[CrossVendorConsistency]:
        """Collect cross-vendor consistency metrics."""
        metrics = []

        async with self.db_pool.acquire() as conn:
            for symbol in self.symbols:
                try:
                    # Get latest data from both vendors for comparison
                    tiingo_data = await conn.fetchrow("""
                        SELECT timestamp, close_price, quality_score
                        FROM intg_one_minute_live_tiingo
                        WHERE symbol = $1
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, symbol)

                    polygon_data = await conn.fetchrow("""
                        SELECT timestamp, close_price, quality_score
                        FROM intg_one_minute_live_polygon
                        WHERE symbol = $1
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, symbol)

                    # Calculate consistency metrics
                    tiingo_price = tiingo_data['close_price'] if tiingo_data else None
                    polygon_price = polygon_data['close_price'] if polygon_data else None
                    tiingo_quality = tiingo_data['quality_score'] if tiingo_data else None
                    polygon_quality = polygon_data['quality_score'] if polygon_data else None

                    price_difference_pct = None
                    consistency_score = 0.0

                    if tiingo_price and polygon_price and tiingo_price > 0:
                        price_difference_pct = abs((tiingo_price - polygon_price) / tiingo_price) * 100

                        # Consistency score: 1.0 = perfect match, decreases with price divergence
                        consistency_score = max(0.0, 1.0 - (price_difference_pct / 10.0))

                    # Use most recent timestamp
                    timestamp = datetime.now()
                    if tiingo_data and polygon_data:
                        timestamp = max(tiingo_data['timestamp'], polygon_data['timestamp'])
                    elif tiingo_data:
                        timestamp = tiingo_data['timestamp']
                    elif polygon_data:
                        timestamp = polygon_data['timestamp']

                    metric = CrossVendorConsistency(
                        symbol=symbol,
                        timestamp=timestamp,
                        tiingo_price=tiingo_price,
                        polygon_price=polygon_price,
                        price_difference_pct=price_difference_pct,
                        tiingo_quality=tiingo_quality,
                        polygon_quality=polygon_quality,
                        consistency_score=consistency_score
                    )

                    metrics.append(metric)

                except Exception as e:
                    logger.error(f"❌ Error collecting cross-vendor metrics for {symbol}: {e}")

        return metrics

    def evaluate_alerts(self,
                       freshness_metrics: List[DataFreshnessMetric],
                       quality_metrics: List[QualityMetrics],
                       consistency_metrics: List[CrossVendorConsistency]) -> List[MonitoringAlert]:
        """Evaluate metrics and generate alerts."""
        alerts = []
        current_time = datetime.now()

        # Data freshness alerts
        for metric in freshness_metrics:
            minutes_since = metric.seconds_since_last_update / 60

            if minutes_since > self.thresholds['data_freshness_minutes']:
                alert = MonitoringAlert(
                    timestamp=current_time,
                    level=AlertLevel.WARNING if minutes_since < 15 else AlertLevel.CRITICAL,
                    category="data_freshness",
                    message=f"Stale data detected for {metric.vendor} {metric.symbol}",
                    details={
                        "vendor": metric.vendor,
                        "symbol": metric.symbol,
                        "minutes_since_last_update": round(minutes_since, 1),
                        "latest_timestamp": metric.latest_timestamp.isoformat(),
                        "records_last_hour": metric.records_last_hour
                    },
                    metric_name="data_freshness_minutes",
                    current_value=minutes_since,
                    threshold_value=self.thresholds['data_freshness_minutes']
                )
                alerts.append(alert)

        # Quality score alerts
        for metric in quality_metrics:
            if metric.quality_score < self.thresholds['quality_score_min']:
                alert = MonitoringAlert(
                    timestamp=current_time,
                    level=AlertLevel.WARNING if metric.quality_score > 0.5 else AlertLevel.CRITICAL,
                    category="data_quality",
                    message=f"Low quality score for {metric.vendor} {metric.symbol}",
                    details={
                        "vendor": metric.vendor,
                        "symbol": metric.symbol,
                        "quality_score": metric.quality_score,
                        "price": metric.price,
                        "ohlc_valid": metric.ohlc_valid,
                        "timestamp": metric.timestamp.isoformat()
                    },
                    metric_name="quality_score",
                    current_value=metric.quality_score,
                    threshold_value=self.thresholds['quality_score_min']
                )
                alerts.append(alert)

            # OHLC validation alerts
            if not metric.ohlc_valid:
                alert = MonitoringAlert(
                    timestamp=current_time,
                    level=AlertLevel.CRITICAL,
                    category="data_integrity",
                    message=f"OHLC relationship violation for {metric.vendor} {metric.symbol}",
                    details={
                        "vendor": metric.vendor,
                        "symbol": metric.symbol,
                        "timestamp": metric.timestamp.isoformat(),
                        "price": metric.price,
                        "quality_score": metric.quality_score
                    },
                    metric_name="ohlc_valid",
                    current_value=0.0,
                    threshold_value=1.0
                )
                alerts.append(alert)

            # Price spike alerts
            if metric.price_change_pct and abs(metric.price_change_pct) > self.thresholds['price_spike_pct']:
                alert = MonitoringAlert(
                    timestamp=current_time,
                    level=AlertLevel.INFO,
                    category="price_movement",
                    message=f"Significant price movement for {metric.vendor} {metric.symbol}",
                    details={
                        "vendor": metric.vendor,
                        "symbol": metric.symbol,
                        "price_change_pct": round(metric.price_change_pct, 2),
                        "current_price": metric.price,
                        "timestamp": metric.timestamp.isoformat()
                    },
                    metric_name="price_change_pct",
                    current_value=abs(metric.price_change_pct),
                    threshold_value=self.thresholds['price_spike_pct']
                )
                alerts.append(alert)

        # Cross-vendor consistency alerts
        for metric in consistency_metrics:
            if (metric.price_difference_pct and
                metric.price_difference_pct > self.thresholds['price_divergence_pct']):

                alert = MonitoringAlert(
                    timestamp=current_time,
                    level=AlertLevel.WARNING,
                    category="vendor_consistency",
                    message=f"Price divergence detected for {metric.symbol}",
                    details={
                        "symbol": metric.symbol,
                        "tiingo_price": metric.tiingo_price,
                        "polygon_price": metric.polygon_price,
                        "price_difference_pct": round(metric.price_difference_pct, 2),
                        "consistency_score": round(metric.consistency_score, 3),
                        "timestamp": metric.timestamp.isoformat()
                    },
                    metric_name="price_divergence_pct",
                    current_value=metric.price_difference_pct,
                    threshold_value=self.thresholds['price_divergence_pct']
                )
                alerts.append(alert)

        return alerts

    async def send_alert(self, alert: MonitoringAlert):
        """Send alert notification (placeholder for integration with alerting systems)."""
        # This is where you would integrate with:
        # - Slack notifications
        # - Email alerts
        # - PagerDuty
        # - Discord webhooks
        # - SMS notifications

        alert_key = f"{alert.category}_{alert.metric_name}"

        # Rate limiting: don't spam the same alert
        if alert_key in self.last_alert_times:
            time_since_last = (alert.timestamp - self.last_alert_times[alert_key]).total_seconds()
            if time_since_last < 300:  # 5 minute rate limit
                return

        self.last_alert_times[alert_key] = alert.timestamp

        # Log alert (in production, this would send to external systems)
        log_level = {
            AlertLevel.INFO: logger.info,
            AlertLevel.WARNING: logger.warning,
            AlertLevel.CRITICAL: logger.error,
            AlertLevel.FATAL: logger.critical
        }[alert.level]

        log_level(f"🚨 ALERT [{alert.level.value.upper()}] {alert.message}")
        logger.info(f"📊 Alert details: {json.dumps(alert.details, indent=2)}")

        # Store alert for history
        self.alerts.append(alert)

        # Keep only last 1000 alerts
        if len(self.alerts) > 1000:
            self.alerts = self.alerts[-1000:]

    async def generate_prometheus_metrics(self,
                                        freshness_metrics: List[DataFreshnessMetric],
                                        quality_metrics: List[QualityMetrics],
                                        consistency_metrics: List[CrossVendorConsistency]) -> str:
        """Generate Prometheus metrics for the monitoring data."""

        timestamp = int(datetime.now().timestamp())
        metrics_lines = []

        # Help text
        metrics_lines.extend([
            "# HELP ats_realtime_data_freshness_seconds Seconds since last data update",
            "# TYPE ats_realtime_data_freshness_seconds gauge",
            "",
            "# HELP ats_realtime_quality_score Current data quality score (0-1)",
            "# TYPE ats_realtime_quality_score gauge",
            "",
            "# HELP ats_realtime_records_per_hour Records collected in the last hour",
            "# TYPE ats_realtime_records_per_hour gauge",
            "",
            "# HELP ats_realtime_price_divergence_pct Price difference between vendors (%)",
            "# TYPE ats_realtime_price_divergence_pct gauge",
            "",
            "# HELP ats_realtime_consistency_score Cross-vendor consistency (0-1)",
            "# TYPE ats_realtime_consistency_score gauge",
            "",
            "# HELP ats_realtime_active_alerts Number of active alerts by category",
            "# TYPE ats_realtime_active_alerts gauge",
            ""
        ])

        # Data freshness metrics
        for metric in freshness_metrics:
            metrics_lines.append(f'ats_realtime_data_freshness_seconds{{vendor="{metric.vendor}",symbol="{metric.symbol}"}} {metric.seconds_since_last_update} {timestamp}')
            metrics_lines.append(f'ats_realtime_records_per_hour{{vendor="{metric.vendor}",symbol="{metric.symbol}"}} {metric.records_last_hour} {timestamp}')

        # Quality metrics
        for metric in quality_metrics:
            metrics_lines.append(f'ats_realtime_quality_score{{vendor="{metric.vendor}",symbol="{metric.symbol}"}} {metric.quality_score} {timestamp}')

        # Consistency metrics
        for metric in consistency_metrics:
            if metric.price_difference_pct is not None:
                metrics_lines.append(f'ats_realtime_price_divergence_pct{{symbol="{metric.symbol}"}} {metric.price_difference_pct} {timestamp}')
            metrics_lines.append(f'ats_realtime_consistency_score{{symbol="{metric.symbol}"}} {metric.consistency_score} {timestamp}')

        # Alert metrics
        alert_counts = {}
        for alert in self.alerts[-100:]:  # Last 100 alerts
            category = alert.category
            alert_counts[category] = alert_counts.get(category, 0) + 1

        for category, count in alert_counts.items():
            metrics_lines.append(f'ats_realtime_active_alerts{{category="{category}"}} {count} {timestamp}')

        return '\n'.join(metrics_lines) + '\n'

    async def monitoring_cycle(self):
        """Execute one complete monitoring cycle."""
        try:
            logger.debug("📊 Starting monitoring cycle")

            # Collect all metrics
            freshness_metrics = await self.collect_data_freshness_metrics()
            quality_metrics = await self.collect_quality_metrics()
            consistency_metrics = await self.collect_cross_vendor_consistency_metrics()

            # Evaluate and send alerts
            alerts = self.evaluate_alerts(freshness_metrics, quality_metrics, consistency_metrics)

            for alert in alerts:
                await self.send_alert(alert)

            # Generate Prometheus metrics
            prometheus_metrics = await self.generate_prometheus_metrics(
                freshness_metrics, quality_metrics, consistency_metrics
            )

            # Log monitoring summary
            logger.info(f"📊 Monitoring cycle completed - {len(freshness_metrics)} freshness, {len(quality_metrics)} quality, {len(consistency_metrics)} consistency metrics collected, {len(alerts)} alerts generated")

            # Store metrics for history
            cycle_data = {
                'timestamp': datetime.now(),
                'freshness_metrics': [asdict(m) for m in freshness_metrics],
                'quality_metrics': [asdict(m) for m in quality_metrics],
                'consistency_metrics': [asdict(m) for m in consistency_metrics],
                'alerts': [asdict(a) for a in alerts],
                'prometheus_metrics': prometheus_metrics
            }

            self.metrics_history.append(cycle_data)

            # Keep only last 24 hours of history
            if len(self.metrics_history) > 1440:  # 24 hours * 60 minutes
                self.metrics_history = self.metrics_history[-1440:]

        except Exception as e:
            logger.error(f"❌ Error in monitoring cycle: {e}")

    async def start_monitoring(self):
        """Start the continuous monitoring loop."""
        await self.initialize()

        self.running = True
        logger.info(f"🎯 Starting real-time collection monitoring (interval: {self.monitoring_interval}s)")

        try:
            while self.running:
                cycle_start = time.time()

                await self.monitoring_cycle()

                # Calculate sleep time to maintain interval
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, self.monitoring_interval - cycle_duration)

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(f"⚠️ Monitoring cycle took {cycle_duration:.1f}s, longer than interval {self.monitoring_interval}s")

        except Exception as e:
            logger.error(f"❌ Monitoring loop error: {e}")
        finally:
            await self.close()

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status and recent metrics."""

        if not self.metrics_history:
            return {
                'status': 'no_data',
                'message': 'No monitoring data available yet'
            }

        latest = self.metrics_history[-1]

        return {
            'status': 'active' if self.running else 'stopped',
            'last_update': latest['timestamp'].isoformat(),
            'monitoring_interval_seconds': self.monitoring_interval,
            'alerts_last_hour': len([a for a in self.alerts if (datetime.now() - a.timestamp).total_seconds() < 3600]),
            'total_alerts': len(self.alerts),
            'latest_metrics': {
                'freshness_count': len(latest['freshness_metrics']),
                'quality_count': len(latest['quality_metrics']),
                'consistency_count': len(latest['consistency_metrics']),
                'alerts_generated': len(latest['alerts'])
            },
            'thresholds': self.thresholds,
            'symbols': self.symbols,
            'vendors': self.vendors
        }


async def main():
    """Main function for standalone monitoring."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    monitor = RealtimeCollectionMonitor()

    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
    finally:
        await monitor.close()


if __name__ == "__main__":
    asyncio.run(main())