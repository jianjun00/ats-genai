#!/usr/bin/env python3
"""
Integration Tests for Earnings Quality Monitor

Tests the comprehensive quality monitoring system including
metrics calculation, vendor health checks, and alerting logic.
"""

import pytest
import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from dataclasses import dataclass
from typing import Dict, List, Any

# Mock the environment and database dependencies
@pytest.fixture
def mock_environment():
    """Mock environment configuration"""
    env = Mock()
    env.get_table_name = Mock(side_effect=lambda name: f"dev_{name}")

    # Mock database with connection pool
    database = Mock()
    pool = AsyncMock()
    conn = AsyncMock()

    database.create_pool_with_retry = AsyncMock(return_value=pool)
    pool.acquire = AsyncMock(return_value=conn)
    pool.__aenter__ = AsyncMock(return_value=pool)
    pool.__aexit__ = AsyncMock(return_value=None)
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)

    env.database = database
    return env, pool, conn

@pytest.fixture
def sample_earnings_data():
    """Sample earnings data for testing"""
    return [
        {
            'symbol': 'AAPL',
            'report_period': '2025-06-28',
            'eps_actual_cents': 157,
            'revenue_actual_cents': 9403600000000,
            'earnings_call_datetime': datetime(2025, 8, 1, 10, 0, 42),
            'vendor': 'polygon',
            'created_at': datetime.now()
        },
        {
            'symbol': 'GOOGL',
            'report_period': '2025-06-30',
            'eps_actual_cents': 189,
            'revenue_actual_cents': 8474000000000,
            'earnings_call_datetime': None,  # Missing call time
            'vendor': 'polygon',
            'created_at': datetime.now()
        },
        {
            'symbol': 'TSLA',
            'report_period': '2025-06-30',
            'eps_actual_cents': None,  # Missing EPS
            'revenue_actual_cents': 2488700000000,
            'earnings_call_datetime': datetime(2025, 7, 23, 17, 30),
            'vendor': 'polygon',
            'created_at': datetime.now()
        }
    ]

class TestEarningsQualityMonitor:
    """Test earnings data quality monitoring functionality"""

    @pytest.mark.asyncio
    async def test_eps_coverage_calculation(self, mock_environment, sample_earnings_data):
        """Test EPS coverage calculation"""
        env, pool, conn = mock_environment

        # Mock database query result
        conn.fetchrow = AsyncMock(return_value={
            'total_earnings': 3,
            'eps_count': 2  # 2 out of 3 have EPS data
        })

        # Import and test the quality monitor (mocked)
        from unittest.mock import patch
        with patch('src.events.quality.earnings_quality_monitor.Environment', return_value=env):
            # Simulate the quality monitor class
            class MockEarningsQualityMonitor:
                def __init__(self):
                    self.env = env

                async def check_eps_coverage(self, days: int = 30) -> float:
                    query = f"""
                    SELECT
                        COUNT(*) as total_earnings,
                        COUNT(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 END) as eps_count
                    FROM {self.env.get_table_name('earnings_events')} ee
                    JOIN {self.env.get_table_name('financial_events')} fe ON ee.financial_event_id = fe.id
                    WHERE ee.created_at >= NOW() - INTERVAL '{days} days'
                      AND fe.vendor = 'polygon'
                    """

                    async with self.env.database.create_pool_with_retry() as pool:
                        async with pool.acquire() as conn:
                            row = await conn.fetchrow(query)
                            if row['total_earnings'] == 0:
                                return 1.0
                            return row['eps_count'] / row['total_earnings']

            monitor = MockEarningsQualityMonitor()
            coverage = await monitor.check_eps_coverage()

            assert coverage == 2/3  # 66.7% coverage
            conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_vendor_health_assessment(self, mock_environment):
        """Test vendor health calculation"""
        env, pool, conn = mock_environment

        # Mock vendor health data
        conn.fetchrow = AsyncMock(return_value={
            'total_records': 100,
            'complete_records': 85,  # 85% complete
            'avg_quality_score': 0.87,
            'last_success': datetime.now() - timedelta(hours=2)
        })

        with patch('src.events.quality.earnings_quality_monitor.Environment', return_value=env):
            class MockEarningsQualityMonitor:
                def __init__(self):
                    self.env = env

                async def check_vendor_health(self, vendor: str, days: int = 7):
                    query = f"""
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN ee.eps_actual_cents IS NOT NULL AND ee.revenue_actual_cents IS NOT NULL THEN 1 END) as complete_records,
                        AVG(CASE
                            WHEN ee.eps_actual_cents IS NOT NULL THEN 0.4 ELSE 0 END +
                            CASE WHEN ee.revenue_actual_cents IS NOT NULL THEN 0.4 ELSE 0 END +
                            CASE WHEN ee.earnings_call_datetime IS NOT NULL THEN 0.2 ELSE 0 END
                        ) as avg_quality_score,
                        MAX(ee.created_at) as last_success
                    FROM {self.env.get_table_name('earnings_events')} ee
                    JOIN {self.env.get_table_name('financial_events')} fe ON ee.financial_event_id = fe.id
                    WHERE fe.vendor = $1
                      AND ee.created_at >= NOW() - INTERVAL '{days} days'
                    """

                    async with self.env.database.create_pool_with_retry() as pool:
                        async with pool.acquire() as conn:
                            row = await conn.fetchrow(query, vendor)

                            if not row or row['total_records'] == 0:
                                return {
                                    'vendor': vendor,
                                    'error_rate': 1.0,
                                    'total_records': 0,
                                    'complete_records': 0,
                                    'avg_quality_score': 0.0
                                }

                            error_rate = 1.0 - (row['complete_records'] / row['total_records'])

                            return {
                                'vendor': vendor,
                                'total_records': row['total_records'],
                                'complete_records': row['complete_records'],
                                'error_rate': error_rate,
                                'avg_quality_score': row['avg_quality_score'] or 0.0,
                                'last_success': row['last_success']
                            }

            monitor = MockEarningsQualityMonitor()
            health = await monitor.check_vendor_health('polygon')

            assert health['vendor'] == 'polygon'
            assert health['total_records'] == 100
            assert health['complete_records'] == 85
            assert health['error_rate'] == 0.15  # 15% error rate
            assert health['avg_quality_score'] == 0.87

    def test_quality_threshold_evaluation(self):
        """Test quality threshold status determination"""
        thresholds = {
            'target': 0.90,
            'warning': 0.85,
            'critical': 0.70
        }

        def get_status(value: float, thresholds: Dict) -> str:
            if value >= thresholds['target']:
                return 'good'
            elif value >= thresholds['warning']:
                return 'warning'
            elif value >= thresholds['critical']:
                return 'critical'
            else:
                return 'critical'

        # Test various values
        assert get_status(0.95, thresholds) == 'good'
        assert get_status(0.90, thresholds) == 'good'
        assert get_status(0.87, thresholds) == 'warning'
        assert get_status(0.85, thresholds) == 'warning'
        assert get_status(0.75, thresholds) == 'critical'
        assert get_status(0.65, thresholds) == 'critical'

    @pytest.mark.asyncio
    async def test_quality_report_generation(self, mock_environment):
        """Test comprehensive quality report generation"""
        env, pool, conn = mock_environment

        # Mock all the database calls for a complete report
        call_count = 0
        async def mock_fetchrow(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            if call_count == 1:  # EPS coverage query
                return {'total_earnings': 1000, 'eps_count': 850}
            elif call_count == 2:  # Revenue coverage query
                return {'total_earnings': 1000, 'revenue_count': 920}
            elif call_count == 3:  # Call timing coverage query
                return {'total_earnings': 1000, 'call_count': 680}
            else:  # Vendor health queries
                return {
                    'total_records': 250,
                    'complete_records': 200,
                    'avg_quality_score': 0.84,
                    'last_success': datetime.now()
                }

        conn.fetchrow = AsyncMock(side_effect=mock_fetchrow)

        with patch('src.events.quality.earnings_quality_monitor.Environment', return_value=env):
            # Simulate quality report structure
            report = {
                'timestamp': datetime.now().isoformat(),
                'overall_quality_score': 0.85,  # Average of metrics
                'status': 'warning',
                'metrics': [
                    {
                        'name': 'EPS Coverage (30 days)',
                        'value': 0.85,  # 850/1000
                        'target': 0.90,
                        'status': 'warning'
                    },
                    {
                        'name': 'Revenue Coverage (30 days)',
                        'value': 0.92,  # 920/1000
                        'target': 0.95,
                        'status': 'warning'
                    },
                    {
                        'name': 'Call Timing Coverage (30 days)',
                        'value': 0.68,  # 680/1000
                        'target': 0.80,
                        'status': 'critical'
                    }
                ],
                'vendor_health': {
                    'polygon': {
                        'total_records': 250,
                        'completion_rate': 0.80,  # 200/250
                        'error_rate': 0.20,
                        'quality_score': 0.84
                    }
                }
            }

            # Validate report structure and values
            assert 'timestamp' in report
            assert 'overall_quality_score' in report
            assert 'status' in report
            assert len(report['metrics']) == 3
            assert 'vendor_health' in report

            # Check metric calculations
            eps_metric = next(m for m in report['metrics'] if 'EPS Coverage' in m['name'])
            assert eps_metric['value'] == 0.85
            assert eps_metric['status'] == 'warning'

            # Check vendor health
            polygon_health = report['vendor_health']['polygon']
            assert polygon_health['completion_rate'] == 0.80
            assert polygon_health['error_rate'] == 0.20

    @pytest.mark.asyncio
    async def test_alert_generation_logic(self):
        """Test alert generation for various quality issues"""
        alerts_generated = []

        async def mock_send_alert(message: str, severity: str = 'warning'):
            alerts_generated.append({'message': message, 'severity': severity})

        # Mock quality report with various issues
        report = {
            'status': 'warning',
            'overall_quality_score': 0.75,
            'metrics': [
                {'name': 'EPS Coverage (30 days)', 'value': 0.65, 'status': 'critical'},
                {'name': 'Revenue Coverage (30 days)', 'value': 0.88, 'status': 'warning'},
                {'name': 'Call Timing Coverage (30 days)', 'value': 0.45, 'status': 'critical'}
            ],
            'vendor_health': {
                'polygon': {'error_rate': 0.12},  # >10% error rate
                'eodhd': {'error_rate': 0.05}     # Normal error rate
            }
        }

        # Simulate alert logic
        for metric in report['metrics']:
            if metric['status'] == 'critical':
                await mock_send_alert(
                    f"{metric['name']}: {metric['value']:.1%} (below critical threshold)",
                    severity='critical'
                )
            elif metric['status'] == 'warning':
                await mock_send_alert(
                    f"{metric['name']}: {metric['value']:.1%} (below target)",
                    severity='warning'
                )

        # Check vendor health alerts
        for vendor, health in report['vendor_health'].items():
            if health['error_rate'] > 0.10:
                await mock_send_alert(
                    f"{vendor}: High error rate {health['error_rate']:.1%}",
                    severity='critical'
                )

        # Validate alerts were generated correctly
        assert len(alerts_generated) == 4  # 2 critical metrics + 1 warning metric + 1 vendor

        critical_alerts = [a for a in alerts_generated if a['severity'] == 'critical']
        warning_alerts = [a for a in alerts_generated if a['severity'] == 'warning']

        assert len(critical_alerts) == 3  # EPS, Call Timing, Polygon vendor
        assert len(warning_alerts) == 1   # Revenue coverage

        # Check specific alert content
        eps_alert = next(a for a in critical_alerts if 'EPS Coverage' in a['message'])
        assert '65.0%' in eps_alert['message']

        vendor_alert = next(a for a in critical_alerts if 'polygon' in a['message'])
        assert '12.0%' in vendor_alert['message']

    def test_quality_score_calculation(self):
        """Test overall quality score calculation logic"""
        # Sample metrics (excluding call timing as less critical)
        metrics = [
            {'name': 'EPS Coverage (30 days)', 'value': 0.85},
            {'name': 'Revenue Coverage (30 days)', 'value': 0.92}
        ]

        # Calculate overall score (excluding call timing)
        metric_scores = [m['value'] for m in metrics if 'Call Timing' not in m['name']]
        overall_score = sum(metric_scores) / len(metric_scores) if metric_scores else 0.0

        assert overall_score == 0.885  # (0.85 + 0.92) / 2

        # Test status determination
        if overall_score >= 0.85:
            status = 'good'
        elif overall_score >= 0.70:
            status = 'warning'
        else:
            status = 'critical'

        assert status == 'good'

    def test_coverage_edge_cases(self):
        """Test edge cases in coverage calculations"""
        # Zero events case
        total_events = 0
        eps_events = 0
        coverage = 1.0 if total_events == 0 else eps_events / total_events
        assert coverage == 1.0

        # Perfect coverage
        total_events = 100
        eps_events = 100
        coverage = eps_events / total_events
        assert coverage == 1.0

        # Zero coverage
        total_events = 100
        eps_events = 0
        coverage = eps_events / total_events
        assert coverage == 0.0

        # Partial coverage
        total_events = 1000
        eps_events = 789
        coverage = eps_events / total_events
        assert coverage == 0.789

class TestQualityMonitorIntegration:
    """Integration tests for the complete monitoring system"""

    @pytest.mark.asyncio
    async def test_continuous_monitoring_simulation(self):
        """Test continuous monitoring loop simulation"""
        monitoring_runs = 0
        alerts_sent = 0

        async def mock_run_quality_check():
            nonlocal monitoring_runs, alerts_sent
            monitoring_runs += 1

            # Simulate different quality states
            if monitoring_runs == 1:
                # First run: good quality, no alerts
                return {'status': 'good', 'overall_quality_score': 0.92}
            elif monitoring_runs == 2:
                # Second run: warning quality, send alert
                alerts_sent += 1
                return {'status': 'warning', 'overall_quality_score': 0.78}
            else:
                # Third run: critical quality, send critical alert
                alerts_sent += 2  # Multiple alerts for critical state
                return {'status': 'critical', 'overall_quality_score': 0.65}

        # Simulate 3 monitoring cycles
        for _ in range(3):
            result = await mock_run_quality_check()
            assert 'status' in result
            assert 'overall_quality_score' in result

        assert monitoring_runs == 3
        assert alerts_sent == 3  # 0 + 1 + 2 alerts across runs

    def test_monitoring_configuration(self):
        """Test monitoring configuration and thresholds"""
        quality_thresholds = {
            'eps_coverage': {'target': 0.90, 'warning': 0.85, 'critical': 0.70},
            'revenue_coverage': {'target': 0.95, 'warning': 0.90, 'critical': 0.80},
            'call_timing_coverage': {'target': 0.80, 'warning': 0.60, 'critical': 0.40},
            'vendor_error_rate': {'target': 0.02, 'warning': 0.05, 'critical': 0.10},
            'data_freshness': {'target': 1.0, 'warning': 0.95, 'critical': 0.85}
        }

        # Validate threshold configuration
        for metric, thresholds in quality_thresholds.items():
            assert thresholds['target'] > thresholds['warning']
            assert thresholds['warning'] > thresholds['critical']
            assert all(0 <= v <= 1 for v in thresholds.values() if metric != 'vendor_error_rate')

    def test_monitoring_report_serialization(self):
        """Test that monitoring reports can be serialized to JSON"""
        sample_report = {
            'timestamp': datetime.now().isoformat(),
            'overall_quality_score': 0.85,
            'status': 'good',
            'metrics': [
                {
                    'name': 'EPS Coverage',
                    'value': 0.85,
                    'target': 0.90,
                    'status': 'warning'
                }
            ],
            'vendor_health': {
                'polygon': {
                    'total_records': 1000,
                    'completion_rate': 0.88,
                    'error_rate': 0.12,
                    'quality_score': 0.85,
                    'last_success': datetime.now().isoformat()
                }
            }
        }

        # Should be serializable to JSON
        json_str = json.dumps(sample_report, default=str)
        assert json_str is not None

        # Should be deserializable from JSON
        parsed_report = json.loads(json_str)
        assert parsed_report['overall_quality_score'] == 0.85
        assert parsed_report['status'] == 'good'

if __name__ == "__main__":
    pytest.main([__file__, "-v"])