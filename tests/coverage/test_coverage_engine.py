"""
Unit Tests for Coverage Analytics Engine

Comprehensive test suite for the data coverage catalog functionality,
including coverage computation, gap detection, and API endpoints.
"""

import pytest
import asyncio
import asyncpg
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from unittest.mock import Mock, AsyncMock, patch
import numpy as np

from src.coverage.coverage_engine import (
    CoverageAnalyticsEngine,
    CoverageQuery,
    CoverageStats,
    CoverageGap,
    CoverageInterval,
    CoverageSummary,
    AggregationLevel,
    CoverageStatus,
    GapSeverity,
    GapType
)

# =====================================================
# Test Fixtures
# =====================================================

@pytest.fixture
def mock_db_pool():
    """Mock database pool for testing"""
    pool = Mock(spec=asyncpg.Pool)
    conn = AsyncMock(spec=asyncpg.Connection)
    
    # Create a context manager mock
    async_context = AsyncMock()
    async_context.__aenter__ = AsyncMock(return_value=conn)
    async_context.__aexit__ = AsyncMock(return_value=None)
    pool.acquire.return_value = async_context
    
    return pool, conn

@pytest.fixture
def coverage_engine(mock_db_pool):
    """Coverage engine with mocked database"""
    pool, conn = mock_db_pool
    engine = CoverageAnalyticsEngine(pool)
    return engine, conn

@pytest.fixture
def sample_coverage_data():
    """Sample coverage data for testing"""
    return {
        'symbol': 'AAPL',
        'vendor': 'polygon',
        'data_type': 'minute',
        'start_time': datetime(2024, 8, 22, 9, 30),
        'end_time': datetime(2024, 8, 22, 10, 30),
        'record_count': 58,
        'expected_count': 60,
        'avg_quality_score': 0.95
    }

@pytest.fixture  
def sample_gap_data():
    """Sample gap data for testing"""
    return {
        'symbol': 'TSLA',
        'vendor': 'tiingo',
        'data_type': 'minute',
        'gap_start': datetime(2024, 8, 22, 10, 15),
        'gap_end': datetime(2024, 8, 22, 10, 20),
        'gap_duration_minutes': 5,
        'expected_records': 5,
        'gap_type': 'missing',
        'gap_severity': 'medium'
    }

# =====================================================
# Coverage Engine Core Tests
# =====================================================

class TestCoverageAnalyticsEngine:
    """Test coverage analytics engine core functionality"""
    
    @pytest.mark.asyncio
    async def test_engine_initialization(self, coverage_engine):
        """Test engine initializes correctly"""
        engine, conn = coverage_engine
        
        # Mock SLA loading
        conn.fetch.return_value = [
            {
                'symbol': None,
                'vendor': 'polygon',
                'data_type': 'minute',
                'min_coverage_percentage': Decimal('95.0'),
                'warning_threshold': Decimal('90.0'),
                'critical_threshold': Decimal('80.0')
            }
        ]
        
        await engine.initialize()
        
        # Verify SLA cache was populated
        assert len(engine._sla_cache) > 0
        conn.fetch.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_compute_coverage_stats_minute_data(self, coverage_engine, sample_coverage_data):
        """Test coverage statistics computation for minute data"""
        engine, conn = coverage_engine
        
        # Mock minute bars query
        conn.fetchrow.return_value = {
            'count': 58,
            'avg_quality': Decimal('0.95'),
            'first_time': sample_coverage_data['start_time'],
            'last_time': sample_coverage_data['end_time']
        }
        
        # Mock database queries - first for minute stats, then for gap count
        conn.fetchrow.side_effect = [
            {  # minute stats query
                'count': 58,
                'avg_quality': Decimal('0.95'),
                'first_time': sample_coverage_data['start_time'],
                'last_time': sample_coverage_data['end_time']
            },
            {  # gap count query
                'gap_count': 1,
                'total_gap_minutes': 2
            }
        ]
        
        stats = await engine.compute_coverage_stats(
            symbol=sample_coverage_data['symbol'],
            vendor=sample_coverage_data['vendor'],
            data_type=sample_coverage_data['data_type'],
            start_time=sample_coverage_data['start_time'],
            end_time=sample_coverage_data['end_time'],
            aggregation_level=AggregationLevel.HOUR
        )
        
        assert stats.symbol == 'AAPL'
        assert stats.vendor == 'polygon'
        assert stats.total_actual == 58
        assert stats.total_expected == 60
        assert abs(stats.coverage_percentage - 96.67) < 0.01
        assert abs(stats.completeness_score - 0.9667) < 0.001
        assert stats.avg_quality_score == 0.95
        assert stats.gap_count == 1
        assert stats.total_gap_duration_minutes == 2
    
    @pytest.mark.asyncio
    async def test_compute_coverage_stats_daily_data(self, coverage_engine):
        """Test coverage statistics computation for daily data"""
        engine, conn = coverage_engine
        
        # Mock daily prices query and gap count
        conn.fetchrow.side_effect = [
            {  # daily stats query
                'count': 5,
                'avg_quality': None,
                'first_time': datetime(2024, 8, 19),
                'last_time': datetime(2024, 8, 23)
            },
            {  # gap count query
                'gap_count': 0,
                'total_gap_minutes': 0
            }
        ]
        
        stats = await engine.compute_coverage_stats(
            symbol='MSFT',
            vendor='fmp',
            data_type='daily',
            start_time=datetime(2024, 8, 19),
            end_time=datetime(2024, 8, 24),
            aggregation_level=AggregationLevel.WEEK
        )
        
        assert stats.symbol == 'MSFT'
        assert stats.vendor == 'fmp'
        assert stats.total_actual == 5
        assert stats.coverage_percentage == 100.0  # 5/5 trading days
        assert stats.avg_quality_score is None
        assert stats.gap_count == 0
    
    @pytest.mark.asyncio
    async def test_calculate_expected_records(self, coverage_engine):
        """Test expected record calculation for different aggregation levels"""
        engine, _ = coverage_engine
        
        # Test minute data expectations
        hour_expected = engine._calculate_expected_records(
            'minute', AggregationLevel.HOUR, timedelta(hours=1)
        )
        assert hour_expected == 60
        
        day_expected = engine._calculate_expected_records(
            'minute', AggregationLevel.DAY, timedelta(days=1)
        )
        assert day_expected == int(6.5 * 60)  # 6.5 trading hours
        
        # Test daily data expectations
        week_expected = engine._calculate_expected_records(
            'daily', AggregationLevel.WEEK, timedelta(weeks=1)
        )
        assert week_expected == 5  # 5 trading days per week
    
    @pytest.mark.asyncio
    async def test_hierarchical_aggregations(self, coverage_engine):
        """Test hierarchical aggregation computation"""
        engine, conn = coverage_engine
        
        # Mock database responses for different aggregation levels
        mock_responses = [
            # Hour level - stats then gaps
            {'count': 58, 'avg_quality': Decimal('0.95'), 'first_time': datetime.now(), 'last_time': datetime.now()},
            {'gap_count': 1, 'total_gap_minutes': 2},
            # Day level - stats then gaps
            {'count': 390, 'avg_quality': Decimal('0.94'), 'first_time': datetime.now(), 'last_time': datetime.now()},
            {'gap_count': 5, 'total_gap_minutes': 15},
            # Week level - stats then gaps
            {'count': 1950, 'avg_quality': Decimal('0.93'), 'first_time': datetime.now(), 'last_time': datetime.now()},
            {'gap_count': 12, 'total_gap_minutes': 45},
            # Month level - stats then gaps
            {'count': 8580, 'avg_quality': Decimal('0.92'), 'first_time': datetime.now(), 'last_time': datetime.now()},
            {'gap_count': 25, 'total_gap_minutes': 120}
        ]
        
        conn.fetchrow.side_effect = mock_responses
        
        aggregations = await engine.compute_hierarchical_aggregations(
            symbol='AAPL',
            vendor='polygon', 
            data_type='minute',
            base_date=date(2024, 8, 22)
        )
        
        assert len(aggregations) == 4
        assert aggregations[0].aggregation_level == 'hour'
        assert aggregations[1].aggregation_level == 'day'
        assert aggregations[2].aggregation_level == 'week'
        assert aggregations[3].aggregation_level == 'month'
        
        # Verify coverage percentages are calculated correctly
        assert aggregations[0].coverage_percentage > 95  # Hour level
        assert aggregations[1].coverage_percentage > 90  # Day level

# =====================================================
# Gap Detection Tests
# =====================================================

class TestGapDetection:
    """Test gap detection functionality"""
    
    @pytest.mark.asyncio
    async def test_detect_gaps_realtime(self, coverage_engine):
        """Test real-time gap detection"""
        engine, conn = coverage_engine
        
        # Mock database response for gap detection
        current_time = datetime(2024, 8, 22, 10, 15)
        previous_time = datetime(2024, 8, 22, 10, 10)
        
        # Mock previous timestamp query
        conn.fetchrow.return_value = {'timestamp': previous_time}
        
        # Test that gap detection doesn't fail
        gaps = await engine.detect_gaps_realtime(
            'AAPL', 'polygon', 'minute', current_time
        )
        
        # Should return list of gaps
        assert isinstance(gaps, list)
    
    @pytest.mark.asyncio
    async def test_detect_gaps_batch(self, coverage_engine):
        """Test batch gap detection for historical data"""
        engine, conn = coverage_engine
        
        # Mock database operations for batch gap detection
        conn.fetch.return_value = [
            {'timestamp': datetime(2024, 8, 22, 10, 0)},
            {'timestamp': datetime(2024, 8, 22, 10, 5)},  # 5 minute gap here
            {'timestamp': datetime(2024, 8, 22, 10, 10)}
        ]
        
        gaps = await engine.detect_gaps_batch(
            'TSLA', 'tiingo', 'minute',
            datetime(2024, 8, 22, 10, 0),
            datetime(2024, 8, 22, 10, 30)
        )
        
        # Should return list of gaps
        assert isinstance(gaps, list)
    
    @pytest.mark.asyncio
    async def test_heal_gaps_from_backfill(self, coverage_engine):
        """Test gap healing when backfill data arrives"""
        engine, conn = coverage_engine
        
        # Mock gaps that can be healed
        backfill_timestamps = [
            datetime(2024, 8, 22, 10, 15),
            datetime(2024, 8, 22, 10, 16),
            datetime(2024, 8, 22, 10, 17)
        ]
        
        # Mock database response for gap healing
        conn.fetch.side_effect = [
            [{'gap_id': 1}],  # First timestamp heals 1 gap
            [{'gap_id': 2}],  # Second timestamp heals 1 gap
            []                # Third timestamp heals no gaps
        ]
        
        healed_count = await engine.heal_gaps_from_backfill(
            'AAPL', 'polygon', 'minute', backfill_timestamps
        )
        
        assert healed_count == 2
        assert conn.fetch.call_count == 3

# =====================================================
# Query and Analysis Tests  
# =====================================================

class TestCoverageQueries:
    """Test coverage query and analysis functionality"""
    
    @pytest.mark.asyncio
    async def test_query_coverage_summary(self, coverage_engine):
        """Test coverage summary queries with filtering"""
        engine, conn = coverage_engine
        
        # Mock coverage summary data
        mock_summary_data = [
            {
                'symbol': 'AAPL', 'vendor': 'polygon', 'data_type': 'minute',
                'current_status': 'active', 'coverage_24h': Decimal('98.5'),
                'quality_24h': Decimal('0.95'), 'records_24h': 390,
                'coverage_7d': Decimal('97.2'), 'coverage_30d': Decimal('96.8'),
                'latest_data_time': datetime.now(), 'hours_since_update': Decimal('0.1'),
                'coverage_trend': 'stable', 'quality_trend': 'improving',
                'gaps_24h': 2
            },
            {
                'symbol': 'TSLA', 'vendor': 'tiingo', 'data_type': 'minute',
                'current_status': 'stale', 'coverage_24h': Decimal('85.0'),
                'quality_24h': Decimal('0.88'), 'records_24h': 340,
                'coverage_7d': Decimal('87.5'), 'coverage_30d': Decimal('89.2'),
                'latest_data_time': datetime.now() - timedelta(hours=2),
                'hours_since_update': Decimal('2.1'),
                'coverage_trend': 'degrading', 'quality_trend': 'stable',
                'gaps_24h': 5
            }
        ]
        
        conn.fetch.return_value = mock_summary_data
        
        # Test query with filters
        query = CoverageQuery(
            symbols=['AAPL', 'TSLA'],
            vendors=['polygon', 'tiingo'],
            min_coverage_percentage=80.0
        )
        
        summary = await engine.query_coverage_summary(query)
        
        assert len(summary) == 2
        assert summary[0].symbol == 'AAPL'
        assert summary[0].coverage_24h == 98.5
        assert summary[1].symbol == 'TSLA'
        assert summary[1].current_status == 'stale'
    
    @pytest.mark.asyncio
    async def test_get_vendor_comparison(self, coverage_engine):
        """Test vendor comparison functionality"""
        engine, conn = coverage_engine
        
        # Mock vendor comparison data
        mock_vendor_data = [
            {
                'vendor': 'polygon',
                'coverage_percentage': Decimal('98.5'),
                'quality_score': Decimal('0.95'),
                'current_status': 'active',
                'latest_data_time': datetime.now(),
                'hours_since_update': Decimal('0.1')
            },
            {
                'vendor': 'tiingo',
                'coverage_percentage': Decimal('92.3'),
                'quality_score': Decimal('0.89'),
                'current_status': 'active',
                'latest_data_time': datetime.now() - timedelta(minutes=30),
                'hours_since_update': Decimal('0.5')
            }
        ]
        
        conn.fetch.return_value = mock_vendor_data
        
        comparison = await engine.get_vendor_comparison('AAPL', 'minute', '24h')
        
        assert comparison['symbol'] == 'AAPL'
        assert comparison['vendor_count'] == 2
        assert comparison['best_vendor']['vendor'] == 'polygon'
        assert comparison['worst_vendor']['vendor'] == 'tiingo'
        assert comparison['average_coverage'] > 90
    
    @pytest.mark.asyncio
    async def test_get_coverage_trends(self, coverage_engine):
        """Test coverage trend analysis"""
        engine, conn = coverage_engine
        
        # Mock trend data
        mock_daily_trends = [
            {
                'date': date(2024, 8, 20),
                'coverage_percentage': Decimal('97.5'),
                'avg_quality_score': Decimal('0.94'),
                'gap_count': 3,
                'total_gap_duration_minutes': 15
            },
            {
                'date': date(2024, 8, 21),
                'coverage_percentage': Decimal('98.2'),
                'avg_quality_score': Decimal('0.95'),
                'gap_count': 2,
                'total_gap_duration_minutes': 8
            }
        ]
        
        mock_hourly_trends = [
            {
                'period_start': datetime(2024, 8, 22, 9),
                'coverage_percentage': Decimal('96.7'),
                'avg_quality_score': Decimal('0.93'),
                'gap_count': 1
            },
            {
                'period_start': datetime(2024, 8, 22, 10),
                'coverage_percentage': Decimal('98.3'),
                'avg_quality_score': Decimal('0.96'),
                'gap_count': 0
            }
        ]
        
        conn.fetch.side_effect = [mock_daily_trends, mock_hourly_trends]
        
        trends = await engine.get_coverage_trends('AAPL', 'polygon', 'minute', 30)
        
        assert 'daily_trends' in trends
        assert 'hourly_trends' in trends
        assert len(trends['daily_trends']) == 2
        assert len(trends['hourly_trends']) == 2
        assert trends['symbol'] == 'AAPL'
        assert trends['period_days'] == 30

# =====================================================
# SLA Monitoring Tests
# =====================================================

class TestSLAMonitoring:
    """Test SLA monitoring and compliance checking"""
    
    @pytest.mark.asyncio
    async def test_check_sla_compliance(self, coverage_engine):
        """Test SLA compliance checking"""
        engine, conn = coverage_engine
        
        # Mock SLA compliance data
        mock_compliance_data = [
            {
                'symbol': 'AAPL', 'vendor': 'polygon', 'data_type': 'minute',
                'coverage_24h': Decimal('98.5'), 'quality_24h': Decimal('0.95'),
                'min_coverage_percentage': Decimal('95.0'),
                'warning_threshold': Decimal('90.0'),
                'critical_threshold': Decimal('80.0'),
                'compliance_status': 'compliant',
                'coverage_gap': Decimal('3.5')
            },
            {
                'symbol': 'TSLA', 'vendor': 'tiingo', 'data_type': 'minute',
                'coverage_24h': Decimal('85.0'), 'quality_24h': Decimal('0.88'),
                'min_coverage_percentage': Decimal('95.0'),
                'warning_threshold': Decimal('90.0'),
                'critical_threshold': Decimal('80.0'),
                'compliance_status': 'critical',
                'coverage_gap': Decimal('-10.0')
            }
        ]
        
        conn.fetch.return_value = mock_compliance_data
        
        compliance = await engine.check_sla_compliance()
        
        assert len(compliance) == 2
        assert compliance[0]['compliance_status'] == 'compliant'
        assert compliance[1]['compliance_status'] == 'critical'
        assert compliance[0]['current_coverage'] == 98.5
        assert compliance[1]['coverage_gap'] == -10.0

# =====================================================
# Performance and Edge Case Tests
# =====================================================

class TestPerformanceAndEdgeCases:
    """Test performance optimizations and edge cases"""
    
    @pytest.mark.asyncio
    async def test_large_dataset_handling(self, coverage_engine):
        """Test handling of large datasets"""
        engine, conn = coverage_engine
        
        # Mock large dataset query - stats then gaps
        conn.fetchrow.side_effect = [
            {  # stats query
                'count': 2000000,  # 2M records
                'avg_quality': Decimal('0.94'),
                'first_time': datetime(2024, 1, 1),
                'last_time': datetime(2024, 8, 22)
            },
            {  # gap count query
                'gap_count': 50,
                'total_gap_minutes': 300
            }
        ]
        
        # This should not raise memory errors or performance issues
        stats = await engine.compute_coverage_stats(
            'AAPL', 'polygon', 'minute',
            datetime(2024, 1, 1), datetime(2024, 8, 22),
            AggregationLevel.MONTH
        )
        
        assert stats.total_actual == 2000000
        assert stats.avg_quality_score == 0.94
    
    @pytest.mark.asyncio
    async def test_empty_data_handling(self, coverage_engine):
        """Test handling of empty or missing data"""
        engine, conn = coverage_engine
        
        # Mock empty dataset - stats then gaps
        conn.fetchrow.side_effect = [
            {  # stats query
                'count': 0, 'avg_quality': None, 'first_time': None, 'last_time': None
            },
            {  # gap count query
                'gap_count': 0, 'total_gap_minutes': 0
            }
        ]
        
        stats = await engine.compute_coverage_stats(
            'UNKNOWN', 'unknown_vendor', 'minute',
            datetime.now() - timedelta(hours=1), datetime.now(),
            AggregationLevel.HOUR
        )
        
        assert stats.total_actual == 0
        assert stats.coverage_percentage == 0.0
        assert stats.avg_quality_score is None
    
    @pytest.mark.asyncio
    async def test_concurrent_updates(self, coverage_engine):
        """Test handling of concurrent coverage updates"""
        engine, conn = coverage_engine
        
        # Simulate concurrent updates for the same symbol/vendor
        tasks = []
        for i in range(10):
            task = engine.compute_coverage_stats(
                'AAPL', 'polygon', 'minute',
                datetime.now() - timedelta(hours=1),
                datetime.now(),
                AggregationLevel.HOUR
            )
            tasks.append(task)
        
        # Mock consistent database responses for concurrent tests
        stats_response = {
            'count': 58, 'avg_quality': Decimal('0.95'), 
            'first_time': datetime.now(), 'last_time': datetime.now()
        }
        gaps_response = {'gap_count': 1, 'total_gap_minutes': 2}
        
        # Each stats call makes 2 fetchrow calls
        conn.fetchrow.side_effect = [stats_response, gaps_response] * 10
        
        # All tasks should complete without deadlocks or errors
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 10
        assert all(result.symbol == 'AAPL' for result in results)
    
    def test_data_model_validation(self):
        """Test data model validation and constraints"""
        
        # Test CoverageStats creation with valid data
        stats = CoverageStats(
            symbol='AAPL',
            vendor='polygon',
            data_type='minute',
            aggregation_level='hour',
            period_start=datetime.now(),
            period_end=datetime.now() + timedelta(hours=1),
            total_expected=60,
            total_actual=58,
            coverage_percentage=96.67,
            completeness_score=0.9667
        )
        
        assert stats.symbol == 'AAPL'
        assert stats.coverage_percentage == 96.67
        
        # Test CoverageGap creation
        gap = CoverageGap(
            symbol='TSLA',
            vendor='tiingo',
            data_type='minute',
            gap_start=datetime.now(),
            gap_end=datetime.now() + timedelta(minutes=5),
            gap_duration_minutes=5,
            expected_records=5,
            gap_type='missing',
            gap_severity='medium',
            trading_day=date.today(),
            is_market_hours=True,
            detection_method='realtime',
            detection_confidence=0.95
        )
        
        assert gap.gap_duration_minutes == 5
        assert gap.gap_severity == 'medium'

# =====================================================
# Integration Tests
# =====================================================

class TestCoverageIntegration:
    """Integration tests for coverage system components"""
    
    @pytest.mark.asyncio
    async def test_end_to_end_coverage_workflow(self, coverage_engine):
        """Test complete coverage tracking workflow"""
        engine, conn = coverage_engine
        
        # Initialize engine
        conn.fetch.return_value = []  # Empty SLA config
        await engine.initialize()
        
        # Simulate new data arrival
        new_timestamp = datetime.now()
        
        # Mock gap detection - previous timestamp query
        previous_time = new_timestamp - timedelta(minutes=5)
        conn.fetchrow.return_value = {'timestamp': previous_time}
        
        # Detect gaps
        gaps = await engine.detect_gaps_realtime(
            'AAPL', 'polygon', 'minute', new_timestamp
        )
        
        assert isinstance(gaps, list)
        
        # Mock coverage computation after gap - stats then gaps
        conn.fetchrow.side_effect = [
            {  # stats query
                'count': 56, 'avg_quality': Decimal('0.94'), 
                'first_time': new_timestamp, 'last_time': new_timestamp
            },
            {  # gap count query
                'gap_count': 1, 'total_gap_minutes': 4
            }
        ]
        
        # Compute updated coverage stats
        stats = await engine.compute_coverage_stats(
            'AAPL', 'polygon', 'minute',
            new_timestamp - timedelta(hours=1), new_timestamp,
            AggregationLevel.HOUR
        )
        
        assert stats.total_actual == 56
        assert stats.coverage_percentage < 100  # Should reflect the gap

if __name__ == "__main__":
    pytest.main([__file__, "-v"])