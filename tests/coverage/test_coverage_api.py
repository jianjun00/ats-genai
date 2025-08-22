"""
Unit Tests for Coverage API

Test suite for the coverage catalog API endpoints,
including REST API functionality and WebSocket streaming.
"""

import pytest
import asyncio
import json
from datetime import datetime, timedelta, date
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi.websockets import WebSocket

from src.coverage.coverage_api import (
    CoverageAPI, 
    CoverageQueryRequest,
    CoverageStatsResponse,
    VendorComparisonResponse,
    CoverageHeatmapResponse,
    SLAComplianceResponse
)
from src.coverage.coverage_engine import CoverageSummary

# =====================================================
# Test Fixtures
# =====================================================

@pytest.fixture
def mock_db_pool():
    """Mock database pool for API testing"""
    pool = Mock()
    conn = AsyncMock()
    
    # Create a context manager mock
    async_context = AsyncMock()
    async_context.__aenter__ = AsyncMock(return_value=conn)
    async_context.__aexit__ = AsyncMock(return_value=None)
    pool.acquire.return_value = async_context
    
    return pool, conn

@pytest.fixture
def coverage_api(mock_db_pool):
    """Coverage API with mocked dependencies"""
    pool, conn = mock_db_pool
    api = CoverageAPI(pool)
    return api, conn

@pytest.fixture
def sample_coverage_summary():
    """Sample coverage summary data"""
    return [
        CoverageSummary(
            symbol='AAPL',
            vendor='polygon',
            data_type='minute',
            current_status='active',
            coverage_24h=98.5,
            quality_24h=0.95,
            gaps_24h=2,
            records_24h=390,
            coverage_7d=97.2,
            coverage_30d=96.8,
            latest_data_time=datetime.now(),
            hours_since_update=0.1,
            coverage_trend='stable',
            quality_trend='improving'
        ),
        CoverageSummary(
            symbol='TSLA',
            vendor='tiingo',
            data_type='minute',
            current_status='stale',
            coverage_24h=85.0,
            quality_24h=0.88,
            gaps_24h=5,
            records_24h=340,
            coverage_7d=87.5,
            coverage_30d=89.2,
            latest_data_time=datetime.now() - timedelta(hours=2),
            hours_since_update=2.1,
            coverage_trend='degrading',
            quality_trend='stable'
        )
    ]

# =====================================================
# Coverage API Core Tests
# =====================================================

class TestCoverageAPI:
    """Test coverage API core functionality"""
    
    @pytest.mark.asyncio
    async def test_api_initialization(self, coverage_api):
        """Test API initializes correctly"""
        api, conn = coverage_api
        
        # Mock coverage engine initialization
        with patch.object(api.coverage_engine, 'initialize') as mock_init:
            await api.initialize()
            mock_init.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_coverage_summary(self, coverage_api, sample_coverage_summary):
        """Test coverage summary endpoint"""
        api, conn = coverage_api
        
        # Mock coverage engine query
        with patch.object(api.coverage_engine, 'query_coverage_summary') as mock_query:
            mock_query.return_value = sample_coverage_summary
            
            result = await api.get_coverage_summary(
                symbols=['AAPL', 'TSLA'],
                vendors=['polygon', 'tiingo'],
                min_coverage=80.0
            )
            
            assert len(result) == 2
            assert result[0].symbol == 'AAPL'
            assert result[1].symbol == 'TSLA'
            mock_query.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_coverage_stats(self, coverage_api):
        """Test coverage statistics endpoint"""
        api, conn = coverage_api
        
        # Mock database query
        mock_stats_data = [
            {
                'symbol': 'AAPL',
                'vendor': 'polygon',
                'data_type': 'minute',
                'aggregation_level': 'hour',
                'period_start': datetime(2024, 8, 22, 10),
                'period_end': datetime(2024, 8, 22, 11),
                'coverage_percentage': 96.67,
                'completeness_score': 0.9667,
                'avg_quality_score': 0.95,
                'gap_count': 1,
                'total_gap_duration_minutes': 2,
                'records_per_minute': 58.0
            }
        ]
        
        conn.fetch.return_value = mock_stats_data
        
        result = await api.get_coverage_stats(
            symbol='AAPL',
            vendor='polygon',
            data_type='minute',
            aggregation_level='hour'
        )
        
        assert len(result) == 1
        assert result[0].symbol == 'AAPL'
        assert result[0].coverage_percentage == 96.67
        assert result[0].gap_count == 1
    
    @pytest.mark.asyncio
    async def test_get_vendor_comparison(self, coverage_api):
        """Test vendor comparison endpoint"""
        api, conn = coverage_api
        
        # Mock comparison data
        mock_comparison = {
            'symbol': 'AAPL',
            'data_type': 'minute',
            'time_period': '24h',
            'vendors': [
                {
                    'vendor': 'polygon',
                    'coverage_percentage': 98.5,
                    'quality_score': 0.95,
                    'status': 'active',
                    'latest_data_time': datetime.now(),
                    'hours_since_update': 0.1
                },
                {
                    'vendor': 'tiingo',
                    'coverage_percentage': 92.3,
                    'quality_score': 0.89,
                    'status': 'active',
                    'latest_data_time': datetime.now() - timedelta(minutes=30),
                    'hours_since_update': 0.5
                }
            ],
            'best_vendor': {
                'vendor': 'polygon',
                'coverage_percentage': 98.5,
                'quality_score': 0.95,
                'status': 'active',
                'latest_data_time': datetime.now(),
                'hours_since_update': 0.1
            },
            'worst_vendor': {
                'vendor': 'tiingo',
                'coverage_percentage': 92.3,
                'quality_score': 0.89,
                'status': 'active',
                'latest_data_time': datetime.now() - timedelta(minutes=30),
                'hours_since_update': 0.5
            },
            'average_coverage': 95.4,
            'coverage_variance': 19.22,
            'vendor_count': 2
        }
        
        with patch.object(api.coverage_engine, 'get_vendor_comparison') as mock_comparison_func:
            mock_comparison_func.return_value = mock_comparison
            
            result = await api.get_vendor_comparison('AAPL', 'minute', '24h')
            
            assert result.symbol == 'AAPL'
            assert result.vendor_count == 2
            assert result.best_vendor['vendor'] == 'polygon'
            assert result.average_coverage == 95.4
    
    @pytest.mark.asyncio
    async def test_get_coverage_gaps(self, coverage_api):
        """Test coverage gaps endpoint"""
        api, conn = coverage_api
        
        # Mock gaps data
        mock_gaps = [
            {
                'gap_id': 1,
                'symbol': 'AAPL',
                'vendor': 'polygon',
                'data_type': 'minute',
                'gap_start': datetime(2024, 8, 22, 10, 15),
                'gap_end': datetime(2024, 8, 22, 10, 20),
                'gap_duration_minutes': 5,
                'expected_records': 5,
                'gap_type': 'missing',
                'gap_severity': 'medium',
                'trading_day': date(2024, 8, 22),
                'is_market_hours': True,
                'detection_method': 'realtime',
                'detection_confidence': 0.95,
                'is_resolved': False,
                'resolution_method': None,
                'resolved_at': None,
                'resolution_notes': None,
                'detected_at': datetime(2024, 8, 22, 10, 20)
            }
        ]
        
        conn.fetch.return_value = mock_gaps
        
        result = await api.get_coverage_gaps(
            symbol='AAPL',
            vendor='polygon',
            severity='medium',
            resolved=False
        )
        
        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'
        assert result[0]['gap_severity'] == 'medium'
        assert result[0]['is_resolved'] is False

# =====================================================
# Advanced API Features Tests
# =====================================================

class TestAdvancedAPIFeatures:
    """Test advanced API features like heatmaps and trends"""
    
    @pytest.mark.asyncio
    async def test_get_coverage_heatmap(self, coverage_api):
        """Test coverage heatmap generation"""
        api, conn = coverage_api
        
        symbols = ['AAPL', 'TSLA']
        vendors = ['polygon', 'tiingo']
        start_date = date(2024, 8, 20)
        end_date = date(2024, 8, 22)
        
        # Mock database queries for heatmap data
        mock_heatmap_data = [
            # AAPL/polygon coverage for each day
            {'coverage_percentage': 98.5, 'avg_quality_score': 0.95},  # 8/20
            {'coverage_percentage': 97.2, 'avg_quality_score': 0.94},  # 8/21
            {'coverage_percentage': 98.8, 'avg_quality_score': 0.96},  # 8/22
            # AAPL/tiingo coverage
            {'coverage_percentage': 92.1, 'avg_quality_score': 0.89},  # 8/20
            {'coverage_percentage': 91.5, 'avg_quality_score': 0.88},  # 8/21
            {'coverage_percentage': 93.2, 'avg_quality_score': 0.90},  # 8/22
            # TSLA/polygon coverage
            {'coverage_percentage': 95.3, 'avg_quality_score': 0.92},  # 8/20
            {'coverage_percentage': 96.1, 'avg_quality_score': 0.93},  # 8/21
            {'coverage_percentage': 94.7, 'avg_quality_score': 0.91},  # 8/22
            # TSLA/tiingo coverage
            {'coverage_percentage': 87.8, 'avg_quality_score': 0.85},  # 8/20
            {'coverage_percentage': 89.2, 'avg_quality_score': 0.87},  # 8/21
            {'coverage_percentage': 88.5, 'avg_quality_score': 0.86},  # 8/22
        ]
        
        conn.fetchrow.side_effect = mock_heatmap_data
        
        result = await api.get_coverage_heatmap(symbols, vendors, start_date, end_date)
        
        assert result.symbols == symbols
        assert result.vendors == vendors
        assert len(result.time_periods) == 3  # 3 days
        assert len(result.coverage_matrix) == 2  # 2 symbols
        assert len(result.coverage_matrix[0]) == 2  # 2 vendors per symbol
        assert len(result.coverage_matrix[0][0]) == 3  # 3 time periods per vendor
        
        # Check specific values
        assert result.coverage_matrix[0][0][0] == 98.5  # AAPL/polygon on 8/20
        assert result.quality_matrix[1][1][2] == 0.86   # TSLA/tiingo on 8/22
    
    @pytest.mark.asyncio
    async def test_get_coverage_trends(self, coverage_api):
        """Test coverage trends analysis"""
        api, conn = coverage_api
        
        # Mock trends data
        mock_trends = {
            'daily_trends': [
                {
                    'date': date(2024, 8, 20),
                    'coverage_percentage': 97.5,
                    'avg_quality_score': 0.94,
                    'gap_count': 3,
                    'total_gap_duration_minutes': 15
                },
                {
                    'date': date(2024, 8, 21),
                    'coverage_percentage': 98.2,
                    'avg_quality_score': 0.95,
                    'gap_count': 2,
                    'total_gap_duration_minutes': 8
                }
            ],
            'hourly_trends': [
                {
                    'period_start': datetime(2024, 8, 22, 9),
                    'coverage_percentage': 96.7,
                    'avg_quality_score': 0.93,
                    'gap_count': 1
                },
                {
                    'period_start': datetime(2024, 8, 22, 10),
                    'coverage_percentage': 98.3,
                    'avg_quality_score': 0.96,
                    'gap_count': 0
                }
            ],
            'symbol': 'AAPL',
            'vendor': 'polygon',
            'data_type': 'minute',
            'period_days': 30
        }
        
        with patch.object(api.coverage_engine, 'get_coverage_trends') as mock_trends_func:
            mock_trends_func.return_value = mock_trends
            
            result = await api.get_coverage_trends('AAPL', 'polygon', 'minute', 30)
            
            assert result['symbol'] == 'AAPL'
            assert len(result['daily_trends']) == 2
            assert len(result['hourly_trends']) == 2
            assert result['period_days'] == 30
    
    @pytest.mark.asyncio
    async def test_get_sla_compliance(self, coverage_api):
        """Test SLA compliance checking"""
        api, conn = coverage_api
        
        # Mock SLA compliance data
        mock_compliance = [
            {
                'symbol': 'AAPL',
                'vendor': 'polygon',
                'data_type': 'minute',
                'current_coverage': 98.5,
                'required_coverage': 95.0,
                'compliance_status': 'compliant',
                'coverage_gap': 3.5,
                'quality_score': 0.95
            },
            {
                'symbol': 'TSLA',
                'vendor': 'tiingo',
                'data_type': 'minute',
                'current_coverage': 85.0,
                'required_coverage': 95.0,
                'compliance_status': 'violation',
                'coverage_gap': -10.0,
                'quality_score': 0.88
            }
        ]
        
        with patch.object(api.coverage_engine, 'check_sla_compliance') as mock_sla_func:
            mock_sla_func.return_value = mock_compliance
            
            result = await api.get_sla_compliance()
            
            assert len(result) == 2
            assert result[0].compliance_status == 'compliant'
            assert result[1].compliance_status == 'violation'
            assert result[0].coverage_gap == 3.5
            assert result[1].coverage_gap == -10.0
    
    @pytest.mark.asyncio
    async def test_get_top_coverage_issues(self, coverage_api):
        """Test top coverage issues identification"""
        api, conn = coverage_api
        
        # Mock top issues data
        mock_issues = [
            {
                'symbol': 'TSLA',
                'vendor': 'tiingo',
                'data_type': 'minute',
                'issue_type': 'low_coverage',
                'severity_score': 15.0,
                'description': 'Coverage: 85.0%'
            },
            {
                'symbol': 'MSFT',
                'vendor': 'fmp',
                'data_type': 'minute',
                'issue_type': 'frequent_gaps',
                'severity_score': 80.0,
                'description': 'Gaps in 24h: 8'
            }
        ]
        
        conn.fetch.return_value = mock_issues
        
        result = await api.get_top_coverage_issues(limit=10)
        
        assert len(result) == 2
        assert result[0]['issue_type'] == 'low_coverage'
        assert result[1]['issue_type'] == 'frequent_gaps'
        assert result[1]['severity_score'] == 80.0

# =====================================================
# Utility and Management Tests
# =====================================================

class TestUtilityFeatures:
    """Test utility and management features"""
    
    @pytest.mark.asyncio
    async def test_trigger_coverage_refresh(self, coverage_api):
        """Test manual coverage refresh trigger"""
        api, conn = coverage_api
        
        start_time = datetime.now()
        
        # Test specific symbol/vendor refresh
        result = await api.trigger_coverage_refresh('AAPL', 'polygon')
        
        assert result['success'] is True
        assert 'AAPL/polygon' in result['scope']
        assert 'duration_ms' in result
        assert 'timestamp' in result
        
        # Test global refresh
        result = await api.trigger_coverage_refresh()
        
        assert result['success'] is True
        assert 'all active' in result['scope']
    
    @pytest.mark.asyncio
    async def test_websocket_coverage_updates(self, coverage_api):
        """Test WebSocket real-time updates"""
        api, conn = coverage_api
        
        # Mock WebSocket
        websocket = AsyncMock(spec=WebSocket)
        
        # Mock initial summary data
        with patch.object(api, 'get_coverage_summary') as mock_summary:
            mock_summary.return_value = [
                CoverageSummary(
                    symbol='AAPL', vendor='polygon', data_type='minute',
                    current_status='active', coverage_24h=98.5, quality_24h=0.95,
                    gaps_24h=2, records_24h=390, coverage_7d=97.2, coverage_30d=96.8,
                    latest_data_time=datetime.now(), hours_since_update=0.1,
                    coverage_trend='stable', quality_trend='improving'
                )
            ]
            
            # Mock recent updates query
            conn.fetch.return_value = [
                {
                    'symbol': 'AAPL',
                    'vendor': 'polygon',
                    'data_type': 'minute',
                    'coverage_24h': 98.7,
                    'quality_24h': 0.96,
                    'current_status': 'active',
                    'last_updated': datetime.now()
                }
            ]
            
            # Test streaming with a timeout to avoid infinite loop
            with patch('asyncio.sleep') as mock_sleep:
                mock_sleep.side_effect = [None, asyncio.CancelledError()]
                
                try:
                    await api.stream_coverage_updates(websocket)
                except asyncio.CancelledError:
                    pass
                
                # Verify WebSocket interactions
                websocket.accept.assert_called_once()
                assert websocket.send_json.call_count >= 1
                
                # Check initial summary was sent
                initial_call = websocket.send_json.call_args_list[0]
                initial_data = initial_call[0][0]
                assert initial_data['type'] == 'initial_summary'
                assert len(initial_data['data']) == 1

# =====================================================
# Error Handling Tests
# =====================================================

class TestErrorHandling:
    """Test error handling and edge cases"""
    
    @pytest.mark.asyncio
    async def test_database_connection_error(self, coverage_api):
        """Test handling of database connection errors"""
        api, conn = coverage_api
        
        # Simulate database connection error
        conn.fetch.side_effect = asyncio.TimeoutError("Database timeout")
        
        with pytest.raises(asyncio.TimeoutError):
            await api.get_coverage_gaps()
    
    @pytest.mark.asyncio
    async def test_invalid_parameters(self, coverage_api):
        """Test handling of invalid parameters"""
        api, conn = coverage_api
        
        # Test with invalid date range for heatmap
        with pytest.raises((ValueError, TypeError)):
            await api.get_coverage_heatmap(
                symbols=['AAPL'],
                vendors=['polygon'],
                start_date=date(2024, 8, 22),
                end_date=date(2024, 8, 20)  # End before start
            )
    
    @pytest.mark.asyncio
    async def test_empty_results_handling(self, coverage_api):
        """Test handling of empty query results"""
        api, conn = coverage_api
        
        # Mock empty results
        conn.fetch.return_value = []
        
        result = await api.get_coverage_gaps(symbol='NONEXISTENT')
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_websocket_connection_error(self, coverage_api):
        """Test WebSocket error handling"""
        api, conn = coverage_api
        
        # Mock WebSocket that fails during communication
        websocket = AsyncMock(spec=WebSocket)
        websocket.send_json.side_effect = Exception("WebSocket connection lost")
        
        # Should handle the error gracefully
        with patch.object(api, 'get_coverage_summary') as mock_summary:
            mock_summary.return_value = []
            
            # This should not raise an exception
            await api.stream_coverage_updates(websocket)
            
            websocket.close.assert_called_once()

# =====================================================
# Performance Tests
# =====================================================

class TestPerformance:
    """Test performance characteristics of the API"""
    
    @pytest.mark.asyncio
    async def test_large_heatmap_generation(self, coverage_api):
        """Test performance with large heatmap requests"""
        api, conn = coverage_api
        
        # Large number of symbols and vendors
        symbols = [f'SYMBOL{i:03d}' for i in range(100)]
        vendors = ['polygon', 'tiingo', 'fmp', 'alphavantage']
        start_date = date(2024, 8, 1)
        end_date = date(2024, 8, 31)  # 31 days
        
        # Mock database responses (would be many calls)
        conn.fetchrow.return_value = {
            'coverage_percentage': 95.0,
            'avg_quality_score': 0.90
        }
        
        # This should complete without timeout or memory issues
        result = await api.get_coverage_heatmap(symbols, vendors, start_date, end_date)
        
        assert len(result.symbols) == 100
        assert len(result.vendors) == 4
        assert len(result.time_periods) == 31
        assert result.metadata['total_cells'] == 100 * 4 * 31
    
    @pytest.mark.asyncio
    async def test_concurrent_api_requests(self, coverage_api):
        """Test handling of concurrent API requests"""
        api, conn = coverage_api
        
        # Mock database responses
        conn.fetch.return_value = []
        
        # Create multiple concurrent requests
        tasks = [
            api.get_coverage_summary(),
            api.get_coverage_gaps(),
            api.get_top_coverage_issues(),
            api.get_sla_compliance()
        ]
        
        # All requests should complete successfully
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check that no exceptions were raised
        for result in results:
            assert not isinstance(result, Exception)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])