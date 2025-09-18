#!/usr/bin/env python3
"""
Comprehensive tests for the Gap Detection Engine

Tests cover:
- Gap detection algorithms
- Gap classification and severity determination
- Backfill operations for different vendors
- Market hours logic
- Error handling and edge cases
- Gap record management
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta, timezone
import os

# Import the module under test
import sys
sys.path.append('src')

from domains.market_data.services.realtime.gap_detector import (
    GapDetectionEngine,
    DataGap
)

class TestDataGap:
    """Test the DataGap data structure"""

    def test_data_gap_creation(self):
        """Test creating DataGap with all fields"""
        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(minutes=10)

        gap = DataGap(
            vendor='polygon',
            symbol='AAPL',
            gap_start=start_time,
            gap_end=end_time,
            gap_duration_minutes=10,
            missing_bars_count=10,
            gap_type='connection_loss',
            severity='medium',
            detection_method='realtime'
        )

        assert gap.vendor == 'polygon'
        assert gap.symbol == 'AAPL'
        assert gap.gap_start == start_time
        assert gap.gap_end == end_time
        assert gap.gap_duration_minutes == 10
        assert gap.missing_bars_count == 10
        assert gap.gap_type == 'connection_loss'
        assert gap.severity == 'medium'
        assert gap.detection_method == 'realtime'

    def test_data_gap_defaults(self):
        """Test DataGap creation with default values"""
        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(minutes=5)

        gap = DataGap(
            vendor='tiingo',
            symbol='MSFT',
            gap_start=start_time,
            gap_end=end_time,
            gap_duration_minutes=5,
            missing_bars_count=5,
            gap_type='temporary_delay',
            severity='low'
        )

        assert gap.detection_method == 'realtime'

class TestGapDetectionEngine:
    """Test the main GapDetectionEngine class"""

    @pytest.fixture
    def mock_env(self):
        """Mock environment configuration"""
        with patch('market_data.realtime.gap_detector.Environment') as mock_env_class:
            mock_env = Mock()
            mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
            mock_env_class.return_value = mock_env
            yield mock_env

    @pytest.fixture
    def gap_detector(self, mock_env):
        """Create a gap detector instance with mocked dependencies"""
        with patch.dict(os.environ, {
            'GAP_THRESHOLD_MINUTES': '5',
            'CRITICAL_GAP_MINUTES': '15',
            'MAX_BACKFILL_SYMBOLS': '100',
            'ENABLE_AUTO_BACKFILL': 'true',
            'MARKET_HOURS_ONLY': 'true',
            'POLYGON_API_KEY': 'test_polygon_key',
            'TIINGO_API_KEY': 'test_tiingo_key',
            'FMP_API_KEY': 'test_fmp_key'
        }):
            detector = GapDetectionEngine()
            return detector

    def test_detector_initialization(self, gap_detector):
        """Test gap detector initialization"""
        assert gap_detector.gap_threshold_minutes == 5
        assert gap_detector.critical_gap_minutes == 15
        assert gap_detector.max_backfill_symbols == 100
        assert gap_detector.enable_auto_backfill is True
        assert gap_detector.market_hours_only is True
        assert gap_detector.polygon_api_key == 'test_polygon_key'
        assert gap_detector.tiingo_api_key == 'test_tiingo_key'
        assert gap_detector.fmp_api_key == 'test_fmp_key'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialize_database_connection(self, gap_detector, mock_env):
        """Test database initialization"""
        mock_pool = AsyncMock()

        with patch('market_data.realtime.gap_detector.asyncpg.create_pool', return_value=mock_pool):
            await gap_detector.initialize()
            assert gap_detector.pool == mock_pool
            mock_env.get_database_url.assert_called_once()

    def test_classify_gap_type(self, gap_detector):
        """Test gap type classification"""
        base_time = datetime.now(timezone.utc)

        # Test connection loss (>60 minutes)
        gap_type = gap_detector._classify_gap_type(
            base_time, base_time + timedelta(minutes=90), 90
        )
        assert gap_type == 'connection_loss'

        # Test API error (30-60 minutes)
        gap_type = gap_detector._classify_gap_type(
            base_time, base_time + timedelta(minutes=45), 45
        )
        assert gap_type == 'api_error'

        # Test rate limit (10-30 minutes)
        gap_type = gap_detector._classify_gap_type(
            base_time, base_time + timedelta(minutes=20), 20
        )
        assert gap_type == 'rate_limit'

        # Test temporary delay (<10 minutes)
        gap_type = gap_detector._classify_gap_type(
            base_time, base_time + timedelta(minutes=5), 5
        )
        assert gap_type == 'temporary_delay'

    def test_determine_gap_severity(self, gap_detector):
        """Test gap severity determination"""
        # Test critical severity
        severity = gap_detector._determine_gap_severity(20)  # Above critical threshold (15)
        assert severity == 'critical'

        # Test high severity
        severity = gap_detector._determine_gap_severity(12)
        assert severity == 'high'

        # Test medium severity
        severity = gap_detector._determine_gap_severity(7)
        assert severity == 'medium'

        # Test low severity
        severity = gap_detector._determine_gap_severity(3)
        assert severity == 'low'

    def test_is_market_hours_gap(self, gap_detector):
        """Test market hours gap detection"""
        # Mock Eastern timezone
        eastern_tz = gap_detector.eastern_tz

        # Test gap during market hours (9:30 AM - 4:00 PM ET)
        market_day = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)  # 9:30 AM ET
        gap_start = market_day
        gap_end = market_day + timedelta(minutes=30)

        assert gap_detector._is_market_hours_gap(gap_start, gap_end) is True

        # Test gap outside market hours (evening)
        evening = datetime(2025, 1, 15, 22, 0, 0, tzinfo=timezone.utc)  # 5:00 PM ET
        gap_start = evening
        gap_end = evening + timedelta(minutes=30)

        assert gap_detector._is_market_hours_gap(gap_start, gap_end) is False

        # Test gap that spans market hours
        pre_market = datetime(2025, 1, 15, 13, 0, 0, tzinfo=timezone.utc)  # 8:00 AM ET
        during_market = datetime(2025, 1, 15, 15, 0, 0, tzinfo=timezone.utc)  # 10:00 AM ET

        assert gap_detector._is_market_hours_gap(pre_market, during_market) is True

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_active_symbols(self, gap_detector):
        """Test getting active symbols for gap detection"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        # Mock database response
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL'},
            {'symbol': 'MSFT'},
            {'symbol': 'GOOGL'}
        ]

        symbols = await gap_detector._get_active_symbols('polygon')

        assert symbols == ['AAPL', 'MSFT', 'GOOGL']
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_detect_symbol_gaps(self, gap_detector):
        """Test detecting gaps for a specific symbol"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        # Mock gap detection query result
        base_time = datetime.now(timezone.utc)
        mock_conn.fetch.return_value = [
            {
                'prev_timestamp': base_time - timedelta(minutes=10),
                'timestamp': base_time,
                'gap_minutes': 10.0
            },
            {
                'prev_timestamp': base_time - timedelta(minutes=25),
                'timestamp': base_time - timedelta(minutes=10),
                'gap_minutes': 15.0
            }
        ]

        gaps = await gap_detector._detect_symbol_gaps('polygon', 'AAPL')

        assert len(gaps) == 2
        assert gaps[0].gap_duration_minutes == 10
        assert gaps[0].severity == 'medium'
        assert gaps[1].gap_duration_minutes == 15
        assert gaps[1].severity == 'critical'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_detect_symbol_gaps_market_hours_filter(self, gap_detector):
        """Test that market hours filtering works correctly"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        # Mock gap that occurs outside market hours
        weekend_time = datetime(2025, 1, 18, 20, 0, 0, tzinfo=timezone.utc)  # Saturday evening
        mock_conn.fetch.return_value = [
            {
                'prev_timestamp': weekend_time - timedelta(minutes=10),
                'timestamp': weekend_time,
                'gap_minutes': 10.0
            }
        ]

        gaps = await gap_detector._detect_symbol_gaps('polygon', 'AAPL')

        # Should filter out non-market hours gaps
        assert len(gaps) == 0

    def test_prioritize_gaps(self, gap_detector):
        """Test gap prioritization logic"""
        base_time = datetime.now(timezone.utc)

        gaps = [
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
                gap_duration_minutes=5, missing_bars_count=5,
                gap_type='temporary_delay', severity='low'
            ),
            DataGap(
                vendor='polygon', symbol='MSFT',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=20),
                gap_duration_minutes=20, missing_bars_count=20,
                gap_type='connection_loss', severity='critical'
            ),
            DataGap(
                vendor='tiingo', symbol='GOOGL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
                gap_duration_minutes=10, missing_bars_count=10,
                gap_type='api_error', severity='medium'
            )
        ]

        prioritized = gap_detector._prioritize_gaps(gaps)

        # Critical severity should come first
        assert prioritized[0].severity == 'critical'
        assert prioritized[0].symbol == 'MSFT'

        # Then medium severity
        assert prioritized[1].severity == 'medium'
        assert prioritized[1].symbol == 'GOOGL'

        # Finally low severity
        assert prioritized[2].severity == 'low'
        assert prioritized[2].symbol == 'AAPL'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_gaps(self, gap_detector):
        """Test storing detected gaps in database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        base_time = datetime.now(timezone.utc)
        gaps = [
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
                gap_duration_minutes=10, missing_bars_count=10,
                gap_type='api_error', severity='medium'
            ),
            DataGap(
                vendor='tiingo', symbol='MSFT',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
                gap_duration_minutes=5, missing_bars_count=5,
                gap_type='temporary_delay', severity='low'
            )
        ]

        await gap_detector._store_gaps(gaps)

        # Should insert one record per gap
        assert mock_conn.execute.call_count == 2
        assert gap_detector.gaps_detected == 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backfill_polygon_gap(self, gap_detector):
        """Test backfilling gap using Polygon API"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='polygon', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
            gap_duration_minutes=10, missing_bars_count=10,
            gap_type='api_error', severity='medium'
        )

        # Mock successful API response
        mock_response_data = {
            'results': [
                {
                    't': int((base_time + timedelta(minutes=1)).timestamp() * 1000),
                    'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000,
                    'vw': 150.5, 'n': 500
                },
                {
                    't': int((base_time + timedelta(minutes=2)).timestamp() * 1000),
                    'o': 151.0, 'h': 153.0, 'l': 150.0, 'c': 152.0, 'v': 1100000,
                    'vw': 151.5, 'n': 550
                }
            ]
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock _store_backfilled_data
        gap_detector._store_backfilled_data = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_polygon_gap(gap)

            assert success is True
            gap_detector._store_backfilled_data.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backfill_tiingo_gap(self, gap_detector):
        """Test backfilling gap using Tiingo API"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='tiingo', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
            gap_duration_minutes=5, missing_bars_count=5,
            gap_type='temporary_delay', severity='low'
        )

        # Mock successful API response
        mock_response_data = [
            {
                'date': (base_time + timedelta(minutes=1)).isoformat() + 'Z',
                'open': 150.0, 'high': 152.0, 'low': 149.0, 'close': 151.0, 'volume': 1000000
            },
            {
                'date': (base_time + timedelta(minutes=2)).isoformat() + 'Z',
                'open': 151.0, 'high': 153.0, 'low': 150.0, 'close': 152.0, 'volume': 1100000
            }
        ]

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock _store_backfilled_tiingo_data
        gap_detector._store_backfilled_tiingo_data = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_tiingo_gap(gap)

            assert success is True
            gap_detector._store_backfilled_tiingo_data.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backfill_fmp_gap(self, gap_detector):
        """Test backfilling gap using FMP API"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='fmp', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
            gap_duration_minutes=5, missing_bars_count=5,
            gap_type='temporary_delay', severity='low'
        )

        # Mock successful API response
        mock_response_data = [
            {
                'date': (base_time + timedelta(minutes=1)).isoformat() + 'Z',
                'open': 150.0, 'high': 152.0, 'low': 149.0, 'close': 151.0, 'volume': 1000000
            },
            {
                'date': (base_time + timedelta(minutes=2)).isoformat() + 'Z',
                'open': 151.0, 'high': 153.0, 'low': 150.0, 'close': 152.0, 'volume': 1100000
            }
        ]

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock _store_backfilled_fmp_data
        gap_detector._store_backfilled_fmp_data = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_fmp_gap(gap)

            assert success is True
            gap_detector._store_backfilled_fmp_data.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_backfilled_data(self, gap_detector):
        """Test storing backfilled data in database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        # Mock _get_instrument_id
        gap_detector._get_instrument_id = AsyncMock(return_value=123)

        results = [
            {
                't': int(datetime.now(timezone.utc).timestamp() * 1000),
                'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000,
                'vw': 150.5, 'n': 500
            }
        ]

        await gap_detector._store_backfilled_data('polygon', 'AAPL', results)

        # Should call execute once per result
        mock_conn.execute.assert_called_once()
        gap_detector._get_instrument_id.assert_called_once_with('AAPL')

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_instrument_id(self, gap_detector):
        """Test getting instrument ID for symbol"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        mock_conn.fetchval.return_value = 123

        instrument_id = await gap_detector._get_instrument_id('AAPL')

        assert instrument_id == 123
        mock_conn.fetchval.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_instrument_id_not_found(self, gap_detector):
        """Test getting instrument ID when symbol not found"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        mock_conn.fetchval.return_value = None

        instrument_id = await gap_detector._get_instrument_id('UNKNOWN')

        assert instrument_id == 0
        mock_conn.fetchval.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mark_gap_backfilled(self, gap_detector):
        """Test marking gap as successfully backfilled"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='polygon', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
            gap_duration_minutes=10, missing_bars_count=10,
            gap_type='api_error', severity='medium'
        )

        await gap_detector._mark_gap_backfilled(gap)

        mock_conn.execute.assert_called_once()

        # Check that the SQL contains expected status update
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'backfill_status' in sql_call
        assert 'completed' in sql_call

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mark_gap_failed(self, gap_detector):
        """Test marking gap backfill as failed"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='polygon', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
            gap_duration_minutes=10, missing_bars_count=10,
            gap_type='api_error', severity='medium'
        )

        error_message = "API rate limit exceeded"
        await gap_detector._mark_gap_failed(gap, error_message)

        mock_conn.execute.assert_called_once()

        # Check that the SQL contains expected status update
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'backfill_status' in sql_call
        assert 'failed' in sql_call

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_update_collection_status(self, gap_detector):
        """Test updating collection status based on detected gaps"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        base_time = datetime.now(timezone.utc)
        gaps = [
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
                gap_duration_minutes=10, missing_bars_count=10,
                gap_type='api_error', severity='medium'
            ),
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time - timedelta(minutes=30), gap_end=base_time - timedelta(minutes=25),
                gap_duration_minutes=5, missing_bars_count=5,
                gap_type='temporary_delay', severity='low'
            )
        ]

        await gap_detector._update_collection_status(gaps)

        # Should update collection status for the symbol/vendor combination
        mock_conn.execute.assert_called_once()

        # Check that the SQL contains expected updates
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'consecutive_missing_bars' in sql_call
        assert 'collection_health_score' in sql_call

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_gap_detection_complete_flow(self, gap_detector):
        """Test the complete gap detection flow"""
        # Mock all dependencies
        mock_pool = AsyncMock()
        gap_detector.pool = mock_pool

        gap_detector._detect_all_gaps = AsyncMock(return_value=[])
        gap_detector._store_gaps = AsyncMock()
        gap_detector._prioritize_gaps = AsyncMock(return_value=[])
        gap_detector._execute_backfills = AsyncMock()
        gap_detector._update_collection_status = AsyncMock()

        await gap_detector.run_gap_detection()

        # Verify all steps were called
        gap_detector._detect_all_gaps.assert_called_once()
        gap_detector._store_gaps.assert_called_once()
        gap_detector._prioritize_gaps.assert_called_once()
        gap_detector._execute_backfills.assert_called_once()
        gap_detector._update_collection_status.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_execute_backfills(self, gap_detector):
        """Test executing backfill operations"""
        base_time = datetime.now(timezone.utc)
        priority_gaps = [
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
                gap_duration_minutes=10, missing_bars_count=10,
                gap_type='api_error', severity='critical'
            ),
            DataGap(
                vendor='tiingo', symbol='MSFT',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
                gap_duration_minutes=5, missing_bars_count=5,
                gap_type='temporary_delay', severity='medium'
            )
        ]

        # Mock backfill methods
        gap_detector._backfill_gap = AsyncMock(return_value=True)
        gap_detector._mark_gap_backfilled = AsyncMock()
        gap_detector._mark_gap_failed = AsyncMock()

        await gap_detector._execute_backfills(priority_gaps)

        # Should attempt to backfill both gaps
        assert gap_detector._backfill_gap.call_count == 2
        assert gap_detector._mark_gap_backfilled.call_count == 2
        assert gap_detector._mark_gap_failed.call_count == 0

        # Check statistics
        assert gap_detector.gaps_backfilled == 2
        assert gap_detector.backfill_errors == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_shutdown(self, gap_detector):
        """Test graceful shutdown"""
        mock_pool = AsyncMock()
        gap_detector.pool = mock_pool

        await gap_detector.shutdown()
        mock_pool.close.assert_called_once()

class TestAPIErrorHandling:
    """Test API error handling scenarios"""

    @pytest.fixture
    def gap_detector(self):
        with patch('market_data.realtime.gap_detector.Environment'):
            with patch.dict(os.environ, {
                'POLYGON_API_KEY': 'test_key',
                'TIINGO_API_KEY': 'test_key',
                'FMP_API_KEY': 'test_key'
            }):
                detector = GapDetectionEngine()
                return detector

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_polygon_api_rate_limit(self, gap_detector):
        """Test handling Polygon API rate limits"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='polygon', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
            gap_duration_minutes=10, missing_bars_count=10,
            gap_type='api_error', severity='medium'
        )

        mock_response = AsyncMock()
        mock_response.status = 429  # Rate limit

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_polygon_gap(gap)
            assert success is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_api_server_error(self, gap_detector):
        """Test handling Tiingo API server errors"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='tiingo', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
            gap_duration_minutes=5, missing_bars_count=5,
            gap_type='temporary_delay', severity='low'
        )

        mock_response = AsyncMock()
        mock_response.status = 500  # Server error

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_tiingo_gap(gap)
            assert success is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fmp_api_authentication_error(self, gap_detector):
        """Test handling FMP API authentication errors"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='fmp', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
            gap_duration_minutes=5, missing_bars_count=5,
            gap_type='temporary_delay', severity='low'
        )

        mock_response = AsyncMock()
        mock_response.status = 401  # Unauthorized

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_fmp_gap(gap)
            assert success is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_network_timeout_handling(self, gap_detector):
        """Test handling network timeouts during backfill"""
        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='polygon', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
            gap_duration_minutes=10, missing_bars_count=10,
            gap_type='api_error', severity='medium'
        )

        mock_session = AsyncMock()
        mock_session.get.side_effect = asyncio.TimeoutError("Request timeout")

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_polygon_gap(gap)
            assert success is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_missing_api_key(self, gap_detector):
        """Test handling missing API keys"""
        gap_detector.polygon_api_key = None

        base_time = datetime.now(timezone.utc)
        gap = DataGap(
            vendor='polygon', symbol='AAPL',
            gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
            gap_duration_minutes=10, missing_bars_count=10,
            gap_type='api_error', severity='medium'
        )

        success = await gap_detector._backfill_polygon_gap(gap)
        assert success is False

class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    @pytest.fixture
    def gap_detector(self):
        with patch('market_data.realtime.gap_detector.Environment'):
            return GapDetectionEngine()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_empty_gaps_list(self, gap_detector):
        """Test handling empty gaps list"""
        gap_detector._store_gaps = AsyncMock()
        gap_detector._prioritize_gaps = AsyncMock(return_value=[])
        gap_detector._execute_backfills = AsyncMock()
        gap_detector._update_collection_status = AsyncMock()

        await gap_detector._store_gaps([])

        # Should handle empty list gracefully
        assert gap_detector.gaps_detected == 0

    def test_gap_prioritization_edge_cases(self, gap_detector):
        """Test gap prioritization with edge cases"""
        # Test empty list
        prioritized = gap_detector._prioritize_gaps([])
        assert prioritized == []

        # Test single gap
        base_time = datetime.now(timezone.utc)
        single_gap = [
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=5),
                gap_duration_minutes=5, missing_bars_count=5,
                gap_type='temporary_delay', severity='low'
            )
        ]

        prioritized = gap_detector._prioritize_gaps(single_gap)
        assert len(prioritized) == 1
        assert prioritized[0].symbol == 'AAPL'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_error_during_gap_storage(self, gap_detector):
        """Test handling database errors during gap storage"""
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        gap_detector.pool = mock_pool

        base_time = datetime.now(timezone.utc)
        gaps = [
            DataGap(
                vendor='polygon', symbol='AAPL',
                gap_start=base_time, gap_end=base_time + timedelta(minutes=10),
                gap_duration_minutes=10, missing_bars_count=10,
                gap_type='api_error', severity='medium'
            )
        ]

        # Should not raise exception, just handle gracefully
        await gap_detector._store_gaps(gaps)

        # Stats should still be updated
        assert gap_detector.gaps_detected == 1

if __name__ == '__main__':
    pytest.main([__file__, '-v'])