#!/usr/bin/env python3
"""
Comprehensive tests for the Real-Time Streaming Collector

Tests cover:
- Database initialization and connection handling
- Data collection and processing
- Quality scoring and validation
- Gap detection and error handling
- Market hours logic
- Vendor-specific data parsing
"""

import pytest
import asyncio
import asyncpg
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta, timezone
import json
import os
from dataclasses import asdict

# Import the module under test
import sys
sys.path.append('src')

from market_data.realtime.streaming_collector import (
    RealtimeStreamingCollector, 
    MinuteBar
)

class TestMinuteBar:
    """Test the MinuteBar data structure"""
    
    def test_minute_bar_creation(self):
        """Test creating a MinuteBar with all fields"""
        timestamp = datetime.now(timezone.utc)
        bar = MinuteBar(
            vendor='polygon',
            symbol='AAPL',
            instrument_id=123,
            timestamp=timestamp,
            open_price=150.0,
            high_price=152.0,
            low_price=149.0,
            close_price=151.0,
            volume=1000000,
            vwap=150.5,
            trade_count=500,
            received_at=timestamp + timedelta(seconds=30),
            data_latency_ms=30000,
            collection_method='websocket',
            quality_score=0.95
        )
        
        assert bar.vendor == 'polygon'
        assert bar.symbol == 'AAPL'
        assert bar.instrument_id == 123
        assert bar.timestamp == timestamp
        assert bar.open_price == 150.0
        assert bar.high_price == 152.0
        assert bar.low_price == 149.0
        assert bar.close_price == 151.0
        assert bar.volume == 1000000
        assert bar.vwap == 150.5
        assert bar.trade_count == 500
        assert bar.data_latency_ms == 30000
        assert bar.collection_method == 'websocket'
        assert bar.quality_score == 0.95
    
    def test_minute_bar_defaults(self):
        """Test MinuteBar creation with default values"""
        timestamp = datetime.now(timezone.utc)
        bar = MinuteBar(
            vendor='tiingo',
            symbol='MSFT',
            instrument_id=456,
            timestamp=timestamp,
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            volume=500000
        )
        
        assert bar.vwap is None
        assert bar.trade_count is None
        assert bar.received_at is None
        assert bar.data_latency_ms is None
        assert bar.collection_method == 'websocket'
        assert bar.quality_score == 0.8

class TestRealtimeStreamingCollector:
    """Test the main RealtimeStreamingCollector class"""
    
    @pytest.fixture
    def mock_env(self):
        """Mock environment configuration"""
        with patch('market_data.realtime.streaming_collector.Environment') as mock_env_class:
            mock_env = Mock()
            mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
            mock_env_class.return_value = mock_env
            yield mock_env
    
    @pytest.fixture
    def collector(self, mock_env):
        """Create a collector instance with mocked dependencies"""
        with patch.dict(os.environ, {
            'MAX_LATENCY_SECONDS': '120',
            'UNIVERSE_SIZE': '100',
            'MARKET_HOURS_ONLY': 'true',
            'POLYGON_API_KEY': 'test_polygon_key',
            'TIINGO_API_KEY': 'test_tiingo_key',
            'FMP_API_KEY': 'test_fmp_key'
        }):
            collector = RealtimeStreamingCollector()
            return collector
    
    def test_collector_initialization(self, collector):
        """Test collector initialization with environment variables"""
        assert collector.max_latency_seconds == 120
        assert collector.collection_universe_size == 100
        assert collector.market_hours_only is True
        assert collector.polygon_api_key == 'test_polygon_key'
        assert collector.tiingo_api_key == 'test_tiingo_key'
        assert collector.fmp_api_key == 'test_fmp_key'
    
    @pytest.mark.asyncio
    async def test_initialize_database_connection(self, collector, mock_env):
        """Test database initialization"""
        mock_pool = AsyncMock()
        
        with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
            with patch.object(collector, '_load_active_universe', new_callable=AsyncMock):
                with patch.object(collector, '_initialize_collection_status', new_callable=AsyncMock):
                    await collector.initialize()
                    
                    assert collector.pool == mock_pool
                    mock_env.get_database_url.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_load_active_universe(self, collector):
        """Test loading active universe from database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock database response
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL', 'instrument_id': 1},
            {'symbol': 'MSFT', 'instrument_id': 2},
            {'symbol': 'GOOGL', 'instrument_id': 3}
        ]
        
        collector.pool = mock_pool
        await collector._load_active_universe()
        
        assert 'AAPL' in collector.universe_symbols
        assert 'MSFT' in collector.universe_symbols
        assert 'GOOGL' in collector.universe_symbols
        assert collector.instrument_mapping['AAPL'] == 1
        assert collector.instrument_mapping['MSFT'] == 2
        assert collector.instrument_mapping['GOOGL'] == 3
    
    @pytest.mark.asyncio
    async def test_initialize_collection_status(self, collector):
        """Test initializing collection status for all vendors and symbols"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        collector.pool = mock_pool
        collector.universe_symbols = {'AAPL', 'MSFT'}
        
        await collector._initialize_collection_status()
        
        # Should call execute for each vendor/symbol combination
        assert mock_conn.execute.call_count == 6  # 2 symbols × 3 vendors
    
    def test_calculate_quality_score(self, collector):
        """Test quality score calculation logic"""
        # Test perfect data
        data = {'open': 100, 'high': 101, 'low': 99, 'close': 100.5, 'volume': 1000}
        score = collector._calculate_quality_score(data, latency_ms=30000)  # 30 seconds
        assert score == 1.0
        
        # Test high latency penalty
        score = collector._calculate_quality_score(data, latency_ms=120000)  # 2 minutes
        assert score == 0.7  # -0.3 penalty
        
        # Test missing fields penalty
        incomplete_data = {'open': 100, 'close': 100.5}  # Missing high, low, volume
        score = collector._calculate_quality_score(incomplete_data, latency_ms=30000)
        assert score == 0.7  # -0.3 for 3 missing fields
        
        # Test combined penalties
        score = collector._calculate_quality_score(incomplete_data, latency_ms=120000)
        assert score == 0.4  # -0.3 latency - 0.3 missing fields
        
        # Test extreme latency
        score = collector._calculate_quality_score(data, latency_ms=360000)  # 6 minutes
        assert score == 0.5  # -0.5 penalty
    
    @pytest.mark.asyncio
    async def test_process_polygon_minute_bar(self, collector):
        """Test processing Polygon minute bar data"""
        collector.universe_symbols = {'AAPL'}
        collector.instrument_mapping = {'AAPL': 123}
        
        # Mock store_minute_bar method
        collector._store_minute_bar = AsyncMock()
        
        # Sample Polygon WebSocket message
        polygon_data = {
            'ev': 'AM',  # Minute aggregate
            'sym': 'AAPL',
            't': int(datetime.now(timezone.utc).timestamp() * 1000),  # Current timestamp in ms
            'o': 150.0,
            'h': 152.0,
            'l': 149.0,
            'c': 151.0,
            'v': 1000000,
            'vw': 150.5,
            'n': 500
        }
        
        await collector._process_polygon_minute_bar(polygon_data)
        
        # Verify store_minute_bar was called with correct data
        collector._store_minute_bar.assert_called_once()
        bar = collector._store_minute_bar.call_args[0][0]
        
        assert bar.vendor == 'polygon'
        assert bar.symbol == 'AAPL'
        assert bar.instrument_id == 123
        assert bar.open_price == 150.0
        assert bar.high_price == 152.0
        assert bar.low_price == 149.0
        assert bar.close_price == 151.0
        assert bar.volume == 1000000
        assert bar.vwap == 150.5
        assert bar.trade_count == 500
        assert bar.collection_method == 'websocket'
    
    @pytest.mark.asyncio
    async def test_process_polygon_minute_bar_unknown_symbol(self, collector):
        """Test processing Polygon data for unknown symbol"""
        collector.universe_symbols = {'MSFT'}  # AAPL not in universe
        collector._store_minute_bar = AsyncMock()
        
        polygon_data = {
            'ev': 'AM',
            'sym': 'AAPL',  # Not in universe
            't': int(datetime.now(timezone.utc).timestamp() * 1000),
            'o': 150.0,
            'h': 152.0,
            'l': 149.0,
            'c': 151.0,
            'v': 1000000
        }
        
        await collector._process_polygon_minute_bar(polygon_data)
        
        # Should not store data for unknown symbol
        collector._store_minute_bar.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_store_minute_bar(self, collector):
        """Test storing minute bar in database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        collector.pool = mock_pool
        
        # Mock _update_collection_status
        collector._update_collection_status = AsyncMock()
        
        timestamp = datetime.now(timezone.utc)
        bar = MinuteBar(
            vendor='polygon',
            symbol='AAPL',
            instrument_id=123,
            timestamp=timestamp,
            open_price=150.0,
            high_price=152.0,
            low_price=149.0,
            close_price=151.0,
            volume=1000000,
            vwap=150.5,
            trade_count=500,
            received_at=timestamp + timedelta(seconds=30),
            data_latency_ms=30000,
            collection_method='websocket',
            quality_score=0.95
        )
        
        await collector._store_minute_bar(bar)
        
        # Verify database insert was called
        mock_conn.execute.assert_called_once()
        
        # Verify collection status was updated
        collector._update_collection_status.assert_called_once_with(bar)
        
        # Verify stats were incremented
        assert collector.collection_stats['bars_stored'] == 1
    
    @pytest.mark.asyncio
    async def test_update_collection_status(self, collector):
        """Test updating collection status"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        collector.pool = mock_pool
        
        timestamp = datetime.now(timezone.utc)
        bar = MinuteBar(
            vendor='polygon',
            symbol='AAPL',
            instrument_id=123,
            timestamp=timestamp,
            open_price=150.0,
            high_price=152.0,
            low_price=149.0,
            close_price=151.0,
            volume=1000000,
            data_latency_ms=30000
        )
        
        await collector._update_collection_status(bar)
        
        # Verify database update was called
        mock_conn.execute.assert_called_once()
        
        # Check that the SQL contains expected fields
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'last_received_timestamp' in sql_call
        assert 'data_delay_minutes' in sql_call
        assert 'collection_health_score' in sql_call
    
    def test_should_collect_now_market_hours(self, collector):
        """Test market hours collection logic"""
        collector.market_hours_only = True
        
        # Mock market hours check
        with patch('market_data.realtime.streaming_collector.is_market_open', return_value=True):
            assert collector.should_collect_now() is True
        
        with patch('market_data.realtime.streaming_collector.is_market_open', return_value=False):
            assert collector.should_collect_now() is False
    
    def test_should_collect_now_24_7_mode(self, collector):
        """Test 24/7 collection mode"""
        collector.market_hours_only = False
        
        # Should always collect in 24/7 mode
        with patch('market_data.realtime.streaming_collector.is_market_open', return_value=False):
            assert collector.should_collect_now() is True
    
    @pytest.mark.asyncio
    async def test_detect_gaps(self, collector):
        """Test gap detection logic"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        collector.pool = mock_pool
        
        # Mock gap detection query result
        mock_conn.fetch.return_value = [
            {
                'vendor': 'polygon',
                'symbol': 'AAPL',
                'last_received_timestamp': datetime.now(timezone.utc) - timedelta(minutes=10),
                'expected_timestamp': datetime.now(timezone.utc) - timedelta(minutes=9),
                'minutes_since_last': 10.0
            }
        ]
        
        # Mock _handle_detected_gap
        collector._handle_detected_gap = AsyncMock()
        
        await collector._detect_gaps()
        
        # Verify gap was detected and handled
        collector._handle_detected_gap.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_handle_detected_gap(self, collector):
        """Test handling detected gaps"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        collector.pool = mock_pool
        
        # Mock _trigger_backfill
        collector._trigger_backfill = AsyncMock()
        
        gap_data = {
            'vendor': 'polygon',
            'symbol': 'AAPL',
            'last_received_timestamp': datetime.now(timezone.utc) - timedelta(minutes=65),
            'minutes_since_last': 65.0
        }
        
        await collector._handle_detected_gap(gap_data)
        
        # Verify gap record was inserted
        mock_conn.execute.assert_called_once()
        
        # Verify backfill was triggered for large gap
        collector._trigger_backfill.assert_called_once()
        
        # Verify stats were updated
        assert collector.collection_stats['gaps_detected'] == 1
    
    @pytest.mark.asyncio
    async def test_shutdown(self, collector):
        """Test graceful shutdown"""
        # Set up mocked resources
        mock_pool = AsyncMock()
        mock_connection = AsyncMock()
        
        collector.pool = mock_pool
        collector.active_connections = {'test': mock_connection}
        collector.running = True
        
        await collector.shutdown()
        
        # Verify shutdown process
        assert collector.running is False
        mock_connection.close.assert_called_once()
        mock_pool.close.assert_called_once()

class TestPolygonIntegration:
    """Test Polygon-specific integration logic"""
    
    @pytest.fixture
    def collector(self):
        with patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'}):
            with patch('market_data.realtime.streaming_collector.Environment'):
                collector = RealtimeStreamingCollector()
                collector.universe_symbols = {'AAPL', 'MSFT'}
                collector.instrument_mapping = {'AAPL': 1, 'MSFT': 2}
                return collector
    
    @pytest.mark.asyncio
    async def test_polygon_websocket_stream_setup(self, collector):
        """Test Polygon WebSocket connection setup"""
        mock_websocket = AsyncMock()
        mock_websocket.send = AsyncMock()
        mock_websocket.__aiter__ = AsyncMock(return_value=iter([]))
        
        with patch('market_data.realtime.streaming_collector.websockets.connect', return_value=mock_websocket):
            try:
                await collector._polygon_websocket_stream()
            except:
                pass  # We expect this to fail in test environment
            
            # Verify authentication message was sent
            auth_calls = [call for call in mock_websocket.send.call_args_list 
                         if 'auth' in str(call)]
            assert len(auth_calls) >= 1
            
            # Verify subscription messages were sent
            subscribe_calls = [call for call in mock_websocket.send.call_args_list 
                              if 'subscribe' in str(call)]
            assert len(subscribe_calls) >= 1
    
    def test_polygon_message_parsing(self, collector):
        """Test parsing various Polygon message formats"""
        collector._store_minute_bar = AsyncMock()
        
        # Test single message
        single_message = {
            'ev': 'AM',
            'sym': 'AAPL',
            't': int(datetime.now(timezone.utc).timestamp() * 1000),
            'o': 150.0,
            'h': 152.0,
            'l': 149.0,
            'c': 151.0,
            'v': 1000000,
            'vw': 150.5,
            'n': 500
        }
        
        asyncio.run(collector._process_polygon_minute_bar(single_message))
        assert collector._store_minute_bar.call_count == 1
        
        # Reset mock
        collector._store_minute_bar.reset_mock()
        
        # Test array of messages
        message_array = [single_message, single_message.copy()]
        # This would be handled in the main message processing loop

class TestErrorHandling:
    """Test error handling and edge cases"""
    
    @pytest.fixture
    def collector(self):
        with patch('market_data.realtime.streaming_collector.Environment'):
            return RealtimeStreamingCollector()
    
    @pytest.mark.asyncio
    async def test_database_connection_failure(self, collector):
        """Test handling database connection failures"""
        with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', 
                   side_effect=Exception("Connection failed")):
            with pytest.raises(Exception, match="Connection failed"):
                await collector.initialize()
    
    @pytest.mark.asyncio
    async def test_malformed_polygon_data(self, collector):
        """Test handling malformed Polygon data"""
        collector.universe_symbols = {'AAPL'}
        collector.instrument_mapping = {'AAPL': 1}
        collector._store_minute_bar = AsyncMock()
        
        # Missing required fields
        malformed_data = {
            'ev': 'AM',
            'sym': 'AAPL'
            # Missing timestamp and price data
        }
        
        # Should not raise exception, just skip the data
        await collector._process_polygon_minute_bar(malformed_data)
        collector._store_minute_bar.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_store_minute_bar_database_error(self, collector):
        """Test handling database errors during storage"""
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        collector.pool = mock_pool
        
        timestamp = datetime.now(timezone.utc)
        bar = MinuteBar(
            vendor='polygon',
            symbol='AAPL',
            instrument_id=123,
            timestamp=timestamp,
            open_price=150.0,
            high_price=152.0,
            low_price=149.0,
            close_price=151.0,
            volume=1000000
        )
        
        # Should not raise exception, just log error
        await collector._store_minute_bar(bar)
        
        # Stats should not be incremented on error
        assert collector.collection_stats['bars_stored'] == 0

class TestMetricsAndStats:
    """Test metrics collection and statistics"""
    
    @pytest.fixture
    def collector(self):
        with patch('market_data.realtime.streaming_collector.Environment'):
            return RealtimeStreamingCollector()
    
    def test_initial_stats(self, collector):
        """Test initial statistics state"""
        expected_stats = {
            'bars_received': 0,
            'bars_stored': 0,
            'connection_errors': 0,
            'data_quality_failures': 0,
            'avg_latency_ms': 0.0,
            'gaps_detected': 0,
            'backfills_triggered': 0
        }
        
        assert collector.collection_stats == expected_stats
    
    @pytest.mark.asyncio
    async def test_stats_updates(self, collector):
        """Test that statistics are properly updated"""
        # Mock successful bar storage
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        collector.pool = mock_pool
        collector._update_collection_status = AsyncMock()
        
        timestamp = datetime.now(timezone.utc)
        bar = MinuteBar(
            vendor='polygon',
            symbol='AAPL',
            instrument_id=123,
            timestamp=timestamp,
            open_price=150.0,
            high_price=152.0,
            low_price=149.0,
            close_price=151.0,
            volume=1000000
        )
        
        await collector._store_minute_bar(bar)
        assert collector.collection_stats['bars_stored'] == 1
        
        # Test gap detection stats
        gap_data = {
            'vendor': 'polygon',
            'symbol': 'AAPL',
            'last_received_timestamp': datetime.now(timezone.utc) - timedelta(minutes=10),
            'minutes_since_last': 10.0
        }
        
        await collector._handle_detected_gap(gap_data)
        assert collector.collection_stats['gaps_detected'] == 1

if __name__ == '__main__':
    pytest.main([__file__, '-v'])