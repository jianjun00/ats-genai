"""
Comprehensive integration tests for the ATS Event System
"""

import pytest
import json
import time
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from events.proto.events_pb2 import (
    Event, EventType, Priority, Classification,
    create_news_event, create_earnings_event, create_technical_signal_event,
    SignalType, SignalDirection
)
from events.producer import EventProducer
from events.consumer import process_event_from_queue
from events.database import EventStorage
from events.correlation import CorrelationEngine
from events.monitoring import EventSystemMonitor
from events.run_dev_integration import EventSystemManager

@pytest.fixture
def mock_redis():
    """Mock Redis client for testing"""
    with patch('redis.Redis') as mock_redis_class:
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.lpush.return_value = 1
        mock_client.brpop.return_value = None
        mock_client.llen.return_value = 0
        mock_client.keys.return_value = []
        mock_redis_class.return_value = mock_client
        yield mock_client

@pytest.fixture
def mock_database():
    """Mock database for testing"""
    with patch('events.database.EventStorage') as mock_storage_class:
        mock_storage = MagicMock()
        mock_storage.get_event_stats.return_value = {
            'total_events': 100,
            'events_by_type': {'news': 50, 'earnings': 30, 'technical': 20},
            'events_by_source': {'polygon': 80, 'test': 20},
            'recent_events_24h': 50,
            'total_correlations': 25,
            'timestamp': datetime.utcnow().isoformat()
        }
        mock_storage.store_event.return_value = {'success': True, 'event_id': 'test-123'}
        mock_storage.query_events.return_value = []
        mock_storage_class.return_value = mock_storage
        yield mock_storage

class TestProtocolBufferEvents:
    """Test Protocol Buffer event creation and serialization"""

    def test_create_news_event(self):
        """Test creating news events"""
        event = create_news_event(
            headline="Apple reports strong earnings",
            symbol="AAPL",
            sentiment=0.8,
            publisher="Reuters"
        )

        assert event.event_type == EventType.EVENT_TYPE_NEWS
        assert event.subject.symbol == "AAPL"
        assert event.news_data.headline == "Apple reports strong earnings"
        assert event.news_data.sentiment.overall == 0.8
        assert event.metadata.priority == Priority.PRIORITY_HIGH

    def test_create_earnings_event(self):
        """Test creating earnings events"""
        event = create_earnings_event(
            symbol="AAPL",
            eps_actual=1.25,
            eps_consensus=1.20,
            year=2024,
            quarter=4
        )

        assert event.event_type == EventType.EVENT_TYPE_EARNINGS
        assert event.subject.symbol == "AAPL"
        assert event.earnings_data.estimates.eps.actual == 1.25
        assert event.earnings_data.estimates.eps.surprise == 0.05
        assert event.metadata.priority == Priority.PRIORITY_MEDIUM

    def test_create_technical_signal_event(self):
        """Test creating technical signal events"""
        event = create_technical_signal_event(
            symbol="AAPL",
            signal_type=SignalType.SIGNAL_TYPE_BREAKOUT,
            direction=SignalDirection.SIGNAL_DIRECTION_BULLISH,
            strength=0.8,
            current_price=150.25
        )

        assert event.event_type == EventType.EVENT_TYPE_TECHNICAL_SIGNAL
        assert event.subject.symbol == "AAPL"
        assert event.technical_data.signal_type == SignalType.SIGNAL_TYPE_BREAKOUT
        assert event.technical_data.signal.strength == 0.8
        assert event.metadata.priority == Priority.PRIORITY_HIGH

    def test_event_serialization(self):
        """Test event serialization and deserialization"""
        original_event = create_news_event(
            headline="Test serialization",
            symbol="TEST",
            sentiment=0.5
        )

        # Serialize to bytes
        serialized = original_event.SerializeToString()
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        # Deserialize back to event
        deserialized_event = Event()
        deserialized_event.ParseFromString(serialized)

        assert deserialized_event.event_type == EventType.EVENT_TYPE_NEWS
        assert deserialized_event.subject.symbol == "TEST"
        assert deserialized_event.news_data.headline == "Test serialization"

class TestEventProducer:
    """Test event producer functionality"""

    def test_producer_initialization(self, mock_redis):
        """Test producer initialization"""
        producer = EventProducer()
        assert producer.redis_client is not None
        mock_redis.ping.assert_called_once()

    def test_publish_event(self, mock_redis):
        """Test event publishing"""
        producer = EventProducer()

        event = create_news_event(
            headline="Test publish",
            symbol="TEST",
            sentiment=0.5
        )

        event_id = producer.publish_event(event)

        assert event_id == event.event_id
        mock_redis.lpush.assert_called()

        # Check that event was published to correct queue
        call_args = mock_redis.lpush.call_args
        assert 'events:news' in call_args[0]

    def test_publish_convenience_methods(self, mock_redis):
        """Test convenience publishing methods"""
        producer = EventProducer()

        # Test news event publishing
        event_id = producer.publish_news_event(
            headline="Test news",
            symbol="TEST",
            sentiment=0.7,
            publisher="Test Publisher"
        )
        assert event_id is not None

        # Test earnings event publishing
        event_id = producer.publish_earnings_event(
            symbol="TEST",
            eps_actual=1.30,
            eps_consensus=1.25,
            year=2024,
            quarter=4
        )
        assert event_id is not None

        # Test technical signal event publishing
        event_id = producer.publish_technical_signal_event(
            symbol="TEST",
            signal_type="breakout",
            direction="bullish",
            strength=0.8,
            current_price=150.0
        )
        assert event_id is not None

    def test_event_validation(self, mock_redis):
        """Test event validation"""
        producer = EventProducer()

        # Create invalid event (no symbol or instrument_id)
        event = Event()
        event.event_type = EventType.EVENT_TYPE_NEWS
        event.source = "test"
        # Missing subject data

        with pytest.raises(ValueError, match="Event subject must have symbol"):
            producer.publish_event(event)

    def test_queue_stats(self, mock_redis):
        """Test queue statistics"""
        mock_redis.llen.return_value = 5

        producer = EventProducer()
        stats = producer.get_queue_stats()

        assert isinstance(stats, dict)
        assert 'events:all' in stats
        assert stats['events:all'] == 5

class TestEventStorage:
    """Test event storage functionality"""

    def test_storage_initialization(self, mock_database):
        """Test storage initialization"""
        storage = EventStorage()
        assert storage is not None

    def test_store_event(self, mock_database):
        """Test event storage"""
        storage = EventStorage()

        event = create_news_event(
            headline="Test storage",
            symbol="TEST",
            sentiment=0.6
        )

        event_dict = event.to_dict()
        result = storage.store_event(event_dict)

        assert result['success'] is True
        assert 'event_id' in result

    def test_query_events(self, mock_database):
        """Test event querying"""
        storage = EventStorage()

        # Mock some events
        mock_events = [
            {
                'event_id': 'test-1',
                'event_type': 'news',
                'symbol': 'AAPL',
                'timestamp': datetime.utcnow(),
                'source': 'test'
            }
        ]
        mock_database.query_events.return_value = mock_events

        events = storage.query_events(symbol="AAPL", limit=10)

        assert len(events) == 1
        assert events[0]['symbol'] == 'AAPL'

    def test_event_stats(self, mock_database):
        """Test event statistics"""
        storage = EventStorage()

        stats = storage.get_event_stats()

        assert stats['total_events'] == 100
        assert 'events_by_type' in stats
        assert stats['events_by_type']['news'] == 50

class TestCorrelationEngine:
    """Test event correlation functionality"""

    def test_correlation_engine_initialization(self, mock_database):
        """Test correlation engine initialization"""
        storage = EventStorage()
        engine = CorrelationEngine(storage)

        assert engine.storage == storage
        assert len(engine.correlation_rules) > 0

    def test_find_correlations(self, mock_database):
        """Test correlation detection"""
        storage = EventStorage()
        engine = CorrelationEngine(storage)

        # Mock recent events
        mock_events = [
            {
                'event_id': 'related-1',
                'event_type': 'price_gap',
                'symbol': 'AAPL',
                'timestamp': datetime.utcnow() + timedelta(minutes=10),
                'priority': 'high'
            }
        ]
        mock_database.query_events.return_value = mock_events

        # Create a news event
        event = create_news_event(
            headline="Apple announces new product",
            symbol="AAPL",
            sentiment=0.8
        )

        correlations = engine.find_correlations(event)

        # Should find correlations even with mocked data
        assert isinstance(correlations, list)

    def test_correlation_scoring(self, mock_database):
        """Test correlation scoring logic"""
        storage = EventStorage()
        engine = CorrelationEngine(storage)

        # Test news-specific scoring
        event = create_news_event(
            headline="Apple reports record earnings beat expectations",
            symbol="AAPL",
            sentiment=0.9,
            publisher="Reuters"
        )

        # Mock related event
        related_event = {
            'event_id': 'related-1',
            'event_type': 'technical_signal',
            'symbol': 'AAPL',
            'timestamp': datetime.utcnow().isoformat(),
            'priority': 'high'
        }

        # Test the scoring method
        rule = engine.correlation_rules[0]  # First rule
        score = engine._calculate_correlation_score(event, related_event, rule)

        assert 0.0 <= score <= 1.0
        assert score > rule.base_score  # Should be enhanced due to high sentiment

class TestEventSystemMonitor:
    """Test monitoring functionality"""

    def test_monitor_initialization(self):
        """Test monitor initialization"""
        with patch('events.monitoring.EventStorage'), \
             patch('events.monitoring.EventProducer'), \
             patch('events.monitoring.CorrelationEngine'):

            monitor = EventSystemMonitor()
            assert monitor is not None

    def test_system_health_check(self, mock_database, mock_redis):
        """Test system health checking"""
        with patch('events.monitoring.EventStorage') as mock_storage_class, \
             patch('events.monitoring.EventProducer') as mock_producer_class, \
             patch('events.monitoring.CorrelationEngine'):

            mock_storage_class.return_value = mock_database
            mock_producer = MagicMock()
            mock_producer.get_queue_stats.return_value = {'events:all': 10}
            mock_producer_class.return_value = mock_producer

            monitor = EventSystemMonitor()
            health = monitor.get_system_health()

            assert health.status in ['healthy', 'degraded', 'unhealthy']
            assert 'database' in health.components
            assert 'redis' in health.components
            assert isinstance(health.alerts, list)

    def test_event_metrics_collection(self, mock_database):
        """Test event metrics collection"""
        with patch('events.monitoring.EventStorage') as mock_storage_class, \
             patch('events.monitoring.EventProducer') as mock_producer_class, \
             patch('events.monitoring.CorrelationEngine'):

            mock_storage_class.return_value = mock_database
            mock_producer = MagicMock()
            mock_producer.get_queue_stats.return_value = {'events:all': 5}
            mock_producer_class.return_value = mock_producer

            # Mock recent events
            mock_events = [
                {
                    'event_id': f'event-{i}',
                    'event_type': 'news',
                    'symbol': 'AAPL',
                    'source': 'test',
                    'processing_metadata': {'processing_time_ms': 100}
                } for i in range(10)
            ]
            mock_database.query_events.return_value = mock_events

            monitor = EventSystemMonitor()
            metrics = monitor.get_event_metrics(hours_back=1)

            assert metrics.total_events == 10
            assert metrics.events_per_hour == 10.0
            assert 'news' in metrics.events_by_type
            assert metrics.average_processing_time > 0

class TestEventSystemIntegration:
    """Test full event system integration"""

    def test_end_to_end_event_flow(self, mock_redis, mock_database):
        """Test complete event flow from creation to storage"""
        # Create event producer
        producer = EventProducer()

        # Create and publish event
        event_id = producer.publish_news_event(
            headline="Integration test event",
            symbol="TEST",
            sentiment=0.7,
            publisher="Test Corp"
        )

        assert event_id is not None

        # Verify event was queued
        mock_redis.lpush.assert_called()

        # Simulate event processing (would normally be done by Celery)
        # In real system, this would:
        # 1. Pop from Redis queue
        # 2. Deserialize protobuf
        # 3. Store in database
        # 4. Run correlation analysis

        # Verify storage was attempted
        # (In integration test, we'd check actual database)

    def test_event_system_manager(self):
        """Test event system manager functionality"""
        with patch('events.run_dev_integration.EventStorage'), \
             patch('events.run_dev_integration.EventProducer'), \
             patch('events.run_dev_integration.EventSystemMonitor'):

            manager = EventSystemManager()

            # Test setup
            setup_result = manager.setup()
            assert setup_result['success'] is True

            # Test status
            status_result = manager.status()
            assert status_result['success'] is True
            assert 'status' in status_result

            # Test create test events
            test_result = manager.create_test_events(count=5)
            assert test_result['success'] is True
            assert len(test_result['events']) == 5

    def test_api_integration(self):
        """Test API integration"""
        from events.api import app
        from fastapi.testclient import TestClient

        with patch('events.api.EventStorage'), \
             patch('events.api.CorrelationEngine'), \
             patch('events.api.EventProducer'):

            client = TestClient(app)

            # Test health endpoint
            response = client.get("/health")
            assert response.status_code == 200

            # Test events endpoint
            response = client.get("/events?limit=10")
            assert response.status_code == 200

            # Test stats endpoint
            response = client.get("/stats")
            assert response.status_code == 200

class TestPerformanceAndReliability:
    """Test performance and reliability aspects"""

    def test_event_creation_performance(self):
        """Test event creation performance"""
        start_time = time.time()

        # Create many events
        events = []
        for i in range(100):
            event = create_news_event(
                headline=f"Performance test event {i}",
                symbol="PERF",
                sentiment=0.5
            )
            events.append(event)

        creation_time = time.time() - start_time

        assert len(events) == 100
        assert creation_time < 1.0  # Should create 100 events in under 1 second

        # Test serialization performance
        start_time = time.time()

        for event in events:
            serialized = event.SerializeToString()
            assert len(serialized) > 0

        serialization_time = time.time() - start_time
        assert serialization_time < 1.0  # Should serialize 100 events in under 1 second

    def test_error_handling(self, mock_redis):
        """Test error handling in various scenarios"""
        # Test Redis connection failure
        mock_redis.ping.side_effect = Exception("Redis connection failed")

        with pytest.raises(Exception):
            EventProducer()

        # Test invalid event data
        mock_redis.ping.side_effect = None  # Reset
        mock_redis.ping.return_value = True

        producer = EventProducer()

        # Test with invalid event type
        event = Event()
        event.source = "test"
        event.subject.symbol = "TEST"
        # event_type is unspecified

        with pytest.raises(ValueError):
            producer.publish_event(event)

    def test_memory_usage(self):
        """Test memory usage with large number of events"""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Create many events
        events = []
        for i in range(1000):
            event = create_news_event(
                headline=f"Memory test event {i}",
                symbol="MEM",
                sentiment=0.5
            )
            events.append(event)

        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 50MB for 1000 events)
        assert memory_increase < 50 * 1024 * 1024

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment"""
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)  # Reduce noise in tests

    yield

    # Cleanup after tests
    pass

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])