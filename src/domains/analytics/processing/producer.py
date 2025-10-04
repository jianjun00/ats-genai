"""
Event Producer - Redis-based event publishing for ATS
"""

import redis
import uuid
import logging
from datetime import datetime
from typing import Dict

from domains.analytics.events.proto.events_pb2 import (
    Event, EventType, create_news_event, create_earnings_event,
    create_technical_signal_event
)

logger = logging.getLogger(__name__)

class EventProducer:
    """Redis-based event producer for publishing events to queues"""

    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0):
        """Initialize event producer with Redis connection"""
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=False)
        self._test_connection()

    def _test_connection(self):
        """Test Redis connection on initialization"""
        try:
            self.redis_client.ping()
            logger.info("✅ Connected to Redis successfully")
        except redis.ConnectionError as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise

    def publish_event(self, event: Event) -> str:
        """
        Publish event to Redis queue with Protocol Buffer serialization

        Args:
            event: Event instance to publish

        Returns:
            str: Event ID of published event

        Raises:
            ValueError: If event validation fails
            redis.RedisError: If Redis operation fails
        """

        # 1. Set system metadata if not already set
        if not event.event_id:
            event.event_id = str(uuid.uuid4())

        if not event.ingestion_time:
            event.ingestion_time = datetime.utcnow()

        # 2. Validate event (basic validation)
        self._validate_event(event)

        # 3. Determine queue name based on event type
        queue_name = self._get_queue_name(event.event_type)

        # 4. Serialize to bytes and publish to Redis
        try:
            serialized_event = event.SerializeToString()

            # Use LPUSH to add to queue (consumers will use BRPOP)
            result = self.redis_client.lpush(queue_name, serialized_event)

            # Also add to a general events list for monitoring
            self.redis_client.lpush("events:all", serialized_event)

            logger.info(f"📤 Published event {event.event_id} to queue {queue_name}")

            # Trim queues to prevent memory issues (keep last 10000 events)
            self.redis_client.ltrim(queue_name, 0, 9999)
            self.redis_client.ltrim("events:all", 0, 9999)

            return event.event_id

        except redis.RedisError as e:
            logger.error(f"❌ Failed to publish event {event.event_id}: {e}")
            raise

    def _validate_event(self, event: Event):
        """Basic event validation"""
        if not event.event_id:
            raise ValueError("Event ID is required")

        if not event.subject.symbol and not event.subject.instrument_id:
            raise ValueError("Event subject must have symbol or instrument_id")

        if event.event_type == EventType.EVENT_TYPE_UNSPECIFIED:
            raise ValueError("Event type must be specified")

        if not event.source:
            raise ValueError("Event source is required")

        # Type-specific validation
        if event.event_type == EventType.EVENT_TYPE_NEWS:
            if not event.news_data or not event.news_data.headline:
                raise ValueError("News events must have headline")

        elif event.event_type == EventType.EVENT_TYPE_EARNINGS:
            if not event.earnings_data:
                raise ValueError("Earnings events must have earnings data")

        elif event.event_type == EventType.EVENT_TYPE_TECHNICAL_SIGNAL:
            if not event.technical_data or not event.technical_data.indicator:
                raise ValueError("Technical signal events must have indicator")

    def _get_queue_name(self, event_type: EventType) -> str:
        """Generate Redis queue name based on event type"""
        type_name = event_type.name.lower().replace('event_type_', '')
        return f"events:{type_name}"

    def publish_news_event(self, headline: str, symbol: str, sentiment: float = 0.0,
                          publisher: str = "unknown", url: str = "", source: str = "polygon") -> str:
        """
        Convenience method to publish news events

        Args:
            headline: News headline
            symbol: Stock symbol
            sentiment: Sentiment score (-1.0 to 1.0)
            publisher: News publisher
            url: News URL
            source: Data source

        Returns:
            str: Event ID of published event
        """
        event = create_news_event(headline, symbol, sentiment, publisher, url)
        event.source = source
        return self.publish_event(event)

    def publish_earnings_event(self, symbol: str, eps_actual: float, eps_consensus: float,
                              year: int, quarter: int, source: str = "polygon") -> str:
        """
        Convenience method to publish earnings events

        Args:
            symbol: Stock symbol
            eps_actual: Actual EPS reported
            eps_consensus: Consensus EPS estimate
            year: Earnings year
            quarter: Earnings quarter
            source: Data source

        Returns:
            str: Event ID of published event
        """
        event = create_earnings_event(symbol, eps_actual, eps_consensus, year, quarter)
        event.source = source
        return self.publish_event(event)

    def publish_technical_signal_event(self, symbol: str, signal_type: str, direction: str,
                                     strength: float, current_price: float,
                                     indicator: str = "RSI", source: str = "ats-internal") -> str:
        """
        Convenience method to publish technical signal events

        Args:
            symbol: Stock symbol
            signal_type: Type of signal (breakout, breakdown, etc.)
            direction: Signal direction (bullish, bearish, neutral)
            strength: Signal strength (0.0-1.0)
            current_price: Current price when signal generated
            indicator: Technical indicator name
            source: Data source

        Returns:
            str: Event ID of published event
        """
        from domains.analytics.events.proto.events_pb2 import SignalType, SignalDirection

        # Convert string enums to proper enum values
        signal_type_enum = getattr(SignalType, f"SIGNAL_TYPE_{signal_type.upper()}", SignalType.SIGNAL_TYPE_UNSPECIFIED)
        direction_enum = getattr(SignalDirection, f"SIGNAL_DIRECTION_{direction.upper()}", SignalDirection.SIGNAL_DIRECTION_UNSPECIFIED)

        event = create_technical_signal_event(symbol, signal_type_enum, direction_enum,
                                            strength, current_price, indicator)
        event.source = source
        return self.publish_event(event)

    def get_queue_stats(self) -> Dict[str, int]:
        """Get current queue statistics"""
        stats = {}

        # Common queue names
        queue_names = [
            "events:all",
            "events:news",
            "events:earnings",
            "events:technical_signal",
            "events:corporate_action",
            "events:economic_indicator"
        ]

        for queue_name in queue_names:
            try:
                length = self.redis_client.llen(queue_name)
                stats[queue_name] = length
            except redis.RedisError:
                stats[queue_name] = -1  # Indicate error

        return stats

    def clear_queue(self, queue_name: str) -> bool:
        """Clear specific queue (useful for testing)"""
        try:
            result = self.redis_client.delete(queue_name)
            logger.info(f"🗑️ Cleared queue {queue_name}")
            return result > 0
        except redis.RedisError as e:
            logger.error(f"❌ Failed to clear queue {queue_name}: {e}")
            return False

    def clear_all_queues(self) -> bool:
        """Clear all event queues (useful for testing)"""
        try:
            pattern = "events:*"
            keys = self.redis_client.keys(pattern)
            if keys:
                result = self.redis_client.delete(*keys)
                logger.info(f"🗑️ Cleared {len(keys)} event queues")
                return result > 0
            return True
        except redis.RedisError as e:
            logger.error(f"❌ Failed to clear event queues: {e}")
            return False

    def close(self):
        """Close Redis connection"""
        if self.redis_client:
            self.redis_client.close()
            logger.info("🔒 Redis connection closed")


# Convenience function for quick event publishing
def publish_news(headline: str, symbol: str, sentiment: float = 0.0, **kwargs) -> str:
    """Quick function to publish news event"""
    producer = EventProducer()
    try:
        return producer.publish_news_event(headline, symbol, sentiment, **kwargs)
    finally:
        producer.close()


def publish_earnings(symbol: str, eps_actual: float, eps_consensus: float,
                    year: int, quarter: int, **kwargs) -> str:
    """Quick function to publish earnings event"""
    producer = EventProducer()
    try:
        return producer.publish_earnings_event(symbol, eps_actual, eps_consensus,
                                             year, quarter, **kwargs)
    finally:
        producer.close()


# Example usage and testing
if __name__ == "__main__":
    pass

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create producer
    producer = EventProducer()

    try:
        # Test publishing different types of events
        print("🧪 Testing event publishing...")

        # 1. News event
        news_event_id = producer.publish_news_event(
            headline="Apple Reports Strong Q4 Earnings, Beats Expectations",
            symbol="AAPL",
            sentiment=0.8,
            publisher="Bloomberg",
            url="https://bloomberg.com/apple-earnings"
        )
        print(f"✅ Published news event: {news_event_id}")

        # 2. Earnings event
        earnings_event_id = producer.publish_earnings_event(
            symbol="AAPL",
            eps_actual=1.25,
            eps_consensus=1.20,
            year=2024,
            quarter=4
        )
        print(f"✅ Published earnings event: {earnings_event_id}")

        # 3. Technical signal event
        signal_event_id = producer.publish_technical_signal_event(
            symbol="AAPL",
            signal_type="breakout",
            direction="bullish",
            strength=0.8,
            current_price=150.25,
            indicator="RSI"
        )
        print(f"✅ Published technical signal event: {signal_event_id}")

        # 4. Show queue stats
        stats = producer.get_queue_stats()
        print(f"📊 Queue statistics:")
        for queue, count in stats.items():
            print(f"   {queue}: {count} events")

    except Exception as e:
        print(f"❌ Error during testing: {e}")

    finally:
        producer.close()