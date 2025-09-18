"""
run_dev Integration - Commands for event system management
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from src.domains.analytics.events.database import EventStorage
from src.domains.analytics.events.producer import EventProducer
from src.domains.analytics.events.consumer import celery_app, process_event_from_queue
from src.domains.analytics.events.monitoring import EventSystemMonitor

logger = logging.getLogger(__name__)

class EventSystemManager:
    """Manager for event system operations via run_dev"""

    def __init__(self):
        """Initialize event system manager"""
        self.storage = None
        self.producer = None
        self.monitor = None

    def _ensure_connections(self):
        """Ensure all connections are established"""
        if not self.storage:
            self.storage = EventStorage()
        if not self.producer:
            self.producer = EventProducer()
        if not self.monitor:
            self.monitor = EventSystemMonitor()

    def setup(self) -> Dict[str, Any]:
        """Setup event system (called by run_dev setup)"""
        try:
            logger.info("🚀 Setting up ATS Event System...")

            self._ensure_connections()

            # Test database connection
            db_stats = self.storage.get_event_stats()
            if 'error' in db_stats:
                return {
                    'success': False,
                    'error': f"Database setup failed: {db_stats['error']}",
                    'component': 'database'
                }

            # Test Redis connection
            queue_stats = self.producer.get_queue_stats()
            if any(depth < 0 for depth in queue_stats.values()):
                return {
                    'success': False,
                    'error': "Redis connection failed",
                    'component': 'redis'
                }

            # Setup complete
            setup_info = {
                'database': {
                    'status': 'connected',
                    'total_events': db_stats.get('total_events', 0)
                },
                'redis': {
                    'status': 'connected',
                    'queues': len(queue_stats)
                },
                'timestamp': datetime.utcnow().isoformat()
            }

            logger.info("✅ Event system setup complete")
            return {
                'success': True,
                'message': 'Event system initialized successfully',
                'setup_info': setup_info
            }

        except Exception as e:
            logger.error(f"❌ Event system setup failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'component': 'setup'
            }

    def start_api_server(self, host: str = "0.0.0.0", port: int = 8000) -> Dict[str, Any]:
        """Start the event API server"""
        try:
            import uvicorn

            logger.info(f"🌐 Starting Event API server on {host}:{port}")

            # Run server (this will block)
            uvicorn.run(
                "events.api:app",
                host=host,
                port=port,
                log_level="info",
                reload=False
            )

            return {'success': True, 'message': f'API server started on {host}:{port}'}

        except Exception as e:
            logger.error(f"❌ Failed to start API server: {e}")
            return {'success': False, 'error': str(e)}

    def start_celery_worker(self, concurrency: int = 4) -> Dict[str, Any]:
        """Start Celery worker for event processing"""
        try:
            logger.info(f"👷 Starting Celery worker with concurrency={concurrency}")

            # Start Celery worker
            celery_app.worker_main([
                'worker',
                '--loglevel=info',
                f'--concurrency={concurrency}',
                '--queues=event_processing,batch_processing,scheduled_tasks'
            ])

            return {'success': True, 'message': f'Celery worker started with {concurrency} processes'}

        except Exception as e:
            logger.error(f"❌ Failed to start Celery worker: {e}")
            return {'success': False, 'error': str(e)}

    def start_celery_beat(self) -> Dict[str, Any]:
        """Start Celery Beat scheduler"""
        try:
            logger.info("⏰ Starting Celery Beat scheduler")

            celery_app.start(['celery', 'beat', '--loglevel=info'])

            return {'success': True, 'message': 'Celery Beat scheduler started'}

        except Exception as e:
            logger.error(f"❌ Failed to start Celery Beat: {e}")
            return {'success': False, 'error': str(e)}

    def status(self) -> Dict[str, Any]:
        """Get event system status"""
        try:
            self._ensure_connections()

            health = self.monitor.get_system_health()
            metrics = self.monitor.get_event_metrics(hours_back=1)

            status_info = {
                'overall_status': health.status,
                'uptime_seconds': health.uptime_seconds,
                'components': {
                    'database': health.components.get('database', {}).get('status', 'unknown'),
                    'redis': health.components.get('redis', {}).get('status', 'unknown'),
                    'processing': health.components.get('processing', {}).get('status', 'unknown')
                },
                'metrics': {
                    'total_events': metrics.total_events,
                    'events_per_hour': metrics.events_per_hour,
                    'queue_depth': sum(metrics.queue_depths.values()),
                    'error_rate': metrics.error_rate
                },
                'alerts': health.alerts[:5],  # First 5 alerts
                'timestamp': datetime.utcnow().isoformat()
            }

            return {
                'success': True,
                'status': status_info
            }

        except Exception as e:
            logger.error(f"❌ Failed to get system status: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def query_events(self, symbol: str = None, event_type: str = None,
                    hours_back: int = 24, limit: int = 100) -> Dict[str, Any]:
        """Query events via run_dev"""
        try:
            self._ensure_connections()

            after_timestamp = datetime.utcnow() - timedelta(hours=hours_back)

            events = self.storage.query_events(
                symbol=symbol,
                event_type=event_type,
                after_timestamp=after_timestamp,
                limit=limit
            )

            # Convert events for display
            display_events = []
            for event in events:
                display_events.append({
                    'event_id': event['event_id'],
                    'type': event['event_type'],
                    'symbol': event.get('symbol', 'N/A'),
                    'timestamp': event['timestamp'].isoformat(),
                    'source': event['source'],
                    'priority': event['priority']
                })

            return {
                'success': True,
                'query': {
                    'symbol': symbol,
                    'event_type': event_type,
                    'hours_back': hours_back,
                    'limit': limit
                },
                'results': {
                    'count': len(display_events),
                    'events': display_events
                }
            }

        except Exception as e:
            logger.error(f"❌ Failed to query events: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def create_test_events(self, count: int = 10) -> Dict[str, Any]:
        """Create test events for development"""
        try:
            self._ensure_connections()

            created_events = []
            symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

            for i in range(count):
                symbol = symbols[i % len(symbols)]

                if i % 3 == 0:
                    # Create news event
                    event_id = self.producer.publish_news_event(
                        headline=f"Test news for {symbol} #{i+1}",
                        symbol=symbol,
                        sentiment=0.5,
                        publisher="test-publisher",
                        source="test"
                    )
                elif i % 3 == 1:
                    # Create earnings event
                    event_id = self.producer.publish_earnings_event(
                        symbol=symbol,
                        eps_actual=1.25 + i * 0.1,
                        eps_consensus=1.20,
                        year=2024,
                        quarter=4,
                        source="test"
                    )
                else:
                    # Create technical signal event
                    event_id = self.producer.publish_technical_signal_event(
                        symbol=symbol,
                        signal_type="breakout",
                        direction="bullish",
                        strength=0.7 + i * 0.02,
                        current_price=150.0 + i,
                        source="test"
                    )

                created_events.append({
                    'event_id': event_id,
                    'symbol': symbol,
                    'type': ['news', 'earnings', 'technical'][i % 3]
                })

            return {
                'success': True,
                'message': f'Created {count} test events',
                'events': created_events
            }

        except Exception as e:
            logger.error(f"❌ Failed to create test events: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def process_pending_events(self, batch_size: int = 100) -> Dict[str, Any]:
        """Process pending events in queues"""
        try:
            self._ensure_connections()

            queue_stats = self.producer.get_queue_stats()
            total_pending = sum(depth for depth in queue_stats.values() if depth > 0)

            if total_pending == 0:
                return {
                    'success': True,
                    'message': 'No pending events to process',
                    'processed': 0
                }

            # Process events from priority queues
            priority_queues = [
                'events:news',
                'events:earnings',
                'events:technical_signal',
                'events:corporate_action'
            ]

            processed_count = 0
            for queue_name in priority_queues:
                queue_depth = queue_stats.get(queue_name, 0)
                if queue_depth > 0:
                    # Process up to batch_size events from this queue
                    process_count = min(queue_depth, batch_size)

                    for _ in range(process_count):
                        result = process_event_from_queue.delay(queue_name).get(timeout=60)
                        if result['status'] == 'success':
                            processed_count += 1
                        elif result['status'] == 'no_events':
                            break

            return {
                'success': True,
                'message': f'Processed {processed_count} events',
                'processed': processed_count,
                'remaining': sum(self.producer.get_queue_stats().values()) - processed_count
            }

        except Exception as e:
            logger.error(f"❌ Failed to process events: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def clear_queues(self, confirm: bool = False) -> Dict[str, Any]:
        """Clear all event queues (development only)"""
        if not confirm:
            return {
                'success': False,
                'error': 'Must set confirm=True to clear queues',
                'warning': 'This will delete all pending events!'
            }

        try:
            self._ensure_connections()

            success = self.producer.clear_all_queues()

            return {
                'success': success,
                'message': 'All event queues cleared' if success else 'Failed to clear queues',
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Failed to clear queues: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive event system statistics"""
        try:
            self._ensure_connections()

            db_stats = self.storage.get_event_stats()
            queue_stats = self.producer.get_queue_stats()
            metrics = self.monitor.get_event_metrics(hours_back=24)

            return {
                'success': True,
                'stats': {
                    'database': db_stats,
                    'queues': queue_stats,
                    'metrics': {
                        'total_events_24h': metrics.total_events,
                        'events_per_hour': metrics.events_per_hour,
                        'events_by_type': metrics.events_by_type,
                        'events_by_source': metrics.events_by_source,
                        'average_processing_time_ms': metrics.average_processing_time,
                        'error_rate': metrics.error_rate
                    }
                },
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Failed to get stats: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def cleanup(self) -> Dict[str, Any]:
        """Cleanup event system resources"""
        try:
            if self.storage:
                self.storage.close()
            if self.producer:
                self.producer.close()
            if self.monitor:
                self.monitor.close()

            return {
                'success': True,
                'message': 'Event system cleanup complete'
            }

        except Exception as e:
            logger.error(f"❌ Failed to cleanup: {e}")
            return {
                'success': False,
                'error': str(e)
            }

# Global manager instance
event_manager = EventSystemManager()

# Functions that can be called by run_dev
def event_system_setup():
    """Setup event system (called by run_dev setup)"""
    return event_manager.setup()

def event_system_status():
    """Get event system status (called by run_dev status)"""
    return event_manager.status()

def event_system_start_api(host="0.0.0.0", port=8000):
    """Start event API server"""
    return event_manager.start_api_server(host, port)

def event_system_start_worker(concurrency=4):
    """Start Celery worker"""
    return event_manager.start_celery_worker(concurrency)

def event_system_query(symbol=None, event_type=None, hours_back=24, limit=100):
    """Query events"""
    return event_manager.query_events(symbol, event_type, hours_back, limit)

def event_system_create_test_events(count=10):
    """Create test events"""
    return event_manager.create_test_events(count)

def event_system_process_events(batch_size=100):
    """Process pending events"""
    return event_manager.process_pending_events(batch_size)

def event_system_clear_queues(confirm=False):
    """Clear event queues"""
    return event_manager.clear_queues(confirm)

def event_system_stats():
    """Get event system statistics"""
    return event_manager.get_stats()

def event_system_cleanup():
    """Cleanup event system"""
    return event_manager.cleanup()

# CLI interface
if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "setup":
            result = event_system_setup()
            print(json.dumps(result, indent=2))

        elif command == "status":
            result = event_system_status()
            print(json.dumps(result, indent=2))

        elif command == "query":
            symbol = sys.argv[2] if len(sys.argv) > 2 else None
            result = event_system_query(symbol=symbol)
            print(json.dumps(result, indent=2))

        elif command == "test":
            count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            result = event_system_create_test_events(count)
            print(json.dumps(result, indent=2))

        elif command == "process":
            result = event_system_process_events()
            print(json.dumps(result, indent=2))

        elif command == "stats":
            result = event_system_stats()
            print(json.dumps(result, indent=2))

        elif command == "start-api":
            port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
            result = event_system_start_api(port=port)
            print(json.dumps(result, indent=2))

        else:
            print("Unknown command. Available: setup, status, query [symbol], test [count], process, stats, start-api [port]")
    else:
        print("Event System run_dev Integration")
        print("Usage: python run_dev_integration.py [command] [args...]")