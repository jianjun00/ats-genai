"""
Event Consumer - Celery-based event processing for ATS
"""

import redis
import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from celery import Celery, Task
from celery.signals import worker_ready, worker_shutdown

from events.proto.events_pb2 import Event, EventType, MessageToDict
from events.database import EventStorage
from events.correlation import CorrelationEngine

logger = logging.getLogger(__name__)

# Celery app configuration
celery_app = Celery('event_processor')
celery_app.conf.update(
    # Broker and backend
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',

    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',

    # Timezone and task settings
    timezone='UTC',
    enable_utc=True,

    # Task routing
    task_routes={
        'events.consumer.process_event_from_queue': {'queue': 'event_processing'},
        'events.consumer.batch_process_events': {'queue': 'batch_processing'},
        'events.consumer.hourly_event_processing': {'queue': 'scheduled_tasks'},
    },

    # Worker configuration
    worker_prefetch_multiplier=4,
    task_acks_late=True,

    # Beat schedule for periodic tasks
    beat_schedule={
        'hourly-event-processing': {
            'task': 'events.consumer.hourly_event_processing',
            'schedule': 3600.0,  # Every hour
        },
        'queue-monitoring': {
            'task': 'events.consumer.monitor_queue_health',
            'schedule': 300.0,  # Every 5 minutes
        }
    },
)

# Global connections (will be initialized in worker_ready signal)
redis_client = None
event_storage = None
correlation_engine = None

@worker_ready.connect
def initialize_worker(sender=None, **kwargs):
    """Initialize worker connections when worker starts"""
    global redis_client, event_storage, correlation_engine

    try:
        # Initialize Redis connection
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        redis_client.ping()
        logger.info("✅ Worker initialized Redis connection")

        # Initialize database connection
        event_storage = EventStorage()
        logger.info("✅ Worker initialized database connection")

        # Initialize correlation engine
        correlation_engine = CorrelationEngine(event_storage)
        logger.info("✅ Worker initialized correlation engine")

    except Exception as e:
        logger.error(f"❌ Failed to initialize worker: {e}")
        raise

@worker_shutdown.connect
def cleanup_worker(sender=None, **kwargs):
    """Cleanup connections when worker shuts down"""
    global redis_client, event_storage, correlation_engine

    if redis_client:
        redis_client.close()
    if event_storage:
        event_storage.close()

    logger.info("✅ Worker connections cleaned up")

class EventProcessingTask(Task):
    """Base task class for event processing with error handling"""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure"""
        logger.error(f"❌ Task {task_id} failed: {exc}")
        logger.error(f"Traceback: {einfo}")

@celery_app.task(bind=True, base=EventProcessingTask, max_retries=3)
def process_event_from_queue(self, queue_name: str) -> Dict[str, Any]:
    """
    Process single event from Redis queue

    Args:
        queue_name: Name of Redis queue to process

    Returns:
        Dict with processing results
    """
    global redis_client, event_storage, correlation_engine

    start_time = datetime.utcnow()
    result = {
        'status': 'success',
        'event_id': None,
        'processing_time_ms': 0,
        'correlations_found': 0,
        'error': None
    }

    try:
        # 1. Pop event from queue (non-blocking with timeout)
        queue_result = redis_client.brpop([queue_name], timeout=30)
        if not queue_result:
            result['status'] = 'no_events'
            result['message'] = f"No events in queue {queue_name}"
            return result

        queue, serialized_event = queue_result

        # 2. Deserialize Protocol Buffer
        event = Event()
        event.ParseFromString(serialized_event)
        result['event_id'] = event.event_id

        logger.info(f"📥 Processing event {event.event_id} from queue {queue_name}")

        # 3. Store event in PostgreSQL (as JSONB)
        event_dict = MessageToDict(event, preserving_proto_field_name=True)
        storage_result = event_storage.store_event(event_dict)

        if not storage_result['success']:
            raise Exception(f"Failed to store event: {storage_result['error']}")

        # 4. Run correlation analysis
        correlations = correlation_engine.find_correlations(event)
        result['correlations_found'] = len(correlations)

        if correlations:
            for correlation in correlations:
                event_storage.store_correlation(correlation)
                logger.info(f"🔗 Found correlation: {correlation['primary_event_id']} -> {correlation['related_event_id']}")

        # 5. Update processing metadata
        processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        result['processing_time_ms'] = processing_time

        event_storage.update_event_metadata(event.event_id, {
            'processed_at': datetime.utcnow(),
            'processing_time_ms': processing_time,
            'correlations_found': len(correlations)
        })

        logger.info(f"✅ Successfully processed event {event.event_id} in {processing_time:.0f}ms")

        return result

    except Exception as exc:
        # Calculate processing time even on error
        processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        result['processing_time_ms'] = processing_time
        result['status'] = 'error'
        result['error'] = str(exc)

        logger.error(f"❌ Error processing event from {queue_name}: {exc}")
        logger.error(traceback.format_exc())

        # Retry with exponential backoff
        countdown = 2 ** self.request.retries
        raise self.retry(exc=exc, countdown=countdown, max_retries=3)

@celery_app.task(bind=True, base=EventProcessingTask)
def batch_process_events(self, queue_names: List[str], batch_size: int = 50) -> Dict[str, Any]:
    """
    Process events in batches for efficiency

    Args:
        queue_names: List of queue names to process
        batch_size: Number of events to process per queue

    Returns:
        Dict with batch processing results
    """
    result = {
        'status': 'success',
        'queues_processed': len(queue_names),
        'total_events_processed': 0,
        'errors': []
    }

    for queue_name in queue_names:
        events_processed = 0
        errors_encountered = 0

        for _ in range(batch_size):
            try:
                task_result = process_event_from_queue.delay(queue_name).get(timeout=60)

                if task_result['status'] == 'success':
                    events_processed += 1
                elif task_result['status'] == 'no_events':
                    break  # No more events in this queue
                else:
                    errors_encountered += 1
                    result['errors'].append(f"Queue {queue_name}: {task_result.get('error', 'Unknown error')}")

            except Exception as e:
                errors_encountered += 1
                result['errors'].append(f"Queue {queue_name}: {str(e)}")

                # Stop processing this queue if too many errors
                if errors_encountered >= 5:
                    break

        result['total_events_processed'] += events_processed
        logger.info(f"📊 Batch processed {events_processed} events from {queue_name}")

    return result

@celery_app.task
def hourly_event_processing() -> Dict[str, Any]:
    """
    Scheduled task to process events every hour
    """
    logger.info("⏰ Starting hourly event processing")

    # Define priority queue order (process high-priority queues first)
    priority_queues = [
        'events:news',
        'events:earnings',
        'events:technical_signal',
        'events:corporate_action',
        'events:economic_indicator'
    ]

    # Process each queue
    result = batch_process_events.delay(priority_queues, batch_size=100).get(timeout=3600)

    logger.info(f"⏰ Completed hourly processing: {result['total_events_processed']} events processed")
    return result

@celery_app.task
def monitor_queue_health() -> Dict[str, Any]:
    """
    Monitor queue health and alert on issues
    """
    global redis_client

    try:
        # Check queue sizes
        queue_stats = {}
        alert_threshold = 1000  # Alert if queue has more than 1000 events

        queues_to_monitor = [
            'events:all',
            'events:news',
            'events:earnings',
            'events:technical_signal',
            'events:corporate_action',
            'events:economic_indicator'
        ]

        for queue_name in queues_to_monitor:
            size = redis_client.llen(queue_name)
            queue_stats[queue_name] = size

            if size > alert_threshold:
                logger.warning(f"⚠️ Queue {queue_name} has {size} events (threshold: {alert_threshold})")

        # Check Redis connection health
        redis_client.ping()

        return {
            'status': 'healthy',
            'queue_stats': queue_stats,
            'timestamp': datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Queue health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }

@celery_app.task
def cleanup_old_events(days_to_keep: int = 90) -> Dict[str, Any]:
    """
    Clean up old events from Redis queues

    Args:
        days_to_keep: Number of days of events to keep
    """
    global event_storage

    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)

        # This would typically involve moving old events to cold storage
        # For now, we'll just log the operation
        result = event_storage.archive_old_events(cutoff_date)

        logger.info(f"🗄️ Archived {result.get('archived_count', 0)} events older than {cutoff_date}")

        return {
            'status': 'success',
            'cutoff_date': cutoff_date.isoformat(),
            'archived_count': result.get('archived_count', 0)
        }

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }

# Utility functions for manual processing
def process_single_event(event_json: str) -> bool:
    """Process a single event from JSON string"""
    try:
        event = Event()
        event_dict = json.loads(event_json)
        event._from_dict(event_dict)

        result = process_event_from_queue.delay("manual").get(timeout=60)
        return result['status'] == 'success'

    except Exception as e:
        logger.error(f"❌ Failed to process single event: {e}")
        return False

def get_processing_stats() -> Dict[str, Any]:
    """Get current processing statistics"""
    global redis_client, event_storage

    try:
        # Get queue statistics
        queue_stats = {}
        for queue in ['events:all', 'events:news', 'events:earnings', 'events:technical_signal']:
            queue_stats[queue] = redis_client.llen(queue)

        # Get database statistics
        db_stats = event_storage.get_event_stats()

        return {
            'queue_stats': queue_stats,
            'database_stats': db_stats,
            'timestamp': datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Failed to get processing stats: {e}")
        return {'error': str(e)}

# CLI interface for testing
if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "test_process":
            # Test processing a single queue
            result = process_event_from_queue("events:news")
            print(f"Processing result: {result}")

        elif command == "test_batch":
            # Test batch processing
            result = batch_process_events(["events:news", "events:earnings"], 10)
            print(f"Batch processing result: {result}")

        elif command == "stats":
            # Show processing statistics
            stats = get_processing_stats()
            print(f"Processing statistics: {json.dumps(stats, indent=2)}")

        elif command == "health":
            # Check queue health
            health = monitor_queue_health()
            print(f"Queue health: {json.dumps(health, indent=2)}")

        else:
            print("Unknown command. Available: test_process, test_batch, stats, health")
    else:
        print("Event Consumer - Celery Tasks")
        print("Usage: python consumer.py [test_process|test_batch|stats|health]")