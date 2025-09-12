"""
Event Database Storage - PostgreSQL with JSONB support for ATS Events
"""

import json
import logging
import psycopg2
import hashlib
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
from psycopg2.extras import RealDictCursor, Json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Simple configuration for events database
class EventsDatabaseConfig:
    """Simple database configuration for events"""
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', 5432))
        self.database = os.getenv('POSTGRES_DB', 'ats_dev')
        self.user = os.getenv('POSTGRES_USER', 'ats_user')
        self.password = os.getenv('POSTGRES_PASSWORD', 'dev_password')

logger = logging.getLogger(__name__)

class EventStorage:
    """PostgreSQL-based event storage with JSONB support"""

    def __init__(self, config: Optional[EventsDatabaseConfig] = None):
        """Initialize event storage with database configuration"""
        self.config = config or EventsDatabaseConfig()
        self.connection = None
        self._ensure_tables_exist()

    def _get_connection(self):
        """Get or create database connection"""
        if not self.connection or self.connection.closed:
            try:
                self.connection = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.user,
                    password=self.config.password
                )
                self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                logger.info("✅ Connected to PostgreSQL for event storage")
            except psycopg2.Error as e:
                logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
                raise
        return self.connection

    def _ensure_tables_exist(self):
        """Create event tables if they don't exist"""
        with self._get_cursor() as cursor:
            # Events table with JSONB support
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    event_id UUID PRIMARY KEY,
                    event_type VARCHAR(50) NOT NULL,
                    symbol VARCHAR(20),
                    timestamp TIMESTAMPTZ NOT NULL,
                    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source VARCHAR(100) NOT NULL,
                    source_id VARCHAR(255),
                    priority VARCHAR(20) DEFAULT 'medium',
                    classification VARCHAR(20) DEFAULT 'public',
                    event_data JSONB NOT NULL,
                    search_vector TSVECTOR,
                    confidence DECIMAL(3,2) DEFAULT 0.0,
                    processing_metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            ''')

            # Event correlations table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS event_correlations (
                    id SERIAL PRIMARY KEY,
                    primary_event_id UUID REFERENCES events(event_id),
                    related_event_id UUID REFERENCES events(event_id),
                    correlation_type VARCHAR(50) NOT NULL,
                    correlation_score DECIMAL(3,2) NOT NULL,
                    time_lag_seconds INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(primary_event_id, related_event_id, correlation_type)
                );
            ''')

            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_events_symbol_type ON events(symbol, event_type)",
                "CREATE INDEX IF NOT EXISTS idx_events_source ON events(source)",
                "CREATE INDEX IF NOT EXISTS idx_events_priority ON events(priority)",
                "CREATE INDEX IF NOT EXISTS idx_events_search_vector ON events USING GIN(search_vector)",
                "CREATE INDEX IF NOT EXISTS idx_events_data_gin ON events USING GIN(event_data)",
                "CREATE INDEX IF NOT EXISTS idx_correlations_primary ON event_correlations(primary_event_id)",
                "CREATE INDEX IF NOT EXISTS idx_correlations_score ON event_correlations(correlation_score DESC)"
            ]

            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                except psycopg2.Error as e:
                    if "already exists" not in str(e):
                        logger.warning(f"Failed to create index: {e}")

            # Create function to update search vector
            cursor.execute('''
                CREATE OR REPLACE FUNCTION update_event_search_vector()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.search_vector := setweight(to_tsvector('english',
                        COALESCE(NEW.event_data->>'headline', '')), 'A') ||
                        setweight(to_tsvector('english',
                        COALESCE(NEW.event_data->>'summary', '')), 'B') ||
                        setweight(to_tsvector('english',
                        COALESCE(NEW.symbol, '')), 'C');
                    NEW.updated_at := NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            ''')

            # Create trigger for search vector
            cursor.execute('''
                DROP TRIGGER IF EXISTS update_search_vector_trigger ON events;
                CREATE TRIGGER update_search_vector_trigger
                    BEFORE INSERT OR UPDATE ON events
                    FOR EACH ROW EXECUTE FUNCTION update_event_search_vector();
            ''')

            logger.info("✅ Event database tables and indexes created/verified")

    @contextmanager
    def _get_cursor(self):
        """Context manager for database cursors"""
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
        finally:
            cursor.close()

    def store_event(self, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Store event in PostgreSQL as JSONB

        Args:
            event_dict: Event data as dictionary

        Returns:
            Dict with storage result
        """
        result = {'success': False, 'error': None, 'event_id': None}

        try:
            with self._get_cursor() as cursor:
                # Extract key fields for indexing
                event_id = event_dict.get('event_id')
                event_type = event_dict.get('event_type', '').replace('EVENT_TYPE_', '').lower()
                symbol = event_dict.get('subject', {}).get('symbol', '')
                timestamp = self._parse_timestamp(event_dict.get('timestamp'))
                ingestion_time = self._parse_timestamp(event_dict.get('ingestion_time'))
                source = event_dict.get('source', '')
                source_id = event_dict.get('source_id', '')
                confidence = float(event_dict.get('confidence', 0.0))

                # Extract priority and classification
                metadata = event_dict.get('metadata', {})
                priority = metadata.get('priority', 'PRIORITY_MEDIUM').replace('PRIORITY_', '').lower()
                classification = metadata.get('classification', 'CLASSIFICATION_PUBLIC').replace('CLASSIFICATION_', '').lower()

                # Insert event
                cursor.execute('''
                    INSERT INTO events (
                        event_id, event_type, symbol, timestamp, ingestion_time,
                        source, source_id, priority, classification, event_data, confidence
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (event_id) DO UPDATE SET
                        event_data = EXCLUDED.event_data,
                        updated_at = NOW()
                    RETURNING event_id;
                ''', (
                    event_id, event_type, symbol, timestamp, ingestion_time,
                    source, source_id, priority, classification, Json(event_dict), confidence
                ))

                stored_event_id = cursor.fetchone()['event_id']

                result['success'] = True
                result['event_id'] = str(stored_event_id)

                logger.info(f"✅ Stored event {event_id} in database")

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Failed to store event: {e}")

        return result

    def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get single event by ID"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute('''
                    SELECT event_id, event_type, symbol, timestamp, ingestion_time,
                           source, source_id, priority, classification, event_data,
                           confidence, created_at, updated_at
                    FROM events
                    WHERE event_id = %s
                ''', (event_id,))

                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None

        except Exception as e:
            logger.error(f"❌ Failed to get event {event_id}: {e}")
            return None

    def query_events(self, symbol: str = None, event_type: str = None,
                    after_timestamp: datetime = None, before_timestamp: datetime = None,
                    source: str = None, priority: str = None, limit: int = 100,
                    offset: int = 0) -> List[Dict[str, Any]]:
        """
        Query events with filtering

        Args:
            symbol: Filter by symbol
            event_type: Filter by event type
            after_timestamp: Events after this timestamp
            before_timestamp: Events before this timestamp
            source: Filter by source
            priority: Filter by priority
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of event dictionaries
        """
        try:
            with self._get_cursor() as cursor:
                # Build dynamic query
                where_clauses = []
                params = []

                if symbol:
                    where_clauses.append("symbol = %s")
                    params.append(symbol)

                if event_type:
                    where_clauses.append("event_type = %s")
                    params.append(event_type.replace('EVENT_TYPE_', '').lower())

                if after_timestamp:
                    where_clauses.append("timestamp >= %s")
                    params.append(after_timestamp)

                if before_timestamp:
                    where_clauses.append("timestamp <= %s")
                    params.append(before_timestamp)

                if source:
                    where_clauses.append("source = %s")
                    params.append(source)

                if priority:
                    where_clauses.append("priority = %s")
                    params.append(priority.replace('PRIORITY_', '').lower())

                where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                query = f'''
                    SELECT event_id, event_type, symbol, timestamp, ingestion_time,
                           source, source_id, priority, classification, event_data,
                           confidence, created_at, updated_at
                    FROM events
                    {where_clause}
                    ORDER BY timestamp DESC
                    LIMIT %s OFFSET %s
                '''

                params.extend([limit, offset])
                cursor.execute(query, params)

                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"❌ Failed to query events: {e}")
            raise RuntimeError(f"Database query failed: {e}. Ensure event database is accessible and properly configured.")

    def search_events(self, search_text: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Full-text search events"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute('''
                    SELECT event_id, event_type, symbol, timestamp, source, event_data,
                           ts_rank(search_vector, plainto_tsquery('english', %s)) as rank
                    FROM events
                    WHERE search_vector @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC, timestamp DESC
                    LIMIT %s
                ''', (search_text, search_text, limit))

                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"❌ Failed to search events: {e}")
            raise RuntimeError(f"Event search failed: {e}. Ensure event database search functionality is properly configured.")

    def store_correlation(self, correlation: Dict[str, Any]) -> bool:
        """Store event correlation"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute('''
                    INSERT INTO event_correlations (
                        primary_event_id, related_event_id, correlation_type,
                        correlation_score, time_lag_seconds
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (primary_event_id, related_event_id, correlation_type)
                    DO UPDATE SET
                        correlation_score = EXCLUDED.correlation_score,
                        time_lag_seconds = EXCLUDED.time_lag_seconds
                ''', (
                    correlation['primary_event_id'],
                    correlation['related_event_id'],
                    correlation['correlation_type'],
                    correlation['correlation_score'],
                    correlation.get('time_lag_seconds')
                ))

                logger.info(f"✅ Stored correlation: {correlation['primary_event_id']} -> {correlation['related_event_id']}")
                return True

        except Exception as e:
            logger.error(f"❌ Failed to store correlation: {e}")
            return False

    def get_correlations(self, event_id: str, min_score: float = 0.5) -> List[Dict[str, Any]]:
        """Get correlations for an event"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute('''
                    SELECT c.*, e1.symbol as primary_symbol, e2.symbol as related_symbol,
                           e1.event_type as primary_type, e2.event_type as related_type
                    FROM event_correlations c
                    JOIN events e1 ON c.primary_event_id = e1.event_id
                    JOIN events e2 ON c.related_event_id = e2.event_id
                    WHERE (c.primary_event_id = %s OR c.related_event_id = %s)
                      AND c.correlation_score >= %s
                    ORDER BY c.correlation_score DESC
                ''', (event_id, event_id, min_score))

                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"❌ Failed to get correlations: {e}")
            raise RuntimeError(f"Event correlation query failed: {e}. Ensure correlation tables are accessible and properly configured.")

    def update_event_metadata(self, event_id: str, metadata: Dict[str, Any]) -> bool:
        """Update event processing metadata"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute('''
                    UPDATE events
                    SET processing_metadata = processing_metadata || %s,
                        updated_at = NOW()
                    WHERE event_id = %s
                ''', (Json(metadata), event_id))

                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"❌ Failed to update event metadata: {e}")
            return False

    def get_event_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with self._get_cursor() as cursor:
                # Total events
                cursor.execute("SELECT COUNT(*) as total_events FROM events")
                total_events = cursor.fetchone()['total_events']

                # Events by type
                cursor.execute('''
                    SELECT event_type, COUNT(*) as count
                    FROM events
                    GROUP BY event_type
                    ORDER BY count DESC
                ''')
                events_by_type = {row['event_type']: row['count'] for row in cursor.fetchall()}

                # Events by source
                cursor.execute('''
                    SELECT source, COUNT(*) as count
                    FROM events
                    GROUP BY source
                    ORDER BY count DESC
                ''')
                events_by_source = {row['source']: row['count'] for row in cursor.fetchall()}

                # Recent events (last 24 hours)
                cursor.execute('''
                    SELECT COUNT(*) as recent_events
                    FROM events
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                ''')
                recent_events = cursor.fetchone()['recent_events']

                # Total correlations
                cursor.execute("SELECT COUNT(*) as total_correlations FROM event_correlations")
                total_correlations = cursor.fetchone()['total_correlations']

                return {
                    'total_events': total_events,
                    'events_by_type': events_by_type,
                    'events_by_source': events_by_source,
                    'recent_events_24h': recent_events,
                    'total_correlations': total_correlations,
                    'timestamp': datetime.utcnow().isoformat()
                }

        except Exception as e:
            logger.error(f"❌ Failed to get event stats: {e}")
            return {'error': str(e)}

    def archive_old_events(self, cutoff_date: datetime) -> Dict[str, Any]:
        """Archive old events (placeholder for future cold storage)"""
        try:
            with self._get_cursor() as cursor:
                # For now, just count old events
                cursor.execute('''
                    SELECT COUNT(*) as old_events
                    FROM events
                    WHERE timestamp < %s
                ''', (cutoff_date,))

                old_count = cursor.fetchone()['old_events']

                # In a real implementation, we would move these to cold storage
                # For now, just return the count
                return {
                    'archived_count': 0,  # Would be old_count if actually archived
                    'old_events_found': old_count,
                    'cutoff_date': cutoff_date.isoformat()
                }

        except Exception as e:
            logger.error(f"❌ Failed to archive old events: {e}")
            return {'error': str(e)}

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object"""
        if not timestamp_str:
            return datetime.utcnow()

        try:
            # Handle ISO format with timezone
            if isinstance(timestamp_str, str):
                timestamp_str = timestamp_str.replace('Z', '+00:00')
                if '.' not in timestamp_str and '+' not in timestamp_str:
                    timestamp_str += '+00:00'
                return datetime.fromisoformat(timestamp_str)
            return timestamp_str
        except:
            return datetime.utcnow()

    def close(self):
        """Close database connection"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("🔒 Event storage database connection closed")

# CLI interface for testing
if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create storage instance
    storage = EventStorage()

    try:
        if len(sys.argv) > 1:
            command = sys.argv[1]

            if command == "stats":
                stats = storage.get_event_stats()
                print(f"Database statistics: {json.dumps(stats, indent=2)}")

            elif command == "test":
                # Test storing a sample event
                test_event = {
                    'event_id': 'test-12345',
                    'event_type': 'EVENT_TYPE_NEWS',
                    'timestamp': datetime.utcnow().isoformat(),
                    'source': 'test',
                    'subject': {'symbol': 'AAPL'},
                    'metadata': {'priority': 'PRIORITY_HIGH'},
                    'news_data': {'headline': 'Test News Event'}
                }

                result = storage.store_event(test_event)
                print(f"Storage result: {result}")

                if result['success']:
                    # Try to retrieve it
                    retrieved = storage.get_event('test-12345')
                    print(f"Retrieved event: {retrieved}")

            else:
                print("Unknown command. Available: stats, test")
        else:
            print("Event Database Storage")
            print("Usage: python database.py [stats|test]")

    finally:
        storage.close()