"""
Analytics Service Integration for xAI Financial Events
Stores extracted events in ATS database and exposes them via analytics API
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, date, time
from typing import List, Dict, Any, Optional
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

from .xai_event_extractor import OptimizedXAIEventExtractor, FinancialEvent

def convert_dates_to_strings(obj):
    """Convert date/datetime objects to strings for JSON serialization"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, time):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_dates_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates_to_strings(item) for item in obj]
    return obj

logger = logging.getLogger(__name__)

class AnalyticsEventIntegration:
    """
    Integration layer between xAI Event Extractor and ATS Analytics Service
    """

    def __init__(
        self,
        xai_api_key: str,
        analytics_base_url: str = "http://localhost:4000",
        db_connection_string: str = None
    ):
        self.analytics_url = analytics_base_url

        # Set up database connection
        if db_connection_string:
            self.db_connection_string = db_connection_string
        else:
            # Use environment variables
            self.db_connection_string = (
                f"host={os.getenv('DB_HOST', 'localhost')} "
                f"port={os.getenv('DB_PORT', '5432')} "
                f"user={os.getenv('DB_USER', 'postgres')} "
                f"password={os.getenv('DB_PASSWORD', 'intg_password')} "
                f"dbname={os.getenv('DB_NAME', 'intg_db')}"
            )

        # Initialize xAI extractor
        self.event_extractor = OptimizedXAIEventExtractor(
            api_key=xai_api_key,
            enable_cache=True,
            cache_ttl_hours=6  # 6-hour cache for financial events
        )

        logger.info(f"Analytics integration initialized: {analytics_base_url}")
        logger.info(f"Database connection: {self.db_connection_string.replace(os.getenv('DB_PASSWORD', 'intg_password'), '***')}")

    async def create_events_table(self):
        """Create financial events table in ATS database"""

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS intg_financial_events (
            event_id SERIAL PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            company_symbol VARCHAR(20),
            details TEXT NOT NULL,
            event_date DATE NOT NULL,
            event_time TIME,
            impact_level VARCHAR(20) NOT NULL,
            sentiment VARCHAR(20),
            confidence_score REAL,
            source_url TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_financial_events_symbol ON intg_financial_events(company_symbol);
        CREATE INDEX IF NOT EXISTS idx_financial_events_date ON intg_financial_events(event_date);
        CREATE INDEX IF NOT EXISTS idx_financial_events_type ON intg_financial_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_financial_events_impact ON intg_financial_events(impact_level);
        """

        try:
            # Execute via direct database connection
            conn = psycopg2.connect(self.db_connection_string)
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info("✅ Financial events table created successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Error creating events table: {e}")
            return False

    async def store_events(self, events: List[FinancialEvent]) -> int:
        """Store financial events in ATS database"""

        if not events:
            return 0

        # Prepare batch insert
        values = []
        for event in events:
            event_time_str = f"'{event.event_time}'" if event.event_time else "NULL"
            sentiment_str = f"'{event.sentiment}'" if event.sentiment else "NULL"
            confidence_str = str(event.confidence_score) if event.confidence_score else "NULL"
            source_url_str = f"'{event.source_url}'" if event.source_url else "NULL"
            symbol_str = f"'{event.company_symbol}'" if event.company_symbol else "NULL"

            values.append(f"""(
                '{event.event_type}',
                {symbol_str},
                '{event.details.replace("'", "''")}',
                '{event.event_date}',
                {event_time_str},
                '{event.impact_level}',
                {sentiment_str},
                {confidence_str},
                {source_url_str}
            )""")

        insert_sql = f"""
        INSERT INTO intg_financial_events
        (event_type, company_symbol, details, event_date, event_time, impact_level,
         sentiment, confidence_score, source_url)
        VALUES {', '.join(values)}
        ON CONFLICT DO NOTHING;
        """

        try:
            # Execute via direct database connection
            conn = psycopg2.connect(self.db_connection_string)
            cursor = conn.cursor()
            cursor.execute(insert_sql)
            rows_affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ Stored {rows_affected} financial events successfully")
            return rows_affected

        except Exception as e:
            logger.error(f"❌ Error storing events: {e}")
            return 0

    async def extract_and_store_events(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """Extract events from xAI and store in analytics database"""

        logger.info(f"Starting event extraction and storage: {start_date} to {end_date}")

        try:
            # Extract events using xAI
            events = await self.event_extractor.extract_events_batch(
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
                force_refresh=force_refresh
            )

            if not events:
                return {
                    "success": True,
                    "events_extracted": 0,
                    "events_stored": 0,
                    "message": "No events found for the specified criteria"
                }

            # Store events in database
            stored_count = await self.store_events(events)

            # Get cache statistics
            cache_stats = await self.event_extractor.get_cache_stats()

            return {
                "success": True,
                "events_extracted": len(events),
                "events_stored": stored_count,
                "date_range": f"{start_date} to {end_date}",
                "symbols": symbols or "All",
                "cache_stats": cache_stats,
                "events_preview": [
                    {
                        "type": event.event_type,
                        "symbol": event.company_symbol,
                        "date": event.event_date,
                        "impact": event.impact_level,
                        "details": event.details[:100] + "..." if len(event.details) > 100 else event.details
                    }
                    for event in events[:5]  # Show first 5 events
                ]
            }

        except Exception as e:
            logger.error(f"❌ Error in extract_and_store_events: {e}")
            return {
                "success": False,
                "error": str(e),
                "events_extracted": 0,
                "events_stored": 0
            }

    def get_events_from_analytics(
        self,
        symbol: str = None,
        event_type: str = None,
        start_date: str = None,
        end_date: str = None,
        impact_level: str = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Query financial events from analytics service"""

        # Build query conditions
        conditions = []
        if symbol:
            conditions.append(f"company_symbol = '{symbol}'")
        if event_type:
            conditions.append(f"event_type = '{event_type}'")
        if start_date:
            conditions.append(f"event_date >= '{start_date}'")
        if end_date:
            conditions.append(f"event_date <= '{end_date}'")
        if impact_level:
            conditions.append(f"impact_level = '{impact_level}'")

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
        SELECT
            event_id,
            event_type,
            company_symbol,
            details,
            event_date,
            event_time,
            impact_level,
            sentiment,
            confidence_score,
            source_url,
            extracted_at
        FROM intg_financial_events
        {where_clause}
        ORDER BY event_date DESC, event_time DESC NULLS LAST
        LIMIT {limit};
        """

        try:
            # Execute via direct database connection
            conn = psycopg2.connect(self.db_connection_string)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            # Convert results to list of dictionaries and serialize dates
            events = [convert_dates_to_strings(dict(row)) for row in results]

            return {
                "success": True,
                "events": events,
                "count": len(events),
                "query_params": {
                    "symbol": symbol,
                    "event_type": event_type,
                    "date_range": f"{start_date} to {end_date}" if start_date and end_date else None,
                    "impact_level": impact_level,
                    "limit": limit
                }
            }

        except Exception as e:
            logger.error(f"❌ Error querying events: {e}")
            return {
                "success": False,
                "error": str(e),
                "events": []
            }

    def get_events_summary(self) -> Dict[str, Any]:
        """Get summary statistics of stored financial events"""

        summary_query = """
        SELECT
            COUNT(*) as total_events,
            COUNT(DISTINCT company_symbol) as unique_symbols,
            COUNT(DISTINCT event_type) as event_types,
            MIN(event_date) as earliest_date,
            MAX(event_date) as latest_date,
            COUNT(*) FILTER (WHERE impact_level = 'high') as high_impact_events,
            COUNT(*) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '7 days') as events_last_week,
            COUNT(*) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '30 days') as events_last_month
        FROM intg_financial_events;

        SELECT event_type, COUNT(*) as count
        FROM intg_financial_events
        GROUP BY event_type
        ORDER BY count DESC;

        SELECT company_symbol, COUNT(*) as event_count
        FROM intg_financial_events
        WHERE company_symbol IS NOT NULL
        GROUP BY company_symbol
        ORDER BY event_count DESC
        LIMIT 10;
        """

        try:
            # Execute via direct database connection
            conn = psycopg2.connect(self.db_connection_string)
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Split and execute each query separately
            queries = summary_query.split(';')
            all_results = []

            for query in queries:
                query = query.strip()
                if query:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    all_results.extend([convert_dates_to_strings(dict(row)) for row in results])

            cursor.close()
            conn.close()

            return {
                "success": True,
                "summary": all_results,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error getting events summary: {e}")
            return {
                "success": False,
                "error": str(e)
            }

# REST API endpoints for analytics service integration
class FinancialEventsAPI:
    """REST API endpoints for financial events in analytics service"""

    def __init__(self, integration: AnalyticsEventIntegration):
        self.integration = integration

    async def handle_extract_events(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle POST /financial_events/extract"""

        start_date = params.get('start_date')
        end_date = params.get('end_date')
        symbols = params.get('symbols', [])
        force_refresh = params.get('force_refresh', False)

        if not start_date or not end_date:
            return {
                "success": False,
                "error": "start_date and end_date are required"
            }

        return await self.integration.extract_and_store_events(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            force_refresh=force_refresh
        )

    def handle_get_events(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GET /financial_events"""

        return self.integration.get_events_from_analytics(
            symbol=params.get('symbol'),
            event_type=params.get('event_type'),
            start_date=params.get('start_date'),
            end_date=params.get('end_date'),
            impact_level=params.get('impact_level'),
            limit=int(params.get('limit', 100))
        )

    def handle_get_summary(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GET /financial_events/summary"""

        return self.integration.get_events_summary()

# Usage example
async def demo_integration():
    """Demonstrate the analytics integration"""

    print("🚀 Financial Events Analytics Integration Demo")
    print("=" * 60)

    # Initialize integration (replace with real API key)
    integration = AnalyticsEventIntegration(
        xai_api_key="test_key",
        analytics_base_url="http://localhost:4000"
    )

    # Create table
    print("📋 Creating financial events table...")
    table_created = await integration.create_events_table()
    print(f"   Table creation: {'✅ Success' if table_created else '❌ Failed'}")

    # Extract and store events
    print("\n📊 Extracting and storing financial events...")
    result = await integration.extract_and_store_events(
        start_date="2025-09-01",
        end_date="2025-09-13",
        symbols=["AAPL", "TSLA", "MSFT"]
    )

    print(f"   Extraction: {'✅ Success' if result['success'] else '❌ Failed'}")
    print(f"   Events extracted: {result.get('events_extracted', 0)}")
    print(f"   Events stored: {result.get('events_stored', 0)}")

    # Query events from analytics
    print("\n🔍 Querying stored events...")
    events_data = integration.get_events_from_analytics(
        impact_level="high",
        limit=5
    )

    if events_data['success']:
        print(f"   Found {events_data['count']} high-impact events")
        for i, event in enumerate(events_data['events'][:3], 1):
            symbol = event.get('company_symbol', 'MARKET')
            print(f"   {i}. {event['event_date']} | {symbol}: {event['details'][:60]}...")

    # Get summary
    print("\n📈 Events summary:")
    summary = integration.get_events_summary()
    if summary['success'] and summary.get('summary'):
        stats = summary['summary'][0] if summary['summary'] else {}
        print(f"   Total events: {stats.get('total_events', 0)}")
        print(f"   Unique symbols: {stats.get('unique_symbols', 0)}")
        print(f"   High impact events: {stats.get('high_impact_events', 0)}")

if __name__ == "__main__":
    asyncio.run(demo_integration())