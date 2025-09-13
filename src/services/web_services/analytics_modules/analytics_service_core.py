"""
Core service initialization and configuration
"""

#!/usr/bin/env python3
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components


#!/usr/bin/env python3
"""
Unified Analytics Service - Consolidated from 5 separate analytics services

This service combines functionality from:
- analytics_service.py (main service, 6046 lines)
- analytics_service_class.py (type-aware features, 383 lines)
- type_aware_analytics_service.py (specialized type handling, 531 lines)
- universe_analytics_service.py (universe analytics, 310 lines)
- analytics_service.py.backup (removed - was duplicate)

Features:
- Web-based analytics dashboard for 30-year price database
- Type-aware dataset analysis and intelligent EDA
- Universe analytics and cross-instrument analysis
- Ray distributed computing integration
- Training dataset management and visualization
- Real-time data quality monitoring
"""

import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components

# Import visualization components
try:
    from visualization.multi_panel_trading_chart import MultiPanelTradingChart
    from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig
    VISUALIZATION_AVAILABLE = True
    logger.info("✅ Multi-panel trading visualization loaded")
except ImportError as e:
    VISUALIZATION_AVAILABLE = False
    logger.warning(f"⚠️ Multi-panel visualization not available: {e}")

# Import type system components (from analytics_service_class.py)
try:
    from schema.registry import schema_registry
    from schema.types import FieldSemantics
    TYPE_SYSTEM_AVAILABLE = True
    logger.info("✅ Type system components loaded")
except ImportError as e:
    TYPE_SYSTEM_AVAILABLE = False
    logger.warning(f"⚠️ Type system not available: {e}")

# Ray EDA integration for massive dataset analysis
try:
    RAY_AVAILABLE = True
    logger.info("✅ Ray EDA engine loaded - distributed computing enabled")
except ImportError as e:
    RAY_AVAILABLE = False
    logger.warning(f"⚠️ Ray EDA engine not available: {e}. Falling back to traditional methods")

# Dataset metadata cache - expires after 4 hours
DATASET_CACHE = {
    'data': None,
    'timestamp': 0,
    'ttl': 4 * 60 * 60  # 4 hours in seconds
}

class UnifiedAnalyticsService:
    """
    Unified Analytics Service combining all analytics functionality.

    This class consolidates:
    1. Web dashboard serving (from analytics_service.py)
    2. Type-aware analysis (from analytics_service_class.py & type_aware_analytics_service.py)
    3. Universe analytics (from universe_analytics_service.py)
    4. Training dataset management
    5. Ray distributed computing integration
    """

    def __init__(self, db_manager=None):
        """Initialize unified analytics service with all capabilities."""
        self.db = db_manager
        self.type_system_enabled = TYPE_SYSTEM_AVAILABLE
        self.ray_enabled = RAY_AVAILABLE
        self.visualization_enabled = VISUALIZATION_AVAILABLE

        # Initialize visualization components
        if self.visualization_enabled:
            self.multi_panel_chart = MultiPanelTradingChart()
            self.feature_extractor = MultiTimeframeFeatureExtractor(TrainingDataConfig())

        logger.info("🚀 Unified Analytics Service initialized")
        logger.info(f"   Type system: {'✅ Enabled' if self.type_system_enabled else '❌ Disabled'}")
        logger.info(f"   Ray computing: {'✅ Enabled' if self.ray_enabled else '❌ Disabled'}")
        logger.info(f"   Multi-panel visualization: {'✅ Enabled' if self.visualization_enabled else '❌ Disabled'}")

        if self.type_system_enabled:
            logger.info(f"   Available schemas: {list(schema_registry.get_schema_summary()['entities'].keys())}")

    # ==============================================
    # TYPE-AWARE ANALYSIS (from analytics_service_class.py)
    # ==============================================

    async def get_intelligent_filters(self, table_name: str) -> Dict[str, Any]:
        """Generate intelligent filter definitions using type system."""
        if not self.type_system_enabled:
            logger.warning("Type system not available, falling back to basic filters")
            return self._get_basic_filters(table_name)

        try:
            filterable_fields = {}

            # Try to get schema for this table
            schema = schema_registry.get_table_schema(table_name)

            # Get all filterable fields from schema
            for field_name, field_def in schema.fields.items():
                if field_def.is_filterable:
                    filter_config = {
                        "field_name": field_name,
                        "display_name": field_def.ui_label,
                        "field_type": field_def.field_type.value,
                        "semantics": field_def.semantics.value,
                        "description": field_def.description,
                        "help_text": field_def.ui_help_text,
                        "placeholder": field_def.ui_placeholder,
                        "nullable": field_def.nullable,
                        "eda_priority": field_def.eda_priority
                    }

                    # Add semantic-specific configurations
                    if field_def.semantics == FieldSemantics.PRICE:
                        filter_config.update({
                            "min_value": 0,
                            "step": 0.01,
                            "format": "currency"
                        })
                    elif field_def.semantics == FieldSemantics.DATE:
                        filter_config.update({
                            "format": "date",
                            "date_range": True
                        })
                    elif field_def.semantics == FieldSemantics.SYMBOL:
                        filter_config.update({
                            "autocomplete": True,
                            "multi_select": True
                        })

                    filterable_fields[field_name] = filter_config

            return {
                "table_name": table_name,
                "filterable_fields": filterable_fields,
                "schema_available": True,
                "total_filterable": len(filterable_fields)
            }

        except Exception as e:
            logger.error(f"Error generating intelligent filters for {table_name}: {e}")
            return self._get_basic_filters(table_name)

    def _get_basic_filters(self, table_name: str) -> Dict[str, Any]:
        """Fallback basic filter generation when type system unavailable."""
        # Basic filter definitions for common financial data tables
        basic_filters = {
            "symbol": {"field_type": "string", "multi_select": True},
            "date": {"field_type": "date", "date_range": True},
            "price": {"field_type": "numeric", "min_value": 0, "format": "currency"},
            "volume": {"field_type": "numeric", "min_value": 0},
            "exchange": {"field_type": "string", "multi_select": True}
        }

        return {
            "table_name": table_name,
            "filterable_fields": basic_filters,
            "schema_available": False,
            "total_filterable": len(basic_filters)
        }

    # ==============================================
    # NEWS AND EARNINGS EVENTS ANALYSIS
    # ==============================================

    def get_earnings_events(self, limit: int = 100, symbol: str = None) -> Dict[str, Any]:
        """Get recent earnings events from dev_earnings_events table."""
        try:
            from core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Query earnings events
                    earnings_query = """
                        SELECT
                            symbol,
                            report_period,
                            report_type,
                            eps_actual_cents,
                            eps_estimated_cents,
                            eps_surprise_pct,
                            revenue_actual_cents,
                            revenue_estimated_cents,
                            revenue_surprise_pct,
                            earnings_call_datetime,
                            earnings_beat,
                            revenue_beat,
                            guidance_raised,
                            guidance_lowered,
                            created_at,
                            updated_at
                        FROM dev_earnings_events
                        WHERE 1=1
                    """

                    if symbol:
                        earnings_query += " AND UPPER(symbol) = UPPER(%s)"
                        params = [symbol]
                    else:
                        params = []

                    earnings_query += " ORDER BY report_period DESC, created_at DESC LIMIT %s"
                    params.append(limit)

                    cursor.execute(earnings_query, params)
                    earnings_events = cursor.fetchall()

                    # Process earnings data
                    processed_events = []
                    for event in earnings_events:
                        processed_event = {
                            'symbol': event['symbol'],
                            'report_period': event['report_period'].strftime('%Y-%m-%d') if event['report_period'] else None,
                            'report_type': event['report_type'],
                            'eps_actual': round(event['eps_actual_cents'] / 100, 2) if event['eps_actual_cents'] is not None else None,
                            'eps_estimated': round(event['eps_estimated_cents'] / 100, 2) if event['eps_estimated_cents'] is not None else None,
                            'eps_surprise_pct': float(event['eps_surprise_pct']) if event['eps_surprise_pct'] is not None else None,
                            'revenue_actual_millions': round(event['revenue_actual_cents'] / 100_000_000, 1) if event['revenue_actual_cents'] is not None else None,
                            'revenue_estimated_millions': round(event['revenue_estimated_cents'] / 100_000_000, 1) if event['revenue_estimated_cents'] is not None else None,
                            'revenue_surprise_pct': float(event['revenue_surprise_pct']) if event['revenue_surprise_pct'] is not None else None,
                            'earnings_call_datetime': event['earnings_call_datetime'].isoformat() if event['earnings_call_datetime'] else None,
                            'earnings_beat': event['earnings_beat'],
                            'revenue_beat': event['revenue_beat'],
                            'guidance_raised': event['guidance_raised'],
                            'guidance_lowered': event['guidance_lowered'],
                            'created_at': event['created_at'].isoformat() if event['created_at'] else None,
                            'updated_at': event['updated_at'].isoformat() if event['updated_at'] else None
                        }
                        processed_events.append(processed_event)

                    # Get summary statistics
                    unique_symbols = set(event['symbol'] for event in processed_events)

                    # Count beats and misses
                    eps_beats = sum(1 for event in processed_events if event['earnings_beat'] is True)
                    eps_misses = sum(1 for event in processed_events if event['earnings_beat'] is False)
                    revenue_beats = sum(1 for event in processed_events if event['revenue_beat'] is True)
                    revenue_misses = sum(1 for event in processed_events if event['revenue_beat'] is False)

                    # Count guidance changes
                    guidance_raised_count = sum(1 for event in processed_events if event['guidance_raised'] is True)
                    guidance_lowered_count = sum(1 for event in processed_events if event['guidance_lowered'] is True)

                    logger.info(f"Retrieved {len(processed_events)} earnings events")

                    return {
                        'success': True,
                        'events': processed_events,
                        'total_events': len(processed_events),
                        'unique_symbols': len(unique_symbols),
                        'summary': {
                            'eps_beats': eps_beats,
                            'eps_misses': eps_misses,
                            'revenue_beats': revenue_beats,
                            'revenue_misses': revenue_misses,
                            'guidance_raised': guidance_raised_count,
                            'guidance_lowered': guidance_lowered_count
                        },
                        'symbol_filter': symbol,
                        'query_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Error getting earnings events: {e}")
            return {
                'success': False,
                'error': str(e),
                'events': [],
                'total_events': 0
            }

    def get_news_events(self, limit: int = 100, symbol: str = None) -> Dict[str, Any]:
        """Get recent news events from Polygon and Tiingo sources."""
        try:
            from core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Get recent news events from both sources
                    news_events = []

                    # Query Polygon news
                    polygon_query = """
                        SELECT
                            'Polygon' as source,
                            vendor_id as event_id,
                            title,
                            description,
                            published_utc as published_at,
                            tickers,
                            keywords,
                            article_url,
                            publisher_name,
                            created_at
                        FROM dev_news_polygon
                        WHERE 1=1
                    """

                    if symbol:
                        polygon_query += " AND %s = ANY(tickers)"
                        params = [symbol.upper()]
                    else:
                        params = []

                    polygon_query += " ORDER BY published_utc DESC LIMIT %s"
                    params.append(limit // 2)

                    try:
                        cursor.execute(polygon_query, params)
                        polygon_news = cursor.fetchall()

                        for news in polygon_news:
                            news_events.append({
                                'source': news['source'],
                                'event_id': news['event_id'],
                                'title': news['title'],
                                'description': news['description'][:500] + '...' if news['description'] and len(news['description']) > 500 else news['description'],
                                'published_at': news['published_at'].isoformat() if news['published_at'] else None,
                                'symbols': news['tickers'] or [],
                                'keywords': news['keywords'] or [],
                                'url': news['article_url'],
                                'publisher': news['publisher_name'],
                                'created_at': news['created_at'].isoformat() if news['created_at'] else None
                            })

                        logger.info(f"Retrieved {len(polygon_news)} Polygon news events")

                    except Exception as e:
                        logger.warning(f"Could not fetch Polygon news: {e}")

                    # Query Tiingo news
                    tiingo_query = """
                        SELECT
                            'Tiingo' as source,
                            vendor_id::text as event_id,
                            title,
                            description,
                            published_date as published_at,
                            tickers,
                            tags as keywords,
                            article_url as url,
                            source as publisher,
                            created_at
                        FROM dev_news_tiingo
                        WHERE 1=1
                    """

                    if symbol:
                        tiingo_query += " AND %s = ANY(tickers)"
                        params = [symbol.upper()]
                    else:
                        params = []

                    tiingo_query += " ORDER BY published_date DESC LIMIT %s"
                    params.append(limit // 2)

                    try:
                        cursor.execute(tiingo_query, params)
                        tiingo_news = cursor.fetchall()

                        for news in tiingo_news:
                            news_events.append({
                                'source': news['source'],
                                'event_id': news['event_id'],
                                'title': news['title'],
                                'description': news['description'][:500] + '...' if news['description'] and len(news['description']) > 500 else news['description'],
                                'published_at': news['published_at'].isoformat() if news['published_at'] else None,
                                'symbols': news['tickers'] or [],
                                'keywords': news['keywords'] or [],
                                'url': news['url'],
                                'publisher': news['publisher'],
                                'created_at': news['created_at'].isoformat() if news['created_at'] else None
                            })

                        logger.info(f"Retrieved {len(tiingo_news)} Tiingo news events")

                    except Exception as e:
                        logger.warning(f"Could not fetch Tiingo news: {e}")

                    # Sort all events by published date
                    news_events.sort(key=lambda x: x['published_at'] or '1970-01-01', reverse=True)

                    # Get summary statistics
                    total_events = len(news_events)
                    unique_symbols = set()
                    sources_count = {}

                    for event in news_events:
                        unique_symbols.update(event['symbols'])
                        source = event['source']
                        sources_count[source] = sources_count.get(source, 0) + 1

                    return {
                        'success': True,
                        'events': news_events[:limit],  # Limit final results
                        'total_events': total_events,
                        'unique_symbols': len(unique_symbols),
                        'sources': sources_count,
                        'symbol_filter': symbol,
                        'query_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Error getting news events: {e}")
            return {
                'success': False,
                'error': str(e),
                'events': [],
                'total_events': 0
            }

