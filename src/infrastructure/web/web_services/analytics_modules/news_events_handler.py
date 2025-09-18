"""
News events and economic data handling
"""

#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from src.core.database.connection_manager import get_connection_manager
from src.core.config.settings import get_settings


    # ==============================================
    # NEWS EVENTS ANALYSIS
    # ==============================================

    def get_earnings_events(self, limit: int = 100, symbol: str = None) -> Dict[str, Any]:
        """Get recent earnings events from dev_earnings_events table."""
        try:
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime, timedelta

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
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime, timedelta

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

