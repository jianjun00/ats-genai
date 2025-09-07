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
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings


    # ==============================================
    # NEWS EVENTS ANALYSIS
    # ==============================================
    
    def get_news_events(self, limit: int = 100, symbol: str = None) -> Dict[str, Any]:
        """Get recent news events from Polygon and Tiingo sources."""
        try:
            from core.database.connection_manager import get_raw_connection
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

