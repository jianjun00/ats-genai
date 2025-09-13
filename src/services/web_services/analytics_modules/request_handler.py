"""
HTTP request handling and server management
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
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.platform.database.connection_manager import get_connection_manager
from core.config.settings import get_settings
from core.sanitizers.json_sanitizer import JSONSanitizer
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .analytics_service_core import UnifiedAnalyticsService


# ==============================================
# HTTP REQUEST HANDLER (from analytics_service.py)
# ==============================================

class UnifiedAnalyticsRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the unified analytics service."""

    def __init__(self, *args, **kwargs):
        self.analytics_service = UnifiedAnalyticsService()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests."""
        try:
            logger.info(f"📍 GET request: {self.path}")

            if self.path == '/health':
                self._serve_health_check()
            elif self.path == '/eda' or self.path == '/':
                self._serve_eda_dashboard()
            elif self.path.startswith('/api/intelligent-filters/'):
                self._serve_intelligent_filters()
            elif self.path.startswith('/api/universe-analytics'):
                self._serve_universe_analytics()
            elif self.path.startswith('/api/multi-panel-chart'):
                asyncio.run(self._serve_multi_panel_chart())
            elif self.path.startswith('/api/v1/training-datasets'):
                if '/navigation-metadata' in self.path:
                    self._serve_navigation_metadata()
                elif '/navigate' in self.path:
                    self._serve_navigation()
                elif '/multi-timeframe' in self.path:
                    self._serve_training_dataset_multi_timeframe()
                elif '/sequence/' in self.path:
                    self._serve_training_dataset_sequence()
                elif '/sequences' in self.path:
                    self._serve_training_dataset_sequences()
                elif '/visualization-data' in self.path:
                    self._serve_training_dataset_visualization_data()
                else:
                    self._serve_training_datasets()
            elif self.path.startswith('/api/ray-analytics/'):
                self._serve_ray_analytics()
            elif self.path.startswith('/api/news-events'):
                self._serve_news_events()
            elif self.path.startswith('/api/earnings-events'):
                self._serve_earnings_events()
            elif self.path == '/api/bar-collection-metrics':
                self._serve_bar_collection_metrics()
            elif self.path == '/api/tables':
                self._serve_tables_list()
            elif self.path.startswith('/api/table-info/'):
                self._serve_table_info()
            elif self.path.startswith('/api/table-columns/'):
                self._serve_table_columns()
            elif self.path.startswith('/api/table-sample/'):
                self._serve_table_sample()
            elif self.path.startswith('/api/table-distributions/'):
                self._serve_table_distributions()
            else:
                self._serve_404()

        except Exception as e:
            logger.error(f"Error handling GET request: {e}")
            self._serve_500(str(e))

    def _serve_health_check(self):
        """Serve health check response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        health_status = {
            "status": "healthy",
            "service": "ats-unified-analytics",
            "timestamp": datetime.now().isoformat(),
            "features": {
                "type_system": self.analytics_service.type_system_enabled,
                "ray_computing": self.analytics_service.ray_enabled,
                "universe_analytics": True,
                "training_datasets": True
            }
        }

        self.wfile.write(json.dumps(health_status).encode('utf-8'))

    def _serve_eda_dashboard(self):
        """Serve the unified EDA dashboard."""
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()

        html_content = self.analytics_service.get_eda_dashboard_html()
        self.wfile.write(html_content.encode('utf-8'))

    def _serve_intelligent_filters(self):
        """Serve intelligent filter definitions."""
        # Extract table name from path
        path_parts = self.path.split('/')
        table_name = path_parts[-1] if len(path_parts) > 3 else 'default'

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        # This would be async in a real implementation
        filters = asyncio.run(self.analytics_service.get_intelligent_filters(table_name))
        self.wfile.write(json.dumps(filters, indent=2).encode('utf-8'))

    def _serve_universe_analytics(self):
        """Serve universe analytics."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        analytics = asyncio.run(self.analytics_service.get_universe_analytics())
        self.wfile.write(json.dumps(analytics, indent=2).encode('utf-8'))

    async def _serve_multi_panel_chart(self):
        """Serve multi-panel chart generation API."""
        try:
            # Parse query parameters
            from urllib.parse import urlparse, parse_qs
            parsed_url = urlparse(self.path)
            params = parse_qs(parsed_url.query)

            symbol = params.get('symbol', ['AAPL'])[0]
            timeframe = params.get('timeframe', ['1h'])[0]
            dataset_id = int(params.get('dataset_id', ['1'])[0])

            # Generate chart
            result = await self.analytics_service.generate_multi_panel_chart(
                symbol=symbol,
                timeframe=timeframe,
                dataset_id=dataset_id
            )

            # Send response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            self.wfile.write(json.dumps(result).encode('utf-8'))

        except Exception as e:
            logger.error(f"❌ Error serving multi-panel chart: {e}")

            error_response = {
                "success": False,
                "error": f"Server error: {str(e)}"
            }

            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            self.wfile.write(json.dumps(error_response).encode('utf-8'))

    def _serve_training_datasets(self):
        """Serve training datasets."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        datasets = self.analytics_service.get_training_datasets()
        self.wfile.write(json.dumps(datasets, indent=2).encode('utf-8'))

    def _serve_training_dataset_sequence(self):
        """Serve training dataset sequence data for OHLC visualization."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id and row_index from path like /api/v1/training-datasets/sequence/1/50
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[5])  # /api/v1/training-datasets/sequence/{dataset_id}/{row_index}
            row_index = int(path_parts[6]) if len(path_parts) > 6 else 0
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or row_index"}).encode('utf-8'))
            return

        # Extract timeframe from query parameters (e.g., ?timeframe=5m)
        timeframe = query_params.get('timeframe', ['5m'])[0]  # Default to 5m if not specified

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            sequence_data = self.analytics_service.get_training_dataset_sequence(dataset_id, row_index, timeframe)
            self.wfile.write(json.dumps(sequence_data, indent=2, default=str).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting sequence data for dataset {dataset_id}, row {row_index}, timeframe {timeframe}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "row_index": row_index,
                "timeframe": timeframe,
                "message": "No ArrayRecord data available - please generate training data first"
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_training_dataset_sequences(self):
        """Serve available sequences for a training dataset."""
        from urllib.parse import urlparse

        # Extract dataset_id from path like /api/v1/training-datasets/38/sequences
        path_parts = urlparse(self.path).path.split('/')
        try:
            dataset_id = int(path_parts[4])  # /api/v1/training-datasets/{dataset_id}/sequences
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id"}).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            sequences = self.analytics_service.get_training_dataset_sequences(dataset_id)
            self.wfile.write(json.dumps(sequences, indent=2).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting sequences for dataset {dataset_id}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "sequences": [],
                "total_count": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_training_dataset_visualization_data(self):
        """Serve visualization data for training dataset sequences."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id from path like /api/v1/training-datasets/{dataset_id}/visualization-data
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[4])  # /api/v1/training-datasets/{dataset_id}/visualization-data
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id"}).encode('utf-8'))
            return

        # Extract query parameters
        start_idx = int(query_params.get('start_idx', ['0'])[0])
        sequence_id = query_params.get('sequence_id', [None])[0]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Get visualization data from the analytics service
            viz_data = self.analytics_service.get_training_dataset_visualization_data(dataset_id, start_idx, sequence_id)
            self.wfile.write(json.dumps(viz_data, indent=2, default=str).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting visualization data for dataset {dataset_id}, start_idx {start_idx}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "start_idx": start_idx,
                "message": "No ArrayRecord data available - please generate training data first"
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_training_dataset_multi_timeframe(self):
        """Serve multi-timeframe OHLC data for a specific sequence."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id and sequence_id from path like /api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[4])  # /api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe
            sequence_id = path_parts[6]      # sequence_id
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or sequence_id"}).encode('utf-8'))
            return

        # Extract row_index from query parameters (e.g., ?row_index=50)
        row_index = int(query_params.get('row_index', [50])[0])
        logger.info(f"Multi-timeframe request: dataset_id={dataset_id}, sequence_id={sequence_id}, row_index={row_index}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Get multi-timeframe data from the analytics service with row index
            multi_data = self.analytics_service.get_training_dataset_sequence_multi_timeframe(dataset_id, sequence_id, row_index)
            self.wfile.write(json.dumps(multi_data, indent=2, default=str).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting multi-timeframe data for dataset {dataset_id}, sequence {sequence_id}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "sequence_id": sequence_id,
                "message": "Failed to load multi-timeframe data"
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_navigation_metadata(self):
        """Serve navigation metadata for a sequence."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL - /api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/navigation-metadata
        path_parts = self.path.split('/')
        try:
            dataset_id = int(path_parts[4])
            sequence_id = path_parts[6]
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or sequence_id"}).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Test multiple row_index values to find available range
            available_positions = []
            max_position = 0

            # Test positions to find working range
            for test_index in [0, 10, 25, 50, 75, 100]:
                try:
                    result = self.analytics_service.get_training_dataset_sequence_multi_timeframe(
                        dataset_id, sequence_id, test_index
                    )

                    if result.get('success') and result.get('table_data'):
                        table_data = result['table_data']
                        if table_data and len(table_data) > 0:
                            available_positions.append({
                                'row_index': test_index,
                                'bars': len(table_data),
                                'start_timestamp': table_data[0].get('timestamp'),
                                'end_timestamp': table_data[-1].get('timestamp'),
                                'start_price': table_data[0].get('open'),
                                'end_price': table_data[-1].get('close')
                            })
                            max_position = max(max_position, test_index)

                except Exception:
                    break

            # Convert timestamps to readable dates
            def format_timestamp(ts):
                if ts:
                    try:
                        from datetime import datetime
                        return datetime.fromtimestamp(ts).isoformat()
                    except:
                        return ts
                return None

            # Prepare metadata
            metadata = {
                'sequence_id': sequence_id,
                'dataset_id': dataset_id,
                'navigation': {
                    'min_row_index': 0,
                    'max_row_index': max_position,
                    'total_positions': max_position + 1,
                    'window_size': 21,
                    'default_position': 10
                },
                'sample_positions': [
                    {
                        'row_index': pos['row_index'],
                        'description': f"Position {pos['row_index']} ({pos['bars']} bars)",
                        'start_time': format_timestamp(pos['start_timestamp']),
                        'end_time': format_timestamp(pos['end_timestamp']),
                        'price_range': {
                            'start': pos['start_price'],
                            'end': pos['end_price']
                        }
                    }
                    for pos in available_positions[:5]
                ],
                'timeframes_available': ['5m', '15m', '1h', '1d', '1w']
            }

            self.wfile.write(json.dumps(metadata, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error getting navigation metadata: {e}")
            error_response = {
                'error': str(e),
                'sequence_id': sequence_id,
                'dataset_id': dataset_id
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_navigation(self):
        """Serve navigation to a specific position in the sequence."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id and sequence_id from path
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[4])
            sequence_id = path_parts[6]
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or sequence_id"}).encode('utf-8'))
            return

        # Get navigation parameters
        row_index = int(query_params.get('row_index', [10])[0])
        direction = query_params.get('direction', [None])[0]

        logger.info(f"🔍 NAVIGATION DEBUG: dataset_id={dataset_id}, sequence_id={sequence_id}, initial_row_index={row_index}, direction={direction}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Handle navigation directions
            original_row_index = row_index
            if direction:
                # Get current valid range (simplified)
                max_position = 100  # Default max, could be determined dynamically
                min_position = 0

                if direction == 'next':
                    row_index = min(row_index + 10, max_position)
                elif direction == 'prev':
                    row_index = max(row_index - 10, min_position)
                elif direction == 'first':
                    row_index = min_position
                elif direction == 'last':
                    row_index = max_position

            logger.info(f"🔍 NAVIGATION DEBUG: direction={direction}, original_row={original_row_index} -> new_row={row_index}")

            # Get the data for the specified position
            result = self.analytics_service.get_training_dataset_sequence_multi_timeframe(
                dataset_id, sequence_id, row_index
            )

            logger.info(f"🔍 NAVIGATION DEBUG: API call result success={result.get('success')}, table_data_count={len(result.get('table_data', []))}")

            # Add navigation context to the response
            if result.get('success'):
                result['navigation_context'] = {
                    'current_row_index': row_index,
                    'direction_used': direction,
                    'timestamp_range': {
                        'start': result['table_data'][0].get('timestamp') if result.get('table_data') else None,
                        'end': result['table_data'][-1].get('timestamp') if result.get('table_data') else None
                    }
                }

            self.wfile.write(json.dumps(result, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Navigation failed: {e}")
            error_response = {
                'error': str(e),
                'sequence_id': sequence_id,
                'dataset_id': dataset_id,
                'requested_row_index': row_index
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_ray_analytics(self):
        """Serve Ray distributed analytics."""
        # Extract dataset ID from path
        path_parts = self.path.split('/')
        dataset_id = path_parts[-1] if len(path_parts) > 3 else '1'

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        analytics = asyncio.run(self.analytics_service.get_ray_analytics(dataset_id))
        self.wfile.write(json.dumps(analytics, indent=2).encode('utf-8'))

    def _serve_bar_collection_metrics(self):
        """Serve bar collection metrics."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        metrics = self.analytics_service.get_bar_collection_metrics()
        self.wfile.write(json.dumps(metrics, indent=2, default=str).encode('utf-8'))

    def _serve_tables_list(self):
        """Serve list of database tables."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public'
                        AND tablename LIKE %s
                        ORDER BY tablename
                    """, ('dev_%',))

                    tables = [row['tablename'] for row in cursor.fetchall()]
                    response = {"tables": tables}

        except Exception as e:
            logger.error(f"Error getting tables list: {e}")
            response = {
                "tables": [
                    "dev_daily_price", "dev_training_dataset", "dev_instrument",
                    "dev_daily_price_polygon", "dev_daily_price_tiingo", "dev_daily_price_eodhd"
                ],
                "error": str(e)
            }

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_table_info(self):
        """Serve table information."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor
            from psycopg2 import sql

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get row count (using safe SQL identifier)
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) as count FROM {}").format(
                            sql.Identifier(table_name)
                        )
                    )
                    row_count = cursor.fetchone()['count']

                    # Get column count
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM information_schema.columns
                        WHERE table_name = %s
                    """, (table_name,))
                    column_count = cursor.fetchone()['count']

                    # Get table size
                    cursor.execute("""
                        SELECT pg_size_pretty(pg_total_relation_size(%s)) as size
                    """, (table_name,))
                    size = cursor.fetchone()['size']

                    response = {
                        "table_name": table_name,
                        "row_count": row_count,
                        "column_count": column_count,
                        "size": size,
                        "last_updated": "Unknown"
                    }

        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name}

        self.wfile.write(JSONSanitizer.safe_json_dumps(response, indent=2).encode('utf-8'))

    def _serve_table_columns(self):
        """Serve table column information."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))

                    columns = []
                    for row in cursor.fetchall():
                        columns.append({
                            "name": row['column_name'],
                            "type": row['data_type'],
                            "nullable": row['is_nullable'] == 'YES'
                        })

                    response = {"table_name": table_name, "columns": columns}

        except Exception as e:
            logger.error(f"Error getting columns for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name, "columns": []}

        self.wfile.write(JSONSanitizer.safe_json_dumps(response, indent=2).encode('utf-8'))

    def _serve_table_sample(self):
        """Serve sample data from table."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    from psycopg2 import sql
                    cursor.execute(
                        sql.SQL("SELECT * FROM {} LIMIT 10").format(
                            sql.Identifier(table_name)
                        )
                    )

                    rows = []
                    for row in cursor.fetchall():
                        row_dict = dict(row)
                        # Use JSON sanitizer to handle all data types (Decimal, datetime, etc.)
                        sanitized_row = JSONSanitizer.sanitize_value(row_dict)
                        rows.append(sanitized_row)

                    response = {"table_name": table_name, "rows": rows}

        except Exception as e:
            logger.error(f"Error getting sample data for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name, "rows": []}

        self.wfile.write(JSONSanitizer.safe_json_dumps(response, indent=2).encode('utf-8'))

    def _serve_table_distributions(self):
        """Serve column distributions and statistics."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from psycopg2 import sql

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get column info first
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))

                    columns = {}
                    for row in cursor.fetchall():
                        column_name, data_type = row['column_name'], row['data_type']
                        try:
                            # Basic statistics for each column using safe query construction
                            cursor.execute(
                                sql.SQL("""
                                    SELECT
                                        COUNT(*) as count,
                                        COUNT(DISTINCT {}) as unique,
                                        COUNT(*) - COUNT({}) as nulls
                                    FROM {}
                                """).format(
                                    sql.Identifier(column_name),
                                    sql.Identifier(column_name),
                                    sql.Identifier(table_name)
                                )
                            )

                            result = cursor.fetchone()
                            count, unique, nulls = result['count'], result['unique'], result['nulls']

                            stats = {
                                "count": count,
                                "unique": unique,
                                "nulls": nulls,
                                "type": data_type
                            }

                            # For numeric columns, get min/max using safe query construction
                            if data_type in ['integer', 'bigint', 'numeric', 'real', 'double precision']:
                                cursor.execute(
                                    sql.SQL("SELECT MIN({}) as min_val, MAX({}) as max_val FROM {}").format(
                                        sql.Identifier(column_name),
                                        sql.Identifier(column_name),
                                        sql.Identifier(table_name)
                                    )
                                )
                                result = cursor.fetchone()
                                if result['min_val'] is not None:
                                    # Use JSON sanitizer for min/max values (handles Decimal types)
                                    stats["min"] = JSONSanitizer.sanitize_value(result['min_val'])
                                    stats["max"] = JSONSanitizer.sanitize_value(result['max_val'])

                            # For text columns, get top values using safe query construction
                            elif data_type in ['text', 'character varying', 'character']:
                                cursor.execute(
                                    sql.SQL("""
                                        SELECT {}, COUNT(*) as freq
                                        FROM {}
                                        WHERE {} IS NOT NULL
                                        GROUP BY {}
                                        ORDER BY freq DESC
                                        LIMIT 5
                                    """).format(
                                        sql.Identifier(column_name),
                                        sql.Identifier(table_name),
                                        sql.Identifier(column_name),
                                        sql.Identifier(column_name)
                                    )
                                )

                                top_values = [row[column_name] for row in cursor.fetchall()]
                                if top_values:
                                    stats["top_values"] = top_values

                            columns[column_name] = stats

                        except Exception as col_error:
                            logger.error(f"Error analyzing column {column_name}: {col_error}")
                            columns[column_name] = {"error": str(col_error)}

                    response = {"table_name": table_name, "columns": columns}

        except Exception as e:
            logger.error(f"Error getting distributions for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name, "columns": {}}

        self.wfile.write(JSONSanitizer.safe_json_dumps(response, indent=2).encode('utf-8'))

    def _serve_news_events(self):
        """Serve news events from Polygon and Tiingo sources."""
        from urllib.parse import urlparse, parse_qs

        # Parse query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Get parameters
        limit = int(query_params.get('limit', [100])[0])
        symbol = query_params.get('symbol', [None])[0]

        # Limit the results to reasonable bounds
        limit = min(limit, 500)  # Max 500 events

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Get news events from the analytics service
            news_data = self.analytics_service.get_news_events(limit=limit, symbol=symbol)
            self.wfile.write(json.dumps(news_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving news events: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "events": [],
                "total_events": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_earnings_events(self):
        """Serve earnings events from dev_earnings_events table."""
        from urllib.parse import urlparse, parse_qs

        # Parse query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Get parameters
        limit = int(query_params.get('limit', [100])[0])
        symbol = query_params.get('symbol', [None])[0]

        # Limit the results to reasonable bounds
        limit = min(limit, 500)  # Max 500 events

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Get earnings events from the analytics service
            earnings_data = self.analytics_service.get_earnings_events(limit=limit, symbol=symbol)
            self.wfile.write(json.dumps(earnings_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving earnings events: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "events": [],
                "total_events": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_404(self):
        """Serve 404 response."""
        self.send_response(404)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        error_response = {
            "error": "Not found",
            "path": self.path,
            "available_endpoints": [
                "/health", "/eda", "/api/intelligent-filters/{table}",
                "/api/universe-analytics", "/api/v1/training-datasets",
                "/api/news-events", "/api/earnings-events", "/api/ray-analytics/{dataset_id}", "/api/multi-panel-chart"
            ]
        }

        self.wfile.write(JSONSanitizer.safe_json_dumps(error_response).encode('utf-8'))

    def _serve_500(self, error_message: str):
        """Serve 500 response."""
        self.send_response(500)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        error_response = {
            "error": "Internal server error",
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        }

        self.wfile.write(JSONSanitizer.safe_json_dumps(error_response).encode('utf-8'))


def start_unified_analytics_server(port: int = 3000):
    """Start the unified analytics server."""
    logger.info("🚀 Starting ATS Unified Analytics Service")
    logger.info(f"   Port: {port}")
    logger.info("   Features: Type-aware EDA, Universe Analytics, Ray Computing, Training Datasets")

    server = ThreadingHTTPServer(('0.0.0.0', port), UnifiedAnalyticsRequestHandler)

    try:
        logger.info(f"✅ Server started at http://0.0.0.0:{port}")
        logger.info("   Available endpoints:")
        logger.info("   • /health - Health check")
        logger.info("   • /eda - Main dashboard")
        logger.info("   • /api/intelligent-filters/{table} - Type-aware filters")
        logger.info("   • /api/universe-analytics - Cross-instrument analysis")
        logger.info("   • /api/v1/training-datasets - ML dataset management")
        logger.info("   • /api/ray-analytics/{dataset_id} - Distributed analytics")

        server.serve_forever()

    except KeyboardInterrupt:
        logger.info("🛑 Shutting down unified analytics service...")
        server.shutdown()
        logger.info("✅ Service stopped")


if __name__ == "__main__":
    port = int(os.getenv('ANALYTICS_PORT', 3000))
    start_unified_analytics_server(port)