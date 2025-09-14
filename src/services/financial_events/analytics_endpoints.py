"""
Analytics Service Endpoints Extension for Financial Events
Adds xAI financial events endpoints to the existing analytics service
"""

import asyncio
import json
import logging
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any

logger = logging.getLogger(__name__)

class FinancialEventsEndpoints:
    """
    Endpoint handlers for financial events in analytics service
    Add these to the existing analytics service HTTP handler
    """

    def __init__(self, analytics_integration):
        self.integration = analytics_integration

    def route_financial_events(self, method: str, path: str, query_params: Dict[str, Any], body: Dict[str, Any] = None) -> Dict[str, Any]:
        """Route financial events API requests"""

        try:
            if path == "/financial_events/extract" and method == "POST":
                # Extract events from xAI and store in database
                return asyncio.run(self._handle_extract_events(body or {}))

            elif path == "/financial_events" and method == "GET":
                # Get stored financial events with filters
                return self._handle_get_events(query_params)

            elif path == "/financial_events/summary" and method == "GET":
                # Get events summary and statistics
                return self._handle_get_summary(query_params)

            elif path == "/financial_events/cache/stats" and method == "GET":
                # Get cache performance statistics
                return asyncio.run(self._handle_cache_stats())

            elif path == "/financial_events/cache/clear" and method == "POST":
                # Clear events cache
                return asyncio.run(self._handle_clear_cache())

            else:
                return {
                    "success": False,
                    "error": f"Financial events endpoint not found: {method} {path}",
                    "available_endpoints": [
                        "POST /financial_events/extract - Extract and store events",
                        "GET /financial_events - Query stored events",
                        "GET /financial_events/summary - Get events statistics",
                        "GET /financial_events/cache/stats - Get cache statistics",
                        "POST /financial_events/cache/clear - Clear cache"
                    ]
                }

        except Exception as e:
            logger.error(f"❌ Error handling financial events request: {e}")
            return {
                "success": False,
                "error": f"Internal server error: {str(e)}"
            }

    async def _handle_extract_events(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle event extraction request"""

        # Validate required parameters
        if not params.get('start_date') or not params.get('end_date'):
            return {
                "success": False,
                "error": "start_date and end_date are required",
                "example": {
                    "start_date": "2025-09-01",
                    "end_date": "2025-09-13",
                    "symbols": ["AAPL", "TSLA"],
                    "force_refresh": False
                }
            }

        # Extract and store events
        result = await self.integration.extract_and_store_events(
            start_date=params['start_date'],
            end_date=params['end_date'],
            symbols=params.get('symbols', []),
            force_refresh=params.get('force_refresh', False)
        )

        return result

    def _handle_get_events(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle get events request"""

        # Convert query parameters
        symbol = params.get('symbol')
        event_type = params.get('event_type')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        impact_level = params.get('impact_level')
        limit = int(params.get('limit', [100])[0]) if isinstance(params.get('limit'), list) else int(params.get('limit', 100))

        return self.integration.get_events_from_analytics(
            symbol=symbol,
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            impact_level=impact_level,
            limit=limit
        )

    def _handle_get_summary(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle get summary request"""
        return self.integration.get_events_summary()

    async def _handle_cache_stats(self) -> Dict[str, Any]:
        """Handle cache statistics request"""

        try:
            cache_stats = await self.integration.event_extractor.get_cache_stats()
            return {
                "success": True,
                "cache_statistics": cache_stats,
                "timestamp": asyncio.get_event_loop().time()
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to get cache stats: {e}"
            }

    async def _handle_clear_cache(self) -> Dict[str, Any]:
        """Handle clear cache request"""

        try:
            await self.integration.event_extractor.clear_cache()
            return {
                "success": True,
                "message": "Cache cleared successfully"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to clear cache: {e}"
            }

# HTML Dashboard for Financial Events
FINANCIAL_EVENTS_HTML = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATS Financial Events Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: rgba(255,255,255,0.1);
            padding: 30px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
        }
        .header {
            text-align: center;
            margin-bottom: 40px;
            border-bottom: 2px solid rgba(255,255,255,0.3);
            padding-bottom: 20px;
        }
        .header h1 {
            font-size: 2.5em;
            margin: 0;
            background: linear-gradient(45deg, #FFD700, #FFA500);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .controls {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
            padding: 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
        }
        .control-group {
            display: flex;
            flex-direction: column;
        }
        .control-group label {
            margin-bottom: 5px;
            font-weight: bold;
            color: #FFD700;
        }
        .control-group input, .control-group select, .control-group button {
            padding: 10px;
            border: none;
            border-radius: 8px;
            background: rgba(255,255,255,0.9);
            color: #333;
            font-size: 14px;
        }
        .btn {
            background: linear-gradient(45deg, #FF6B6B, #FF8E53);
            color: white;
            cursor: pointer;
            font-weight: bold;
            transition: all 0.3s ease;
        }
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.3);
        }
        .btn.extract {
            background: linear-gradient(45deg, #4ECDC4, #44A08D);
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: linear-gradient(135deg, rgba(255,255,255,0.2), rgba(255,255,255,0.1));
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.3);
        }
        .stat-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #FFD700;
            margin: 10px 0;
        }
        .events-container {
            background: rgba(255,255,255,0.1);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.3);
        }
        .events-header {
            background: linear-gradient(45deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            font-size: 1.3em;
            font-weight: bold;
        }
        .event-item {
            padding: 15px 20px;
            border-bottom: 1px solid rgba(255,255,255,0.2);
            transition: background 0.3s ease;
        }
        .event-item:hover {
            background: rgba(255,255,255,0.1);
        }
        .event-meta {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .event-symbol {
            background: #FFD700;
            color: #333;
            padding: 4px 12px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 0.9em;
        }
        .event-type {
            background: #4ECDC4;
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8em;
            text-transform: uppercase;
        }
        .event-date {
            color: #FFD700;
            font-weight: bold;
        }
        .impact-high { border-left: 5px solid #FF6B6B; }
        .impact-medium { border-left: 5px solid #FFD700; }
        .impact-low { border-left: 5px solid #4ECDC4; }
        .loading {
            text-align: center;
            padding: 40px;
            font-size: 1.2em;
            color: #FFD700;
        }
        .status {
            padding: 15px;
            margin: 10px 0;
            border-radius: 8px;
            text-align: center;
            font-weight: bold;
        }
        .status.success { background: rgba(76, 175, 80, 0.3); border: 1px solid #4CAF50; }
        .status.error { background: rgba(244, 67, 54, 0.3); border: 1px solid #f44336; }
        .status.info { background: rgba(33, 150, 243, 0.3); border: 1px solid #2196F3; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 ATS Financial Events Dashboard</h1>
            <p>Real-time financial event extraction and analysis powered by xAI Grok</p>
        </div>

        <div class="controls">
            <div class="control-group">
                <label>Start Date</label>
                <input type="date" id="startDate" value="2025-09-01">
            </div>
            <div class="control-group">
                <label>End Date</label>
                <input type="date" id="endDate" value="2025-09-13">
            </div>
            <div class="control-group">
                <label>Symbols (comma-separated)</label>
                <input type="text" id="symbols" placeholder="AAPL, TSLA, MSFT" value="AAPL,TSLA,MSFT">
            </div>
            <div class="control-group">
                <label>Event Type</label>
                <select id="eventType">
                    <option value="">All Types</option>
                    <option value="earnings">Earnings</option>
                    <option value="fed_announcement">Fed Announcement</option>
                    <option value="stock_event">Stock Event</option>
                    <option value="economic_indicator">Economic Indicator</option>
                    <option value="m_a">M&A</option>
                </select>
            </div>
            <div class="control-group">
                <label>Impact Level</label>
                <select id="impactLevel">
                    <option value="">All Levels</option>
                    <option value="high">High Impact</option>
                    <option value="medium">Medium Impact</option>
                    <option value="low">Low Impact</option>
                </select>
            </div>
            <div class="control-group">
                <label>Actions</label>
                <button class="btn extract" onclick="extractEvents()">🎯 Extract Events</button>
                <button class="btn" onclick="loadEvents()" style="margin-top: 10px;">📊 Load Events</button>
            </div>
        </div>

        <div id="statusMessage"></div>

        <div class="stats-grid">
            <div class="stat-card">
                <div>Total Events</div>
                <div class="stat-value" id="totalEvents">-</div>
            </div>
            <div class="stat-card">
                <div>High Impact</div>
                <div class="stat-value" id="highImpactEvents">-</div>
            </div>
            <div class="stat-card">
                <div>Unique Symbols</div>
                <div class="stat-value" id="uniqueSymbols">-</div>
            </div>
            <div class="stat-card">
                <div>Cache Hit Rate</div>
                <div class="stat-value" id="cacheHitRate">-</div>
            </div>
        </div>

        <div class="events-container">
            <div class="events-header">📈 Financial Events</div>
            <div id="eventsContainer">
                <div class="loading">Click "Load Events" to view stored financial events</div>
            </div>
        </div>
    </div>

    <script>
        async function extractEvents() {
            const status = document.getElementById('statusMessage');
            status.innerHTML = '<div class="status info">🔄 Extracting events from xAI Grok...</div>';

            const symbols = document.getElementById('symbols').value
                .split(',')
                .map(s => s.trim())
                .filter(s => s.length > 0);

            const params = {
                start_date: document.getElementById('startDate').value,
                end_date: document.getElementById('endDate').value,
                symbols: symbols,
                force_refresh: false
            };

            try {
                const response = await fetch('/financial_events/extract', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(params)
                });

                const result = await response.json();

                if (result.success) {
                    status.innerHTML = `<div class="status success">
                        ✅ Successfully extracted ${result.events_extracted} events and stored ${result.events_stored} events!
                    </div>`;
                    await loadEvents();
                } else {
                    status.innerHTML = `<div class="status error">❌ Error: ${result.error}</div>`;
                }
            } catch (error) {
                status.innerHTML = `<div class="status error">❌ Network error: ${error.message}</div>`;
            }
        }

        async function loadEvents() {
            const container = document.getElementById('eventsContainer');
            container.innerHTML = '<div class="loading">🔄 Loading events...</div>';

            const params = new URLSearchParams();
            const symbol = document.getElementById('symbols').value.split(',')[0]?.trim();
            if (symbol) params.set('symbol', symbol);

            const eventType = document.getElementById('eventType').value;
            if (eventType) params.set('event_type', eventType);

            const impactLevel = document.getElementById('impactLevel').value;
            if (impactLevel) params.set('impact_level', impactLevel);

            params.set('start_date', document.getElementById('startDate').value);
            params.set('end_date', document.getElementById('endDate').value);
            params.set('limit', '20');

            try {
                const response = await fetch(`/financial_events?${params}`);
                const result = await response.json();

                if (result.success) {
                    displayEvents(result.events);
                    await updateStats();
                } else {
                    container.innerHTML = `<div class="loading">❌ Error loading events: ${result.error}</div>`;
                }
            } catch (error) {
                container.innerHTML = `<div class="loading">❌ Network error: ${error.message}</div>`;
            }
        }

        function displayEvents(events) {
            const container = document.getElementById('eventsContainer');

            if (events.length === 0) {
                container.innerHTML = '<div class="loading">📭 No events found for the specified criteria</div>';
                return;
            }

            const eventsHtml = events.map(event => `
                <div class="event-item impact-${event.impact_level}">
                    <div class="event-meta">
                        <div>
                            ${event.company_symbol ? `<span class="event-symbol">${event.company_symbol}</span>` : ''}
                            <span class="event-type">${event.event_type}</span>
                        </div>
                        <div class="event-date">${event.event_date} ${event.event_time || ''}</div>
                    </div>
                    <div>${event.details}</div>
                    ${event.sentiment ? `<div style="margin-top: 8px; color: #4ECDC4;">Sentiment: ${event.sentiment}</div>` : ''}
                </div>
            `).join('');

            container.innerHTML = eventsHtml;
        }

        async function updateStats() {
            try {
                const response = await fetch('/financial_events/summary');
                const result = await response.json();

                if (result.success && result.summary.length > 0) {
                    const stats = result.summary[0];
                    document.getElementById('totalEvents').textContent = stats.total_events || 0;
                    document.getElementById('highImpactEvents').textContent = stats.high_impact_events || 0;
                    document.getElementById('uniqueSymbols').textContent = stats.unique_symbols || 0;
                }

                // Update cache stats
                const cacheResponse = await fetch('/financial_events/cache/stats');
                const cacheResult = await cacheResponse.json();

                if (cacheResult.success) {
                    const hitRate = cacheResult.cache_statistics.hit_rate || 'N/A';
                    document.getElementById('cacheHitRate').textContent = hitRate;
                }

            } catch (error) {
                console.error('Error updating stats:', error);
            }
        }

        // Load events on page load
        document.addEventListener('DOMContentLoaded', () => {
            loadEvents();
        });
    </script>
</body>
</html>
'''