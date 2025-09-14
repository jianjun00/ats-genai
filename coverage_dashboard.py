#!/usr/bin/env python3
"""
Data Coverage Monitoring Dashboard
Real-time web dashboard for monitoring data coverage gaps and backfill operations
"""

import asyncio
import asyncpg
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import os
import threading

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoverageDashboardHandler(BaseHTTPRequestHandler):
    """HTTP handler for the coverage dashboard."""

    def __init__(self, *args, db_pool=None, **kwargs):
        self.db_pool = db_pool
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)

        try:
            if path == '/' or path == '/dashboard':
                self.serve_dashboard()
            elif path == '/api/coverage-summary':
                self.serve_coverage_summary()
            elif path == '/api/priority-gaps':
                self.serve_priority_gaps()
            elif path == '/api/coverage-trend':
                self.serve_coverage_trend()
            elif path == '/api/backfill-queue':
                self.serve_backfill_queue()
            elif path == '/api/recent-operations':
                self.serve_recent_operations()
            elif path == '/health':
                self.serve_health()
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            logger.error(f"Error handling request {path}: {e}")
            self.send_error(500, f"Internal Server Error: {str(e)}")

    def serve_dashboard(self):
        """Serve the main dashboard HTML."""
        html_content = self.generate_dashboard_html()
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))

    def serve_coverage_summary(self):
        """Serve coverage summary data."""
        async def get_data():
            async with self.db_pool.acquire() as conn:
                return await conn.fetch("""
                    SELECT vendor, data_type, total_symbols, symbols_complete,
                           symbols_missing, coverage_percentage, last_scan_time
                    FROM v_current_coverage_summary
                    ORDER BY vendor, data_type
                """)

        data = asyncio.run(get_data())
        response = [dict(row) for row in data]

        # Convert datetime to string
        for item in response:
            if item['last_scan_time']:
                item['last_scan_time'] = item['last_scan_time'].isoformat()

        self.send_json_response(response)

    def serve_priority_gaps(self):
        """Serve priority gaps requiring backfill."""
        async def get_data():
            async with self.db_pool.acquire() as conn:
                return await conn.fetch("""
                    SELECT vendor, data_type, symbol, gap_start_date, gap_end_date,
                           gap_days, priority_score, adjusted_priority, backfill_status,
                           estimated_effort_minutes, created_at
                    FROM v_active_backfill_queue
                    LIMIT 50
                """)

        data = asyncio.run(get_data())
        response = []
        for row in data:
            item = dict(row)
            # Convert dates to strings
            item['gap_start_date'] = item['gap_start_date'].isoformat()
            item['gap_end_date'] = item['gap_end_date'].isoformat()
            item['created_at'] = item['created_at'].isoformat()
            response.append(item)

        self.send_json_response(response)

    def serve_coverage_trend(self):
        """Serve coverage trending data."""
        async def get_data():
            async with self.db_pool.acquire() as conn:
                return await conn.fetch("""
                    SELECT metric_date, vendor, data_type, coverage_percentage,
                           coverage_change
                    FROM v_coverage_trending
                    WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
                    ORDER BY vendor, data_type, metric_date
                """)

        data = asyncio.run(get_data())
        response = []
        for row in data:
            item = dict(row)
            item['metric_date'] = item['metric_date'].isoformat()
            response.append(item)

        self.send_json_response(response)

    def serve_backfill_queue(self):
        """Serve backfill queue status."""
        async def get_data():
            async with self.db_pool.acquire() as conn:
                return await conn.fetch("""
                    SELECT
                        COUNT(*) as total_gaps,
                        COUNT(CASE WHEN backfill_status = 'pending' THEN 1 END) as pending,
                        COUNT(CASE WHEN backfill_status = 'in_progress' THEN 1 END) as in_progress,
                        COUNT(CASE WHEN backfill_status = 'completed' THEN 1 END) as completed,
                        COUNT(CASE WHEN backfill_status = 'failed' THEN 1 END) as failed,
                        AVG(priority_score) as avg_priority,
                        SUM(estimated_effort_minutes) as total_effort_minutes
                    FROM dev_coverage_gaps
                    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                """)

        data = asyncio.run(get_data())
        response = dict(data[0]) if data else {}

        self.send_json_response(response)

    def serve_recent_operations(self):
        """Serve recent backfill operations."""
        async def get_data():
            async with self.db_pool.acquire() as conn:
                return await conn.fetch("""
                    SELECT operation_type, vendor, data_type,
                           array_length(symbols_requested, 1) as symbols_count,
                           status, duration_seconds, created_at, completed_at,
                           total_files_updated, error_log
                    FROM dev_backfill_operations
                    WHERE created_at >= NOW() - INTERVAL '7 days'
                    ORDER BY created_at DESC
                    LIMIT 20
                """)

        data = asyncio.run(get_data())
        response = []
        for row in data:
            item = dict(row)
            item['created_at'] = item['created_at'].isoformat()
            if item['completed_at']:
                item['completed_at'] = item['completed_at'].isoformat()
            response.append(item)

        self.send_json_response(response)

    def serve_health(self):
        """Serve health check."""
        self.send_json_response({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

    def send_json_response(self, data):
        """Send JSON response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode('utf-8'))

    def generate_dashboard_html(self):
        """Generate the main dashboard HTML."""
        return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATS Data Coverage Monitoring Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            color: #333;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem 2rem;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header h1 { font-size: 1.8rem; margin-bottom: 0.5rem; }
        .header p { opacity: 0.9; }
        .container { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 1.5rem; }
        .card {
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: 0 2px 20px rgba(0,0,0,0.1);
            border: 1px solid #e0e0e0;
        }
        .card h2 {
            color: #2c3e50;
            margin-bottom: 1rem;
            font-size: 1.3rem;
            border-bottom: 2px solid #3498db;
            padding-bottom: 0.5rem;
        }
        .metric {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.75rem 0;
            border-bottom: 1px solid #f0f0f0;
        }
        .metric:last-child { border-bottom: none; }
        .metric-value {
            font-weight: 600;
            font-size: 1.1rem;
        }
        .status-pending { color: #f39c12; }
        .status-in-progress { color: #3498db; }
        .status-completed { color: #27ae60; }
        .status-failed { color: #e74c3c; }
        .priority-critical { background: #ffe6e6; border-left: 4px solid #e74c3c; }
        .priority-high { background: #fff3e0; border-left: 4px solid #f39c12; }
        .priority-medium { background: #e8f5e8; border-left: 4px solid #27ae60; }
        .gap-item {
            padding: 0.75rem;
            margin: 0.5rem 0;
            border-radius: 6px;
            border-left: 4px solid #3498db;
        }
        .refresh-btn {
            background: #3498db;
            color: white;
            border: none;
            padding: 0.5rem 1rem;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9rem;
            margin-left: 1rem;
        }
        .refresh-btn:hover { background: #2980b9; }
        .loading { text-align: center; color: #7f8c8d; padding: 2rem; }
        .chart-container { height: 300px; margin-top: 1rem; }
        .timestamp {
            font-size: 0.8rem;
            color: #7f8c8d;
            text-align: right;
            margin-top: 1rem;
        }
        .status-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 8px;
        }
        .status-good { background: #27ae60; }
        .status-warning { background: #f39c12; }
        .status-critical { background: #e74c3c; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 ATS Data Coverage Monitoring Dashboard</h1>
        <p>Real-time monitoring of daily prices and minute bar coverage across all vendors</p>
        <button class="refresh-btn" onclick="refreshDashboard()">🔄 Refresh All</button>
    </div>

    <div class="container">
        <div class="grid">
            <!-- Coverage Summary -->
            <div class="card">
                <h2>📊 Coverage Summary</h2>
                <div id="coverage-summary" class="loading">Loading coverage data...</div>
            </div>

            <!-- Priority Gaps -->
            <div class="card">
                <h2>🚨 Priority Gaps</h2>
                <div id="priority-gaps" class="loading">Loading priority gaps...</div>
            </div>

            <!-- Backfill Queue Status -->
            <div class="card">
                <h2>⚡ Backfill Queue</h2>
                <div id="backfill-queue" class="loading">Loading queue status...</div>
            </div>

            <!-- Recent Operations -->
            <div class="card">
                <h2>🔧 Recent Operations</h2>
                <div id="recent-operations" class="loading">Loading recent operations...</div>
            </div>

            <!-- Coverage Trend Chart -->
            <div class="card" style="grid-column: span 2;">
                <h2>📈 Coverage Trend (30 Days)</h2>
                <div class="chart-container">
                    <canvas id="coverage-chart"></canvas>
                </div>
            </div>
        </div>
    </div>

    <script>
        let coverageChart = null;

        async function fetchData(endpoint) {
            try {
                const response = await fetch(`/api/${endpoint}`);
                return await response.json();
            } catch (error) {
                console.error(`Error fetching ${endpoint}:`, error);
                return null;
            }
        }

        function renderCoverageSummary(data) {
            const container = document.getElementById('coverage-summary');
            if (!data || data.length === 0) {
                container.innerHTML = '<p>No coverage data available</p>';
                return;
            }

            let html = '';
            data.forEach(item => {
                const statusClass = item.coverage_percentage >= 90 ? 'status-good' :
                                  item.coverage_percentage >= 70 ? 'status-warning' : 'status-critical';

                html += `
                    <div class="metric">
                        <div>
                            <span class="status-indicator ${statusClass}"></span>
                            <strong>${item.vendor}</strong> ${item.data_type}
                        </div>
                        <div class="metric-value">${item.coverage_percentage?.toFixed(1) || 0}%</div>
                    </div>
                    <div style="font-size: 0.9rem; color: #7f8c8d; margin-left: 16px; margin-bottom: 10px;">
                        ${item.symbols_complete || 0}/${item.total_symbols || 0} symbols
                    </div>
                `;
            });

            container.innerHTML = html;
        }

        function renderPriorityGaps(data) {
            const container = document.getElementById('priority-gaps');
            if (!data || data.length === 0) {
                container.innerHTML = '<p>✅ No high-priority gaps found</p>';
                return;
            }

            let html = '';
            data.slice(0, 8).forEach(gap => {
                const priorityClass = gap.adjusted_priority >= 20 ? 'priority-critical' :
                                    gap.adjusted_priority >= 10 ? 'priority-high' : 'priority-medium';

                html += `
                    <div class="gap-item ${priorityClass}">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong>${gap.symbol}</strong> (${gap.vendor})
                                <div style="font-size: 0.8rem; color: #7f8c8d;">
                                    ${gap.gap_start_date} to ${gap.gap_end_date} (${gap.gap_days} days)
                                </div>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-weight: 600;">Priority: ${gap.adjusted_priority?.toFixed(1)}</div>
                                <div style="font-size: 0.8rem; color: #7f8c8d;">
                                    ~${gap.estimated_effort_minutes || 0}min
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            });

            container.innerHTML = html;
        }

        function renderBackfillQueue(data) {
            const container = document.getElementById('backfill-queue');
            if (!data) {
                container.innerHTML = '<p>No queue data available</p>';
                return;
            }

            const totalEffortHours = Math.round((data.total_effort_minutes || 0) / 60);

            container.innerHTML = `
                <div class="metric">
                    <span>Total Gaps</span>
                    <span class="metric-value">${data.total_gaps || 0}</span>
                </div>
                <div class="metric">
                    <span>Pending</span>
                    <span class="metric-value status-pending">${data.pending || 0}</span>
                </div>
                <div class="metric">
                    <span>In Progress</span>
                    <span class="metric-value status-in-progress">${data.in_progress || 0}</span>
                </div>
                <div class="metric">
                    <span>Completed</span>
                    <span class="metric-value status-completed">${data.completed || 0}</span>
                </div>
                <div class="metric">
                    <span>Failed</span>
                    <span class="metric-value status-failed">${data.failed || 0}</span>
                </div>
                <div class="metric">
                    <span>Estimated Effort</span>
                    <span class="metric-value">${totalEffortHours}h</span>
                </div>
            `;
        }

        function renderRecentOperations(data) {
            const container = document.getElementById('recent-operations');
            if (!data || data.length === 0) {
                container.innerHTML = '<p>No recent operations</p>';
                return;
            }

            let html = '';
            data.slice(0, 5).forEach(op => {
                const duration = op.duration_seconds ? `${op.duration_seconds}s` : 'N/A';
                const statusClass = `status-${op.status}`;

                html += `
                    <div class="metric">
                        <div>
                            <strong>${op.vendor}</strong> ${op.data_type}
                            <div style="font-size: 0.8rem; color: #7f8c8d;">
                                ${op.symbols_count || 0} symbols, ${duration}
                            </div>
                        </div>
                        <span class="metric-value ${statusClass}">${op.status}</span>
                    </div>
                `;
            });

            container.innerHTML = html;
        }

        function renderCoverageTrend(data) {
            const ctx = document.getElementById('coverage-chart').getContext('2d');

            if (coverageChart) {
                coverageChart.destroy();
            }

            if (!data || data.length === 0) {
                ctx.fillText('No trend data available', 10, 50);
                return;
            }

            // Group data by vendor/data_type
            const datasets = {};
            data.forEach(item => {
                const key = `${item.vendor} ${item.data_type}`;
                if (!datasets[key]) {
                    datasets[key] = {
                        label: key,
                        data: [],
                        borderColor: getVendorColor(item.vendor),
                        backgroundColor: getVendorColor(item.vendor) + '20',
                        tension: 0.1
                    };
                }
                datasets[key].data.push({
                    x: item.metric_date,
                    y: item.coverage_percentage
                });
            });

            coverageChart = new Chart(ctx, {
                type: 'line',
                data: {
                    datasets: Object.values(datasets)
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        x: {
                            type: 'time',
                            time: {
                                unit: 'day'
                            }
                        },
                        y: {
                            beginAtZero: true,
                            max: 100,
                            title: {
                                display: true,
                                text: 'Coverage %'
                            }
                        }
                    },
                    plugins: {
                        legend: {
                            position: 'top'
                        }
                    }
                }
            });
        }

        function getVendorColor(vendor) {
            const colors = {
                'firstrate': '#3498db',
                'polygon': '#e74c3c',
                'tiingo': '#27ae60',
                'eodhd': '#f39c12'
            };
            return colors[vendor] || '#7f8c8d';
        }

        async function refreshDashboard() {
            const [summary, gaps, queue, operations, trend] = await Promise.all([
                fetchData('coverage-summary'),
                fetchData('priority-gaps'),
                fetchData('backfill-queue'),
                fetchData('recent-operations'),
                fetchData('coverage-trend')
            ]);

            renderCoverageSummary(summary);
            renderPriorityGaps(gaps);
            renderBackfillQueue(queue);
            renderRecentOperations(operations);
            renderCoverageTrend(trend);

            // Update timestamp
            const timestamp = new Date().toLocaleString();
            document.querySelectorAll('.timestamp').forEach(el => {
                el.textContent = `Last updated: ${timestamp}`;
            });
        }

        // Initial load
        refreshDashboard();

        // Auto-refresh every 30 seconds
        setInterval(refreshDashboard, 30000);
    </script>

    <div class="timestamp" style="text-align: center; margin-top: 2rem;">
        Last updated: Loading...
    </div>
</body>
</html>'''

class CoverageDashboardServer:
    """Main server class for the coverage dashboard."""

    def __init__(self, db_config: Dict, port: int = 8080, host: str = "localhost"):
        self.db_config = db_config
        self.port = port
        self.host = host
        self.db_pool = None
        self.server = None

    async def initialize(self):
        """Initialize database connection pool."""
        self.db_pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=2,
            max_size=10
        )
        logger.info(f"✅ Connected to database: {self.db_config['host']}:{self.db_config['port']}")

    def run_server(self):
        """Run the HTTP server."""
        # Create a custom handler class with db_pool
        class CustomHandler(CoverageDashboardHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, db_pool=self.db_pool, **kwargs)

        CustomHandler.db_pool = self.db_pool

        self.server = HTTPServer((self.host, self.port), CustomHandler)
        logger.info(f"🚀 Coverage Dashboard Server running at http://{self.host}:{self.port}")
        logger.info("🎯 Available endpoints:")
        logger.info("   / - Main dashboard")
        logger.info("   /api/coverage-summary - Coverage summary data")
        logger.info("   /api/priority-gaps - Priority gaps for backfill")
        logger.info("   /api/coverage-trend - Coverage trending data")
        logger.info("   /api/backfill-queue - Queue status")
        logger.info("   /api/recent-operations - Recent backfill operations")
        logger.info("   /health - Health check")

        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            logger.info("👋 Dashboard server stopped by user")
        finally:
            if self.db_pool:
                asyncio.run(self.db_pool.close())

    async def close(self):
        """Close database connections and server."""
        if self.db_pool:
            await self.db_pool.close()
        if self.server:
            self.server.shutdown()

async def main():
    """Main entry point."""
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 4432)),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'intg_password'),
        'database': os.getenv('DB_NAME', 'intg_db')
    }

    # Server configuration
    port = int(os.getenv('DASHBOARD_PORT', 8080))
    host = os.getenv('DASHBOARD_HOST', 'localhost')

    print("🚀 ATS DATA COVERAGE MONITORING DASHBOARD")
    print("="*60)
    print(f"🔗 Starting at: http://{host}:{port}")
    print(f"📊 Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print()

    dashboard = CoverageDashboardServer(db_config, port, host)

    try:
        await dashboard.initialize()

        # Run server in thread to allow async cleanup
        server_thread = threading.Thread(target=dashboard.run_server, daemon=True)
        server_thread.start()

        # Keep main thread alive
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down dashboard...")
        await dashboard.close()

if __name__ == "__main__":
    asyncio.run(main())