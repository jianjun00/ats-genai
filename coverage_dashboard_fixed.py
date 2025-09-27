#!/usr/bin/env python3
"""
Data Coverage Monitoring Dashboard - Fixed Version
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
import concurrent.futures

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoverageDashboardHandler(BaseHTTPRequestHandler):
    """HTTP handler for the coverage dashboard."""
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query_params = parse_qs(parsed_path.query)
        
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
    def serve_dashboard(self):
        """Serve the main dashboard HTML."""
        html_content = self.generate_dashboard_html()
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))
    
    def execute_db_query(self, query):
        """Execute database query synchronously."""
        # Get database config from server
        db_config = getattr(self.server, 'db_config', {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db'
        })
        
        async def run_query():
            conn = await asyncpg.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database']
            )
            result = await conn.fetch(query)
            return result
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(run_query())
        return result
        logger.error(f"Database query error: {e}")
        return None

    def serve_coverage_summary(self):
        """Serve coverage summary data."""
        query = """
            SELECT vendor, data_type, total_symbols, symbols_complete, 
                   symbols_missing, coverage_percentage, last_scan_time
            FROM v_current_coverage_summary
            ORDER BY vendor, data_type
        """
        
        data = self.execute_db_query(query)
        response = []
        
        if data:
            for row in data:
                item = dict(row)
                # Convert datetime to string
                if item.get('last_scan_time'):
                    item['last_scan_time'] = item['last_scan_time'].isoformat()
                response.append(item)
        
        self.send_json_response(response)
    
    def serve_priority_gaps(self):
        """Serve priority gaps requiring backfill."""
        query = """
            SELECT vendor, data_type, symbol, gap_start_date, gap_end_date,
                   gap_days, priority_score, adjusted_priority, backfill_status,
                   estimated_effort_minutes, created_at
            FROM v_active_backfill_queue
            LIMIT 50
        """
        
        data = self.execute_db_query(query)
        response = []
        
        if data:
            for row in data:
                item = dict(row)
                # Convert dates to strings
                if item.get('gap_start_date'):
                    item['gap_start_date'] = item['gap_start_date'].isoformat()
                if item.get('gap_end_date'):
                    item['gap_end_date'] = item['gap_end_date'].isoformat()
                if item.get('created_at'):
                    item['created_at'] = item['created_at'].isoformat()
                response.append(item)
        
        self.send_json_response(response)
    
    def serve_coverage_trend(self):
        """Serve coverage trending data."""
        query = """
            SELECT metric_date, vendor, data_type, coverage_percentage,
                   coverage_change
            FROM v_coverage_trending
            WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY vendor, data_type, metric_date
        """
        
        data = self.execute_db_query(query)
        response = []
        
        if data:
            for row in data:
                item = dict(row)
                if item.get('metric_date'):
                    item['metric_date'] = item['metric_date'].isoformat()
                response.append(item)
        
        self.send_json_response(response)
    
    def serve_backfill_queue(self):
        """Serve backfill queue status."""
        query = """
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
        """
        
        data = self.execute_db_query(query)
        response = {}
        
        if data and len(data) > 0:
            response = dict(data[0])
        
        self.send_json_response(response)
    
    def serve_recent_operations(self):
        """Serve recent backfill operations."""
        query = """
            SELECT operation_type, vendor, data_type,
                   array_length(symbols_requested, 1) as symbols_count,
                   status, duration_seconds, created_at, completed_at,
                   total_files_updated, error_log
            FROM dev_backfill_operations
            WHERE created_at >= NOW() - INTERVAL '7 days'
            ORDER BY created_at DESC
            LIMIT 20
        """
        
        data = self.execute_db_query(query)
        response = []
        
        if data:
            for row in data:
                item = dict(row)
                if item.get('created_at'):
                    item['created_at'] = item['created_at'].isoformat()
                if item.get('completed_at'):
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
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
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
        .wide-card { grid-column: span 2; }
        @media (max-width: 1200px) {
            .wide-card { grid-column: span 1; }
        }
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
            <div class="card wide-card">
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
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
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
                const coverage = item.coverage_percentage || 0;
                const statusClass = coverage >= 90 ? 'status-good' : 
                                  coverage >= 70 ? 'status-warning' : 'status-critical';
                
                html += `
                    <div class="metric">
                        <div>
                            <span class="status-indicator ${statusClass}"></span>
                            <strong>${item.vendor || 'Unknown'}</strong> ${item.data_type || 'Unknown'}
                        </div>
                        <div class="metric-value">${coverage.toFixed(1)}%</div>
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
                const priority = gap.adjusted_priority || gap.priority_score || 0;
                const priorityClass = priority >= 20 ? 'priority-critical' :
                                    priority >= 10 ? 'priority-high' : 'priority-medium';
                
                html += `
                    <div class="gap-item ${priorityClass}">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <strong>${gap.symbol || 'Unknown'}</strong> (${gap.vendor || 'Unknown'})
                                <div style="font-size: 0.8rem; color: #7f8c8d;">
                                    ${gap.gap_start_date || 'Unknown'} to ${gap.gap_end_date || 'Unknown'} (${gap.gap_days || 0} days)
                                </div>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-weight: 600;">Priority: ${priority.toFixed(1)}</div>
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
                const statusClass = `status-${op.status || 'unknown'}`;
                
                html += `
                    <div class="metric">
                        <div>
                            <strong>${op.vendor || 'Unknown'}</strong> ${op.data_type || 'Unknown'}
                            <div style="font-size: 0.8rem; color: #7f8c8d;">
                                ${op.symbols_count || 0} symbols, ${duration}
                            </div>
                        </div>
                        <span class="metric-value ${statusClass}">${op.status || 'Unknown'}</span>
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
                // Draw "No data" message
                ctx.fillStyle = '#7f8c8d';
                ctx.font = '16px sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText('No trend data available', ctx.canvas.width / 2, ctx.canvas.height / 2);
                return;
            }

            // Group data by vendor/data_type
            const datasets = {};
            data.forEach(item => {
                const key = `${item.vendor || 'Unknown'} ${item.data_type || 'Unknown'}`;
                if (!datasets[key]) {
                    datasets[key] = {
                        label: key,
                        data: [],
                        borderColor: getVendorColor(item.vendor),
                        backgroundColor: getVendorColor(item.vendor) + '20',
                        tension: 0.1,
                        fill: false
                    };
                }
                datasets[key].data.push({
                    x: item.metric_date,
                    y: item.coverage_percentage || 0
                });
            });

            // Sort data points by date
            Object.values(datasets).forEach(dataset => {
                dataset.data.sort((a, b) => new Date(a.x) - new Date(b.x));
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
                                unit: 'day',
                                displayFormats: {
                                    day: 'MMM dd'
                                }
                            },
                            title: {
                                display: true,
                                text: 'Date'
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
                        },
                        tooltip: {
                            mode: 'index',
                            intersect: false
                        }
                    },
                    interaction: {
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
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
            console.log('Refreshing dashboard data...');
            
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
            const timestampElement = document.querySelector('.timestamp');
            if (timestampElement) {
                timestampElement.textContent = `Last updated: ${timestamp}`;
            }
            
            console.log('Dashboard refresh complete');
        }

        // Initial load
        document.addEventListener('DOMContentLoaded', function() {
            refreshDashboard();
        });

        // Auto-refresh every 30 seconds
        setInterval(refreshDashboard, 30000);
    </script>

    <div class="timestamp" style="text-align: center; margin-top: 2rem; font-size: 0.9rem; color: #7f8c8d;">
        Last updated: Loading...
    </div>
</body>
</html>'''

def main():
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
    print(f"🔗 Dashboard URL: http://{host}:{port}")
    print(f"📊 Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print("🎯 Available endpoints:")
    print("   / - Main dashboard")
    print("   /api/coverage-summary - Coverage summary data")
    print("   /api/priority-gaps - Priority gaps for backfill")
    print("   /api/coverage-trend - Coverage trending data")
    print("   /api/backfill-queue - Queue status")
    print("   /api/recent-operations - Recent backfill operations")
    print("   /health - Health check")
    print()
    
    # Create server with db config
    server = HTTPServer((host, port), CoverageDashboardHandler)
    server.db_config = db_config
    
    logger.info(f"🚀 Coverage Dashboard Server running at http://{host}:{port}")
    
    server.serve_forever()
if __name__ == "__main__":
    main()