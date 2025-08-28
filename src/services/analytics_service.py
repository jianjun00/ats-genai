#!/usr/bin/env python3
"""
ATS Analytics Service - External Script for Kubernetes
Provides web-based analytics dashboard for 30-year price database
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys
import os
from typing import Dict, List, Optional

# Add the src directory to the path for imports
sys.path.insert(0, '/workspace/src')
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobManager:
    """Job management functionality for analytics service using centralized connection manager."""
    
    def __init__(self):
        self.db_manager = get_connection_manager()
        self.settings = get_settings()
        
    async def initialize(self):
        """Initialize database connection using centralized manager."""
        try:
            # Test the centralized connection
            if await self.db_manager.check_async_connection():
                logger.info("✅ Database connection established via centralized manager")
            else:
                logger.warning("⚠️ Database connection check failed")
        except Exception as e:
            logger.warning(f"Database initialization failed: {e}")
    
    async def get_job_stats(self) -> Dict:
        """Get job statistics using centralized connection manager."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = self.settings.get_table_name("runs")
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total_jobs,
                            COUNT(CASE WHEN status = 'running' THEN 1 END) as running_jobs,
                            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs,
                            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_jobs,
                            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_jobs
                        FROM {table_name}
                    """)
                    
                    result = cursor.fetchone()
                    
                    return {
                        "total_jobs": result['total_jobs'],
                        "running_jobs": result['running_jobs'],
                        "completed_jobs": result['completed_jobs'],
                        "failed_jobs": result['failed_jobs'],
                        "pending_jobs": result['pending_jobs']
                    }
                
        except Exception as e:
            logger.error(f"Database error getting job stats: {e}")
            return {"error": str(e)}
    
    async def get_recent_jobs(self, limit: int = 10) -> List[Dict]:
        """Get recent jobs using centralized connection manager."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = self.settings.get_table_name("runs")
                    cursor.execute(f"""
                        SELECT id, run_type, status, start_time, end_time, created_by, 
                               created_at, error_message, parameters
                        FROM {table_name}
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (limit,))
                    
                    jobs = cursor.fetchall()
                    
                    result = []
                    for job in jobs:
                        duration = None
                        if job['start_time'] and job['end_time']:
                            duration = int((job['end_time'] - job['start_time']).total_seconds())
                        
                        result.append({
                            "job_id": str(job['id']),
                            "job_type": job['run_type'],
                            "status": job['status'],
                            "user_id": job['created_by'] or 'system',
                            "start_time": job['start_time'].isoformat() if job['start_time'] else None,
                            "end_time": job['end_time'].isoformat() if job['end_time'] else None,
                            "duration_seconds": duration,
                            "created_at": job['created_at'].isoformat() if job['created_at'] else None,
                            "error_message": job['error_message'],
                            "parameters": job['parameters'] or {}
                        })
                    
                    return result
                
        except Exception as e:
            logger.error(f"Database error getting recent jobs: {e}")
            return []
    
    async def get_collection_status(self) -> Dict:
        """Get REAL collection job status from actual running processes."""
        import subprocess
        import os
        
        def check_real_process_status(log_path: str, process_name: str) -> Dict:
            """Check if a real process is running based on log activity."""
            status = {
                "status": "inactive",
                "last_activity": None,
                "records": 0
            }
            
            try:
                if os.path.exists(log_path):
                    # Check log modification time
                    log_mtime = os.path.getmtime(log_path)
                    last_activity = datetime.fromtimestamp(log_mtime)
                    status['last_activity'] = last_activity.isoformat()
                    
                    # Consider active if modified within last 5 minutes
                    minutes_ago = (datetime.now() - last_activity).total_seconds() / 60
                    if minutes_ago < 5:
                        status['status'] = 'running'
                    elif minutes_ago < 60:
                        status['status'] = 'recent'
                    
                    # Try to extract record count from logs
                    try:
                        result = subprocess.run(['tail', '-50', log_path], 
                                              capture_output=True, text=True, timeout=3)
                        log_lines = result.stdout
                        
                        # Look for record counts
                        for line in reversed(log_lines.split('\n')):
                            if 'records' in line.lower():
                                # Extract numbers from line
                                import re
                                numbers = re.findall(r'(\d+(?:,\d+)*)', line)
                                if numbers:
                                    # Take the largest number found
                                    record_count = max(int(n.replace(',', '')) for n in numbers)
                                    status['records'] = record_count
                                    break
                    except:
                        pass
                        
            except Exception as e:
                logger.debug(f"Error checking {process_name}: {e}")
            
            return status
        
        # Check REAL collection processes
        real_jobs = {
            "price_backfills": {
                "polygon_30y": check_real_process_status("/tmp/polygon_30year_daily_backfill.log", "Polygon 30Y"),
                "tiingo_30y": check_real_process_status("/tmp/tiingo_30year_backfill.log", "Tiingo 30Y"),
                "eodhd_30y": check_real_process_status("/tmp/eodhd_30year_backfill.log", "EODHD 30Y"),
            },
            "events_collection": {
                "polygon": check_real_process_status("/tmp/polygon_earnings_fixed.log", "Polygon Events"),
                "eodhd": check_real_process_status("/tmp/eodhd_events.log", "EODHD Events"),
                "tiingo": check_real_process_status("/tmp/tiingo_events.log", "Tiingo Events"),
            },
            "minute_data": {
                "polygon": check_real_process_status("/tmp/polygon_minute_backfill.log", "Polygon Minutes"),
            },
            "last_updated": datetime.now().isoformat()
        }
        
        return real_jobs

# Initialize global job manager
job_manager = JobManager()

class AnalyticsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            response = {"status": "healthy", "service": "ats-analytics", "timestamp": datetime.now().isoformat()}
            self.wfile.write(json.dumps(response).encode())
            
        elif self.path == '/' or self.path == '/dashboard':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>ATS Analytics Dashboard</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                    .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
                    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .metric { font-size: 2em; font-weight: bold; color: #2c3e50; }
                    .btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 4px; text-decoration: none; display: inline-block; margin: 5px; }
                    .job-item { padding: 8px; margin: 4px 0; border-radius: 4px; font-size: 0.9em; border-left: 3px solid #ddd; }
                    .job-running { border-left-color: #3498db; background: #e8f4fd; }
                    .job-completed { border-left-color: #27ae60; background: #eafaf1; }
                    .job-failed { border-left-color: #e74c3c; background: #fdf2f2; }
                    .job-pending { border-left-color: #f39c12; background: #fef9e7; }
                    .job-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(80px, 1fr)); gap: 10px; margin-bottom: 10px; }
                    .stat-item { text-align: center; }
                    .stat-value { font-size: 1.2em; font-weight: bold; }
                    .stat-label { font-size: 0.8em; color: #666; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>ATS Analytics Platform</h1>
                    <p>30-Year Price History Database - Development Environment</p>
                </div>
                
                <div class="grid">
                    <div class="card">
                        <h3>Database Summary</h3>
                        <div class="metric">7.95M+</div>
                        <p>Total price records across all vendors</p>
                        <ul>
                            <li><strong>Instruments:</strong> 17,700 unique symbols</li>
                            <li><strong>ETFs:</strong> 23 critical market factors</li>
                            <li><strong>Date Range:</strong> 1995-2025 (30 years)</li>
                        </ul>
                    </div>
                    
                    <div class="card">
                        <h3>Vendor Coverage</h3>
                        <ul>
                            <li><strong>Tiingo:</strong> 6.56M records, 2,355 symbols</li>
                            <li><strong>EODHD:</strong> 728K records, 268 symbols</li>
                            <li><strong>Polygon:</strong> 666K records, 849 symbols</li>
                        </ul>
                        <a href="/api/vendors" class="btn">Vendor Details</a>
                    </div>
                    
                    <div class="card">
                        <h3>Job Management</h3>
                        <div id="job-stats">Loading job statistics...</div>
                        <div id="recent-jobs" style="margin-top: 15px; max-height: 150px; overflow-y: auto;">
                            <p>Loading recent jobs...</p>
                        </div>
                        <a href="/api/jobs/stats" class="btn">Job Stats</a>
                        <a href="/api/jobs/recent" class="btn">Recent Jobs</a>
                    </div>
                    
                    <div class="card">
                        <h3>API Endpoints</h3>
                        <a href="/health" class="btn">Health Check</a>
                        <a href="/api/summary" class="btn">Data Summary</a>
                        <a href="/api/jobs/stats" class="btn">Job Stats</a>
                        <a href="/api/collections/status" class="btn">Collection Status</a>
                    </div>
                </div>
                
                <div style="margin-top: 20px; text-align: center; color: #7f8c8d;">
                    <p>ATS Analytics Service | Development Environment | External Access Available</p>
                </div>
                
                <script>
                    async function loadJobStats() {
                        try {
                            const response = await fetch('/api/jobs/stats');
                            const stats = await response.json();
                            
                            if (stats.error) {
                                document.getElementById('job-stats').innerHTML = '<p style="color: #e74c3c;">Database unavailable</p>';
                                return;
                            }
                            
                            document.getElementById('job-stats').innerHTML = `
                                <div class="job-stats">
                                    <div class="stat-item">
                                        <div class="stat-value">${stats.total_jobs || 0}</div>
                                        <div class="stat-label">Total</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-value" style="color: #3498db;">${stats.running_jobs || 0}</div>
                                        <div class="stat-label">Running</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-value" style="color: #27ae60;">${stats.completed_jobs || 0}</div>
                                        <div class="stat-label">Completed</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-value" style="color: #e74c3c;">${stats.failed_jobs || 0}</div>
                                        <div class="stat-label">Failed</div>
                                    </div>
                                </div>
                            `;
                        } catch (error) {
                            console.error('Failed to load job stats:', error);
                            document.getElementById('job-stats').innerHTML = '<p style="color: #e74c3c;">Failed to load stats</p>';
                        }
                    }
                    
                    async function loadRecentJobs() {
                        try {
                            const response = await fetch('/api/jobs/recent');
                            const data = await response.json();
                            
                            if (!data.jobs || data.jobs.length === 0) {
                                document.getElementById('recent-jobs').innerHTML = '<p style="color: #666;">No recent jobs found</p>';
                                return;
                            }
                            
                            const jobsHtml = data.jobs.slice(0, 5).map(job => {
                                const statusClass = `job-${job.status}`;
                                const duration = job.duration_seconds ? `${Math.round(job.duration_seconds/60)}m` : 'N/A';
                                const timeAgo = job.created_at ? new Date(job.created_at).toLocaleString() : 'Unknown';
                                
                                return `
                                    <div class="job-item ${statusClass}">
                                        <div style="font-weight: bold;">${job.job_type}</div>
                                        <div style="font-size: 0.8em; color: #666;">
                                            ${job.status.toUpperCase()} | ${job.user_id} | ${duration}
                                        </div>
                                        ${job.error_message ? `<div style="color: #e74c3c; font-size: 0.8em;">${job.error_message}</div>` : ''}
                                    </div>
                                `;
                            }).join('');
                            
                            document.getElementById('recent-jobs').innerHTML = jobsHtml;
                        } catch (error) {
                            console.error('Failed to load recent jobs:', error);
                            document.getElementById('recent-jobs').innerHTML = '<p style="color: #e74c3c;">Failed to load jobs</p>';
                        }
                    }
                    
                    // Load data on page load
                    document.addEventListener('DOMContentLoaded', function() {
                        loadJobStats();
                        loadRecentJobs();
                        
                        // Refresh every 30 seconds
                        setInterval(() => {
                            loadJobStats();
                            loadRecentJobs();
                        }, 30000);
                    });
                </script>
            </body>
            </html>
            """
            self.wfile.write(html.encode())
            
        elif self.path == '/api/summary':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            summary = {
                "environment": "development",
                "database": "dev_db",
                "total_records": "7,953,657",
                "vendors": {
                    "tiingo": {"records": "6,559,540", "symbols": 2355},
                    "eodhd": {"records": "727,905", "symbols": 268}, 
                    "polygon": {"records": "666,212", "symbols": 849}
                },
                "instruments": "17,700",
                "etfs": "23",
                "date_range": "1995-2025",
                "status": "operational",
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(summary, indent=2).encode())
            
        elif self.path == '/api/vendors':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            vendors = {
                "vendors": [
                    {
                        "name": "tiingo",
                        "records": 6559540,
                        "symbols": 2355,
                        "coverage": "1995-2025",
                        "status": "active"
                    },
                    {
                        "name": "eodhd", 
                        "records": 727905,
                        "symbols": 268,
                        "coverage": "1995-2025",
                        "status": "active"
                    },
                    {
                        "name": "polygon",
                        "records": 666212,
                        "symbols": 849,
                        "coverage": "2015-2025",
                        "status": "active"
                    }
                ],
                "total_records": 7953657,
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(vendors, indent=2).encode())
        
        elif self.path == '/api/jobs/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Need to run async function in sync context
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                stats = loop.run_until_complete(job_manager.get_job_stats())
                self.wfile.write(json.dumps(stats, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting job stats: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path == '/api/jobs/recent':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                jobs = loop.run_until_complete(job_manager.get_recent_jobs(15))
                response = {"jobs": jobs, "total": len(jobs), "timestamp": datetime.now().isoformat()}
                self.wfile.write(json.dumps(response, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting recent jobs: {e}")
                error_response = {"jobs": [], "total": 0, "error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path == '/api/collections/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                status = loop.run_until_complete(job_manager.get_collection_status())
                self.wfile.write(json.dumps(status, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting collection status: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
            
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error = {"error": "Not found", "path": self.path}
            self.wfile.write(json.dumps(error).encode())

async def initialize_job_manager():
    """Initialize the job manager."""
    try:
        await job_manager.initialize()
        logger.info("✅ Job manager initialized")
    except Exception as e:
        logger.warning(f"⚠️  Job manager initialization failed: {e}")

def main():
    """Main entry point for analytics service"""
    port = int(os.getenv('PORT', 3000))
    
    try:
        # Initialize job manager
        logger.info("🔧 Initializing job manager...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(initialize_job_manager())
        loop.close()
        
        server = HTTPServer(('0.0.0.0', port), AnalyticsHandler)
        logger.info(f"🚀 ATS Analytics Service starting on port {port}")
        logger.info("📊 Serving 30-year price database analytics with job management")
        logger.info(f"🌐 External access available")
        logger.info("🔧 Job management endpoints: /api/jobs/stats, /api/jobs/recent")
        
        server.serve_forever()
        
    except KeyboardInterrupt:
        logger.info("📊 Analytics service stopped")
        server.server_close()
    except Exception as e:
        logger.error(f"❌ Analytics service error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()