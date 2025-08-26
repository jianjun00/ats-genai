#!/usr/bin/env python3
"""
ATS Analytics Service - External Script for Kubernetes
Provides web-based analytics dashboard for 30-year price database
"""

import asyncio
import json
import logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
                <title>ATS Analytics Dashboard</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                    .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
                    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .metric { font-size: 2em; font-weight: bold; color: #2c3e50; }
                    .btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 4px; text-decoration: none; display: inline-block; margin: 5px; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>📊 ATS Analytics Platform</h1>
                    <p>30-Year Price History Database - Development Environment</p>
                </div>
                
                <div class="grid">
                    <div class="card">
                        <h3>📈 Database Summary</h3>
                        <div class="metric">7.95M+</div>
                        <p>Total price records across all vendors</p>
                        <ul>
                            <li><strong>Instruments:</strong> 17,700 unique symbols</li>
                            <li><strong>ETFs:</strong> 23 critical market factors</li>
                            <li><strong>Date Range:</strong> 1995-2025 (30 years)</li>
                        </ul>
                    </div>
                    
                    <div class="card">
                        <h3>🏢 Vendor Coverage</h3>
                        <ul>
                            <li><strong>Tiingo:</strong> 6.56M records, 2,355 symbols</li>
                            <li><strong>EODHD:</strong> 728K records, 268 symbols</li>
                            <li><strong>Polygon:</strong> 666K records, 849 symbols</li>
                        </ul>
                        <a href="/api/vendors" class="btn">Vendor Details</a>
                    </div>
                    
                    <div class="card">
                        <h3>🎯 Quality Metrics</h3>
                        <p>Multi-vendor reconciliation system with:</p>
                        <ul>
                            <li>Quality scoring (0-100 scale)</li>
                            <li>Data integrity validation</li>
                            <li>Gap detection and analysis</li>
                            <li>Performance monitoring</li>
                        </ul>
                        <a href="/api/quality" class="btn">Quality Report</a>
                    </div>
                    
                    <div class="card">
                        <h3>🔗 API Endpoints</h3>
                        <a href="/health" class="btn">Health Check</a>
                        <a href="/api/summary" class="btn">Data Summary</a>
                        <a href="/api/instruments" class="btn">Instruments</a>
                        <a href="/api/coverage" class="btn">Coverage Analysis</a>
                    </div>
                </div>
                
                <div style="margin-top: 20px; text-align: center; color: #7f8c8d;">
                    <p>🚀 ATS Analytics Service | Development Environment | External Access Available</p>
                </div>
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
            
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error = {"error": "Not found", "path": self.path}
            self.wfile.write(json.dumps(error).encode())

def main():
    """Main entry point for analytics service"""
    port = int(os.getenv('PORT', 8000))
    
    try:
        server = HTTPServer(('0.0.0.0', port), AnalyticsHandler)
        logger.info(f"🚀 ATS Analytics Service starting on port {port}")
        logger.info("📊 Serving 30-year price database analytics")
        logger.info(f"🌐 External access available")
        
        server.serve_forever()
        
    except KeyboardInterrupt:
        logger.info("📊 Analytics service stopped")
        server.server_close()
    except Exception as e:
        logger.error(f"❌ Analytics service error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()