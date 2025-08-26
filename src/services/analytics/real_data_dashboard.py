#!/usr/bin/env python3
"""
ATS Analytics Dashboard with REAL DATA ONLY
NO MOCK DATA - Connects to real database or fails clearly
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn
import os
import asyncio
import asyncpg
from datetime import datetime
from typing import Dict, List, Any

app = FastAPI(title='ATS Real Data Analytics Dashboard')

# Database connection - NO FALLBACKS
db_pool = None

async def get_db_connection():
    """Get database connection - FAIL if not available"""
    global db_pool
    if not db_pool:
        try:
            db_pool = await asyncpg.create_pool(
                "postgresql://postgres:dev_password@postgres:5432/dev_db",
                min_size=1, max_size=3,
                command_timeout=10
            )
            # Test connection immediately
            async with db_pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
        except Exception as e:
            raise HTTPException(
                status_code=503, 
                detail=f"Database connection failed: {str(e)}. No mock data available."
            )
    return db_pool

@app.get('/api/jobs')
async def get_jobs():
    """Get real job data from vendor_job_progress table - NO MOCK DATA"""
    try:
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            jobs = await conn.fetch("""
                SELECT job_id, vendor, symbol, status, created_at, completed_at, 
                       rows_processed, error_message 
                FROM vendor_job_progress 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            
            if not jobs:
                # Return empty but valid response - no mock data
                return {"jobs": [], "message": "No jobs found in database"}
            
            return {"jobs": [dict(job) for job in jobs]}
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch jobs from database: {str(e)}"
        )

@app.get('/api/datasets')
async def get_datasets():
    """Get real dataset data from dev_training_dataset table - NO MOCK DATA"""
    try:
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            datasets = await conn.fetch("""
                SELECT dataset_id, dataset_name, description, symbols, 
                       creation_timestamp, dataset_size_mb
                FROM dev_training_dataset 
                ORDER BY creation_timestamp DESC 
                LIMIT 10
            """)
            
            if not datasets:
                return {"datasets": [], "message": "No datasets found in database"}
            
            return {"datasets": [dict(dataset) for dataset in datasets]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch datasets from database: {str(e)}"
        )

@app.get('/api/coverage')
async def get_coverage_stats():
    """Get real data coverage statistics - NO MOCK DATA"""
    try:
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            # Get actual record counts from price tables
            polygon_count = await conn.fetchval("SELECT COUNT(*) FROM dev_polygon_prices")
            tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM dev_tiingo_prices") 
            eodhd_count = await conn.fetchval("SELECT COUNT(*) FROM dev_eodhd_prices")
            unique_symbols = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM dev_polygon_prices")
            
            return {
                "total_records": polygon_count + tiingo_count + eodhd_count,
                "vendors": {
                    "polygon": polygon_count,
                    "tiingo": tiingo_count, 
                    "eodhd": eodhd_count
                },
                "unique_symbols": unique_symbols,
                "last_updated": datetime.now().isoformat()
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch coverage stats from database: {str(e)}"
        )

@app.get('/health')
async def health():
    """Health check - tests database connection"""
    try:
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            await conn.fetchval('SELECT 1')
        
        return {
            'status': 'healthy',
            'service': 'real-data-analytics-dashboard',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service unhealthy - database connection failed: {str(e)}"
        )

@app.get('/', response_class=HTMLResponse)
async def dashboard():
    """Dashboard with real data only - shows errors clearly if data unavailable"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>ATS Analytics Dashboard - Real Data Only</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { text-align: center; color: white; margin-bottom: 40px; padding: 30px 0; }
        .header h1 { font-size: 3rem; margin-bottom: 10px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: white; border-radius: 15px; padding: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); }
        .card h3 { color: #4a5568; margin-bottom: 15px; font-size: 1.4rem; }
        .metric { font-size: 2.5rem; font-weight: bold; color: #2d3748; margin: 10px 0; }
        .status { padding: 8px 16px; border-radius: 20px; font-weight: bold; }
        .status.running { background: #bee3f8; color: #1e3a8a; }
        .status.healthy { background: #c6f6d5; color: #22543d; }
        .status.error { background: #fed7d7; color: #c53030; }
        .error-message { color: #c53030; font-size: 0.9em; margin-top: 10px; padding: 10px; background: #fed7d7; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 ATS Analytics Platform</h1>
            <p>Real-time Financial Data & Portfolio Analytics - <strong>REAL DATA ONLY</strong></p>
        </div>
        <div class="grid">
            <div class="card">
                <h3>📊 System Status</h3>
                <div class="metric"><span class="status" id="system-status">Loading...</span></div>
                <p>Analytics service operational status</p>
                <div id="system-error" class="error-message" style="display: none;"></div>
            </div>
            <div class="card">
                <h3>💹 Market Data</h3>
                <div class="metric" id="total-records">Loading...</div>
                <p>Daily price records from database</p>
                <div style="margin-top: 10px; font-size: 0.9em;">
                    <div>Polygon: <span id="polygon-count">-</span></div>
                    <div>Tiingo: <span id="tiingo-count">-</span></div>
                    <div>EODHD: <span id="eodhd-count">-</span></div>
                </div>
                <div id="coverage-error" class="error-message" style="display: none;"></div>
            </div>
            <div class="card">
                <h3>🎯 Portfolio Analytics</h3>
                <div class="metric"><span class="status healthy">Database Required</span></div>
                <p>Performance attribution & risk metrics</p>
                <div>Symbols: <span id="unique-symbols">Loading...</span></div>
            </div>
            <div class="card">
                <h3>🔧 Job Management</h3>
                <div class="metric" id="active-jobs">Loading...</div>
                <p>Recent jobs from vendor_job_progress</p>
                <div id="recent-jobs" style="margin-top: 10px; max-height: 150px; overflow-y: auto; font-size: 0.8em;"></div>
                <div id="jobs-error" class="error-message" style="display: none;"></div>
            </div>
            <div class="card">
                <h3>📚 Datasets</h3>
                <div class="metric" id="dataset-count">Loading...</div>
                <p>Training datasets from database</p>
                <div id="recent-datasets" style="margin-top: 10px; max-height: 150px; overflow-y: auto; font-size: 0.8em;"></div>
                <div id="datasets-error" class="error-message" style="display: none;"></div>
            </div>
            <div class="card">
                <h3>⚡ Live Updates</h3>
                <div class="metric" id="time">Loading...</div>
                <p>Current time & refresh</p>
                <div style="margin-top: 10px;">
                    <button onclick="refreshData()" style="padding: 5px 10px; border: none; background: #4299e1; color: white; border-radius: 3px; cursor: pointer;">Refresh Data</button>
                </div>
            </div>
        </div>
    </div>
    <script>
        async function loadData() {
            // Load coverage stats (real database data)
            try {
                const coverage = await fetch('/api/coverage');
                if (coverage.ok) {
                    const data = await coverage.json();
                    document.getElementById('total-records').textContent = (data.total_records / 1000000).toFixed(2) + 'M';
                    document.getElementById('polygon-count').textContent = (data.vendors.polygon / 1000).toFixed(0) + 'K';
                    document.getElementById('tiingo-count').textContent = (data.vendors.tiingo / 1000).toFixed(0) + 'K';
                    document.getElementById('eodhd-count').textContent = (data.vendors.eodhd / 1000).toFixed(0) + 'K';
                    document.getElementById('unique-symbols').textContent = data.unique_symbols;
                    document.getElementById('coverage-error').style.display = 'none';
                } else {
                    throw new Error(`HTTP ${coverage.status}: ${await coverage.text()}`);
                }
            } catch (e) {
                console.error('Coverage load failed:', e);
                document.getElementById('total-records').textContent = 'ERROR';
                document.getElementById('coverage-error').textContent = 'Database connection failed: ' + e.message;
                document.getElementById('coverage-error').style.display = 'block';
            }
            
            // Load job data (real database data)
            try {
                const jobs = await fetch('/api/jobs');
                if (jobs.ok) {
                    const data = await jobs.json();
                    document.getElementById('active-jobs').textContent = data.jobs.length;
                    if (data.jobs.length === 0) {
                        document.getElementById('recent-jobs').innerHTML = '<em>No jobs found in database</em>';
                    } else {
                        document.getElementById('recent-jobs').innerHTML = data.jobs.slice(0, 5).map(job => 
                            `<div style="margin: 3px 0; padding: 3px; border-left: 3px solid #ddd;">
                                <strong>${job.vendor}/${job.symbol}</strong> 
                                <span class="status ${job.status === 'completed' ? 'healthy' : job.status === 'running' ? 'running' : 'error'}" style="font-size: 0.7em; padding: 1px 4px;">${job.status}</span><br>
                                <small>${new Date(job.created_at).toLocaleDateString()}</small>
                            </div>`
                        ).join('');
                    }
                    document.getElementById('jobs-error').style.display = 'none';
                } else {
                    throw new Error(`HTTP ${jobs.status}: ${await jobs.text()}`);
                }
            } catch (e) {
                console.error('Jobs load failed:', e);
                document.getElementById('active-jobs').textContent = 'ERROR';
                document.getElementById('jobs-error').textContent = 'Database connection failed: ' + e.message;
                document.getElementById('jobs-error').style.display = 'block';
            }
            
            // Load dataset data (real database data)
            try {
                const datasets = await fetch('/api/datasets');
                if (datasets.ok) {
                    const data = await datasets.json();
                    document.getElementById('dataset-count').textContent = data.datasets.length;
                    if (data.datasets.length === 0) {
                        document.getElementById('recent-datasets').innerHTML = '<em>No datasets found in database</em>';
                    } else {
                        document.getElementById('recent-datasets').innerHTML = data.datasets.slice(0, 5).map(ds => 
                            `<div style="margin: 3px 0; padding: 3px; border-left: 3px solid #ddd;">
                                <strong>${ds.dataset_name}</strong><br>
                                <small>${ds.symbols} • ${(ds.dataset_size_mb || 0).toFixed(1)}MB</small><br>
                                <small>${new Date(ds.creation_timestamp).toLocaleDateString()}</small>
                            </div>`
                        ).join('');
                    }
                    document.getElementById('datasets-error').style.display = 'none';
                } else {
                    throw new Error(`HTTP ${datasets.status}: ${await datasets.text()}`);
                }
            } catch (e) {
                console.error('Datasets load failed:', e);
                document.getElementById('dataset-count').textContent = 'ERROR';
                document.getElementById('datasets-error').textContent = 'Database connection failed: ' + e.message;
                document.getElementById('datasets-error').style.display = 'block';
            }
            
            // Check system health
            try {
                const health = await fetch('/health');
                if (health.ok) {
                    document.getElementById('system-status').textContent = 'Healthy';
                    document.getElementById('system-status').className = 'status healthy';
                    document.getElementById('system-error').style.display = 'none';
                } else {
                    throw new Error(`HTTP ${health.status}: ${await health.text()}`);
                }
            } catch (e) {
                console.error('Health check failed:', e);
                document.getElementById('system-status').textContent = 'Unhealthy';
                document.getElementById('system-status').className = 'status error';
                document.getElementById('system-error').textContent = 'System health check failed: ' + e.message;
                document.getElementById('system-error').style.display = 'block';
            }
        }
        
        function refreshData() {
            loadData();
            document.getElementById('time').textContent = new Date().toLocaleTimeString();
        }
        
        // Update time every second
        setInterval(() => {
            document.getElementById('time').textContent = new Date().toLocaleTimeString();
        }, 1000);
        
        // Load data initially and every 30 seconds
        loadData();
        setInterval(loadData, 30000);
    </script>
</body>
</html>
    """

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    print(f'🚀 Starting Real Data Analytics Dashboard on port {port}')
    print('📊 NO MOCK DATA - Database connection required')
    uvicorn.run(app, host='0.0.0.0', port=port)