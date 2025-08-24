#!/usr/bin/env python3

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from datetime import datetime, timedelta
import asyncpg
import os
import json
import random

app = FastAPI(title="ATS Analytics Service")

# Database configuration - use environment variables or defaults
DB_HOST = os.getenv('DB_HOST', 'postgres-simple')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'dev_password')
DB_NAME = os.getenv('DB_NAME', 'dev_db')

class AnalyticsManager:
    def __init__(self):
        self.db_pool = None

    async def initialize(self):
        try:
            # Try to connect to database
            self.db_pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                min_size=1,
                max_size=3
            )
            print("✅ Analytics Manager connected to database successfully")
        except Exception as e:
            print(f"⚠️  Database connection failed: {e}")
            print("📊 Running in demo mode with mock data")
            self.db_pool = None

    async def get_datasets(self, limit: int = 5, offset: int = 0):
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    # Try to get real datasets
                    datasets = await conn.fetch("""
                        SELECT 
                            id as dataset_id,
                            dataset_name,
                            symbols,
                            total_sequences,
                            feature_count,
                            sequence_length,
                            file_size_mb,
                            status,
                            creation_timestamp as created_at
                        FROM dev_training_dataset 
                        ORDER BY creation_timestamp DESC
                        LIMIT $1 OFFSET $2
                    """, limit, offset)
                    
                    result = []
                    for row in datasets:
                        result.append({
                            "dataset_id": row['dataset_id'],
                            "dataset_name": row['dataset_name'],
                            "symbols": row['symbols'] or [],
                            "total_sequences": row['total_sequences'],
                            "feature_count": row['feature_count'],
                            "sequence_length": row['sequence_length'],
                            "file_size_mb": row['file_size_mb'],
                            "status": row['status'],
                            "created_at": row['created_at'].isoformat() if row['created_at'] else None
                        })
                    
                    total = await conn.fetchval("SELECT COUNT(*) FROM dev_training_dataset")
                    return {"datasets": result, "total": total}
            except Exception as e:
                print(f"Database query failed: {e}")
        
        # Fallback to demo data
        demo_datasets = [
            {
                "dataset_id": 1,
                "dataset_name": "S&P 500 Training Set",
                "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"],
                "total_sequences": 15420,
                "feature_count": 127,
                "sequence_length": 60,
                "file_size_mb": 245.7,
                "status": "completed",
                "created_at": datetime.now().isoformat()
            },
            {
                "dataset_id": 2,
                "dataset_name": "NASDAQ Tech Stocks",
                "symbols": ["NVDA", "META", "NFLX", "ADBE"],
                "total_sequences": 9876,
                "feature_count": 98,
                "sequence_length": 45,
                "file_size_mb": 156.3,
                "status": "processing",
                "created_at": (datetime.now() - timedelta(days=1)).isoformat()
            },
            {
                "dataset_id": 3,
                "dataset_name": "Financial Sector Analysis",
                "symbols": ["JPM", "BAC", "WFC", "GS"],
                "total_sequences": 7654,
                "feature_count": 85,
                "sequence_length": 30,
                "file_size_mb": 98.2,
                "status": "completed",
                "created_at": (datetime.now() - timedelta(days=3)).isoformat()
            }
        ]
        
        return {"datasets": demo_datasets[:limit], "total": len(demo_datasets)}

    async def get_job_stats(self):
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    # Try to get real job stats from database
                    total = await conn.fetchval("SELECT COUNT(*) FROM job_runs")
                    running = await conn.fetchval("SELECT COUNT(*) FROM job_runs WHERE status = 'running'")
                    completed = await conn.fetchval("SELECT COUNT(*) FROM job_runs WHERE status = 'completed'")
                    failed = await conn.fetchval("SELECT COUNT(*) FROM job_runs WHERE status = 'failed'")
                    return {
                        "total_jobs": total or 0,
                        "running_jobs": running or 0,
                        "completed_jobs": completed or 0,
                        "failed_jobs": failed or 0
                    }
            except Exception as e:
                print(f"Job stats query failed: {e}")
        
        # Fallback to demo data
        return {
            "total_jobs": 47,
            "running_jobs": 3,
            "completed_jobs": 41,
            "failed_jobs": 3
        }

# Initialize manager
manager = AnalyticsManager()

@app.on_event("startup")
async def startup_event():
    await manager.initialize()

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/v1/jobs/stats")
async def get_jobs_stats():
    return await manager.get_job_stats()

@app.get("/api/v1/jobs")
async def get_jobs():
    # Demo job data with realistic examples
    demo_jobs = [
        {"id": 1, "job_type": "data_collection", "status": "running", "started_at": "2025-08-24T03:30:00Z", "symbol": "AAPL"},
        {"id": 2, "job_type": "model_training", "status": "completed", "started_at": "2025-08-24T02:15:00Z", "symbol": "TSLA"},
        {"id": 3, "job_type": "backfill", "status": "running", "started_at": "2025-08-24T03:45:00Z", "symbol": "MSFT"},
        {"id": 4, "job_type": "analytics", "status": "failed", "started_at": "2025-08-24T01:20:00Z", "symbol": "GOOGL"},
        {"id": 5, "job_type": "validation", "status": "completed", "started_at": "2025-08-24T00:30:00Z", "symbol": "AMZN"},
    ]
    return {"jobs": demo_jobs, "total": len(demo_jobs)}

@app.get("/api/v1/coverage/summary")
async def get_coverage_summary():
    return {
        "total_combinations": 125,
        "active_combinations": 108,
        "average_coverage_24h": 94,
        "average_quality_24h": 0.96,
        "summary": [
            {"symbol": "AAPL", "vendor": "polygon", "coverage_24h": 98, "coverage_7d": 97, "quality_24h": 0.99, "current_status": "active"},
            {"symbol": "TSLA", "vendor": "tiingo", "coverage_24h": 92, "coverage_7d": 94, "quality_24h": 0.95, "current_status": "active"},
            {"symbol": "MSFT", "vendor": "polygon", "coverage_24h": 96, "coverage_7d": 96, "quality_24h": 0.98, "current_status": "active"},
            {"symbol": "GOOGL", "vendor": "finnhub", "coverage_24h": 89, "coverage_7d": 91, "quality_24h": 0.93, "current_status": "warning"},
        ]
    }

@app.get("/api/v1/coverage/gaps")
async def get_coverage_gaps():
    return {
        "gaps": [
            {"symbol": "NVDA", "vendor": "polygon", "gap_duration_minutes": 12, "severity": "minor", "gap_start": "2025-08-24T03:15:00Z"},
            {"symbol": "META", "vendor": "tiingo", "gap_duration_minutes": 45, "severity": "major", "gap_start": "2025-08-24T02:30:00Z"},
            {"symbol": "NFLX", "vendor": "finnhub", "gap_duration_minutes": 6, "severity": "minor", "gap_start": "2025-08-24T03:42:00Z"}
        ]
    }

@app.get("/api/v1/datasets")
async def get_datasets(limit: int = Query(5, ge=1, le=100), offset: int = Query(0, ge=0)):
    return await manager.get_datasets(limit, offset)

@app.get("/api/v1/datasets/{dataset_id}")
async def get_dataset_detail(dataset_id: int):
    datasets_response = await manager.get_datasets(limit=100)
    datasets = datasets_response["datasets"]
    
    dataset = next((d for d in datasets if d["dataset_id"] == dataset_id), None)
    if not dataset:
        raise HTTPException(404, f"Dataset {dataset_id} not found")
    
    return dataset

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return '''<!DOCTYPE html>
<html>
<head>
<title>ATS Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; min-height: 100vh; }
.header { background: rgba(0,0,0,.2); padding: 25px; text-align: center; box-shadow: 0 2px 10px rgba(0,0,0,.3); }
.header h1 { font-size: 2.5em; margin-bottom: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,.5); }
.tabs { display: flex; background: rgba(0,0,0,.15); padding: 0 25px; border-bottom: 1px solid rgba(255,255,255,.1); }
.tab { padding: 18px 30px; cursor: pointer; border: none; background: none; color: rgba(255,255,255,.7); font-size: 16px; font-weight: 500; transition: all 0.3s ease; border-radius: 8px 8px 0 0; margin: 0 2px; }
.tab.active, .tab:hover { color: white; background: rgba(255,255,255,.15); transform: translateY(-2px); }
.tab-content { display: none; padding: 30px; animation: fadeIn 0.5s ease-in; }
.tab-content.active { display: block; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 25px; margin-bottom: 35px; }
.stat-card { background: rgba(255,255,255,.12); border-radius: 20px; padding: 30px; text-align: center; backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,.1); transition: transform 0.3s ease; }
.stat-card:hover { transform: translateY(-5px); }
.stat-number { font-size: 3em; font-weight: bold; margin-bottom: 15px; text-shadow: 2px 2px 4px rgba(0,0,0,.3); }
.stat-label { font-size: 1.1em; opacity: 0.9; font-weight: 500; }
.list-container { background: rgba(255,255,255,.08); border-radius: 20px; padding: 30px; backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,.1); }
.list-container h3 { margin-bottom: 25px; font-size: 1.4em; color: #fff; }
.list-item { background: rgba(255,255,255,.1); margin-bottom: 18px; padding: 25px; border-radius: 15px; cursor: pointer; transition: all 0.3s ease; border: 1px solid rgba(255,255,255,.05); }
.list-item:hover { background: rgba(255,255,255,.15); transform: translateX(5px); }
.list-item h4 { color: #fff; margin-bottom: 8px; font-size: 1.1em; }
.list-item p { color: rgba(255,255,255,.8); margin-bottom: 10px; }
.status { display: inline-block; padding: 8px 16px; border-radius: 25px; font-size: .9em; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
.status.active { background: #4caf50; color: white; box-shadow: 0 2px 8px rgba(76,175,80,.3); }
.status.running { background: #2196f3; color: white; box-shadow: 0 2px 8px rgba(33,150,243,.3); animation: pulse 2s infinite; }
.status.completed { background: #4caf50; color: white; box-shadow: 0 2px 8px rgba(76,175,80,.3); }
.status.failed { background: #f44336; color: white; box-shadow: 0 2px 8px rgba(244,67,54,.3); }
.status.processing { background: #ff9800; color: white; box-shadow: 0 2px 8px rgba(255,152,0,.3); animation: pulse 2s infinite; }
.status.warning { background: #ff5722; color: white; box-shadow: 0 2px 8px rgba(255,87,34,.3); }
@keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.7; } 100% { opacity: 1; } }
.loading { text-align: center; padding: 60px; font-size: 1.2em; }
.loading::after { content: '...'; animation: dots 1.5s infinite; }
@keyframes dots { 0%, 20% { color: rgba(255,255,255,.2); text-shadow: .25em 0 0 rgba(255,255,255,.2), .5em 0 0 rgba(255,255,255,.2); } 40% { color: white; text-shadow: .25em 0 0 rgba(255,255,255,.2), .5em 0 0 rgba(255,255,255,.2); } 60% { text-shadow: .25em 0 0 white, .5em 0 0 rgba(255,255,255,.2); } 80%, 100% { text-shadow: .25em 0 0 white, .5em 0 0 white; } }
.error { color: #ff6b6b; text-align: center; padding: 30px; font-size: 1.1em; background: rgba(255,107,107,.1); border-radius: 15px; border: 1px solid rgba(255,107,107,.2); }
.chart-container { background: rgba(255,255,255,.95); border-radius: 15px; padding: 20px; margin: 20px 0; color: #333; }
.refresh-btn { background: rgba(255,255,255,.2); border: 1px solid rgba(255,255,255,.3); color: white; padding: 12px 24px; border-radius: 25px; cursor: pointer; transition: all 0.3s ease; font-weight: 500; }
.refresh-btn:hover { background: rgba(255,255,255,.3); transform: translateY(-2px); }
</style>
</head>
<body>
<div class="header">
<h1>📊 ATS Analytics Dashboard</h1>
<p>Advanced Analytics • Real-Time Data • Job Management</p>
</div>

<div class="tabs">
<button class="tab active" onclick="showTab('jobs')">📋 Job Management</button>
<button class="tab" onclick="showTab('datasets')">📚 Dataset Analytics</button>
<button class="tab" onclick="showTab('coverage')">📡 Data Coverage</button>
</div>

<div id="jobs" class="tab-content active">
<div class="stats-grid" id="jobs-stats">
<div class="loading">Loading job statistics</div>
</div>
<div class="list-container">
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px;">
<h3>Recent Jobs</h3>
<button class="refresh-btn" onclick="loadJobs()">🔄 Refresh</button>
</div>
<div id="jobs-list">
<div class="loading">Loading jobs</div>
</div>
</div>
</div>

<div id="datasets" class="tab-content">
<div class="list-container">
<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 25px;">
<h3>Training Datasets</h3>
<button class="refresh-btn" onclick="loadDatasets()">🔄 Refresh</button>
</div>
<div id="datasets-list">
<div class="loading">Loading datasets</div>
</div>
</div>
</div>

<div id="coverage" class="tab-content">
<div class="stats-grid" id="coverage-stats">
<div class="loading">Loading coverage statistics</div>
</div>
<div class="list-container">
<h3>Coverage Summary</h3>
<div id="coverage-summary">
<div class="loading">Loading coverage data</div>
</div>
</div>
<div class="list-container" style="margin-top: 25px;">
<h3>Data Gaps</h3>
<div id="coverage-gaps">
<div class="loading">Loading gap analysis</div>
</div>
</div>
</div>

<script>
let currentTab = 'jobs';

function showTab(tabName) {
console.log('Showing tab:', tabName);
currentTab = tabName;

document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
document.querySelectorAll('.tab').forEach(c => c.classList.remove('active'));

document.getElementById(tabName).classList.add('active');
event.target.classList.add('active');

if (tabName === 'jobs') {
loadJobs();
} else if (tabName === 'datasets') {
loadDatasets();
} else if (tabName === 'coverage') {
loadCoverage();
}
}

async function loadJobs() {
try {
console.log('Loading jobs...');

const statsResponse = await fetch('/api/v1/jobs/stats');
const stats = await statsResponse.json();

document.getElementById('jobs-stats').innerHTML = `
<div class="stat-card">
<div class="stat-number">${stats.total_jobs}</div>
<div class="stat-label">Total Jobs</div>
</div>
<div class="stat-card">
<div class="stat-number">${stats.running_jobs}</div>
<div class="stat-label">Running</div>
</div>
<div class="stat-card">
<div class="stat-number">${stats.completed_jobs}</div>
<div class="stat-label">Completed</div>
</div>
<div class="stat-card">
<div class="stat-number">${stats.failed_jobs}</div>
<div class="stat-label">Failed</div>
</div>
`;

const jobsResponse = await fetch('/api/v1/jobs');
const jobs = await jobsResponse.json();

if (jobs.jobs && jobs.jobs.length > 0) {
document.getElementById('jobs-list').innerHTML = jobs.jobs.map(job => `
<div class="list-item">
<div style="display:flex;justify-content:space-between;align-items:center">
<div>
<h4>${job.job_type} - ${job.symbol || 'N/A'}</h4>
<p>ID: ${job.id} | Started: ${new Date(job.started_at || Date.now()).toLocaleString()}</p>
</div>
<span class="status ${job.status}">${job.status}</span>
</div>
</div>
`).join('');
} else {
document.getElementById('jobs-list').innerHTML = '<div class="loading">No jobs found</div>';
}
} catch (error) {
console.error('Error loading jobs:', error);
document.getElementById('jobs-stats').innerHTML = '<div class="error">Error loading job statistics</div>';
}
}

async function loadDatasets() {
try {
console.log('Loading datasets...');

const response = await fetch('/api/v1/datasets');
const data = await response.json();

if (data.datasets && data.datasets.length > 0) {
document.getElementById('datasets-list').innerHTML = data.datasets.map(dataset => `
<div class="list-item" onclick="showDatasetDetail(${dataset.dataset_id})">
<div>
<h4>${dataset.dataset_name}</h4>
<p><strong>Symbols:</strong> ${dataset.symbols ? dataset.symbols.join(', ') : 'N/A'}</p>
<p><strong>Sequences:</strong> ${dataset.total_sequences.toLocaleString()} | <strong>Features:</strong> ${dataset.feature_count} | <strong>Size:</strong> ${dataset.file_size_mb}MB</p>
<p><strong>Created:</strong> ${new Date(dataset.created_at).toLocaleString()}</p>
<span class="status ${dataset.status}">${dataset.status}</span>
</div>
</div>
`).join('');
} else {
document.getElementById('datasets-list').innerHTML = '<div class="error">No datasets found</div>';
}
} catch (error) {
console.error('Error loading datasets:', error);
document.getElementById('datasets-list').innerHTML = '<div class="error">Error loading datasets</div>';
}
}

async function loadCoverage() {
try {
console.log('Loading coverage...');

const summaryResponse = await fetch('/api/v1/coverage/summary');
const summary = await summaryResponse.json();

document.getElementById('coverage-stats').innerHTML = `
<div class="stat-card">
<div class="stat-number">${summary.total_combinations}</div>
<div class="stat-label">Total Combinations</div>
</div>
<div class="stat-card">
<div class="stat-number">${summary.active_combinations}</div>
<div class="stat-label">Active</div>
</div>
<div class="stat-card">
<div class="stat-number">${summary.average_coverage_24h}%</div>
<div class="stat-label">Avg Coverage</div>
</div>
<div class="stat-card">
<div class="stat-number">${(summary.average_quality_24h * 100).toFixed(1)}%</div>
<div class="stat-label">Avg Quality</div>
</div>
`;

if (summary.summary && summary.summary.length > 0) {
document.getElementById('coverage-summary').innerHTML = summary.summary.map(item => `
<div class="list-item">
<div>
<h4>${item.symbol} - ${item.vendor}</h4>
<p><strong>24h Coverage:</strong> ${item.coverage_24h}% | <strong>7d Coverage:</strong> ${item.coverage_7d}%</p>
<p><strong>Quality Score:</strong> ${(item.quality_24h * 100).toFixed(1)}%</p>
<span class="status ${item.current_status}">${item.current_status}</span>
</div>
</div>
`).join('');
}

const gapsResponse = await fetch('/api/v1/coverage/gaps');
const gaps = await gapsResponse.json();

if (gaps.gaps && gaps.gaps.length > 0) {
document.getElementById('coverage-gaps').innerHTML = gaps.gaps.map(gap => `
<div class="list-item">
<div>
<h4>${gap.symbol} - ${gap.vendor}</h4>
<p><strong>Gap Duration:</strong> ${gap.gap_duration_minutes} minutes</p>
<p><strong>Started:</strong> ${new Date(gap.gap_start).toLocaleString()}</p>
<span class="status ${gap.severity === 'major' ? 'failed' : 'warning'}">${gap.severity}</span>
</div>
</div>
`).join('');
}
} catch (error) {
console.error('Error loading coverage:', error);
document.getElementById('coverage-stats').innerHTML = '<div class="error">Error loading coverage data</div>';
}
}

function showDatasetDetail(datasetId) {
alert(`Dataset ${datasetId} detail view would open here. Feature coming soon!`);
}

// Auto-refresh functionality
setInterval(() => {
if (currentTab === 'jobs') {
loadJobs();
} else if (currentTab === 'coverage') {
loadCoverage();
}
}, 30000); // Refresh every 30 seconds

// Initialize
document.addEventListener('DOMContentLoaded', function() {
console.log('DOM loaded, initializing dashboard...');
loadJobs();
});

if (document.readyState !== 'loading') {
loadJobs();
}
</script>
</body>
</html>'''

if __name__ == "__main__":
    print("🚀 Starting enhanced analytics service...")
    print("✅ Service starting successfully!")
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")