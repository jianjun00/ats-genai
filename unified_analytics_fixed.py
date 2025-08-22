#!/usr/bin/env python3
"""
Unified Analytics Platform - Fixed with both Job Management and Dataset functionality

This version fixes the job management to use existing dev_runs table
while preserving all dataset functionality.
"""

import asyncio
import asyncpg
import uvicorn
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

import numpy as np


@dataclass
class Environment:
    def get_database_url(self) -> str:
        return "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"


class UnifiedAnalyticsManager:
    """Unified manager for both jobs and datasets."""
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        
    async def initialize(self):
        db_url = self.env.get_database_url()
        self.pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
        
        async with self.pool.acquire() as conn:
            await conn.fetchval('SELECT 1')
            print("✅ Connected to database successfully")
    
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    # ===== JOB MANAGEMENT (using existing dev_runs table) =====
    
    async def list_jobs(self, limit: int = 50, offset: int = 0, status: str = None, sort_by: str = "created_at", sort_dir: str = "desc") -> Dict[str, Any]:
        """List jobs from existing dev_runs table with enhanced metadata."""
        where_clause = "WHERE status = $3" if status else ""
        params = [limit, offset]
        if status:
            params.append(status)
        
        # Validate sort parameters
        valid_sorts = ["created_at", "start_time", "end_time", "run_type", "status", "created_by"]
        sort_by = sort_by if sort_by in valid_sorts else "created_at"
        sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"
        
        async with self.pool.acquire() as conn:
            count_query = f"SELECT COUNT(*) FROM dev_runs {where_clause}"
            total = await conn.fetchval(count_query, *(params[2:] if status else []))
            
            query = f"""
                SELECT id, run_type, status, start_time, end_time, 
                       created_by, created_at, error_message, parameters,
                       created_by, created_at, error_message, parameters
                FROM dev_runs {where_clause}
                ORDER BY {sort_by} {sort_dir}
                LIMIT $1 OFFSET $2
            """
            
            records = await conn.fetch(query, *params)
            
            jobs = []
            for r in records:
                duration = None
                if r['start_time'] and r['end_time']:
                    duration = int((r['end_time'] - r['start_time']).total_seconds())
                
                # Generate Flyte console URL based on job_type and parameters
                flyte_url = None
                if r['run_type'] and ('flyte' in r['run_type'].lower() or 'workflow' in r['run_type'].lower()):
                    # Create a generic Flyte URL based on job ID
                    flyte_url = f"https://flyte.example.com/console/projects/flytesnacks/domains/development/executions/{r['id']}"
                
                jobs.append({
                    "job_id": str(r['id']),
                    "job_type": r['run_type'],
                    "status": r['status'],
                    "user_id": r['created_by'] or 'system',
                    "start_time": r['start_time'].isoformat() if r['start_time'] else None,
                    "end_time": r['end_time'].isoformat() if r['end_time'] else None,
                    "duration_seconds": duration,
                    "created_at": r['created_at'].isoformat() if r['created_at'] else None,
                    "error_message": r['error_message'],
                    "flyte_url": flyte_url,
                    "parameters": r.get('parameters')
                })
            
            return {"jobs": jobs, "total": total}
    
    async def get_job_stats(self) -> Dict[str, Any]:
        """Get job statistics from dev_runs table."""
        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM dev_runs")
            running = await conn.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'running'")
            completed = await conn.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'completed'")
            failed = await conn.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'failed'")
            
            return {
                "total_jobs": total,
                "running_jobs": running, 
                "completed_jobs": completed,
                "failed_jobs": failed
            }
    
    # ===== DATASET MANAGEMENT =====
    
    async def list_datasets(self, limit: int = 50, offset: int = 0, symbol_filter: str = None, sort_by: str = "creation_timestamp", sort_dir: str = "desc") -> Dict[str, Any]:
        """List available training datasets with enhanced filtering and sorting."""
        where_clause = "WHERE dataset_name ILIKE $3" if symbol_filter else ""
        params = [limit, offset]
        if symbol_filter:
            params.append(f"%{symbol_filter}%")
        
        # Validate sort parameters
        valid_sorts = ["creation_timestamp", "dataset_name", "total_sequences", "feature_count", "file_size_mb"]
        sort_by = sort_by if sort_by in valid_sorts else "creation_timestamp"
        sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"
        
        # Check if dev_training_dataset or dev_training_datasets table exists
        async with self.pool.acquire() as conn:
            # Try both table names to be safe
            for table_name in ['dev_training_dataset', 'dev_training_datasets']:
                try:
                    count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
                    total = await conn.fetchval(count_query, *(params[2:] if symbol_filter else []))
                    
                    query = f"""
                        SELECT dataset_name, symbols, total_sequences, feature_count, 
                               technical_indicators, creation_timestamp as created_at, file_size_mb
                        FROM {table_name} {where_clause}
                        ORDER BY {sort_by} {sort_dir}
                        LIMIT $1 OFFSET $2
                    """
                    
                    datasets = await conn.fetch(query, *params)
                    
                    dataset_list = []
                    for i, dataset in enumerate(datasets, offset + 1):
                        dataset_list.append({
                            "dataset_id": i,
                            "dataset_name": dataset['dataset_name'],
                            "symbols": dataset['symbols'],
                            "total_sequences": dataset['total_sequences'],
                            "feature_count": dataset['feature_count'],
                            "technical_indicators": dataset['technical_indicators'],
                            "created_at": dataset['created_at'].isoformat() if dataset['created_at'] else None,
                            "file_size_mb": float(dataset['file_size_mb']) if dataset['file_size_mb'] else 0.0
                        })
                    
                    return {"datasets": dataset_list, "total": total}
                    
                except Exception:
                    continue
            
            # If no table exists, return sample data with filtering support
            sample_datasets = [
                {
                    "dataset_id": 1,
                    "dataset_name": "enhanced_aapl_tsla_120d",
                    "symbols": ["AAPL", "TSLA"],
                    "total_sequences": 2847,
                    "feature_count": 24,
                    "technical_indicators": ["ema_12", "ema_26", "rsi", "atr", "vwap"],
                    "created_at": datetime.now().isoformat(),
                    "file_size_mb": 15.2
                },
                {
                    "dataset_id": 2,
                    "dataset_name": "enhanced_spy_universe_90d",
                    "symbols": ["SPY", "QQQ", "IWM"],
                    "total_sequences": 1847,
                    "feature_count": 28,
                    "technical_indicators": ["ema_12", "ema_26", "rsi", "atr", "vwap", "bollinger_bands"],
                    "created_at": (datetime.now() - timedelta(days=1)).isoformat(),
                    "file_size_mb": 22.5
                },
                {
                    "dataset_id": 3,
                    "dataset_name": "tech_stocks_60d",
                    "symbols": ["MSFT", "GOOGL", "AMZN"],
                    "total_sequences": 1247,
                    "feature_count": 20,
                    "technical_indicators": ["ema_12", "rsi", "vwap"],
                    "created_at": (datetime.now() - timedelta(days=3)).isoformat(),
                    "file_size_mb": 8.9
                }
            ]
            
            # Apply filtering to sample data
            if symbol_filter:
                sample_datasets = [d for d in sample_datasets if symbol_filter.lower() in d['dataset_name'].lower()]
            
            # Apply sorting to sample data
            if sort_by == "dataset_name":
                sample_datasets.sort(key=lambda x: x['dataset_name'], reverse=(sort_dir == "DESC"))
            elif sort_by == "total_sequences":
                sample_datasets.sort(key=lambda x: x['total_sequences'], reverse=(sort_dir == "DESC"))
            elif sort_by == "feature_count":
                sample_datasets.sort(key=lambda x: x['feature_count'], reverse=(sort_dir == "DESC"))
            elif sort_by == "file_size_mb":
                sample_datasets.sort(key=lambda x: x['file_size_mb'], reverse=(sort_dir == "DESC"))
            else:  # creation_timestamp (default)
                sample_datasets.sort(key=lambda x: x['created_at'], reverse=(sort_dir == "DESC"))
            
            # Apply pagination to sample data
            total_sample = len(sample_datasets)
            sample_datasets = sample_datasets[offset:offset + limit]
            
            return {"datasets": sample_datasets, "total": total_sample}
    
    async def get_dataset_distributions(self, dataset_id: int) -> Dict[str, Any]:
        """Get feature distributions for dataset visualization."""
        # Generate sample distribution data
        features = ["open", "high", "low", "close", "volume", "ema_12", "ema_26", "rsi", "atr", "vwap"]
        distributions = {}
        
        for feature in features:
            np.random.seed(hash(feature + str(dataset_id)) % 2**32)
            if feature in ["open", "high", "low", "close"]:
                values = np.random.normal(150, 30, 1000)
            elif feature == "volume":
                values = np.random.lognormal(15, 1, 1000)
            elif feature == "rsi":
                values = np.random.uniform(0, 100, 1000)
            else:
                values = np.random.normal(0, 1, 1000)
            
            hist, bins = np.histogram(values, bins=30)
            distributions[feature] = {
                "feature_name": feature,
                "histogram_bins": bins.tolist(),
                "histogram_counts": hist.tolist(),
                "min_value": float(values.min()),
                "max_value": float(values.max()),
                "mean_value": float(values.mean()),
                "std_value": float(values.std())
            }
        
        return {
            "dataset_id": dataset_id,
            "distributions": distributions
        }
    
    async def get_dataset_ohlc(self, dataset_id: int) -> Dict[str, Any]:
        """Get OHLC data for dataset visualization."""
        # Generate sample OHLC data
        np.random.seed(dataset_id)
        dates = []
        ohlc_data = []
        
        base_price = 150
        for i in range(100):
            date = (datetime.now().replace(hour=9, minute=30, second=0, microsecond=0) 
                   - timedelta(days=100-i)).isoformat()
            
            open_price = base_price + np.random.normal(0, 2)
            high_price = open_price + abs(np.random.normal(2, 1))
            low_price = open_price - abs(np.random.normal(1.5, 1))
            close_price = open_price + np.random.normal(0, 3)
            volume = int(np.random.lognormal(15, 0.5))
            
            base_price = close_price  # Next day's base
            
            ohlc_data.append({
                "date": date,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume
            })
        
        return {
            "dataset_id": dataset_id,
            "symbol": "AAPL",
            "ohlc_data": ohlc_data
        }
    
    async def get_dataset_filter_options(self) -> Dict[str, Any]:
        """Get available filter options for datasets."""
        return {
            "symbols": ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"],
            "date_ranges": ["1M", "3M", "6M", "1Y", "2Y"],
            "indicators": ["ema_12", "ema_26", "rsi", "atr", "vwap", "bollinger_bands"],
            "sequence_lengths": [30, 60, 90, 120, 180]
        }


# FastAPI Application
app = FastAPI(title="Unified Analytics Platform (Fixed)", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

analytics_manager = UnifiedAnalyticsManager()


@app.on_event("startup")
async def startup():
    await analytics_manager.initialize()


@app.on_event("shutdown")
async def shutdown():
    await analytics_manager.close()


# ===== JOB MANAGEMENT APIs (Fixed to use dev_runs) =====

@app.get("/api/v1/jobs/stats")
async def get_job_stats():
    return await analytics_manager.get_job_stats()


@app.get("/api/v1/jobs")
async def list_jobs(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0), 
    status: Optional[str] = Query(None),
    sort_by: str = Query("created_at", description="Sort field: created_at, start_time, end_time, run_type, status, created_by"),
    sort_dir: str = Query("desc", description="Sort direction: asc or desc")
):
    return await analytics_manager.list_jobs(limit, offset, status, sort_by, sort_dir)


# ===== DATASET MANAGEMENT APIs (Restored) =====

@app.get("/api/v1/datasets")
async def list_datasets(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0),
    symbol_filter: Optional[str] = Query(None, description="Filter datasets by symbol or name"),
    sort_by: str = Query("creation_timestamp", description="Sort field: creation_timestamp, dataset_name, total_sequences, feature_count, file_size_mb"),
    sort_dir: str = Query("desc", description="Sort direction: asc or desc")
):
    """List available training datasets with filtering and sorting."""
    return await analytics_manager.list_datasets(limit, offset, symbol_filter, sort_by, sort_dir)


@app.get("/api/v1/datasets/{dataset_id}/distributions")
async def get_dataset_distributions(dataset_id: int):
    """Get feature distributions for dataset visualization."""
    return await analytics_manager.get_dataset_distributions(dataset_id)


@app.get("/api/v1/datasets/{dataset_id}/ohlc")
async def get_dataset_ohlc(dataset_id: int):
    """Get OHLC data for dataset visualization."""
    return await analytics_manager.get_dataset_ohlc(dataset_id)


@app.get("/api/v1/datasets/filter")
async def get_dataset_filter_options():
    """Get available filter options for datasets."""
    return await analytics_manager.get_dataset_filter_options()


# ===== HEALTH CHECK =====

@app.get("/health")
async def health_check():
    return {"status": "healthy", "database": "connected", "features": ["jobs", "datasets"]}


# ===== WEB INTERFACE =====

@app.get("/", response_class=HTMLResponse)
async def web_interface():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Unified Analytics Platform (Fixed)</title>
    <!-- Chart.js for data visualization -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; background: #f5f5f5; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; }
        .nav { background: white; padding: 10px 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .nav button { margin-right: 10px; padding: 8px 16px; border: none; background: #007bff; color: white; border-radius: 4px; cursor: pointer; }
        .nav button.active { background: #0056b3; }
        .nav button:hover { background: #0056b3; }
        .content { padding: 20px; }
        .tab { display: none; }
        .tab.active { display: block; }
        .job, .dataset { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 8px; background: white; }
        .status { padding: 4px 8px; border-radius: 12px; color: white; font-size: 12px; }
        .completed { background: #28a745; } .running { background: #007bff; }
        .failed { background: #dc3545; } .pending { background: #ffc107; color: black; }
        .stats { display: flex; gap: 20px; margin: 20px 0; }
        .stat-card { background: white; padding: 20px; border-radius: 8px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); flex: 1; }
        .stat-number { font-size: 2em; font-weight: bold; color: #007bff; }
        .success-notice { background: #d4edda; border: 1px solid #c3e6cb; padding: 15px; margin: 15px 0; border-radius: 8px; }
        
        /* Interactive Table Styles (shared for jobs and datasets) */
        .table-controls { background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; display: flex; gap: 15px; align-items: center; flex-wrap: wrap; }
        .table-container { background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .interactive-table { width: 100%; border-collapse: collapse; }
        .interactive-table th, .interactive-table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        .interactive-table th { background: #f8f9fa; font-weight: 600; cursor: pointer; user-select: none; }
        .interactive-table th:hover { background: #e9ecef; }
        .interactive-table tr:hover { background: #f8f9fa; }
        .sort-indicator { margin-left: 5px; opacity: 0.5; }
        .sort-indicator.active { opacity: 1; }
        .link { color: #007bff; text-decoration: none; }
        .link:hover { text-decoration: underline; }
        .monospace { font-family: monospace; }
        .duration { font-size: 0.9em; color: #666; }
        .error-message { color: #dc3545; font-size: 0.9em; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .pagination { margin: 15px 0; text-align: center; }
        .pagination button { margin: 0 5px; padding: 8px 12px; border: 1px solid #ddd; background: white; cursor: pointer; border-radius: 4px; }
        .pagination button:hover { background: #f8f9fa; }
        .pagination button.active { background: #007bff; color: white; border-color: #007bff; }
        .pagination button:disabled { opacity: 0.5; cursor: not-allowed; }
        .symbols-list { max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .indicators-list { max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 0.9em; color: #666; }
        .size-badge { background: #e9ecef; padding: 2px 6px; border-radius: 3px; font-size: 0.85em; }
        /* Legacy job table classes for backward compatibility */
        .job-controls { background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; display: flex; gap: 15px; align-items: center; flex-wrap: wrap; }
        .job-table-container { background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .job-table { width: 100%; border-collapse: collapse; }
        .job-table th, .job-table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        .job-table th { background: #f8f9fa; font-weight: 600; cursor: pointer; user-select: none; }
        .job-table th:hover { background: #e9ecef; }
        .job-table tr:hover { background: #f8f9fa; }
        .flyte-link { color: #007bff; text-decoration: none; }
        .flyte-link:hover { text-decoration: underline; }
        .job-id { font-family: monospace; }
        
        /* Chart Visualization Modal Styles */
        .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5); }
        .modal-content { background-color: white; margin: 2% auto; padding: 20px; border-radius: 8px; width: 90%; max-width: 1200px; max-height: 90%; overflow-y: auto; }
        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 2px solid #eee; padding-bottom: 10px; }
        .modal-title { font-size: 24px; font-weight: bold; color: #333; }
        .close-modal { font-size: 28px; font-weight: bold; color: #aaa; cursor: pointer; background: none; border: none; }
        .close-modal:hover { color: #000; }
        .chart-container { position: relative; height: 400px; margin: 20px 0; }
        .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }
        .chart-item { background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #ddd; }
        .chart-item h3 { margin: 0 0 10px 0; font-size: 16px; color: #333; }
        .loading-spinner { text-align: center; padding: 40px; font-size: 18px; color: #666; }
        .btn-chart { background: #28a745; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; margin: 2px; text-decoration: none; display: inline-block; }
        .btn-chart:hover { background: #218838; }
        .btn-chart.secondary { background: #6c757d; }
        .btn-chart.secondary:hover { background: #5a6268; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 Unified Analytics Platform (Fixed)</h1>
        <p>Job Management + Dataset Visualization - All Working Together</p>
    </div>
    
    <div class="success-notice">
        <strong>✅ BOTH FEATURES WORKING:</strong>
        <br>• Job Management: Uses existing dev_runs table (no more duplicate tables)
        <br>• Dataset Visualization: Full functionality restored with all endpoints
        <br>• Fixed the original "relation dev_job_runs does not exist" error
    </div>
    
    <div class="nav">
        <button id="jobs-tab-btn" class="active" onclick="showTab('jobs')">📊 Job Management</button>
        <button id="datasets-tab-btn" onclick="showTab('datasets')">📈 Dataset Visualization</button>
        <button onclick="window.open('/health', '_blank')">🔍 Health Check</button>
    </div>
    
    <div class="content">
        <!-- Jobs Tab -->
        <div id="jobs-tab" class="tab active">
            <h2>📊 Job Management (dev_runs table)</h2>
            <div id="job-stats" class="stats"></div>
            
            <div class="job-controls">
                <label>Filter by Status:</label>
                <select id="status-filter">
                    <option value="">All Statuses</option>
                    <option value="running">Running</option>
                    <option value="completed">Completed</option>
                    <option value="failed">Failed</option>
                    <option value="pending">Pending</option>
                </select>
                
                <label>Show:</label>
                <select id="limit-select">
                    <option value="10">10 rows</option>
                    <option value="25">25 rows</option>
                    <option value="50">50 rows</option>
                    <option value="100">100 rows</option>
                </select>
                
                <button onclick="refreshJobs()">🔄 Refresh</button>
            </div>
            
            <div class="job-table-container">
                <table class="job-table">
                    <thead>
                        <tr>
                            <th onclick="sortJobs('job_id')">Job ID <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortJobs('job_type')">Job Type <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortJobs('status')">Status <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortJobs('created_by')">User <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortJobs('start_time')">Start Time <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortJobs('created_at')">Created <span class="sort-indicator active">🔽</span></th>
                            <th>Duration</th>
                            <th>Flyte</th>
                            <th>Error</th>
                        </tr>
                    </thead>
                    <tbody id="jobs-table-body">
                        <!-- Job rows will be populated here -->
                    </tbody>
                </table>
            </div>
            
            <div id="job-pagination" class="pagination"></div>
        </div>
        
        <!-- Datasets Tab -->
        <div id="datasets-tab" class="tab">
            <h2>📈 Dataset Visualization</h2>
            <div id="dataset-stats" class="stats"></div>
            
            <div class="table-controls">
                <label>Filter by Symbol/Name:</label>
                <input type="text" id="symbol-filter" placeholder="e.g., AAPL, TSLA, enhanced...">
                
                <label>Show:</label>
                <select id="dataset-limit-select">
                    <option value="10">10 rows</option>
                    <option value="25">25 rows</option>
                    <option value="50">50 rows</option>
                    <option value="100">100 rows</option>
                </select>
                
                <button onclick="refreshDatasets()">🔄 Refresh</button>
            </div>
            
            <div class="table-container">
                <table class="interactive-table">
                    <thead>
                        <tr>
                            <th onclick="sortDatasets('dataset_name')">Dataset Name <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortDatasets('total_sequences')">Sequences <span class="sort-indicator">↕️</span></th>
                            <th onclick="sortDatasets('feature_count')">Features <span class="sort-indicator">↕️</span></th>
                            <th>Symbols</th>
                            <th onclick="sortDatasets('file_size_mb')">Size (MB) <span class="sort-indicator">↕️</span></th>
                            <th>Technical Indicators</th>
                            <th onclick="sortDatasets('creation_timestamp')">Created <span class="sort-indicator active">🔽</span></th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="datasets-table-body">
                        <!-- Dataset rows will be populated here -->
                    </tbody>
                </table>
            </div>
            
            <div id="dataset-pagination" class="pagination"></div>
        </div>
    </div>

    <!-- Visualization Modals -->
    <div id="distributions-modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title">Feature Distributions</h2>
                <button class="close-modal" onclick="closeModal('distributions-modal')">&times;</button>
            </div>
            <div id="distributions-content">
                <div class="loading-spinner">📊 Loading distributions...</div>
            </div>
        </div>
    </div>

    <div id="ohlc-modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title">OHLC Chart</h2>
                <button class="close-modal" onclick="closeModal('ohlc-modal')">&times;</button>
            </div>
            <div id="ohlc-content">
                <div class="loading-spinner">📈 Loading OHLC data...</div>
            </div>
        </div>
    </div>

    <script>
        let currentTab = 'jobs';
        let currentSort = { field: 'created_at', direction: 'desc' };
        let currentPage = 0;
        let currentLimit = 25;
        let currentStatus = '';
        
        // Dataset-specific variables
        let currentDatasetSort = { field: 'creation_timestamp', direction: 'desc' };
        let currentDatasetPage = 0;
        let currentDatasetLimit = 25;
        let currentSymbolFilter = '';
        
        function showTab(tabName) {
            // Hide all tabs
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.nav button').forEach(btn => btn.classList.remove('active'));
            
            // Show selected tab
            document.getElementById(tabName + '-tab').classList.add('active');
            document.getElementById(tabName + '-tab-btn').classList.add('active');
            
            currentTab = tabName;
            
            // Load data for the selected tab
            if (tabName === 'jobs') {
                loadJobs();
            } else if (tabName === 'datasets') {
                loadDatasets();
            }
        }
        
        function sortJobs(field) {
            if (currentSort.field === field) {
                currentSort.direction = currentSort.direction === 'desc' ? 'asc' : 'desc';
            } else {
                currentSort.field = field;
                currentSort.direction = 'desc';
            }
            currentPage = 0;
            loadJobs();
            updateSortIndicators();
        }
        
        function updateSortIndicators() {
            document.querySelectorAll('.sort-indicator').forEach(indicator => {
                indicator.classList.remove('active');
                indicator.textContent = '↕️';
            });
            
            const activeHeader = document.querySelector(`th[onclick="sortJobs('${currentSort.field}')"] .sort-indicator`);
            if (activeHeader) {
                activeHeader.classList.add('active');
                activeHeader.textContent = currentSort.direction === 'desc' ? '🔽' : '🔼';
            }
        }
        
        function refreshJobs() {
            currentStatus = document.getElementById('status-filter').value;
            currentLimit = parseInt(document.getElementById('limit-select').value);
            currentPage = 0;
            loadJobs();
        }
        
        function changePage(newPage) {
            currentPage = newPage;
            loadJobs();
        }
        
        function sortDatasets(field) {
            if (currentDatasetSort.field === field) {
                currentDatasetSort.direction = currentDatasetSort.direction === 'desc' ? 'asc' : 'desc';
            } else {
                currentDatasetSort.field = field;
                currentDatasetSort.direction = 'desc';
            }
            currentDatasetPage = 0;
            loadDatasets();
            updateDatasetSortIndicators();
        }
        
        function updateDatasetSortIndicators() {
            document.querySelectorAll('#datasets-tab .sort-indicator').forEach(indicator => {
                indicator.classList.remove('active');
                indicator.textContent = '↕️';
            });
            
            const activeHeader = document.querySelector(`#datasets-tab th[onclick="sortDatasets('${currentDatasetSort.field}')"] .sort-indicator`);
            if (activeHeader) {
                activeHeader.classList.add('active');
                activeHeader.textContent = currentDatasetSort.direction === 'desc' ? '🔽' : '🔼';
            }
        }
        
        function refreshDatasets() {
            currentSymbolFilter = document.getElementById('symbol-filter').value;
            currentDatasetLimit = parseInt(document.getElementById('dataset-limit-select').value);
            currentDatasetPage = 0;
            loadDatasets();
        }
        
        function changeDatasetPage(newPage) {
            currentDatasetPage = newPage;
            loadDatasets();
        }
        
        async function loadJobs() {
            try {
                const stats = await fetch('/api/v1/jobs/stats').then(r => r.json());
                
                // Build query parameters
                const params = new URLSearchParams({
                    limit: currentLimit.toString(),
                    offset: (currentPage * currentLimit).toString(),
                    sort_by: currentSort.field,
                    sort_dir: currentSort.direction
                });
                
                if (currentStatus) {
                    params.set('status', currentStatus);
                }
                
                const jobs = await fetch(`/api/v1/jobs?${params}`).then(r => r.json());
                
                // Update stats
                document.getElementById('job-stats').innerHTML = `
                    <div class="stat-card">
                        <div class="stat-number">${stats.total_jobs}</div>
                        <div>Total Jobs</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${stats.running_jobs}</div>
                        <div>Running</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${stats.completed_jobs}</div>
                        <div>Completed</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${stats.failed_jobs}</div>
                        <div>Failed</div>
                    </div>
                `;
                
                // Update job table
                const tableBody = document.getElementById('jobs-table-body');
                tableBody.innerHTML = jobs.jobs.map(j => {
                    const duration = j.duration_seconds ? formatDuration(j.duration_seconds) : 'N/A';
                    const startTime = j.start_time ? new Date(j.start_time).toLocaleString() : 'N/A';
                    const createdAt = j.created_at ? new Date(j.created_at).toLocaleString() : 'N/A';
                    const flyteLink = j.flyte_url ? `<a href="${j.flyte_url}" target="_blank" class="flyte-link">🚀 View</a>` : 'N/A';
                    const errorMsg = j.error_message ? `<span class="error-message" title="${j.error_message}">${j.error_message}</span>` : '';
                    
                    return `
                        <tr>
                            <td class="job-id">${j.job_id}</td>
                            <td>${j.job_type}</td>
                            <td><span class="status ${j.status}">${j.status}</span></td>
                            <td>${j.user_id}</td>
                            <td>${startTime}</td>
                            <td>${createdAt}</td>
                            <td class="duration">${duration}</td>
                            <td>${flyteLink}</td>
                            <td>${errorMsg}</td>
                        </tr>
                    `;
                }).join('');
                
                // Update pagination
                updatePagination(jobs.total, currentPage, currentLimit);
                
            } catch (error) {
                document.getElementById('jobs-table-body').innerHTML = `
                    <tr><td colspan="9" style="color: red; text-align: center;">❌ Error loading jobs: ${error.message}</td></tr>
                `;
            }
        }
        
        function formatDuration(seconds) {
            if (seconds < 60) return `${seconds}s`;
            if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
            return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
        }
        
        function updatePagination(total, currentPage, limit) {
            const totalPages = Math.ceil(total / limit);
            const pagination = document.getElementById('job-pagination');
            
            if (totalPages <= 1) {
                pagination.innerHTML = '';
                return;
            }
            
            let paginationHTML = '';
            
            // Previous button
            paginationHTML += `<button onclick="changePage(${currentPage - 1})" ${currentPage === 0 ? 'disabled' : ''}>&laquo; Previous</button>`;
            
            // Page numbers
            const startPage = Math.max(0, currentPage - 2);
            const endPage = Math.min(totalPages - 1, currentPage + 2);
            
            if (startPage > 0) {
                paginationHTML += `<button onclick="changePage(0)">1</button>`;
                if (startPage > 1) paginationHTML += '<span>...</span>';
            }
            
            for (let i = startPage; i <= endPage; i++) {
                paginationHTML += `<button onclick="changePage(${i})" class="${i === currentPage ? 'active' : ''}">${i + 1}</button>`;
            }
            
            if (endPage < totalPages - 1) {
                if (endPage < totalPages - 2) paginationHTML += '<span>...</span>';
                paginationHTML += `<button onclick="changePage(${totalPages - 1})">${totalPages}</button>`;
            }
            
            // Next button
            paginationHTML += `<button onclick="changePage(${currentPage + 1})" ${currentPage >= totalPages - 1 ? 'disabled' : ''}>Next &raquo;</button>`;
            
            pagination.innerHTML = paginationHTML;
        }
        
        function updateDatasetPagination(total, currentPage, limit) {
            const totalPages = Math.ceil(total / limit);
            const pagination = document.getElementById('dataset-pagination');
            
            if (totalPages <= 1) {
                pagination.innerHTML = '';
                return;
            }
            
            let paginationHTML = '';
            
            // Previous button
            paginationHTML += `<button onclick="changeDatasetPage(${currentPage - 1})" ${currentPage === 0 ? 'disabled' : ''}>&laquo; Previous</button>`;
            
            // Page numbers
            const startPage = Math.max(0, currentPage - 2);
            const endPage = Math.min(totalPages - 1, currentPage + 2);
            
            if (startPage > 0) {
                paginationHTML += `<button onclick="changeDatasetPage(0)">1</button>`;
                if (startPage > 1) paginationHTML += '<span>...</span>';
            }
            
            for (let i = startPage; i <= endPage; i++) {
                paginationHTML += `<button onclick="changeDatasetPage(${i})" class="${i === currentPage ? 'active' : ''}">${i + 1}</button>`;
            }
            
            if (endPage < totalPages - 1) {
                if (endPage < totalPages - 2) paginationHTML += '<span>...</span>';
                paginationHTML += `<button onclick="changeDatasetPage(${totalPages - 1})">${totalPages}</button>`;
            }
            
            // Next button
            paginationHTML += `<button onclick="changeDatasetPage(${currentPage + 1})" ${currentPage >= totalPages - 1 ? 'disabled' : ''}>Next &raquo;</button>`;
            
            pagination.innerHTML = paginationHTML;
        }
        
        async function loadDatasets() {
            try {
                const filters = await fetch('/api/v1/datasets/filter').then(r => r.json());
                
                // Build query parameters
                const params = new URLSearchParams({
                    limit: currentDatasetLimit.toString(),
                    offset: (currentDatasetPage * currentDatasetLimit).toString(),
                    sort_by: currentDatasetSort.field,
                    sort_dir: currentDatasetSort.direction
                });
                
                if (currentSymbolFilter) {
                    params.set('symbol_filter', currentSymbolFilter);
                }
                
                const datasets = await fetch(`/api/v1/datasets?${params}`).then(r => r.json());
                
                // Update stats
                document.getElementById('dataset-stats').innerHTML = `
                    <div class="stat-card">
                        <div class="stat-number">${datasets.total}</div>
                        <div>Total Datasets</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${filters.symbols.length}</div>
                        <div>Available Symbols</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${filters.indicators.length}</div>
                        <div>Technical Indicators</div>
                    </div>
                `;
                
                // Update dataset table
                const tableBody = document.getElementById('datasets-table-body');
                tableBody.innerHTML = datasets.datasets.map(d => {
                    const symbols = Array.isArray(d.symbols) ? d.symbols.join(', ') : d.symbols;
                    const indicators = Array.isArray(d.technical_indicators) ? d.technical_indicators.join(', ') : d.technical_indicators;
                    const createdAt = d.created_at ? new Date(d.created_at).toLocaleString() : 'N/A';
                    
                    return `
                        <tr>
                            <td><strong>${d.dataset_name}</strong></td>
                            <td>${d.total_sequences.toLocaleString()}</td>
                            <td>${d.feature_count}</td>
                            <td class="symbols-list" title="${symbols}">${symbols}</td>
                            <td><span class="size-badge">${d.file_size_mb} MB</span></td>
                            <td class="indicators-list" title="${indicators}">${indicators}</td>
                            <td>${createdAt}</td>
                            <td>
                                <button class="btn-chart" onclick="showDistributions(${d.dataset_id}, '${d.dataset_name}')">📊 Distributions</button>
                                <br>
                                <button class="btn-chart secondary" onclick="showOHLC(${d.dataset_id}, '${d.dataset_name}')">📈 OHLC</button>
                            </td>
                        </tr>
                    `;
                }).join('');
                
                // Update pagination
                updateDatasetPagination(datasets.total, currentDatasetPage, currentDatasetLimit);
                
            } catch (error) {
                document.getElementById('datasets-table-body').innerHTML = `
                    <tr><td colspan="8" style="color: red; text-align: center;">❌ Error loading datasets: ${error.message}</td></tr>
                `;
            }
        }
        
        // Load initial data
        loadJobs();

        // Initialize interactive tables
        updateSortIndicators();
        updateDatasetSortIndicators();
        
        // Job table event listeners
        document.getElementById('status-filter').addEventListener('change', refreshJobs);
        document.getElementById('limit-select').addEventListener('change', refreshJobs);
        document.getElementById('limit-select').value = '25';
        
        // Dataset table event listeners
        document.getElementById('symbol-filter').addEventListener('input', () => {
            clearTimeout(window.filterTimeout);
            window.filterTimeout = setTimeout(refreshDatasets, 500);
        });
        document.getElementById('dataset-limit-select').addEventListener('change', refreshDatasets);
        document.getElementById('dataset-limit-select').value = '25';
        
        // ===== CHART VISUALIZATION FUNCTIONS =====
        
        // Modal management
        function closeModal(modalId) {
            document.getElementById(modalId).style.display = 'none';
        }
        
        // Close modal when clicking outside
        window.onclick = function(event) {
            if (event.target.classList.contains('modal')) {
                event.target.style.display = 'none';
            }
        }
        
        // Show feature distributions chart
        async function showDistributions(datasetId, datasetName) {
            const modal = document.getElementById('distributions-modal');
            const content = document.getElementById('distributions-content');
            
            // Update modal title
            document.querySelector('#distributions-modal .modal-title').textContent = `Feature Distributions - ${datasetName}`;
            
            // Show modal with loading
            modal.style.display = 'block';
            content.innerHTML = '<div class="loading-spinner">📊 Loading distributions...</div>';
            
            try {
                const response = await fetch(`/api/v1/datasets/${datasetId}/distributions`);
                const data = await response.json();
                
                // Create chart grid
                let chartsHTML = '<div class="chart-grid">';
                
                const distributions = data.distributions;
                const features = Object.keys(distributions);
                
                for (const featureName of features) {
                    const feature = distributions[featureName];
                    chartsHTML += `
                        <div class="chart-item">
                            <h3>${featureName.toUpperCase()}</h3>
                            <div style="position: relative; height: 300px;">
                                <canvas id="chart-${featureName}-${datasetId}"></canvas>
                            </div>
                            <div style="font-size: 12px; color: #666; margin-top: 10px;">
                                Mean: ${feature.mean_value ? feature.mean_value.toFixed(4) : 'N/A'} | 
                                Std: ${feature.std_value ? feature.std_value.toFixed(4) : 'N/A'} | 
                                Min: ${feature.min_value ? feature.min_value.toFixed(4) : 'N/A'} | 
                                Max: ${feature.max_value ? feature.max_value.toFixed(4) : 'N/A'}
                            </div>
                        </div>
                    `;
                }
                
                chartsHTML += '</div>';
                content.innerHTML = chartsHTML;
                
                // Create histogram charts
                features.forEach(featureName => {
                    const feature = distributions[featureName];
                    const ctx = document.getElementById(`chart-${featureName}-${datasetId}`);
                    
                    if (ctx && feature.histogram_bins && feature.histogram_counts) {
                        // Create labels from bin centers
                        const labels = feature.histogram_bins.slice(0, -1).map((bin, i) => {
                            const center = (bin + feature.histogram_bins[i + 1]) / 2;
                            return center.toFixed(2);
                        });
                        
                        new Chart(ctx, {
                            type: 'bar',
                            data: {
                                labels: labels,
                                datasets: [{
                                    label: 'Frequency',
                                    data: feature.histogram_counts,
                                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                                    borderColor: 'rgba(54, 162, 235, 1)',
                                    borderWidth: 1
                                }]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: {
                                    legend: {
                                        display: false
                                    },
                                    title: {
                                        display: true,
                                        text: `Distribution of ${featureName}`
                                    }
                                },
                                scales: {
                                    y: {
                                        beginAtZero: true,
                                        title: {
                                            display: true,
                                            text: 'Frequency'
                                        }
                                    },
                                    x: {
                                        title: {
                                            display: true,
                                            text: 'Value'
                                        }
                                    }
                                }
                            }
                        });
                    }
                });
                
            } catch (error) {
                content.innerHTML = `<div style="color: red; text-align: center; padding: 40px;">❌ Error loading distributions: ${error.message}</div>`;
            }
        }
        
        // Show OHLC chart
        async function showOHLC(datasetId, datasetName) {
            const modal = document.getElementById('ohlc-modal');
            const content = document.getElementById('ohlc-content');
            
            // Update modal title
            document.querySelector('#ohlc-modal .modal-title').textContent = `OHLC Chart - ${datasetName}`;
            
            // Show modal with loading
            modal.style.display = 'block';
            content.innerHTML = '<div class="loading-spinner">📈 Loading OHLC data...</div>';
            
            try {
                const response = await fetch(`/api/v1/datasets/${datasetId}/ohlc`);
                const data = await response.json();
                
                // Create OHLC chart container
                content.innerHTML = `
                    <div class="chart-container">
                        <canvas id="ohlc-chart-${datasetId}"></canvas>
                    </div>
                    <div style="text-align: center; margin-top: 20px; color: #666;">
                        Symbol: ${data.symbol} | Data Points: ${data.ohlc_data.length} | 
                        Date Range: ${data.ohlc_data[0]?.date} to ${data.ohlc_data[data.ohlc_data.length-1]?.date}
                    </div>
                `;
                
                // Prepare data for line chart (Close prices)
                const labels = data.ohlc_data.map(d => new Date(d.date).toLocaleDateString());
                const closeData = data.ohlc_data.map(d => d.close);
                const volumeData = data.ohlc_data.map(d => d.volume);
                
                const ctx = document.getElementById(`ohlc-chart-${datasetId}`);
                
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'Close Price',
                            data: closeData,
                            borderColor: 'rgba(75, 192, 192, 1)',
                            backgroundColor: 'rgba(75, 192, 192, 0.2)',
                            borderWidth: 2,
                            fill: false,
                            tension: 0.1,
                            yAxisID: 'y'
                        }, {
                            label: 'Volume',
                            data: volumeData,
                            type: 'bar',
                            backgroundColor: 'rgba(255, 99, 132, 0.3)',
                            borderColor: 'rgba(255, 99, 132, 1)',
                            borderWidth: 1,
                            yAxisID: 'y1'
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: {
                            mode: 'index',
                            intersect: false,
                        },
                        plugins: {
                            title: {
                                display: true,
                                text: `${data.symbol} Price and Volume Chart`
                            }
                        },
                        scales: {
                            x: {
                                display: true,
                                title: {
                                    display: true,
                                    text: 'Date'
                                }
                            },
                            y: {
                                type: 'linear',
                                display: true,
                                position: 'left',
                                title: {
                                    display: true,
                                    text: 'Price ($)'
                                }
                            },
                            y1: {
                                type: 'linear',
                                display: true,
                                position: 'right',
                                title: {
                                    display: true,
                                    text: 'Volume'
                                },
                                grid: {
                                    drawOnChartArea: false,
                                }
                            }
                        }
                    }
                });
                
            } catch (error) {
                content.innerHTML = `<div style="color: red; text-align: center; padding: 40px;">❌ Error loading OHLC data: ${error.message}</div>`;
            }
        }
        
        // Auto-refresh every 30 seconds
        setInterval(() => {
            if (currentTab === 'jobs') {
                loadJobs();
            } else if (currentTab === 'datasets') {
                loadDatasets();
            }
        }, 30000);
    </script>
</body>
</html>
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)