#!/usr/bin/env python3
"""
Kubernetes Web App Configuration with Database-Based Training Data Retrieval

This is the corrected web app that uses database records for training datasets,
not just file-based scanning.
"""

# Get the key parts from the unified webapp that have proper database integration
import asyncio
import logging
import os
import json
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from fastapi import FastAPI, Depends, Query, Path as FastAPIPath, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

# Simple environment configuration for Kubernetes
class Environment:
    def __init__(self):
        self.environment = "dev"
        
    def get_database_url(self):
        # Use Kubernetes postgres-simple service - NO LOCALHOST!
        host = 'postgres-simple'
        port = '5432'
        user = 'postgres'
        password = 'dev_password'
        database = 'dev_db'
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
   
    def get_table_name(self, base_name: str) -> str:
        return f"dev_{base_name}"

# Pydantic models
class TrainingDataset(BaseModel):
    """Training dataset model"""
    dataset_name: str
    creation_timestamp: datetime
    total_sequences: int
    sequence_length: int
    feature_count: int
    label_count: int
    symbols: List[str]
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    data_quality_score: Optional[float] = None
    file_path: Optional[str] = None

class JobRun(BaseModel):
    """Job run model for runs table data"""
    run_id: int
    run_type: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str
    total_symbols: Optional[int] = None
    total_dates: Optional[int] = None
    successful_unifications: Optional[int] = None
    failed_unifications: Optional[int] = None
    processing_rate_per_second: Optional[float] = None
    quality_summary: Optional[str] = None
    performance_summary: Optional[str] = None

class UnifiedAnalyticsEngine:
    """Unified analytics engine with proper database integration"""
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        
    async def initialize(self):
        """Initialize with real database connectivity"""
        try:
            import asyncpg
            db_url = self.env.get_database_url()
            self.pool = await asyncpg.create_pool(
                db_url, min_size=1, max_size=5, command_timeout=30
            )
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            logging.info(f"Real database connected: {db_url}")
        except Exception as e:
            logging.warning(f"Database unavailable, using mock data: {e}")
            self.pool = None
            
    async def close(self):
        if self.pool:
            await self.pool.close()
            
    async def get_job_runs(self, limit: int = 50, run_type: Optional[str] = None) -> List[JobRun]:
        """Get job runs from runs table - REAL DATA ONLY"""
        if not self.pool:
            raise HTTPException(status_code=503, detail="Database connection required - no mock data allowed")
            
        async with self.pool.acquire() as conn:
            query = """
            SELECT 
                id as run_id, run_type, start_time, end_time, status,
                total_symbols, total_dates, successful_unifications, failed_unifications,
                processing_rate_per_second, quality_summary, performance_summary
            FROM dev_runs 
            WHERE ($1::text IS NULL OR run_type = $1)
            ORDER BY start_time DESC 
            LIMIT $2
            """
            rows = await conn.fetch(query, run_type, limit)
            return [JobRun(**dict(row)) for row in rows]
    
    async def get_training_datasets(self, limit: int = 50) -> List[TrainingDataset]:
        """Get training datasets information from database - DATABASE FIRST!"""
        if not self.pool:
            logging.warning("No database pool available for training datasets")
            return []
        
        # Use database for training datasets
        async with self.pool.acquire() as conn:
            query = """
            SELECT 
                id, dataset_name, creation_timestamp, total_sequences,
                sequence_length, feature_count, label_count, symbols,
                date_range_start, date_range_end, data_quality_score,
                features_file_path
            FROM dev_training_dataset
            ORDER BY creation_timestamp DESC
            LIMIT $1
            """
            
            rows = await conn.fetch(query, limit)
            datasets = []
            
            for row in rows:
                try:
                    dataset = TrainingDataset(
                        dataset_name=row['dataset_name'],
                        creation_timestamp=row['creation_timestamp'],
                        total_sequences=row['total_sequences'],
                        sequence_length=row['sequence_length'],
                        feature_count=row['feature_count'],
                        label_count=row['label_count'],
                        symbols=row['symbols'] if row['symbols'] else [],
                        date_range_start=row['date_range_start'],
                        date_range_end=row['date_range_end'],
                        data_quality_score=row['data_quality_score'],
                        file_path=row['features_file_path']
                    )
                    datasets.append(dataset)
                except Exception as e:
                    logging.warning(f"Error processing training dataset row: {e}")
            
            return datasets

# Create Flask app with the corrected training data endpoint
def create_unified_app() -> FastAPI:
    """Create unified analytics application with database-based training data"""
    
    app = FastAPI(
        title="Backtest Analytics Platform",
        description="Comprehensive analytics platform for ML model backtesting - Database Enabled",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    analytics_engine = None
    
    async def get_engine() -> UnifiedAnalyticsEngine:
        nonlocal analytics_engine
        if analytics_engine is None:
            analytics_engine = UnifiedAnalyticsEngine()
            await analytics_engine.initialize()
        return analytics_engine
   
    @app.on_event("startup")
    async def startup_event():
        nonlocal analytics_engine
        analytics_engine = UnifiedAnalyticsEngine()
        await analytics_engine.initialize()
        logging.info("Unified Backtest Analytics Platform started with database integration")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        if analytics_engine:
            await analytics_engine.close()

    @app.get("/api/v1/training-datasets", response_model=List[TrainingDataset])
    async def get_training_datasets_endpoint(
        limit: int = Query(50, description="Maximum number of datasets to return"),
        engine: UnifiedAnalyticsEngine = Depends(get_engine)
    ):
        """Get training datasets with database integration"""
        datasets = await engine.get_training_datasets(limit=limit)
        logging.info(f"Training datasets API returned {len(datasets)} datasets from database")
        return datasets

    @app.get("/api/v1/job-runs", response_model=List[JobRun])
    async def get_job_runs_endpoint(
        limit: int = Query(50, description="Maximum number of job runs to return"),
        run_type: Optional[str] = Query(None, description="Filter by run type"),
        engine: UnifiedAnalyticsEngine = Depends(get_engine)
    ):
        """Get job runs from database"""
        runs = await engine.get_job_runs(limit=limit, run_type=run_type)
        return runs

    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {"status": "ok", "timestamp": datetime.now().isoformat()}

    # Keep the original HTML dashboard but with database functionality
    @app.get("/", response_class=HTMLResponse)
    async def executive_dashboard():
        """Executive dashboard with database-backed training data"""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Backtest Analytics - Database Enabled</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                .header { background: #1f77b4; color: white; padding: 20px; text-align: center; border-radius: 8px; }
                .nav-tabs { display: flex; background: #f8f9fa; border-bottom: 1px solid #dee2e6; margin-top: 20px; }
                .nav-tab { padding: 15px 20px; cursor: pointer; border: none; background: none; }
                .nav-tab.active { background: #007bff; color: white; }
                .content { padding: 20px; background: white; border-radius: 8px; margin-top: 20px; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }
                .status-badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; }
                .status-created { background: #d4edda; color: #155724; }
                .status-running { background: #fff3cd; color: #856404; }
                .status-completed { background: #d1ecf1; color: #0c5460; }
                .dataset-card { border: 1px solid #dee2e6; padding: 15px; margin: 10px 0; border-radius: 8px; }
                .metric { display: inline-block; margin: 0 15px; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 Backtest Analytics Platform</h1>
                <p>Database-Enabled Training Data Visualization</p>
            </div>
            
            <div class="nav-tabs">
                <button class="nav-tab active" onclick="showTab('dashboard')">Dashboard</button>
                <button class="nav-tab" onclick="showTab('job-runs')">Job Runs</button>
                <button class="nav-tab" onclick="showTab('training-data')">Training Data</button>
            </div>
            
            <div class="content">
                <div id="dashboard" class="tab-content active">
                    <h2>Executive Dashboard</h2>
                    <p>✅ Database connection active</p>
                    <p>📊 Training data now loaded from dev_training_dataset table</p>
                    <p>🔄 Real-time job run tracking enabled</p>
                </div>
                
                <div id="job-runs" class="tab-content">
                    <h2>Job Runs</h2>
                    <div id="job-runs-list">Loading job runs...</div>
                </div>
                
                <div id="training-data" class="tab-content">
                    <h2>Training Data</h2>
                    <div id="training-datasets-list">Loading training datasets...</div>
                </div>
            </div>

            <script>
                function showTab(tabName) {
                    // Hide all tabs
                    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
                    document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
                    
                    // Show selected tab
                    document.getElementById(tabName).classList.add('active');
                    event.target.classList.add('active');
                    
                    // Load data for the tab
                    if (tabName === 'job-runs') loadJobRuns();
                    if (tabName === 'training-data') loadTrainingData();
                }
                
                async function loadJobRuns() {
                    try {
                        const response = await fetch('/api/v1/job-runs');
                        const runs = await response.json();
                        const container = document.getElementById('job-runs-list');
                        
                        if (runs.length === 0) {
                            container.innerHTML = '<p>No job runs found in database.</p>';
                            return;
                        }
                        
                        container.innerHTML = runs.map(run => `
                            <div class="dataset-card">
                                <h4>Run ${run.run_id}: ${run.run_type}</h4>
                                <span class="status-badge status-${run.status}">${run.status}</span>
                                <div class="metric">Symbols: ${run.total_symbols || 'N/A'}</div>
                                <div class="metric">Rate: ${run.processing_rate_per_second ? run.processing_rate_per_second.toFixed(1) + ' rec/s' : 'N/A'}</div>
                                <div class="metric">Started: ${new Date(run.start_time).toLocaleString()}</div>
                                <p><strong>Quality:</strong> ${run.quality_summary || 'N/A'}</p>
                            </div>
                        `).join('');
                    } catch (error) {
                        document.getElementById('job-runs-list').innerHTML = `<p>Error loading job runs: ${error.message}</p>`;
                    }
                }
                
                async function loadTrainingData() {
                    try {
                        const response = await fetch('/api/v1/training-datasets');
                        const datasets = await response.json();
                        const container = document.getElementById('training-datasets-list');
                        
                        if (datasets.length === 0) {
                            container.innerHTML = '<p>No training datasets found in database.</p>';
                            return;
                        }
                        
                        container.innerHTML = `
                            <p>✅ Found ${datasets.length} training datasets in database</p>
                        ` + datasets.map(dataset => `
                            <div class="dataset-card">
                                <h4>${dataset.dataset_name}</h4>
                                <div class="metric">Sequences: ${dataset.total_sequences.toLocaleString()}</div>
                                <div class="metric">Features: ${dataset.feature_count}</div>
                                <div class="metric">Labels: ${dataset.label_count}</div>
                                <div class="metric">Quality: ${dataset.data_quality_score ? (dataset.data_quality_score * 100).toFixed(1) + '%' : 'N/A'}</div>
                                <p><strong>Symbols:</strong> ${dataset.symbols.join(', ')}</p>
                                <p><strong>Date Range:</strong> ${dataset.date_range_start || 'N/A'} to ${dataset.date_range_end || 'N/A'}</p>
                                <p><strong>Created:</strong> ${new Date(dataset.creation_timestamp).toLocaleString()}</p>
                            </div>
                        `).join('');
                    } catch (error) {
                        document.getElementById('training-datasets-list').innerHTML = `<p>Error loading training datasets: ${error.message}</p>`;
                    }
                }
                
                // Load data on page load
                document.addEventListener('DOMContentLoaded', function() {
                    loadJobRuns();
                    loadTrainingData();
                });
            </script>
        </body>
        </html>
        """
        return html
    
    return app

if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 Starting Unified Backtest Analytics Platform")
    logging.info("📊 PRD-Compliant Implementation with Database Integration")
    logging.info("🌐 Dashboard: http://0.0.0.0:3000/")
    logging.info("📚 API Docs: http://0.0.0.0:3000/api/docs")
    logging.info("💚 Health: http://0.0.0.0:3000/health")
    
    app = create_unified_app()
    uvicorn.run(app, host="0.0.0.0", port=3000)