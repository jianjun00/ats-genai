#!/usr/bin/env python3
"""
Enhanced Unified Analytics Platform with Data Coverage Catalog

Extended analytics platform that includes:
1. Job Management Dashboard - Track Flyte jobs and metadata
2. Training Dataset Catalog - Browse and search datasets  
3. Dataset Comparison Engine - Statistical comparison between datasets
4. Data Coverage Catalog - Real-time price data coverage tracking (NEW)
5. Job-to-Dataset Navigation - End-to-end workflow tracking

The coverage catalog provides comprehensive visibility into price data
availability across all instruments, vendors, and time intervals.
"""

import asyncio
import logging
import os
import json
import uuid
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
import asyncpg
from scipy import stats
from scipy.spatial.distance import jensenshannon

# Import coverage modules
from src.coverage.coverage_api import CoverageAPI, create_coverage_api_routes

# ===== Environment Configuration =====
class Environment:
    def __init__(self):
        self.environment = "dev"
        
    def get_database_url(self):
        host = 'postgres-simple'
        port = '5432'
        user = 'postgres'
        password = 'dev_password'
        database = 'dev_db'
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
   
    def get_table_name(self, base_name: str) -> str:
        return f"dev_{base_name}"

# ===== Enhanced Analytics Engine with Coverage =====
class EnhancedAnalyticsEngine:
    """Enhanced analytics engine with integrated coverage tracking"""
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        self.coverage_api = None
        
    async def initialize(self):
        """Initialize with database connectivity and coverage integration"""
        try:
            db_url = self.env.get_database_url()
            self.pool = await asyncpg.create_pool(
                db_url, min_size=2, max_size=10, command_timeout=60
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            
            # Initialize coverage API
            self.coverage_api = CoverageAPI(self.pool)
            await self.coverage_api.initialize()
            
            logging.info(f"✅ Enhanced Analytics Engine with Coverage initialized: {db_url}")
        except Exception as e:
            logging.warning(f"❌ Database unavailable: {e}")
            self.pool = None
            self.coverage_api = None
            
    async def close(self):
        if self.pool:
            await self.pool.close()

def create_enhanced_analytics_app() -> FastAPI:
    """Create enhanced analytics web application with coverage catalog"""
    
    app = FastAPI(
        title="ATS Enhanced Analytics Platform with Coverage Catalog",
        description="Comprehensive ML workflow analytics with job management, dataset catalog, comparison engine, and real-time data coverage tracking",
        version="3.0.0",
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
    
    @app.on_event("startup")
    async def startup_event():
        nonlocal analytics_engine
        analytics_engine = EnhancedAnalyticsEngine()
        await analytics_engine.initialize()
        
        # Add coverage API routes
        if analytics_engine.coverage_api:
            create_coverage_api_routes(app, analytics_engine.coverage_api)
        
        logging.info("🚀 Enhanced Analytics Platform with Coverage started")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        if analytics_engine:
            await analytics_engine.close()
    
    # ===== Health Check =====
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "database": "connected" if analytics_engine.pool else "disconnected",
            "coverage_enabled": analytics_engine.coverage_api is not None
        }
    
    # ===== Enhanced Web Dashboard with Coverage =====
    @app.get("/", response_class=HTMLResponse)
    async def enhanced_analytics_dashboard():
        """Enhanced analytics platform dashboard with coverage catalog"""
        
        html = '''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>ATS Enhanced Analytics Platform</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body { 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                    background: #f5f7fa; 
                }
                .container { 
                    max-width: 1800px; margin: 0 auto; 
                    background: white; box-shadow: 0 4px 12px rgba(0,0,0,0.05); 
                }
                
                .header { 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; padding: 30px; text-align: center;
                }
                .header h1 { font-size: 2.5em; margin-bottom: 10px; }
                
                .nav-tabs {
                    display: flex; background: #f8f9fa; border-bottom: 2px solid #dee2e6;
                    padding: 0 30px; overflow-x: auto;
                }
                .nav-tab {
                    padding: 15px 20px; cursor: pointer; border: none; background: none;
                    font-weight: 500; color: #666; border-bottom: 3px solid transparent;
                    transition: all 0.3s; white-space: nowrap;
                }
                .nav-tab.active { color: #667eea; border-bottom-color: #667eea; }
                .nav-tab:hover { color: #667eea; background: rgba(102, 126, 234, 0.1); }
                
                .content { padding: 30px; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }
                
                .grid { 
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px; margin: 20px 0;
                }
                .card {
                    background: white; border: 1px solid #e9ecef; border-radius: 8px;
                    padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .card h3 { margin-bottom: 15px; color: #495057; }
                
                .coverage-summary-grid {
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 15px; margin: 20px 0;
                }
                .coverage-metric {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; padding: 20px; border-radius: 8px; text-align: center;
                }
                .coverage-metric .value { font-size: 2.5em; font-weight: bold; }
                .coverage-metric .label { font-size: 0.9em; opacity: 0.9; margin-top: 5px; }
                
                .coverage-status {
                    display: inline-block; padding: 4px 8px; border-radius: 4px;
                    font-size: 0.8em; font-weight: 500; margin: 2px;
                }
                .status-active { background: #d4edda; color: #155724; }
                .status-stale { background: #fff3cd; color: #856404; }
                .status-missing { background: #f8d7da; color: #721c24; }
                .status-degraded { background: #e2e3e5; color: #383d41; }
                
                .filters-panel {
                    background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px;
                    padding: 20px; margin-bottom: 20px;
                }
                .filter-group {
                    display: flex; gap: 15px; align-items: center; flex-wrap: wrap;
                    margin-bottom: 15px;
                }
                .filter-input {
                    padding: 8px 12px; border: 1px solid #ced4da; border-radius: 4px;
                    font-size: 14px;
                }
                
                .btn {
                    background: #667eea; color: white; border: none; padding: 10px 20px;
                    border-radius: 4px; cursor: pointer; font-weight: 500;
                    transition: all 0.3s; margin: 5px;
                }
                .btn:hover { background: #5a67d8; }
                .btn-secondary { background: #6c757d; }
                .btn-secondary:hover { background: #545b62; }
                
                .loading { text-align: center; padding: 40px; color: #6c757d; }
                .error {
                    background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px;
                    margin: 20px 0;
                }
                
                .heatmap-container {
                    background: white; border-radius: 8px; padding: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin: 20px 0;
                }
                
                .gap-list {
                    max-height: 400px; overflow-y: auto;
                }
                .gap-item {
                    padding: 10px; border-bottom: 1px solid #dee2e6; 
                    background: #f8f9fa; margin-bottom: 5px; border-radius: 4px;
                }
                .gap-severity-critical { border-left: 4px solid #dc3545; }
                .gap-severity-high { border-left: 4px solid #fd7e14; }
                .gap-severity-medium { border-left: 4px solid #ffc107; }
                .gap-severity-low { border-left: 4px solid #28a745; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 ATS Enhanced Analytics Platform</h1>
                    <p>Complete ML Workflow Analytics with Real-Time Data Coverage Tracking</p>
                </div>
                
                <div class="nav-tabs">
                    <button class="nav-tab active" onclick="showTab('jobs')">Job Management</button>
                    <button class="nav-tab" onclick="showTab('datasets')">Dataset Catalog</button>
                    <button class="nav-tab" onclick="showTab('comparison')">Dataset Comparison</button>
                    <button class="nav-tab" onclick="showTab('coverage')">📊 Data Coverage</button>
                    <button class="nav-tab" onclick="showTab('coverage-heatmap')">🔥 Coverage Heatmap</button>
                    <button class="nav-tab" onclick="showTab('coverage-gaps')">⚠️ Gap Analysis</button>
                    <button class="nav-tab" onclick="showTab('analytics')">Workflow Analytics</button>
                </div>
                
                <div class="content">
                    <!-- Job Management Tab (existing functionality) -->
                    <div id="jobs" class="tab-content active">
                        <h2>Job Management Dashboard</h2>
                        <p>Track Flyte jobs and ML workflow execution...</p>
                        <!-- Existing job management content would go here -->
                    </div>
                    
                    <!-- Dataset Catalog Tab (existing functionality) -->
                    <div id="datasets" class="tab-content">
                        <h2>Training Dataset Catalog</h2>
                        <p>Browse and search training datasets...</p>
                        <!-- Existing dataset catalog content would go here -->
                    </div>
                    
                    <!-- Dataset Comparison Tab (existing functionality) -->
                    <div id="comparison" class="tab-content">
                        <h2>Dataset Comparison Engine</h2>
                        <p>Statistical comparison between datasets...</p>
                        <!-- Existing comparison content would go here -->
                    </div>
                    
                    <!-- NEW: Data Coverage Catalog Tab -->
                    <div id="coverage" class="tab-content">
                        <h2>📊 Real-Time Data Coverage Catalog</h2>
                        
                        <!-- Coverage Summary Metrics -->
                        <div id="coverage-metrics" class="coverage-summary-grid">
                            <div class="coverage-metric">
                                <div class="value" id="total-symbols">-</div>
                                <div class="label">Tracked Symbols</div>
                            </div>
                            <div class="coverage-metric">
                                <div class="value" id="total-vendors">-</div>
                                <div class="label">Data Vendors</div>
                            </div>
                            <div class="coverage-metric">
                                <div class="value" id="avg-coverage">-</div>
                                <div class="label">Avg Coverage 24h</div>
                            </div>
                            <div class="coverage-metric">
                                <div class="value" id="active-feeds">-</div>
                                <div class="label">Active Feeds</div>
                            </div>
                        </div>
                        
                        <!-- Coverage Filters -->
                        <div class="filters-panel">
                            <h3>Coverage Filters</h3>
                            <div class="filter-group">
                                <label>Symbols:</label>
                                <input type="text" id="coverage-symbols" class="filter-input" placeholder="AAPL,TSLA,MSFT">
                                
                                <label>Vendors:</label>
                                <select id="coverage-vendors" class="filter-input" multiple>
                                    <option value="polygon">Polygon</option>
                                    <option value="tiingo">Tiingo</option>
                                    <option value="fmp">FMP</option>
                                    <option value="alphavantage">Alpha Vantage</option>
                                </select>
                                
                                <label>Data Type:</label>
                                <select id="coverage-data-type" class="filter-input">
                                    <option value="minute">Minute Bars</option>
                                    <option value="daily">Daily Prices</option>
                                </select>
                                
                                <button class="btn" onclick="loadCoverageSummary()">Apply Filters</button>
                                <button class="btn btn-secondary" onclick="clearCoverageFilters()">Clear</button>
                            </div>
                        </div>
                        
                        <!-- Coverage Summary Table -->
                        <div id="coverage-loading" class="loading">Loading coverage data...</div>
                        <div id="coverage-error" class="error" style="display: none;"></div>
                        <div class="card">
                            <h3>Coverage Summary by Symbol/Vendor</h3>
                            <div style="max-height: 500px; overflow-y: auto;">
                                <table id="coverage-table" class="data-table" style="display: none; width: 100%; border-collapse: collapse;">
                                    <thead></thead>
                                    <tbody></tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                    
                    <!-- NEW: Coverage Heatmap Tab -->
                    <div id="coverage-heatmap" class="tab-content">
                        <h2>🔥 Coverage Heatmap Visualization</h2>
                        
                        <div class="filters-panel">
                            <h3>Heatmap Configuration</h3>
                            <div class="filter-group">
                                <label>Symbols:</label>
                                <input type="text" id="heatmap-symbols" class="filter-input" 
                                       value="AAPL,TSLA,MSFT,GOOGL,AMZN" placeholder="Comma-separated symbols">
                                
                                <label>Vendors:</label>
                                <input type="text" id="heatmap-vendors" class="filter-input" 
                                       value="polygon,tiingo,fmp" placeholder="Comma-separated vendors">
                                
                                <label>Start Date:</label>
                                <input type="date" id="heatmap-start-date" class="filter-input">
                                
                                <label>End Date:</label>
                                <input type="date" id="heatmap-end-date" class="filter-input">
                                
                                <button class="btn" onclick="generateCoverageHeatmap()">Generate Heatmap</button>
                            </div>
                        </div>
                        
                        <div id="heatmap-loading" class="loading" style="display: none;">Generating heatmap...</div>
                        <div id="heatmap-error" class="error" style="display: none;"></div>
                        <div id="coverage-heatmap-container" class="heatmap-container">
                            <div id="coverage-heatmap-plot" style="height: 600px;"></div>
                        </div>
                    </div>
                    
                    <!-- NEW: Gap Analysis Tab -->
                    <div id="coverage-gaps" class="tab-content">
                        <h2>⚠️ Coverage Gap Analysis</h2>
                        
                        <div class="filters-panel">
                            <h3>Gap Filters</h3>
                            <div class="filter-group">
                                <label>Symbol:</label>
                                <input type="text" id="gaps-symbol" class="filter-input" placeholder="AAPL">
                                
                                <label>Vendor:</label>
                                <select id="gaps-vendor" class="filter-input">
                                    <option value="">All Vendors</option>
                                    <option value="polygon">Polygon</option>
                                    <option value="tiingo">Tiingo</option>
                                    <option value="fmp">FMP</option>
                                </select>
                                
                                <label>Severity:</label>
                                <select id="gaps-severity" class="filter-input">
                                    <option value="">All Severities</option>
                                    <option value="critical">Critical</option>
                                    <option value="high">High</option>
                                    <option value="medium">Medium</option>
                                    <option value="low">Low</option>
                                </select>
                                
                                <label>Resolved:</label>
                                <select id="gaps-resolved" class="filter-input">
                                    <option value="">All Gaps</option>
                                    <option value="false">Unresolved Only</option>
                                    <option value="true">Resolved Only</option>
                                </select>
                                
                                <button class="btn" onclick="loadCoverageGaps()">Load Gaps</button>
                                <button class="btn btn-secondary" onclick="refreshGaps()">Refresh</button>
                            </div>
                        </div>
                        
                        <div id="gaps-loading" class="loading" style="display: none;">Loading coverage gaps...</div>
                        <div id="gaps-error" class="error" style="display: none;"></div>
                        
                        <div class="card">
                            <h3>Recent Coverage Gaps</h3>
                            <div id="gaps-list" class="gap-list">
                                <!-- Gap items will be populated here -->
                            </div>
                        </div>
                    </div>
                    
                    <!-- Workflow Analytics Tab (existing functionality) -->
                    <div id="analytics" class="tab-content">
                        <h2>Workflow Analytics</h2>
                        <p>End-to-end workflow analysis...</p>
                        <!-- Existing analytics content would go here -->
                    </div>
                </div>
            </div>

            <script>
                // Global variables
                let currentCoverageData = [];
                let currentGapsData = [];
                
                // Initialize default date range for heatmap
                document.addEventListener('DOMContentLoaded', function() {
                    const today = new Date();
                    const weekAgo = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
                    
                    document.getElementById('heatmap-end-date').value = today.toISOString().split('T')[0];
                    document.getElementById('heatmap-start-date').value = weekAgo.toISOString().split('T')[0];
                    
                    // Load initial data
                    loadCoverageSummary();
                });
                
                // Tab switching
                function showTab(tabName) {
                    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
                    document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
                    
                    document.getElementById(tabName).classList.add('active');
                    event.target.classList.add('active');
                    
                    // Load data for coverage tabs
                    if (tabName === 'coverage') loadCoverageSummary();
                    if (tabName === 'coverage-gaps') loadCoverageGaps();
                }
                
                // Coverage Summary Functions
                async function loadCoverageSummary() {
                    const loading = document.getElementById('coverage-loading');
                    const error = document.getElementById('coverage-error');
                    const table = document.getElementById('coverage-table');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    table.style.display = 'none';
                    
                    try {
                        // Build query parameters
                        const params = new URLSearchParams();
                        const symbols = document.getElementById('coverage-symbols').value;
                        const vendors = Array.from(document.getElementById('coverage-vendors').selectedOptions)
                                            .map(option => option.value).join(',');
                        const dataType = document.getElementById('coverage-data-type').value;
                        
                        if (symbols) params.append('symbols', symbols);
                        if (vendors) params.append('vendors', vendors);
                        if (dataType) params.append('data_types', dataType);
                        
                        const response = await fetch(`/api/v1/coverage/summary?${params}`);
                        const data = await response.json();
                        
                        currentCoverageData = data.summary;
                        loading.style.display = 'none';
                        
                        // Update summary metrics
                        updateCoverageMetrics(data.summary);
                        
                        // Create coverage table
                        renderCoverageTable(data.summary);
                        table.style.display = 'table';
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading coverage data: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                function updateCoverageMetrics(summaryData) {
                    const uniqueSymbols = new Set(summaryData.map(item => item.symbol));
                    const uniqueVendors = new Set(summaryData.map(item => item.vendor));
                    const avgCoverage = summaryData.length > 0 ? 
                        summaryData.reduce((sum, item) => sum + item.coverage_24h, 0) / summaryData.length : 0;
                    const activeFeeds = summaryData.filter(item => item.current_status === 'active').length;
                    
                    document.getElementById('total-symbols').textContent = uniqueSymbols.size;
                    document.getElementById('total-vendors').textContent = uniqueVendors.size;
                    document.getElementById('avg-coverage').textContent = avgCoverage.toFixed(1) + '%';
                    document.getElementById('active-feeds').textContent = activeFeeds;
                }
                
                function renderCoverageTable(data) {
                    const table = document.getElementById('coverage-table');
                    const thead = table.querySelector('thead');
                    const tbody = table.querySelector('tbody');
                    
                    // Create table headers
                    thead.innerHTML = `
                        <tr>
                            <th>Symbol</th>
                            <th>Vendor</th>
                            <th>Status</th>
                            <th>Coverage 24h</th>
                            <th>Quality 24h</th>
                            <th>Gaps 24h</th>
                            <th>Latest Data</th>
                            <th>Trend</th>
                        </tr>
                    `;
                    
                    // Create table rows
                    tbody.innerHTML = data.map(item => `
                        <tr>
                            <td><strong>${item.symbol}</strong></td>
                            <td>${item.vendor}</td>
                            <td><span class="coverage-status status-${item.current_status}">${item.current_status}</span></td>
                            <td>${item.coverage_24h.toFixed(1)}%</td>
                            <td>${item.quality_24h ? item.quality_24h.toFixed(3) : 'N/A'}</td>
                            <td>${item.gaps_24h}</td>
                            <td>${item.latest_data_time ? new Date(item.latest_data_time).toLocaleString() : 'N/A'}</td>
                            <td>${item.coverage_trend || 'stable'}</td>
                        </tr>
                    `).join('');
                }
                
                // Coverage Heatmap Functions
                async function generateCoverageHeatmap() {
                    const loading = document.getElementById('heatmap-loading');
                    const error = document.getElementById('heatmap-error');
                    const container = document.getElementById('coverage-heatmap-plot');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    
                    try {
                        const symbols = document.getElementById('heatmap-symbols').value;
                        const vendors = document.getElementById('heatmap-vendors').value;
                        const startDate = document.getElementById('heatmap-start-date').value;
                        const endDate = document.getElementById('heatmap-end-date').value;
                        
                        const params = new URLSearchParams({
                            symbols: symbols,
                            vendors: vendors,
                            start_date: startDate,
                            end_date: endDate,
                            data_type: 'minute'
                        });
                        
                        const response = await fetch(`/api/v1/coverage/heatmap?${params}`);
                        const data = await response.json();
                        
                        loading.style.display = 'none';
                        
                        // Create heatmap visualization
                        renderCoverageHeatmap(data, container);
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error generating heatmap: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                function renderCoverageHeatmap(data, container) {
                    // Prepare data for Plotly heatmap
                    const zData = [];
                    const yLabels = [];
                    
                    for (let s = 0; s < data.symbols.length; s++) {
                        for (let v = 0; v < data.vendors.length; v++) {
                            yLabels.push(`${data.symbols[s]}/${data.vendors[v]}`);
                            zData.push(data.coverage_matrix[s][v]);
                        }
                    }
                    
                    const trace = {
                        z: zData,
                        x: data.time_periods,
                        y: yLabels,
                        type: 'heatmap',
                        colorscale: [
                            [0, '#dc3545'],    // Red for low coverage
                            [0.5, '#ffc107'],  // Yellow for medium coverage
                            [1, '#28a745']     // Green for high coverage
                        ],
                        zmin: 0,
                        zmax: 100,
                        colorbar: {
                            title: 'Coverage %'
                        }
                    };
                    
                    const layout = {
                        title: 'Data Coverage Heatmap',
                        xaxis: { title: 'Date' },
                        yaxis: { title: 'Symbol/Vendor' },
                        margin: { l: 150, r: 50, t: 50, b: 50 }
                    };
                    
                    Plotly.newPlot(container, [trace], layout, {responsive: true});
                }
                
                // Coverage Gaps Functions
                async function loadCoverageGaps() {
                    const loading = document.getElementById('gaps-loading');
                    const error = document.getElementById('gaps-error');
                    const gapsList = document.getElementById('gaps-list');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    
                    try {
                        const params = new URLSearchParams();
                        const symbol = document.getElementById('gaps-symbol').value;
                        const vendor = document.getElementById('gaps-vendor').value;
                        const severity = document.getElementById('gaps-severity').value;
                        const resolved = document.getElementById('gaps-resolved').value;
                        
                        if (symbol) params.append('symbol', symbol);
                        if (vendor) params.append('vendor', vendor);
                        if (severity) params.append('severity', severity);
                        if (resolved) params.append('resolved', resolved);
                        
                        const response = await fetch(`/api/v1/coverage/gaps?${params}`);
                        const data = await response.json();
                        
                        currentGapsData = data.gaps;
                        loading.style.display = 'none';
                        
                        // Render gaps list
                        renderGapsList(data.gaps, gapsList);
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading coverage gaps: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                function renderGapsList(gaps, container) {
                    if (gaps.length === 0) {
                        container.innerHTML = '<p>No coverage gaps found with current filters.</p>';
                        return;
                    }
                    
                    container.innerHTML = gaps.map(gap => `
                        <div class="gap-item gap-severity-${gap.gap_severity}">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div>
                                    <strong>${gap.symbol}/${gap.vendor}</strong>
                                    <span class="coverage-status status-${gap.gap_severity}">${gap.gap_severity}</span>
                                </div>
                                <div>
                                    ${gap.gap_duration_minutes} minutes
                                    ${gap.is_resolved ? '<span class="coverage-status status-active">Resolved</span>' : ''}
                                </div>
                            </div>
                            <div style="margin-top: 5px; font-size: 0.9em; color: #666;">
                                <div>Gap: ${new Date(gap.gap_start).toLocaleString()} → ${new Date(gap.gap_end).toLocaleString()}</div>
                                <div>Type: ${gap.gap_type} | Detection: ${gap.detection_method} | Confidence: ${(gap.detection_confidence * 100).toFixed(0)}%</div>
                            </div>
                        </div>
                    `).join('');
                }
                
                // Utility functions
                function clearCoverageFilters() {
                    document.getElementById('coverage-symbols').value = '';
                    document.getElementById('coverage-vendors').selectedIndex = -1;
                    document.getElementById('coverage-data-type').value = 'minute';
                    loadCoverageSummary();
                }
                
                function refreshGaps() {
                    loadCoverageGaps();
                }
                
                // Auto-refresh coverage data every 60 seconds
                setInterval(() => {
                    if (document.querySelector('.tab-content.active').id === 'coverage') {
                        loadCoverageSummary();
                    }
                }, 60000);
            </script>
        </body>
        </html>
        '''
        return html
    
    return app

if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 Starting ATS Enhanced Analytics Platform with Coverage Catalog")
    logging.info("📊 Features: Job Management, Dataset Catalog, Comparison Engine, Data Coverage Tracking")
    logging.info("🌐 Dashboard: http://0.0.0.0:5000/")
    logging.info("📚 API Docs: http://0.0.0.0:5000/api/docs")
    
    app = create_enhanced_analytics_app()
    uvicorn.run(app, host="0.0.0.0", port=5000)