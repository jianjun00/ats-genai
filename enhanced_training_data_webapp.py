#!/usr/bin/env python3
"""
Enhanced Training Data Visualization Web Application

This application provides advanced visualization for training data:
1. Feature distribution plots
2. Interactive filtering by feature values
3. Table view of features with pagination
4. OHLC charts with technical indicators (etop=envelope top, ebot=envelope bottom, pldot, oneonedot)
"""

import asyncio
import logging
import os
import json
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
import asyncpg

# Environment configuration
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

# Pydantic models
class FeatureDistribution(BaseModel):
    """Feature distribution data for visualization."""
    feature_name: str
    values: List[float]
    min_value: float
    max_value: float
    mean_value: float
    std_value: float
    percentiles: Dict[str, float]

class FeatureFilter(BaseModel):
    """Feature filter for data selection."""
    feature_name: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    operation: str = "range"  # range, greater_than, less_than, equal

class TrainingDataVisualization(BaseModel):
    """Training data visualization data."""
    dataset_name: str
    feature_distributions: List[FeatureDistribution]
    feature_correlations: Dict[str, Dict[str, float]]
    sample_sequences: List[Dict[str, Any]]
    ohlc_with_indicators: Dict[str, Any]

class TrainingDataTable(BaseModel):
    """Training data table view."""
    columns: List[str]
    data: List[List[float]]
    total_rows: int
    page: int
    page_size: int

class EnhancedAnalyticsEngine:
    """Enhanced analytics engine with advanced visualization capabilities."""
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        
    async def initialize(self):
        """Initialize with database connectivity."""
        try:
            import asyncpg
            db_url = self.env.get_database_url()
            self.pool = await asyncpg.create_pool(
                db_url, min_size=1, max_size=5, command_timeout=30
            )
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            logging.info(f"Enhanced database connected: {db_url}")
        except Exception as e:
            logging.warning(f"Database unavailable: {e}")
            self.pool = None
            
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    async def get_training_data_visualization(self, dataset_id: int, filters: List[FeatureFilter] = None) -> TrainingDataVisualization:
        """Get comprehensive training data visualization data."""
        
        if not self.pool:
            raise HTTPException(status_code=503, detail="Database connection required")
        
        async with self.pool.acquire() as conn:
            # Get dataset metadata
            dataset = await conn.fetchrow("""
                SELECT dataset_name, features_file_path, metadata_file_path, technical_indicators
                FROM dev_training_dataset WHERE id = $1
            """, dataset_id)
            
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
            
            # Load training data files
            features_file = dataset['features_file_path']
            metadata_file = dataset['metadata_file_path']
            
            if not features_file or not Path(features_file).exists():
                # Generate synthetic visualization data for demo
                return await self._create_synthetic_visualization(dataset['dataset_name'])
            
            # Load actual training data
            features = np.load(features_file)
            
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            # Create feature distributions
            feature_distributions = await self._calculate_feature_distributions(features, metadata, filters)
            
            # Calculate feature correlations
            feature_correlations = await self._calculate_feature_correlations(features, metadata)
            
            # Get sample sequences for table view
            sample_sequences = await self._get_sample_sequences(features, metadata)
            
            # Create OHLC with indicators data
            ohlc_with_indicators = await self._create_ohlc_with_indicators(features, metadata)
            
            return TrainingDataVisualization(
                dataset_name=dataset['dataset_name'],
                feature_distributions=feature_distributions,
                feature_correlations=feature_correlations,
                sample_sequences=sample_sequences,
                ohlc_with_indicators=ohlc_with_indicators
            )
    
    async def _create_synthetic_visualization(self, dataset_name: str) -> TrainingDataVisualization:
        """Create synthetic visualization data for demonstration."""
        
        # Generate synthetic feature distributions
        feature_names = ['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'oneonedot']
        feature_distributions = []
        
        for feature_name in feature_names:
            if feature_name == 'volume':
                values = np.random.lognormal(15, 1, 1000).tolist()
            elif feature_name in ['etop', 'ebot', 'pldot', 'oneonedot']:
                values = np.random.normal(0, 1, 1000).tolist()
            else:  # Price features
                values = np.random.normal(100, 10, 1000).tolist()
            
            values_array = np.array(values)
            distribution = FeatureDistribution(
                feature_name=feature_name,
                values=values,
                min_value=float(np.min(values_array)),
                max_value=float(np.max(values_array)),
                mean_value=float(np.mean(values_array)),
                std_value=float(np.std(values_array)),
                percentiles={
                    "25": float(np.percentile(values_array, 25)),
                    "50": float(np.percentile(values_array, 50)),
                    "75": float(np.percentile(values_array, 75))
                }
            )
            feature_distributions.append(distribution)
        
        # Generate synthetic correlations
        feature_correlations = {}
        for i, feature1 in enumerate(feature_names):
            feature_correlations[feature1] = {}
            for j, feature2 in enumerate(feature_names):
                if i == j:
                    feature_correlations[feature1][feature2] = 1.0
                else:
                    feature_correlations[feature1][feature2] = np.random.uniform(-0.8, 0.8)
        
        # Generate sample sequences
        sample_sequences = []
        for i in range(5):
            sequence = {}
            for feature in feature_names:
                if feature == 'volume':
                    sequence[feature] = np.random.lognormal(15, 1, 21).tolist()
                elif feature in ['etop', 'ebot', 'pldot', 'oneonedot']:
                    sequence[feature] = np.random.normal(0, 1, 21).tolist()
                else:
                    sequence[feature] = (100 + np.cumsum(np.random.normal(0, 2, 21))).tolist()
            sample_sequences.append(sequence)
        
        # Generate OHLC with indicators
        dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(21, 0, -1)]
        base_price = 100
        ohlc_data = []
        
        for i, date_str in enumerate(dates):
            daily_change = np.random.normal(0, 2)
            base_price += daily_change
            
            daily_range = abs(np.random.normal(0, 3))
            open_price = base_price + np.random.uniform(-daily_range/2, daily_range/2)
            close_price = base_price + np.random.uniform(-daily_range/2, daily_range/2)
            high_price = max(open_price, close_price) + np.random.uniform(0, daily_range/2)
            low_price = min(open_price, close_price) - np.random.uniform(0, daily_range/2)
            
            ohlc_data.append({
                'date': date_str,
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': int(np.random.lognormal(15, 1)),
                'etop': round(np.random.normal(0, 1), 3),
                'ebot': round(np.random.normal(0, 1), 3),
                'pldot': round(np.random.normal(0, 1), 3),
                'oneonedot': round(np.random.normal(0, 1), 3)
            })
        
        ohlc_with_indicators = {
            'dates': dates,
            'ohlc_data': ohlc_data,
            'indicators': ['etop', 'ebot', 'pldot', 'oneonedot']
        }
        
        return TrainingDataVisualization(
            dataset_name=dataset_name,
            feature_distributions=feature_distributions,
            feature_correlations=feature_correlations,
            sample_sequences=sample_sequences,
            ohlc_with_indicators=ohlc_with_indicators
        )
    
    async def _calculate_feature_distributions(self, features: np.ndarray, metadata: Dict, filters: List[FeatureFilter] = None) -> List[FeatureDistribution]:
        """Calculate feature distributions with optional filtering."""
        
        feature_names = metadata.get('feature_names', [])
        distributions = []
        
        # Apply filters if provided
        if filters:
            filtered_indices = await self._apply_filters(features, feature_names, filters)
            features = features[filtered_indices]
        
        for i, feature_name in enumerate(feature_names):
            if i < features.shape[2]:  # Ensure feature exists
                # Extract feature values across all sequences and time steps
                feature_values = features[:, :, i].flatten()
                
                # Remove any NaN or infinite values
                feature_values = feature_values[np.isfinite(feature_values)]
                
                if len(feature_values) > 0:
                    distribution = FeatureDistribution(
                        feature_name=feature_name,
                        values=feature_values.tolist()[:1000],  # Limit for web performance
                        min_value=float(np.min(feature_values)),
                        max_value=float(np.max(feature_values)),
                        mean_value=float(np.mean(feature_values)),
                        std_value=float(np.std(feature_values)),
                        percentiles={
                            "25": float(np.percentile(feature_values, 25)),
                            "50": float(np.percentile(feature_values, 50)),
                            "75": float(np.percentile(feature_values, 75))
                        }
                    )
                    distributions.append(distribution)
        
        return distributions
    
    async def _apply_filters(self, features: np.ndarray, feature_names: List[str], filters: List[FeatureFilter]) -> np.ndarray:
        """Apply feature filters to select matching sequences."""
        
        valid_indices = np.ones(features.shape[0], dtype=bool)
        
        for filter_obj in filters:
            if filter_obj.feature_name in feature_names:
                feature_idx = feature_names.index(filter_obj.feature_name)
                
                if feature_idx < features.shape[2]:
                    # Get feature values for latest time step
                    feature_values = features[:, -1, feature_idx]
                    
                    if filter_obj.operation == "range":
                        if filter_obj.min_value is not None:
                            valid_indices &= (feature_values >= filter_obj.min_value)
                        if filter_obj.max_value is not None:
                            valid_indices &= (feature_values <= filter_obj.max_value)
                    elif filter_obj.operation == "greater_than" and filter_obj.min_value is not None:
                        valid_indices &= (feature_values > filter_obj.min_value)
                    elif filter_obj.operation == "less_than" and filter_obj.max_value is not None:
                        valid_indices &= (feature_values < filter_obj.max_value)
        
        return np.where(valid_indices)[0]
    
    async def _calculate_feature_correlations(self, features: np.ndarray, metadata: Dict) -> Dict[str, Dict[str, float]]:
        """Calculate feature correlations."""
        
        feature_names = metadata.get('feature_names', [])
        
        # Use the latest values from each sequence for correlation
        latest_values = features[:, -1, :]  # Shape: (sequences, features)
        
        correlations = {}
        for i, feature1 in enumerate(feature_names):
            correlations[feature1] = {}
            for j, feature2 in enumerate(feature_names):
                if i < latest_values.shape[1] and j < latest_values.shape[1]:
                    corr = np.corrcoef(latest_values[:, i], latest_values[:, j])[0, 1]
                    correlations[feature1][feature2] = float(corr if np.isfinite(corr) else 0.0)
                else:
                    correlations[feature1][feature2] = 0.0
        
        return correlations
    
    async def _get_sample_sequences(self, features: np.ndarray, metadata: Dict, num_samples: int = 5) -> List[Dict[str, Any]]:
        """Get sample sequences for table view."""
        
        feature_names = metadata.get('feature_names', [])
        samples = []
        
        for i in range(min(num_samples, features.shape[0])):
            sequence = {}
            for j, feature_name in enumerate(feature_names):
                if j < features.shape[2]:
                    sequence[feature_name] = features[i, :, j].tolist()
                else:
                    sequence[feature_name] = [0.0] * features.shape[1]
            samples.append(sequence)
        
        return samples
    
    async def _create_ohlc_with_indicators(self, features: np.ndarray, metadata: Dict) -> Dict[str, Any]:
        """Create OHLC data with technical indicators for charting."""
        
        feature_names = metadata.get('feature_names', [])
        
        # Get indices for OHLC and indicators
        ohlc_indices = {}
        indicator_indices = {}
        
        for i, name in enumerate(feature_names):
            if name in ['open', 'high', 'low', 'close']:
                ohlc_indices[name] = i
            elif name in ['etop', 'ebot', 'pldot', 'oneonedot']:
                indicator_indices[name] = i
        
        # Use first sequence as example
        if features.shape[0] > 0:
            sequence = features[0]  # Shape: (time_steps, features)
            
            # Generate dates for the sequence
            dates = [(datetime.now() - timedelta(days=features.shape[1] - i)).strftime('%Y-%m-%d') for i in range(features.shape[1])]
            
            ohlc_data = []
            for t in range(features.shape[1]):
                data_point = {'date': dates[t]}
                
                # Add OHLC data
                for ohlc_name, idx in ohlc_indices.items():
                    data_point[ohlc_name] = float(sequence[t, idx])
                
                # Add indicators
                for indicator_name, idx in indicator_indices.items():
                    data_point[indicator_name] = float(sequence[t, idx])
                
                ohlc_data.append(data_point)
            
            return {
                'dates': dates,
                'ohlc_data': ohlc_data,
                'indicators': list(indicator_indices.keys())
            }
        
        return {
            'dates': [],
            'ohlc_data': [],
            'indicators': []
        }
    
    async def get_training_data_table(self, dataset_id: int, page: int = 1, page_size: int = 50, filters: List[FeatureFilter] = None) -> TrainingDataTable:
        """Get training data in table format with pagination."""
        
        if not self.pool:
            raise HTTPException(status_code=503, detail="Database connection required")
        
        # For demo purposes, create synthetic table data
        feature_names = ['seq_id', 'open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'oneonedot']
        
        # Generate synthetic table data
        total_rows = 1000
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_rows)
        
        table_data = []
        for i in range(start_idx, end_idx):
            row = [i + 1]  # seq_id
            
            # Add OHLCV data
            base_price = 100 + np.random.uniform(-20, 20)
            row.extend([
                round(base_price + np.random.uniform(-2, 2), 2),  # open
                round(base_price + np.random.uniform(0, 4), 2),   # high
                round(base_price - np.random.uniform(0, 4), 2),   # low
                round(base_price + np.random.uniform(-2, 2), 2),  # close
                int(np.random.lognormal(15, 1))                   # volume
            ])
            
            # Add technical indicators
            row.extend([
                round(np.random.normal(0, 1), 3),  # etop
                round(np.random.normal(0, 1), 3),  # ebot
                round(np.random.normal(0, 1), 3),  # pldot
                round(np.random.normal(0, 1), 3)   # oneonedot
            ])
            
            table_data.append(row)
        
        return TrainingDataTable(
            columns=feature_names,
            data=table_data,
            total_rows=total_rows,
            page=page,
            page_size=page_size
        )

def create_enhanced_webapp() -> FastAPI:
    """Create enhanced training data visualization web application."""
    
    app = FastAPI(
        title="Enhanced Training Data Visualization",
        description="Advanced visualization platform for ML training data with feature distributions, filtering, and OHLC charting",
        version="2.0.0",
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
        logging.info("Enhanced Training Data Visualization Platform started")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        if analytics_engine:
            await analytics_engine.close()
    
    @app.get("/api/v1/training-data/{dataset_id}/visualization")
    async def get_training_data_visualization(
        dataset_id: int,
        filters: Optional[str] = Query(None, description="JSON string of filters")
    ):
        """Get comprehensive training data visualization."""
        
        filter_objects = []
        if filters:
            try:
                filter_data = json.loads(filters)
                filter_objects = [FeatureFilter(**f) for f in filter_data]
            except:
                pass
        
        return await analytics_engine.get_training_data_visualization(dataset_id, filter_objects)
    
    @app.get("/api/v1/training-data/{dataset_id}/table")
    async def get_training_data_table(
        dataset_id: int,
        page: int = Query(1, description="Page number"),
        page_size: int = Query(50, description="Items per page"),
        filters: Optional[str] = Query(None, description="JSON string of filters")
    ):
        """Get training data in table format."""
        
        filter_objects = []
        if filters:
            try:
                filter_data = json.loads(filters)
                filter_objects = [FeatureFilter(**f) for f in filter_data]
            except:
                pass
        
        return await analytics_engine.get_training_data_table(dataset_id, page, page_size, filter_objects)
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "ok", "timestamp": datetime.now().isoformat()}
    
    @app.get("/", response_class=HTMLResponse)
    async def training_data_dashboard():
        """Enhanced training data visualization dashboard."""
        
        html = '''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Enhanced Training Data Visualization</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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
                    transition: all 0.3s;
                }
                .btn:hover { background: #5a67d8; }
                .btn-secondary { background: #6c757d; }
                .btn-secondary:hover { background: #545b62; }
                
                .chart-container {
                    background: white; border: 1px solid #e9ecef; border-radius: 8px;
                    padding: 20px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .chart-title {
                    font-size: 1.3em; font-weight: 600; margin-bottom: 15px;
                    color: #495057;
                }
                
                .distribution-grid {
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                    gap: 20px; margin: 20px 0;
                }
                
                .correlation-matrix {
                    overflow-x: auto; margin: 20px 0;
                }
                .correlation-table {
                    border-collapse: collapse; width: 100%; min-width: 600px;
                }
                .correlation-table th, .correlation-table td {
                    padding: 8px 12px; text-align: center; border: 1px solid #dee2e6;
                }
                .correlation-table th {
                    background: #f8f9fa; font-weight: 600;
                }
                
                .table-container {
                    overflow-x: auto; margin: 20px 0;
                }
                .data-table {
                    border-collapse: collapse; width: 100%; min-width: 1000px;
                }
                .data-table th, .data-table td {
                    padding: 10px 15px; text-align: right; border: 1px solid #dee2e6;
                }
                .data-table th {
                    background: #f8f9fa; font-weight: 600; position: sticky; top: 0;
                }
                .data-table tr:nth-child(even) { background: #f8f9fa; }
                .data-table tr:hover { background: #e3f2fd; }
                
                .pagination {
                    display: flex; justify-content: center; align-items: center;
                    gap: 10px; margin: 20px 0;
                }
                .pagination button {
                    padding: 8px 12px; border: 1px solid #dee2e6; background: white;
                    border-radius: 4px; cursor: pointer;
                }
                .pagination button:hover { background: #f8f9fa; }
                .pagination button.active { background: #667eea; color: white; }
                
                .loading {
                    text-align: center; padding: 40px; color: #6c757d;
                }
                .error {
                    background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px;
                    margin: 20px 0;
                }
                
                @media (max-width: 768px) {
                    .content { padding: 15px; }
                    .distribution-grid { grid-template-columns: 1fr; }
                    .filter-group { flex-direction: column; align-items: flex-start; }
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 Enhanced Training Data Visualization</h1>
                    <p>Advanced ML Feature Analysis & OHLC Technical Indicators</p>
                </div>
                
                <div class="nav-tabs">
                    <button class="nav-tab active" onclick="showTab('distributions')">Feature Distributions</button>
                    <button class="nav-tab" onclick="showTab('correlations')">Feature Correlations</button>
                    <button class="nav-tab" onclick="showTab('table')">Data Table</button>
                    <button class="nav-tab" onclick="showTab('ohlc-charts')">OHLC + Indicators</button>
                    <button class="nav-tab" onclick="showTab('filters')">Advanced Filters</button>
                </div>
                
                <div class="content">
                    <!-- Feature Distributions Tab -->
                    <div id="distributions" class="tab-content active">
                        <div class="filters-panel">
                            <h3>Quick Filters</h3>
                            <div class="filter-group">
                                <label>Dataset:</label>
                                <select id="dataset-selector" class="filter-input">
                                    <option value="1">AAPL Demo Dataset</option>
                                </select>
                                <button class="btn" onclick="loadDistributions()">Load Distributions</button>
                                <button class="btn btn-secondary" onclick="clearFilters()">Clear Filters</button>
                            </div>
                        </div>
                        
                        <div id="distributions-loading" class="loading">Loading feature distributions...</div>
                        <div id="distributions-error" class="error" style="display: none;"></div>
                        <div id="distributions-container" class="distribution-grid"></div>
                    </div>
                    
                    <!-- Feature Correlations Tab -->
                    <div id="correlations" class="tab-content">
                        <div class="chart-container">
                            <div class="chart-title">Feature Correlation Matrix</div>
                            <div id="correlations-loading" class="loading">Loading correlations...</div>
                            <div id="correlations-error" class="error" style="display: none;"></div>
                            <div class="correlation-matrix">
                                <table id="correlation-table" class="correlation-table" style="display: none;">
                                    <thead></thead>
                                    <tbody></tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Data Table Tab -->
                    <div id="table" class="tab-content">
                        <div class="filters-panel">
                            <div class="filter-group">
                                <label>Page Size:</label>
                                <select id="page-size-selector" class="filter-input">
                                    <option value="25">25 rows</option>
                                    <option value="50" selected>50 rows</option>
                                    <option value="100">100 rows</option>
                                </select>
                                <button class="btn" onclick="loadTable()">Refresh Table</button>
                            </div>
                        </div>
                        
                        <div id="table-loading" class="loading">Loading data table...</div>
                        <div id="table-error" class="error" style="display: none;"></div>
                        <div class="table-container">
                            <table id="data-table" class="data-table" style="display: none;">
                                <thead></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                        <div id="pagination" class="pagination"></div>
                    </div>
                    
                    <!-- OHLC Charts Tab -->
                    <div id="ohlc-charts" class="tab-content">
                        <div class="chart-container">
                            <div class="chart-title">OHLC Chart with Technical Indicators</div>
                            <div id="ohlc-loading" class="loading">Loading OHLC chart...</div>
                            <div id="ohlc-error" class="error" style="display: none;"></div>
                            <div id="ohlc-chart" style="height: 600px;"></div>
                        </div>
                    </div>
                    
                    <!-- Advanced Filters Tab -->
                    <div id="filters" class="tab-content">
                        <div class="filters-panel">
                            <h3>Advanced Feature Filtering</h3>
                            <div id="filter-builder">
                                <div class="filter-group">
                                    <label>Feature:</label>
                                    <select id="filter-feature" class="filter-input">
                                        <option value="close">Close Price</option>
                                        <option value="volume">Volume</option>
                                        <option value="etop">Elliott Top</option>
                                        <option value="ebot">Elliott Bottom</option>
                                        <option value="pldot">Pivot Line Dot</option>
                                        <option value="oneonedot">One-One-Dot</option>
                                    </select>
                                    <label>Min Value:</label>
                                    <input type="number" id="filter-min" class="filter-input" placeholder="Min">
                                    <label>Max Value:</label>
                                    <input type="number" id="filter-max" class="filter-input" placeholder="Max">
                                    <button class="btn" onclick="addFilter()">Add Filter</button>
                                </div>
                            </div>
                            <div id="active-filters"></div>
                            <button class="btn" onclick="applyFilters()">Apply Filters</button>
                        </div>
                        
                        <div class="chart-container">
                            <div class="chart-title">Filtered Data Preview</div>
                            <div id="filtered-preview">No filters applied</div>
                        </div>
                    </div>
                </div>
            </div>

            <script>
                // Global variables
                let currentDataset = 1;
                let currentPage = 1;
                let currentPageSize = 50;
                let activeFilters = [];
                
                // Tab switching
                function showTab(tabName) {
                    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
                    document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
                    
                    document.getElementById(tabName).classList.add('active');
                    event.target.classList.add('active');
                    
                    // Load data for the tab
                    if (tabName === 'distributions') loadDistributions();
                    if (tabName === 'correlations') loadCorrelations();
                    if (tabName === 'table') loadTable();
                    if (tabName === 'ohlc-charts') loadOHLCChart();
                }
                
                // Load feature distributions
                async function loadDistributions() {
                    const container = document.getElementById('distributions-container');
                    const loading = document.getElementById('distributions-loading');
                    const error = document.getElementById('distributions-error');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    container.innerHTML = '';
                    
                    try {
                        const filtersParam = activeFilters.length > 0 ? 
                            '?filters=' + encodeURIComponent(JSON.stringify(activeFilters)) : '';
                        
                        const response = await fetch(`/api/v1/training-data/${currentDataset}/visualization${filtersParam}`);
                        const data = await response.json();
                        
                        loading.style.display = 'none';
                        
                        // Create distribution plots
                        data.feature_distributions.forEach(dist => {
                            const chartDiv = document.createElement('div');
                            chartDiv.className = 'chart-container';
                            chartDiv.innerHTML = `
                                <div class="chart-title">${dist.feature_name} Distribution</div>
                                <div id="dist-${dist.feature_name}" style="height: 300px;"></div>
                                <div style="margin-top: 10px; font-size: 14px; color: #666;">
                                    <strong>Stats:</strong> 
                                    Mean: ${dist.mean_value.toFixed(3)} | 
                                    Std: ${dist.std_value.toFixed(3)} | 
                                    Range: [${dist.min_value.toFixed(3)}, ${dist.max_value.toFixed(3)}]
                                </div>
                            `;
                            container.appendChild(chartDiv);
                            
                            // Create Plotly histogram
                            const trace = {
                                x: dist.values,
                                type: 'histogram',
                                name: dist.feature_name,
                                marker: { color: '#667eea', opacity: 0.7 }
                            };
                            
                            const layout = {
                                title: '',
                                xaxis: { title: dist.feature_name },
                                yaxis: { title: 'Frequency' },
                                margin: { t: 20, r: 20, b: 60, l: 60 }
                            };
                            
                            Plotly.newPlot(`dist-${dist.feature_name}`, [trace], layout, {responsive: true});
                        });
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading distributions: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Load feature correlations
                async function loadCorrelations() {
                    const loading = document.getElementById('correlations-loading');
                    const error = document.getElementById('correlations-error');
                    const table = document.getElementById('correlation-table');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    table.style.display = 'none';
                    
                    try {
                        const response = await fetch(`/api/v1/training-data/${currentDataset}/visualization`);
                        const data = await response.json();
                        
                        loading.style.display = 'none';
                        
                        // Build correlation table
                        const correlations = data.feature_correlations;
                        const features = Object.keys(correlations);
                        
                        // Create header
                        const thead = table.querySelector('thead');
                        thead.innerHTML = '<tr><th>Feature</th>' + 
                            features.map(f => `<th>${f}</th>`).join('') + '</tr>';
                        
                        // Create rows
                        const tbody = table.querySelector('tbody');
                        tbody.innerHTML = features.map(feature1 => {
                            return '<tr><td><strong>' + feature1 + '</strong></td>' +
                                features.map(feature2 => {
                                    const corr = correlations[feature1][feature2];
                                    const color = Math.abs(corr) > 0.7 ? '#e74c3c' : 
                                                 Math.abs(corr) > 0.4 ? '#f39c12' : '#27ae60';
                                    return `<td style="background-color: ${color}20; color: ${color}; font-weight: bold;">
                                        ${corr.toFixed(3)}
                                    </td>`;
                                }).join('') + '</tr>';
                        }).join('');
                        
                        table.style.display = 'table';
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading correlations: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Load data table
                async function loadTable() {
                    const loading = document.getElementById('table-loading');
                    const error = document.getElementById('table-error');
                    const table = document.getElementById('data-table');
                    const pagination = document.getElementById('pagination');
                    
                    currentPageSize = parseInt(document.getElementById('page-size-selector').value);
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    table.style.display = 'none';
                    pagination.innerHTML = '';
                    
                    try {
                        const response = await fetch(
                            `/api/v1/training-data/${currentDataset}/table?page=${currentPage}&page_size=${currentPageSize}`
                        );
                        const data = await response.json();
                        
                        loading.style.display = 'none';
                        
                        // Create table header
                        const thead = table.querySelector('thead');
                        thead.innerHTML = '<tr>' + 
                            data.columns.map(col => `<th>${col}</th>`).join('') + '</tr>';
                        
                        // Create table body
                        const tbody = table.querySelector('tbody');
                        tbody.innerHTML = data.data.map(row => 
                            '<tr>' + row.map(cell => `<td>${typeof cell === 'number' ? cell.toFixed(3) : cell}</td>`).join('') + '</tr>'
                        ).join('');
                        
                        table.style.display = 'table';
                        
                        // Create pagination
                        const totalPages = Math.ceil(data.total_rows / currentPageSize);
                        createPagination(pagination, currentPage, totalPages);
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading table: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Load OHLC chart
                async function loadOHLCChart() {
                    const loading = document.getElementById('ohlc-loading');
                    const error = document.getElementById('ohlc-error');
                    const chart = document.getElementById('ohlc-chart');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    
                    try {
                        const response = await fetch(`/api/v1/training-data/${currentDataset}/visualization`);
                        const data = await response.json();
                        
                        loading.style.display = 'none';
                        
                        const ohlcData = data.ohlc_with_indicators;
                        
                        // Create OHLC candlestick trace
                        const candlestick = {
                            x: ohlcData.dates,
                            open: ohlcData.ohlc_data.map(d => d.open),
                            high: ohlcData.ohlc_data.map(d => d.high),
                            low: ohlcData.ohlc_data.map(d => d.low),
                            close: ohlcData.ohlc_data.map(d => d.close),
                            type: 'candlestick',
                            name: 'OHLC',
                            yaxis: 'y'
                        };
                        
                        // Create indicator traces
                        const traces = [candlestick];
                        const colors = ['#e74c3c', '#27ae60', '#f39c12', '#9b59b6'];
                        
                        ohlcData.indicators.forEach((indicator, idx) => {
                            traces.push({
                                x: ohlcData.dates,
                                y: ohlcData.ohlc_data.map(d => d[indicator]),
                                type: 'scatter',
                                mode: 'lines',
                                name: indicator,
                                line: { color: colors[idx % colors.length] },
                                yaxis: 'y2'
                            });
                        });
                        
                        const layout = {
                            title: 'OHLC Chart with Technical Indicators',
                            xaxis: { title: 'Date' },
                            yaxis: { 
                                title: 'Price', 
                                domain: [0.3, 1]
                            },
                            yaxis2: { 
                                title: 'Indicators', 
                                domain: [0, 0.25],
                                side: 'right'
                            },
                            margin: { t: 50, r: 80, b: 50, l: 80 },
                            showlegend: true
                        };
                        
                        Plotly.newPlot('ohlc-chart', traces, layout, {responsive: true});
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading OHLC chart: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Pagination helpers
                function createPagination(container, currentPage, totalPages) {
                    container.innerHTML = '';
                    
                    if (currentPage > 1) {
                        const prevBtn = document.createElement('button');
                        prevBtn.textContent = 'Previous';
                        prevBtn.onclick = () => goToPage(currentPage - 1);
                        container.appendChild(prevBtn);
                    }
                    
                    const pageInfo = document.createElement('span');
                    pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
                    pageInfo.style.margin = '0 15px';
                    container.appendChild(pageInfo);
                    
                    if (currentPage < totalPages) {
                        const nextBtn = document.createElement('button');
                        nextBtn.textContent = 'Next';
                        nextBtn.onclick = () => goToPage(currentPage + 1);
                        container.appendChild(nextBtn);
                    }
                }
                
                function goToPage(page) {
                    currentPage = page;
                    loadTable();
                }
                
                // Filter functions
                function addFilter() {
                    const feature = document.getElementById('filter-feature').value;
                    const min = document.getElementById('filter-min').value;
                    const max = document.getElementById('filter-max').value;
                    
                    if (!min && !max) {
                        alert('Please enter min or max value');
                        return;
                    }
                    
                    const filter = {
                        feature_name: feature,
                        min_value: min ? parseFloat(min) : null,
                        max_value: max ? parseFloat(max) : null,
                        operation: 'range'
                    };
                    
                    activeFilters.push(filter);
                    updateActiveFiltersDisplay();
                    
                    // Clear inputs
                    document.getElementById('filter-min').value = '';
                    document.getElementById('filter-max').value = '';
                }
                
                function updateActiveFiltersDisplay() {
                    const container = document.getElementById('active-filters');
                    container.innerHTML = '<h4>Active Filters:</h4>' + 
                        activeFilters.map((filter, idx) => `
                            <div style="background: #e3f2fd; padding: 10px; margin: 5px 0; border-radius: 4px; display: flex; justify-content: space-between; align-items: center;">
                                <span>${filter.feature_name}: ${filter.min_value || 'min'} - ${filter.max_value || 'max'}</span>
                                <button onclick="removeFilter(${idx})" style="background: #f44336; color: white; border: none; padding: 5px 10px; border-radius: 3px; cursor: pointer;">Remove</button>
                            </div>
                        `).join('');
                }
                
                function removeFilter(index) {
                    activeFilters.splice(index, 1);
                    updateActiveFiltersDisplay();
                }
                
                function clearFilters() {
                    activeFilters = [];
                    updateActiveFiltersDisplay();
                }
                
                function applyFilters() {
                    // Reload current tab with filters
                    const activeTab = document.querySelector('.nav-tab.active').textContent;
                    if (activeTab.includes('Distributions')) loadDistributions();
                    else if (activeTab.includes('Table')) loadTable();
                }
                
                // Initialize on page load
                document.addEventListener('DOMContentLoaded', function() {
                    loadDistributions();
                });
            </script>
        </body>
        </html>
        '''
        return html
    
    return app

if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 Starting Enhanced Training Data Visualization Platform")
    logging.info("📊 Features: Distributions, Correlations, Table View, OHLC Charts")
    logging.info("🌐 Dashboard: http://0.0.0.0:4000/")
    logging.info("📚 API Docs: http://0.0.0.0:4000/api/docs")
    
    app = create_enhanced_webapp()
    uvicorn.run(app, host="0.0.0.0", port=4000)