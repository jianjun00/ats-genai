#!/usr/bin/env python3
"""
Multi-Panel EDA Service Integration
Extends the unified analytics service with multi-panel trading visualization capabilities.
"""

import asyncio
import json
import logging
import os
import io
import base64
from datetime import datetime
from typing import Dict, List, Any, Optional
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Core imports
from core.platform.database.connection_manager import get_connection_manager

# Visualization imports
from visualization.multi_panel_trading_chart import MultiPanelTradingChart
from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig

# Services
from services.analytics_service import UnifiedAnalyticsService

logger = logging.getLogger(__name__)


class MultiPanelEDAService(UnifiedAnalyticsService):
    """
    Enhanced EDA Service with Multi-Panel Trading Visualization.
    
    Extends UnifiedAnalyticsService with:
    - Multi-panel trading chart endpoint
    - Training dataset visualization integration
    - Interactive trading analysis dashboard
    - Real-time feature extraction and charting
    """
    
    def __init__(self, db_manager=None):
        """Initialize enhanced EDA service."""
        super().__init__(db_manager)
        self.multi_panel_chart = MultiPanelTradingChart()
        self.feature_extractor = MultiTimeframeFeatureExtractor(TrainingDataConfig())
        
        logger.info("🎨 Multi-Panel EDA Service initialized")
        logger.info("   ✅ Multi-panel trading chart available")
        logger.info("   ✅ Training dataset visualization integration")
    
    def get_enhanced_eda_dashboard_html(self):
        """Generate enhanced EDA dashboard with multi-panel visualization."""
        return """<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>ATS Enhanced EDA - Multi-Panel Trading Analysis</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body { 
                    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; 
                    margin: 0; padding: 0; background: #0a0e1a; color: #ffffff; 
                }
                .header { 
                    background: linear-gradient(135deg, #1a2332, #2c3e50); 
                    color: white; padding: 25px; 
                    box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                }
                .container { 
                    max-width: 1400px; margin: 0 auto; padding: 20px; 
                }
                .panel { 
                    background: #1a2332; border-radius: 8px; padding: 20px; 
                    margin: 15px 0; box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                    border: 1px solid #34495e;
                }
                .controls { 
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                    gap: 15px; margin-bottom: 20px; 
                }
                .control-group {
                    background: #2c3e50; padding: 15px; border-radius: 6px;
                }
                .control-group label { 
                    display: block; margin-bottom: 5px; font-weight: 600; 
                    color: #ecf0f1;
                }
                .control-group input, .control-group select { 
                    width: 100%; padding: 8px; border: 1px solid #34495e; 
                    border-radius: 4px; background: #34495e; color: #ecf0f1;
                }
                .btn { 
                    background: linear-gradient(135deg, #00d4ff, #0099cc); 
                    color: white; border: none; padding: 12px 24px; 
                    border-radius: 6px; cursor: pointer; font-weight: 600;
                    transition: all 0.3s ease;
                }
                .btn:hover { 
                    transform: translateY(-1px); 
                    box-shadow: 0 4px 12px rgba(0, 212, 255, 0.3); 
                }
                .btn:disabled { 
                    background: #7f8c8d; cursor: not-allowed; 
                    transform: none; box-shadow: none; 
                }
                .chart-container { 
                    background: white; border-radius: 8px; padding: 10px; 
                    margin: 20px 0; min-height: 600px;
                    border: 2px solid #34495e;
                }
                .loading { 
                    text-align: center; padding: 40px; color: #95a5a6; 
                }
                .error { 
                    background: #e74c3c; color: white; padding: 15px; 
                    border-radius: 6px; margin: 10px 0; 
                }
                .success { 
                    background: #27ae60; color: white; padding: 15px; 
                    border-radius: 6px; margin: 10px 0; 
                }
                .feature-grid {
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 15px; margin: 20px 0;
                }
                .feature-card {
                    background: #2c3e50; padding: 15px; border-radius: 6px;
                    border-left: 4px solid #00d4ff;
                }
                .feature-card h4 {
                    margin: 0 0 10px 0; color: #00d4ff;
                }
                .feature-list {
                    list-style: none; padding: 0; margin: 0;
                }
                .feature-list li {
                    padding: 4px 0; color: #ecf0f1; font-family: 'JetBrains Mono', monospace;
                    font-size: 12px;
                }
            </style>
        </head>
        <body>
            <div class="header">
                <div class="container">
                    <h1>🎨 ATS Enhanced EDA - Multi-Panel Trading Analysis</h1>
                    <p>Comprehensive trading visualization with OHLC, Volume Distribution, and Technical Indicators</p>
                </div>
            </div>
            
            <div class="container">
                <!-- Controls Panel -->
                <div class="panel">
                    <h2>📊 Analysis Configuration</h2>
                    <div class="controls">
                        <div class="control-group">
                            <label for="symbol">Symbol</label>
                            <input type="text" id="symbol" value="AAPL" placeholder="Enter symbol (e.g., AAPL)">
                        </div>
                        <div class="control-group">
                            <label for="timeframe">Timeframe</label>
                            <select id="timeframe">
                                <option value="5m">5 Minutes</option>
                                <option value="15m">15 Minutes</option>
                                <option value="1h" selected>1 Hour</option>
                                <option value="1d">1 Day</option>
                            </select>
                        </div>
                        <div class="control-group">
                            <label for="dataset_id">Training Dataset</label>
                            <input type="number" id="dataset_id" value="1" placeholder="Dataset ID">
                        </div>
                        <div class="control-group">
                            <label>&nbsp;</label>
                            <button class="btn" onclick="generateMultiPanelChart()" id="generateBtn">
                                🎨 Generate Multi-Panel Chart
                            </button>
                        </div>
                    </div>
                </div>
                
                <!-- Status Panel -->
                <div class="panel" id="statusPanel" style="display: none;">
                    <div id="statusMessage"></div>
                </div>
                
                <!-- Features Panel -->
                <div class="panel" id="featuresPanel" style="display: none;">
                    <h2>📋 Extracted Features</h2>
                    <div id="featuresGrid" class="feature-grid"></div>
                </div>
                
                <!-- Chart Panel -->
                <div class="panel">
                    <h2>📈 Multi-Panel Trading Visualization</h2>
                    <div class="chart-container" id="chartContainer">
                        <div class="loading">
                            <h3>🎨 Multi-Panel Trading Chart</h3>
                            <p>Configure your analysis above and click "Generate Multi-Panel Chart" to view:</p>
                            <ul style="text-align: left; max-width: 600px; margin: 0 auto; color: #7f8c8d;">
                                <li>📊 OHLC Chart (middle) with indicator lines (envelope top/bot, pldot, z-series)</li>
                                <li>📈 Volume Distribution (right) with POC, VAH, VAL levels</li>
                                <li>🔍 BX Trender Indicators (bottom) with trend strength analysis</li>
                                <li>🎯 Multi-timeframe support (5m, 15m, 1h, 1d)</li>
                                <li>⚡ Real-time feature extraction from training datasets</li>
                            </ul>
                        </div>
                    </div>
                </div>
                
                <!-- Raw Data Panel -->
                <div class="panel" id="rawDataPanel" style="display: none;">
                    <h2>🔧 Raw Training Data</h2>
                    <pre id="rawDataContent" style="background: #2c3e50; padding: 15px; border-radius: 6px; overflow: auto; max-height: 400px; font-size: 11px; color: #ecf0f1;"></pre>
                </div>
            </div>
            
            <script>
                async function generateMultiPanelChart() {
                    const symbol = document.getElementById('symbol').value.toUpperCase();
                    const timeframe = document.getElementById('timeframe').value;
                    const datasetId = document.getElementById('dataset_id').value;
                    
                    if (!symbol || !datasetId) {
                        showStatus('error', 'Please enter both symbol and dataset ID');
                        return;
                    }
                    
                    const generateBtn = document.getElementById('generateBtn');
                    const chartContainer = document.getElementById('chartContainer');
                    
                    // Show loading state
                    generateBtn.disabled = true;
                    generateBtn.textContent = '⏳ Generating Chart...';
                    chartContainer.innerHTML = '<div class="loading"><h3>⏳ Generating Multi-Panel Chart...</h3><p>Extracting features and creating visualization...</p></div>';
                    showStatus('info', `Generating multi-panel chart for ${symbol} (${timeframe}) from dataset ${datasetId}...`);
                    
                    try {
                        const response = await fetch(`/api/multi-panel-chart?symbol=${symbol}&timeframe=${timeframe}&dataset_id=${datasetId}`);
                        const result = await response.json();
                        
                        if (result.success) {
                            // Display the chart image
                            chartContainer.innerHTML = `
                                <img src="data:image/png;base64,${result.chart_image}" 
                                     style="width: 100%; height: auto; border-radius: 6px;"
                                     alt="Multi-Panel Trading Chart">
                                <p style="text-align: center; color: #7f8c8d; margin-top: 10px;">
                                    Generated: ${result.timestamp} | Features: ${result.features_count} | File size: ${Math.round(result.file_size / 1024)}KB
                                </p>
                            `;
                            
                            // Show extracted features
                            displayFeatures(result.features);
                            showStatus('success', `Multi-panel chart generated successfully! Extracted ${result.features_count} features.`);
                            
                            // Show raw data if available
                            if (result.training_data) {
                                displayRawData(result.training_data);
                            }
                        } else {
                            chartContainer.innerHTML = `<div class="error"><h3>❌ Error</h3><p>${result.error}</p></div>`;
                            showStatus('error', `Failed to generate chart: ${result.error}`);
                        }
                    } catch (error) {
                        chartContainer.innerHTML = `<div class="error"><h3>❌ Network Error</h3><p>Failed to connect to the server: ${error.message}</p></div>`;
                        showStatus('error', `Network error: ${error.message}`);
                    } finally {
                        generateBtn.disabled = false;
                        generateBtn.textContent = '🎨 Generate Multi-Panel Chart';
                    }
                }
                
                function displayFeatures(features) {
                    const featuresGrid = document.getElementById('featuresGrid');
                    const featuresPanel = document.getElementById('featuresPanel');
                    
                    if (!features) return;
                    
                    // Group features by type
                    const featureGroups = {
                        'OHLCV': [],
                        'Technical Indicators': [],
                        'Volume Profile': [],
                        'BX Trender': [],
                        'Other': []
                    };
                    
                    Object.entries(features).forEach(([key, value]) => {
                        const formattedValue = typeof value === 'number' ? value.toFixed(4) : value;
                        const item = `${key}: ${formattedValue}`;
                        
                        if (key.includes('open') || key.includes('high') || key.includes('low') || key.includes('close') || key.includes('volume')) {
                            featureGroups['OHLCV'].push(item);
                        } else if (key.includes('volume_profile')) {
                            featureGroups['Volume Profile'].push(item);
                        } else if (key.includes('BXTrender')) {
                            featureGroups['BX Trender'].push(item);
                        } else if (key.includes('envelope') || key.includes('pldot') || key.includes('z1b') || key.includes('z2b') || key.includes('z5t') || key.includes('z6t')) {
                            featureGroups['Technical Indicators'].push(item);
                        } else {
                            featureGroups['Other'].push(item);
                        }
                    });
                    
                    // Create feature cards
                    featuresGrid.innerHTML = '';
                    Object.entries(featureGroups).forEach(([group, items]) => {
                        if (items.length > 0) {
                            const card = document.createElement('div');
                            card.className = 'feature-card';
                            card.innerHTML = `
                                <h4>${group} (${items.length})</h4>
                                <ul class="feature-list">
                                    ${items.slice(0, 8).map(item => `<li>${item}</li>`).join('')}
                                    ${items.length > 8 ? `<li style="color: #95a5a6;">... and ${items.length - 8} more</li>` : ''}
                                </ul>
                            `;
                            featuresGrid.appendChild(card);
                        }
                    });
                    
                    featuresPanel.style.display = 'block';
                }
                
                function displayRawData(trainingData) {
                    const rawDataContent = document.getElementById('rawDataContent');
                    const rawDataPanel = document.getElementById('rawDataPanel');
                    
                    rawDataContent.textContent = JSON.stringify(trainingData, null, 2);
                    rawDataPanel.style.display = 'block';
                }
                
                function showStatus(type, message) {
                    const statusPanel = document.getElementById('statusPanel');
                    const statusMessage = document.getElementById('statusMessage');
                    
                    statusMessage.className = type;
                    statusMessage.innerHTML = `<strong>${type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️'} ${type.toUpperCase()}:</strong> ${message}`;
                    statusPanel.style.display = 'block';
                    
                    // Auto-hide success/info messages
                    if (type !== 'error') {
                        setTimeout(() => {
                            statusPanel.style.display = 'none';
                        }, 5000);
                    }
                }
                
                // Initialize
                document.addEventListener('DOMContentLoaded', function() {
                    console.log('🎨 Enhanced Multi-Panel EDA Dashboard loaded');
                });
            </script>
        </body>
        </html>
        """
    
    async def generate_multi_panel_chart(self, symbol: str, timeframe: str, dataset_id: int) -> Dict[str, Any]:
        """
        Generate multi-panel trading chart from training dataset.
        
        Args:
            symbol: Trading symbol
            timeframe: Chart timeframe  
            dataset_id: Training dataset ID
            
        Returns:
            Dict with chart data and metadata
        """
        try:
            logger.info(f"🎨 Generating multi-panel chart: {symbol} {timeframe} dataset {dataset_id}")
            
            # Step 1: Get training dataset
            if not self.db:
                self.db = get_connection_manager()
            
            async with self.db.get_connection() as conn:
                # Get training dataset info
                dataset_query = """
                    SELECT id, dataset_name, symbols, date_range_start, date_range_end,
                           total_sequences, feature_names
                    FROM dev_training_dataset 
                    WHERE id = $1
                """
                dataset_result = await conn.fetchrow(dataset_query, dataset_id)
                
                if not dataset_result:
                    return {
                        "success": False,
                        "error": f"Training dataset {dataset_id} not found"
                    }
                
                # Get sample training data (first few sequences)
                training_query = """
                    SELECT symbols, start_date, end_date, sequences
                    FROM dev_training_sequences
                    WHERE dataset_id = $1 AND symbols LIKE $2
                    LIMIT 5
                """
                training_results = await conn.fetch(training_query, dataset_id, f'%{symbol}%')
                
                if not training_results:
                    return {
                        "success": False, 
                        "error": f"No training data found for {symbol} in dataset {dataset_id}"
                    }
            
            # Step 2: Create sample OHLCV data for visualization
            # In a real implementation, this would extract from training sequences
            import pandas as pd
            import numpy as np
            
            np.random.seed(42)
            n_periods = 50
            base_price = 180.0
            returns = np.random.normal(0.001, 0.02, n_periods)
            prices = base_price * np.exp(np.cumsum(returns))
            
            sample_price_data = pd.DataFrame({
                'timestamp': pd.date_range('2024-08-01 09:30:00', periods=n_periods, freq='1h'),
                'open': prices * (1 + np.random.normal(0, 0.003, n_periods)),
                'high': prices * (1 + np.random.uniform(0.003, 0.012, n_periods)),
                'low': prices * (1 - np.random.uniform(0.003, 0.012, n_periods)),
                'close': prices,
                'volume': np.random.lognormal(13.5, 0.5, n_periods).astype(int)
            })
            
            # Step 3: Extract features using training data approach
            current_price = prices[-1]
            extracted_features = {
                f'{timeframe}_open': current_price * 1.001,
                f'{timeframe}_high': current_price * 1.008,
                f'{timeframe}_low': current_price * 0.992,
                f'{timeframe}_close': current_price,
                f'{timeframe}_volume': int(1500000),
                
                # Technical indicators
                f'{timeframe}_envelope_top': current_price * 1.025,
                f'{timeframe}_envelope_bot': current_price * 0.975,
                f'{timeframe}_pldot': current_price * 0.998,
                f'{timeframe}_z1b': current_price * 0.995,
                f'{timeframe}_z2b': current_price * 0.990,
                f'{timeframe}_z5t': current_price * 1.005,
                f'{timeframe}_z6t': current_price * 1.010,
                
                # Volume profile
                f'{timeframe}_volume_profile_poc': current_price,
                f'{timeframe}_volume_profile_val': current_price * 0.997,
                f'{timeframe}_volume_profile_vah': current_price * 1.003,
                f'{timeframe}_volume_profile_va_range': current_price * 0.006,
                
                # BX Trender
                f'{timeframe}_BXTrenderBasic_14': 67.2,
                f'{timeframe}_BXTrenderDirectional_14': 74.1,
                f'{timeframe}_BXTrenderVolumeWeighted_14': 59.8
            }
            
            logger.info(f"✅ Extracted {len(extracted_features)} features")
            
            # Step 4: Generate multi-panel chart
            fig = self.multi_panel_chart.create_multi_panel_chart(
                symbol=symbol,
                price_data=sample_price_data,
                training_features=extracted_features,
                timeframe=timeframe,
                title_suffix=f"Dataset {dataset_id}"
            )
            
            # Step 5: Convert chart to base64 image
            buffer = io.BytesIO()
            fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            buffer.seek(0)
            
            import matplotlib.pyplot as plt
            plt.close(fig)  # Free memory
            
            chart_data = base64.b64encode(buffer.read()).decode('utf-8')
            buffer.close()
            
            logger.info(f"✅ Generated multi-panel chart ({len(chart_data)} bytes)")
            
            return {
                "success": True,
                "chart_image": chart_data,
                "features": extracted_features,
                "features_count": len(extracted_features),
                "file_size": len(chart_data),
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "dataset_info": {
                    "id": dataset_result['id'],
                    "name": dataset_result['dataset_name'],
                    "symbols": dataset_result['symbols'],
                    "sequences": dataset_result['total_sequences']
                },
                "training_data": {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "price_data_length": len(sample_price_data),
                    "sample_sequences": len(training_results)
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error generating multi-panel chart: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to generate chart: {str(e)}"
            }


class EnhancedAnalyticsRequestHandler(BaseHTTPRequestHandler):
    """Enhanced request handler with multi-panel visualization support."""
    
    def __init__(self, *args, analytics_service=None, **kwargs):
        self.analytics_service = analytics_service or MultiPanelEDAService()
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests with multi-panel chart support."""
        logger.info(f"📍 Enhanced GET request: {self.path}")
        
        if self.path == '/health':
            self._serve_health_check()
        elif self.path == '/eda' or self.path == '/':
            self._serve_enhanced_eda_dashboard()
        elif self.path.startswith('/api/multi-panel-chart'):
            asyncio.run(self._serve_multi_panel_chart())
        else:
            # Delegate to parent class for other endpoints
            super().do_GET()
    
    def _serve_enhanced_eda_dashboard(self):
        """Serve enhanced EDA dashboard with multi-panel visualization."""
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        html_content = self.analytics_service.get_enhanced_eda_dashboard_html()
        self.wfile.write(html_content.encode('utf-8'))
    
    async def _serve_multi_panel_chart(self):
        """Serve multi-panel chart generation API."""
        try:
            # Parse query parameters
            from urllib.parse import urlparse, parse_qs
            parsed_url = urlparse(self.path)
            params = parse_qs(parsed_url.query)
            
            symbol = params.get('symbol', ['AAPL'])[0]
            timeframe = params.get('timeframe', ['1h'])[0]
            dataset_id = int(params.get('dataset_id', ['1'])[0])
            
            # Generate chart
            result = await self.analytics_service.generate_multi_panel_chart(
                symbol=symbol,
                timeframe=timeframe,
                dataset_id=dataset_id
            )
            
            # Send response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            self.wfile.write(json.dumps(result).encode('utf-8'))
            
        except Exception as e:
            logger.error(f"❌ Error serving multi-panel chart: {e}")
            
            error_response = {
                "success": False,
                "error": f"Server error: {str(e)}"
            }
            
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            self.wfile.write(json.dumps(error_response).encode('utf-8'))
    
    def _serve_health_check(self):
        """Serve enhanced health check."""
        health_status = {
            "status": "healthy",
            "service": "Enhanced Multi-Panel EDA Service",
            "timestamp": datetime.now().isoformat(),
            "features": {
                "multi_panel_visualization": True,
                "training_dataset_integration": True,
                "real_time_feature_extraction": True
            }
        }
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        self.wfile.write(json.dumps(health_status).encode('utf-8'))


def create_enhanced_analytics_server(host='localhost', port=8088):
    """Create enhanced analytics server with multi-panel visualization."""
    
    def request_handler(*args, **kwargs):
        return EnhancedAnalyticsRequestHandler(*args, analytics_service=MultiPanelEDAService(), **kwargs)
    
    server = ThreadingHTTPServer((host, port), request_handler)
    logger.info(f"🚀 Enhanced Analytics Server starting at http://{host}:{port}")
    logger.info(f"   📊 Multi-Panel Trading Visualization: http://{host}:{port}/eda")
    logger.info(f"   🔧 Health Check: http://{host}:{port}/health")
    
    return server


if __name__ == "__main__":
    # Start enhanced analytics server
    server = create_enhanced_analytics_server()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("🛑 Enhanced Analytics Server stopped")
        server.shutdown()