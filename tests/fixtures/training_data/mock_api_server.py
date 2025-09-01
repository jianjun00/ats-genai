#!/usr/bin/env python3
"""
Mock API server for hermetic testing of training data visualization.

This provides a lightweight HTTP server that simulates the analytics service
API responses without requiring the full ATS infrastructure.
"""

import json
import os
from typing import Dict, Any, List
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading
import time


class MockTrainingDataAPI(BaseHTTPRequestHandler):
    """Mock HTTP handler for training data API endpoints"""
    
    def __init__(self, *args, **kwargs):
        # Load mock data
        fixture_dir = os.path.dirname(__file__)
        with open(os.path.join(fixture_dir, 'mock_datasets.json'), 'r') as f:
            self.mock_data = json.load(f)
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)
        
        try:
            if path == '/health':
                self._send_response(200, {'status': 'healthy', 'service': 'mock-analytics'})
                
            elif path == '/api/v1/training-datasets/':
                self._handle_datasets_list()
                
            elif path.startswith('/api/v1/training-datasets/') and path.endswith('/data'):
                dataset_id = path.split('/')[-2]
                self._handle_table_data(dataset_id, query_params)
                
            elif path.startswith('/api/v1/training-datasets/') and 'visualization-data' in path:
                dataset_id = path.split('/')[-2]
                self._handle_visualization_data(dataset_id, query_params)
                
            elif path == '/eda':
                self._handle_eda_page()
                
            elif path == '/training-eda':
                self._handle_training_eda_page()
                
            else:
                self._send_error(404, f"Endpoint not found: {path}")
                
        except Exception as e:
            self._send_error(500, f"Internal server error: {str(e)}")
    
    def _handle_datasets_list(self):
        """Return list of available training datasets"""
        response = self.mock_data['api_responses']['datasets_list']
        self._send_response(200, response)
    
    def _handle_table_data(self, dataset_id: str, query_params: Dict):
        """Return table data for a specific dataset"""
        # Find dataset
        dataset = self._find_dataset(dataset_id)
        if not dataset:
            self._send_error(404, f"Dataset {dataset_id} not found")
            return
        
        # Get pagination parameters
        page = int(query_params.get('page', [1])[0])
        limit = int(query_params.get('limit', [10])[0])
        
        # Get sample data
        sample_data = dataset.get('sample_data', [])
        
        # Apply pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = sample_data[start_idx:end_idx]
        
        response = {
            'data': paginated_data,
            'total_count': len(sample_data),
            'page': page,
            'limit': limit,
            'has_more': end_idx < len(sample_data)
        }
        
        self._send_response(200, response)
    
    def _handle_visualization_data(self, dataset_id: str, query_params: Dict):
        """Return visualization data for a specific dataset"""
        dataset = self._find_dataset(dataset_id)
        if not dataset:
            self._send_error(404, f"Dataset {dataset_id} not found")
            return
        
        # Get sequence index (default to 0)
        sequence_index = int(query_params.get('sequence_index', [0])[0])
        
        # Get sample data for visualization
        sample_data = dataset.get('sample_data', [])
        
        response = {
            'data': sample_data,
            'sequence_length': dataset.get('sequence_length', 60),
            'features': dataset.get('features', []),
            'technical_indicators': dataset.get('features', []),
            'sequence_index': sequence_index
        }
        
        self._send_response(200, response)
    
    def _handle_eda_page(self):
        """Return mock EDA HTML page"""
        html = self._generate_mock_eda_html()
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())
    
    def _handle_training_eda_page(self):
        """Return mock training EDA HTML page"""  
        html = self._generate_mock_training_eda_html()
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())
    
    def _find_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Find dataset by ID"""
        for dataset in self.mock_data['datasets']:
            if dataset['id'] == dataset_id:
                return dataset
        return None
    
    def _send_response(self, status_code: int, data: Any):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def _send_error(self, status_code: int, message: str):
        """Send error response"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        error_response = {'error': message, 'status_code': status_code}
        self.wfile.write(json.dumps(error_response).encode())
    
    def _generate_mock_eda_html(self) -> str:
        """Generate mock EDA HTML page"""
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>ATS EDA Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                .tab { padding: 10px; margin: 5px; background: #f0f0f0; cursor: pointer; }
                .tab.active { background: #007cba; color: white; }
                .section { padding: 20px; margin: 10px; border: 1px solid #ddd; }
                .hidden { display: none; }
            </style>
        </head>
        <body>
            <h1>ATS EDA Dashboard</h1>
            <div class="tabs">
                <div class="tab active" data-tab="overview">Overview</div>
                <div class="tab" data-tab="training">Training Datasets</div>
                <div class="tab" data-tab="market-data">Market Data</div>
            </div>
            <div id="training-dataset-section" class="section hidden">
                <h2>Training Dataset Analysis</h2>
                <div id="dataset-selector">
                    <label for="training-dataset-select">Select Dataset:</label>
                    <select id="training-dataset-select">
                        <option value="">-- Select Dataset --</option>
                        <option value="15">AAPL Hourly Training Dataset</option>
                        <option value="16">MSFT Hourly Training Dataset</option>
                        <option value="17">GOOGL Training Dataset</option>
                    </select>
                </div>
                <div id="ohlc-visualization" class="section">
                    <h3>OHLC Data Visualization</h3>
                    <div id="ohlc-chart" style="width: 100%; height: 500px;"></div>
                    <button onclick="updateOHLCVisualization()">Refresh Visualization</button>
                    <button onclick="randomSample()">Random Sample</button>
                </div>
                <div id="training-data-table" class="section">
                    <h3>Training Data Table</h3>
                    <div id="training-data-content"></div>
                </div>
            </div>
            <script>
                // Mock JavaScript functions
                function updateOHLCVisualization(datasetId, sequenceIndex) {
                    console.log('Updating OHLC visualization for dataset:', datasetId);
                    const chartDiv = document.getElementById('ohlc-chart');
                    
                    // Mock Plotly chart creation
                    const mockData = [
                        {
                            x: [1, 2, 3, 4, 5],
                            open: [150.0, 151.0, 152.0, 151.5, 153.0],
                            high: [151.5, 152.5, 153.5, 152.5, 154.0],
                            low: [149.5, 150.5, 151.5, 150.8, 152.0],
                            close: [151.0, 152.0, 151.5, 153.0, 153.5],
                            type: 'candlestick',
                            name: 'OHLC'
                        },
                        {
                            x: [1, 2, 3, 4, 5],
                            y: [152.0, 153.0, 154.0, 153.5, 155.0],
                            type: 'scatter',
                            name: 'Envelope Top (etop)'
                        },
                        {
                            x: [1, 2, 3, 4, 5], 
                            y: [149.0, 150.0, 150.5, 149.8, 151.0],
                            type: 'scatter',
                            name: 'Envelope Bottom (ebot)'
                        }
                    ];
                    
                    Plotly.newPlot(chartDiv, mockData, {title: 'OHLC with Technical Indicators'});
                }
                
                function loadTrainingDataTable(datasetId, page) {
                    console.log('Loading training data table for dataset:', datasetId);
                    const contentDiv = document.getElementById('training-data-content');
                    
                    // Mock table creation
                    contentDiv.innerHTML = `
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>Sequence ID</th>
                                    <th>Technical Indicators</th>
                                    <th>OHLC Data</th>
                                    <th>Labels</th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr>
                                    <td>1</td>
                                    <td>
                                        <div class="feature-item"><strong>etop:</strong> 151.2500</div>
                                        <div class="feature-item"><strong>ebot:</strong> 148.5000</div>
                                        <div class="feature-item"><strong>pldot:</strong> 149.7500</div>
                                    </td>
                                    <td>
                                        <div class="feature-item"><strong>5m_high:</strong> 150.2500</div>
                                        <div class="feature-item"><strong>5m_low:</strong> 148.7500</div>
                                        <div class="feature-item"><strong>5m_close:</strong> 149.5000</div>
                                    </td>
                                    <td>
                                        <div class="label-item"><strong>target_return:</strong> 0.0125</div>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    `;
                }
                
                function randomSample() {
                    console.log('Getting random sample');
                    const select = document.getElementById('training-dataset-select');
                    if (select.value) {
                        updateOHLCVisualization(select.value, Math.floor(Math.random() * 100));
                    }
                }
                
                // Tab switching
                document.addEventListener('DOMContentLoaded', function() {
                    const tabs = document.querySelectorAll('.tab');
                    tabs.forEach(tab => {
                        tab.addEventListener('click', function() {
                            tabs.forEach(t => t.classList.remove('active'));
                            this.classList.add('active');
                            
                            if (this.getAttribute('data-tab') === 'training') {
                                document.getElementById('training-dataset-section').classList.remove('hidden');
                            } else {
                                document.getElementById('training-dataset-section').classList.add('hidden');
                            }
                        });
                    });
                    
                    // Dataset selection handler
                    const select = document.getElementById('training-dataset-select');
                    select.addEventListener('change', function() {
                        if (this.value) {
                            updateOHLCVisualization(this.value, 0);
                            loadTrainingDataTable(this.value, 1);
                        }
                    });
                });
            </script>
        </body>
        </html>
        '''
    
    def _generate_mock_training_eda_html(self) -> str:
        """Generate mock training EDA HTML page"""
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Training Dataset EDA</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                .section { padding: 20px; margin: 10px; border: 1px solid #ddd; }
                .data-table { width: 100%; border-collapse: collapse; }
                .data-table th, .data-table td { border: 1px solid #ddd; padding: 8px; }
                .feature-item, .label-item { margin: 2px 0; font-size: 12px; }
                .hidden { display: none; }
            </style>
        </head>
        <body>
            <h1>Training Dataset EDA</h1>
            <div id="dataset-selector">
                <select id="training-dataset-select">
                    <option value="">-- Select Dataset --</option>
                    <option value="15">AAPL Hourly Training Dataset</option>
                    <option value="16">MSFT Hourly Training Dataset</option>
                    <option value="17">GOOGL Training Dataset</option>
                </select>
            </div>
            <div id="ohlc-visualization" class="section">
                <div id="ohlc-chart" style="width: 100%; height: 500px;"></div>
                <button onclick="updateOHLCVisualization()">Refresh Visualization</button>
            </div>
            <div id="training-data-table" class="section">
                <div id="training-data-content"></div>
            </div>
            <script>
                function updateOHLCVisualization() {
                    const select = document.getElementById('training-dataset-select');
                    if (select.value) {
                        const chartDiv = document.getElementById('ohlc-chart');
                        const mockData = [{
                            x: [1, 2, 3], open: [150, 151, 152], 
                            high: [151, 152, 153], low: [149, 150, 151], 
                            close: [151, 152, 153], type: 'candlestick', name: 'OHLC'
                        }];
                        Plotly.newPlot(chartDiv, mockData);
                    }
                }
                
                function loadTrainingDataTable() {
                    const contentDiv = document.getElementById('training-data-content');
                    contentDiv.innerHTML = '<table class="data-table"><tbody><tr><td>Mock Table Data</td></tr></tbody></table>';
                }
                
                document.getElementById('training-dataset-select').addEventListener('change', function() {
                    if (this.value) {
                        updateOHLCVisualization();
                        loadTrainingDataTable();
                    }
                });
            </script>
        </body>
        </html>
        '''
    
    def log_message(self, format, *args):
        """Override to reduce logging noise"""
        pass


class MockAPIServer:
    """Mock API server for testing"""
    
    def __init__(self, port: int = 3001):
        self.port = port
        self.server = None
        self.thread = None
    
    def start(self):
        """Start the mock server in a background thread"""
        self.server = HTTPServer(('localhost', self.port), MockTrainingDataAPI)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
        
        # Wait a moment for server to start
        time.sleep(0.5)
        print(f"Mock API server started on http://localhost:{self.port}")
    
    def stop(self):
        """Stop the mock server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.thread:
            self.thread.join()
        print("Mock API server stopped")
    
    def is_running(self) -> bool:
        """Check if server is running"""
        return self.thread and self.thread.is_alive()


if __name__ == "__main__":
    # Run mock server standalone for testing
    server = MockAPIServer(3001)
    try:
        server.start()
        print("Mock server running at http://localhost:3001")
        print("Test endpoints:")
        print("  GET /health")
        print("  GET /api/v1/training-datasets/")
        print("  GET /api/v1/training-datasets/15/data")
        print("  GET /api/v1/training-datasets/15/visualization-data")
        print("  GET /eda")
        print("  GET /training-eda")
        print("\nPress Ctrl+C to stop")
        
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
        print("\nServer stopped")