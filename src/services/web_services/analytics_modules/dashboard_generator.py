"""
EDA dashboard HTML generation and visualization
"""

#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings


    # ==============================================
    # WEB DASHBOARD SERVING (from analytics_service.py)
    # ==============================================

    def get_eda_dashboard_html(self):
        """Generate the main EDA dashboard HTML."""
        return """<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>ATS Unified Analytics - EDA Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
                .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                .unified-badge { background: #e74c3c; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; }
                .feature-list { display: flex; gap: 15px; margin: 10px 0; }
                .feature-item { background: #3498db; color: white; padding: 5px 10px; border-radius: 15px; font-size: 0.9em; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 ATS Unified Analytics Dashboard <span class="unified-badge">CONSOLIDATED</span></h1>
                <p>Consolidated analytics service with type-aware analysis, universe analytics, and distributed computing</p>
                <div class="feature-list">
                    <div class="feature-item">📊 Type-Aware EDA</div>
                    <div class="feature-item">🌐 Universe Analytics</div>
                    <div class="feature-item">⚡ Ray Computing</div>
                    <div class="feature-item">🤖 Training Datasets</div>
                    <div class="feature-item">📈 Real-time Quality</div>
                </div>
            </div>

            <div class="main-content">
                <h2>Select Analysis Type</h2>
                <button onclick="loadEDA()">📊 Exploratory Data Analysis</button>
                <button onclick="loadBarCollectionMetrics()">📈 Bar Collection Metrics</button>
                <button onclick="loadUniverseAnalytics()">🌐 Universe Analytics</button>
                <button onclick="loadTrainingDatasets()">🤖 Training Datasets</button>
                <button onclick="loadNewsEvents()">📰 News Events</button>
                <button onclick="loadEarningsEvents()">📊 Earnings Events</button>
                <button onclick="loadMultiPanelVisualization()">🎨 Multi-Panel Trading Charts</button>
                <button onclick="loadRayAnalytics()">⚡ Distributed Analytics</button>

                <div id="analysis-content">
                    <p style="text-align: center; margin-top: 50px; color: #666;">
                        Select an analysis type above to begin
                    </p>
                </div>
            </div>

            <script>
                async function loadEDA() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>📊 Exploratory Data Analysis</h3>
                        <p>Loading database tables...</p>
                    `;

                    try {
                        // First get list of available tables
                        const tablesResponse = await fetch('/api/tables');
                        let tables = [];

                        if (tablesResponse.ok) {
                            const tablesData = await tablesResponse.json();
                            tables = tablesData.tables || [];
                        } else {
                            // Fallback to common financial tables
                            tables = [
                                'dev_daily_prices', 'dev_training_datasets', 'dev_instruments',
                                'dev_daily_prices_polygon', 'dev_daily_prices_tiingo', 'dev_daily_prices_eodhd'
                            ];
                        }

                        const html = `
                            <h3>📊 Exploratory Data Analysis</h3>
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>Select Table</h4>
                                <select id="table-selector" onchange="loadTableData()" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    <option value="">Choose a table...</option>
                                    ${tables.map(table => `<option value="${table}">${table}</option>`).join('')}
                                </select>
                            </div>

                            <div id="table-content" style="display: none;">
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>📈 Table Info</h4>
                                        <div id="table-info">Select a table to view information</div>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>📊 Column Summary</h4>
                                        <div id="column-summary">Select a table to view columns</div>
                                    </div>
                                </div>

                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>📋 Sample Data</h4>
                                    <div id="sample-data" style="max-height: 400px; overflow: auto;">
                                        <p>Select a table to view sample data</p>
                                    </div>
                                </div>

                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📈 Column Distributions</h4>
                                    <div id="column-distributions">
                                        <p>Select a table to view column distributions and statistics</p>
                                    </div>
                                </div>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📊 EDA</h3><p style="color: red;">Error loading EDA interface: ' + error.message + '</p>';
                    }
                }

                async function loadTableData() {
                    const tableName = document.getElementById('table-selector').value;
                    if (!tableName) {
                        document.getElementById('table-content').style.display = 'none';
                        return;
                    }

                    document.getElementById('table-content').style.display = 'block';
                    document.getElementById('table-info').innerHTML = '<p>Loading table information...</p>';
                    document.getElementById('column-summary').innerHTML = '<p>Loading column information...</p>';
                    document.getElementById('sample-data').innerHTML = '<p>Loading sample data...</p>';
                    document.getElementById('column-distributions').innerHTML = '<p>Loading distributions...</p>';

                    try {
                        // Load table info
                        const infoResponse = await fetch(`/api/table-info/${tableName}`);
                        if (infoResponse.ok) {
                            const info = await infoResponse.json();
                            document.getElementById('table-info').innerHTML = `
                                <p><strong>Row Count:</strong> ${info.row_count}</p>
                                <p><strong>Column Count:</strong> ${info.column_count}</p>
                                <p><strong>Table Size:</strong> ${info.size}</p>
                                <p><strong>Last Updated:</strong> ${info.last_updated || 'Unknown'}</p>
                            `;
                        }

                        // Load column info
                        const columnsResponse = await fetch(`/api/table-columns/${tableName}`);
                        if (columnsResponse.ok) {
                            const columns = await columnsResponse.json();
                            const columnHtml = columns.columns.map(col => `
                                <div style="margin: 5px 0; padding: 5px; background: #f8f9fa; border-radius: 3px;">
                                    <strong>${col.name}</strong> (${col.type})
                                    ${col.nullable ? '' : ' <em>NOT NULL</em>'}
                                </div>
                            `).join('');
                            document.getElementById('column-summary').innerHTML = columnHtml;
                        }

                        // Load sample data
                        const sampleResponse = await fetch(`/api/table-sample/${tableName}`);
                        if (sampleResponse.ok) {
                            const sample = await sampleResponse.json();
                            if (sample.rows && sample.rows.length > 0) {
                                const headers = Object.keys(sample.rows[0]);
                                const tableHtml = `
                                    <table style="width: 100%; border-collapse: collapse; font-size: 0.9em;">
                                        <thead>
                                            <tr style="background: #f1f3f4;">
                                                ${headers.map(h => `<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">${h}</th>`).join('')}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            ${sample.rows.slice(0, 10).map(row => `
                                                <tr>
                                                    ${headers.map(h => `<td style="padding: 8px; border: 1px solid #ddd;">${row[h] !== null ? row[h] : '<em>null</em>'}</td>`).join('')}
                                                </tr>
                                            `).join('')}
                                        </tbody>
                                    </table>
                                `;
                                document.getElementById('sample-data').innerHTML = tableHtml;
                            } else {
                                document.getElementById('sample-data').innerHTML = '<p>No data found in table</p>';
                            }
                        }

                        // Load column distributions
                        const distResponse = await fetch(`/api/table-distributions/${tableName}`);
                        if (distResponse.ok) {
                            const distributions = await distResponse.json();
                            let distHtml = '';

                            for (const [colName, stats] of Object.entries(distributions.columns || {})) {
                                distHtml += `
                                    <div style="margin: 15px 0; padding: 15px; background: #f8f9fa; border-radius: 5px;">
                                        <h5>${colName}</h5>
                                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 10px; margin: 10px 0;">
                                            <div><strong>Count:</strong> ${stats.count || 0}</div>
                                            <div><strong>Unique:</strong> ${stats.unique || 0}</div>
                                            <div><strong>Nulls:</strong> ${stats.nulls || 0}</div>
                                            <div><strong>Type:</strong> ${stats.type || 'unknown'}</div>
                                        </div>
                                        ${stats.min !== undefined ? `<div><strong>Min:</strong> ${stats.min} <strong>Max:</strong> ${stats.max}</div>` : ''}
                                        ${stats.top_values ? `<div><strong>Top Values:</strong> ${stats.top_values.slice(0, 5).join(', ')}</div>` : ''}
                                    </div>
                                `;
                            }

                            document.getElementById('column-distributions').innerHTML = distHtml || '<p>No distribution data available</p>';
                        }

                    } catch (error) {
                        document.getElementById('table-info').innerHTML = '<p style="color: red;">Error: ' + error.message + '</p>';
                    }
                }

                async function loadBarCollectionMetrics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>📈 Bar Collection Metrics</h3><p>Loading bar collection data...</p>';

                    try {
                        const response = await fetch('/api/bar-collection-metrics');
                        const data = await response.json();

                        if (data.error) {
                            document.getElementById('analysis-content').innerHTML =
                                `<h3>📈 Bar Collection Metrics</h3><p style="color: red;">Error: ${data.error}</p>`;
                            return;
                        }

                        const summary = data.summary || {};
                        const metrics = data.metrics || {};

                        let html = `
                            <h3>📈 Bar Collection Metrics</h3>
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>📊 Overall Summary (Last 7 Days)</h4>
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                    <div style="text-align: center; padding: 10px; background: #e3f2fd; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #1976d2;">${summary.total_bars_collected?.toLocaleString() || 0}</div>
                                        <div style="font-weight: bold;">Total Bars</div>
                                    </div>
                                    <div style="text-align: center; padding: 10px; background: #e8f5e8; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #388e3c;">${summary.total_unique_symbols || 0}</div>
                                        <div style="font-weight: bold;">Unique Symbols</div>
                                    </div>
                                    <div style="text-align: center; padding: 10px; background: #fff3e0; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #f57c00;">${summary.active_vendors || 0}</div>
                                        <div style="font-weight: bold;">Active Vendors</div>
                                    </div>
                                    <div style="text-align: center; padding: 10px; background: #fce4ec; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #c2185b;">${summary.avg_latency_ms?.toFixed(0) || 0}ms</div>
                                        <div style="font-weight: bold;">Avg Latency</div>
                                    </div>
                                </div>
                            </div>
                        `;

                        // Per-vendor metrics
                        for (const [tableName, vendorData] of Object.entries(metrics)) {
                            if (vendorData.error) {
                                html += `
                                    <div style="background: #ffebee; padding: 15px; border-radius: 8px; border: 1px solid #ef5350; margin-bottom: 15px;">
                                        <h4 style="color: #c62828;">🚨 ${vendorData.vendor} - Error</h4>
                                        <p>${vendorData.error}</p>
                                    </div>
                                `;
                                continue;
                            }

                            const stats = vendorData.overall_stats || {};
                            const collectionData = vendorData.collection_time_metrics || [];
                            const barData = vendorData.bar_time_metrics || [];

                            html += `
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>📊 ${vendorData.vendor} Metrics</h4>

                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                        <!-- Collection Time Chart -->
                                        <div>
                                            <h5>📅 Bars by Collection Time (Last 24h)</h5>
                                            <div style="background: #f9f9f9; padding: 10px; border-radius: 4px; max-height: 200px; overflow-y: auto;">
                                                <table style="width: 100%; font-size: 0.9em;">
                                                    <thead>
                                                        <tr style="background: #e0e0e0;">
                                                            <th style="padding: 5px; text-align: left;">Collection Hour</th>
                                                            <th style="padding: 5px; text-align: right;">Bars</th>
                                                            <th style="padding: 5px; text-align: right;">Symbols</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        ${collectionData.map(row => `
                                                            <tr>
                                                                <td style="padding: 3px;">${new Date(row.collection_hour).toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.bars_collected?.toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.unique_symbols}</td>
                                                            </tr>
                                                        `).join('')}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>

                                        <!-- Bar Time Chart -->
                                        <div>
                                            <h5>⏰ Bars by Bar Time (Last 24h)</h5>
                                            <div style="background: #f9f9f9; padding: 10px; border-radius: 4px; max-height: 200px; overflow-y: auto;">
                                                <table style="width: 100%; font-size: 0.9em;">
                                                    <thead>
                                                        <tr style="background: #e0e0e0;">
                                                            <th style="padding: 5px; text-align: left;">Bar Hour</th>
                                                            <th style="padding: 5px; text-align: right;">Bars</th>
                                                            <th style="padding: 5px; text-align: right;">Avg Vol</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        ${barData.map(row => `
                                                            <tr>
                                                                <td style="padding: 3px;">${new Date(row.bar_hour).toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.bars_count?.toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.avg_volume ? Math.round(row.avg_volume).toLocaleString() : 'N/A'}</td>
                                                            </tr>
                                                        `).join('')}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>

                                    <!-- Vendor Stats -->
                                    <div style="background: #f5f5f5; padding: 10px; border-radius: 4px;">
                                        <strong>7-Day Stats:</strong>
                                        Total Bars: <strong>${stats.total_bars?.toLocaleString() || 0}</strong> |
                                        Symbols: <strong>${stats.total_symbols || 0}</strong> |
                                        Avg Quality: <strong>${stats.avg_quality_score?.toFixed(2) || 'N/A'}</strong> |
                                        Avg Latency: <strong>${stats.avg_latency_ms?.toFixed(0) || 0}ms</strong>
                                    </div>
                                </div>
                            `;
                        }

                        html += `
                            <div style="background: #f0f0f0; padding: 10px; border-radius: 4px; font-size: 0.9em; color: #666;">
                                <strong>Last Updated:</strong> ${new Date(data.timestamp).toLocaleString()}
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        console.error('Error loading bar collection metrics:', error);
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📈 Bar Collection Metrics</h3><p style="color: red;">Error loading metrics. Check console for details.</p>';
                    }
                }

                async function loadUniverseAnalytics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🌐 Universe Analytics</h3><p>Loading cross-instrument analysis...</p>';

                    try {
                        const response = await fetch('/api/universe-analytics');
                        const data = await response.json();

                        const html = `
                            <h3>🌐 Universe Analytics</h3>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 20px;">
                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📊 Composition</h4>
                                    <p><strong>Total Instruments:</strong> ${data.composition.total_instruments}</p>
                                    <p><strong>By Exchange:</strong> ${JSON.stringify(data.composition.by_exchange)}</p>
                                    <p><strong>By Sector:</strong> ${JSON.stringify(data.composition.by_sector)}</p>
                                </div>
                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📈 Performance</h4>
                                    <p><strong>Sharpe Ratio:</strong> ${data.performance.sharpe_ratio}</p>
                                    <p><strong>Max Drawdown:</strong> ${data.performance.max_drawdown}</p>
                                    <p><strong>Analysis Time:</strong> ${data.analysis_timestamp}</p>
                                </div>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-top: 20px;">
                                <h4>🔗 Correlations</h4>
                                <p><strong>Universe:</strong> ${data.universe_name}</p>
                                <p><em>Note: This is a demonstration of the universe analytics API. In a full implementation,
                                this would show correlation matrices, sector analysis, and interactive charts.</em></p>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🌐 Universe Analytics</h3><p style="color: red;">Error loading universe analytics: ' + error.message + '</p>';
                    }
                }

                async function loadNewsEvents() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>📰 News Events</h3><p>Loading news events from Polygon and Tiingo...</p>';

                    try {
                        // Fetch news events
                        const response = await fetch('/api/news-events?limit=50');
                        const data = await response.json();

                        let html = ''; // Declare html at function level to avoid scoping issues

                        if (data.success && data.events) {
                            html = `
                                <h3>📰 News Events Analysis</h3>
                                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #007bff;">Total Events</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.total_events}</div>
                                    </div>
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #28a745;">Unique Symbols</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.unique_symbols}</div>
                                    </div>
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #6f42c1;">Sources</h4>
                                        <div style="font-size: 16px; color: #333;">${Object.keys(data.sources || {}).join(', ')}</div>
                                    </div>
                                </div>

                                <div style="background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;">
                                    <div style="background: #007bff; color: white; padding: 15px;">
                                        <h4 style="margin: 0;">📰 Recent News Events</h4>
                                    </div>
                                    <div style="max-height: 600px; overflow-y: auto;">
                                        <table style="width: 100%; border-collapse: collapse;">
                                            <thead style="background: #f8f9fa; position: sticky; top: 0;">
                                                <tr>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Title</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Source</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Symbols</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Published</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                            `;

                            data.events.forEach((event, index) => {
                                const publishedDate = event.published_at ? new Date(event.published_at).toLocaleString() : 'N/A';
                                const symbols = (event.symbols || []).slice(0, 3).join(', ') + (event.symbols && event.symbols.length > 3 ? '...' : '');
                                const backgroundColor = index % 2 === 0 ? 'white' : '#f8f9fa';

                                html += `
                                    <tr style="background: ${backgroundColor};">
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">
                                            <div style="font-weight: bold; margin-bottom: 4px;">${event.title || 'Untitled'}</div>
                                            <div style="font-size: 12px; color: #666; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                                                ${event.description || 'No description'}
                                            </div>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">
                                            <span style="background: #007bff; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px;">
                                                ${event.source}
                                            </span>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; font-size: 12px;">
                                            ${symbols || 'N/A'}
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; font-size: 12px;">
                                            ${publishedDate}
                                        </td>
                                    </tr>
                                `;
                            });

                            html += `
                                            </tbody>
                                        </table>
                                    </div>
                                </div>

                                <div style="margin-top: 20px; padding: 15px; background: #e9ecef; border-radius: 8px;">
                                    <h5>📊 Sources Breakdown:</h5>
                                    <div style="display: flex; gap: 20px;">
                            `;

                            Object.entries(data.sources || {}).forEach(([source, count]) => {
                                html += `<div><strong>${source}:</strong> ${count} events</div>`;
                            });

                            html += `
                                    </div>
                                </div>
                            `;

                        } else {
                            html = `
                                <h3>📰 News Events</h3>
                                <div style="text-align: center; padding: 40px;">
                                    <p>No news events available or error occurred.</p>
                                    ${data.error ? `<p style="color: red;">Error: ${data.error}</p>` : ''}
                                </div>
                            `;
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📰 News Events</h3><p style="color: red;">Error loading news events: ' + error.message + '</p>';
                    }
                }

                async function loadEarningsEvents() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>📊 Earnings Events</h3><p>Loading earnings events data...</p>';

                    try {
                        // Fetch earnings events
                        const response = await fetch('/api/earnings-events?limit=50');
                        const data = await response.json();

                        let html = '';

                        if (data.success && data.events) {
                            html = `
                                <h3>📊 Earnings Events Analysis</h3>

                                <!-- Summary Cards -->
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px;">
                                    <div style="background: #e3f2fd; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #1976d2;">Total Events</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.total_events}</div>
                                    </div>
                                    <div style="background: #e8f5e8; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #388e3c;">EPS Beats</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.summary.eps_beats}</div>
                                        <div style="font-size: 12px; color: #666;">vs ${data.summary.eps_misses} misses</div>
                                    </div>
                                    <div style="background: #fff3e0; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #f57c00;">Revenue Beats</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.summary.revenue_beats}</div>
                                        <div style="font-size: 12px; color: #666;">vs ${data.summary.revenue_misses} misses</div>
                                    </div>
                                    <div style="background: #f3e5f5; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #7b1fa2;">Guidance Changes</h4>
                                        <div style="font-size: 16px; color: #333;">
                                            <span style="color: #4caf50;">↑${data.summary.guidance_raised}</span> |
                                            <span style="color: #f44336;">↓${data.summary.guidance_lowered}</span>
                                        </div>
                                    </div>
                                    <div style="background: #fce4ec; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #c2185b;">Unique Symbols</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.unique_symbols}</div>
                                    </div>
                                </div>

                                <!-- Earnings Events Table -->
                                <div style="background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;">
                                    <div style="background: #673ab7; color: white; padding: 15px;">
                                        <h4 style="margin: 0;">📊 Recent Earnings Events</h4>
                                    </div>
                                    <div style="max-height: 600px; overflow-y: auto;">
                                        <table style="width: 100%; border-collapse: collapse;">
                                            <thead style="background: #f8f9fa; position: sticky; top: 0;">
                                                <tr>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Symbol</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Period</th>
                                                    <th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">EPS</th>
                                                    <th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">Revenue (M)</th>
                                                    <th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">Beats</th>
                                                    <th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">Guidance</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                            `;

                            data.events.forEach((event, index) => {
                                const backgroundColor = index % 2 === 0 ? 'white' : '#f8f9fa';
                                const reportDate = event.report_period ? event.report_period : 'N/A';

                                // Format EPS data
                                const epsActual = event.eps_actual !== null ? `$${event.eps_actual}` : 'N/A';
                                const epsEstimated = event.eps_estimated !== null ? `$${event.eps_estimated}` : 'N/A';
                                const epsSurprise = event.eps_surprise_pct !== null ? `${event.eps_surprise_pct.toFixed(1)}%` : 'N/A';

                                // Format Revenue data
                                const revenueActual = event.revenue_actual_millions !== null ? `$${event.revenue_actual_millions}M` : 'N/A';
                                const revenueEstimated = event.revenue_estimated_millions !== null ? `$${event.revenue_estimated_millions}M` : 'N/A';
                                const revenueSurprise = event.revenue_surprise_pct !== null ? `${event.revenue_surprise_pct.toFixed(1)}%` : 'N/A';

                                // Beat/miss indicators
                                const epsBeat = event.earnings_beat === true ? '✅' : event.earnings_beat === false ? '❌' : '❓';
                                const revenueBeat = event.revenue_beat === true ? '✅' : event.revenue_beat === false ? '❌' : '❓';

                                // Guidance indicators
                                let guidanceIndicator = '➖';
                                if (event.guidance_raised === true) guidanceIndicator = '📈';
                                else if (event.guidance_lowered === true) guidanceIndicator = '📉';

                                html += `
                                    <tr style="background: ${backgroundColor};">
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">
                                            <div style="font-weight: bold; color: #333;">${event.symbol}</div>
                                            <div style="font-size: 12px; color: #666;">${event.report_type}</div>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; font-size: 14px;">
                                            ${reportDate}
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center;">
                                            <div style="font-weight: bold;">${epsActual}</div>
                                            <div style="font-size: 12px; color: #666;">est: ${epsEstimated}</div>
                                            <div style="font-size: 12px; color: ${event.eps_surprise_pct > 0 ? '#4caf50' : '#f44336'};">${epsSurprise}</div>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center;">
                                            <div style="font-weight: bold;">${revenueActual}</div>
                                            <div style="font-size: 12px; color: #666;">est: ${revenueEstimated}</div>
                                            <div style="font-size: 12px; color: ${event.revenue_surprise_pct > 0 ? '#4caf50' : '#f44336'};">${revenueSurprise}</div>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center;">
                                            <div>EPS: ${epsBeat}</div>
                                            <div>Rev: ${revenueBeat}</div>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center; font-size: 20px;">
                                            ${guidanceIndicator}
                                        </td>
                                    </tr>
                                `;
                            });

                            html += `
                                            </tbody>
                                        </table>
                                    </div>
                                </div>

                                <div style="margin-top: 20px; padding: 15px; background: #e9ecef; border-radius: 8px;">
                                    <h5>📈 Performance Summary:</h5>
                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                                        <div>
                                            <strong>EPS Performance:</strong><br>
                                            Beats: ${data.summary.eps_beats} | Misses: ${data.summary.eps_misses}<br>
                                            Success Rate: ${data.summary.eps_beats + data.summary.eps_misses > 0 ?
                                                Math.round(data.summary.eps_beats / (data.summary.eps_beats + data.summary.eps_misses) * 100) : 0}%
                                        </div>
                                        <div>
                                            <strong>Revenue Performance:</strong><br>
                                            Beats: ${data.summary.revenue_beats} | Misses: ${data.summary.revenue_misses}<br>
                                            Success Rate: ${data.summary.revenue_beats + data.summary.revenue_misses > 0 ?
                                                Math.round(data.summary.revenue_beats / (data.summary.revenue_beats + data.summary.revenue_misses) * 100) : 0}%
                                        </div>
                                    </div>
                                </div>
                            `;

                        } else {
                            html = `
                                <h3>📊 Earnings Events</h3>
                                <div style="text-align: center; padding: 40px;">
                                    <p>No earnings events available or error occurred.</p>
                                    ${data.error ? `<p style="color: red;">Error: ${data.error}</p>` : ''}
                                </div>
                            `;
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📊 Earnings Events</h3><p style="color: red;">Error loading earnings events: ' + error.message + '</p>';
                    }
                }

                async function loadTrainingDatasets() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🤖 Training Datasets</h3><p>Loading ML dataset visualization...</p>';

                    try {
                        console.log('🔍 DATASET DEBUG: Fetching training datasets...');
                        const response = await fetch('/api/v1/training-datasets');
                        console.log('🔍 DATASET DEBUG: Response status:', response.status);

                        if (!response.ok) {
                            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                        }

                        const data = await response.json();
                        console.log('🔍 DATASET DEBUG: Response data:', data);
                        console.log('🔍 DATASET DEBUG: Datasets count:', data.datasets ? data.datasets.length : 0);

                        let html = `
                            <h3>🤖 Training Datasets with OHLC Visualization</h3>
                            <div style="margin-bottom: 20px; padding: 15px; background: #f8f9fa; border-radius: 8px;">
                                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: center;">
                                    <div>
                                        <label for="dataset-selector" style="font-weight: bold;">Select Dataset:</label>
                                        <select id="dataset-selector" onchange="loadSequenceFiles()" style="margin-left: 10px; padding: 5px; border-radius: 4px; border: 1px solid #ccc;">
                                            <option value="">Choose a dataset...</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label for="sequence-selector" style="font-weight: bold;">Select Sequence:</label>
                                        <select id="sequence-selector" style="margin-left: 10px; padding: 5px; border-radius: 4px; border: 1px solid #ccc;" disabled>
                                            <option value="">Choose a sequence...</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label for="row-selector" style="font-weight: bold;">Row Index:</label>
                                        <input type="number" id="row-selector" min="0" max="1000" value="50" style="margin-left: 10px; padding: 5px; width: 80px; border-radius: 4px; border: 1px solid #ccc;">
                                    </div>
                                    <button onclick="loadDatasetVisualization()" style="padding: 8px 15px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                        📊 Visualize
                                    </button>
                                </div>
                            </div>

                            <div id="dataset-visualization" style="display: none;">
                                <!-- Time Navigation Controls -->
                                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4 style="margin: 0 0 15px 0; font-size: 16px;">🎯 Time Navigation</h4>

                                    <!-- Navigation Buttons and Position Display -->
                                    <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 15px;">
                                        <button id="nav-first" onclick="navigateToPosition('first')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">⏪ First</button>
                                        <button id="nav-prev" onclick="navigateDirection('prev')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">⬅️ Prev</button>

                                        <div style="flex: 1; margin: 0 15px;">
                                            <input type="range" id="position-slider" min="0" max="100" value="10"
                                                   style="width: 100%; height: 8px; border-radius: 4px; background: #ddd; outline: none;"
                                                   oninput="navigateToPosition(this.value)" onchange="navigateToPosition(this.value)">
                                        </div>

                                        <button id="nav-next" onclick="navigateDirection('next')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">➡️ Next</button>
                                        <button id="nav-last" onclick="navigateToPosition('last')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">⏩ Last</button>
                                    </div>

                                    <!-- Position Info -->
                                    <div style="display: flex; justify-content: space-between; font-size: 14px; color: #666;">
                                        <div id="position-info">Position 10 of 101</div>
                                        <div id="date-info">Loading...</div>
                                        <div id="bars-info">21 bars</div>
                                        <div id="loading-status" style="color: #007acc; display: none;">🔄 Loading...</div>
                                    </div>
                                </div>

                                <!-- Multi-Timeframe OHLC Charts Grid -->
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px;">
                                    <!-- 5-Minute Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 5-Minute OHLC</h4>
                                        <div id="ohlc-chart-5m" style="height: 300px;"></div>
                                    </div>

                                    <!-- 15-Minute Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 15-Minute OHLC</h4>
                                        <div id="ohlc-chart-15m" style="height: 300px;"></div>
                                    </div>

                                    <!-- 1-Hour Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 1-Hour OHLC</h4>
                                        <div id="ohlc-chart-1h" style="height: 300px;"></div>
                                    </div>

                                    <!-- Daily Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 Daily OHLC</h4>
                                        <div id="ohlc-chart-1d" style="height: 300px;"></div>
                                    </div>

                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 Weekly OHLC</h4>
                                        <div id="ohlc-chart-1w" style="height: 300px;"></div>
                                    </div>
                                </div>

                                <!-- Dataset Information -->
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4 style="margin-top: 0;">📊 Multi-Timeframe Dataset Information</h4>
                                    <div id="dataset-info"></div>
                                </div>
                                </div>

                                <!-- Sequence Data Table -->
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4 style="margin-top: 0;">📋 Training Sequence Data (±10 bars from selected row)</h4>
                                    <div id="sequence-table" style="overflow-x: auto;"></div>
                                </div>
                            </div>

                            <!-- Available Datasets Summary -->
                            <div style="margin-top: 20px; background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                <h4>📚 Available Datasets Summary (${data.total_count} total)</h4>
                                <div id="datasets-summary"></div>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                        // Populate dataset selector
                        const selector = document.getElementById('dataset-selector');
                        console.log('🔍 DATASET DEBUG: Dataset selector found:', !!selector);

                        if (data.datasets && data.datasets.length > 0) {
                            console.log('🔍 DATASET DEBUG: Populating selector with', data.datasets.length, 'datasets');
                            data.datasets.forEach((dataset, index) => {
                                const option = document.createElement('option');
                                option.value = dataset.id;
                                option.textContent = `[ID: ${dataset.id}] ${dataset.dataset_name} (${dataset.total_sequences} sequences, ${dataset.symbols})`;
                                selector.appendChild(option);
                                console.log(`🔍 DATASET DEBUG: Added dataset ${index + 1}:`, option.textContent);
                            });

                            // Show datasets summary
                            let summaryHtml = `
                                <div style="display: grid; grid-template-columns: auto 2fr 1fr 1fr 1fr; gap: 10px; padding: 10px; background: #f8f9fa; border-bottom: 2px solid #dee2e6; font-weight: bold;">
                                    <div>Dataset ID</div>
                                    <div>Name & Symbols</div>
                                    <div>Sequences</div>
                                    <div>Quality Score</div>
                                    <div>Created</div>
                                </div>
                            `;
                            data.datasets.forEach(dataset => {
                                summaryHtml += `
                                    <div style="display: grid; grid-template-columns: auto 2fr 1fr 1fr 1fr; gap: 10px; padding: 10px; border-bottom: 1px solid #eee;">
                                        <div><strong>ID: ${dataset.id}</strong></div>
                                        <div><strong>${dataset.dataset_name}</strong><br><small>${dataset.symbols}</small></div>
                                        <div>${dataset.total_sequences} sequences</div>
                                        <div>Quality: ${dataset.data_quality_score}</div>
                                        <div><small>${new Date(dataset.creation_timestamp).toLocaleDateString()}</small></div>
                                    </div>
                                `;
                            });
                            document.getElementById('datasets-summary').innerHTML = summaryHtml;
                        } else {
                            console.log('🔍 DATASET DEBUG: No datasets found in response');
                            document.getElementById('datasets-summary').innerHTML = '<p>No training datasets found.</p>';
                        }

                    } catch (error) {
                        console.error('🔍 DATASET DEBUG: Error loading datasets:', error);
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🤖 Training Datasets</h3><p style="color: red;">Error loading training datasets: ' + error.message + '</p>';
                    }
                }

                async function loadSequenceFiles() {
                    const datasetId = document.getElementById('dataset-selector').value;
                    const sequenceSelector = document.getElementById('sequence-selector');

                    if (!datasetId) {
                        sequenceSelector.innerHTML = '<option value="">Choose a sequence...</option>';
                        sequenceSelector.disabled = true;
                        return;
                    }

                    sequenceSelector.innerHTML = '<option value="">Loading sequences...</option>';
                    sequenceSelector.disabled = true;

                    try {
                        const response = await fetch(`/api/v1/training-datasets/${datasetId}/sequences`);
                        const data = await response.json();

                        if (data.sequences && data.sequences.length > 0) {
                            let options = '<option value="">Choose a sequence...</option>';
                            data.sequences.forEach(seq => {
                                const timeframeSummary = seq.timeframes ? seq.timeframes.join(', ') : 'multi-timeframe';
                                options += `<option value="${seq.sequence_id}">${seq.description} (${timeframeSummary}, ${seq.total_size_mb}MB)</option>`;
                            });
                            sequenceSelector.innerHTML = options;
                            sequenceSelector.disabled = false;
                        } else {
                            sequenceSelector.innerHTML = '<option value="">No sequences found</option>';
                            sequenceSelector.disabled = true;
                        }
                    } catch (error) {
                        console.error('Error loading sequence files:', error);
                        sequenceSelector.innerHTML = '<option value="">Error loading sequences</option>';
                        sequenceSelector.disabled = true;
                    }
                }

                async function loadDatasetVisualization() {
                    const datasetId = document.getElementById('dataset-selector').value;
                    const sequenceId = document.getElementById('sequence-selector').value;
                    const rowIndex = document.getElementById('row-selector').value || 0;

                    console.log('🎯 CLIENT DEBUG: Starting visualization load');
                    console.log(`   Dataset ID: ${datasetId}`);
                    console.log(`   Sequence ID: ${sequenceId}`);
                    console.log(`   Row Index: ${rowIndex}`);

                    if (!datasetId) {
                        alert('Please select a dataset first');
                        return;
                    }

                    if (!sequenceId) {
                        alert('Please select a sequence first');
                        return;
                    }

                    // Show loading state
                    document.getElementById('dataset-visualization').style.display = 'block';

                    // Set loading state for all timeframe charts
                    const timeframes = ['5m', '15m', '1h', '1d', '1w'];
                    timeframes.forEach(tf => {
                        document.getElementById(`ohlc-chart-${tf}`).innerHTML = `<p>Loading ${tf} chart...</p>`;
                    });

                    document.getElementById('dataset-info').innerHTML = '<p>Loading dataset info...</p>';
                    document.getElementById('sequence-table').innerHTML = '<p>Loading sequence data...</p>';

                    try {
                        // Use NEW multi-timeframe endpoint with row index parameter
                        const apiUrl = `/api/v1/training-datasets/${datasetId}/sequences/${sequenceId}/multi-timeframe?row_index=${rowIndex}`;
                        console.log(`🌐 CLIENT DEBUG: Fetching from ${apiUrl} (row index: ${rowIndex})`);

                        const response = await fetch(apiUrl);
                        const multiTimeframeData = await response.json();

                        console.log('✅ CLIENT DEBUG: Multi-timeframe data received');
                        console.log(`   Success: ${multiTimeframeData.success}`);
                        console.log(`   Sequence ID: ${multiTimeframeData.sequence_id}`);
                        console.log(`   Available timeframes: ${multiTimeframeData.available_timeframes}`);
                        console.log(`   OHLC data keys: ${Object.keys(multiTimeframeData.ohlc_data || {})}`);
                        console.log(`   Table rows: ${multiTimeframeData.table_data?.length || 0}`);

                        if (multiTimeframeData.error) {
                            throw new Error(multiTimeframeData.error);
                        }

                        if (!multiTimeframeData.success) {
                            throw new Error('Multi-timeframe data fetch failed');
                        }

                        // Display dataset info
                        const symbol = multiTimeframeData.sequence_id ? multiTimeframeData.sequence_id.split('_')[0] : 'UNKNOWN';
                        document.getElementById('dataset-info').innerHTML = `
                            <div style="line-height: 1.6;">
                                <p><strong>Dataset:</strong> ${multiTimeframeData.dataset_name}</p>
                                <p><strong>Symbol:</strong> ${symbol}</p>
                                <p><strong>Sequence ID:</strong> ${multiTimeframeData.sequence_id}</p>
                                <p><strong>Available Timeframes:</strong> ${multiTimeframeData.available_timeframes.join(', ')}</p>
                                <p><strong>Total OHLC Records:</strong> ${Object.values(multiTimeframeData.ohlc_data || {}).reduce((total, data) => total + data.length, 0)}</p>
                            </div>
                        `;

                        console.log('📊 CLIENT DEBUG: Starting Plotly chart creation');

                        // Create OHLC charts for each timeframe
                        for (const timeframe of timeframes) {
                            const chartDiv = document.getElementById('ohlc-chart-' + timeframe);
                            const ohlcData = multiTimeframeData.ohlc_data[timeframe];

                            console.log('📈 CLIENT DEBUG: Processing ' + timeframe + ' chart');
                            console.log('   Data available: ' + !!ohlcData);
                            console.log('   Data length: ' + (ohlcData ? ohlcData.length : 0));

                            if (ohlcData && ohlcData.length > 0) {
                                console.log('   Sample data: ', ohlcData[0]);

                                // Prepare data for Plotly - timestamp is Unix epoch seconds
                                const dates = ohlcData.map(bar => new Date(bar.timestamp * 1000));
                                const opens = ohlcData.map(bar => bar.open);
                                const highs = ohlcData.map(bar => bar.high);
                                const lows = ohlcData.map(bar => bar.low);
                                const closes = ohlcData.map(bar => bar.close);

                                console.log('   Prepared ' + dates.length + ' data points for ' + timeframe);
                                console.log('   Date range: ' + dates[0] + ' to ' + dates[dates.length-1]);

                                const plotlyData = [{
                                    x: dates,
                                    open: opens,
                                    high: highs,
                                    low: lows,
                                    close: closes,
                                    type: 'candlestick',
                                    name: symbol + ' ' + timeframe.toUpperCase(),
                                    increasing: { line: { color: '#00CC88' }},
                                    decreasing: { line: { color: '#FF6B6B' }}
                                }];

                                const layout = {
                                    title: symbol + ' - ' + timeframe.toUpperCase() + ' OHLC',
                                    xaxis: { title: 'Time' },
                                    yaxis: { title: 'Price ($)' },
                                    height: 280,
                                    margin: { t: 40, b: 40, l: 60, r: 20 },
                                    showlegend: false
                                };

                                console.log('🎨 CLIENT DEBUG: Creating ' + timeframe + ' Plotly chart');

                                try {
                                    await Plotly.newPlot(chartDiv, plotlyData, layout, {responsive: true});
                                    console.log('✅ CLIENT DEBUG: ' + timeframe + ' chart created successfully');
                                } catch (plotlyError) {
                                    console.error('❌ CLIENT DEBUG: ' + timeframe + ' Plotly error:', plotlyError);
                                    chartDiv.innerHTML = '<p style="color: red;">Error creating ' + timeframe + ' chart: ' + plotlyError.message + '</p>';
                                }
                            } else {
                                console.log('⚠️  CLIENT DEBUG: No data for ' + timeframe);
                                chartDiv.innerHTML = '<p style="color: orange;">No ' + timeframe + ' data available</p>';
                            }
                        }

                        console.log('📋 CLIENT DEBUG: Creating table view');

                        // Create table view from 1h data
                        const tableData = multiTimeframeData.table_data;
                        if (tableData && tableData.length > 0) {
                            console.log('✅ CLIENT DEBUG: Table data available: ' + tableData.length + ' rows');
                            console.log('   Sample table row:', tableData[0]);

                            let tableHtml = '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">' +
                                '<thead>' +
                                '<tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">' +
                                '<th style="padding: 8px; text-align: left;">Timestamp</th>' +
                                '<th style="padding: 8px; text-align: right;">Open</th>' +
                                '<th style="padding: 8px; text-align: right;">High</th>' +
                                '<th style="padding: 8px; text-align: right;">Low</th>' +
                                '<th style="padding: 8px; text-align: right;">Close</th>' +
                                '<th style="padding: 8px; text-align: right;">Volume</th>' +
                                '</tr>' +
                                '</thead>' +
                                '<tbody>';

                            tableData.forEach((row, idx) => {
                                const date = new Date(row.timestamp * 1000);
                                const bgColor = idx % 2 === 0 ? 'background: #f9f9f9;' : '';
                                tableHtml += '<tr style="border-bottom: 1px solid #eee; ' + bgColor + '">' +
                                    '<td style="padding: 6px;">' + date.toLocaleString() + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.open?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.high?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.low?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.close?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">' + (row.volume?.toLocaleString() || 'N/A') + '</td>' +
                                    '</tr>';
                            });

                            tableHtml += '</tbody></table>';
                            document.getElementById('sequence-table').innerHTML = tableHtml;

                            console.log('✅ CLIENT DEBUG: Table created with ' + tableData.length + ' rows');
                        } else {
                            console.log('⚠️  CLIENT DEBUG: No table data available');
                            document.getElementById('sequence-table').innerHTML = '<p style="color: orange;">No table data available</p>';
                        }

                        console.log('✅ CLIENT DEBUG: Visualization loading completed');

                    } catch (error) {
                        console.error('❌ CLIENT DEBUG: Visualization error:', error);

                        // Set error state for all charts
                        timeframes.forEach(tf => {
                            document.getElementById(`ohlc-chart-${tf}`).innerHTML = `<p style="color: red;">Error loading ${tf} chart: ${error.message}</p>`;
                        });
                        document.getElementById('dataset-info').innerHTML = `<p style="color: red;">Error loading dataset info: ${error.message}</p>`;
                        document.getElementById('sequence-table').innerHTML = `<p style="color: red;">Error loading sequence data: ${error.message}</p>`;
                    }
                }

                function createTimeframeOHLCChart(timeframe, sequenceData) {
                    const data = sequenceData.data;
                    const chartId = `ohlc-chart-${timeframe}`;

                    if (!data || data.length === 0) {
                        document.getElementById(chartId).innerHTML = `<p>No ${timeframe} sequence data available</p>`;
                        return;
                    }

                    // Generate x-axis values (time steps or actual datetime if available)
                    const xValues = data.map((bar, idx) => {
                        // Use datetime if available, otherwise time steps
                        if (bar.datetime) {
                            return new Date(bar.datetime);
                        }
                        return `Step ${idx + 1}`;
                    });

                    // Create OHLC candlestick trace
                    const ohlcTrace = {
                        x: xValues,
                        open: data.map(d => d.open || 0),
                        high: data.map(d => d.high || 0),
                        low: data.map(d => d.low || 0),
                        close: data.map(d => d.close || 0),
                        type: 'candlestick',
                        name: 'OHLC',
                        increasing: {line: {color: '#00c851'}},
                        decreasing: {line: {color: '#ff4444'}},
                        showlegend: false  // Hide legend in individual charts to save space
                    };

                    const traces = [ohlcTrace];

                    // Add envelope_top indicator
                    if (data.some(d => d.envelope_top > 0)) {
                        traces.push({
                            x: xValues,
                            y: data.map(d => d.envelope_top),
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Env Top',
                            line: {color: '#ff9999', width: 1.5, dash: 'dot'},
                            yaxis: 'y',
                            showlegend: false
                        });
                    }

                    // Add envelope_bot indicator
                    if (data.some(d => d.envelope_bot > 0)) {
                        traces.push({
                            x: xValues,
                            y: data.map(d => d.envelope_bot),
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Env Bot',
                            line: {color: '#99ff99', width: 1.5, dash: 'dot'},
                            yaxis: 'y',
                            showlegend: false
                        });
                    }

                    // Add pldot indicator
                    const pldotValues = data.map(d => d.pldot || null);
                    if (pldotValues.some(v => v !== null && v > 0)) {
                        traces.push({
                            x: xValues,
                            y: pldotValues,
                            type: 'scatter',
                            mode: 'markers',
                            name: 'PL Dot',
                            marker: {size: 6, color: '#9999ff'},
                            yaxis: 'y',
                            showlegend: false
                        });
                    }

                    // Chart layout with compact design for grid
                    const layout = {
                        title: {
                            text: `${sequenceData.symbol} - ${timeframe.toUpperCase()}`,
                            font: {size: 14}
                        },
                        xaxis: {
                            title: '',  // No x-axis title to save space
                            type: data[0]?.datetime ? 'date' : 'category',
                            showticklabels: true
                        },
                        yaxis: {
                            title: {
                                text: 'Price',
                                font: {size: 12}
                            },
                            side: 'left'
                        },
                        showlegend: false,  // No legend to save space
                        height: 300,
                        margin: {l: 50, r: 20, t: 30, b: 30}
                    };

                    // Create the plot
                    Plotly.newPlot(chartId, traces, layout, {responsive: true});
                }

                function createSequenceTable(sequenceData) {
                    const data = sequenceData.data;
                    if (!data || data.length === 0) {
                        document.getElementById('sequence-table').innerHTML = '<p>No sequence data available</p>';
                        return;
                    }

                    // Check if datetime features are available
                    const hasDatetimeFeatures = data[0] && data[0].datetime && data[0].datetime !== null;

                    // Create table with all sequence data
                    let tableHtml = `
                        <table style="width: 100%; border-collapse: collapse; font-size: 0.9em;">
                            <thead style="background: #f8f9fa;">
                                <tr>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Step</th>
                                    ${hasDatetimeFeatures ? '<th style="border: 1px solid #ddd; padding: 8px;">DateTime</th>' : ''}
                                    ${hasDatetimeFeatures ? '<th style="border: 1px solid #ddd; padding: 8px;">Hour EDT</th>' : ''}
                                    <th style="border: 1px solid #ddd; padding: 8px;">Open</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">High</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Low</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Close</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Volume</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Envelope Top</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Envelope Bot</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">PL Dot</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;

                    data.forEach((bar, index) => {
                        const isSelectedBar = index === sequenceData.selected_bar;
                        const rowStyle = isSelectedBar ? 'background: #fff3cd; font-weight: bold;' : '';

                        tableHtml += `
                            <tr style="${rowStyle}">
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: center;">${bar.time_step + 1}${isSelectedBar ? ' 🎯' : ''}</td>
                                ${hasDatetimeFeatures ? `<td style="border: 1px solid #ddd; padding: 6px; text-align: center; font-size: 0.8em;">${bar.datetime || 'N/A'}</td>` : ''}
                                ${hasDatetimeFeatures ? `<td style="border: 1px solid #ddd; padding: 6px; text-align: center;">${bar.hour_of_day_edt || 0}</td>` : ''}
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.open.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.high.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.low.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.close.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.volume ? bar.volume.toLocaleString() : '0'}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.envelope_top ? bar.envelope_top.toFixed(2) : '0.00'}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.envelope_bot ? bar.envelope_bot.toFixed(2) : '0.00'}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.pldot ? bar.pldot.toFixed(2) : '0.00'}</td>
                            </tr>
                        `;
                    });

                    tableHtml += '</tbody></table>';

                    // Add summary information
                    const summaryHtml = `
                        <div style="margin-top: 15px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                            <p style="margin: 5px 0;"><strong>Showing ${data.length} time steps</strong> | Selected bar highlighted with 🎯</p>
                            <p style="margin: 5px 0;"><strong>Price Range:</strong> ${Math.min(...data.map(d => d.low)).toFixed(2)} - ${Math.max(...data.map(d => d.high)).toFixed(2)}</p>
                            <p style="margin: 5px 0;"><strong>Technical Indicators:</strong> Envelope Top/Bottom (support/resistance), PL Dot (pivot lows)</p>
                        </div>
                    `;

                    document.getElementById('sequence-table').innerHTML = tableHtml + summaryHtml;
                }

                async function loadMultiPanelVisualization() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>🎨 Multi-Panel Trading Charts</h3>
                        <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                            <h4>Generate Multi-Panel Trading Visualization</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: center; margin-bottom: 15px;">
                                <div>
                                    <label for="symbol-input" style="font-weight: bold;">Symbol:</label>
                                    <input type="text" id="symbol-input" value="AAPL" placeholder="Enter symbol"
                                           style="margin-left: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 100px;">
                                </div>
                                <div>
                                    <label for="timeframe-select" style="font-weight: bold;">Timeframe:</label>
                                    <select id="timeframe-select" style="margin-left: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        <option value="5m">5 Minutes</option>
                                        <option value="15m">15 Minutes</option>
                                        <option value="1h" selected>1 Hour</option>
                                        <option value="1d">1 Day</option>
                                    </select>
                                </div>
                                <div>
                                    <label for="dataset-input" style="font-weight: bold;">Dataset ID:</label>
                                    <input type="number" id="dataset-input" value="1" min="1" placeholder="Dataset ID"
                                           style="margin-left: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 80px;">
                                </div>
                                <button onclick="generateMultiPanelChart()" id="generate-btn"
                                        style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                                    🎨 Generate Chart
                                </button>
                            </div>

                            <div style="background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #007bff;">
                                <h5 style="margin: 0 0 10px 0;">📊 Chart Layout</h5>
                                <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 10px; margin-bottom: 10px;">
                                    <div style="background: #e3f2fd; padding: 8px; border-radius: 4px; text-align: center; font-size: 12px;">
                                        📈 OHLC Chart + Indicator Lines<br>
                                        <small>(envelope top/bot, pldot, z1b, z2b, z5t, z6t)</small>
                                    </div>
                                    <div style="background: #f3e5f5; padding: 8px; border-radius: 4px; text-align: center; font-size: 12px;">
                                        📊 Volume Distribution<br>
                                        <small>(POC, VAH, VAL)</small>
                                    </div>
                                </div>
                                <div style="background: #e8f5e8; padding: 8px; border-radius: 4px; text-align: center; font-size: 12px;">
                                    🔍 BX Trender Indicators<br>
                                    <small>(Basic, Directional, Volume Weighted)</small>
                                </div>
                            </div>
                        </div>

                        <!-- Status Panel -->
                        <div id="status-panel" style="display: none; margin-bottom: 20px;">
                            <div id="status-message"></div>
                        </div>

                        <!-- Features Panel -->
                        <div id="features-panel" style="display: none; background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                            <h4>📋 Extracted Features</h4>
                            <div id="features-grid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px;"></div>
                        </div>

                        <!-- Chart Panel -->
                        <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                            <h4>📈 Multi-Panel Trading Visualization</h4>
                            <div id="chart-container" style="text-align: center; min-height: 400px; padding: 40px;">
                                <h4 style="color: #666;">🎨 Multi-Panel Trading Chart</h4>
                                <p style="color: #888; margin-bottom: 20px;">Configure your analysis above and click "Generate Chart" to create a comprehensive trading visualization</p>
                                <div style="background: #f8f9fa; padding: 20px; border-radius: 6px; display: inline-block; text-align: left;">
                                    <h5 style="margin: 0 0 10px 0;">Features:</h5>
                                    <ul style="margin: 0; color: #666; font-size: 14px;">
                                        <li>📊 OHLC candlesticks with technical indicator lines</li>
                                        <li>📈 Volume profile distribution with key levels</li>
                                        <li>🔍 BX Trender trend strength analysis</li>
                                        <li>🎯 Multi-timeframe support (5m, 15m, 1h, 1d)</li>
                                        <li>⚡ Real-time feature extraction from training datasets</li>
                                    </ul>
                                </div>
                            </div>
                        </div>
                    `;
                }

                async function generateMultiPanelChart() {
                    const symbol = document.getElementById('symbol-input').value.toUpperCase().trim();
                    const timeframe = document.getElementById('timeframe-select').value;
                    const datasetId = document.getElementById('dataset-input').value;

                    if (!symbol || !datasetId) {
                        showStatus('error', 'Please enter both symbol and dataset ID');
                        return;
                    }

                    const generateBtn = document.getElementById('generate-btn');
                    const chartContainer = document.getElementById('chart-container');

                    // Show loading state
                    generateBtn.disabled = true;
                    generateBtn.textContent = '⏳ Generating...';
                    chartContainer.innerHTML = '<div style="text-align: center; padding: 40px;"><h4>⏳ Generating Multi-Panel Chart...</h4><p>Extracting features and creating visualization...</p></div>';
                    showStatus('info', `Generating multi-panel chart for ${symbol} (${timeframe}) from dataset ${datasetId}...`);

                    try {
                        const response = await fetch(`/api/multi-panel-chart?symbol=${symbol}&timeframe=${timeframe}&dataset_id=${datasetId}`);
                        const result = await response.json();

                        if (result.success) {
                            // Display the chart image
                            chartContainer.innerHTML = `
                                <img src="data:image/png;base64,${result.chart_image}"
                                     style="width: 100%; height: auto; border-radius: 6px; border: 2px solid #ddd;"
                                     alt="Multi-Panel Trading Chart">
                                <div style="text-align: center; color: #666; margin-top: 15px; font-size: 14px;">
                                    <strong>${symbol} ${timeframe.toUpperCase()} Multi-Panel Analysis</strong><br>
                                    Generated: ${result.timestamp} | Features: ${result.features_count} | Dataset: ${datasetId}
                                </div>
                            `;

                            // Show extracted features
                            displayFeatures(result.features);
                            showStatus('success', `Multi-panel chart generated successfully! Extracted ${result.features_count} features.`);
                        } else {
                            chartContainer.innerHTML = `<div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 6px; margin: 20px;"><h4>❌ Error</h4><p>${result.error}</p></div>`;
                            showStatus('error', `Failed to generate chart: ${result.error}`);
                        }
                    } catch (error) {
                        chartContainer.innerHTML = `<div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 6px; margin: 20px;"><h4>❌ Network Error</h4><p>Failed to connect to server: ${error.message}</p></div>`;
                        showStatus('error', `Network error: ${error.message}`);
                    } finally {
                        generateBtn.disabled = false;
                        generateBtn.textContent = '🎨 Generate Chart';
                    }
                }

                function displayFeatures(features) {
                    if (!features) return;

                    const featuresGrid = document.getElementById('features-grid');
                    const featuresPanel = document.getElementById('features-panel');

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
                            if (!key.includes('volume_profile')) featureGroups['OHLCV'].push(item);
                            else featureGroups['Volume Profile'].push(item);
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
                            card.style.cssText = 'background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #007bff;';
                            card.innerHTML = `
                                <h5 style="margin: 0 0 10px 0; color: #007bff;">${group} (${items.length})</h5>
                                <ul style="margin: 0; padding-left: 20px; font-size: 12px; font-family: monospace;">
                                    ${items.slice(0, 6).map(item => `<li>${item}</li>`).join('')}
                                    ${items.length > 6 ? `<li style="color: #666;">... and ${items.length - 6} more</li>` : ''}
                                </ul>
                            `;
                            featuresGrid.appendChild(card);
                        }
                    });

                    featuresPanel.style.display = 'block';
                }

                function showStatus(type, message) {
                    const statusPanel = document.getElementById('status-panel');
                    const statusMessage = document.getElementById('status-message');

                    const colors = {
                        'error': '#f8d7da; color: #721c24; border-left: 4px solid #dc3545;',
                        'success': '#d4edda; color: #155724; border-left: 4px solid #28a745;',
                        'info': '#d1ecf1; color: #0c5460; border-left: 4px solid #17a2b8;'
                    };

                    statusMessage.style.cssText = `background: ${colors[type]} padding: 15px; border-radius: 6px;`;
                    statusMessage.innerHTML = `<strong>${type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️'} ${type.toUpperCase()}:</strong> ${message}`;
                    statusPanel.style.display = 'block';

                    // Auto-hide success/info messages
                    if (type !== 'error') {
                        setTimeout(() => {
                            statusPanel.style.display = 'none';
                        }, 5000);
                    }
                }

                function loadRayAnalytics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>⚡ Distributed Analytics</h3><p>Loading Ray distributed computing...</p>';
                    // Implementation would load Ray analytics interface
                }

                // ==============================================
                // TIME NAVIGATION FUNCTIONS
                // ==============================================

                let currentRowIndex = 10;
                let currentDatasetId = null;
                let currentSequenceId = null;
                let isNavigating = false;
                let navigationMetadata = null;

                async function loadNavigationMetadata() {
                    if (!currentDatasetId || !currentSequenceId) return;

                    try {
                        const url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigation-metadata`;
                        const response = await fetch(url);

                        if (!response.ok) throw new Error(`HTTP ${response.status}`);

                        navigationMetadata = await response.json();
                        updateNavigationRanges();

                        console.log('✅ Navigation metadata loaded:', navigationMetadata);

                    } catch (error) {
                        console.error('❌ Failed to load navigation metadata:', error);
                    }
                }

                function updateNavigationRanges() {
                    if (!navigationMetadata) return;

                    const slider = document.getElementById('position-slider');
                    const nav = navigationMetadata.navigation;

                    if (slider) {
                        slider.min = nav.min_row_index;
                        slider.max = nav.max_row_index;
                        slider.value = currentRowIndex;
                    }
                }

                async function navigateToPosition(position) {
                    if (isNavigating || !currentDatasetId || !currentSequenceId) return;

                    try {
                        setNavigationLoadingState(true);

                        let url;
                        if (typeof position === 'string') {
                            // Direction-based navigation
                            if (position === 'first' || position === 'last') {
                                url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigate?direction=${position}`;
                            } else {
                                url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigate?direction=${position}&row_index=${currentRowIndex}`;
                            }
                        } else {
                            // Position-based navigation
                            url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigate?row_index=${position}`;
                        }

                        console.log('🎯 Navigating to:', url);

                        const response = await fetch(url);
                        if (!response.ok) throw new Error(`HTTP ${response.status}`);

                        const data = await response.json();

                        if (data.success) {
                            updateVisualizationFromNavigation(data);
                            console.log('✅ Navigation successful:', data.navigation_context);
                        } else {
                            throw new Error('Navigation was not successful');
                        }

                    } catch (error) {
                        console.error('❌ Navigation failed:', error);
                        showNavigationError(`Navigation failed: ${error.message}`);
                    } finally {
                        setNavigationLoadingState(false);
                    }
                }

                async function navigateDirection(direction) {
                    await navigateToPosition(direction);
                }

                function updateVisualizationFromNavigation(navigationData) {
                    console.log('🔍 CLIENT DEBUG: Navigation data received:', navigationData);

                    const navContext = navigationData.navigation_context;
                    const tableData = navigationData.table_data || [];
                    // The API returns 'ohlc_data' not 'multi_timeframe_data'
                    const multiTimeframeData = navigationData.ohlc_data || navigationData.multi_timeframe_data || {};

                    console.log('🔍 CLIENT DEBUG: Table data count:', tableData.length);
                    console.log('🔍 CLIENT DEBUG: Multi-timeframe keys:', Object.keys(multiTimeframeData));
                    console.log('🔍 CLIENT DEBUG: Navigation context:', navContext);
                    console.log('🔍 CLIENT DEBUG: All response keys:', Object.keys(navigationData));

                    // Update current position
                    currentRowIndex = navContext.current_row_index;
                    console.log('🔍 CLIENT DEBUG: Updated currentRowIndex to:', currentRowIndex);

                    // Update navigation UI
                    updateNavigationDisplay(navContext, tableData);

                    // Update charts with new data
                    if (multiTimeframeData && Object.keys(multiTimeframeData).length > 0) {
                        const timeframes = ['5m', '15m', '1h', '1d', '1w'];
                        timeframes.forEach(tf => {
                            if (multiTimeframeData[tf]) {
                                console.log(`🔍 CLIENT DEBUG: Updating ${tf} chart with ${multiTimeframeData[tf].data?.length || multiTimeframeData[tf].length || 'unknown'} data points`);
                                // Handle both structures: {data: [...]} and direct array
                                const chartData = multiTimeframeData[tf].data ? multiTimeframeData[tf] : {data: multiTimeframeData[tf]};
                                createTimeframeOHLCChart(tf, chartData);
                            } else {
                                console.log(`🔍 CLIENT DEBUG: No ${tf} data available`);
                            }
                        });
                    } else {
                        console.log('🔍 CLIENT DEBUG: No multi-timeframe data to update charts');
                    }

                    // Update table
                    if (tableData.length > 0) {
                        console.log('🔍 CLIENT DEBUG: Updating table with', tableData.length, 'rows');
                        updateSequenceTable(tableData);
                    } else {
                        console.log('🔍 CLIENT DEBUG: No table data to update');
                    }

                    // Update dataset info
                    updateDatasetInfo(navigationData);
                }

                function updateNavigationDisplay(navContext, tableData) {
                    const positionInfo = document.getElementById('position-info');
                    const dateInfo = document.getElementById('date-info');
                    const barsInfo = document.getElementById('bars-info');
                    const slider = document.getElementById('position-slider');

                    if (positionInfo && navigationMetadata) {
                        const totalPositions = navigationMetadata.navigation.total_positions;
                        positionInfo.textContent = `Position ${currentRowIndex} of ${totalPositions}`;
                    }

                    if (dateInfo && navContext.timestamp_range && navContext.timestamp_range.start) {
                        const startDate = new Date(navContext.timestamp_range.start * 1000);
                        dateInfo.textContent = startDate.toLocaleDateString() + ' ' + startDate.toLocaleTimeString();
                    }

                    if (barsInfo) {
                        barsInfo.textContent = `${tableData.length} bars`;
                    }

                    if (slider) {
                        slider.value = currentRowIndex;
                    }
                }

                function updateSequenceTable(tableData) {
                    const tableDiv = document.getElementById('sequence-table');
                    if (!tableDiv || !tableData || tableData.length === 0) {
                        console.log('🔍 CLIENT DEBUG: Cannot update table - missing tableDiv or data');
                        return;
                    }

                    console.log('🔍 CLIENT DEBUG: First row data sample:', tableData[0]);

                    let tableHtml = `
                        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                            <thead>
                                <tr style="background: #f1f3f4;">
                                    <th style="padding: 8px; border: 1px solid #ddd;">Time</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Open</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">High</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Low</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Close</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Volume</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;

                    // Show first 10 rows
                    tableData.slice(0, 10).forEach((row, index) => {
                        const timestamp = new Date(row.timestamp * 1000);
                        const timeStr = timestamp.toLocaleTimeString();

                        if (index === 0) {
                            console.log(`🔍 CLIENT DEBUG: First row - timestamp: ${row.timestamp}, open: ${row.open}, close: ${row.close}`);
                        }

                        tableHtml += `
                            <tr>
                                <td style="padding: 8px; border: 1px solid #ddd;">${timeStr}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.open || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.high || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.low || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.close || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">${parseInt(row.volume || 0).toLocaleString()}</td>
                            </tr>
                        `;
                    });

                    tableHtml += '</tbody></table>';
                    const oldHtml = tableDiv.innerHTML;
                    tableDiv.innerHTML = tableHtml;

                    console.log(`🔍 CLIENT DEBUG: Table updated - HTML changed: ${oldHtml !== tableHtml}`);
                }

                function updateDatasetInfo(navigationData) {
                    const infoDiv = document.getElementById('dataset-info');
                    if (!infoDiv) return;

                    const symbol = navigationData.sequence_id ? navigationData.sequence_id.split('_')[0] : 'UNKNOWN';

                    infoDiv.innerHTML = `
                        <div style="line-height: 1.6;">
                            <p><strong>Dataset:</strong> ${navigationData.dataset_name || 'Loading...'}</p>
                            <p><strong>Symbol:</strong> ${symbol}</p>
                            <p><strong>Position:</strong> ${currentRowIndex}</p>
                            <p><strong>Timeframes:</strong> ${navigationData.available_timeframes ? navigationData.available_timeframes.join(', ') : '5m, 15m, 1h, 1d, 1w'}</p>
                        </div>
                    `;
                }

                function setNavigationLoadingState(loading) {
                    isNavigating = loading;
                    const loadingStatus = document.getElementById('loading-status');
                    const buttons = document.querySelectorAll('#nav-first, #nav-prev, #nav-next, #nav-last');
                    const slider = document.getElementById('position-slider');

                    if (loadingStatus) {
                        loadingStatus.style.display = loading ? 'block' : 'none';
                    }

                    buttons.forEach(btn => {
                        if (btn) btn.disabled = loading;
                    });

                    if (slider) {
                        slider.disabled = loading;
                    }
                }

                function showNavigationError(message) {
                    console.error('Navigation Error:', message);
                    // You could add a toast notification here
                }

                // Override the existing loadDatasetVisualization to integrate navigation
                const originalLoadDatasetVisualization = loadDatasetVisualization;
                loadDatasetVisualization = async function() {
                    // Store current selection for navigation
                    currentDatasetId = document.getElementById('dataset-selector').value;
                    currentSequenceId = document.getElementById('sequence-selector').value;

                    // Call the original function
                    await originalLoadDatasetVisualization();

                    // Load navigation metadata after visualization loads
                    await loadNavigationMetadata();
                };

                // Add keyboard shortcuts for navigation
                document.addEventListener('keydown', function(e) {
                    if (isNavigating || !currentDatasetId || !currentSequenceId) return;

                    // Only handle navigation shortcuts when in training datasets view
                    const datasetVisualization = document.getElementById('dataset-visualization');
                    if (!datasetVisualization || datasetVisualization.style.display === 'none') return;

                    switch(e.key) {
                        case 'ArrowLeft':
                            e.preventDefault();
                            navigateDirection('prev');
                            break;
                        case 'ArrowRight':
                            e.preventDefault();
                            navigateDirection('next');
                            break;
                        case 'Home':
                            e.preventDefault();
                            navigateToPosition('first');
                            break;
                        case 'End':
                            e.preventDefault();
                            navigateToPosition('last');
                            break;
                    }
                });

                console.log('🎮 Time Navigation initialized. Keyboard shortcuts: ← → (prev/next), Home/End (first/last)');

            </script>
        </body>
        </html>
        """

