"""
Dashboard Template Engine for ATS Analytics Platform.

Extracted from analytics_service.py to separate HTML template generation
from business logic and reduce file size from 3,817 to manageable chunks.
"""


class DashboardTemplateEngine:
    """Generates HTML templates for analytics dashboards."""

    def __init__(self):
        pass

    def get_eda_dashboard_html(self) -> str:
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
                <button onclick="loadMultiPanelVisualization()">🎨 Multi-Panel Trading Charts</button>
                <button onclick="loadNewsAnalytics()">📰 News & Signals</button>
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

                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>📊 Statistical Analysis</h4>
                                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px;">
                                        <div>
                                            <h5>Numeric Columns</h5>
                                            <div id="numeric-stats">Select a table to view numeric statistics</div>
                                        </div>
                                        <div>
                                            <h5>Categorical Columns</h5>
                                            <div id="categorical-stats">Select a table to view categorical statistics</div>
                                        </div>
                                    </div>
                                </div>

                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📈 Visualizations</h4>
                                    <div style="margin-bottom: 15px;">
                                        <label for="viz-column">Select Column for Visualization:</label>
                                        <select id="viz-column" style="width: 200px; padding: 5px; margin-left: 10px;">
                                            <option value="">Choose a column...</option>
                                        </select>
                                        <button onclick="generateVisualization()" style="margin-left: 10px; padding: 5px 15px;">Generate Chart</button>
                                    </div>
                                    <div id="visualization-area" style="min-height: 400px; border: 1px solid #eee; border-radius: 4px; padding: 20px;">
                                        <p style="text-align: center; color: #666;">Select a column and click 'Generate Chart' to create visualization</p>
                                    </div>
                                </div>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML = `
                            <h3>📊 Exploratory Data Analysis</h3>
                            <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                <h4>Error Loading Tables</h4>
                                <p>${error.message}</p>
                                <p>Using fallback table list...</p>
                            </div>

                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-top: 20px;">
                                <h4>Select Table (Fallback)</h4>
                                <select id="table-selector" onchange="loadTableData()" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    <option value="">Choose a table...</option>
                                    <option value="dev_daily_prices">dev_daily_prices</option>
                                    <option value="dev_training_datasets">dev_training_datasets</option>
                                    <option value="dev_instruments">dev_instruments</option>
                                    <option value="dev_daily_prices_polygon">dev_daily_prices_polygon</option>
                                    <option value="dev_daily_prices_tiingo">dev_daily_prices_tiingo</option>
                                    <option value="dev_daily_prices_eodhd">dev_daily_prices_eodhd</option>
                                </select>
                            </div>
                        `;
                    }
                }

                async function loadTableData() {
                    const tableName = document.getElementById('table-selector').value;
                    if (!tableName) return;

                    document.getElementById('table-content').style.display = 'block';
                    document.getElementById('table-info').innerHTML = '<p>Loading table information...</p>';
                    document.getElementById('column-summary').innerHTML = '<p>Loading column information...</p>';
                    document.getElementById('sample-data').innerHTML = '<p>Loading sample data...</p>';
                    document.getElementById('numeric-stats').innerHTML = '<p>Loading statistics...</p>';
                    document.getElementById('categorical-stats').innerHTML = '<p>Loading statistics...</p>';

                    try {
                        // Load table info
                        const infoResponse = await fetch(`/api/table_info?table=${tableName}`);
                        if (infoResponse.ok) {
                            const infoData = await infoResponse.json();
                            document.getElementById('table-info').innerHTML = `
                                <strong>Rows:</strong> ${infoData.row_count?.toLocaleString() || 'Unknown'}<br>
                                <strong>Columns:</strong> ${infoData.column_count || 'Unknown'}<br>
                                <strong>Size:</strong> ${infoData.size_mb ? infoData.size_mb + ' MB' : 'Unknown'}
                            `;
                        } else {
                            document.getElementById('table-info').innerHTML = '<p style="color: #666;">Table info not available</p>';
                        }

                        // Load column info
                        const columnsResponse = await fetch(`/api/columns?table=${tableName}`);
                        if (columnsResponse.ok) {
                            const columnsData = await columnsResponse.json();
                            const columns = columnsData.columns || [];

                            // Update visualization column selector
                            const vizSelect = document.getElementById('viz-column');
                            vizSelect.innerHTML = '<option value="">Choose a column...</option>';
                            columns.forEach(col => {
                                vizSelect.innerHTML += `<option value="${col.name}">${col.name} (${col.type})</option>`;
                            });

                            document.getElementById('column-summary').innerHTML = `
                                <div style="max-height: 200px; overflow-y: auto;">
                                    <table style="width: 100%; font-size: 0.9em;">
                                        <tr style="background: #f8f9fa; font-weight: bold;">
                                            <td style="padding: 5px; border-bottom: 1px solid #ddd;">Column</td>
                                            <td style="padding: 5px; border-bottom: 1px solid #ddd;">Type</td>
                                        </tr>
                                        ${columns.map(col => `
                                            <tr>
                                                <td style="padding: 5px; border-bottom: 1px solid #eee;">${col.name}</td>
                                                <td style="padding: 5px; border-bottom: 1px solid #eee;">${col.type}</td>
                                            </tr>
                                        `).join('')}
                                    </table>
                                </div>
                            `;
                        } else {
                            document.getElementById('column-summary').innerHTML = '<p style="color: #666;">Column info not available</p>';
                        }

                        // Load sample data
                        const sampleResponse = await fetch(`/api/sample_data?table=${tableName}&limit=10`);
                        if (sampleResponse.ok) {
                            const sampleData = await sampleResponse.json();
                            const rows = sampleData.rows || [];
                            const columns = sampleData.columns || [];

                            if (rows.length > 0) {
                                document.getElementById('sample-data').innerHTML = `
                                    <table style="width: 100%; font-size: 0.85em; border-collapse: collapse;">
                                        <tr style="background: #f8f9fa; font-weight: bold;">
                                            ${columns.map(col => `<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">${col}</th>`).join('')}
                                        </tr>
                                        ${rows.map(row => `
                                            <tr>
                                                ${columns.map(col => `<td style="padding: 8px; border: 1px solid #eee;">${row[col] || ''}</td>`).join('')}
                                            </tr>
                                        `).join('')}
                                    </table>
                                `;
                            } else {
                                document.getElementById('sample-data').innerHTML = '<p style="color: #666;">No sample data available</p>';
                            }
                        } else {
                            document.getElementById('sample-data').innerHTML = '<p style="color: #666;">Sample data not available</p>';
                        }

                        // Load statistics
                        const statsResponse = await fetch(`/api/table_stats?table=${tableName}`);
                        if (statsResponse.ok) {
                            const statsData = await statsResponse.json();

                            // Numeric statistics
                            const numericStats = statsData.numeric_stats || {};
                            if (Object.keys(numericStats).length > 0) {
                                let numericHtml = '';
                                for (const [column, stats] of Object.entries(numericStats)) {
                                    numericHtml += `
                                        <div style="margin-bottom: 10px; padding: 8px; background: #f8f9fa; border-radius: 4px;">
                                            <strong>${column}</strong><br>
                                            <small>
                                                Mean: ${stats.mean?.toFixed(2) || 'N/A'} |
                                                Std: ${stats.std?.toFixed(2) || 'N/A'} |
                                                Min: ${stats.min || 'N/A'} |
                                                Max: ${stats.max || 'N/A'}
                                            </small>
                                        </div>
                                    `;
                                }
                                document.getElementById('numeric-stats').innerHTML = numericHtml;
                            } else {
                                document.getElementById('numeric-stats').innerHTML = '<p style="color: #666;">No numeric columns found</p>';
                            }

                            // Categorical statistics
                            const categoricalStats = statsData.categorical_stats || {};
                            if (Object.keys(categoricalStats).length > 0) {
                                let categoricalHtml = '';
                                for (const [column, stats] of Object.entries(categoricalStats)) {
                                    categoricalHtml += `
                                        <div style="margin-bottom: 10px; padding: 8px; background: #f8f9fa; border-radius: 4px;">
                                            <strong>${column}</strong><br>
                                            <small>
                                                Unique: ${stats.unique || 'N/A'} |
                                                Most Common: ${stats.top || 'N/A'} (${stats.freq || 'N/A'})
                                            </small>
                                        </div>
                                    `;
                                }
                                document.getElementById('categorical-stats').innerHTML = categoricalHtml;
                            } else {
                                document.getElementById('categorical-stats').innerHTML = '<p style="color: #666;">No categorical columns found</p>';
                            }
                        } else {
                            document.getElementById('numeric-stats').innerHTML = '<p style="color: #666;">Statistics not available</p>';
                            document.getElementById('categorical-stats').innerHTML = '<p style="color: #666;">Statistics not available</p>';
                        }

                    } catch (error) {
                        console.error('Error loading table data:', error);
                        document.getElementById('table-info').innerHTML = '<p style="color: #dc3545;">Error loading table info</p>';
                        document.getElementById('column-summary').innerHTML = '<p style="color: #dc3545;">Error loading columns</p>';
                        document.getElementById('sample-data').innerHTML = '<p style="color: #dc3545;">Error loading sample data</p>';
                    }
                }

                async function generateVisualization() {
                    const tableName = document.getElementById('table-selector').value;
                    const columnName = document.getElementById('viz-column').value;

                    if (!tableName || !columnName) {
                        alert('Please select both a table and column');
                        return;
                    }

                    document.getElementById('visualization-area').innerHTML = '<p>Generating visualization...</p>';

                    try {
                        const response = await fetch(`/api/visualization?table=${tableName}&column=${columnName}`);
                        if (response.ok) {
                            const vizData = await response.json();

                            // Create Plotly visualization
                            const layout = {
                                title: `Distribution of ${columnName} in ${tableName}`,
                                xaxis: { title: columnName },
                                yaxis: { title: 'Frequency' },
                                margin: { t: 50, b: 50, l: 50, r: 50 }
                            };

                            Plotly.newPlot('visualization-area', vizData.data, layout);
                        } else {
                            document.getElementById('visualization-area').innerHTML = '<p style="color: #dc3545;">Error generating visualization</p>';
                        }
                    } catch (error) {
                        console.error('Error generating visualization:', error);
                        document.getElementById('visualization-area').innerHTML = '<p style="color: #dc3545;">Error generating visualization</p>';
                    }
                }

                async function loadBarCollectionMetrics() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>📈 Bar Collection Metrics</h3>
                        <p>Loading metrics...</p>
                    `;

                    try {
                        const response = await fetch('/api/bar_collection_metrics');
                        if (response.ok) {
                            const data = await response.json();

                            const html = `
                                <h3>📈 Bar Collection Metrics</h3>
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px;">
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>📊 Collection Overview</h4>
                                        <p><strong>Total Collections:</strong> ${data.total_collections || 0}</p>
                                        <p><strong>Active Collections:</strong> ${data.active_collections || 0}</p>
                                        <p><strong>Total Symbols:</strong> ${data.total_symbols || 0}</p>
                                        <p><strong>Data Quality Score:</strong> ${(data.quality_score || 0).toFixed(2)}%</p>
                                    </div>

                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>⏰ Temporal Coverage</h4>
                                        <p><strong>Date Range:</strong> ${data.date_range?.start || 'N/A'} to ${data.date_range?.end || 'N/A'}</p>
                                        <p><strong>Days Covered:</strong> ${data.days_covered || 0}</p>
                                        <p><strong>Missing Days:</strong> ${data.missing_days || 0}</p>
                                        <p><strong>Completeness:</strong> ${(data.completeness || 0).toFixed(1)}%</p>
                                    </div>

                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>📈 Volume Analysis</h4>
                                        <p><strong>Avg Daily Volume:</strong> ${(data.avg_volume || 0).toLocaleString()}</p>
                                        <p><strong>High Volume Days:</strong> ${data.high_volume_days || 0}</p>
                                        <p><strong>Low Volume Days:</strong> ${data.low_volume_days || 0}</p>
                                        <p><strong>Volume Trend:</strong> ${data.volume_trend || 'N/A'}</p>
                                    </div>

                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>💹 Price Metrics</h4>
                                        <p><strong>Avg Price Change:</strong> ${(data.avg_price_change || 0).toFixed(2)}%</p>
                                        <p><strong>Volatility:</strong> ${(data.volatility || 0).toFixed(2)}%</p>
                                        <p><strong>Max Drawdown:</strong> ${(data.max_drawdown || 0).toFixed(2)}%</p>
                                        <p><strong>Price Trend:</strong> ${data.price_trend || 'N/A'}</p>
                                    </div>
                                </div>

                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>🎯 Top Performers</h4>
                                    <div id="top-performers" style="max-height: 300px; overflow-y: auto;">
                                        ${(data.top_performers || []).map(symbol => `
                                            <div style="display: flex; justify-content: space-between; padding: 8px; border-bottom: 1px solid #eee;">
                                                <span><strong>${symbol.symbol}</strong></span>
                                                <span style="color: ${symbol.performance >= 0 ? '#28a745' : '#dc3545'};">
                                                    ${symbol.performance >= 0 ? '+' : ''}${(symbol.performance || 0).toFixed(2)}%
                                                </span>
                                            </div>
                                        `).join('')}
                                    </div>
                                </div>

                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📊 Collection Health</h4>
                                    <div style="margin-bottom: 15px;">
                                        <div style="display: flex; justify-content: space-between;">
                                            <span>Data Completeness</span>
                                            <span>${(data.completeness || 0).toFixed(1)}%</span>
                                        </div>
                                        <div style="background: #e9ecef; height: 20px; border-radius: 10px; overflow: hidden; margin-top: 5px;">
                                            <div style="background: #28a745; height: 100%; width: ${data.completeness || 0}%; transition: width 0.3s ease;"></div>
                                        </div>
                                    </div>

                                    <div style="margin-bottom: 15px;">
                                        <div style="display: flex; justify-content: space-between;">
                                            <span>Quality Score</span>
                                            <span>${(data.quality_score || 0).toFixed(1)}%</span>
                                        </div>
                                        <div style="background: #e9ecef; height: 20px; border-radius: 10px; overflow: hidden; margin-top: 5px;">
                                            <div style="background: #007bff; height: 100%; width: ${data.quality_score || 0}%; transition: width 0.3s ease;"></div>
                                        </div>
                                    </div>

                                    <p style="color: #666; font-size: 0.9em; margin-top: 15px;">
                                        Last updated: ${data.last_updated || 'Unknown'}
                                    </p>
                                </div>
                            `;

                            document.getElementById('analysis-content').innerHTML = html;

                        } else {
                            document.getElementById('analysis-content').innerHTML = `
                                <h3>📈 Bar Collection Metrics</h3>
                                <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                    <h4>Error Loading Metrics</h4>
                                    <p>Unable to load bar collection metrics. Please try again later.</p>
                                </div>
                            `;
                        }
                    } catch (error) {
                        console.error('Error loading metrics:', error);
                        document.getElementById('analysis-content').innerHTML = `
                            <h3>📈 Bar Collection Metrics</h3>
                            <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                <h4>Error</h4>
                                <p>${error.message}</p>
                            </div>
                        `;
                    }
                }

                async function loadUniverseAnalytics() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>🌐 Universe Analytics</h3>
                        <p>Loading universe data...</p>
                    `;

                    try {
                        const response = await fetch('/api/universe_analytics');
                        if (response.ok) {
                            const data = await response.json();

                            const html = `
                                <h3>🌐 Universe Analytics</h3>

                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin-bottom: 20px;">
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #007bff;">🏢 Total Symbols</h4>
                                        <h2 style="margin: 10px 0; color: #007bff;">${(data.total_symbols || 0).toLocaleString()}</h2>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #28a745;">✅ Active</h4>
                                        <h2 style="margin: 10px 0; color: #28a745;">${(data.active_symbols || 0).toLocaleString()}</h2>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #ffc107;">📈 Exchanges</h4>
                                        <h2 style="margin: 10px 0; color: #ffc107;">${(data.exchanges || 0).toLocaleString()}</h2>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #dc3545;">🏭 Sectors</h4>
                                        <h2 style="margin: 10px 0; color: #dc3545;">${(data.sectors || 0).toLocaleString()}</h2>
                                    </div>
                                </div>

                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>💼 Top Sectors by Market Cap</h4>
                                        <div style="max-height: 300px; overflow-y: auto;">
                                            ${(data.top_sectors || []).map((sector, index) => `
                                                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px; border-bottom: 1px solid #eee;">
                                                    <div style="display: flex; align-items: center;">
                                                        <span style="background: ${['#007bff', '#28a745', '#ffc107', '#dc3545', '#6610f2'][index % 5]}; color: white; width: 20px; height: 20px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.8em; margin-right: 10px;">
                                                            ${index + 1}
                                                        </span>
                                                        <strong>${sector.name}</strong>
                                                    </div>
                                                    <span style="font-family: monospace;">$${(sector.market_cap / 1e9).toFixed(1)}B</span>
                                                </div>
                                            `).join('')}
                                        </div>
                                    </div>

                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>🏛️ Top Exchanges by Volume</h4>
                                        <div style="max-height: 300px; overflow-y: auto;">
                                            ${(data.top_exchanges || []).map((exchange, index) => `
                                                <div style="display: flex; justify-content: space-between; align-items: center; padding: 8px; border-bottom: 1px solid #eee;">
                                                    <div style="display: flex; align-items: center;">
                                                        <span style="background: ${['#17a2b8', '#fd7e14', '#20c997', '#e83e8c', '#6f42c1'][index % 5]}; color: white; width: 20px; height: 20px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.8em; margin-right: 10px;">
                                                            ${index + 1}
                                                        </span>
                                                        <strong>${exchange.name}</strong>
                                                    </div>
                                                    <span style="font-family: monospace;">${(exchange.volume / 1e6).toFixed(0)}M</span>
                                                </div>
                                            `).join('')}
                                        </div>
                                    </div>
                                </div>

                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>🎯 Market Performance Overview</h4>
                                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 4px;">
                                            <h5 style="margin: 0; color: #666;">Market Cap</h5>
                                            <h3 style="margin: 5px 0; color: #007bff;">$${((data.total_market_cap || 0) / 1e12).toFixed(1)}T</h3>
                                        </div>
                                        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 4px;">
                                            <h5 style="margin: 0; color: #666;">Avg P/E Ratio</h5>
                                            <h3 style="margin: 5px 0; color: #28a745;">${(data.avg_pe || 0).toFixed(1)}x</h3>
                                        </div>
                                        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 4px;">
                                            <h5 style="margin: 0; color: #666;">Dividend Yield</h5>
                                            <h3 style="margin: 5px 0; color: #ffc107;">${(data.avg_dividend_yield || 0).toFixed(2)}%</h3>
                                        </div>
                                        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 4px;">
                                            <h5 style="margin: 0; color: #666;">Beta</h5>
                                            <h3 style="margin: 5px 0; color: #dc3545;">${(data.avg_beta || 0).toFixed(2)}</h3>
                                        </div>
                                    </div>
                                </div>

                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📊 Universe Health Metrics</h4>
                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                                        <div>
                                            <h5>Data Coverage</h5>
                                            <div style="margin-bottom: 10px;">
                                                <div style="display: flex; justify-content: space-between;">
                                                    <span>Price Data</span>
                                                    <span>${(data.price_coverage || 0).toFixed(1)}%</span>
                                                </div>
                                                <div style="background: #e9ecef; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 3px;">
                                                    <div style="background: #28a745; height: 100%; width: ${data.price_coverage || 0}%; transition: width 0.3s ease;"></div>
                                                </div>
                                            </div>
                                            <div style="margin-bottom: 10px;">
                                                <div style="display: flex; justify-content: space-between;">
                                                    <span>Volume Data</span>
                                                    <span>${(data.volume_coverage || 0).toFixed(1)}%</span>
                                                </div>
                                                <div style="background: #e9ecef; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 3px;">
                                                    <div style="background: #007bff; height: 100%; width: ${data.volume_coverage || 0}%; transition: width 0.3s ease;"></div>
                                                </div>
                                            </div>
                                        </div>
                                        <div>
                                            <h5>Quality Metrics</h5>
                                            <div style="margin-bottom: 10px;">
                                                <div style="display: flex; justify-content: space-between;">
                                                    <span>Data Accuracy</span>
                                                    <span>${(data.accuracy_score || 0).toFixed(1)}%</span>
                                                </div>
                                                <div style="background: #e9ecef; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 3px;">
                                                    <div style="background: #ffc107; height: 100%; width: ${data.accuracy_score || 0}%; transition: width 0.3s ease;"></div>
                                                </div>
                                            </div>
                                            <div style="margin-bottom: 10px;">
                                                <div style="display: flex; justify-content: space-between;">
                                                    <span>Completeness</span>
                                                    <span>${(data.completeness_score || 0).toFixed(1)}%</span>
                                                </div>
                                                <div style="background: #e9ecef; height: 8px; border-radius: 4px; overflow: hidden; margin-top: 3px;">
                                                    <div style="background: #dc3545; height: 100%; width: ${data.completeness_score || 0}%; transition: width 0.3s ease;"></div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    <p style="color: #666; font-size: 0.9em; margin-top: 15px; text-align: center;">
                                        Last updated: ${data.last_updated || 'Unknown'} | Next update: ${data.next_update || 'Scheduled'}
                                    </p>
                                </div>
                            `;

                            document.getElementById('analysis-content').innerHTML = html;

                        } else {
                            document.getElementById('analysis-content').innerHTML = `
                                <h3>🌐 Universe Analytics</h3>
                                <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                    <h4>Error Loading Universe Data</h4>
                                    <p>Unable to load universe analytics. Please try again later.</p>
                                </div>
                            `;
                        }
                    } catch (error) {
                        console.error('Error loading universe analytics:', error);
                        document.getElementById('analysis-content').innerHTML = `
                            <h3>🌐 Universe Analytics</h3>
                            <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                <h4>Error</h4>
                                <p>${error.message}</p>
                            </div>
                        `;
                    }
                }

                async function loadTrainingDatasets() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>🤖 Training Datasets</h3>
                        <p>Loading training datasets...</p>
                    `;

                    try {
                        const response = await fetch('/api/training_datasets');
                        if (response.ok) {
                            const datasets = await response.json();

                            let html = `
                                <h3>🤖 Training Datasets</h3>
                                <div style="margin-bottom: 20px;">
                                    <button onclick="refreshTrainingDatasets()" style="background: #007bff; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;">
                                        🔄 Refresh Datasets
                                    </button>
                                </div>
                            `;

                            if (datasets && datasets.length > 0) {
                                html += `
                                    <div style="display: grid; gap: 15px;">
                                        ${datasets.map(dataset => `
                                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                                <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 15px;">
                                                    <div>
                                                        <h4 style="margin: 0 0 5px 0; color: #007bff;">${dataset.dataset_name || `Dataset ${dataset.id}`}</h4>
                                                        <p style="margin: 0; color: #666; font-size: 0.9em;">
                                                            ID: ${dataset.id} | Created: ${dataset.creation_timestamp || 'Unknown'}
                                                        </p>
                                                    </div>
                                                    <span style="background: #28a745; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8em;">
                                                        Active
                                                    </span>
                                                </div>

                                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 15px;">
                                                    <div style="text-align: center; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                                        <div style="font-size: 0.8em; color: #666;">Symbols</div>
                                                        <div style="font-weight: bold; color: #007bff;">${dataset.symbols ? dataset.symbols.split(',').length : 0}</div>
                                                    </div>
                                                    <div style="text-align: center; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                                        <div style="font-size: 0.8em; color: #666;">Sequences</div>
                                                        <div style="font-weight: bold; color: #28a745;">${(dataset.total_sequences || 0).toLocaleString()}</div>
                                                    </div>
                                                    <div style="text-align: center; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                                        <div style="font-size: 0.8em; color: #666;">Quality</div>
                                                        <div style="font-weight: bold; color: #ffc107;">${((dataset.data_quality_score || 0) * 100).toFixed(1)}%</div>
                                                    </div>
                                                    <div style="text-align: center; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                                        <div style="font-size: 0.8em; color: #666;">Size</div>
                                                        <div style="font-weight: bold; color: #dc3545;">${(dataset.file_size_mb || 0).toFixed(1)} MB</div>
                                                    </div>
                                                </div>

                                                <div style="margin-bottom: 15px;">
                                                    <strong>Symbols:</strong>
                                                    <span style="font-family: monospace; font-size: 0.9em;">${dataset.symbols || 'N/A'}</span>
                                                </div>

                                                <div style="margin-bottom: 15px;">
                                                    <strong>Date Range:</strong>
                                                    ${dataset.start_date || 'N/A'} to ${dataset.end_date || 'N/A'}
                                                </div>

                                                <div style="display: flex; gap: 10px; flex-wrap: wrap;">
                                                    <button onclick="viewDatasetDetails(${dataset.id})"
                                                            style="background: #007bff; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em;">
                                                        📋 View Details
                                                    </button>
                                                    <button onclick="viewDatasetSequences(${dataset.id})"
                                                            style="background: #28a745; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em;">
                                                        🔍 Browse Sequences
                                                    </button>
                                                    <button onclick="downloadDataset(${dataset.id})"
                                                            style="background: #17a2b8; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em;">
                                                        💾 Download
                                                    </button>
                                                </div>
                                            </div>
                                        `).join('')}
                                    </div>
                                `;
                            } else {
                                html += `
                                    <div style="background: #fff3cd; color: #856404; padding: 20px; border-radius: 8px; border: 1px solid #ffeaa7; text-align: center;">
                                        <h4>No Training Datasets Found</h4>
                                        <p>No training datasets are currently available. Create one using the training data generation pipeline.</p>
                                    </div>
                                `;
                            }

                            document.getElementById('analysis-content').innerHTML = html;

                        } else {
                            document.getElementById('analysis-content').innerHTML = `
                                <h3>🤖 Training Datasets</h3>
                                <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                    <h4>Error Loading Datasets</h4>
                                    <p>Unable to load training datasets. Please try again later.</p>
                                </div>
                            `;
                        }
                    } catch (error) {
                        console.error('Error loading training datasets:', error);
                        document.getElementById('analysis-content').innerHTML = `
                            <h3>🤖 Training Datasets</h3>
                            <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                <h4>Error</h4>
                                <p>${error.message}</p>
                            </div>
                        `;
                    }
                }

                async function viewDatasetDetails(datasetId) {
                    try {
                        const response = await fetch(`/api/training_dataset/${datasetId}`);
                        if (response.ok) {
                            const dataset = await response.json();

                            const detailsHtml = `
                                <div style="position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; display: flex; align-items: center; justify-content: center;" onclick="closeModal(event)">
                                    <div style="background: white; padding: 30px; border-radius: 8px; max-width: 800px; max-height: 80vh; overflow-y: auto; margin: 20px;" onclick="event.stopPropagation()">
                                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 2px solid #007bff; padding-bottom: 10px;">
                                            <h3 style="margin: 0; color: #007bff;">📋 Dataset Details: ${dataset.dataset_name || `Dataset ${datasetId}`}</h3>
                                            <button onclick="closeModal()" style="background: #dc3545; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer;">✕</button>
                                        </div>

                                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                            <div>
                                                <h4>Basic Information</h4>
                                                <p><strong>ID:</strong> ${dataset.id}</p>
                                                <p><strong>Name:</strong> ${dataset.dataset_name || 'N/A'}</p>
                                                <p><strong>Created:</strong> ${dataset.creation_timestamp || 'N/A'}</p>
                                                <p><strong>File Size:</strong> ${(dataset.file_size_mb || 0).toFixed(2)} MB</p>
                                            </div>
                                            <div>
                                                <h4>Quality Metrics</h4>
                                                <p><strong>Data Quality:</strong> ${((dataset.data_quality_score || 0) * 100).toFixed(1)}%</p>
                                                <p><strong>Feature Completeness:</strong> ${((dataset.feature_completeness || 0) * 100).toFixed(1)}%</p>
                                                <p><strong>Label Completeness:</strong> ${((dataset.label_completeness || 0) * 100).toFixed(1)}%</p>
                                                <p><strong>Total Sequences:</strong> ${(dataset.total_sequences || 0).toLocaleString()}</p>
                                            </div>
                                        </div>

                                        <div style="margin-bottom: 20px;">
                                            <h4>Symbols & Date Range</h4>
                                            <p><strong>Symbols:</strong> <span style="font-family: monospace; background: #f8f9fa; padding: 2px 4px;">${dataset.symbols || 'N/A'}</span></p>
                                            <p><strong>Date Range:</strong> ${dataset.start_date || 'N/A'} to ${dataset.end_date || 'N/A'}</p>
                                        </div>

                                        <div style="margin-bottom: 20px;">
                                            <h4>Sequence Configuration</h4>
                                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px;">
                                                <div style="background: #f8f9fa; padding: 10px; border-radius: 4px;">
                                                    <strong>Sequence Length:</strong><br>
                                                    ${dataset.sequence_length || 'N/A'}
                                                </div>
                                                <div style="background: #f8f9fa; padding: 10px; border-radius: 4px;">
                                                    <strong>Prediction Horizon:</strong><br>
                                                    ${dataset.prediction_horizon || 'N/A'}
                                                </div>
                                                <div style="background: #f8f9fa; padding: 10px; border-radius: 4px;">
                                                    <strong>Features:</strong><br>
                                                    ${dataset.feature_count || 'N/A'}
                                                </div>
                                                <div style="background: #f8f9fa; padding: 10px; border-radius: 4px;">
                                                    <strong>Label Type:</strong><br>
                                                    ${dataset.label_type || 'N/A'}
                                                </div>
                                            </div>
                                        </div>

                                        ${dataset.technical_indicators ? `
                                            <div style="margin-bottom: 20px;">
                                                <h4>Technical Indicators</h4>
                                                <div style="background: #f8f9fa; padding: 10px; border-radius: 4px; font-family: monospace; white-space: pre-wrap;">${JSON.stringify(dataset.technical_indicators, null, 2)}</div>
                                            </div>
                                        ` : ''}

                                        <div style="display: flex; gap: 10px; justify-content: center;">
                                            <button onclick="viewDatasetSequences(${datasetId}); closeModal();"
                                                    style="background: #28a745; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer;">
                                                🔍 Browse Sequences
                                            </button>
                                            <button onclick="downloadDataset(${datasetId})"
                                                    style="background: #17a2b8; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer;">
                                                💾 Download Dataset
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            `;

                            document.body.insertAdjacentHTML('beforeend', detailsHtml);

                        } else {
                            alert('Error loading dataset details');
                        }
                    } catch (error) {
                        console.error('Error loading dataset details:', error);
                        alert('Error loading dataset details');
                    }
                }

                function closeModal(event) {
                    if (event && event.target !== event.currentTarget) return;
                    const modal = document.querySelector('div[style*="position: fixed"]');
                    if (modal) modal.remove();
                }

                async function viewDatasetSequences(datasetId) {
                    try {
                        const response = await fetch(`/api/training_dataset_sequences/${datasetId}`);
                        if (response.ok) {
                            const data = await response.json();

                            document.getElementById('analysis-content').innerHTML = `
                                <div style="margin-bottom: 20px;">
                                    <button onclick="loadTrainingDatasets()" style="background: #6c757d; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;">
                                        ← Back to Datasets
                                    </button>
                                </div>

                                <h3>🔍 Dataset Sequences: ${data.dataset_name || `Dataset ${datasetId}`}</h3>

                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">
                                        <div style="text-align: center;">
                                            <div style="font-size: 0.9em; color: #666;">Total Sequences</div>
                                            <div style="font-size: 1.5em; font-weight: bold; color: #007bff;">${data.total_sequences || 0}</div>
                                        </div>
                                        <div style="text-align: center;">
                                            <div style="font-size: 0.9em; color: #666;">Sequence Length</div>
                                            <div style="font-size: 1.5em; font-weight: bold; color: #28a745;">${data.sequence_length || 0}</div>
                                        </div>
                                        <div style="text-align: center;">
                                            <div style="font-size: 0.9em; color: #666;">Features</div>
                                            <div style="font-size: 1.5em; font-weight: bold; color: #ffc107;">${data.feature_count || 0}</div>
                                        </div>
                                        <div style="text-align: center;">
                                            <div style="font-size: 0.9em; color: #666;">Symbols</div>
                                            <div style="font-size: 1.5em; font-weight: bold; color: #dc3545;">${data.symbol_count || 0}</div>
                                        </div>
                                    </div>
                                </div>

                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>Browse Sequences</h4>
                                    <div style="display: flex; gap: 10px; margin-bottom: 15px; align-items: center;">
                                        <label>Sequence Index:</label>
                                        <input type="number" id="sequence-index" min="0" max="${(data.total_sequences || 1) - 1}" value="0"
                                               style="width: 100px; padding: 5px; border: 1px solid #ddd; border-radius: 4px;">
                                        <button onclick="loadSequenceData(${datasetId})"
                                                style="background: #007bff; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer;">
                                            Load Sequence
                                        </button>
                                        <span style="color: #666; font-size: 0.9em;">
                                            (0 - ${(data.total_sequences || 1) - 1})
                                        </span>
                                    </div>

                                    <div id="sequence-viewer" style="border: 1px solid #eee; border-radius: 4px; padding: 20px; background: #f8f9fa;">
                                        <p style="text-align: center; color: #666;">Enter a sequence index and click 'Load Sequence' to view data</p>
                                    </div>
                                </div>
                            `;

                        } else {
                            alert('Error loading dataset sequences');
                        }
                    } catch (error) {
                        console.error('Error loading dataset sequences:', error);
                        alert('Error loading dataset sequences');
                    }
                }

                async function loadSequenceData(datasetId) {
                    const sequenceIndex = document.getElementById('sequence-index').value;

                    document.getElementById('sequence-viewer').innerHTML = '<p>Loading sequence data...</p>';

                    try {
                        const response = await fetch(`/api/training_dataset_sequence/${datasetId}/${sequenceIndex}`);
                        if (response.ok) {
                            const data = await response.json();

                            const html = `
                                <h5>Sequence ${sequenceIndex} - Symbol: ${data.symbol || 'Unknown'}</h5>

                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                    <div>
                                        <h6>Sequence Information</h6>
                                        <p><strong>Symbol:</strong> ${data.symbol || 'N/A'}</p>
                                        <p><strong>Date Range:</strong> ${data.start_date || 'N/A'} to ${data.end_date || 'N/A'}</p>
                                        <p><strong>Length:</strong> ${data.sequence_length || 0} time steps</p>
                                        <p><strong>Features:</strong> ${data.feature_count || 0}</p>
                                    </div>
                                    <div>
                                        <h6>Labels/Targets</h6>
                                        <p><strong>Label Type:</strong> ${data.label_type || 'N/A'}</p>
                                        <p><strong>Target Return:</strong> ${data.target_return ? (data.target_return * 100).toFixed(2) + '%' : 'N/A'}</p>
                                        <p><strong>Prediction Horizon:</strong> ${data.prediction_horizon || 'N/A'}</p>
                                    </div>
                                </div>

                                <div style="margin-bottom: 20px;">
                                    <h6>Feature Data Preview</h6>
                                    <div style="max-height: 300px; overflow: auto; border: 1px solid #ddd; border-radius: 4px;">
                                        <table style="width: 100%; font-size: 0.8em; border-collapse: collapse;">
                                            <thead style="background: #f8f9fa; position: sticky; top: 0;">
                                                <tr>
                                                    ${(data.feature_names || []).map(name => `<th style="padding: 8px; border-bottom: 1px solid #ddd; text-align: left;">${name}</th>`).join('')}
                                                </tr>
                                            </thead>
                                            <tbody>
                                                ${(data.features || []).slice(0, 10).map((row, idx) => `
                                                    <tr style="background: ${idx % 2 === 0 ? '#fff' : '#f8f9fa'};">
                                                        ${(Array.isArray(row) ? row : []).map(val => `
                                                            <td style="padding: 6px; border-bottom: 1px solid #eee;">
                                                                ${typeof val === 'number' ? val.toFixed(4) : val}
                                                            </td>
                                                        `).join('')}
                                                    </tr>
                                                `).join('')}
                                            </tbody>
                                        </table>
                                        ${(data.features || []).length > 10 ? `
                                            <div style="text-align: center; padding: 10px; color: #666; font-size: 0.9em;">
                                                ... and ${(data.features || []).length - 10} more rows
                                            </div>
                                        ` : ''}
                                    </div>
                                </div>
                            `;

                            document.getElementById('sequence-viewer').innerHTML = html;

                        } else {
                            document.getElementById('sequence-viewer').innerHTML = '<p style="color: #dc3545;">Error loading sequence data</p>';
                        }
                    } catch (error) {
                        console.error('Error loading sequence data:', error);
                        document.getElementById('sequence-viewer').innerHTML = '<p style="color: #dc3545;">Error loading sequence data</p>';
                    }
                }

                async function downloadDataset(datasetId) {
                    try {
                        const response = await fetch(`/api/download_dataset/${datasetId}`);
                        if (response.ok) {
                            const blob = await response.blob();
                            const url = window.URL.createObjectURL(blob);
                            const a = document.createElement('a');
                            a.style.display = 'none';
                            a.href = url;
                            a.download = `training_dataset_${datasetId}.zip`;
                            document.body.appendChild(a);
                            a.click();
                            window.URL.revokeObjectURL(url);
                            document.body.removeChild(a);
                        } else {
                            alert('Error downloading dataset');
                        }
                    } catch (error) {
                        console.error('Error downloading dataset:', error);
                        alert('Error downloading dataset');
                    }
                }

                async function refreshTrainingDatasets() {
                    loadTrainingDatasets();
                }

                async function loadMultiPanelVisualization() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>🎨 Multi-Panel Trading Charts</h3>
                        <p>Loading visualization interface...</p>
                    `;

                    const html = `
                        <h3>🎨 Multi-Panel Trading Charts</h3>

                        <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                            <h4>Chart Configuration</h4>
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                <div>
                                    <label for="chart-symbol" style="display: block; margin-bottom: 5px; font-weight: bold;">Symbol:</label>
                                    <input type="text" id="chart-symbol" placeholder="e.g., AAPL"
                                           style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                </div>
                                <div>
                                    <label for="chart-start-date" style="display: block; margin-bottom: 5px; font-weight: bold;">Start Date:</label>
                                    <input type="date" id="chart-start-date"
                                           style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                </div>
                                <div>
                                    <label for="chart-end-date" style="display: block; margin-bottom: 5px; font-weight: bold;">End Date:</label>
                                    <input type="date" id="chart-end-date"
                                           style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                </div>
                                <div>
                                    <label for="chart-timeframe" style="display: block; margin-bottom: 5px; font-weight: bold;">Timeframe:</label>
                                    <select id="chart-timeframe" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        <option value="1d">Daily</option>
                                        <option value="1h">Hourly</option>
                                        <option value="15m">15 Minutes</option>
                                        <option value="5m">5 Minutes</option>
                                    </select>
                                </div>
                            </div>
                            <div style="margin-top: 15px;">
                                <button onclick="generateTradingChart()"
                                        style="background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer;">
                                    📈 Generate Chart
                                </button>
                            </div>
                        </div>

                        <div id="trading-chart-container" style="background: white; border-radius: 8px; border: 1px solid #ddd; padding: 20px; min-height: 600px;">
                            <p style="text-align: center; color: #666; margin-top: 50px;">
                                Configure chart settings above and click 'Generate Chart' to create visualization
                            </p>
                        </div>
                    `;

                    document.getElementById('analysis-content').innerHTML = html;

                    // Set default dates
                    const endDate = new Date();
                    const startDate = new Date();
                    startDate.setMonth(endDate.getMonth() - 3); // 3 months ago

                    document.getElementById('chart-end-date').value = endDate.toISOString().split('T')[0];
                    document.getElementById('chart-start-date').value = startDate.toISOString().split('T')[0];
                }

                async function generateTradingChart() {
                    const symbol = document.getElementById('chart-symbol').value;
                    const startDate = document.getElementById('chart-start-date').value;
                    const endDate = document.getElementById('chart-end-date').value;
                    const timeframe = document.getElementById('chart-timeframe').value;

                    if (!symbol || !startDate || !endDate) {
                        alert('Please fill in all required fields');
                        return;
                    }

                    document.getElementById('trading-chart-container').innerHTML = '<p>Generating multi-panel trading chart...</p>';

                    try {
                        const response = await fetch(`/api/multi_panel_chart?symbol=${symbol}&start_date=${startDate}&end_date=${endDate}&timeframe=${timeframe}`);
                        if (response.ok) {
                            const chartData = await response.json();

                            // Create multi-panel chart with Plotly
                            const traces = [];

                            // Price panel (OHLC)
                            traces.push({
                                x: chartData.timestamps,
                                open: chartData.open,
                                high: chartData.high,
                                low: chartData.low,
                                close: chartData.close,
                                type: 'candlestick',
                                name: `${symbol} Price`,
                                yaxis: 'y1'
                            });

                            // Volume panel
                            traces.push({
                                x: chartData.timestamps,
                                y: chartData.volume,
                                type: 'bar',
                                name: 'Volume',
                                yaxis: 'y2',
                                marker: { color: 'rgba(158, 185, 243, 0.6)' }
                            });

                            // Technical indicators if available
                            if (chartData.sma_20) {
                                traces.push({
                                    x: chartData.timestamps,
                                    y: chartData.sma_20,
                                    type: 'scatter',
                                    mode: 'lines',
                                    name: 'SMA 20',
                                    line: { color: 'orange' },
                                    yaxis: 'y1'
                                });
                            }

                            if (chartData.sma_50) {
                                traces.push({
                                    x: chartData.timestamps,
                                    y: chartData.sma_50,
                                    type: 'scatter',
                                    mode: 'lines',
                                    name: 'SMA 50',
                                    line: { color: 'red' },
                                    yaxis: 'y1'
                                });
                            }

                            const layout = {
                                title: `${symbol} - Multi-Panel Trading Chart`,
                                xaxis: {
                                    title: 'Date',
                                    type: 'date',
                                    rangeslider: { visible: false }
                                },
                                yaxis: {
                                    title: 'Price ($)',
                                    domain: [0.3, 1],
                                    side: 'left'
                                },
                                yaxis2: {
                                    title: 'Volume',
                                    domain: [0, 0.25],
                                    side: 'left'
                                },
                                height: 600,
                                margin: { t: 50, b: 50, l: 80, r: 50 },
                                showlegend: true,
                                legend: {
                                    x: 0,
                                    y: 1,
                                    bgcolor: 'rgba(255, 255, 255, 0.8)'
                                }
                            };

                            Plotly.newPlot('trading-chart-container', traces, layout);

                        } else {
                            document.getElementById('trading-chart-container').innerHTML = '<p style="color: #dc3545;">Error generating chart. Please check symbol and date range.</p>';
                        }
                    } catch (error) {
                        console.error('Error generating chart:', error);
                        document.getElementById('trading-chart-container').innerHTML = '<p style="color: #dc3545;">Error generating chart</p>';
                    }
                }

                async function loadNewsAnalytics() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>📰 News & Signals Analytics</h3>
                        <div style="background: #fff3cd; color: #856404; padding: 20px; border-radius: 8px; border: 1px solid #ffeaa7;">
                            <h4>🚧 Coming Soon</h4>
                            <p>News analytics and signal generation features are under development. This will include:</p>
                            <ul>
                                <li>📰 Real-time news sentiment analysis</li>
                                <li>📊 News impact on price movements</li>
                                <li>🎯 Signal generation from news events</li>
                                <li>📈 Correlation analysis between news and returns</li>
                            </ul>
                        </div>
                    `;
                }

                async function loadRayAnalytics() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>⚡ Distributed Analytics with Ray</h3>
                        <p>Loading Ray cluster information...</p>
                    `;

                    try {
                        const response = await fetch('/api/ray_status');
                        let html = '<h3>⚡ Distributed Analytics with Ray</h3>';

                        if (response.ok) {
                            const rayData = await response.json();

                            html += `
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px;">
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: ${rayData.cluster_status === 'running' ? '#28a745' : '#dc3545'};">
                                            ${rayData.cluster_status === 'running' ? '✅' : '❌'} Cluster Status
                                        </h4>
                                        <p style="margin: 5px 0; font-weight: bold;">${rayData.cluster_status || 'Unknown'}</p>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #007bff;">🖥️ Nodes</h4>
                                        <p style="margin: 5px 0; font-size: 1.5em; font-weight: bold;">${rayData.total_nodes || 0}</p>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #ffc107;">⚡ CPUs</h4>
                                        <p style="margin: 5px 0; font-size: 1.5em; font-weight: bold;">${rayData.total_cpus || 0}</p>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; text-align: center;">
                                        <h4 style="margin: 0; color: #17a2b8;">💾 Memory</h4>
                                        <p style="margin: 5px 0; font-size: 1.2em; font-weight: bold;">${(rayData.total_memory_gb || 0).toFixed(1)} GB</p>
                                    </div>
                                </div>
                            `;

                            if (rayData.cluster_status === 'running') {
                                html += `
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                        <h4>📊 Available Ray Operations</h4>
                                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px;">
                                            <button onclick="runRayAnalysis('portfolio_optimization')"
                                                    style="background: #007bff; color: white; border: none; padding: 15px; border-radius: 4px; cursor: pointer;">
                                                📈 Portfolio Optimization
                                            </button>
                                            <button onclick="runRayAnalysis('risk_analysis')"
                                                    style="background: #28a745; color: white; border: none; padding: 15px; border-radius: 4px; cursor: pointer;">
                                                ⚠️ Risk Analysis
                                            </button>
                                            <button onclick="runRayAnalysis('backtesting')"
                                                    style="background: #ffc107; color: black; border: none; padding: 15px; border-radius: 4px; cursor: pointer;">
                                                📊 Strategy Backtesting
                                            </button>
                                            <button onclick="runRayAnalysis('correlation_matrix')"
                                                    style="background: #17a2b8; color: white; border: none; padding: 15px; border-radius: 4px; cursor: pointer;">
                                                🔗 Correlation Analysis
                                            </button>
                                        </div>
                                    </div>

                                    <div id="ray-results" style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>Analysis Results</h4>
                                        <p style="color: #666;">Select an operation above to run distributed analysis</p>
                                    </div>
                                `;
                            } else {
                                html += `
                                    <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                        <h4>Ray Cluster Not Available</h4>
                                        <p>The Ray cluster is not currently running. To enable distributed analytics:</p>
                                        <ol>
                                            <li>Start Ray cluster: <code>ray start --head</code></li>
                                            <li>Or connect to existing cluster: <code>ray start --address=&lt;head-address&gt;</code></li>
                                            <li>Refresh this page to see Ray analytics</li>
                                        </ol>
                                    </div>
                                `;
                            }
                        } else {
                            html += `
                                <div style="background: #fff3cd; color: #856404; padding: 20px; border-radius: 8px; border: 1px solid #ffeaa7;">
                                    <h4>Ray Status Unknown</h4>
                                    <p>Unable to determine Ray cluster status. Ray distributed computing features may not be available.</p>
                                </div>
                            `;
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        console.error('Error loading Ray analytics:', error);
                        document.getElementById('analysis-content').innerHTML = `
                            <h3>⚡ Distributed Analytics with Ray</h3>
                            <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px; border: 1px solid #f1aeb5;">
                                <h4>Error</h4>
                                <p>${error.message}</p>
                            </div>
                        `;
                    }
                }

                async function runRayAnalysis(analysisType) {
                    const button = event.target;
                    const originalText = button.innerHTML;
                    button.innerHTML = '⏳ Running...';
                    button.disabled = true;

                    document.getElementById('ray-results').innerHTML = `
                        <h4>Analysis Results - ${analysisType}</h4>
                        <p>Running distributed analysis on Ray cluster...</p>
                    `;

                    try {
                        const response = await fetch(`/api/ray_analysis/${analysisType}`, { method: 'POST' });
                        if (response.ok) {
                            const results = await response.json();

                            let resultsHtml = `<h4>Analysis Results - ${analysisType}</h4>`;

                            if (results.status === 'completed') {
                                resultsHtml += `
                                    <div style="margin-bottom: 15px;">
                                        <span style="background: #d4edda; color: #155724; padding: 4px 8px; border-radius: 4px;">
                                            ✅ Completed in ${results.execution_time || 'N/A'} seconds
                                        </span>
                                    </div>

                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 4px; margin-bottom: 15px;">
                                        <h5>Summary</h5>
                                        <p>${results.summary || 'Analysis completed successfully.'}</p>
                                    </div>
                                `;

                                if (results.data) {
                                    resultsHtml += `
                                        <div style="background: #f8f9fa; padding: 15px; border-radius: 4px;">
                                            <h5>Results</h5>
                                            <pre style="background: white; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.9em;">${JSON.stringify(results.data, null, 2)}</pre>
                                        </div>
                                    `;
                                }
                            } else {
                                resultsHtml += `
                                    <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px;">
                                        <h5>Analysis Failed</h5>
                                        <p>${results.error || 'Unknown error occurred'}</p>
                                    </div>
                                `;
                            }

                            document.getElementById('ray-results').innerHTML = resultsHtml;

                        } else {
                            document.getElementById('ray-results').innerHTML = `
                                <h4>Analysis Results - ${analysisType}</h4>
                                <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px;">
                                    <h5>Error</h5>
                                    <p>Failed to run Ray analysis. Please check cluster status.</p>
                                </div>
                            `;
                        }
                    } catch (error) {
                        console.error('Error running Ray analysis:', error);
                        document.getElementById('ray-results').innerHTML = `
                            <h4>Analysis Results - ${analysisType}</h4>
                            <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px;">
                                <h5>Error</h5>
                                <p>${error.message}</p>
                            </div>
                        `;
                    } finally {
                        button.innerHTML = originalText;
                        button.disabled = false;
                    }
                }
            </script>
        </body>
        </html>
        """