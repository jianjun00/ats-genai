import React, { useState, useEffect, useCallback } from 'react';
import { 
  LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, 
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, ScatterChart, Scatter
} from 'recharts';
import './DataCoverageDashboard.css';

const DataCoverageDashboard = () => {
  const [coverageOverview, setCoverageOverview] = useState([]);
  const [coverageSummary, setCoverageSummary] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState('AAPL');
  const [selectedVendor, setSelectedVendor] = useState('polygon');
  const [vendorComparison, setVendorComparison] = useState(null);
  const [coverageTrends, setCoverageTrends] = useState([]);
  const [coverageGaps, setCoverageGaps] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('overview');
  const [filters, setFilters] = useState({
    vendors: [],
    dataTypes: [],
    minCoverage: null
  });
  const [wsConnection, setWsConnection] = useState(null);
  const [realtimeData, setRealtimeData] = useState(null);

  const COVERAGE_API_BASE_URL = process.env.REACT_APP_COVERAGE_API_BASE_URL || 'http://localhost:8001';
  const WS_BASE_URL = COVERAGE_API_BASE_URL.replace('http', 'ws');

  // Chart colors for different vendors
  const VENDOR_COLORS = {
    'polygon': '#00d4ff',
    'fmp': '#00ff88', 
    'tiingo': '#ffa502',
    'alphavantage': '#a55eea'
  };

  const CHART_COLORS = ['#00d4ff', '#00ff88', '#ff4757', '#ffa502', '#a55eea', '#26de81'];

  // Establish WebSocket connection for real-time updates
  useEffect(() => {
    const ws = new WebSocket(`${WS_BASE_URL}/ws/coverage/realtime`);
    
    ws.onopen = () => {
      console.log('✅ Connected to coverage WebSocket');
      setWsConnection(ws);
    };
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      if (data.type === 'coverage_overview') {
        setCoverageOverview(data.data);
        setRealtimeData(data);
      } else if (data.type === 'coverage_summary') {
        setCoverageSummary(data.data);
      } else if (data.type === 'heartbeat') {
        // Keep connection alive
      }
    };
    
    ws.onerror = (error) => {
      console.error('❌ WebSocket error:', error);
      setError('Real-time connection failed');
    };
    
    ws.onclose = () => {
      console.log('🔌 WebSocket connection closed');
      setWsConnection(null);
    };
    
    return () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };
  }, [WS_BASE_URL]);

  // Fetch coverage data
  const fetchCoverageData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      // Fetch coverage overview
      const overviewResponse = await fetch(`${COVERAGE_API_BASE_URL}/api/v1/coverage/overview`);
      if (overviewResponse.ok) {
        const overviewData = await overviewResponse.json();
        setCoverageOverview(overviewData);
      }

      // Fetch coverage summary with filters
      const summaryParams = new URLSearchParams();
      if (filters.vendors.length > 0) {
        filters.vendors.forEach(vendor => summaryParams.append('vendors', vendor));
      }
      if (filters.dataTypes.length > 0) {
        filters.dataTypes.forEach(type => summaryParams.append('data_types', type));
      }
      if (filters.minCoverage !== null) {
        summaryParams.append('min_coverage', filters.minCoverage);
      }

      const summaryResponse = await fetch(`${COVERAGE_API_BASE_URL}/api/v1/coverage/summary?${summaryParams}`);
      if (summaryResponse.ok) {
        const summaryData = await summaryResponse.json();
        setCoverageSummary(summaryData);
      }

      // Fetch vendor comparison for selected symbol
      if (selectedSymbol) {
        const comparisonResponse = await fetch(
          `${COVERAGE_API_BASE_URL}/api/v1/coverage/comparison/${selectedSymbol}?data_type=minute&time_period=24h`
        );
        if (comparisonResponse.ok) {
          const comparisonData = await comparisonResponse.json();
          setVendorComparison(comparisonData);
        }
      }

      // Fetch coverage trends for selected symbol and vendor
      if (selectedSymbol && selectedVendor) {
        const trendsResponse = await fetch(
          `${COVERAGE_API_BASE_URL}/api/v1/coverage/trends/${selectedSymbol}/${selectedVendor}?data_type=minute&days_back=30`
        );
        if (trendsResponse.ok) {
          const trendsData = await trendsResponse.json();
          setCoverageTrends(trendsData);
        }
      }

      // Fetch recent coverage gaps
      const gapsResponse = await fetch(
        `${COVERAGE_API_BASE_URL}/api/v1/coverage/gaps?unresolved_only=true&limit=50`
      );
      if (gapsResponse.ok) {
        const gapsData = await gapsResponse.json();
        setCoverageGaps(gapsData);
      }

    } catch (err) {
      console.error('❌ Failed to fetch coverage data:', err);
      setError(`Failed to load coverage data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  }, [COVERAGE_API_BASE_URL, selectedSymbol, selectedVendor, filters]);

  // Initial data fetch
  useEffect(() => {
    fetchCoverageData();
  }, [fetchCoverageData]);

  // Test Slack alerts
  const testSlackAlert = async () => {
    try {
      const response = await fetch(`${COVERAGE_API_BASE_URL}/api/v1/coverage/alerts/test`, {
        method: 'POST'
      });
      if (response.ok) {
        alert('✅ Test alert sent to Slack!');
      } else {
        alert('❌ Failed to send test alert');
      }
    } catch (error) {
      console.error('Error sending test alert:', error);
      alert('❌ Error sending test alert');
    }
  };

  // Check and send coverage alerts
  const checkCoverageAlerts = async () => {
    try {
      const response = await fetch(`${COVERAGE_API_BASE_URL}/api/v1/coverage/alerts/check`, {
        method: 'POST'
      });
      if (response.ok) {
        const result = await response.json();
        alert(`✅ Checked coverage alerts: ${result.alerts_sent} alerts sent`);
        // Refresh data after alerts
        fetchCoverageData();
      }
    } catch (error) {
      console.error('Error checking alerts:', error);
      alert('❌ Error checking coverage alerts');
    }
  };

  // Format percentage values
  const formatPercentage = (value, decimals = 1) => {
    return `${Number(value).toFixed(decimals)}%`;
  };

  // Format duration in minutes to human readable
  const formatDuration = (minutes) => {
    if (minutes < 60) return `${minutes}m`;
    if (minutes < 1440) return `${Math.round(minutes / 60)}h`;
    return `${Math.round(minutes / 1440)}d`;
  };

  // Get status color based on coverage percentage
  const getStatusColor = (coverage) => {
    if (coverage >= 95) return '#00ff88';
    if (coverage >= 85) return '#ffa502';
    if (coverage >= 70) return '#ff7675';
    return '#ff4757';
  };

  if (loading && coverageOverview.length === 0) {
    return (
      <div className="coverage-dashboard-loading">
        <div className="loading-spinner"></div>
        <p>Loading data coverage analytics...</p>
      </div>
    );
  }

  return (
    <div className="data-coverage-dashboard">
      {/* Header */}
      <header className="coverage-header">
        <h1>📊 Data Coverage Analytics</h1>
        <div className="header-controls">
          <div className="connection-status">
            {wsConnection ? (
              <span className="connected">🟢 Real-time Connected</span>
            ) : (
              <span className="disconnected">🔴 Offline</span>
            )}
          </div>
          <button className="test-alert-btn" onClick={testSlackAlert}>
            🧪 Test Slack Alert
          </button>
          <button className="check-alerts-btn" onClick={checkCoverageAlerts}>
            🚨 Check Alerts
          </button>
        </div>
      </header>

      {error && (
        <div className="error-banner">
          <span>⚠️ {error}</span>
          <button onClick={fetchCoverageData}>🔄 Retry</button>
        </div>
      )}

      {/* Navigation Tabs */}
      <nav className="coverage-nav">
        <button 
          className={activeTab === 'overview' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('overview')}
        >
          Overview
        </button>
        <button 
          className={activeTab === 'summary' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('summary')}
        >
          Coverage Summary
        </button>
        <button 
          className={activeTab === 'comparison' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('comparison')}
        >
          Vendor Comparison
        </button>
        <button 
          className={activeTab === 'trends' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('trends')}
        >
          Coverage Trends
        </button>
        <button 
          className={activeTab === 'gaps' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('gaps')}
        >
          Gap Analysis
        </button>
      </nav>

      {/* Main Content */}
      <main className="coverage-main">
        {/* Overview Tab */}
        {activeTab === 'overview' && (
          <div className="overview-tab">
            {/* Overview Cards */}
            <div className="overview-cards">
              {coverageOverview.map((overview, index) => (
                <div key={index} className="overview-card">
                  <div className="card-header">
                    <h3>{overview.vendor.toUpperCase()}</h3>
                    <span className="data-type">{overview.data_type}</span>
                  </div>
                  <div className="card-metrics">
                    <div className="metric">
                      <span className="metric-label">Average Coverage</span>
                      <span 
                        className="metric-value"
                        style={{ color: getStatusColor(overview.avg_coverage) }}
                      >
                        {formatPercentage(overview.avg_coverage)}
                      </span>
                    </div>
                    <div className="metric">
                      <span className="metric-label">Total Symbols</span>
                      <span className="metric-value">{overview.total_symbols}</span>
                    </div>
                    <div className="metrics-row">
                      <div className="metric-small">
                        <span className="metric-label">Active</span>
                        <span className="metric-value active">{overview.active_symbols}</span>
                      </div>
                      <div className="metric-small">
                        <span className="metric-label">Stale</span>
                        <span className="metric-value stale">{overview.stale_symbols}</span>
                      </div>
                      <div className="metric-small">
                        <span className="metric-label">Missing</span>
                        <span className="metric-value missing">{overview.missing_symbols}</span>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Coverage Distribution Chart */}
            {coverageOverview.length > 0 && (
              <div className="chart-container">
                <h3>Coverage Distribution by Vendor</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={coverageOverview}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis 
                      dataKey="vendor" 
                      stroke="#888"
                      tick={{ fontSize: 12 }}
                    />
                    <YAxis 
                      stroke="#888"
                      tick={{ fontSize: 12 }}
                      tickFormatter={formatPercentage}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: '#1a2332', 
                        border: '1px solid #00d4ff',
                        color: '#fff'
                      }}
                      formatter={(value, name) => [
                        name === 'avg_coverage' ? formatPercentage(value) : value,
                        name === 'avg_coverage' ? 'Average Coverage' : 
                        name === 'total_symbols' ? 'Total Symbols' : name
                      ]}
                    />
                    <Bar dataKey="avg_coverage" fill="#00d4ff" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Symbol Status Distribution */}
            {coverageOverview.length > 0 && (
              <div className="chart-container">
                <h3>Symbol Status Distribution</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={coverageOverview}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis dataKey="vendor" stroke="#888" tick={{ fontSize: 12 }} />
                    <YAxis stroke="#888" tick={{ fontSize: 12 }} />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: '#1a2332', 
                        border: '1px solid #00d4ff',
                        color: '#fff'
                      }}
                    />
                    <Legend />
                    <Bar dataKey="active_symbols" stackId="a" fill="#00ff88" name="Active" />
                    <Bar dataKey="stale_symbols" stackId="a" fill="#ffa502" name="Stale" />
                    <Bar dataKey="missing_symbols" stackId="a" fill="#ff4757" name="Missing" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        )}

        {/* Coverage Summary Tab */}
        {activeTab === 'summary' && (
          <div className="summary-tab">
            {/* Filters */}
            <div className="filters-section">
              <h3>Filters</h3>
              <div className="filters-row">
                <div className="filter-group">
                  <label>Minimum Coverage:</label>
                  <input 
                    type="number" 
                    min="0" 
                    max="100" 
                    value={filters.minCoverage || ''} 
                    onChange={(e) => setFilters(prev => ({ 
                      ...prev, 
                      minCoverage: e.target.value ? parseFloat(e.target.value) : null 
                    }))}
                    placeholder="e.g., 90"
                  />
                </div>
                <button className="apply-filters-btn" onClick={fetchCoverageData}>
                  Apply Filters
                </button>
              </div>
            </div>

            {/* Summary Table */}
            <div className="summary-table-container">
              <h3>Coverage Summary ({coverageSummary.length} entries)</h3>
              <table className="coverage-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Vendor</th>
                    <th>Data Type</th>
                    <th>Status</th>
                    <th>24h Coverage</th>
                    <th>7d Coverage</th>
                    <th>30d Coverage</th>
                    <th>24h Records</th>
                    <th>Last Update</th>
                    <th>Quality Score</th>
                  </tr>
                </thead>
                <tbody>
                  {coverageSummary.map((item, index) => (
                    <tr key={index}>
                      <td className="symbol-cell">{item.symbol}</td>
                      <td className="vendor-cell">
                        <span 
                          className="vendor-badge"
                          style={{ backgroundColor: VENDOR_COLORS[item.vendor] || '#666' }}
                        >
                          {item.vendor.toUpperCase()}
                        </span>
                      </td>
                      <td>{item.data_type}</td>
                      <td>
                        <span className={`status-badge ${item.current_status}`}>
                          {item.current_status}
                        </span>
                      </td>
                      <td 
                        className="coverage-cell"
                        style={{ color: getStatusColor(item.coverage_24h) }}
                      >
                        {formatPercentage(item.coverage_24h)}
                      </td>
                      <td className="coverage-cell">
                        {item.coverage_7d ? formatPercentage(item.coverage_7d) : 'N/A'}
                      </td>
                      <td className="coverage-cell">
                        {item.coverage_30d ? formatPercentage(item.coverage_30d) : 'N/A'}
                      </td>
                      <td className="records-cell">
                        {item.records_24h.toLocaleString()}
                      </td>
                      <td className="time-cell">
                        {item.hours_since_update ? `${item.hours_since_update.toFixed(1)}h ago` : 'N/A'}
                      </td>
                      <td className="quality-cell">
                        {item.quality_24h ? item.quality_24h.toFixed(1) : 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Vendor Comparison Tab */}
        {activeTab === 'comparison' && (
          <div className="comparison-tab">
            {/* Symbol Selection */}
            <div className="symbol-selection">
              <label>Select Symbol for Comparison:</label>
              <input 
                type="text" 
                value={selectedSymbol} 
                onChange={(e) => setSelectedSymbol(e.target.value.toUpperCase())}
                placeholder="Enter symbol (e.g., AAPL)"
              />
              <button onClick={fetchCoverageData}>🔍 Compare</button>
            </div>

            {/* Vendor Comparison Results */}
            {vendorComparison && (
              <div className="vendor-comparison">
                <h3>Vendor Comparison for {vendorComparison.symbol}</h3>
                
                {/* Comparison Summary Cards */}
                <div className="comparison-cards">
                  <div className="comparison-summary-card">
                    <h4>Summary</h4>
                    <div className="summary-metrics">
                      <div className="metric">
                        <span>Vendors Available:</span>
                        <span>{vendorComparison.vendor_count}</span>
                      </div>
                      <div className="metric">
                        <span>Average Coverage:</span>
                        <span>{formatPercentage(vendorComparison.average_coverage)}</span>
                      </div>
                      <div className="metric">
                        <span>Coverage Variance:</span>
                        <span>{vendorComparison.coverage_variance.toFixed(2)}</span>
                      </div>
                    </div>
                  </div>
                  
                  {vendorComparison.best_vendor && (
                    <div className="comparison-summary-card best">
                      <h4>🏆 Best Vendor</h4>
                      <div className="vendor-details">
                        <div className="vendor-name">{vendorComparison.best_vendor.vendor.toUpperCase()}</div>
                        <div className="vendor-coverage">
                          {formatPercentage(vendorComparison.best_vendor.coverage_percentage)}
                        </div>
                        <div className="vendor-status">{vendorComparison.best_vendor.status}</div>
                      </div>
                    </div>
                  )}
                  
                  {vendorComparison.worst_vendor && (
                    <div className="comparison-summary-card worst">
                      <h4>📉 Needs Improvement</h4>
                      <div className="vendor-details">
                        <div className="vendor-name">{vendorComparison.worst_vendor.vendor.toUpperCase()}</div>
                        <div className="vendor-coverage">
                          {formatPercentage(vendorComparison.worst_vendor.coverage_percentage)}
                        </div>
                        <div className="vendor-status">{vendorComparison.worst_vendor.status}</div>
                      </div>
                    </div>
                  )}
                </div>

                {/* Vendor Comparison Chart */}
                <div className="chart-container">
                  <h4>Coverage Comparison by Vendor</h4>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={vendorComparison.vendors}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                      <XAxis dataKey="vendor" stroke="#888" tick={{ fontSize: 12 }} />
                      <YAxis stroke="#888" tick={{ fontSize: 12 }} tickFormatter={formatPercentage} />
                      <Tooltip 
                        contentStyle={{ 
                          backgroundColor: '#1a2332', 
                          border: '1px solid #00d4ff',
                          color: '#fff'
                        }}
                        formatter={(value, name) => [
                          formatPercentage(value),
                          name === 'coverage_percentage' ? 'Coverage' : 'Quality Score'
                        ]}
                      />
                      <Bar 
                        dataKey="coverage_percentage" 
                        fill="#00d4ff"
                        name="Coverage %"
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Detailed Vendor Table */}
                <div className="vendor-details-table">
                  <h4>Detailed Vendor Comparison</h4>
                  <table className="comparison-table">
                    <thead>
                      <tr>
                        <th>Vendor</th>
                        <th>Coverage %</th>
                        <th>Quality Score</th>
                        <th>Status</th>
                        <th>Last Update</th>
                        <th>Hours Since Update</th>
                      </tr>
                    </thead>
                    <tbody>
                      {vendorComparison.vendors.map((vendor, index) => (
                        <tr key={index}>
                          <td>
                            <span 
                              className="vendor-badge"
                              style={{ backgroundColor: VENDOR_COLORS[vendor.vendor] || '#666' }}
                            >
                              {vendor.vendor.toUpperCase()}
                            </span>
                          </td>
                          <td 
                            className="coverage-cell"
                            style={{ color: getStatusColor(vendor.coverage_percentage) }}
                          >
                            {formatPercentage(vendor.coverage_percentage)}
                          </td>
                          <td className="quality-cell">
                            {vendor.quality_score ? vendor.quality_score.toFixed(1) : 'N/A'}
                          </td>
                          <td>
                            <span className={`status-badge ${vendor.status}`}>
                              {vendor.status}
                            </span>
                          </td>
                          <td className="time-cell">
                            {vendor.latest_data_time ? 
                              new Date(vendor.latest_data_time).toLocaleString() : 'N/A'
                            }
                          </td>
                          <td className="time-cell">
                            {vendor.hours_since_update ? 
                              `${vendor.hours_since_update.toFixed(1)}h` : 'N/A'
                            }
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Coverage Trends Tab */}
        {activeTab === 'trends' && (
          <div className="trends-tab">
            {/* Trend Controls */}
            <div className="trend-controls">
              <div className="control-group">
                <label>Symbol:</label>
                <input 
                  type="text" 
                  value={selectedSymbol} 
                  onChange={(e) => setSelectedSymbol(e.target.value.toUpperCase())}
                />
              </div>
              <div className="control-group">
                <label>Vendor:</label>
                <select 
                  value={selectedVendor} 
                  onChange={(e) => setSelectedVendor(e.target.value)}
                >
                  <option value="polygon">Polygon</option>
                  <option value="fmp">FMP</option>
                  <option value="tiingo">Tiingo</option>
                  <option value="alphavantage">Alpha Vantage</option>
                </select>
              </div>
              <button onClick={fetchCoverageData}>📈 Update Trends</button>
            </div>

            {/* Coverage Trends Chart */}
            {coverageTrends.length > 0 && (
              <div className="chart-container">
                <h3>Coverage Trends - {selectedSymbol} ({selectedVendor.toUpperCase()})</h3>
                <ResponsiveContainer width="100%" height={400}>
                  <LineChart data={coverageTrends}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis 
                      dataKey="period_start" 
                      stroke="#888"
                      tick={{ fontSize: 12 }}
                      tickFormatter={(value) => new Date(value).toLocaleDateString()}
                    />
                    <YAxis 
                      stroke="#888"
                      tick={{ fontSize: 12 }}
                      tickFormatter={formatPercentage}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: '#1a2332', 
                        border: '1px solid #00d4ff',
                        color: '#fff'
                      }}
                      labelFormatter={(value) => `Date: ${new Date(value).toLocaleDateString()}`}
                      formatter={(value, name) => [
                        name === 'coverage_percentage' ? formatPercentage(value) : value,
                        name === 'coverage_percentage' ? 'Coverage %' : 
                        name === 'gap_count' ? 'Gaps' : name
                      ]}
                    />
                    <Line 
                      type="monotone" 
                      dataKey="coverage_percentage" 
                      stroke="#00d4ff" 
                      strokeWidth={2}
                      name="Coverage %"
                    />
                    <Line 
                      type="monotone" 
                      dataKey="gap_count" 
                      stroke="#ff4757" 
                      strokeWidth={2}
                      yAxisId="right"
                      name="Gap Count"
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Trends Statistics */}
            {coverageTrends.length > 0 && (
              <div className="trends-stats">
                <h4>Trend Statistics</h4>
                <div className="stats-grid">
                  <div className="stat-card">
                    <div className="stat-label">Average Coverage</div>
                    <div className="stat-value">
                      {formatPercentage(
                        coverageTrends.reduce((acc, trend) => acc + trend.coverage_percentage, 0) / coverageTrends.length
                      )}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-label">Total Gaps</div>
                    <div className="stat-value">
                      {coverageTrends.reduce((acc, trend) => acc + trend.gap_count, 0)}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-label">Best Day</div>
                    <div className="stat-value">
                      {formatPercentage(Math.max(...coverageTrends.map(t => t.coverage_percentage)))}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-label">Worst Day</div>
                    <div className="stat-value">
                      {formatPercentage(Math.min(...coverageTrends.map(t => t.coverage_percentage)))}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Gap Analysis Tab */}
        {activeTab === 'gaps' && (
          <div className="gaps-tab">
            <h3>Coverage Gap Analysis</h3>
            
            {coverageGaps.length > 0 ? (
              <>
                {/* Gap Summary */}
                <div className="gaps-summary">
                  <div className="summary-cards">
                    <div className="summary-card">
                      <div className="card-label">Total Unresolved Gaps</div>
                      <div className="card-value">{coverageGaps.length}</div>
                    </div>
                    <div className="summary-card">
                      <div className="card-label">Critical Gaps</div>
                      <div className="card-value critical">
                        {coverageGaps.filter(gap => gap.gap_severity === 'critical').length}
                      </div>
                    </div>
                    <div className="summary-card">
                      <div className="card-label">High Priority Gaps</div>
                      <div className="card-value high">
                        {coverageGaps.filter(gap => gap.gap_severity === 'high').length}
                      </div>
                    </div>
                    <div className="summary-card">
                      <div className="card-label">Average Gap Duration</div>
                      <div className="card-value">
                        {formatDuration(
                          coverageGaps.reduce((acc, gap) => acc + gap.gap_duration_minutes, 0) / coverageGaps.length
                        )}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Gaps Table */}
                <div className="gaps-table-container">
                  <table className="gaps-table">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Vendor</th>
                        <th>Data Type</th>
                        <th>Gap Start</th>
                        <th>Gap End</th>
                        <th>Duration</th>
                        <th>Expected Records</th>
                        <th>Severity</th>
                        <th>Gap Type</th>
                        <th>Market Hours</th>
                        <th>Confidence</th>
                      </tr>
                    </thead>
                    <tbody>
                      {coverageGaps.map((gap, index) => (
                        <tr key={index}>
                          <td className="symbol-cell">{gap.symbol}</td>
                          <td>
                            <span 
                              className="vendor-badge"
                              style={{ backgroundColor: VENDOR_COLORS[gap.vendor] || '#666' }}
                            >
                              {gap.vendor.toUpperCase()}
                            </span>
                          </td>
                          <td>{gap.data_type}</td>
                          <td className="time-cell">
                            {new Date(gap.gap_start).toLocaleString()}
                          </td>
                          <td className="time-cell">
                            {new Date(gap.gap_end).toLocaleString()}
                          </td>
                          <td className="duration-cell">
                            {formatDuration(gap.gap_duration_minutes)}
                          </td>
                          <td className="records-cell">{gap.expected_records}</td>
                          <td>
                            <span className={`severity-badge ${gap.gap_severity}`}>
                              {gap.gap_severity}
                            </span>
                          </td>
                          <td className="type-cell">{gap.gap_type}</td>
                          <td className="market-hours-cell">
                            {gap.is_market_hours ? '✅' : '❌'}
                          </td>
                          <td className="confidence-cell">
                            {(gap.detection_confidence * 100).toFixed(0)}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <div className="no-gaps">
                <h4>🎉 No Unresolved Coverage Gaps Found!</h4>
                <p>All data sources are operating within acceptable coverage thresholds.</p>
              </div>
            )}
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="coverage-footer">
        <div className="footer-content">
          <span>🔗 Coverage API: {COVERAGE_API_BASE_URL}</span>
          <span>📊 {coverageSummary.length} coverage entries</span>
          <span>⚡ Real-time monitoring with Slack alerts</span>
          {realtimeData && (
            <span>🕒 Last updated: {new Date(realtimeData.timestamp).toLocaleTimeString()}</span>
          )}
        </div>
      </footer>
    </div>
  );
};

export default DataCoverageDashboard;