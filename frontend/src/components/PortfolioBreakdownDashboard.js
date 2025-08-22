import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  PieChart, Pie, Cell, ResponsiveContainer, LineChart, Line,
  ComposedChart, Area, ReferenceLine
} from 'recharts';
import './PortfolioBreakdownDashboard.css';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D', '#FFC658', '#FF7C7C', '#8DD1E1'];

const PortfolioBreakdownDashboard = ({ backtestRunId }) => {
  const [portfolioData, setPortfolioData] = useState([]);
  const [selectedDate, setSelectedDate] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [currentBreakdown, setCurrentBreakdown] = useState(null);

  useEffect(() => {
    fetchPortfolioBreakdown();
  }, [backtestRunId]);

  const fetchPortfolioBreakdown = async (targetDate = null) => {
    try {
      setLoading(true);
      const dateParam = targetDate ? `?target_date=${targetDate}` : '';
      const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000';
      const response = await fetch(`${API_BASE_URL}/api/v1/backtests/${backtestRunId}/portfolio-breakdown${dateParam}`);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      setPortfolioData(data);
      
      if (data && data.length > 0) {
        // Set the most recent date as current if no target date specified
        const latestBreakdown = data[data.length - 1];
        setCurrentBreakdown(latestBreakdown);
        setSelectedDate(latestBreakdown.date);
      }
      
      setError(null);
    } catch (err) {
      console.error('Error fetching portfolio breakdown:', err);
      setError('Failed to load portfolio breakdown data');
    } finally {
      setLoading(false);
    }
  };

  const handleDateChange = (date) => {
    setSelectedDate(date);
    const breakdown = portfolioData.find(d => d.date === date);
    if (breakdown) {
      setCurrentBreakdown(breakdown);
    } else {
      // Fetch specific date data
      fetchPortfolioBreakdown(date);
    }
  };

  const formatCurrency = (value) => {
    if (value >= 1000000) {
      return `$${(value / 1000000).toFixed(1)}M`;
    } else if (value >= 1000) {
      return `$${(value / 1000).toFixed(1)}K`;
    }
    return `$${value.toFixed(0)}`;
  };

  const formatPercent = (value) => {
    return `${(value * 100).toFixed(2)}%`;
  };

  if (loading) {
    return (
      <div className="portfolio-breakdown-dashboard loading">
        <div className="loading-spinner">Loading portfolio breakdown...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="portfolio-breakdown-dashboard error">
        <div className="error-message">{error}</div>
      </div>
    );
  }

  if (!currentBreakdown) {
    return (
      <div className="portfolio-breakdown-dashboard">
        <div className="no-data">No portfolio breakdown data available</div>
      </div>
    );
  }

  // Prepare data for visualizations
  const holdingsForChart = currentBreakdown.holdings.map(holding => ({
    symbol: holding.symbol,
    weight: holding.weight * 100,
    market_value: holding.market_value,
    daily_return: holding.daily_return * 100,
    daily_pnl: holding.daily_pnl
  }));

  const sectorData = Object.entries(currentBreakdown.sector_allocation).map(([sector, weight]) => ({
    sector,
    weight: weight * 100,
    value: weight
  }));

  const performanceContributors = [
    ...currentBreakdown.top_contributors.map(item => ({ ...item, type: 'contributor' })),
    ...currentBreakdown.top_detractors.map(item => ({ ...item, type: 'detractor' }))
  ].sort((a, b) => b.pnl - a.pnl);

  // Portfolio value over time (if multiple dates available)
  const timeSeriesData = portfolioData.map(breakdown => ({
    date: breakdown.date,
    portfolio_value: breakdown.total_portfolio_value,
    daily_return: breakdown.daily_return * 100
  }));

  return (
    <div className="portfolio-breakdown-dashboard">
      <div className="dashboard-header">
        <h2>Portfolio Breakdown Analysis</h2>
        <div className="date-selector">
          <label htmlFor="date-select">Analysis Date:</label>
          <select 
            id="date-select"
            value={selectedDate || ''}
            onChange={(e) => handleDateChange(e.target.value)}
          >
            {portfolioData.map(breakdown => (
              <option key={breakdown.date} value={breakdown.date}>
                {new Date(breakdown.date).toLocaleDateString()}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="summary-cards">
        <div className="summary-card">
          <h3>Portfolio Value</h3>
          <div className="metric-value">{formatCurrency(currentBreakdown.total_portfolio_value)}</div>
        </div>
        <div className="summary-card">
          <h3>Daily Return</h3>
          <div className={`metric-value ${currentBreakdown.daily_return >= 0 ? 'positive' : 'negative'}`}>
            {formatPercent(currentBreakdown.daily_return)}
          </div>
        </div>
        <div className="summary-card">
          <h3>Cumulative Return</h3>
          <div className={`metric-value ${currentBreakdown.cumulative_return >= 0 ? 'positive' : 'negative'}`}>
            {formatPercent(currentBreakdown.cumulative_return)}
          </div>
        </div>
        <div className="summary-card">
          <h3>Cash Position</h3>
          <div className="metric-value">{formatCurrency(currentBreakdown.cash_position)}</div>
        </div>
      </div>

      {/* Main Charts Row */}
      <div className="charts-row">
        {/* Holdings Breakdown */}
        <div className="chart-container">
          <h3>Holdings Breakdown</h3>
          <ResponsiveContainer width="100%" height={400}>
            <ComposedChart data={holdingsForChart}>
              <CartesianGrid strokeDasharray="3 3" stroke="#333" />
              <XAxis 
                dataKey="symbol" 
                stroke="#ccc"
                fontSize={12}
              />
              <YAxis 
                yAxisId="weight"
                orientation="left"
                stroke="#ccc"
                fontSize={12}
              />
              <YAxis 
                yAxisId="return"
                orientation="right"
                stroke="#ccc"
                fontSize={12}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#2a2a2a',
                  border: '1px solid #444',
                  borderRadius: '4px'
                }}
                formatter={(value, name) => {
                  if (name === 'weight') return [`${value.toFixed(1)}%`, 'Weight'];
                  if (name === 'daily_return') return [`${value.toFixed(2)}%`, 'Daily Return'];
                  return [value, name];
                }}
              />
              <Legend />
              <Bar 
                yAxisId="weight"
                dataKey="weight" 
                name="Portfolio Weight"
                fill="#0088FE" 
              />
              <Line 
                yAxisId="return"
                type="monotone" 
                dataKey="daily_return" 
                name="Daily Return"
                stroke="#FF8042" 
                strokeWidth={2}
                dot={{ fill: '#FF8042', r: 4 }}
              />
              <ReferenceLine yAxisId="return" y={0} stroke="#666" strokeDasharray="2 2" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* Sector Allocation */}
        <div className="chart-container">
          <h3>Sector Allocation</h3>
          <ResponsiveContainer width="100%" height={400}>
            <PieChart>
              <Pie
                data={sectorData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={120}
                paddingAngle={5}
                dataKey="value"
                label={({ sector, weight }) => `${sector}: ${weight.toFixed(1)}%`}
                labelLine={false}
              >
                {sectorData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#2a2a2a',
                  border: '1px solid #444',
                  borderRadius: '4px'
                }}
                formatter={(value, name) => [`${(value * 100).toFixed(1)}%`, 'Allocation']}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Performance Attribution */}
      <div className="attribution-section">
        <h3>Daily Performance Attribution</h3>
        <div className="attribution-grid">
          <div className="contributors">
            <h4>Top Contributors</h4>
            {currentBreakdown.top_contributors.map((contributor, index) => (
              <div key={index} className="attribution-item positive">
                <span className="symbol">{contributor.symbol}</span>
                <span className="pnl">+{formatCurrency(contributor.pnl)}</span>
                <span className="return">({formatPercent(contributor.daily_return)})</span>
              </div>
            ))}
          </div>
          
          <div className="detractors">
            <h4>Top Detractors</h4>
            {currentBreakdown.top_detractors.map((detractor, index) => (
              <div key={index} className="attribution-item negative">
                <span className="symbol">{detractor.symbol}</span>
                <span className="pnl">{formatCurrency(detractor.pnl)}</span>
                <span className="return">({formatPercent(detractor.daily_return)})</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Portfolio Timeline (if multiple dates) */}
      {timeSeriesData.length > 1 && (
        <div className="chart-container full-width">
          <h3>Portfolio Value Over Time</h3>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={timeSeriesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#333" />
              <XAxis 
                dataKey="date" 
                stroke="#ccc"
                fontSize={12}
              />
              <YAxis 
                yAxisId="value"
                orientation="left"
                stroke="#ccc"
                fontSize={12}
                tickFormatter={formatCurrency}
              />
              <YAxis 
                yAxisId="return"
                orientation="right"
                stroke="#ccc"
                fontSize={12}
                tickFormatter={(value) => `${value.toFixed(1)}%`}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#2a2a2a',
                  border: '1px solid #444',
                  borderRadius: '4px'
                }}
                formatter={(value, name) => {
                  if (name === 'Portfolio Value') return [formatCurrency(value), name];
                  if (name === 'Daily Return') return [`${value.toFixed(2)}%`, name];
                  return [value, name];
                }}
              />
              <Legend />
              <Area
                yAxisId="value"
                type="monotone"
                dataKey="portfolio_value"
                name="Portfolio Value"
                fill="#0088FE"
                fillOpacity={0.3}
                stroke="#0088FE"
                strokeWidth={2}
              />
              <Bar
                yAxisId="return"
                dataKey="daily_return"
                name="Daily Return"
                fill="#00C49F"
                fillOpacity={0.8}
              />
              <ReferenceLine yAxisId="return" y={0} stroke="#666" strokeDasharray="2 2" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Detailed Holdings Table */}
      <div className="holdings-table-container">
        <h3>Detailed Holdings</h3>
        <div className="holdings-table">
          <table>
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Shares</th>
                <th>Price</th>
                <th>Market Value</th>
                <th>Weight</th>
                <th>Daily P&L</th>
                <th>Daily Return</th>
              </tr>
            </thead>
            <tbody>
              {currentBreakdown.holdings.map((holding, index) => (
                <tr key={index}>
                  <td className="symbol-cell">{holding.symbol}</td>
                  <td>{holding.shares.toFixed(0)}</td>
                  <td>{formatCurrency(holding.price)}</td>
                  <td>{formatCurrency(holding.market_value)}</td>
                  <td>{formatPercent(holding.weight)}</td>
                  <td className={holding.daily_pnl >= 0 ? 'positive' : 'negative'}>
                    {formatCurrency(holding.daily_pnl)}
                  </td>
                  <td className={holding.daily_return >= 0 ? 'positive' : 'negative'}>
                    {formatPercent(holding.daily_return)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default PortfolioBreakdownDashboard;