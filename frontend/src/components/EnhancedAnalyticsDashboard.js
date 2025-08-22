import React, { useState, useEffect } from 'react';
import { 
  LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, 
  Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, ReferenceLine 
} from 'recharts';
import ModelComparisonDashboard from './ModelComparisonDashboard';
import PortfolioBreakdownDashboard from './PortfolioBreakdownDashboard';
import DataCoverageDashboard from './DataCoverageDashboard';
import './EnhancedAnalyticsDashboard.css';

const EnhancedAnalyticsDashboard = () => {
  const [backtests, setBacktests] = useState([]);
  const [selectedBacktest, setSelectedBacktest] = useState(null);
  const [backtestDetails, setBacktestDetails] = useState(null);
  const [symbolPerformance, setSymbolPerformance] = useState([]);
  const [performance, setPerformance] = useState([]);
  const [marketRegimes, setMarketRegimes] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('overview');
  const [selectedBacktestsForComparison, setSelectedBacktestsForComparison] = useState([]);
  const [comparisonResults, setComparisonResults] = useState(null);

  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000';

  // Fetch all data
  useEffect(() => {
    const fetchAllData = async () => {
      try {
        setLoading(true);
        setError(null);

        // Fetch backtests
        const backtestsResponse = await fetch(`${API_BASE_URL}/api/v1/backtests`);
        if (!backtestsResponse.ok) throw new Error('Failed to fetch backtests');
        const backtestsData = await backtestsResponse.json();
        setBacktests(backtestsData);

        // Select comprehensive backtest by default
        const comprehensiveBacktest = backtestsData.find(bt => 
          bt.backtest_run_id === 'comprehensive_2022_2025'
        ) || backtestsData[0];
        
        if (comprehensiveBacktest) {
          setSelectedBacktest(comprehensiveBacktest.backtest_run_id);
          await fetchBacktestData(comprehensiveBacktest.backtest_run_id);
        }

        // Fetch market regimes
        const regimesResponse = await fetch(`${API_BASE_URL}/api/v1/market-regimes`);
        if (regimesResponse.ok) {
          const regimesData = await regimesResponse.json();
          setMarketRegimes(regimesData);
        }

      } catch (err) {
        setError(`Failed to load data: ${err.message}`);
      } finally {
        setLoading(false);
      }
    };

    fetchAllData();
  }, [API_BASE_URL]);

  const fetchBacktestData = async (backtestId) => {
    try {
      setLoading(true);

      // Fetch detailed backtest results
      const detailsResponse = await fetch(`${API_BASE_URL}/api/v1/backtests/${backtestId}/details`);
      if (detailsResponse.ok) {
        const detailsData = await detailsResponse.json();
        setBacktestDetails(detailsData);
      }

      // Fetch symbol performance
      const symbolsResponse = await fetch(`${API_BASE_URL}/api/v1/backtests/${backtestId}/symbols`);
      if (symbolsResponse.ok) {
        const symbolsData = await symbolsResponse.json();
        setSymbolPerformance(symbolsData);
      }

      // Fetch performance time series
      const performanceResponse = await fetch(`${API_BASE_URL}/api/v1/backtests/${backtestId}/performance`);
      if (performanceResponse.ok) {
        const performanceData = await performanceResponse.json();
        setPerformance(performanceData);
      }

    } catch (err) {
      setError(`Failed to load backtest data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleBacktestChange = (backtestId) => {
    setSelectedBacktest(backtestId);
    fetchBacktestData(backtestId);
  };

  const formatPercentage = (value, decimals = 1) => {
    return `${(value * 100).toFixed(decimals)}%`;
  };

  const formatLargePercentage = (value, decimals = 0) => {
    return `${(value * 100).toFixed(decimals)}%`;
  };

  const formatCurrency = (value) => {
    if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`;
    if (value >= 1e6) return `$${(value / 1e6).toFixed(1)}M`;
    if (value >= 1e3) return `$${(value / 1e3).toFixed(0)}K`;
    return `$${value.toFixed(0)}`;
  };

  const formatNumber = (value) => {
    return new Intl.NumberFormat().format(value);
  };

  // Backtest comparison functions
  const handleBacktestSelection = (backtestId, isSelected) => {
    if (isSelected) {
      setSelectedBacktestsForComparison(prev => [...prev, backtestId]);
    } else {
      setSelectedBacktestsForComparison(prev => prev.filter(id => id !== backtestId));
    }
  };

  const selectBacktestForAnalysis = (backtestId) => {
    setSelectedBacktest(backtestId);
    setActiveTab('overview');
    handleBacktestChange(backtestId);
  };

  const performBacktestComparison = async () => {
    if (selectedBacktestsForComparison.length < 2) return;
    
    try {
      setLoading(true);
      const comparisonData = [];
      
      for (const backtestId of selectedBacktestsForComparison) {
        const [detailsResponse, performanceResponse, symbolsResponse] = await Promise.all([
          fetch(`${API_BASE_URL}/api/v1/backtests/${backtestId}/details`),
          fetch(`${API_BASE_URL}/api/v1/backtests/${backtestId}/performance`),
          fetch(`${API_BASE_URL}/api/v1/backtests/${backtestId}/symbols`)
        ]);
        
        const details = detailsResponse.ok ? await detailsResponse.json() : null;
        const performance = performanceResponse.ok ? await performanceResponse.json() : [];
        const symbols = symbolsResponse.ok ? await symbolsResponse.json() : [];
        
        comparisonData.push({
          backtestId,
          details,
          performance,
          symbols
        });
      }
      
      setComparisonResults(comparisonData);
      setActiveTab('comparison');
    } catch (error) {
      console.error('Error performing comparison:', error);
      setError('Failed to compare backtests');
    } finally {
      setLoading(false);
    }
  };

  const combineBacktests = async () => {
    if (selectedBacktestsForComparison.length < 2) return;
    
    try {
      setLoading(true);
      // Implement combined analysis logic
      console.log('Combining backtests:', selectedBacktestsForComparison);
      
      // For now, just perform comparison
      await performBacktestComparison();
      
    } catch (error) {
      console.error('Error combining backtests:', error);
      setError('Failed to combine backtests');
    } finally {
      setLoading(false);
    }
  };

  // Chart colors
  const COLORS = ['#00d4ff', '#00ff88', '#ff4757', '#ffa502', '#a55eea', '#26de81', '#fd79a8', '#e17055'];

  if (loading && !backtestDetails) {
    return (
      <div className="dashboard-loading">
        <div className="loading-spinner"></div>
        <p>Loading comprehensive analytics...</p>
      </div>
    );
  }

  const selectedBacktestData = backtests.find(bt => bt.backtest_run_id === selectedBacktest);

  return (
    <div className="enhanced-dashboard">
      {/* Header */}
      <header className="dashboard-header">
        <h1>📊 Portfolio Analytics Platform</h1>
        <div className="header-controls">
          <select 
            value={selectedBacktest || ''} 
            onChange={(e) => handleBacktestChange(e.target.value)}
            className="backtest-selector"
          >
            {backtests.map((bt) => (
              <option key={bt.backtest_run_id} value={bt.backtest_run_id}>
                {bt.strategy_name}
              </option>
            ))}
          </select>
        </div>
      </header>

      {error && (
        <div className="error-banner">
          <span>⚠️ {error}</span>
        </div>
      )}

      {/* Navigation Tabs */}
      <nav className="dashboard-nav">
        <button 
          className={activeTab === 'overview' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('overview')}
        >
          Overview
        </button>
        <button 
          className={activeTab === 'performance' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('performance')}
        >
          Performance
        </button>
        <button 
          className={activeTab === 'symbols' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('symbols')}
        >
          Symbol Analysis
        </button>
        <button 
          className={activeTab === 'regimes' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('regimes')}
        >
          Market Regimes
        </button>
        <button 
          className={activeTab === 'portfolio' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('portfolio')}
        >
          Portfolio Breakdown
        </button>
        <button 
          className={activeTab === 'backtests' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('backtests')}
        >
          Backtest Comparison
        </button>
        <button 
          className={activeTab === 'comparison' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('comparison')}
        >
          Model Comparison
        </button>
        <button 
          className={activeTab === 'coverage' ? 'nav-tab active' : 'nav-tab'}
          onClick={() => setActiveTab('coverage')}
        >
          Data Coverage
        </button>
      </nav>

      {/* Main Content */}
      <main className="dashboard-main">
        {/* Overview Tab */}
        {activeTab === 'overview' && selectedBacktestData && (
          <div className="overview-tab">
            {/* Summary Cards */}
            <div className="summary-cards">
              <div className="summary-card highlight">
                <div className="card-label">Total Return</div>
                <div className="card-value primary">
                  {formatLargePercentage(selectedBacktestData.total_return)}
                </div>
                <div className="card-change positive">
                  +{formatCurrency(selectedBacktestData.final_value - selectedBacktestData.initial_capital)} gain
                </div>
              </div>

              <div className="summary-card">
                <div className="card-label">Annualized Return</div>
                <div className="card-value">
                  {formatLargePercentage(selectedBacktestData.annualized_return)}
                </div>
                <div className="card-sublabel">Annual compound growth</div>
              </div>

              <div className="summary-card">
                <div className="card-label">Sharpe Ratio</div>
                <div className="card-value">
                  {selectedBacktestData.sharpe_ratio?.toFixed(2) || 'N/A'}
                </div>
                <div className="card-sublabel">Risk-adjusted return</div>
              </div>

              <div className="summary-card">
                <div className="card-label">Max Drawdown</div>
                <div className="card-value warning">
                  {formatPercentage(selectedBacktestData.max_drawdown)}
                </div>
                <div className="card-sublabel">Peak-to-trough decline</div>
              </div>

              <div className="summary-card">
                <div className="card-label">Portfolio Value</div>
                <div className="card-value">
                  {formatCurrency(selectedBacktestData.final_value)}
                </div>
                <div className="card-sublabel">
                  From {formatCurrency(selectedBacktestData.initial_capital)}
                </div>
              </div>

              <div className="summary-card">
                <div className="card-label">Universe Size</div>
                <div className="card-value">
                  {selectedBacktestData.universe_size}
                </div>
                <div className="card-sublabel">Symbols analyzed</div>
              </div>
            </div>

            {/* Performance Chart */}
            {performance.length > 0 && (
              <div className="chart-container">
                <h3>Portfolio Performance Over Time</h3>
                <ResponsiveContainer width="100%" height={400}>
                  <AreaChart data={performance}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis 
                      dataKey="date" 
                      stroke="#888"
                      tick={{ fontSize: 12 }}
                    />
                    <YAxis 
                      stroke="#888"
                      tick={{ fontSize: 12 }}
                      tickFormatter={formatCurrency}
                    />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: '#1a2332', 
                        border: '1px solid #00d4ff',
                        color: '#fff'
                      }}
                      labelFormatter={(value) => `Date: ${value}`}
                      formatter={(value, name) => [
                        name === 'portfolio_value' ? formatCurrency(value) : formatPercentage(value),
                        name === 'portfolio_value' ? 'Portfolio Value' : 
                        name === 'cumulative_return' ? 'Cumulative Return' : 'Drawdown'
                      ]}
                    />
                    <Area 
                      type="monotone" 
                      dataKey="portfolio_value" 
                      stroke="#00d4ff" 
                      fill="rgba(0, 212, 255, 0.1)"
                      strokeWidth={2}
                    />
                    <ReferenceLine y={selectedBacktestData.initial_capital} stroke="#666" strokeDasharray="2 2" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Key Insights */}
            {backtestDetails?.key_insights && (
              <div className="insights-container">
                <h3>🎯 Key Insights</h3>
                <ul className="insights-list">
                  {backtestDetails.key_insights.map((insight, index) => (
                    <li key={index} className="insight-item">
                      {insight}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* Performance Tab */}
        {activeTab === 'performance' && (
          <div className="performance-tab">
            {performance.length > 0 && (
              <>
                {/* Cumulative vs Drawdown Chart */}
                <div className="chart-container">
                  <h3>Cumulative Return vs Drawdown</h3>
                  <ResponsiveContainer width="100%" height={400}>
                    <LineChart data={performance}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                      <XAxis dataKey="date" stroke="#888" tick={{ fontSize: 12 }} />
                      <YAxis stroke="#888" tick={{ fontSize: 12 }} tickFormatter={formatPercentage} />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#1a2332', border: '1px solid #00d4ff', color: '#fff' }}
                        formatter={(value, name) => [
                          formatPercentage(value),
                          name === 'cumulative_return' ? 'Cumulative Return' : 'Drawdown'
                        ]}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="cumulative_return" 
                        stroke="#00ff88" 
                        strokeWidth={2}
                        name="Cumulative Return"
                      />
                      <Line 
                        type="monotone" 
                        dataKey="drawdown" 
                        stroke="#ff4757" 
                        strokeWidth={2}
                        name="Drawdown"
                      />
                      <ReferenceLine y={0} stroke="#666" strokeDasharray="2 2" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>

                {/* Performance Statistics */}
                <div className="stats-grid">
                  <div className="stat-card">
                    <div className="stat-label">Best Day</div>
                    <div className="stat-value positive">
                      {Math.max(...performance.map(p => p.daily_return)).toFixed(4) * 100}%
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-label">Worst Day</div>
                    <div className="stat-value negative">
                      {Math.min(...performance.map(p => p.daily_return)).toFixed(4) * 100}%
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-label">Volatility (Annualized)</div>
                    <div className="stat-value">
                      {(Math.sqrt(252) * 
                        Math.sqrt(performance.reduce((acc, p, i) => 
                          i === 0 ? 0 : acc + Math.pow(p.daily_return - 
                            performance.reduce((sum, pp) => sum + pp.daily_return, 0) / performance.length, 2
                          ), 0) / (performance.length - 1)) * 100
                      ).toFixed(1)}%
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-label">Trading Days</div>
                    <div className="stat-value">
                      {performance.length}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        )}

        {/* Symbol Analysis Tab */}
        {activeTab === 'symbols' && (
          <div className="symbols-tab">
            {symbolPerformance.length > 0 && (
              <>
                {/* Top Performers Bar Chart */}
                <div className="chart-container">
                  <h3>Symbol Performance Ranking</h3>
                  <ResponsiveContainer width="100%" height={400}>
                    <BarChart data={symbolPerformance.slice(0, 10)} layout="horizontal">
                      <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                      <XAxis type="number" stroke="#888" tickFormatter={formatLargePercentage} />
                      <YAxis dataKey="symbol" type="category" stroke="#888" width={60} />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#1a2332', border: '1px solid #00d4ff', color: '#fff' }}
                        formatter={(value) => [formatLargePercentage(value), 'Total Return']}
                      />
                      <Bar dataKey="total_return" fill="#00d4ff" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Symbol Performance Table */}
                <div className="table-container">
                  <h3>Detailed Symbol Performance</h3>
                  <table className="performance-table">
                    <thead>
                      <tr>
                        <th>Rank</th>
                        <th>Symbol</th>
                        <th>Start Price</th>
                        <th>End Price</th>
                        <th>Total Return</th>
                        <th>Trading Days</th>
                      </tr>
                    </thead>
                    <tbody>
                      {symbolPerformance.map((symbol) => (
                        <tr key={symbol.symbol}>
                          <td className="rank-cell">#{symbol.rank}</td>
                          <td className="symbol-cell">{symbol.symbol}</td>
                          <td>${symbol.start_price.toFixed(2)}</td>
                          <td>${symbol.end_price.toFixed(2)}</td>
                          <td className={symbol.total_return > 0 ? 'positive' : 'negative'}>
                            {formatLargePercentage(symbol.total_return)}
                          </td>
                          <td>{symbol.trading_days}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}

        {/* Market Regimes Tab */}
        {activeTab === 'regimes' && (
          <div className="regimes-tab">
            {marketRegimes.length > 0 && (
              <div className="regimes-container">
                <h3>🌍 Market Regime Analysis (2022-2025)</h3>
                <div className="regimes-grid">
                  {marketRegimes.map((regime, index) => (
                    <div key={index} className="regime-card">
                      <div className="regime-header">
                        <h4>{regime.period_name}</h4>
                        <span className="regime-period">
                          {regime.start_date} to {regime.end_date}
                        </span>
                      </div>
                      <div className="regime-context">
                        {regime.market_context}
                      </div>
                      {regime.characteristics && (
                        <div className="regime-characteristics">
                          <strong>Key Characteristics:</strong>
                          <ul>
                            {regime.characteristics.map((char, i) => (
                              <li key={i}>{char}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {regime.key_events && (
                        <div className="regime-events">
                          <strong>Key Events:</strong>
                          <ul>
                            {regime.key_events.map((event, i) => (
                              <li key={i}>{event}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Portfolio Breakdown Tab */}
        {activeTab === 'portfolio' && selectedBacktest && (
          <div className="portfolio-tab">
            <PortfolioBreakdownDashboard backtestRunId={selectedBacktest} />
          </div>
        )}

        {/* Backtests Table Tab */}
        {activeTab === 'backtests' && (
          <div className="backtests-tab">
            <div className="backtests-header">
              <h2>Backtest Analysis & Comparison</h2>
              <p>Select backtests to analyze individually or compare side-by-side</p>
            </div>
            
            <div className="backtests-table-container">
              <table className="backtests-table">
                <thead>
                  <tr>
                    <th>Select</th>
                    <th>Strategy Name</th>
                    <th>Period</th>
                    <th>Total Return</th>
                    <th>Annualized Return</th>
                    <th>Sharpe Ratio</th>
                    <th>Max Drawdown</th>
                    <th>Status</th>
                    <th>Universe Size</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {backtests.map(backtest => (
                    <tr key={backtest.backtest_run_id} className="backtest-row">
                      <td>
                        <input 
                          type="checkbox" 
                          id={`select-${backtest.backtest_run_id}`}
                          onChange={(e) => handleBacktestSelection(backtest.backtest_run_id, e.target.checked)}
                        />
                      </td>
                      <td className="strategy-name">{backtest.strategy_name}</td>
                      <td className="period">
                        {new Date(backtest.start_date).toLocaleDateString()} - {new Date(backtest.end_date).toLocaleDateString()}
                      </td>
                      <td className={`return-value ${backtest.total_return >= 0 ? 'positive' : 'negative'}`}>
                        {formatPercentage(backtest.total_return)}
                      </td>
                      <td className={`return-value ${(backtest.annualized_return || 0) >= 0 ? 'positive' : 'negative'}`}>
                        {backtest.annualized_return ? formatPercentage(backtest.annualized_return) : 'N/A'}
                      </td>
                      <td className="metric-value">
                        {backtest.sharpe_ratio ? backtest.sharpe_ratio.toFixed(2) : 'N/A'}
                      </td>
                      <td className="drawdown-value">
                        {formatPercentage(backtest.max_drawdown)}
                      </td>
                      <td>
                        <span className={`status-badge ${backtest.status}`}>
                          {backtest.status}
                        </span>
                      </td>
                      <td className="universe-size">
                        {backtest.universe_size || 'N/A'}
                      </td>
                      <td className="actions">
                        <button 
                          className="action-btn analyze"
                          onClick={() => selectBacktestForAnalysis(backtest.backtest_run_id)}
                        >
                          Analyze
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {selectedBacktestsForComparison.length >= 2 && (
              <div className="comparison-actions">
                <h3>Compare Selected Backtests</h3>
                <div className="comparison-buttons">
                  <button 
                    className="btn-primary"
                    onClick={performBacktestComparison}
                  >
                    Compare Performance
                  </button>
                  <button 
                    className="btn-secondary"
                    onClick={combineBacktests}
                  >
                    Combine Analysis
                  </button>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Comparison Results Tab */}
        {activeTab === 'comparison' && comparisonResults && (
          <div className="comparison-results-tab">
            <div className="comparison-header">
              <h2>Backtest Comparison Analysis</h2>
              <p>Comparing {comparisonResults.length} backtests side-by-side</p>
            </div>

            {/* Comparison Summary Table */}
            <div className="comparison-summary-table">
              <h3>Performance Comparison Summary</h3>
              <table className="comparison-table">
                <thead>
                  <tr>
                    <th>Strategy</th>
                    <th>Period</th>
                    <th>Total Return</th>
                    <th>Annualized Return</th>
                    <th>Sharpe Ratio</th>
                    <th>Max Drawdown</th>
                    <th>Volatility</th>
                    <th>Best Month</th>
                    <th>Worst Month</th>
                  </tr>
                </thead>
                <tbody>
                  {comparisonResults.map((result, index) => {
                    const backtest = backtests.find(bt => bt.backtest_run_id === result.backtestId);
                    return (
                      <tr key={index}>
                        <td className="strategy-name">{backtest?.strategy_name || result.backtestId}</td>
                        <td className="period">
                          {backtest ? `${new Date(backtest.start_date).toLocaleDateString()} - ${new Date(backtest.end_date).toLocaleDateString()}` : 'N/A'}
                        </td>
                        <td className={`return-value ${(backtest?.total_return || 0) >= 0 ? 'positive' : 'negative'}`}>
                          {backtest ? formatLargePercentage(backtest.total_return) : 'N/A'}
                        </td>
                        <td className={`return-value ${(backtest?.annualized_return || 0) >= 0 ? 'positive' : 'negative'}`}>
                          {backtest?.annualized_return ? formatLargePercentage(backtest.annualized_return) : 'N/A'}
                        </td>
                        <td className="metric-value">
                          {backtest?.sharpe_ratio ? backtest.sharpe_ratio.toFixed(2) : 'N/A'}
                        </td>
                        <td className="drawdown-value">
                          {backtest ? formatPercentage(backtest.max_drawdown) : 'N/A'}
                        </td>
                        <td className="metric-value">
                          {result.performance && result.performance.length > 0 ? 
                            (Math.sqrt(252) * Math.sqrt(
                              result.performance.reduce((acc, p, i) => 
                                i === 0 ? 0 : acc + Math.pow(p.daily_return - 
                                  result.performance.reduce((sum, pp) => sum + pp.daily_return, 0) / result.performance.length, 2
                                ), 0) / (result.performance.length - 1)) * 100
                            ).toFixed(1) + '%' : 'N/A'
                          }
                        </td>
                        <td className="positive">
                          {result.performance && result.performance.length > 0 ? 
                            (Math.max(...result.performance.map(p => p.daily_return)) * 100).toFixed(2) + '%' : 'N/A'
                          }
                        </td>
                        <td className="negative">
                          {result.performance && result.performance.length > 0 ? 
                            (Math.min(...result.performance.map(p => p.daily_return)) * 100).toFixed(2) + '%' : 'N/A'
                          }
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* Side-by-Side Performance Charts */}
            <div className="comparison-charts">
              <div className="chart-container full-width">
                <h3>Performance Comparison Over Time</h3>
                <ResponsiveContainer width="100%" height={500}>
                  <LineChart>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis 
                      dataKey="date" 
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
                      formatter={(value, name) => [formatPercentage(value), name]}
                    />
                    <Legend />
                    {comparisonResults.map((result, index) => {
                      const backtest = backtests.find(bt => bt.backtest_run_id === result.backtestId);
                      return (
                        <Line 
                          key={index}
                          type="monotone" 
                          dataKey={`cumulative_return_${index}`}
                          data={result.performance || []}
                          stroke={COLORS[index % COLORS.length]}
                          strokeWidth={2}
                          name={backtest?.strategy_name || result.backtestId}
                          connectNulls={true}
                        />
                      );
                    })}
                  </LineChart>
                </ResponsiveContainer>
              </div>

              {/* Risk-Return Scatter Plot */}
              <div className="charts-row">
                <div className="chart-container">
                  <h3>Risk-Return Profile</h3>
                  <ResponsiveContainer width="100%" height={400}>
                    <LineChart 
                      data={comparisonResults.map((result, index) => {
                        const backtest = backtests.find(bt => bt.backtest_run_id === result.backtestId);
                        const volatility = result.performance && result.performance.length > 0 ? 
                          Math.sqrt(252) * Math.sqrt(
                            result.performance.reduce((acc, p, i) => 
                              i === 0 ? 0 : acc + Math.pow(p.daily_return - 
                                result.performance.reduce((sum, pp) => sum + pp.daily_return, 0) / result.performance.length, 2
                              ), 0) / (result.performance.length - 1)
                          ) : 0;
                        return {
                          strategy: backtest?.strategy_name || result.backtestId,
                          annualized_return: (backtest?.annualized_return || 0) * 100,
                          volatility: volatility * 100,
                          sharpe_ratio: backtest?.sharpe_ratio || 0
                        };
                      })}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                      <XAxis 
                        dataKey="volatility"
                        stroke="#888"
                        tick={{ fontSize: 12 }}
                        label={{ value: 'Volatility (%)', position: 'insideBottom', offset: -5 }}
                      />
                      <YAxis 
                        dataKey="annualized_return"
                        stroke="#888"
                        tick={{ fontSize: 12 }}
                        label={{ value: 'Return (%)', angle: -90, position: 'insideLeft' }}
                      />
                      <Tooltip 
                        contentStyle={{ 
                          backgroundColor: '#1a2332', 
                          border: '1px solid #00d4ff',
                          color: '#fff'
                        }}
                        formatter={(value, name) => {
                          if (name === 'annualized_return') return [`${value.toFixed(1)}%`, 'Annual Return'];
                          if (name === 'volatility') return [`${value.toFixed(1)}%`, 'Volatility'];
                          if (name === 'sharpe_ratio') return [value.toFixed(2), 'Sharpe Ratio'];
                          return [value, name];
                        }}
                      />
                      {comparisonResults.map((result, index) => (
                        <Bar
                          key={index}
                          dataKey="sharpe_ratio"
                          fill={COLORS[index % COLORS.length]}
                          fillOpacity={0.6}
                        />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                </div>

                {/* Drawdown Comparison */}
                <div className="chart-container">
                  <h3>Maximum Drawdown Comparison</h3>
                  <ResponsiveContainer width="100%" height={400}>
                    <BarChart 
                      data={comparisonResults.map((result, index) => {
                        const backtest = backtests.find(bt => bt.backtest_run_id === result.backtestId);
                        return {
                          strategy: backtest?.strategy_name || result.backtestId,
                          max_drawdown: Math.abs((backtest?.max_drawdown || 0) * 100)
                        };
                      })}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                      <XAxis 
                        dataKey="strategy"
                        stroke="#888"
                        tick={{ fontSize: 10, angle: -45 }}
                        height={80}
                      />
                      <YAxis 
                        stroke="#888"
                        tick={{ fontSize: 12 }}
                        tickFormatter={(value) => `-${value.toFixed(1)}%`}
                      />
                      <Tooltip 
                        contentStyle={{ 
                          backgroundColor: '#1a2332', 
                          border: '1px solid #00d4ff',
                          color: '#fff'
                        }}
                        formatter={(value) => [`-${value.toFixed(1)}%`, 'Max Drawdown']}
                      />
                      <Bar dataKey="max_drawdown" fill="#ff4757" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            {/* Statistical Significance Tests */}
            <div className="statistical-analysis">
              <h3>Statistical Analysis</h3>
              <div className="stats-grid">
                {comparisonResults.map((result, index) => {
                  const backtest = backtests.find(bt => bt.backtest_run_id === result.backtestId);
                  const returns = result.performance?.map(p => p.daily_return) || [];
                  const positiveReturns = returns.filter(r => r > 0).length;
                  const negativeReturns = returns.filter(r => r < 0).length;
                  const winRate = returns.length > 0 ? (positiveReturns / returns.length) : 0;
                  
                  return (
                    <div key={index} className="stat-card">
                      <h4>{backtest?.strategy_name || result.backtestId}</h4>
                      <div className="stat-row">
                        <span className="stat-label">Win Rate:</span>
                        <span className={`stat-value ${winRate >= 0.5 ? 'positive' : 'negative'}`}>
                          {(winRate * 100).toFixed(1)}%
                        </span>
                      </div>
                      <div className="stat-row">
                        <span className="stat-label">Trading Days:</span>
                        <span className="stat-value">{returns.length}</span>
                      </div>
                      <div className="stat-row">
                        <span className="stat-label">Positive Days:</span>
                        <span className="stat-value positive">{positiveReturns}</span>
                      </div>
                      <div className="stat-row">
                        <span className="stat-label">Negative Days:</span>
                        <span className="stat-value negative">{negativeReturns}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Action Buttons */}
            <div className="comparison-actions">
              <h3>Analysis Actions</h3>
              <div className="comparison-buttons">
                <button 
                  className="btn-primary"
                  onClick={() => setActiveTab('backtests')}
                >
                  Select Different Backtests
                </button>
                <button 
                  className="btn-secondary"
                  onClick={() => {
                    setComparisonResults(null);
                    setSelectedBacktestsForComparison([]);
                    setActiveTab('overview');
                  }}
                >
                  Clear Comparison
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Model Comparison Tab */}
        {activeTab === 'comparison' && !comparisonResults && (
          <div className="comparison-tab">
            <ModelComparisonDashboard />
          </div>
        )}

        {/* Data Coverage Tab */}
        {activeTab === 'coverage' && (
          <div className="coverage-tab">
            <DataCoverageDashboard />
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="dashboard-footer">
        <div className="footer-content">
          <span>🔗 Connected to: {API_BASE_URL}</span>
          <span>📊 {backtests.length} backtests available</span>
          <span>📈 Data coverage monitoring enabled</span>
          <span>⚡ Real-time analytics platform</span>
        </div>
      </footer>
    </div>
  );
};

export default EnhancedAnalyticsDashboard;