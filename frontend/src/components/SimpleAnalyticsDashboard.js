import React, { useState, useEffect } from 'react';
import axios from 'axios';

const SimpleAnalyticsDashboard = () => {
  const [backtests, setBacktests] = useState([]);
  const [selectedBacktest, setSelectedBacktest] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [performance, setPerformance] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8001';

  // Fetch backtests list
  useEffect(() => {
    const fetchBacktests = async () => {
      try {
        setLoading(true);
        const response = await axios.get(`${API_BASE_URL}/api/v1/backtests`);
        setBacktests(response.data);
        if (response.data.length > 0) {
          setSelectedBacktest(response.data[0].backtest_run_id);
        }
      } catch (err) {
        setError(`Failed to load backtests: ${err.message}`);
      } finally {
        setLoading(false);
      }
    };

    fetchBacktests();
  }, [API_BASE_URL]);

  // Fetch metrics and performance data when backtest is selected
  useEffect(() => {
    if (!selectedBacktest) return;

    const fetchBacktestData = async () => {
      try {
        setLoading(true);
        setError(null);

        // Fetch portfolio metrics
        const metricsResponse = await axios.get(
          `${API_BASE_URL}/api/v1/backtests/${selectedBacktest}/metrics`
        );
        setMetrics(metricsResponse.data);

        // Fetch performance data
        const performanceResponse = await axios.get(
          `${API_BASE_URL}/api/v1/backtests/${selectedBacktest}/performance`
        );
        setPerformance(performanceResponse.data);

      } catch (err) {
        setError(`Failed to load backtest data: ${err.message}`);
      } finally {
        setLoading(false);
      }
    };

    fetchBacktestData();
  }, [selectedBacktest, API_BASE_URL]);

  const formatPercentage = (value) => {
    return `${(value * 100).toFixed(2)}%`;
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  if (loading && !metrics) {
    return <div style={{ padding: '20px', textAlign: 'center' }}>Loading analytics data...</div>;
  }

  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif' }}>
      <h1 style={{ marginBottom: '30px', color: '#1f77b4' }}>Portfolio Analytics Dashboard</h1>

      {error && (
        <div style={{
          background: '#fff2f0',
          border: '1px solid #ffccc7',
          padding: '10px',
          borderRadius: '4px',
          marginBottom: '20px',
          color: '#a8071a'
        }}>
          {error}
        </div>
      )}

      {/* Backtest Selection */}
      <div style={{ marginBottom: '30px' }}>
        <label style={{ fontWeight: 'bold', marginRight: '10px' }}>Select Backtest:</label>
        <select
          value={selectedBacktest || ''}
          onChange={(e) => setSelectedBacktest(e.target.value)}
          style={{
            padding: '8px',
            borderRadius: '4px',
            border: '1px solid #d9d9d9',
            minWidth: '300px'
          }}
        >
          {backtests.map((bt) => (
            <option key={bt.backtest_run_id} value={bt.backtest_run_id}>
              {bt.strategy_name} ({bt.start_date} to {bt.end_date})
            </option>
          ))}
        </select>
      </div>

      {/* Portfolio Metrics */}
      {metrics && (
        <div style={{ marginBottom: '30px' }}>
          <h2 style={{ color: '#1890ff', borderBottom: '1px solid #e8e8e8', paddingBottom: '10px' }}>
            Portfolio Performance Metrics
          </h2>
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
            gap: '20px',
            marginTop: '20px'
          }}>
            <div style={{ background: '#f6ffed', padding: '15px', borderRadius: '8px', border: '1px solid #b7eb8f' }}>
              <div style={{ fontSize: '14px', color: '#52c41a', fontWeight: 'bold' }}>Total Return</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#389e0d' }}>
                {formatPercentage(metrics.total_return)}
              </div>
            </div>

            <div style={{ background: '#f0f5ff', padding: '15px', borderRadius: '8px', border: '1px solid #adc6ff' }}>
              <div style={{ fontSize: '14px', color: '#1890ff', fontWeight: 'bold' }}>Sharpe Ratio</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#096dd9' }}>
                {metrics.sharpe_ratio.toFixed(3)}
              </div>
            </div>

            <div style={{ background: '#fff2e8', padding: '15px', borderRadius: '8px', border: '1px solid #ffbb96' }}>
              <div style={{ fontSize: '14px', color: '#fa8c16', fontWeight: 'bold' }}>Max Drawdown</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#d46b08' }}>
                {formatPercentage(metrics.max_drawdown)}
              </div>
            </div>

            <div style={{ background: '#f9f0ff', padding: '15px', borderRadius: '8px', border: '1px solid #d3adf7' }}>
              <div style={{ fontSize: '14px', color: '#722ed1', fontWeight: 'bold' }}>Volatility</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#531dab' }}>
                {formatPercentage(metrics.volatility)}
              </div>
            </div>

            <div style={{ background: '#e6f7ff', padding: '15px', borderRadius: '8px', border: '1px solid #91d5ff' }}>
              <div style={{ fontSize: '14px', color: '#13c2c2', fontWeight: 'bold' }}>Win Rate</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#08979c' }}>
                {formatPercentage(metrics.win_rate)}
              </div>
            </div>

            <div style={{ background: '#fff1f0', padding: '15px', borderRadius: '8px', border: '1px solid #ffa39e' }}>
              <div style={{ fontSize: '14px', color: '#f5222d', fontWeight: 'bold' }}>Number of Trades</div>
              <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#cf1322' }}>
                {metrics.num_trades.toLocaleString()}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Performance Chart */}
      {performance.length > 0 && (
        <div style={{ marginBottom: '30px' }}>
          <h2 style={{ color: '#1890ff', borderBottom: '1px solid #e8e8e8', paddingBottom: '10px' }}>
            Portfolio Performance Over Time
          </h2>
          <div style={{
            background: '#fafafa',
            padding: '20px',
            borderRadius: '8px',
            border: '1px solid #d9d9d9',
            marginTop: '20px'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '10px' }}>
              <span style={{ fontWeight: 'bold' }}>Performance Data Points: {performance.length}</span>
              <span style={{ color: '#666' }}>
                {performance[0]?.date} to {performance[performance.length - 1]?.date}
              </span>
            </div>
            
            {/* Simple table for performance data (first 10 rows) */}
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                <thead>
                  <tr style={{ background: '#f0f0f0' }}>
                    <th style={{ padding: '8px', border: '1px solid #d9d9d9', textAlign: 'left' }}>Date</th>
                    <th style={{ padding: '8px', border: '1px solid #d9d9d9', textAlign: 'right' }}>Portfolio Value</th>
                    <th style={{ padding: '8px', border: '1px solid #d9d9d9', textAlign: 'right' }}>Daily Return</th>
                    <th style={{ padding: '8px', border: '1px solid #d9d9d9', textAlign: 'right' }}>Cumulative Return</th>
                    <th style={{ padding: '8px', border: '1px solid #d9d9d9', textAlign: 'right' }}>Drawdown</th>
                  </tr>
                </thead>
                <tbody>
                  {performance.slice(0, 10).map((point, index) => (
                    <tr key={index}>
                      <td style={{ padding: '8px', border: '1px solid #d9d9d9' }}>{point.date}</td>
                      <td style={{ padding: '8px', border: '1px solid #d9d9d9', textAlign: 'right' }}>
                        {formatCurrency(point.portfolio_value)}
                      </td>
                      <td style={{
                        padding: '8px',
                        border: '1px solid #d9d9d9',
                        textAlign: 'right',
                        color: point.daily_return >= 0 ? '#52c41a' : '#f5222d'
                      }}>
                        {formatPercentage(point.daily_return)}
                      </td>
                      <td style={{
                        padding: '8px',
                        border: '1px solid #d9d9d9',
                        textAlign: 'right',
                        color: point.cumulative_return >= 0 ? '#52c41a' : '#f5222d'
                      }}>
                        {formatPercentage(point.cumulative_return)}
                      </td>
                      <td style={{
                        padding: '8px',
                        border: '1px solid #d9d9d9',
                        textAlign: 'right',
                        color: '#f5222d'
                      }}>
                        {formatPercentage(point.drawdown)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {performance.length > 10 && (
                <div style={{ textAlign: 'center', marginTop: '10px', color: '#666' }}>
                  ... and {performance.length - 10} more data points
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* API Connection Status */}
      <div style={{
        background: '#f6ffed',
        border: '1px solid #b7eb8f',
        padding: '15px',
        borderRadius: '8px',
        marginTop: '20px'
      }}>
        <h3 style={{ margin: '0 0 10px 0', color: '#52c41a' }}>🔗 API Connection Status</h3>
        <p style={{ margin: '5px 0', fontSize: '14px' }}>
          <strong>API Base URL:</strong> {API_BASE_URL}
        </p>
        <p style={{ margin: '5px 0', fontSize: '14px' }}>
          <strong>Status:</strong> {error ? '❌ Error' : '✅ Connected'}
        </p>
        <p style={{ margin: '5px 0', fontSize: '14px' }}>
          <strong>Backtests Available:</strong> {backtests.length}
        </p>
      </div>
    </div>
  );
};

export default SimpleAnalyticsDashboard;