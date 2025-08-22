import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  LineChart, Line, ResponsiveContainer, ComposedChart, ReferenceLine
} from 'recharts';

const ComparisonDashboard = () => {
  const [currentPortfolio, setCurrentPortfolio] = useState(null);
  const [backtestStrategies, setBacktestStrategies] = useState([]);
  const [selectedStrategy, setSelectedStrategy] = useState('comprehensive_2022_2025');
  const [comparisonData, setComparisonData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Mock comparison data
  const mockComparisonData = {
    current_vs_strategy: {
      current_allocation: {
        "AAPL": 0.2106,
        "MSFT": 0.1986,
        "GOOGL": 0.0676,
        "NVDA": 0.1666,
        "JPM": 0.1120,
        "JNJ": 0.1521,
        "Cash": 0.0925
      },
      strategy_allocation: {
        "AAPL": 0.15,
        "MSFT": 0.20,
        "GOOGL": 0.12,
        "NVDA": 0.10,
        "AMZN": 0.18,
        "TSLA": 0.08,
        "META": 0.10,
        "JPM": 0.05,
        "JNJ": 0.02
      },
      deviation_analysis: {
        over_allocated: [
          { symbol: "AAPL", current: 0.2106, target: 0.15, deviation: 0.0606 },
          { symbol: "NVDA", current: 0.1666, target: 0.10, deviation: 0.0666 },
          { symbol: "JPM", current: 0.1120, target: 0.05, deviation: 0.0620 },
          { symbol: "JNJ", current: 0.1521, target: 0.02, deviation: 0.1321 }
        ],
        under_allocated: [
          { symbol: "AMZN", current: 0, target: 0.18, deviation: -0.18 },
          { symbol: "TSLA", current: 0, target: 0.08, deviation: -0.08 },
          { symbol: "META", current: 0, target: 0.10, deviation: -0.10 }
        ],
        alignment_score: 0.68
      },
      performance_comparison: {
        current_ytd: 0.25,
        strategy_ytd: 0.32,
        current_sharpe: 1.35,
        strategy_sharpe: 1.58,
        current_volatility: 0.18,
        strategy_volatility: 0.16,
        tracking_error: 0.08
      }
    },
    rebalancing_recommendations: [
      { action: "SELL", symbol: "JNJ", current_weight: 0.1521, target_weight: 0.02, amount: -131250 },
      { action: "SELL", symbol: "AAPL", current_weight: 0.2106, target_weight: 0.15, amount: -75750 },
      { action: "BUY", symbol: "AMZN", current_weight: 0, target_weight: 0.18, amount: 225000 },
      { action: "BUY", symbol: "META", current_weight: 0, target_weight: 0.10, amount: 125000 },
      { action: "BUY", symbol: "TSLA", current_weight: 0, target_weight: 0.08, amount: 100000 }
    ],
    risk_attribution: {
      sector_risk_current: {
        "Technology": 0.75,
        "Financials": 0.15,
        "Healthcare": 0.10
      },
      sector_risk_strategy: {
        "Technology": 0.82,
        "Financials": 0.08,
        "Healthcare": 0.05,
        "Consumer": 0.05
      }
    }
  };

  useEffect(() => {
    const fetchComparisonData = async () => {
      try {
        setLoading(true);
        
        // In a real implementation, these would be separate API calls:
        // const currentResponse = await fetch('http://localhost:8001/api/v1/portfolio/current');
        // const strategiesResponse = await fetch('http://localhost:8000/api/v1/backtests');
        // const comparisonResponse = await fetch(`http://localhost:8001/api/v1/portfolio/compare/${selectedStrategy}`);
        
        // Simulate API delay
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        setComparisonData(mockComparisonData);
        setBacktestStrategies([
          { id: 'comprehensive_2022_2025', name: 'Comprehensive Strategy 2022-2025' },
          { id: 'adaptive_sr_2024', name: 'Adaptive Support/Resistance 2024' },
          { id: 'momentum_2024', name: 'Momentum Strategy 2024' }
        ]);
        setError(null);
      } catch (err) {
        console.error('Error fetching comparison data:', err);
        setError('Failed to load comparison data');
      } finally {
        setLoading(false);
      }
    };

    fetchComparisonData();
  }, [selectedStrategy]);

  const formatPercent = (value) => {
    return `${(value * 100).toFixed(1)}%`;
  };

  const formatCurrency = (value) => {
    if (Math.abs(value) >= 1000000) {
      return `$${(value / 1000000).toFixed(1)}M`;
    } else if (Math.abs(value) >= 1000) {
      return `$${(value / 1000).toFixed(1)}K`;
    }
    return `$${value.toFixed(0)}`;
  };

  if (loading) {
    return (
      <div className="loading">
        <div className="loading-spinner"></div>
        Loading portfolio comparison...
      </div>
    );
  }

  if (error) {
    return <div className="error">{error}</div>;
  }

  if (!comparisonData) {
    return <div className="error">No comparison data available</div>;
  }

  // Prepare allocation comparison data
  const allSymbols = new Set([
    ...Object.keys(comparisonData.current_vs_strategy.current_allocation),
    ...Object.keys(comparisonData.current_vs_strategy.strategy_allocation)
  ]);

  const allocationComparisonData = Array.from(allSymbols).map(symbol => ({
    symbol,
    current: (comparisonData.current_vs_strategy.current_allocation[symbol] || 0) * 100,
    strategy: (comparisonData.current_vs_strategy.strategy_allocation[symbol] || 0) * 100,
    deviation: ((comparisonData.current_vs_strategy.current_allocation[symbol] || 0) - 
                (comparisonData.current_vs_strategy.strategy_allocation[symbol] || 0)) * 100
  }));

  return (
    <div className="comparison-dashboard">
      {/* Header */}
      <div style={{ marginBottom: '2rem' }}>
        <h2 style={{ color: '#00d4ff', fontSize: '1.75rem', marginBottom: '1rem' }}>
          Portfolio vs Strategy Comparison
        </h2>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem' }}>
          <label style={{ color: '#b4bcc8', fontWeight: '500' }}>Compare against:</label>
          <select 
            value={selectedStrategy}
            onChange={(e) => setSelectedStrategy(e.target.value)}
            style={{
              background: 'rgba(26, 35, 50, 0.8)',
              border: '1px solid rgba(0, 212, 255, 0.3)',
              borderRadius: '6px',
              color: '#ffffff',
              padding: '0.5rem 1rem',
              fontSize: '0.875rem'
            }}
          >
            {backtestStrategies.map(strategy => (
              <option key={strategy.id} value={strategy.id}>
                {strategy.name}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Alignment Score */}
      <div className="summary-cards">
        <div className="summary-card highlight">
          <div className="card-label">Strategy Alignment Score</div>
          <div className="card-value primary">
            {(comparisonData.current_vs_strategy.deviation_analysis.alignment_score * 100).toFixed(0)}%
          </div>
          <div className="card-sublabel">Portfolio matches strategy</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Performance Gap</div>
          <div className="card-value negative">
            {formatPercent(comparisonData.current_vs_strategy.performance_comparison.strategy_ytd - 
                          comparisonData.current_vs_strategy.performance_comparison.current_ytd)}
          </div>
          <div className="card-sublabel">Strategy outperforming YTD</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Tracking Error</div>
          <div className="card-value">
            {formatPercent(comparisonData.current_vs_strategy.performance_comparison.tracking_error)}
          </div>
          <div className="card-sublabel">Deviation from strategy</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Sharpe Difference</div>
          <div className={`card-value ${comparisonData.current_vs_strategy.performance_comparison.strategy_sharpe > 
                          comparisonData.current_vs_strategy.performance_comparison.current_sharpe ? 'negative' : 'positive'}`}>
            {(comparisonData.current_vs_strategy.performance_comparison.strategy_sharpe - 
              comparisonData.current_vs_strategy.performance_comparison.current_sharpe).toFixed(2)}
          </div>
          <div className="card-sublabel">Risk-adjusted return difference</div>
        </div>
      </div>

      {/* Allocation Comparison Chart */}
      <div className="chart-container">
        <h3>Portfolio Allocation Comparison</h3>
        <ResponsiveContainer width="100%" height={400}>
          <ComposedChart data={allocationComparisonData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#333" />
            <XAxis 
              dataKey="symbol" 
              stroke="#ccc"
              fontSize={12}
            />
            <YAxis 
              stroke="#ccc"
              fontSize={12}
              tickFormatter={(value) => `${value}%`}
            />
            <Tooltip 
              contentStyle={{
                backgroundColor: '#2a2a2a',
                border: '1px solid #444',
                borderRadius: '4px'
              }}
              formatter={(value, name) => {
                if (name === 'Current Portfolio') return [`${value.toFixed(1)}%`, name];
                if (name === 'Target Strategy') return [`${value.toFixed(1)}%`, name];
                if (name === 'Deviation') return [`${value > 0 ? '+' : ''}${value.toFixed(1)}%`, name];
                return [value, name];
              }}
            />
            <Legend />
            <Bar 
              dataKey="current" 
              name="Current Portfolio"
              fill="#0088FE" 
            />
            <Bar 
              dataKey="strategy" 
              name="Target Strategy"
              fill="#00C49F" 
            />
            <Line 
              type="monotone" 
              dataKey="deviation" 
              name="Deviation"
              stroke="#FF8042" 
              strokeWidth={2}
              dot={{ fill: '#FF8042', r: 4 }}
            />
            <ReferenceLine y={0} stroke="#666" strokeDasharray="2 2" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* Deviation Analysis */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem', marginBottom: '2rem' }}>
        <div className="chart-container">
          <h3>Over-Allocated Positions</h3>
          {comparisonData.current_vs_strategy.deviation_analysis.over_allocated.map((item, index) => (
            <div key={index} style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'center',
              padding: '0.75rem',
              marginBottom: '0.5rem',
              background: 'rgba(255, 165, 2, 0.1)',
              borderRadius: '6px',
              border: '1px solid rgba(255, 165, 2, 0.2)'
            }}>
              <div>
                <span style={{ color: '#00d4ff', fontWeight: '600' }}>{item.symbol}</span>
                <div style={{ fontSize: '0.875rem', color: '#b4bcc8' }}>
                  Current: {formatPercent(item.current)} | Target: {formatPercent(item.target)}
                </div>
              </div>
              <div style={{ color: '#ffa502', fontWeight: '600', textAlign: 'right' }}>
                +{formatPercent(item.deviation)}
              </div>
            </div>
          ))}
        </div>

        <div className="chart-container">
          <h3>Under-Allocated Positions</h3>
          {comparisonData.current_vs_strategy.deviation_analysis.under_allocated.map((item, index) => (
            <div key={index} style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'center',
              padding: '0.75rem',
              marginBottom: '0.5rem',
              background: 'rgba(255, 71, 87, 0.1)',
              borderRadius: '6px',
              border: '1px solid rgba(255, 71, 87, 0.2)'
            }}>
              <div>
                <span style={{ color: '#00d4ff', fontWeight: '600' }}>{item.symbol}</span>
                <div style={{ fontSize: '0.875rem', color: '#b4bcc8' }}>
                  Current: {formatPercent(item.current || 0)} | Target: {formatPercent(item.target)}
                </div>
              </div>
              <div style={{ color: '#ff4757', fontWeight: '600', textAlign: 'right' }}>
                {formatPercent(item.deviation)}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Rebalancing Recommendations */}
      <div className="chart-container">
        <h3>Rebalancing Recommendations</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'JetBrains Mono, monospace' }}>
            <thead>
              <tr style={{ background: 'rgba(0, 212, 255, 0.1)' }}>
                <th style={{ padding: '0.75rem', textAlign: 'left', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Action</th>
                <th style={{ padding: '0.75rem', textAlign: 'left', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Symbol</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Current Weight</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Target Weight</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Amount</th>
              </tr>
            </thead>
            <tbody>
              {comparisonData.rebalancing_recommendations.map((rec, index) => (
                <tr key={index} style={{ borderBottom: '1px solid rgba(0, 212, 255, 0.1)' }}>
                  <td style={{ 
                    padding: '0.75rem', 
                    color: rec.action === 'BUY' ? '#00ff88' : '#ff4757',
                    fontWeight: '600' 
                  }}>
                    {rec.action}
                  </td>
                  <td style={{ padding: '0.75rem', color: '#00d4ff', fontWeight: '600' }}>{rec.symbol}</td>
                  <td style={{ padding: '0.75rem', textAlign: 'right', color: '#e0e0e0' }}>
                    {formatPercent(rec.current_weight)}
                  </td>
                  <td style={{ padding: '0.75rem', textAlign: 'right', color: '#e0e0e0' }}>
                    {formatPercent(rec.target_weight)}
                  </td>
                  <td style={{ 
                    padding: '0.75rem', 
                    textAlign: 'right', 
                    color: rec.amount > 0 ? '#00ff88' : '#ff4757',
                    fontWeight: '600' 
                  }}>
                    {formatCurrency(rec.amount)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Performance Comparison */}
      <div className="chart-container">
        <h3>Performance Metrics Comparison</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '2rem' }}>
          <div style={{ textAlign: 'center' }}>
            <h4 style={{ color: '#00d4ff', marginBottom: '1rem' }}>YTD Return</h4>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
              <span style={{ color: '#b4bcc8' }}>Current:</span>
              <span style={{ color: '#ffffff', fontWeight: '600' }}>
                {formatPercent(comparisonData.current_vs_strategy.performance_comparison.current_ytd)}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: '#b4bcc8' }}>Strategy:</span>
              <span style={{ color: '#00ff88', fontWeight: '600' }}>
                {formatPercent(comparisonData.current_vs_strategy.performance_comparison.strategy_ytd)}
              </span>
            </div>
          </div>

          <div style={{ textAlign: 'center' }}>
            <h4 style={{ color: '#00d4ff', marginBottom: '1rem' }}>Sharpe Ratio</h4>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
              <span style={{ color: '#b4bcc8' }}>Current:</span>
              <span style={{ color: '#ffffff', fontWeight: '600' }}>
                {comparisonData.current_vs_strategy.performance_comparison.current_sharpe.toFixed(2)}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: '#b4bcc8' }}>Strategy:</span>
              <span style={{ color: '#00ff88', fontWeight: '600' }}>
                {comparisonData.current_vs_strategy.performance_comparison.strategy_sharpe.toFixed(2)}
              </span>
            </div>
          </div>

          <div style={{ textAlign: 'center' }}>
            <h4 style={{ color: '#00d4ff', marginBottom: '1rem' }}>Volatility</h4>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
              <span style={{ color: '#b4bcc8' }}>Current:</span>
              <span style={{ color: '#ffffff', fontWeight: '600' }}>
                {formatPercent(comparisonData.current_vs_strategy.performance_comparison.current_volatility)}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: '#b4bcc8' }}>Strategy:</span>
              <span style={{ color: '#00ff88', fontWeight: '600' }}>
                {formatPercent(comparisonData.current_vs_strategy.performance_comparison.strategy_volatility)}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ComparisonDashboard;