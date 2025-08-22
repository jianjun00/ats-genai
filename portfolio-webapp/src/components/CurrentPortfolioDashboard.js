import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  PieChart, Pie, Cell, ResponsiveContainer, LineChart, Line,
  ComposedChart, Area, ReferenceLine
} from 'recharts';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D', '#FFC658', '#FF7C7C', '#8DD1E1'];

const CurrentPortfolioDashboard = () => {
  const [portfolioData, setPortfolioData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(new Date());

  // Mock current portfolio data (replace with real API call)
  const mockPortfolioData = {
    total_portfolio_value: 1250000.0,
    daily_return: 0.0125,
    total_return: 0.25,
    cash_position: 125000.0,
    invested_amount: 1125000.0,
    holdings: [
      {
        symbol: "AAPL",
        shares: 1500,
        price: 175.50,
        market_value: 263250.0,
        weight: 0.2106,
        daily_pnl: 3150.0,
        daily_return: 0.012,
        cost_basis: 150.00,
        unrealized_pnl: 38250.0,
        sector: "Technology"
      },
      {
        symbol: "MSFT",
        shares: 800,
        price: 310.25,
        market_value: 248200.0,
        weight: 0.1986,
        daily_pnl: -1240.0,
        daily_return: -0.005,
        cost_basis: 280.00,
        unrealized_pnl: 24200.0,
        sector: "Technology"
      },
      {
        symbol: "GOOGL",
        shares: 600,
        price: 140.80,
        market_value: 84480.0,
        weight: 0.0676,
        daily_pnl: 845.0,
        daily_return: 0.010,
        cost_basis: 120.00,
        unrealized_pnl: 12480.0,
        sector: "Technology"
      },
      {
        symbol: "NVDA",
        shares: 400,
        price: 520.75,
        market_value: 208300.0,
        weight: 0.1666,
        daily_pnl: 4165.0,
        daily_return: 0.020,
        cost_basis: 450.00,
        unrealized_pnl: 28300.0,
        sector: "Technology"
      },
      {
        symbol: "JPM",
        shares: 900,
        price: 155.60,
        market_value: 140040.0,
        weight: 0.1120,
        daily_pnl: -700.0,
        daily_return: -0.005,
        cost_basis: 140.00,
        unrealized_pnl: 14040.0,
        sector: "Financials"
      },
      {
        symbol: "JNJ",
        shares: 1200,
        price: 158.45,
        market_value: 190140.0,
        weight: 0.1521,
        daily_pnl: 950.0,
        daily_return: 0.005,
        cost_basis: 150.00,
        unrealized_pnl: 10140.0,
        sector: "Healthcare"
      }
    ],
    sector_allocation: {
      "Technology": 0.6434,
      "Financials": 0.1120,
      "Healthcare": 0.1521,
      "Cash": 0.0925
    },
    performance_metrics: {
      volatility: 0.18,
      sharpe_ratio: 1.35,
      max_drawdown: -0.08,
      beta: 1.05,
      var_95: -15000.0,
      win_rate: 0.62
    }
  };

  useEffect(() => {
    const fetchPortfolioData = async () => {
      try {
        setLoading(true);
        
        // Replace with actual API call
        // const response = await fetch('http://localhost:8001/api/v1/portfolio/current');
        // const data = await response.json();
        
        // Simulate API delay
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        setPortfolioData(mockPortfolioData);
        setLastUpdate(new Date());
        setError(null);
      } catch (err) {
        console.error('Error fetching portfolio data:', err);
        setError('Failed to load portfolio data');
      } finally {
        setLoading(false);
      }
    };

    fetchPortfolioData();
    
    // Set up refresh interval (every 30 seconds)
    const interval = setInterval(fetchPortfolioData, 30000);
    
    return () => clearInterval(interval);
  }, []);

  const formatCurrency = (value) => {
    if (value >= 1000000) {
      return `$${(value / 1000000).toFixed(2)}M`;
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
      <div className="loading">
        <div className="loading-spinner"></div>
        Loading current portfolio...
      </div>
    );
  }

  if (error) {
    return <div className="error">{error}</div>;
  }

  if (!portfolioData) {
    return <div className="error">No portfolio data available</div>;
  }

  // Prepare data for charts
  const holdingsForChart = portfolioData.holdings.map(holding => ({
    symbol: holding.symbol,
    weight: holding.weight * 100,
    market_value: holding.market_value,
    daily_return: holding.daily_return * 100,
    daily_pnl: holding.daily_pnl,
    unrealized_pnl: holding.unrealized_pnl
  }));

  const sectorData = Object.entries(portfolioData.sector_allocation).map(([sector, weight]) => ({
    sector,
    weight: weight * 100,
    value: weight
  }));

  const topGainers = portfolioData.holdings
    .filter(h => h.daily_pnl > 0)
    .sort((a, b) => b.daily_pnl - a.daily_pnl)
    .slice(0, 3);

  const topLosers = portfolioData.holdings
    .filter(h => h.daily_pnl < 0)
    .sort((a, b) => a.daily_pnl - b.daily_pnl)
    .slice(0, 3);

  return (
    <div className="current-portfolio-dashboard">
      {/* Real-time Status */}
      <div className="real-time-header">
        <div className="real-time-indicator">
          Live Portfolio
        </div>
        <div className="last-update">
          Last updated: {lastUpdate.toLocaleTimeString()}
        </div>
      </div>

      {/* Summary Cards */}
      <div className="summary-cards">
        <div className="summary-card highlight">
          <div className="card-label">Total Portfolio Value</div>
          <div className="card-value primary">{formatCurrency(portfolioData.total_portfolio_value)}</div>
          <div className={`card-change ${portfolioData.daily_return >= 0 ? 'positive' : 'negative'}`}>
            {portfolioData.daily_return >= 0 ? '+' : ''}{formatPercent(portfolioData.daily_return)} today
          </div>
        </div>

        <div className="summary-card">
          <div className="card-label">Total Return</div>
          <div className={`card-value ${portfolioData.total_return >= 0 ? 'positive' : 'negative'}`}>
            {portfolioData.total_return >= 0 ? '+' : ''}{formatPercent(portfolioData.total_return)}
          </div>
          <div className="card-sublabel">Since inception</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Daily P&L</div>
          <div className={`card-value ${portfolioData.holdings.reduce((sum, h) => sum + h.daily_pnl, 0) >= 0 ? 'positive' : 'negative'}`}>
            {formatCurrency(portfolioData.holdings.reduce((sum, h) => sum + h.daily_pnl, 0))}
          </div>
          <div className="card-sublabel">Unrealized</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Cash Position</div>
          <div className="card-value">{formatCurrency(portfolioData.cash_position)}</div>
          <div className="card-sublabel">{formatPercent(portfolioData.cash_position / portfolioData.total_portfolio_value)} of total</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Sharpe Ratio</div>
          <div className="card-value">{portfolioData.performance_metrics.sharpe_ratio.toFixed(2)}</div>
          <div className="card-sublabel">Risk-adjusted return</div>
        </div>

        <div className="summary-card">
          <div className="card-label">Max Drawdown</div>
          <div className="card-value negative">{formatPercent(portfolioData.performance_metrics.max_drawdown)}</div>
          <div className="card-sublabel">Peak to trough</div>
        </div>
      </div>

      {/* Main Charts Row */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '2rem', marginBottom: '2rem' }}>
        {/* Holdings Breakdown Chart */}
        <div className="chart-container">
          <h3>Current Holdings</h3>
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
                tickFormatter={(value) => `${value.toFixed(0)}%`}
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
                  if (name === 'Portfolio Weight') return [`${value.toFixed(1)}%`, name];
                  if (name === 'Daily Return') return [`${value.toFixed(2)}%`, name];
                  if (name === 'Market Value') return [formatCurrency(value), name];
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
                formatter={(value) => [`${(value * 100).toFixed(1)}%`, 'Allocation']}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Performance Attribution */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem', marginBottom: '2rem' }}>
        <div className="chart-container">
          <h3>Today's Top Performers</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            <h4 style={{ color: '#00ff88', margin: '0 0 1rem 0' }}>Top Gainers</h4>
            {topGainers.map((stock, index) => (
              <div key={index} style={{ 
                display: 'flex', 
                justifyContent: 'space-between', 
                alignItems: 'center',
                padding: '0.75rem',
                background: 'rgba(0, 255, 136, 0.1)',
                borderRadius: '6px',
                border: '1px solid rgba(0, 255, 136, 0.2)'
              }}>
                <span style={{ color: '#00d4ff', fontWeight: '600' }}>{stock.symbol}</span>
                <div style={{ textAlign: 'right' }}>
                  <div style={{ color: '#00ff88', fontWeight: '600' }}>
                    +{formatCurrency(stock.daily_pnl)}
                  </div>
                  <div style={{ fontSize: '0.875rem', color: '#b4bcc8' }}>
                    ({formatPercent(stock.daily_return)})
                  </div>
                </div>
              </div>
            ))}
            
            {topLosers.length > 0 && (
              <>
                <h4 style={{ color: '#ff4757', margin: '1rem 0 1rem 0' }}>Top Losers</h4>
                {topLosers.map((stock, index) => (
                  <div key={index} style={{ 
                    display: 'flex', 
                    justifyContent: 'space-between', 
                    alignItems: 'center',
                    padding: '0.75rem',
                    background: 'rgba(255, 71, 87, 0.1)',
                    borderRadius: '6px',
                    border: '1px solid rgba(255, 71, 87, 0.2)'
                  }}>
                    <span style={{ color: '#00d4ff', fontWeight: '600' }}>{stock.symbol}</span>
                    <div style={{ textAlign: 'right' }}>
                      <div style={{ color: '#ff4757', fontWeight: '600' }}>
                        {formatCurrency(stock.daily_pnl)}
                      </div>
                      <div style={{ fontSize: '0.875rem', color: '#b4bcc8' }}>
                        ({formatPercent(stock.daily_return)})
                      </div>
                    </div>
                  </div>
                ))}
              </>
            )}
          </div>
        </div>

        <div className="chart-container">
          <h3>Risk Metrics</h3>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            <div style={{ textAlign: 'center', padding: '1rem', background: 'rgba(0, 212, 255, 0.1)', borderRadius: '8px' }}>
              <div style={{ fontSize: '0.875rem', color: '#b4bcc8', marginBottom: '0.5rem' }}>VOLATILITY</div>
              <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#ffffff' }}>
                {formatPercent(portfolioData.performance_metrics.volatility)}
              </div>
            </div>
            
            <div style={{ textAlign: 'center', padding: '1rem', background: 'rgba(0, 212, 255, 0.1)', borderRadius: '8px' }}>
              <div style={{ fontSize: '0.875rem', color: '#b4bcc8', marginBottom: '0.5rem' }}>BETA</div>
              <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#ffffff' }}>
                {portfolioData.performance_metrics.beta.toFixed(2)}
              </div>
            </div>
            
            <div style={{ textAlign: 'center', padding: '1rem', background: 'rgba(255, 165, 2, 0.1)', borderRadius: '8px' }}>
              <div style={{ fontSize: '0.875rem', color: '#b4bcc8', marginBottom: '0.5rem' }}>VAR (95%)</div>
              <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#ffa502' }}>
                {formatCurrency(portfolioData.performance_metrics.var_95)}
              </div>
            </div>
            
            <div style={{ textAlign: 'center', padding: '1rem', background: 'rgba(0, 255, 136, 0.1)', borderRadius: '8px' }}>
              <div style={{ fontSize: '0.875rem', color: '#b4bcc8', marginBottom: '0.5rem' }}>WIN RATE</div>
              <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#00ff88' }}>
                {formatPercent(portfolioData.performance_metrics.win_rate)}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Detailed Holdings Table */}
      <div className="chart-container">
        <h3>Portfolio Holdings Detail</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'JetBrains Mono, monospace' }}>
            <thead>
              <tr style={{ background: 'rgba(0, 212, 255, 0.1)' }}>
                <th style={{ padding: '0.75rem', textAlign: 'left', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Symbol</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Shares</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Price</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Market Value</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Weight</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Daily P&L</th>
                <th style={{ padding: '0.75rem', textAlign: 'right', color: '#00d4ff', borderBottom: '2px solid rgba(0, 212, 255, 0.3)' }}>Unrealized P&L</th>
              </tr>
            </thead>
            <tbody>
              {portfolioData.holdings.map((holding, index) => (
                <tr key={index} style={{ borderBottom: '1px solid rgba(0, 212, 255, 0.1)' }}>
                  <td style={{ padding: '0.75rem', color: '#00d4ff', fontWeight: '600' }}>{holding.symbol}</td>
                  <td style={{ padding: '0.75rem', textAlign: 'right', color: '#e0e0e0' }}>{holding.shares.toFixed(0)}</td>
                  <td style={{ padding: '0.75rem', textAlign: 'right', color: '#e0e0e0' }}>${holding.price.toFixed(2)}</td>
                  <td style={{ padding: '0.75rem', textAlign: 'right', color: '#e0e0e0' }}>{formatCurrency(holding.market_value)}</td>
                  <td style={{ padding: '0.75rem', textAlign: 'right', color: '#e0e0e0' }}>{formatPercent(holding.weight)}</td>
                  <td style={{ 
                    padding: '0.75rem', 
                    textAlign: 'right', 
                    color: holding.daily_pnl >= 0 ? '#00ff88' : '#ff4757',
                    fontWeight: '600' 
                  }}>
                    {holding.daily_pnl >= 0 ? '+' : ''}{formatCurrency(holding.daily_pnl)}
                  </td>
                  <td style={{ 
                    padding: '0.75rem', 
                    textAlign: 'right', 
                    color: holding.unrealized_pnl >= 0 ? '#00ff88' : '#ff4757',
                    fontWeight: '600' 
                  }}>
                    {holding.unrealized_pnl >= 0 ? '+' : ''}{formatCurrency(holding.unrealized_pnl)}
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

export default CurrentPortfolioDashboard;