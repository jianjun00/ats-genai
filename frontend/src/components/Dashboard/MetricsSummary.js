import React from 'react';
import { Row, Col, Card, Statistic, Progress, Skeleton } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import numeral from 'numeral';

const MetricsSummary = ({ data, realTimeData, loading, onDrillDown }) => {
  if (loading) {
    return (
      <div className="metrics-grid">
        {[...Array(6)].map((_, index) => (
          <Card key={index} className="metric-card">
            <Skeleton active paragraph={{ rows: 2 }} />
          </Card>
        ))}
      </div>
    );
  }
  
  if (!data) {
    return null;
  }
  
  // Merge real-time data if available
  const currentData = realTimeData?.latestMetrics || data;
  
  // Helper function to format percentage
  const formatPercent = (value) => numeral(value).format('0.00%');
  
  // Helper function to format currency
  const formatCurrency = (value) => numeral(value).format('$0,0.00');
  
  // Helper function to format ratio
  const formatRatio = (value) => numeral(value).format('0.00');
  
  // Helper function to determine trend
  const getTrend = (current, previous) => {
    if (!previous) return null;
    return current > previous ? 'up' : current < previous ? 'down' : 'neutral';
  };
  
  // Helper function to get trend color
  const getTrendColor = (trend, isPositiveGood = true) => {
    if (trend === 'up') return isPositiveGood ? '#2ca02c' : '#d62728';
    if (trend === 'down') return isPositiveGood ? '#d62728' : '#2ca02c';
    return '#8c8c8c';
  };
  
  // Helper function to get trend icon
  const getTrendIcon = (trend) => {
    if (trend === 'up') return <ArrowUpOutlined />;
    if (trend === 'down') return <ArrowDownOutlined />;
    return null;
  };
  
  const metrics = [
    {
      title: 'Total Return',
      value: formatPercent(currentData.total_return),
      precision: 2,
      trend: getTrend(currentData.total_return, data.total_return),
      isPositiveGood: true,
      onClick: () => onDrillDown({
        type: 'metric',
        metric: 'total_return',
        value: currentData.total_return
      })
    },
    {
      title: 'Annualized Return',
      value: formatPercent(currentData.annualized_return),
      precision: 2,
      trend: getTrend(currentData.annualized_return, data.annualized_return),
      isPositiveGood: true,
      onClick: () => onDrillDown({
        type: 'metric',
        metric: 'annualized_return',
        value: currentData.annualized_return
      })
    },
    {
      title: 'Sharpe Ratio',
      value: formatRatio(currentData.sharpe_ratio),
      precision: 2,
      trend: getTrend(currentData.sharpe_ratio, data.sharpe_ratio),
      isPositiveGood: true,
      onClick: () => onDrillDown({
        type: 'metric',
        metric: 'sharpe_ratio',
        value: currentData.sharpe_ratio
      })
    },
    {
      title: 'Max Drawdown',
      value: formatPercent(currentData.max_drawdown),
      precision: 2,
      trend: getTrend(currentData.max_drawdown, data.max_drawdown),
      isPositiveGood: false,
      onClick: () => onDrillDown({
        type: 'metric',
        metric: 'max_drawdown',
        value: currentData.max_drawdown
      })
    },
    {
      title: 'Volatility',
      value: formatPercent(currentData.volatility),
      precision: 2,
      trend: getTrend(currentData.volatility, data.volatility),
      isPositiveGood: false,
      onClick: () => onDrillDown({
        type: 'metric',
        metric: 'volatility',
        value: currentData.volatility
      })
    },
    {
      title: 'Win Rate',
      value: formatPercent(currentData.win_rate),
      precision: 1,
      trend: getTrend(currentData.win_rate, data.win_rate),
      isPositiveGood: true,
      showProgress: true,
      progressValue: currentData.win_rate * 100,
      onClick: () => onDrillDown({
        type: 'metric',
        metric: 'win_rate',
        value: currentData.win_rate
      })
    }
  ];
  
  // Additional risk metrics
  const riskMetrics = [
    {
      title: 'Value at Risk (95%)',
      value: formatPercent(currentData.var_95),
      isPositiveGood: false
    },
    {
      title: 'Expected Shortfall',
      value: formatPercent(currentData.expected_shortfall_95),
      isPositiveGood: false
    },
    {
      title: 'Sortino Ratio',
      value: formatRatio(currentData.sortino_ratio),
      isPositiveGood: true
    },
    {
      title: 'Calmar Ratio',
      value: formatRatio(currentData.calmar_ratio),
      isPositiveGood: true
    }
  ];
  
  return (
    <div className="metrics-summary-container">
      {/* Primary Metrics */}
      <div className="metrics-grid">
        {metrics.map((metric, index) => (
          <Card
            key={index}
            className="metric-card"
            hoverable
            onClick={metric.onClick}
            style={{ cursor: 'pointer' }}
          >
            <Statistic
              title={metric.title}
              value={metric.value}
              precision={metric.precision}
              valueStyle={{
                color: metric.trend ? getTrendColor(metric.trend, metric.isPositiveGood) : '#262626',
                fontSize: '24px',
                fontWeight: 700
              }}
              prefix={metric.trend && getTrendIcon(metric.trend)}
            />
            
            {metric.showProgress && (
              <Progress
                percent={metric.progressValue}
                showInfo={false}
                strokeColor={metric.progressValue > 50 ? '#2ca02c' : '#ff7f0e'}
                size="small"
                style={{ marginTop: 8 }}
              />
            )}
            
            {realTimeData?.isConnected && (
              <div className="metric-update-indicator">
                <span style={{ fontSize: '10px', color: '#2ca02c' }}>
                  ● Live
                </span>
              </div>
            )}
          </Card>
        ))}
      </div>
      
      {/* Secondary Risk Metrics */}
      <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
        {riskMetrics.map((metric, index) => (
          <Col xs={12} sm={6} key={index}>
            <Card size="small" className="risk-metric-card">
              <Statistic
                title={metric.title}
                value={metric.value}
                valueStyle={{
                  fontSize: '16px',
                  fontWeight: 600,
                  color: metric.isPositiveGood ? '#2ca02c' : '#d62728'
                }}
              />
            </Card>
          </Col>
        ))}
      </Row>
      
      {/* Performance Summary */}
      <Card
        title="Performance Summary"
        size="small"
        style={{ marginTop: 16 }}
        extra={
          realTimeData?.lastUpdate && (
            <span style={{ fontSize: '12px', color: '#8c8c8c' }}>
              Last updated: {new Date(realTimeData.lastUpdate).toLocaleTimeString()}
            </span>
          )
        }
      >
        <Row gutter={[16, 8]}>
          <Col span={8}>
            <div className="summary-item">
              <div className="summary-label">Total Trades</div>
              <div className="summary-value">{numeral(currentData.total_trades).format('0,0')}</div>
            </div>
          </Col>
          <Col span={8}>
            <div className="summary-item">
              <div className="summary-label">Average Win</div>
              <div className="summary-value">{formatPercent(currentData.avg_win)}</div>
            </div>
          </Col>
          <Col span={8}>
            <div className="summary-item">
              <div className="summary-label">Average Loss</div>
              <div className="summary-value">{formatPercent(currentData.avg_loss)}</div>
            </div>
          </Col>
          <Col span={8}>
            <div className="summary-item">
              <div className="summary-label">Profit Factor</div>
              <div className="summary-value">{formatRatio(currentData.profit_factor)}</div>
            </div>
          </Col>
          <Col span={8}>
            <div className="summary-item">
              <div className="summary-label">Max DD Duration</div>
              <div className="summary-value">{currentData.max_drawdown_duration_days} days</div>
            </div>
          </Col>
          <Col span={8}>
            <div className="summary-item">
              <div className="summary-label">Total Days</div>
              <div className="summary-value">{currentData.total_days} days</div>
            </div>
          </Col>
        </Row>
      </Card>
    </div>
  );
};

export default MetricsSummary;