# Design Requirements Document (DRD)
## Backtest Analytics Platform Architecture

**Document Version:** 1.0  
**Created:** August 2025  
**Technical Lead:** AI Trading System Team  

---

## 1. Architecture Overview

### 1.1 System Design Philosophy
Build a **microservices-based analytics platform** that separates concerns:
- **Data Layer**: High-performance time-series storage and retrieval
- **Analytics Engine**: Real-time computation of portfolio metrics
- **Visualization Layer**: Interactive web interface with drill-down capabilities
- **API Gateway**: Unified interface for all client interactions

### 1.2 Core Design Principles
- **Performance-First**: Sub-second response times for interactive analytics
- **Scalability**: Handle millions of data points and 100+ concurrent users
- **Modularity**: Pluggable components for different analysis types
- **Real-Time**: Live updates as backtests progress
- **Extensibility**: Easy addition of new metrics and visualizations

---

## 2. System Architecture

### 2.1 High-Level Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend (React + D3.js)                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ Portfolio   │ │ Model Perf  │ │ Forecast Visualization  │ │
│  │ Dashboard   │ │ Analytics   │ │ & Drill-Down           │ │
│  └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │ WebSocket + REST API
┌─────────────────────┴───────────────────────────────────────┐
│                   API Gateway (FastAPI)                     │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ Portfolio   │ │ Analytics   │ │ Real-Time Event         │ │
│  │ Service     │ │ Engine      │ │ Streaming               │ │
│  └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────────────┐
│                  Data Layer                                 │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ TimescaleDB │ │ Redis Cache │ │ Object Storage          │ │
│  │ (Time Series│ │ (Sessions + │ │ (Charts, Reports,       │ │
│  │  Analytics) │ │  Computed   │ │  Model Artifacts)       │ │
│  │             │ │  Metrics)   │ │                         │ │
│  └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow Architecture
```
Backtest Results → Data Ingestion → Time Series DB → Analytics Engine → API → Frontend
      ↓                ↓                 ↓              ↓           ↓        ↓
  [Pickle Files]  [ETL Pipeline]  [PostgreSQL +   [Real-time    [REST +  [Interactive
   [JSON Logs]    [Validation]     TimescaleDB]    Metrics]     WebSocket] Dashboards]
   [CSV Exports]  [Normalization]  [Indexing]      [Caching]    [Auth]    [Drill-Down]
```

---

## 3. Component Design

### 3.1 Data Architecture

#### 3.1.1 Time Series Schema Design
```sql
-- Portfolio performance time series
CREATE TABLE portfolio_performance (
    backtest_run_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_value DECIMAL(15,4) NOT NULL,
    daily_return DECIMAL(8,6),
    cumulative_return DECIMAL(8,6),
    drawdown DECIMAL(8,6),
    volatility_30d DECIMAL(8,6),
    sharpe_ratio_30d DECIMAL(8,4),
    positions_count INTEGER,
    cash_position DECIMAL(15,4),
    metadata JSONB
);

-- Individual position tracking
CREATE TABLE position_performance (
    backtest_run_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    position_size DECIMAL(15,4),
    market_value DECIMAL(15,4),
    unrealized_pnl DECIMAL(15,4),
    realized_pnl DECIMAL(15,4),
    entry_price DECIMAL(10,4),
    current_price DECIMAL(10,4),
    support_levels DECIMAL(10,4)[],
    resistance_levels DECIMAL(10,4)[],
    confidence_scores DECIMAL(4,3)[],
    model_version INTEGER
);

-- Trade execution tracking
CREATE TABLE trade_execution (
    trade_id UUID PRIMARY KEY,
    backtest_run_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    trade_type VARCHAR(10) NOT NULL, -- 'BUY', 'SELL'
    quantity DECIMAL(15,4) NOT NULL,
    price DECIMAL(10,4) NOT NULL,
    commission DECIMAL(8,4),
    signal_type VARCHAR(20), -- 'support_bounce', 'resistance_break'
    model_confidence DECIMAL(4,3),
    entry_rationale TEXT,
    exit_rationale TEXT
);

-- Model performance metrics
CREATE TABLE model_performance (
    backtest_run_id UUID NOT NULL,
    date DATE NOT NULL,
    model_version INTEGER,
    support_accuracy DECIMAL(5,4),
    resistance_accuracy DECIMAL(5,4),
    prediction_mae DECIMAL(8,6),
    confidence_correlation DECIMAL(5,4),
    predictions_count INTEGER,
    retraining_occurred BOOLEAN,
    processing_time_seconds DECIMAL(8,2)
);
```

#### 3.1.2 Indexing Strategy
```sql
-- Time-based partitioning for performance
SELECT create_hypertable('portfolio_performance', 'timestamp', chunk_time_interval => INTERVAL '1 week');
SELECT create_hypertable('position_performance', 'timestamp', chunk_time_interval => INTERVAL '1 week');
SELECT create_hypertable('trade_execution', 'timestamp', chunk_time_interval => INTERVAL '1 week');

-- Performance optimization indexes
CREATE INDEX idx_portfolio_run_time ON portfolio_performance (backtest_run_id, timestamp DESC);
CREATE INDEX idx_position_symbol_time ON position_performance (symbol, timestamp DESC);
CREATE INDEX idx_trade_symbol_type ON trade_execution (symbol, trade_type, timestamp DESC);
CREATE INDEX idx_model_perf_date ON model_performance (date DESC, model_version);
```

### 3.2 Analytics Engine Design

#### 3.2.1 Portfolio Analytics Service
```python
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, date

@dataclass
class PortfolioMetrics:
    """Comprehensive portfolio performance metrics"""
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    drawdown_duration_days: int
    win_rate: float
    profit_factor: float
    var_95: float
    expected_shortfall: float
    calmar_ratio: float
    sortino_ratio: float
    
    # Time-series data for charts
    equity_curve: pd.Series
    drawdown_series: pd.Series
    rolling_sharpe: pd.Series
    rolling_volatility: pd.Series

@dataclass 
class AttributionMetrics:
    """Performance attribution breakdown"""
    stock_attribution: Dict[str, float]  # symbol -> contribution
    sector_attribution: Dict[str, float]  # sector -> contribution
    signal_attribution: Dict[str, float]  # signal_type -> contribution
    daily_attribution: pd.Series  # daily contribution breakdown

@dataclass
class ModelPerformanceMetrics:
    """Model-specific performance tracking"""
    prediction_accuracy: pd.Series  # daily accuracy over time
    confidence_calibration: Dict[str, float]  # confidence bucket -> actual accuracy
    model_drift_score: float  # degradation indicator
    feature_importance: Dict[str, float]  # feature -> importance score
    regime_performance: Dict[str, PortfolioMetrics]  # market_regime -> metrics

class PortfolioAnalyticsEngine:
    """High-performance analytics engine for portfolio analysis"""
    
    def __init__(self, db_connection, cache_client):
        self.db = db_connection
        self.cache = cache_client
        
    async def compute_portfolio_metrics(
        self, 
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        benchmark_run_id: Optional[str] = None
    ) -> PortfolioMetrics:
        """Compute comprehensive portfolio metrics with caching"""
        
        cache_key = f"portfolio_metrics:{backtest_run_id}:{start_date}:{end_date}"
        cached = await self.cache.get(cache_key)
        if cached:
            return PortfolioMetrics.from_dict(cached)
        
        # Fetch portfolio performance data
        performance_data = await self._fetch_portfolio_data(
            backtest_run_id, start_date, end_date
        )
        
        # Compute metrics
        metrics = self._compute_metrics(performance_data)
        
        # Cache results for 5 minutes
        await self.cache.setex(cache_key, 300, metrics.to_dict())
        
        return metrics
    
    async def compute_attribution_analysis(
        self,
        backtest_run_id: str,
        attribution_type: str = "stock",  # "stock", "sector", "signal"
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> AttributionMetrics:
        """Compute performance attribution at various levels"""
        
        # Fetch trade and position data
        trades = await self._fetch_trade_data(backtest_run_id, start_date, end_date)
        positions = await self._fetch_position_data(backtest_run_id, start_date, end_date)
        
        # Compute attribution
        attribution = self._compute_attribution(trades, positions, attribution_type)
        
        return attribution
    
    async def compute_model_performance(
        self,
        backtest_run_id: str,
        model_comparison_ids: Optional[List[str]] = None
    ) -> ModelPerformanceMetrics:
        """Analyze model prediction performance over time"""
        
        # Fetch model performance data
        model_data = await self._fetch_model_data(backtest_run_id)
        
        # Compute model-specific metrics
        metrics = self._compute_model_metrics(model_data)
        
        # Add comparison if requested
        if model_comparison_ids:
            metrics.comparison_data = await self._compute_model_comparison(
                backtest_run_id, model_comparison_ids
            )
        
        return metrics
    
    def _compute_metrics(self, performance_data: pd.DataFrame) -> PortfolioMetrics:
        """Core portfolio metrics computation"""
        
        returns = performance_data['daily_return'].dropna()
        equity_curve = performance_data['portfolio_value']
        
        # Basic metrics
        total_return = (equity_curve.iloc[-1] / equity_curve.iloc[0]) - 1
        annualized_return = (1 + total_return) ** (252 / len(returns)) - 1
        volatility = returns.std() * np.sqrt(252)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Drawdown analysis
        running_max = equity_curve.expanding().max()
        drawdown_series = (equity_curve - running_max) / running_max
        max_drawdown = drawdown_series.min()
        
        # Risk metrics
        var_95 = np.percentile(returns, 5)
        expected_shortfall = returns[returns <= var_95].mean()
        
        # Rolling metrics for visualization
        rolling_sharpe = returns.rolling(30).mean() / returns.rolling(30).std() * np.sqrt(252)
        rolling_volatility = returns.rolling(30).std() * np.sqrt(252)
        
        return PortfolioMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            var_95=var_95,
            expected_shortfall=expected_shortfall,
            equity_curve=equity_curve,
            drawdown_series=drawdown_series,
            rolling_sharpe=rolling_sharpe,
            rolling_volatility=rolling_volatility
        )
```

### 3.3 Real-Time Analytics Pipeline

#### 3.3.1 Event Streaming Architecture
```python
import asyncio
from typing import AsyncGenerator
import websockets
import json

class RealTimeAnalyticsStreamer:
    """Stream real-time analytics updates to connected clients"""
    
    def __init__(self, analytics_engine, redis_client):
        self.analytics = analytics_engine
        self.redis = redis_client
        self.active_connections = set()
        
    async def handle_client_connection(self, websocket, path):
        """Handle new WebSocket client connections"""
        self.active_connections.add(websocket)
        try:
            async for message in websocket:
                await self.handle_client_message(websocket, message)
        finally:
            self.active_connections.remove(websocket)
    
    async def handle_client_message(self, websocket, message):
        """Process client subscription requests"""
        data = json.loads(message)
        
        if data['type'] == 'subscribe_portfolio':
            backtest_run_id = data['backtest_run_id']
            await self.subscribe_portfolio_updates(websocket, backtest_run_id)
        
        elif data['type'] == 'subscribe_model_performance':
            backtest_run_id = data['backtest_run_id']
            await self.subscribe_model_updates(websocket, backtest_run_id)
    
    async def subscribe_portfolio_updates(self, websocket, backtest_run_id):
        """Stream portfolio performance updates"""
        
        # Send initial data
        initial_metrics = await self.analytics.compute_portfolio_metrics(backtest_run_id)
        await websocket.send(json.dumps({
            'type': 'portfolio_update',
            'data': initial_metrics.to_dict(),
            'timestamp': datetime.now().isoformat()
        }))
        
        # Stream updates
        async for update in self.stream_portfolio_updates(backtest_run_id):
            await websocket.send(json.dumps(update))
    
    async def stream_portfolio_updates(self, backtest_run_id) -> AsyncGenerator:
        """Generate real-time portfolio updates"""
        
        # Listen to Redis pub/sub for backtest updates
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(f"backtest_updates:{backtest_run_id}")
        
        async for message in pubsub.listen():
            if message['type'] == 'message':
                # Recompute metrics with latest data
                metrics = await self.analytics.compute_portfolio_metrics(backtest_run_id)
                
                yield {
                    'type': 'portfolio_update', 
                    'data': metrics.to_dict(),
                    'timestamp': datetime.now().isoformat()
                }
    
    async def broadcast_update(self, update_data):
        """Broadcast updates to all connected clients"""
        if self.active_connections:
            await asyncio.gather(
                *[ws.send(json.dumps(update_data)) for ws in self.active_connections],
                return_exceptions=True
            )
```

### 3.4 Frontend Architecture

#### 3.4.1 React Component Structure
```javascript
// Component hierarchy for portfolio analytics
src/
├── components/
│   ├── Dashboard/
│   │   ├── PortfolioDashboard.jsx       // Main dashboard container
│   │   ├── MetricsSummary.jsx           // Key performance indicators
│   │   └── FilterPanel.jsx              // Time range and strategy filters
│   ├── Charts/
│   │   ├── PortfolioPerformanceChart.jsx // Main equity curve
│   │   ├── DrawdownChart.jsx            // Underwater chart
│   │   ├── RollingMetricsChart.jsx      // Rolling Sharpe, volatility
│   │   ├── AttributionChart.jsx         // Performance attribution
│   │   └── ForecastVisualization.jsx    // Support/resistance forecasts
│   ├── DrillDown/
│   │   ├── DrillDownPanel.jsx           // Context-sensitive details
│   │   ├── StockDetailView.jsx          // Individual stock analysis
│   │   ├── TradeDetailView.jsx          // Trade-level analysis
│   │   └── ModelDetailView.jsx          // Model performance details
│   └── Comparison/
│       ├── StrategyComparison.jsx       // Side-by-side comparison
│       ├── ModelComparison.jsx          // Model performance comparison
│       └── BenchmarkComparison.jsx      // Benchmark analysis
├── hooks/
│   ├── usePortfolioData.js              // Custom hook for portfolio data
│   ├── useRealTimeUpdates.js            // WebSocket integration
│   ├── useDrillDown.js                  // Drill-down state management
│   └── useChartInteractions.js          // Chart interaction handling
├── services/
│   ├── apiClient.js                     // API communication
│   ├── websocketClient.js               // Real-time data streaming
│   └── chartUtils.js                    // Chart configuration utilities
└── utils/
    ├── metricsCalculator.js             // Client-side metric calculations
    ├── formatters.js                    // Data formatting utilities
    └── dateUtils.js                     // Date/time utilities
```

#### 3.4.2 Interactive Chart Design
```javascript
import React, { useState, useEffect, useCallback } from 'react';
import * as d3 from 'd3';
import { usePortfolioData, useRealTimeUpdates } from '../hooks';

const PortfolioPerformanceChart = ({ 
  backtest_run_id, 
  timeRange, 
  onDrillDown,
  enableRealTime = false 
}) => {
  const [selectedPeriod, setSelectedPeriod] = useState(null);
  const [zoomLevel, setZoomLevel] = useState(1);
  
  // Fetch portfolio data
  const { data: portfolioData, loading, error } = usePortfolioData(
    backtest_run_id, 
    timeRange
  );
  
  // Real-time updates
  const realTimeData = useRealTimeUpdates(
    backtest_run_id, 
    enableRealTime
  );
  
  // Combine historical and real-time data
  const chartData = useMemo(() => {
    if (!portfolioData) return null;
    
    let combined = [...portfolioData.equity_curve];
    if (realTimeData?.latest_performance) {
      combined = [...combined, realTimeData.latest_performance];
    }
    
    return combined;
  }, [portfolioData, realTimeData]);
  
  // Handle chart interactions
  const handleBrushSelection = useCallback((selection) => {
    if (selection) {
      const [start, end] = selection.map(d => new Date(d));
      setSelectedPeriod({ start, end });
      
      // Trigger drill-down analysis
      onDrillDown({
        type: 'time_period',
        period: { start, end },
        data: chartData.filter(d => d.date >= start && d.date <= end)
      });
    }
  }, [chartData, onDrillDown]);
  
  // D3 chart rendering
  useEffect(() => {
    if (!chartData || loading) return;
    
    const svg = d3.select('#portfolio-chart');
    svg.selectAll('*').remove(); // Clear previous chart
    
    const margin = { top: 20, right: 30, bottom: 40, left: 50 };
    const width = 800 - margin.left - margin.right;
    const height = 400 - margin.top - margin.bottom;
    
    // Scales
    const xScale = d3.scaleTime()
      .domain(d3.extent(chartData, d => new Date(d.date)))
      .range([0, width]);
    
    const yScale = d3.scaleLinear()
      .domain(d3.extent(chartData, d => d.portfolio_value))
      .range([height, 0]);
    
    // Line generator
    const line = d3.line()
      .x(d => xScale(new Date(d.date)))
      .y(d => yScale(d.portfolio_value))
      .curve(d3.curveMonotoneX);
    
    // Chart container
    const chartArea = svg
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);
    
    // Add axes
    chartArea.append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(xScale));
    
    chartArea.append('g')
      .call(d3.axisLeft(yScale));
    
    // Add portfolio line
    chartArea.append('path')
      .datum(chartData)
      .attr('fill', 'none')
      .attr('stroke', '#1f77b4')
      .attr('stroke-width', 2)
      .attr('d', line);
    
    // Add brush for selection
    const brush = d3.brushX()
      .extent([[0, 0], [width, height]])
      .on('end', (event) => {
        if (event.selection) {
          const [x0, x1] = event.selection;
          const selection = [xScale.invert(x0), xScale.invert(x1)];
          handleBrushSelection(selection);
        }
      });
    
    chartArea.append('g').call(brush);
    
    // Add tooltips
    const tooltip = d3.select('body').append('div')
      .attr('class', 'tooltip')
      .style('opacity', 0);
    
    chartArea.selectAll('.dot')
      .data(chartData.filter((_, i) => i % 5 === 0)) // Sample points for performance
      .enter().append('circle')
      .attr('class', 'dot')
      .attr('cx', d => xScale(new Date(d.date)))
      .attr('cy', d => yScale(d.portfolio_value))
      .attr('r', 3)
      .style('opacity', 0)
      .on('mouseover', (event, d) => {
        tooltip.transition().duration(200).style('opacity', 0.9);
        tooltip.html(`
          <strong>Date:</strong> ${d.date}<br/>
          <strong>Value:</strong> $${d.portfolio_value.toLocaleString()}<br/>
          <strong>Return:</strong> ${(d.daily_return * 100).toFixed(2)}%
        `)
        .style('left', (event.pageX + 10) + 'px')
        .style('top', (event.pageY - 28) + 'px');
      })
      .on('mouseout', () => {
        tooltip.transition().duration(500).style('opacity', 0);
      });
    
  }, [chartData, loading, handleBrushSelection]);
  
  if (loading) return <div>Loading chart...</div>;
  if (error) return <div>Error loading data: {error.message}</div>;
  
  return (
    <div className="portfolio-chart-container">
      <div className="chart-header">
        <h3>Portfolio Performance</h3>
        {realTimeData?.isConnected && (
          <span className="real-time-indicator">🟢 Live</span>
        )}
      </div>
      <svg id="portfolio-chart" width={800} height={400}></svg>
      {selectedPeriod && (
        <div className="selection-info">
          Selected: {selectedPeriod.start.toLocaleDateString()} - {selectedPeriod.end.toLocaleDateString()}
        </div>
      )}
    </div>
  );
};
```

---

## 4. API Design

### 4.1 REST API Endpoints

```python
from fastapi import FastAPI, Depends, Query, Path
from typing import List, Optional
from datetime import date, datetime

app = FastAPI(title="Backtest Analytics API")

# Portfolio Analytics Endpoints
@app.get("/api/v1/backtests")
async def list_backtests(
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0),
    strategy_type: Optional[str] = None,
    start_date: Optional[date] = None
) -> List[BacktestSummary]:
    """List available backtest runs with filtering"""
    pass

@app.get("/api/v1/backtests/{backtest_run_id}/portfolio/metrics")
async def get_portfolio_metrics(
    backtest_run_id: str = Path(...),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    benchmark_id: Optional[str] = None
) -> PortfolioMetrics:
    """Get comprehensive portfolio performance metrics"""
    pass

@app.get("/api/v1/backtests/{backtest_run_id}/portfolio/performance")
async def get_portfolio_performance(
    backtest_run_id: str = Path(...),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    granularity: str = Query("daily", regex="^(daily|hourly|minute)$")
) -> List[PerformanceDataPoint]:
    """Get time-series portfolio performance data"""
    pass

@app.get("/api/v1/backtests/{backtest_run_id}/attribution")
async def get_attribution_analysis(
    backtest_run_id: str = Path(...),
    attribution_type: str = Query("stock", regex="^(stock|sector|signal)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> AttributionMetrics:
    """Get performance attribution breakdown"""
    pass

# Model Performance Endpoints
@app.get("/api/v1/backtests/{backtest_run_id}/model/performance")
async def get_model_performance(
    backtest_run_id: str = Path(...),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> ModelPerformanceMetrics:
    """Get model prediction accuracy and performance metrics"""
    pass

@app.get("/api/v1/backtests/{backtest_run_id}/forecasts")
async def get_forecasts(
    backtest_run_id: str = Path(...),
    symbol: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[ForecastDataPoint]:
    """Get support/resistance forecasts with confidence levels"""
    pass

# Comparison Endpoints
@app.post("/api/v1/comparison/portfolio")
async def compare_portfolios(
    comparison_request: PortfolioComparisonRequest
) -> PortfolioComparisonResult:
    """Compare performance between multiple backtest runs"""
    pass

@app.post("/api/v1/comparison/models")
async def compare_models(
    comparison_request: ModelComparisonRequest
) -> ModelComparisonResult:
    """Compare model performance between different strategies"""
    pass

# Drill-Down Endpoints
@app.get("/api/v1/backtests/{backtest_run_id}/drill-down/period")
async def drill_down_period(
    backtest_run_id: str = Path(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    analysis_type: str = Query("detailed", regex="^(detailed|trades|positions|model)$")
) -> DrillDownAnalysis:
    """Get detailed analysis for specific time period"""
    pass

@app.get("/api/v1/backtests/{backtest_run_id}/drill-down/stock/{symbol}")
async def drill_down_stock(
    backtest_run_id: str = Path(...),
    symbol: str = Path(...),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> StockDrillDownAnalysis:
    """Get detailed analysis for specific stock"""
    pass

@app.get("/api/v1/backtests/{backtest_run_id}/drill-down/trade/{trade_id}")
async def drill_down_trade(
    backtest_run_id: str = Path(...),
    trade_id: str = Path(...)
) -> TradeDetailAnalysis:
    """Get detailed analysis for specific trade"""
    pass

# Real-Time WebSocket Endpoints
@app.websocket("/ws/backtests/{backtest_run_id}/portfolio")
async def portfolio_websocket(websocket: WebSocket, backtest_run_id: str):
    """Real-time portfolio performance updates"""
    pass

@app.websocket("/ws/backtests/{backtest_run_id}/model")
async def model_websocket(websocket: WebSocket, backtest_run_id: str):
    """Real-time model performance updates"""
    pass
```

---

## 5. Performance Optimization

### 5.1 Database Optimization
- **Partitioning**: Time-based partitioning for large datasets
- **Indexing**: Composite indexes on frequently queried columns
- **Materialized Views**: Pre-computed aggregations for common queries
- **Connection Pooling**: Efficient database connection management

### 5.2 Caching Strategy
- **Redis Layers**: L1 (API responses), L2 (computed metrics), L3 (raw data)
- **Cache Keys**: Hierarchical structure for efficient invalidation
- **TTL Strategy**: Short TTL for live data, longer for historical analysis
- **Cache Warming**: Pre-compute popular metrics

### 5.3 Frontend Optimization
- **Data Virtualization**: Render only visible chart data points
- **Progressive Loading**: Load overview first, details on demand
- **WebWorkers**: Offload heavy calculations to background threads
- **Chart Optimization**: Canvas rendering for large datasets

---

## 6. Monitoring & Observability

### 6.1 Application Metrics
- **API Performance**: Response times, error rates, throughput
- **Database Performance**: Query execution times, connection pool usage
- **Cache Performance**: Hit rates, memory usage, eviction rates
- **WebSocket Connections**: Active connections, message throughput

### 6.2 Business Metrics
- **User Engagement**: Dashboard usage, drill-down interactions
- **Analysis Efficiency**: Time from query to insight
- **Model Performance**: Prediction accuracy trends over time
- **System Adoption**: Feature usage statistics

### 6.3 Alerting Strategy
- **Performance Degradation**: Response time > 3 seconds
- **Data Quality Issues**: Missing or inconsistent backtest data
- **Model Drift**: Significant accuracy degradation
- **System Health**: High error rates or connection failures

---

*This DRD provides the technical foundation for building a high-performance, scalable backtest analytics platform that enables deep portfolio analysis and model performance evaluation.*