# Portfolio Platform Architecture Plan

## Overview
Based on user requirements for portfolio breakdown visibility and backtest analytics, we recommend a **dual webapp architecture** with integrated comparison capabilities.

## Architecture Decision: Two Specialized Webapps

### 1. Current Portfolio Webapp (`portfolio-webapp/`)
**Primary Purpose**: Real-time portfolio management and monitoring
**Port**: 3001

**Features:**
- ✅ Live portfolio positions and holdings
- ✅ Real-time P&L and performance metrics  
- ✅ Current portfolio breakdown (daily holdings, sector allocation)
- ✅ Live market data integration (prices, volume)
- ✅ Risk metrics and alerts
- ✅ Order management interface
- ✅ Real-time comparison with benchmark indices

**Technology Stack:**
- React frontend with WebSocket connections
- FastAPI backend with real-time data feeds
- Redis for caching live market data
- WebSocket for real-time updates

### 2. Backtest Analytics Webapp (`analytics-webapp/`)
**Primary Purpose**: Historical analysis and strategy research
**Port**: 3000 (current implementation)

**Features:**
- ✅ Backtest performance analysis (already implemented)
- ✅ Historical portfolio breakdown (already implemented)
- ✅ Strategy comparison and validation
- ✅ Market regime analysis
- ✅ Multi-period performance attribution
- ✅ Risk analytics and drawdown analysis

**Technology Stack:**
- React frontend (current implementation)
- FastAPI backend (current implementation)
- PostgreSQL for historical data storage

## 3. Portfolio Comparison Bridge
**Critical Integration Component**

### Comparison Features:
1. **Live vs Backtest Comparison**
   - Compare current portfolio allocation vs historical backtest allocations
   - Performance attribution: What would backtest strategy do today?
   - Deviation analysis: How far is current portfolio from model recommendations?

2. **Strategy Alignment Dashboard**
   - Real-time scoring: How well does current portfolio match strategy?
   - Rebalancing recommendations based on backtest models
   - Risk alignment: Compare current risk profile with historical optimal

3. **Cross-Platform API**
   ```python
   # Portfolio Comparison API endpoints
   /api/v1/comparison/current-vs-backtest/{strategy_name}
   /api/v1/comparison/allocation-deviation/{backtest_id}
   /api/v1/comparison/performance-attribution/live-vs-historical
   ```

## Implementation Plan

### Phase 1: Current Portfolio Webapp (New)
1. **Setup React app in `portfolio-webapp/` directory**
2. **Implement live portfolio tracking components**
3. **Create real-time data pipeline**
4. **Build current portfolio breakdown dashboard**

### Phase 2: Analytics Enhancement (Existing)
1. **Enhance existing analytics webapp** ✅ (Already done)
2. **Add more sophisticated comparison tools**
3. **Implement advanced performance attribution**

### Phase 3: Integration Bridge
1. **Build comparison API layer**
2. **Create shared component library**
3. **Implement cross-webapp navigation**
4. **Build unified authentication**

## User Experience Flow

### Portfolio Manager Daily Workflow:
1. **Morning**: Check current portfolio webapp
   - Review overnight P&L
   - Check portfolio vs target allocation
   - Review risk metrics and alerts

2. **Mid-day**: Use comparison tools
   - How is current performance vs historical backtests?
   - What would model recommend for rebalancing?
   - Check deviation from strategy guidelines

3. **Research**: Use analytics webapp
   - Deep-dive into strategy performance
   - Analyze market regime changes
   - Validate model assumptions

## Technical Benefits

### Separation of Concerns:
- **Current Portfolio**: Real-time, low-latency, high-frequency updates
- **Analytics**: Historical, batch processing, complex calculations

### Scalability:
- Independent deployment and scaling
- Different performance requirements
- Different data refresh patterns

### Development:
- Teams can work independently
- Different testing strategies (live data vs historical)
- Clear API boundaries

## Shared Infrastructure

### Common Services:
- **Authentication service** (shared login)
- **Database** (PostgreSQL for historical, Redis for real-time)
- **API Gateway** for routing between webapps
- **Shared component library** for consistent UI

### Data Architecture:
```
PostgreSQL (Historical Data)
├── Backtest results and analysis
├── Historical portfolio breakdowns  
├── Market data archive
└── Strategy performance metrics

Redis (Real-time Data) 
├── Live market prices
├── Current portfolio positions
├── Real-time P&L calculations
└── WebSocket session management
```

## Next Steps

1. **Create portfolio-webapp directory structure**
2. **Implement basic current portfolio tracking**  
3. **Build real-time portfolio breakdown component**
4. **Create comparison API endpoints**
5. **Integrate with existing analytics webapp**

This architecture provides the best of both worlds: specialized tools for different use cases while maintaining the ability to compare current portfolio with historical backtest performance.