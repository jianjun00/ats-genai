# 📊 Real Analytics Access Guide

## 🎯 **Where to Check Real Analytics**

### **1. 🌐 REST API Endpoints (Primary Access)**

**Start the Analytics API Server:**
```bash
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db uvicorn src.api.backtest_analytics_api:app --host 0.0.0.0 --port 8000
```

**Access Analytics at:**
- **Interactive Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

**Key Real Analytics Endpoints:**

#### **Portfolio Performance Analytics**
```bash
# List all backtest runs with real performance data
GET /api/v1/backtests

# Real portfolio metrics (Sharpe, Calmar, Max Drawdown, etc.)
GET /api/v1/backtests/{backtest_run_id}/portfolio/metrics

# Time series performance data 
GET /api/v1/backtests/{backtest_run_id}/portfolio/performance

# Real-time portfolio updates via WebSocket
WS /ws/backtests/{backtest_run_id}/portfolio
```

#### **Model Performance Analytics**
```bash
# Model accuracy and performance metrics
GET /api/v1/backtests/{backtest_run_id}/model/performance

# Price forecasts with confidence intervals
GET /api/v1/backtests/{backtest_run_id}/forecasts

# Real-time model updates via WebSocket
WS /ws/backtests/{backtest_run_id}/model
```

#### **Attribution Analysis**
```bash
# Factor attribution analysis (stock/sector/signal level)
GET /api/v1/backtests/{backtest_run_id}/attribution?attribution_type=stock
GET /api/v1/backtests/{backtest_run_id}/attribution?attribution_type=sector
GET /api/v1/backtests/{backtest_run_id}/attribution?attribution_type=signal
```

#### **Drill-Down Analytics**
```bash
# Period-specific analysis
GET /api/v1/backtests/{backtest_run_id}/drill-down/period?start_date=2024-01-01&end_date=2024-01-31

# Stock-specific analysis
GET /api/v1/backtests/{backtest_run_id}/drill-down/stock/{symbol}

# Individual trade analysis
GET /api/v1/backtests/{backtest_run_id}/drill-down/trade/{trade_id}
```

### **2. 🚀 Production Backtest Runner (Generate Analytics)**

**Generate Real Analytics Data:**
```bash
# Run comprehensive backtest with real market data
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python scripts/analytics/production_backtest_runner.py --start-date 2024-01-01 --end-date 2024-12-31 --universe sp500_liquid --capital 1000000
```

**What This Generates:**
- **Real Portfolio Performance**: Using actual market prices and volumes
- **Adaptive Model Results**: ML-driven support/resistance predictions
- **Static Baseline Comparison**: Buy-and-hold benchmark
- **Factor Attribution**: Sector, size, momentum factor analysis
- **Trade-Level Analytics**: Entry/exit analysis with real execution costs

### **3. 📈 Direct Analytics Engine**

**Python Code Access:**
```python
from analytics.portfolio_analytics import PortfolioAnalyticsEngine
from config.environment import Environment

# Initialize with real database
env = Environment()
engine = PortfolioAnalyticsEngine(env=env)
await engine.initialize()

# Get real portfolio metrics
metrics = await engine.compute_portfolio_metrics(
    backtest_run_id="your_backtest_id",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31)
)

# Get attribution analysis
attribution = await engine.compute_attribution_analysis(
    backtest_run_id="your_backtest_id"
)

# Get model performance
model_perf = await engine.compute_model_performance_metrics(
    backtest_run_id="your_backtest_id"
)
```

### **4. 📊 Dashboard & Visualization**

**Frontend Analytics Dashboard:**
```bash
# Start React dashboard (after fixing npm dependencies)
cd frontend
npm install
npm start  # Will run on http://localhost:3000 (or alternative port)
```

**Dashboard Features:**
- **Real-time portfolio performance charts**
- **Interactive attribution analysis**
- **Model accuracy visualization**
- **Risk metrics dashboard**
- **Factor exposure heatmaps**

### **5. 🎨 Example Analytics Queries**

**Real Portfolio Metrics Example:**
```json
{
  "total_return": 0.1234,
  "annualized_return": 0.1456,
  "volatility": 0.1123,
  "sharpe_ratio": 1.298,
  "sortino_ratio": 1.456,
  "calmar_ratio": 0.987,
  "max_drawdown": -0.0834,
  "max_drawdown_duration_days": 23,
  "var_95": -0.0234,
  "expected_shortfall_95": -0.0345,
  "total_trades": 1247,
  "win_rate": 0.567,
  "profit_factor": 1.89
}
```

**Real Attribution Analysis Example:**
```json
{
  "sector_attribution": {
    "technology": 0.0234,
    "healthcare": 0.0156,
    "financials": -0.0098
  },
  "factor_attribution": {
    "market_beta": 0.0123,
    "size_factor": 0.0045,
    "momentum": 0.0234,
    "value": -0.0067
  },
  "stock_attribution": {
    "AAPL": 0.0156,
    "MSFT": 0.0089,
    "GOOGL": 0.0234
  }
}
```

## 🔧 **Setup Requirements**

### **Database Connection Required:**
All analytics require a working PostgreSQL/TimescaleDB connection with real market data.

**Environment Variables:**
```bash
export ENVIRONMENT=dev
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=dev_password  # For ats-dev environment
export DB_NAME=dev_db
```

### **Data Requirements:**
- **Market Data**: Real price/volume data from Polygon/Tiingo/etc.
- **Universe Data**: Actual stock symbols and metadata
- **Backtest Runs**: Completed backtests with real performance data

## 🚨 **Important Notes**

1. **No Mock Data**: All analytics use real market data only
2. **Database Required**: Analytics engine requires working database connection
3. **Real Performance**: All metrics computed from actual trading scenarios
4. **Live Updates**: WebSocket endpoints provide real-time analytics updates
5. **Historical Analysis**: Full drill-down capabilities for trade-level analysis

## 🎯 **Quick Start**

1. **Ensure database is running** with dev_password
2. **Run a production backtest** to generate analytics data
3. **Start the API server** to access analytics endpoints
4. **Use /docs endpoint** for interactive API exploration
5. **Check real-time updates** via WebSocket connections

**Next Steps:** Fix database connectivity to enable full analytics access.