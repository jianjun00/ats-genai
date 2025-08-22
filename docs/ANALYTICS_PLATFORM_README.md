# Backtest Analytics Platform

A comprehensive web application for analyzing backtest performance, model accuracy, and portfolio metrics with interactive visualizations and drill-down capabilities.

## 🎯 **Overview**

This platform provides a complete solution for understanding **what works and what doesn't** in your trading strategies through:

- **Interactive Portfolio Analytics**: Real-time performance metrics and risk analysis
- **Model Performance Tracking**: Prediction accuracy and confidence calibration
- **Attribution Analysis**: Understanding contribution by stocks, sectors, and signals
- **Drill-Down Capabilities**: Click any chart to get detailed analysis
- **Strategy Comparison**: Side-by-side comparison of different approaches
- **Forecast Visualization**: Support/resistance predictions with confidence bands

## 🏗️ **Architecture**

### **Frontend (React + D3.js)**
- Interactive dashboards with real-time updates
- Drill-down capabilities for detailed analysis
- Responsive design for desktop and mobile
- WebSocket integration for live data

### **Backend (FastAPI + PostgreSQL)**
- High-performance analytics engine
- RESTful API with WebSocket support
- Redis caching for sub-second response times
- Comprehensive data models for financial metrics

### **Data Layer (TimescaleDB + Redis)**
- Time-series optimized storage
- Efficient indexing for portfolio queries
- Real-time caching and session management

## 📊 **Key Features**

### **1. Portfolio Performance Dashboard**
```
┌─────────────────────────────────────────────┐
│ 📈 Total Return: +15.2%  📊 Sharpe: 1.34   │
│ 📉 Max DD: -8.1%        🎯 Win Rate: 67%   │
└─────────────────────────────────────────────┘
│                                             │
│ Interactive Equity Curve with Drill-Down   │
│ ┌─────────────────────────────────────────┐ │
│ │     📈                                  │ │
│ │        ╱╲     📈                       │ │
│ │   ╱╲  ╱  ╲   ╱ ╲                      │ │
│ │  ╱  ╲╱    ╲ ╱   ╲                     │ │
│ │ ╱          ╲╱     ╲                    │ │
│ └─────────────────────────────────────────┘ │
│ Click any period for detailed analysis     │
└─────────────────────────────────────────────┘
```

### **2. Model Performance Tracking**
```
Support/Resistance Accuracy Over Time
┌─────────────────────────────────────┐
│ Model Version: v23 (Adaptive)      │
│ ┌─────────────────────────────────┐ │
│ │ Accuracy: 72.3% ████████▓░░░░░░ │ │
│ │ Confidence: 0.68 ██████▓░░░░░░░ │ │
│ │ MAE: 0.024 ████▓░░░░░░░░░░░░░░░ │ │
│ └─────────────────────────────────┘ │
│ Daily Retraining: ✅               │
└─────────────────────────────────────┘
```

### **3. Attribution Analysis**
```
Performance Attribution Breakdown
┌─────────────────────────────────────┐
│ By Stock:                           │
│ AAPL  ████████ +2.1%              │
│ MSFT  ██████   +1.8%              │
│ GOOGL ████     +1.2%              │
│                                     │
│ By Signal Type:                     │
│ Support Bounce    ██████ +3.2%     │
│ Resistance Break  ████   +1.9%     │
│ Mean Reversion    ██     +0.8%     │
└─────────────────────────────────────┘
```

### **4. Drill-Down Analysis**
Click any chart element to see:
- **Time Period Drill-Down**: Detailed analysis for selected dates
- **Stock-Level Analysis**: Individual stock performance and trades
- **Trade-Level Details**: Entry/exit rationale and model confidence
- **Market Context**: Regime detection and major events

## 🚀 **Quick Start**

### **1. One-Command Startup (Recommended)**
```bash
# Start complete dev analytics platform with real data
python run_dev_analytics.py
```

This will automatically:
- Check prerequisites and setup database
- Run production backtest with real adaptive models
- Start backend API and frontend dashboard
- Open analytics interface at http://localhost:3000

### **2. Manual Setup (Advanced)**

#### **Prerequisites**
```bash
# Python 3.9+
python --version

# Node.js 16+
node --version

# PostgreSQL (auto-configured)
# Redis (optional, for caching)
```

#### **Database Setup**
```bash
# Run migrations
PYTHONPATH=src python src/db/migration_manager.py migrate
```

#### **Generate Real Backtest Data**
```bash
# Run production backtest with actual models
PYTHONPATH=src python scripts/analytics/production_backtest_runner.py
```

#### **Start Services**
```bash
# Terminal 1 - Backend API
PYTHONPATH=src uvicorn api.backtest_analytics_api:app --reload

# Terminal 2 - Frontend Dashboard
cd frontend && npm start
```

### **3. Access the Platform**
- **📊 Analytics Dashboard**: http://localhost:3000
- **🔗 API Documentation**: http://localhost:8000/docs
- **❤️ Health Check**: http://localhost:8000/health

## 📱 **User Interface Guide**

### **Main Dashboard**
1. **Filter Panel**: Select time range, benchmark, and metrics to display
2. **Metrics Summary**: Key performance indicators with real-time updates
3. **Interactive Charts**: Click and drag to zoom, click points for drill-down
4. **Real-Time Indicator**: Shows live data connection status

### **Navigation**
- **Dashboard**: Main portfolio analytics view
- **Comparison**: Side-by-side strategy comparison
- **Backtest List**: Browse all available backtest runs
- **Settings**: Configure display preferences

### **Drill-Down Panel**
- **Automatic**: Opens when you click any chart element
- **Contextual**: Shows relevant details based on what you clicked
- **Interactive**: Includes mini-charts and detailed metrics
- **Exportable**: Download drill-down data as PDF/Excel

## 🔧 **Configuration**

### **Environment Variables**
```bash
# Backend Configuration
ENVIRONMENT=dev
DATABASE_URL=postgresql://user:pass@localhost/db
REDIS_URL=redis://localhost:6379

# Frontend Configuration
REACT_APP_API_URL=http://localhost:8000
REACT_APP_WS_URL=ws://localhost:8000
```

### **Analytics Configuration**
```python
# src/analytics/config.py
CACHE_TTL = 300  # 5 minutes
MAX_DRILL_DOWN_DEPTH = 3
REAL_TIME_UPDATE_INTERVAL = 1000  # 1 second
```

## 📈 **Data Models**

### **Portfolio Performance**
```sql
CREATE TABLE portfolio_performance (
    backtest_run_id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_value DECIMAL(15,4),
    daily_return DECIMAL(8,6),
    drawdown DECIMAL(8,6),
    -- ... additional metrics
);
```

### **Model Performance**
```sql
CREATE TABLE model_performance (
    backtest_run_id UUID NOT NULL,
    date DATE NOT NULL,
    support_accuracy DECIMAL(5,4),
    resistance_accuracy DECIMAL(5,4),
    confidence_correlation DECIMAL(5,4),
    -- ... additional metrics
);
```

## 🔍 **API Reference**

### **Portfolio Endpoints**
```bash
# Get portfolio metrics
GET /api/v1/backtests/{id}/portfolio/metrics

# Get performance time series
GET /api/v1/backtests/{id}/portfolio/performance

# Get attribution analysis
GET /api/v1/backtests/{id}/attribution
```

### **Model Endpoints**
```bash
# Get model performance
GET /api/v1/backtests/{id}/model/performance

# Get forecasts
GET /api/v1/backtests/{id}/forecasts
```

### **Drill-Down Endpoints**
```bash
# Period analysis
GET /api/v1/backtests/{id}/drill-down/period

# Stock analysis
GET /api/v1/backtests/{id}/drill-down/stock/{symbol}

# Trade analysis
GET /api/v1/backtests/{id}/drill-down/trade/{trade_id}
```

### **WebSocket Endpoints**
```bash
# Real-time portfolio updates
ws://localhost:8000/ws/backtests/{id}/portfolio

# Real-time model updates
ws://localhost:8000/ws/backtests/{id}/model
```

## 📊 **Sample Queries**

### **1. Compare Adaptive vs Static Models**
```javascript
// Frontend usage
const comparison = await apiClient.post('/api/v1/comparison/portfolio', {
  backtest_run_ids: ['adaptive_2023_100', 'static_2023_100'],
  start_date: '2023-01-01',
  end_date: '2024-06-30'
});

console.log('Winner:', comparison.comparison_summary.best_sharpe);
```

### **2. Get Drill-Down for Specific Period**
```javascript
// When user clicks on chart
const drillDown = await apiClient.get(
  `/api/v1/backtests/adaptive_2023_100/drill-down/period`,
  {
    params: {
      start_date: '2023-03-01',
      end_date: '2023-03-31'
    }
  }
);
```

### **3. Real-Time Updates**
```javascript
// WebSocket connection
const ws = new WebSocket('ws://localhost:8000/ws/backtests/adaptive_2023_100/portfolio');

ws.onmessage = (event) => {
  const update = JSON.parse(event.data);
  updatePortfolioMetrics(update.data);
};
```

## 🎨 **Customization**

### **Adding New Metrics**
1. **Backend**: Add metric calculation in `portfolio_analytics.py`
2. **Frontend**: Add visualization in appropriate chart component
3. **API**: Expose through relevant endpoint

### **Custom Charts**
1. **Create Component**: `src/components/Charts/CustomChart.js`
2. **Add to Dashboard**: Import and use in `Dashboard.js`
3. **Configure Data**: Use existing hooks or create new ones

### **New Drill-Down Types**
1. **Backend**: Add handler in `analytics_engine.py`
2. **Frontend**: Add case in `DrillDownPanel.js`
3. **API**: Add endpoint in `backtest_analytics_api.py`

## 🐛 **Troubleshooting**

### **Common Issues**

#### **"Database connection failed"**
```bash
# Check database is running
pg_isready -h localhost -p 5432

# Check credentials
psql -h localhost -U username -d database_name
```

#### **"Frontend won't start"**
```bash
# Clear npm cache
npm cache clean --force

# Reinstall dependencies
rm -rf node_modules package-lock.json
npm install
```

#### **"WebSocket connection failed"**
```bash
# Check backend is running
curl http://localhost:8000/health

# Check WebSocket endpoint
wscat -c ws://localhost:8000/ws/backtests/test/portfolio
```

#### **"Charts not loading"**
```bash
# Check browser console for errors
# Ensure API endpoints are accessible
curl http://localhost:8000/api/v1/backtests
```

### **Performance Issues**

#### **Slow chart rendering**
- Reduce data points with sampling
- Implement data virtualization
- Use canvas rendering for large datasets

#### **API response times**
- Check database query performance
- Verify Redis caching is working
- Monitor connection pool usage

## 📚 **Advanced Usage**

### **Custom Analytics**
```python
# Add custom metric calculation
class CustomAnalyticsEngine(PortfolioAnalyticsEngine):
    async def compute_custom_metric(self, backtest_id):
        # Your custom logic here
        return custom_value
```

### **Real-Time Integration**
```python
# Publish real-time updates
await redis_client.publish(
    f"backtest_updates:{backtest_id}",
    json.dumps(new_data)
)
```

### **Export Integration**
```javascript
// Custom export functionality
const exportData = await apiClient.get(
  `/api/v1/backtests/${id}/export`,
  { params: { format: 'excel', metrics: ['performance', 'attribution'] } }
);
```

## 🔒 **Security**

### **Authentication**
- JWT token-based authentication
- Role-based access control (RBAC)
- Session management with Redis

### **Data Protection**
- Input validation and sanitization
- SQL injection prevention
- XSS protection in frontend

### **API Security**
- Rate limiting
- CORS configuration
- Request/response logging

## 🚢 **Deployment**

### **Production Setup**
```bash
# Docker deployment
docker-compose up -d

# Kubernetes deployment
kubectl apply -f k8s/

# Environment configuration
export ENVIRONMENT=production
export DATABASE_URL=postgresql://prod_user:pass@db:5432/prod_db
```

### **Monitoring**
- Application metrics with Prometheus
- Error tracking with Sentry
- Performance monitoring with New Relic
- Database monitoring with pgAdmin

## 🤝 **Contributing**

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-analytics`
3. **Make changes** and add tests
4. **Submit pull request** with detailed description

## 📄 **License**

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🎉 **What You Get**

This analytics platform provides **everything you need** to understand your backtest performance:

✅ **Real-time portfolio tracking** with live updates  
✅ **Interactive visualizations** with drill-down capabilities  
✅ **Model performance monitoring** with accuracy tracking  
✅ **Attribution analysis** to see what's working  
✅ **Strategy comparison** to optimize approaches  
✅ **Export capabilities** for presentations and reports  
✅ **Mobile-responsive design** for analysis anywhere  
✅ **WebSocket integration** for real-time collaboration  

**Transform your backtest data into actionable insights!** 🚀