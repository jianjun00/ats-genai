# 🔧 Backend Platform

**APIs, Services, Authentication, and Business Logic**

Complete backend platform documentation consolidating all API services, authentication systems, and business logic components.

---

## 🎯 Platform Overview

The Backend Platform provides the core application layer that exposes business functionality through APIs, handles authentication, manages data persistence, and orchestrates business workflows.

### **Core Services**
- **API Gateway** - Single entry point for all client requests
- **Authentication Service** - User authentication and authorization  
- **Analytics API** - Portfolio analytics and recommendations
- **Data Access Layer** - Database abstraction and ORM
- **Business Logic Services** - Trading algorithms, signal processing

### **Key Technologies**
- **FastAPI** - High-performance async API framework
- **PostgreSQL/TimescaleDB** - Primary data store
- **Redis** - Caching and session management
- **Pydantic** - Data validation and serialization
- **SQLAlchemy** - Database ORM

---

## 🚀 Quick Start

### **Deploy Backend Services**
```bash
# Deploy API gateway
kubectl apply -f k8s/api-gateway-deployment.yaml

# Deploy analytics service
kubectl apply -f k8s/analytics-service-deployment.yaml

# Test API endpoints
curl http://external-ip:port/api/health
curl http://external-ip:port/api/portfolio/recommendations
```

### **Service Dependencies**
```
API Gateway → Authentication Service
     ↓
Analytics API → Business Logic Services  
     ↓
Data Access Layer → PostgreSQL/TimescaleDB
```

---

## 📡 API Specification

### **Portfolio Analytics API**
```python
# Portfolio recommendations endpoint
GET /api/v1/portfolio/recommendations
Parameters:
  - universe: string (SP500, NASDAQ, custom)
  - risk_tolerance: float (0.0-1.0) 
  - target_return: float
  - rebalance_frequency: string (daily, weekly, monthly)

Response:
{
  "recommendations": [
    {
      "symbol": "AAPL",
      "action": "BUY",
      "weight": 0.15,
      "confidence": 0.87,
      "rationale": "Strong Smart Money Zone accumulation detected"
    }
  ],
  "portfolio_metrics": {
    "expected_return": 0.124,
    "sharpe_ratio": 1.34,
    "max_drawdown": 0.081
  }
}
```

### **Model Inference API**
```python
# Real-time predictions endpoint
POST /api/v1/ml/predict/support_resistance
Body:
{
  "symbol": "AAPL",
  "features": {
    "close": 150.25,
    "volume": 50000000,
    "rsi_14": 65.2,
    "bb_position": 0.7
  }
}

Response:
{
  "support_level": 148.50,
  "resistance_level": 152.75,
  "confidence": 0.87,
  "model_version": "v1.2.3"
}
```

### **Data Access API**
```python
# Market data query endpoint
GET /api/v1/data/prices
Parameters:
  - symbols: string (comma-separated)
  - start_date: string (YYYY-MM-DD)
  - end_date: string (YYYY-MM-DD)
  - frequency: string (1d, 1h, 1m)

Response:
{
  "data": [
    {
      "symbol": "AAPL",
      "date": "2024-01-15",
      "open": 149.20,
      "high": 151.75,
      "low": 148.50,
      "close": 150.25,
      "volume": 50000000
    }
  ]
}
```

---

## 🔐 Authentication & Authorization

### **JWT Authentication**
```python
# Login endpoint
POST /api/v1/auth/login
Body:
{
  "email": "user@example.com", 
  "password": "secure_password"
}

Response:
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "bearer",
  "expires_in": 3600,
  "user": {
    "id": "user_123",
    "email": "user@example.com",
    "role": "premium_subscriber"
  }
}
```

### **Role-Based Access Control**
```python
# User roles and permissions
roles = {
    "basic_subscriber": {
        "permissions": ["read:portfolio", "read:recommendations"],
        "rate_limit": "100/hour"
    },
    "premium_subscriber": {
        "permissions": ["read:portfolio", "read:recommendations", "read:analytics"],
        "rate_limit": "1000/hour" 
    },
    "institutional": {
        "permissions": ["*"],
        "rate_limit": "10000/hour"
    }
}
```

### **API Key Management**
```python
# API key authentication for external services
headers = {
    "Authorization": "Bearer your_api_key",
    "Content-Type": "application/json"
}

# Rate limiting per API key
rate_limits = {
    "free_tier": "100/day",
    "paid_tier": "10000/day", 
    "enterprise": "unlimited"
}
```

---

## 💼 Business Logic Services

### **Portfolio Optimization Engine**
```python
class PortfolioOptimizer:
    def optimize_portfolio(
        self,
        universe: List[str],
        target_return: float = 0.12,
        max_drawdown: float = 0.08,
        rebalance_freq: str = 'weekly'
    ) -> PortfolioAllocation:
        """
        AI-powered portfolio optimization:
        - Market-neutral long/short construction
        - 19-factor risk model
        - Smart Money Zone integration
        - Dynamic rebalancing
        """
        signals = self.get_model_signals(universe)
        smz_signals = self.apply_smart_money_filters(signals)
        risk_factors = self.calculate_risk_factors(universe)
        
        allocation = self.solve_optimization(
            signals=smz_signals,
            risk_factors=risk_factors,
            target_return=target_return,
            max_drawdown=max_drawdown
        )
        
        return allocation
```

### **Signal Processing Service**
```python
class SignalProcessor:
    def process_market_signals(self, symbol: str) -> TradingSignal:
        """
        Process multiple signal sources:
        - Technical analysis indicators
        - Smart Money Zone detection
        - Alternative data sentiment
        - Cross-asset correlations
        """
        technical_signals = self.calculate_technical_indicators(symbol)
        smart_money_signals = self.detect_smart_money_zones(symbol) 
        sentiment_signals = self.analyze_sentiment(symbol)
        
        combined_signal = self.ensemble_signals([
            technical_signals,
            smart_money_signals, 
            sentiment_signals
        ])
        
        return TradingSignal(
            symbol=symbol,
            signal_strength=combined_signal.strength,
            confidence=combined_signal.confidence,
            rationale=combined_signal.rationale
        )
```

### **Risk Management Service**
```python
class RiskManager:
    def calculate_portfolio_risk(self, allocation: PortfolioAllocation) -> RiskMetrics:
        """
        Comprehensive risk calculation:
        - Value at Risk (VaR)
        - Expected Shortfall (ES)
        - Maximum Drawdown
        - Sharpe Ratio
        - Risk-adjusted returns
        """
        var_95 = self.calculate_var(allocation, confidence=0.95)
        expected_shortfall = self.calculate_expected_shortfall(allocation)
        max_drawdown = self.calculate_max_drawdown(allocation)
        sharpe_ratio = self.calculate_sharpe_ratio(allocation)
        
        return RiskMetrics(
            var_95=var_95,
            expected_shortfall=expected_shortfall,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio
        )
```

---

## 🗄️ Data Layer Architecture

### **Database Abstraction**
```python
class DataAccessLayer:
    def __init__(self):
        self.db = TimescaleDBConnection()
        self.cache = RedisConnection()
        
    async def get_market_data(
        self, 
        symbols: List[str], 
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Efficient market data retrieval with caching
        """
        cache_key = f"market_data:{'-'.join(symbols)}:{start_date}:{end_date}"
        
        # Check cache first
        cached_data = await self.cache.get(cache_key)
        if cached_data:
            return pd.read_json(cached_data)
            
        # Query database
        query = """
        SELECT symbol, date, open, high, low, close, volume
        FROM dev_daily_prices 
        WHERE symbol = ANY(%s)
          AND date BETWEEN %s AND %s
        ORDER BY symbol, date
        """
        
        data = await self.db.fetch_dataframe(query, symbols, start_date, end_date)
        
        # Cache for future requests
        await self.cache.setex(cache_key, 3600, data.to_json())
        
        return data
```

### **Connection Management**
```python
class DatabaseManager:
    def __init__(self):
        self.connection_pool = asyncpg.create_pool(
            host="postgres",
            port=5432,
            database="dev_db",
            user="postgres", 
            password=os.getenv("DB_PASSWORD"),
            min_size=10,
            max_size=20
        )
    
    async def execute_query(self, query: str, *args) -> List[Dict]:
        async with self.connection_pool.acquire() as conn:
            return await conn.fetch(query, *args)
```

---

## 📊 Monitoring & Health Checks

### **Service Health Endpoints**
```python
@app.get("/health")
async def health_check():
    """Comprehensive health check"""
    checks = {
        "database": await check_database_connection(),
        "redis": await check_redis_connection(),
        "external_apis": await check_external_apis(),
        "ml_models": await check_model_availability()
    }
    
    status = "healthy" if all(checks.values()) else "degraded"
    
    return {
        "status": status,
        "timestamp": datetime.utcnow(),
        "checks": checks,
        "version": "1.2.3"
    }
```

### **Performance Metrics**
- **API Response Time**: < 100ms for 95th percentile
- **Service Availability**: 99.9% uptime
- **Request Throughput**: 10,000 RPS peak capacity
- **Error Rate**: < 0.1% of total requests
- **Cache Hit Ratio**: > 80% for frequently accessed data

---

## 🚀 Deployment Configuration

### **Service Deployment**
```yaml
# k8s/backend-services.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: analytics-api
  namespace: ats-dev
spec:
  replicas: 3
  selector:
    matchLabels:
      app: analytics-api
  template:
    metadata:
      labels:
        app: analytics-api
    spec:
      containers:
      - name: analytics-api
        image: dragonflyer762/ats-genai:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m" 
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

---

**🎯 The Backend Platform provides enterprise-grade APIs with high performance, security, and scalability for algorithmic trading operations.**