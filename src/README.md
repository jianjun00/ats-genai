# Source Code Directory (`src/`)

This directory contains the core application source code for the ATS-GenAI algorithmic trading system.

## Directory Structure

```
src/
├── api/                    # API layer components
├── app/                    # Application runners and utilities
├── auth/                   # Authentication and authorization
├── calendars/              # Market calendar and time management
├── config/                 # Configuration management
├── dao/                    # Data Access Objects
├── db/                     # Database setup and migrations
├── economic_events/        # Economic events data management
├── events/                 # Event system and ingestion
├── frontfill/              # Forward-looking data processing
├── market_data/            # Market data ingestion and processing
├── modeling/               # ML models and forecasting
├── models/                 # Data models and ML architectures
├── monitoring/             # System monitoring and validation
├── pipeline/               # Data processing pipelines
├── portfolio/              # Portfolio management system ⭐
├── secmaster/              # Security master data management
├── sentiment/              # Sentiment analysis
├── signals/                # Technical indicators and signals ⭐
├── state/                  # State management and intervals
├── storage/                # Data storage management
├── training/               # ML model training
├── universe/               # Universe management
├── utils/                  # Shared utilities
├── validation/             # Data validation
└── main.py                 # FastAPI application entry point
```

## Core Components

### 🏛️ **Application Layer**
- **`main.py`**: FastAPI application entry point with routing and middleware
- **`api/`**: REST API endpoints and schemas
- **`app/`**: Application runners and orchestration utilities

### 📊 **Portfolio Management System** ⭐
Located in `portfolio/` - **Recently implemented comprehensive system**:
- **Factor Framework**: 19-factor risk model for market-neutral strategies
- **Signal Generation**: Multi-indicator system with Smart Money Zones
- **Optimization**: Long-short portfolio construction with constraints
- **Performance Metrics**: Advanced metrics (Information Ratio, Calmar Ratio)
- **Recommendation Engine**: Hourly portfolio recommendations for $200K portfolio

### 📈 **Market Data & Signals**
- **`market_data/`**: Multi-vendor data ingestion (Polygon, Tiingo, etc.)
- **`signals/`**: Technical indicators and Smart Money analysis ⭐
- **`secmaster/`**: Security master data and instrument management
- **`events/`**: Unified event system with multi-source reconciliation

### 🗄️ **Data Layer**
- **`dao/`**: Data Access Objects for all database entities
- **`db/`**: Database migrations, setup, and connection management
- **`state/`**: Interval-based state management for trading analysis

### 🤖 **Machine Learning**
- **`modeling/`**: Event features, factor models, and forecasting
- **`models/`**: PyTorch models including Temporal Fusion Transformer
- **`training/`**: ML model training pipelines

### ⚙️ **Infrastructure**
- **`config/`**: Environment-aware configuration management
- **`auth/`**: API authentication and authorization
- **`monitoring/`**: Data quality monitoring and validation
- **`validation/`**: Data validation frameworks

## Key Features

### 🎯 **Environment-Aware Architecture**
- **Multi-environment support**: `dev`, `test`, `intg`, `prod`
- **Automatic table prefixing**: `dev_instruments`, `prod_instruments`, etc.
- **Environment-specific configuration**: `.env.dev`, `.env.prod`, etc.

### 📡 **Multi-Source Data Ingestion**
```python
# Supported data vendors
- Polygon.io        # Primary market data
- Tiingo           # Alternative pricing
- Alpha Vantage    # Economic data
- FRED             # Federal Reserve data
- Finnhub          # News and earnings
- Yahoo Finance    # Additional data
```

### 🔄 **Real-Time Processing**
- **Data Agents**: Real-time market data processing in `market_data/agent/`
- **Event System**: Unified event ingestion and reconciliation
- **State Management**: Interval-based processing (5m, 15m, 1h, 1d)

### 🧠 **Advanced Analytics**
- **Technical Indicators**: 20+ indicators with enhanced configurations
- **Smart Money Zones**: Institutional flow analysis
- **Portfolio Optimization**: Market-neutral long-short strategies
- **Performance Attribution**: Factor-based risk analysis

## Getting Started

### 1. **Environment Setup**
```bash
# Set Python path
export PYTHONPATH=$PYTHONPATH:$(pwd)/src

# Install dependencies
uv pip install -r requirements.txt
```

### 2. **Database Setup**
```bash
# Run migrations
python src/db/migration_manager.py

# Setup database schema
python src/db/setup_trading_db.py
```

### 3. **Configuration**
```bash
# Copy environment file
cp .env.example .env.dev

# Edit configuration
vim .env.dev
```

### 4. **Start Application**
```bash
# FastAPI server
uvicorn src.main:app --reload

# Or with Docker
docker-compose up --build
```

## Architecture Patterns

### 🏗️ **Layered Architecture**
```
┌─────────────────┐
│   API Layer     │  # FastAPI endpoints, schemas
├─────────────────┤
│ Business Logic  │  # Portfolio, signals, modeling
├─────────────────┤
│   Data Layer    │  # DAOs, state management
├─────────────────┤
│ Infrastructure  │  # Database, config, monitoring
└─────────────────┘
```

### 🔌 **Plugin Architecture**
- **Data Vendors**: Easy to add new data sources in `market_data/vendors/`
- **Indicators**: Extensible signal framework in `signals/`
- **Event Sources**: Pluggable event ingestion in `events/ingest/`

### 🎯 **Domain-Driven Design**
- **Portfolio Domain**: Complete portfolio management system
- **Market Data Domain**: Data ingestion and processing
- **Signal Domain**: Technical analysis and indicators
- **State Domain**: Time-based state management

## Integration Points

### 📊 **Portfolio System Integration**
```python
from portfolio.recommendation_engine import HourlyRecommendationEngine
from portfolio.optimization import OptimizationConstraints

# Create $200K market-neutral portfolio
engine = HourlyRecommendationEngine(
    portfolio_value=200000,
    constraints=OptimizationConstraints(target_dollar_neutral=True)
)

# Generate hourly recommendation
recommendation = engine.generate_hourly_recommendation()
```

### 📈 **Signal Generation Integration**
```python
from signals.smart_money_zones import SmartMoneyZoneDetector
from signals.enhanced_indicators import EnhancedIndicatorConfig

# Smart Money analysis
detector = SmartMoneyZoneDetector()
signals = detector.analyze_market_structure(price_data)

# Technical indicators
config = EnhancedIndicatorConfig.create_comprehensive_config()
indicators = config.calculate_all_indicators(price_data)
```

### 🗃️ **Data Access Integration**
```python
from dao.daily_prices_dao import DailyPricesDAO
from config.environment import Environment

# Environment-aware data access
env = Environment()
dao = DailyPricesDAO(env)

# Get data with automatic table prefixing
prices = dao.get_daily_prices('AAPL', start_date, end_date)
```

## Development Guidelines

### 📋 **Code Organization**
1. **Separation of Concerns**: Each directory has a specific responsibility
2. **Environment Awareness**: All components support multi-environment deployment
3. **Testing**: Comprehensive test coverage in parallel `tests/` structure
4. **Documentation**: README files in major directories

### 🔧 **Adding New Components**
1. **Data Vendors**: Add to `market_data/vendors/` with base class inheritance
2. **Indicators**: Extend `signals/enhanced_indicators.py` with new calculations
3. **Models**: Add to `models/` with standardized interfaces
4. **APIs**: Add to `api/` with proper FastAPI routing

### 🧪 **Testing Strategy**
- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **Database Tests**: Test with real database connections
- **End-to-End Tests**: Test complete workflows

## Performance Considerations

### ⚡ **Optimization Strategies**
- **Database Connection Pooling**: Efficient connection management
- **Async Processing**: Non-blocking I/O for data ingestion
- **Caching**: Redis caching for frequently accessed data
- **Batch Processing**: Efficient bulk operations

### 📏 **Scalability**
- **Microservice Ready**: Clean separation enables service extraction
- **Kubernetes Deployment**: Container-based deployment with orchestration
- **Horizontal Scaling**: Stateless design supports scaling
- **Data Partitioning**: Time-based partitioning for large datasets

## Monitoring & Observability

### 📊 **Built-in Monitoring**
- **Data Quality Dashboard**: Real-time data validation in `monitoring/`
- **Performance Metrics**: Portfolio performance tracking
- **System Health**: Database and API health checks
- **Error Tracking**: Comprehensive logging and error handling

### 🚨 **Alerting**
- **Data Anomalies**: Automated detection of data quality issues
- **System Failures**: Health check failures and error rates
- **Portfolio Risks**: Risk threshold violations
- **Performance Degradation**: Latency and throughput monitoring

## Contributing

### 🛠️ **Development Workflow**
1. **Environment Setup**: Follow setup instructions above
2. **Feature Development**: Create feature branches for new development
3. **Testing**: Ensure comprehensive test coverage
4. **Documentation**: Update README files and inline documentation
5. **Code Review**: Follow team code review processes

### 📖 **Documentation Standards**
- **README Files**: Each major directory should have comprehensive README
- **Inline Documentation**: Use docstrings for all public interfaces
- **API Documentation**: FastAPI auto-generates API docs
- **Architecture Documentation**: Keep high-level architecture docs updated

---

**💡 Pro Tip**: Start with the `portfolio/` and `signals/` directories to understand the core trading system, then explore `market_data/` for data ingestion patterns.