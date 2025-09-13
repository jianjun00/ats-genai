# Service Architecture Templates

This directory contains comprehensive templates for implementing consistent service-based architecture across all domains in the ATS platform.

## 📋 Available Templates

### 1. **Service Interface Template** (`service_interface_template.py`)
- **Purpose**: Define public contracts for domain services
- **Features**: Complete CRUD operations, search capabilities, batch processing, domain-specific operations
- **Usage**: Replace `{DOMAIN}` with your domain name (e.g., MarketData, Analytics, Trading, News)

### 2. **Service Implementation Template** (`service_implementation_template.py`)
- **Purpose**: Business logic layer with DAO coordination
- **Features**: Validation, error handling, transaction management, DTO conversion
- **Usage**: Implement actual business rules and DAO integration

### 3. **API Router Template** (`api_router_template.py`)
- **Purpose**: HTTP API layer with FastAPI integration
- **Features**: REST endpoints, request/response models, error handling, batch operations
- **Usage**: Create consistent HTTP APIs across all domains

### 4. **Service Container Template** (`service_container_template.py`)
- **Purpose**: Dependency injection and service lifecycle management
- **Features**: Environment-aware configuration, health checks, resource management
- **Usage**: Configure and manage service dependencies

## 🚀 Quick Start Guide

### Step 1: Choose Your Domain
Select the domain you want to implement:
- `MarketData` - OHLCV data, timeframes, aggregations
- `Analytics` - Technical indicators, performance metrics
- `Trading` - Orders, positions, portfolio management
- `News` - Articles, sentiment analysis, event correlation

### Step 2: Generate Service Files
```bash
# Example for Market Data Service
DOMAIN="MarketData"

# Create service interface
sed "s/{DOMAIN}/$DOMAIN/g" service_interface_template.py > ../market_data/services/interfaces/market_data_service_interface.py

# Create service implementation
sed "s/{DOMAIN}/$DOMAIN/g" service_implementation_template.py > ../market_data/services/impl/market_data_service_impl.py

# Create API router
sed "s/{DOMAIN}/$DOMAIN/g" api_router_template.py > ../../services/web_services/api/market_data_api.py

# Create service container
sed "s/{DOMAIN}/$DOMAIN/g" service_container_template.py > ../market_data/services/config/service_container.py
```

### Step 3: Customize for Your Domain
1. **Add domain-specific DTOs** in the service interface
2. **Implement actual DAO operations** in the service implementation
3. **Add custom endpoints** in the API router
4. **Configure DAO dependencies** in the service container

### Step 4: Implement Business Logic
1. **Define validation rules** specific to your domain
2. **Add business operations** that coordinate multiple DAOs
3. **Implement domain workflows** and complex operations
4. **Add caching and optimization** patterns

## 📚 Implementation Examples

### Market Data Service Example
```python
# Custom DTOs for Market Data
@dataclass
class OHLCVDataDTO:
    symbol: str
    timeframe: str  # '1m', '5m', '1h', '1d'
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

# Custom operations for Market Data
async def get_ohlcv_data(
    self,
    symbol: str,
    timeframe: str,
    date_range: tuple
) -> List[OHLCVDataDTO]:
    """Get OHLCV data for symbol and timeframe"""
```

### Analytics Service Example
```python
# Custom DTOs for Analytics
@dataclass
class IndicatorResultDTO:
    symbol: str
    indicator_type: str  # 'RSI', 'MACD', 'SMA', etc.
    timeframe: str
    values: Dict[str, float]
    calculation_timestamp: datetime

# Custom operations for Analytics
async def calculate_technical_indicators(
    self,
    symbol: str,
    indicators: List[str],
    timeframe: str = "1d"
) -> List[IndicatorResultDTO]:
    """Calculate technical indicators for symbol"""
```

### Trading Service Example
```python
# Custom DTOs for Trading
@dataclass
class OrderDTO:
    symbol: str
    order_type: str  # 'market', 'limit', 'stop'
    side: str        # 'buy', 'sell'
    quantity: int
    price: Optional[float] = None
    stop_price: Optional[float] = None

# Custom operations for Trading
async def place_order(self, order: OrderDTO) -> OrderResultDTO:
    """Place trading order with risk validation"""
```

### News Service Example
```python
# Custom DTOs for News
@dataclass
class NewsArticleDTO:
    title: str
    content: str
    source: str
    published_at: datetime
    symbols: List[str]
    sentiment_score: Optional[float] = None
    relevance_score: Optional[float] = None

# Custom operations for News
async def get_news_by_symbol(
    self,
    symbol: str,
    date_range: tuple,
    sentiment_filter: Optional[str] = None
) -> List[NewsArticleDTO]:
    """Get filtered news articles for symbol"""
```

## 🧪 Testing Templates

### Service Testing Template
```python
class TestMarketDataServiceImpl:
    @pytest.fixture
    def mock_market_data_dao(self):
        dao = Mock()
        dao.get_ohlcv_data = AsyncMock()
        return dao

    @pytest.fixture
    def service(self, mock_market_data_dao):
        return MarketDataServiceImpl(market_data_dao=mock_market_data_dao)

    async def test_get_ohlcv_data_success(self, service, mock_market_data_dao):
        # Setup mock
        mock_data = [create_test_ohlcv_record()]
        mock_market_data_dao.get_ohlcv_data.return_value = mock_data

        # Execute
        result = await service.get_ohlcv_data("AAPL", "1d", date_range)

        # Verify
        assert len(result) == 1
        assert result[0].symbol == "AAPL"
```

### API Testing Template
```python
def test_market_data_api_get_ohlcv(client, mock_service):
    # Setup service mock
    mock_service.get_ohlcv_data.return_value = [test_ohlcv_dto]

    # Test API endpoint
    response = client.get("/api/v1/market-data/AAPL/ohlcv?timeframe=1d")

    # Verify HTTP response
    assert response.status_code == 200
    assert response.json()[0]["symbol"] == "AAPL"
```

## 📊 Service Architecture Patterns

### Standard Service Operations

#### Core CRUD Pattern
```python
# Every service implements these basic operations
async def create_{domain}(dto: {Domain}DTO) -> OperationResult
async def get_{domain}_by_id(id: int) -> Optional[{Domain}DTO]
async def update_{domain}(dto: {Domain}DTO) -> OperationResult
async def delete_{domain}(id: int) -> OperationResult
```

#### Search and Filter Pattern
```python
# Every service implements search capabilities
async def list_{domain}s(criteria: SearchCriteria) -> List[{Domain}DTO]
async def search_{domain}s(query: str, criteria: SearchCriteria) -> List[{Domain}DTO]
async def count_{domain}s(criteria: SearchCriteria) -> int
```

#### Batch Operations Pattern
```python
# Every service implements batch processing
async def create_{domain}s_batch(dtos: List[{Domain}DTO]) -> BulkOperationResult
async def update_{domain}s_batch(dtos: List[{Domain}DTO]) -> BulkOperationResult
```

#### Utility Operations Pattern
```python
# Every service implements utilities
async def validate_{domain}_data(dto: {Domain}DTO) -> OperationResult
async def get_{domain}_metadata() -> Dict[str, Any]
async def health_check() -> Dict[str, Any]
```

### Domain-Specific Extensions

Each domain adds specialized operations:

```python
# Market Data Domain
async def get_aggregated_data(symbol: str, timeframes: List[str]) -> AggregatedDataDTO
async def stream_real_time_data(symbol: str) -> AsyncGenerator[OHLCVDataDTO, None]

# Analytics Domain
async def calculate_portfolio_metrics(portfolio: PortfolioDTO) -> MetricsDTO
async def backtest_strategy(strategy: StrategyDTO, data: DatasetDTO) -> BacktestResultDTO

# Trading Domain
async def get_portfolio_positions() -> List[PositionDTO]
async def calculate_risk_metrics(order: OrderDTO) -> RiskAssessmentDTO

# News Domain
async def analyze_sentiment_impact(symbol: str, date_range: tuple) -> SentimentAnalysisDTO
async def detect_market_events(criteria: EventDetectionCriteria) -> List[MarketEventDTO]
```

## 🔧 Configuration and Environment

### Environment-Specific Configuration
```python
# Development Environment
dev_container = await create_development_container()
dev_service = dev_container.get_market_data_service()

# Integration Environment
intg_container = await create_integration_container()
intg_service = intg_container.get_market_data_service()

# Production Environment
prod_container = await create_production_container()
prod_service = prod_container.get_market_data_service()
```

### Service Health Monitoring
```python
# Container Health Check
container_health = container.get_health_status()

# Service Health Check
service_health = await service.health_check()

# Complete System Health Check
system_health = await container.perform_health_check()
```

## 📈 Performance Optimization Patterns

### Caching Integration
```python
class MarketDataServiceImpl:
    async def get_ohlcv_data(self, symbol: str, timeframe: str):
        cache_key = f"ohlcv:{symbol}:{timeframe}"

        # Check cache first
        cached_result = await self._get_cached_result(cache_key)
        if cached_result:
            return cached_result

        # Fetch from database
        result = await self._fetch_from_dao(symbol, timeframe)

        # Cache for future requests
        await self._set_cached_result(cache_key, result, ttl_seconds=300)

        return result
```

### Batch Processing Optimization
```python
async def create_market_data_batch(self, data_points: List[OHLCVDataDTO]):
    # Group by symbol and timeframe for efficient processing
    grouped_data = self._group_by_symbol_timeframe(data_points)

    results = []
    for (symbol, timeframe), group in grouped_data.items():
        # Use bulk database operations
        batch_result = await self.market_data_dao.bulk_insert(group)
        results.extend(batch_result)

    return BulkOperationResult(results)
```

## 🚀 Migration Strategy

### Phase 1: Generate Templates (5 minutes)
```bash
# Choose your domain and generate all templates
./generate_service_templates.sh MarketData
```

### Phase 2: Customize Business Logic (2-4 hours)
- Add domain-specific DTOs and operations
- Implement actual DAO integration
- Add validation rules and business logic

### Phase 3: Implement API Layer (1-2 hours)
- Add custom endpoints for domain operations
- Implement request/response models
- Add proper error handling

### Phase 4: Configure Dependencies (30 minutes)
- Set up DAO dependencies in service container
- Configure environment-specific settings
- Add health checks and monitoring

### Phase 5: Add Tests (2-3 hours)
- Implement service unit tests with mocked DAOs
- Add API integration tests with mocked services
- Create end-to-end workflow tests

## ✅ Validation Checklist

Before considering your service complete:

### Service Interface ✅
- [ ] All CRUD operations defined
- [ ] Search and filtering operations included
- [ ] Batch operations for performance
- [ ] Domain-specific operations added
- [ ] Proper type hints and documentation

### Service Implementation ✅
- [ ] Business validation logic implemented
- [ ] Error handling with structured responses
- [ ] DTO conversion between service and DAO layers
- [ ] Transaction management for data consistency
- [ ] Logging and monitoring integration

### API Layer ✅
- [ ] All endpoints with proper HTTP status codes
- [ ] Request/response validation
- [ ] Error handling with appropriate HTTP responses
- [ ] Dependency injection configuration
- [ ] OpenAPI documentation

### Service Container ✅
- [ ] Proper dependency initialization order
- [ ] Environment-aware configuration
- [ ] Health checks and monitoring
- [ ] Graceful shutdown and cleanup
- [ ] Resource management

### Testing ✅
- [ ] Service unit tests with 95%+ coverage
- [ ] API integration tests for all endpoints
- [ ] Error scenario testing
- [ ] Performance testing for batch operations
- [ ] End-to-end workflow validation

## 🎯 Success Metrics

Your service implementation is successful when:

- **Code Reduction**: 50-90% less code in client implementations
- **Consistency**: All operations follow the same patterns
- **Testability**: Easy to mock and test in isolation
- **Performance**: <100ms response time for standard operations
- **Reliability**: <0.1% error rate in production
- **Maintainability**: Changes isolated to service layer

Use these templates as your foundation for building scalable, maintainable service-based architecture across the entire ATS platform!