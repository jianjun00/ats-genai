# Backend Engineer Guide

## Your Role & Responsibilities

Design and implement scalable backend services for the portfolio GPT platform:
- **Recommendation Engine API** - Hourly forecast generation for paid users
- **Real-time Market Data Processing** - High-performance data ingestion
- **Model Serving Infrastructure** - Transformer model inference at scale
- **Authentication & Authorization** - Tiered subscription access
- **Performance Optimization** - Caching and latency reduction

## Development Environment

### Kubernetes-First Development
**ALWAYS use Kubernetes for backend development:**

```bash
# Use dev CLI for all operations
run_dev query "SELECT COUNT(*) FROM dev_daily_prices"
run_dev job price-unification --symbols AAPL,MSFT
run_dev logs job-name
```

**❌ NEVER use kubectl directly**  
**✅ ALWAYS use dev CLI for backend operations**

### Database Connections
```bash
# Kubernetes (primary)
DB_HOST=postgres DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db

# Local development (secondary)
DB_HOST=localhost DB_PORT=5433 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=dev_db
```

## Backend Architecture

### Core Services
```
src/
├── api/                   # REST API endpoints
├── services/             # Business logic services
├── models/               # Data models and schemas
├── dao/                  # Database access layer
├── auth/                 # Authentication & authorization
├── cache/                # Redis caching layer
└── monitoring/           # Metrics and logging
```

### Key Technologies
- **FastAPI** - REST API framework
- **PostgreSQL + TimescaleDB** - Time-series data storage
- **Redis** - Caching and session storage
- **Ray** - Distributed computing for ML inference
- **Prometheus** - Metrics collection
- **Kubernetes** - Container orchestration

## Development Workflow

### 1. Test-Driven Development (TDD)
**EVERY backend change follows TDD:**

```bash
# 1. Write failing test first
touch tests/api/test_new_endpoint.py
PYTHONPATH=src pytest tests/api/test_new_endpoint.py -v
# ✅ Should FAIL - proves test works

# 2. Implement minimal code to pass
# (write your backend code)

# 3. Verify test passes
PYTHONPATH=src pytest tests/api/test_new_endpoint.py -v
# ✅ Should PASS

# 4. Run full test suite
PYTHONPATH=src pytest tests/ -v
```

### 2. Integration Testing
**Test actual service startup:**

```bash
# Test API endpoints work
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_backend_api_can_start -v

# Test database connectivity
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestRealWorldScenarios::test_database_connectivity -v

# Test external dependencies
curl -s "http://localhost:8000/health" | jq
```

### 3. Performance Testing
```bash
# Load test API endpoints
ab -n 1000 -c 10 http://localhost:8000/api/recommendations

# Monitor database performance
run_dev query "SELECT * FROM pg_stat_activity WHERE state = 'active'"

# Check cache hit rates
redis-cli info stats | grep keyspace
```

## API Design Patterns

### REST API Standards
```python
# FastAPI endpoint example
@app.get("/api/recommendations/{user_id}")
async def get_recommendations(
    user_id: int,
    subscription_tier: SubscriptionTier = Depends(get_user_tier),
    db: AsyncSession = Depends(get_db)
) -> RecommendationResponse:
    # Implementation
    pass
```

### Error Handling
```python
# Consistent error responses
@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "error": "validation_error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat()
        }
    )
```

### Rate Limiting
```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@app.get("/api/recommendations")
@limiter.limit("100/minute")
async def get_recommendations(request: Request):
    # Rate-limited endpoint
    pass
```

## Database Operations

### Environment-Aware Table Names
```python
from config.environment import Environment

env = Environment()
table_name = env.get_table_name("daily_prices")  # Returns "dev_daily_prices"
```

### Async Database Operations
```python
from sqlalchemy.ext.asyncio import AsyncSession

async def get_user_recommendations(db: AsyncSession, user_id: int):
    result = await db.execute(
        select(Recommendation)
        .where(Recommendation.user_id == user_id)
        .order_by(Recommendation.created_at.desc())
    )
    return result.scalars().all()
```

### Database Migrations
```bash
# Run migrations
uv run python src/db/migration_manager.py

# Check migration status
run_dev query "SELECT * FROM db_version ORDER BY id DESC LIMIT 1"
```

## Caching Strategy

### Redis Caching
```python
import redis.asyncio as redis

class CacheService:
    def __init__(self):
        self.redis = redis.from_url("redis://localhost:6379")
    
    async def get_recommendations(self, user_id: int) -> Optional[List[dict]]:
        cached = await self.redis.get(f"rec:{user_id}")
        return json.loads(cached) if cached else None
    
    async def cache_recommendations(self, user_id: int, recommendations: List[dict]):
        await self.redis.setex(
            f"rec:{user_id}", 
            3600,  # 1 hour TTL
            json.dumps(recommendations)
        )
```

### Cache Invalidation
```python
# Invalidate cache when new data arrives
async def on_new_market_data(symbol: str):
    pattern = f"rec:*:{symbol}:*"
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys)
```

## Model Serving Integration

### Ray Inference
```python
import ray
from ray import serve

@serve.deployment
class RecommendationModel:
    def __init__(self):
        self.model = load_model("recommendation_model.pkl")
    
    async def __call__(self, request: dict) -> dict:
        features = extract_features(request)
        prediction = self.model.predict(features)
        return {"recommendation": prediction.tolist()}

# Deploy model
model = RecommendationModel.bind()
serve.run(model, route_prefix="/model")
```

### Model Health Checks
```python
@app.get("/health/model")
async def model_health_check():
    try:
        # Test model inference
        test_input = generate_test_features()
        result = await call_model_service(test_input)
        return {"status": "healthy", "latency_ms": result.latency}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## Monitoring & Observability

### Prometheus Metrics
```python
from prometheus_client import Counter, Histogram, generate_latest

REQUEST_COUNT = Counter('api_requests_total', 'Total API requests', ['method', 'endpoint'])
REQUEST_LATENCY = Histogram('api_request_duration_seconds', 'Request latency')

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path
    ).inc()
    
    REQUEST_LATENCY.observe(time.time() - start_time)
    return response
```

### Logging
```python
import structlog

logger = structlog.get_logger()

@app.post("/api/recommendations")
async def create_recommendation(recommendation: RecommendationRequest):
    logger.info(
        "Creating recommendation",
        user_id=recommendation.user_id,
        symbols=recommendation.symbols,
        request_id=get_request_id()
    )
    # Implementation
```

## Security Best Practices

### JWT Authentication
```python
from jose import jwt, JWTError

async def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise credentials_exception
        return user_id
    except JWTError:
        raise credentials_exception
```

### Input Validation
```python
from pydantic import BaseModel, validator

class RecommendationRequest(BaseModel):
    symbols: List[str]
    timeframe: str
    risk_level: float
    
    @validator('symbols')
    def validate_symbols(cls, v):
        if len(v) > 50:
            raise ValueError('Too many symbols requested')
        return [s.upper() for s in v if s.isalpha()]
```

## Prompt Template for Backend Tasks

```
As a Backend Engineer for our portfolio GPT platform, help me [task]. Consider:
- Scalability requirements for handling [number] concurrent forecast requests
- Real-time processing architecture for [data source] market data  
- Model serving infrastructure for [model type] transformer inference
- API design for [subscription tier] recommendation delivery
- Authentication and rate limiting for [user type] access patterns
- Caching strategies for optimizing [forecast type] generation
- Integration patterns with [brokerage platform] for trade execution
- Monitoring and alerting for [performance metric] thresholds
```

## Common Backend Tasks

### Deploy New API Endpoint
```bash
# 1. Write failing test
touch tests/api/test_new_endpoint.py

# 2. Implement endpoint
# (edit src/api/endpoints/recommendations.py)

# 3. Test locally
PYTHONPATH=src pytest tests/api/test_new_endpoint.py -v

# 4. Deploy to K8s
kubectl apply -f k8s/api-deployment.yaml

# 5. Verify in cluster
curl -s "http://external-ip:port/api/new-endpoint" | jq
```

### Debug Performance Issue
```bash
# Check API metrics
curl -s "http://localhost:8000/metrics" | grep api_request_duration

# Monitor database queries
run_dev query "SELECT query, calls, total_time FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10"

# Check Redis performance
redis-cli info stats | grep ops_per_sec
```

### Scale Model Serving
```bash
# Check current model performance
run_dev query "SELECT AVG(inference_time_ms) FROM model_metrics WHERE timestamp > NOW() - INTERVAL '1 hour'"

# Scale Ray cluster
kubectl scale deployment ray-workers --replicas=5

# Monitor scaling
kubectl logs deployment/ray-head | grep "scaling"
```

---

*This guide focuses on backend-specific concerns. For broader development practices, see [Development Workflow](../development/DEVELOPMENT_WORKFLOW.md).*