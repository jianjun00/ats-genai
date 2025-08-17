# Portfolio GPT Recommendation Engine Design

## Core Architecture

### Overview
Build an hourly recommendation engine that generates buy/hold/sell recommendations for 50 select stocks using the existing transformer forecasting capabilities.

### Design Principles
1. **Leverage existing infrastructure** - Use current market data pipeline and transformer models
2. **Scalable processing** - Ray-based parallel forecasting for 50+ stocks
3. **Real-time ready** - Hourly forecast generation during market hours
4. **Clear recommendations** - Simple buy/hold/sell with confidence scores
5. **Performance focused** - <5 minute SLA for forecast generation

## System Components

### 1. Forecast Engine
**Purpose**: Generate price predictions using existing transformer models
**Input**: Historical market data for target stocks
**Output**: 1, 3, and 5-day price forecasts with confidence intervals

```python
# Leverage existing: src/modeling/forecast_with_transformer.py
class HourlyForecastEngine:
    def generate_forecasts(symbols: List[str]) -> List[ForecastResult]
    def update_model_with_latest_data() -> bool
```

### 2. Recommendation Logic
**Purpose**: Convert price forecasts into actionable buy/hold/sell recommendations
**Input**: Price forecasts and confidence scores
**Output**: Recommendation with reasoning and confidence

```python
class RecommendationEngine:
    def generate_recommendation(forecast: ForecastResult) -> Recommendation
    def calculate_confidence_score(forecast: ForecastResult) -> float
```

### 3. Data Pipeline
**Purpose**: Orchestrate hourly data refresh and forecast generation
**Input**: Market data from existing sources (Polygon, etc.)
**Output**: Updated forecasts stored in database

```python
class HourlyPipeline:
    def run_hourly_forecasts() -> PipelineResult
    def refresh_market_data() -> bool
    def store_forecasts(forecasts: List[ForecastResult]) -> bool
```

## Database Schema

### Forecasts Table
```sql
CREATE TABLE {env}_forecasts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    forecast_timestamp TIMESTAMP NOT NULL,
    horizon_days INTEGER NOT NULL, -- 1, 3, or 5 days
    current_price DECIMAL(10,4) NOT NULL,
    predicted_price DECIMAL(10,4) NOT NULL,
    confidence_score DECIMAL(5,2) NOT NULL, -- 0-100
    prediction_interval_lower DECIMAL(10,4), -- Lower bound
    prediction_interval_upper DECIMAL(10,4), -- Upper bound
    model_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Recommendations Table
```sql
CREATE TABLE {env}_recommendations (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    recommendation VARCHAR(10) NOT NULL, -- 'buy', 'hold', 'sell'
    confidence_score DECIMAL(5,2) NOT NULL,
    current_price DECIMAL(10,4) NOT NULL,
    target_price_1d DECIMAL(10,4),
    target_price_3d DECIMAL(10,4),
    target_price_5d DECIMAL(10,4),
    expected_return_pct DECIMAL(8,4), -- Expected return percentage
    reasoning TEXT,
    risk_level VARCHAR(10), -- 'low', 'medium', 'high'
    valid_until TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Recommendation Logic

### Buy/Hold/Sell Criteria
```python
def generate_recommendation(forecast_1d, forecast_3d, forecast_5d, confidence):
    expected_return_5d = (forecast_5d.predicted_price - current_price) / current_price
    
    if confidence >= 70:  # High confidence threshold
        if expected_return_5d > 0.05:  # >5% return
            return "buy"
        elif expected_return_5d < -0.03:  # <-3% return
            return "sell"
        else:
            return "hold"
    elif confidence >= 60:  # Medium confidence
        if expected_return_5d > 0.08:  # Higher threshold for medium confidence
            return "buy"
        elif expected_return_5d < -0.05:
            return "sell"
        else:
            return "hold"
    else:
        return "hold"  # Low confidence = hold
```

### Confidence Scoring
```python
def calculate_confidence_score(forecast_result):
    """Calculate confidence based on multiple factors"""
    factors = {
        'prediction_interval_width': 0.3,  # Narrower intervals = higher confidence
        'historical_accuracy': 0.4,        # Model's past performance
        'market_volatility': 0.2,          # Current market conditions
        'data_quality': 0.1                # Completeness of input data
    }
    
    # Weighted combination of confidence factors
    return min(100, sum(factor_score * weight for factor_score, weight in factors.items()))
```

## Implementation Plan

### Phase 1: Core Engine (Week 1)
- [ ] Extend existing transformer model for hourly inference
- [ ] Create recommendation logic with buy/hold/sell rules
- [ ] Database schema for forecasts and recommendations
- [ ] Basic API endpoints for recommendations

### Phase 2: Pipeline Integration (Week 2)
- [ ] Hourly scheduled job for forecast generation
- [ ] Integration with existing market data pipeline
- [ ] Performance optimization for 50-stock processing
- [ ] Error handling and monitoring

### Phase 3: Advanced Features (Week 3-4)
- [ ] Confidence interval calculations
- [ ] Risk level assessments
- [ ] Recommendation reasoning generation
- [ ] Historical accuracy tracking

## Target Stock Universe (MVP)

### Large Cap Tech (20 stocks)
AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, NFLX, ADBE, CRM, ORCL, INTC, AMD, PYPL, UBER, SNAP, TWTR, ZOOM, SHOP, SQ

### Large Cap Traditional (15 stocks)
JPM, BAC, WMT, JNJ, PG, KO, PFE, XOM, CVX, UNH, HD, V, MA, DIS, MCD

### Growth/Volatile (15 stocks)
GME, AMC, PLTR, COIN, HOOD, RBLX, DKNG, SPCE, NKLA, LCID, RIVN, F, GM, NIO, BABA

Total: 50 stocks for MVP

## API Design

### Core Endpoints
```
GET /api/v1/recommendations                    # All current recommendations
GET /api/v1/recommendations/{symbol}           # Specific stock recommendation
GET /api/v1/forecasts/{symbol}                # Detailed forecast data
GET /api/v1/universe                          # List of supported stocks
```

### Response Format
```json
{
  "symbol": "AAPL",
  "recommendation": "buy",
  "confidence_score": 78.5,
  "current_price": 185.25,
  "forecasts": {
    "1_day": {"price": 187.50, "return_pct": 1.22},
    "3_day": {"price": 192.10, "return_pct": 3.70},
    "5_day": {"price": 195.75, "return_pct": 5.67}
  },
  "reasoning": "Strong technical momentum with positive earnings catalyst",
  "risk_level": "medium",
  "valid_until": "2025-01-16T16:00:00Z",
  "last_updated": "2025-01-16T10:00:00Z"
}
```

## Performance Requirements

### Forecast Generation SLA
- **Target**: <5 minutes from market data update to recommendation availability
- **Scalability**: Process 50 stocks in parallel using Ray
- **Frequency**: Every hour during market hours (9:30 AM - 4:00 PM ET)

### API Performance
- **Response Time**: <200ms for recommendation queries
- **Throughput**: 1000+ requests/minute
- **Availability**: 99.5% uptime during market hours

## Risk Management

### Model Risk
- **Validation**: Continuous backtesting against historical data
- **Accuracy Tracking**: Monitor recommendation success rates
- **Fallback**: Revert to previous model if accuracy drops >10%

### Operational Risk
- **Data Quality**: Validate input data completeness
- **Processing Failures**: Retry mechanisms and error alerts
- **Market Conditions**: Adjust confidence thresholds during high volatility

This design leverages existing infrastructure while building a robust, scalable recommendation engine focused on delivering clear value to users.