# Market Data Management (`src/market_data/`)

This directory contains the comprehensive market data ingestion, processing, and management system for ATS-GenAI.

## Overview

The market data system handles multi-vendor data ingestion, real-time processing, historical backfills, reconciliation, and storage across various asset classes and timeframes.

## Directory Structure

```
market_data/
├── agent/                      # Real-time data agents (20+ files)
│   ├── data_agent_orchestrator.py    # Agent coordination
│   ├── instrument_data_agent.py      # Instrument-specific agents
│   ├── polygon_adapter.py            # Polygon real-time adapter
│   ├── tiingo_adapter.py             # Tiingo real-time adapter
│   ├── reconciliation.py             # Real-time reconciliation
│   ├── monitoring/                   # Agent monitoring dashboards
│   └── k8s/                         # Kubernetes deployment configs
├── eod/                        # End-of-day processing (15+ files)
│   ├── daily_price_polygon.py        # Polygon daily prices
│   ├── daily_price_tiingo.py         # Tiingo daily prices
│   ├── unify_daily_prices.py         # Cross-vendor unification
│   ├── turbo_price_backfill.py       # Fast historical backfill
│   └── unified_db_daily_price_market_data_manager.py
├── backfill/                   # Historical data backfill
│   └── unified_backfill_orchestrator.py
├── reconciliation/             # Data reconciliation
│   └── cross_vendor_reconciler.py
├── ingestion/                  # Data ingestion pipelines
│   └── minute_data_pipeline.py
├── news/                       # News data (should be moved)
│   └── turbo_news_backfill.py
├── utils/                      # Market data utilities
│   └── calculate_adjusted_prices.py
├── market_data.py              # Core market data models
├── market_data_manager.py      # Main manager class
├── market_data_simulator.py    # Data simulation for testing
├── model.py                    # Data models
└── signals.py                  # Signal processing (should move to signals/)
```

## ⚠️ **Current Issues & Suggested Refactoring**

### **Problem: Mixed Responsibilities**
The directory contains components with different responsibilities that should be better organized:

```python
# Current problematic mixing:
├── agent/           # Real-time processing
├── eod/             # Batch processing  
├── backfill/        # Historical processing
├── news/            # News data (different domain)
├── signals.py       # Signal processing (belongs in signals/)
├── reconciliation/  # Data quality (could be separate)
```

### **Suggested Refactoring**
```python
market_data/
├── core/
│   ├── market_data_manager.py      # Central coordination
│   ├── data_models.py              # Core data models
│   └── config.py                   # Market data configuration
├── ingestion/
│   ├── real_time/                  # Move agent/ here
│   │   ├── orchestrator.py
│   │   ├── instrument_agent.py
│   │   └── adapters/
│   │       ├── polygon_adapter.py
│   │       ├── tiingo_adapter.py
│   │       └── base_adapter.py
│   ├── batch/                      # Move eod/ here
│   │   ├── daily_processor.py
│   │   ├── price_unifier.py
│   │   └── managers/
│   └── backfill/                   # Historical data
│       ├── backfill_orchestrator.py
│       └── turbo_backfill.py
├── processing/
│   ├── reconciliation/             # Data reconciliation
│   │   ├── cross_vendor_reconciler.py
│   │   └── data_quality_checker.py
│   ├── validation/                 # Data validation
│   └── transformation/             # Data transformation
├── vendors/                        # Vendor-specific implementations
│   ├── polygon/
│   │   ├── polygon_client.py
│   │   ├── polygon_daily.py
│   │   └── polygon_real_time.py
│   ├── tiingo/
│   │   ├── tiingo_client.py
│   │   ├── tiingo_daily.py
│   │   └── tiingo_real_time.py
│   └── base/
│       ├── base_vendor.py
│       └── vendor_factory.py
├── storage/
│   ├── storage_manager.py
│   └── compression/
└── utils/
    ├── price_adjustments.py
    ├── data_simulator.py
    └── market_calendar.py
```

## Core Components

### 🔄 **Real-Time Data Agents** (`agent/`)

The agent system provides real-time market data processing with high throughput and low latency:

```python
from market_data.agent.instrument_data_agent import InstrumentDataAgent
from market_data.agent.polygon_adapter import PolygonAdapter

# Create real-time data agent
agent = InstrumentDataAgent(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    adapters=[PolygonAdapter(), TiingoAdapter()]
)

# Start real-time processing
agent.start_streaming()
```

**Key Features:**
- **Multi-vendor support**: Polygon, Tiingo, Alpha Vantage
- **Instrument-specific agents**: Dedicated agents per symbol
- **Automatic reconciliation**: Real-time data quality checks
- **Kubernetes deployment**: Scalable container deployment
- **Monitoring dashboards**: Grafana dashboards for system health

### 📊 **End-of-Day Processing** (`eod/`)

Batch processing system for daily market data:

```python
from market_data.eod.daily_price_polygon import DailyPricePolygon
from market_data.eod.unify_daily_prices import UnifyDailyPrices

# Polygon daily data
polygon_processor = DailyPricePolygon()
polygon_data = polygon_processor.fetch_daily_prices(['AAPL'], '2024-01-01', '2024-01-31')

# Unify across vendors
unifier = UnifyDailyPrices()
unified_data = unifier.unify_daily_prices(['AAPL'], '2024-01-01', '2024-01-31')
```

**Key Features:**
- **Multi-vendor unification**: Combine data from multiple sources
- **Data quality validation**: Automatic anomaly detection
- **Turbo backfill**: Fast historical data loading
- **Adjusted prices**: Corporate action adjustments
- **Database integration**: Direct storage to TimescaleDB

### ⚡ **Historical Backfill** (`backfill/`)

Efficient historical data loading for new instruments or date ranges:

```python
from market_data.backfill.unified_backfill_orchestrator import UnifiedBackfillOrchestrator

# Large-scale historical backfill
orchestrator = UnifiedBackfillOrchestrator()
orchestrator.run_unified_5year_backfill(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    start_date='2019-01-01',
    end_date='2024-01-01',
    chunk_days=30,
    batch_size=100
)
```

**Key Features:**
- **Chunked processing**: Memory-efficient large dataset handling
- **Progress tracking**: Resumable backfill operations
- **Multi-vendor coordination**: Simultaneous vendor data loading
- **Error handling**: Robust error recovery and retry logic

### 🔍 **Data Reconciliation** (`reconciliation/`)

Cross-vendor data quality and reconciliation:

```python
from market_data.reconciliation.cross_vendor_reconciler import CrossVendorReconciler

reconciler = CrossVendorReconciler()

# Reconcile price data across vendors
reconciliation_report = reconciler.reconcile_daily_prices(
    symbol='AAPL',
    date='2024-01-15',
    vendors=['polygon', 'tiingo']
)

# Check for anomalies
if reconciliation_report.has_anomalies():
    print(f"Price discrepancies: {reconciliation_report.discrepancies}")
```

## Supported Data Vendors

### 📡 **Primary Vendors**

#### **Polygon.io** - Primary Market Data
```python
# Real-time and historical market data
- Stock prices (minute, daily)
- Options data
- Forex data  
- Crypto data
- Corporate actions
- News and fundamentals
```

#### **Tiingo** - Alternative Data Source
```python
# Independent data validation source
- Daily stock prices
- Intraday data
- International markets
- Crypto data
- News sentiment
```

#### **Alpha Vantage** - Economic Data
```python
# Economic indicators and fundamental data
- Economic indicators
- Company fundamentals
- Technical indicators
- Currency exchange rates
```

### 🔌 **Easy Vendor Integration**

Adding new vendors follows a standard pattern:

```python
# 1. Create vendor adapter
class NewVendorAdapter(BaseVendorAdapter):
    def fetch_daily_prices(self, symbols, start_date, end_date):
        # Implement vendor-specific logic
        pass
    
    def fetch_real_time_data(self, symbols):
        # Implement real-time data fetching
        pass

# 2. Register with system
vendor_factory.register_vendor('new_vendor', NewVendorAdapter)

# 3. Use in data agents
agent = InstrumentDataAgent(
    symbols=['AAPL'],
    adapters=[NewVendorAdapter()]
)
```

## Data Types & Coverage

### 📈 **Market Data Types**
- **Equity Data**: Stocks, ETFs, indices
- **Options Data**: Option chains and Greeks
- **Fixed Income**: Bond prices and yields
- **Forex Data**: Currency exchange rates
- **Crypto Data**: Cryptocurrency prices
- **Futures Data**: Commodity and financial futures

### ⏱️ **Timeframe Support**
- **Real-time**: Sub-second market data
- **Minute Data**: 1-minute, 5-minute, 15-minute bars
- **Hourly Data**: 1-hour aggregations
- **Daily Data**: End-of-day prices with adjustments
- **Historical**: Multi-year historical datasets

### 📊 **Data Quality Features**
- **Outlier Detection**: Statistical anomaly detection
- **Missing Data Handling**: Gap identification and filling
- **Corporate Action Adjustments**: Splits, dividends, mergers
- **Cross-Vendor Validation**: Data consistency checks
- **Real-time Monitoring**: Data quality dashboards

## Performance & Scalability

### ⚡ **High-Performance Features**
```python
# Parallel processing
from concurrent.futures import ThreadPoolExecutor

def parallel_data_fetch(symbols, date_range):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for symbol in symbols:
            future = executor.submit(fetch_daily_prices, symbol, date_range)
            futures.append(future)
        
        results = [future.result() for future in futures]
    return results

# Async processing for real-time data
async def process_real_time_stream():
    async for data_point in real_time_stream:
        await process_data_point(data_point)
```

### 📈 **Scalability Patterns**
- **Microservice Architecture**: Independent scaling per component
- **Event-Driven Processing**: Asynchronous event handling
- **Database Partitioning**: Time-based table partitioning
- **Caching Layers**: Redis caching for frequently accessed data
- **Load Balancing**: Horizontal scaling of data agents

## Monitoring & Observability

### 📊 **Built-in Monitoring**
```python
# Performance metrics
market_data_latency_histogram
market_data_throughput_counter
data_quality_score_gauge
vendor_availability_gauge

# Health checks
@app.get("/health/market_data")
async def market_data_health():
    return {
        "polygon_status": await polygon_adapter.health_check(),
        "tiingo_status": await tiingo_adapter.health_check(),
        "data_quality_score": data_quality_monitor.get_score(),
        "last_update": last_data_timestamp
    }
```

### 🚨 **Alerting System**
- **Data Latency Alerts**: When data is delayed beyond thresholds
- **Quality Degradation**: When data quality scores drop
- **Vendor Failures**: When vendor APIs become unavailable
- **Processing Errors**: When data processing fails

## Configuration

### ⚙️ **Environment Configuration**
```python
# .env.dev
POLYGON_API_KEY=your_polygon_key
TIINGO_API_KEY=your_tiingo_key
ALPHA_VANTAGE_API_KEY=your_av_key

# Market data settings
MARKET_DATA_BATCH_SIZE=1000
MARKET_DATA_RETRY_ATTEMPTS=3
MARKET_DATA_TIMEOUT_SECONDS=30

# Real-time settings
REAL_TIME_BUFFER_SIZE=10000
REAL_TIME_FLUSH_INTERVAL_MS=100
```

### 🎛️ **Dynamic Configuration**
```python
from market_data.core.config import MarketDataConfig

config = MarketDataConfig()

# Runtime configuration updates
config.update_batch_size(2000)
config.set_vendor_priority(['polygon', 'tiingo', 'alpha_vantage'])
config.enable_real_time_reconciliation(True)
```

## Usage Examples

### **Basic Daily Data Fetching**
```python
from market_data.core.market_data_manager import MarketDataManager

manager = MarketDataManager()

# Fetch daily prices
prices = manager.get_daily_prices(
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    vendor='polygon'  # Optional: specify vendor
)

# Get unified data (best available from all vendors)
unified_prices = manager.get_unified_daily_prices(
    symbols=['AAPL'],
    start_date='2024-01-01',
    end_date='2024-01-31'
)
```

### **Real-Time Data Streaming**
```python
from market_data.agent.data_agent_orchestrator import DataAgentOrchestrator

# Start real-time data streaming
orchestrator = DataAgentOrchestrator()

# Subscribe to real-time data
def handle_real_time_data(data):
    print(f"Received: {data.symbol} @ {data.price}")

orchestrator.subscribe(['AAPL', 'MSFT'], handle_real_time_data)
orchestrator.start()
```

### **Historical Backfill**
```python
from market_data.backfill.unified_backfill_orchestrator import UnifiedBackfillOrchestrator

orchestrator = UnifiedBackfillOrchestrator()

# Backfill 5 years of data
orchestrator.run_unified_5year_backfill(
    symbols=['AAPL', 'MSFT'],
    start_date='2019-01-01',
    end_date='2024-01-01',
    chunk_days=30
)
```

## Testing

### **Unit Tests**
```python
# Test market data manager
def test_daily_prices_fetch():
    manager = MarketDataManager()
    prices = manager.get_daily_prices(['AAPL'], '2024-01-01', '2024-01-31')
    assert len(prices) > 0
    assert all(p.symbol == 'AAPL' for p in prices)

# Test vendor adapters
def test_polygon_adapter():
    adapter = PolygonAdapter()
    data = adapter.fetch_daily_prices(['AAPL'], '2024-01-01', '2024-01-01')
    assert data is not None
```

### **Integration Tests**
```python
# Test cross-vendor reconciliation
def test_cross_vendor_reconciliation():
    reconciler = CrossVendorReconciler()
    report = reconciler.reconcile_daily_prices('AAPL', '2024-01-15')
    assert report.is_valid()
```

## Best Practices

### **Data Handling**
1. **Always validate data**: Check for anomalies and missing values
2. **Use multiple vendors**: Don't rely on single data source
3. **Handle corporate actions**: Adjust prices for splits and dividends
4. **Monitor data quality**: Track quality metrics continuously
5. **Cache frequently accessed data**: Improve performance with caching

### **Error Handling**
1. **Implement retries**: Handle temporary vendor failures
2. **Graceful degradation**: Fall back to alternative vendors
3. **Log extensively**: Comprehensive logging for debugging
4. **Monitor failures**: Alert on systematic failures
5. **Validate inputs**: Check all input parameters

### **Performance Optimization**
1. **Batch operations**: Process data in batches for efficiency
2. **Parallel processing**: Use concurrency for independent operations
3. **Database optimization**: Use proper indexing and partitioning
4. **Connection pooling**: Reuse database connections
5. **Memory management**: Handle large datasets efficiently

---

**🚨 Note**: This directory would benefit from the suggested refactoring to improve organization and reduce complexity. The current structure mixes different concerns that should be better separated.