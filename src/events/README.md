# Event System & Ingestion (`src/events/`)

This directory contains the unified event system that ingests, processes, and manages events from multiple data sources for the ATS-GenAI trading platform.

## Overview

The event system provides:
- **Multi-Source Event Ingestion** from 10+ external data providers
- **Unified Event Pipeline** with automatic reconciliation and deduplication
- **Real-Time Event Processing** with async processing and queuing
- **Event Type Management** covering earnings, economic data, corporate actions
- **API Layer** for event querying and external integration
- **Database Integration** with efficient storage and retrieval

## Directory Structure

```
events/
├── api.py                  # REST API endpoints for event access
├── db.py                   # Database operations and event storage
├── schemas.py              # Event data schemas and validation
├── ingest/                 # Event ingestion from external sources
│   ├── unified_pipeline.py        # Main ingestion orchestrator
│   ├── polygon_earnings.py        # Polygon earnings events
│   ├── polygon_economic_calendar.py # Polygon economic events
│   ├── polygon_corporate_actions.py # Corporate actions from Polygon
│   ├── polygon_news.py            # News events from Polygon
│   ├── polygon_market_data.py     # Market data events
│   ├── finnhub_earnings.py        # Finnhub earnings data
│   ├── fmp_earnings.py            # Financial Modeling Prep earnings
│   ├── iex_earnings.py            # IEX Cloud earnings
│   ├── yahoo_earnings.py          # Yahoo Finance earnings
│   ├── investing_earnings.py      # Investing.com earnings
│   └── quandl_earnings.py         # Quandl earnings data
└── __init__.py
```

## Core Components

### 🔄 **Unified Event Pipeline** (`ingest/unified_pipeline.py`)

Central orchestrator for multi-source event ingestion:

```python
from events.ingest.unified_pipeline import UnifiedEventPipeline, PipelineConfig

# Configure ingestion pipeline
config = PipelineConfig(
    sources=['polygon', 'finnhub', 'fmp', 'iex'],
    event_types=['earnings', 'economic', 'corporate_actions'],
    reconciliation_enabled=True,
    deduplication_enabled=True,
    batch_size=1000,
    max_concurrent=10
)

# Initialize pipeline
pipeline = UnifiedEventPipeline(config)

# Run complete ingestion
results = await pipeline.run_full_ingestion(
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Real-time ingestion
await pipeline.start_real_time_ingestion()

# Process ingestion results
for source, result in results.items():
    print(f"{source}: {result.events_processed} events, {result.errors} errors")
```

**Key Features:**
- **Multi-Source Coordination**: Orchestrates ingestion from 10+ sources
- **Reconciliation Engine**: Automatically reconciles conflicting data
- **Deduplication**: Eliminates duplicate events across sources
- **Error Handling**: Robust error handling with retry logic
- **Progress Tracking**: Real-time ingestion progress monitoring
- **Async Processing**: High-performance async processing

### 📊 **Event API Layer** (`api.py`)

REST API endpoints for event access and management:

```python
from events.api import EventAPI

# Initialize API
api = EventAPI()

# Get earnings events
earnings = await api.get_earnings_events(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Get economic events
economic_events = await api.get_economic_events(
    countries=['US', 'EU'],
    importance=['high', 'medium'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Get corporate actions
corporate_actions = await api.get_corporate_actions(
    symbols=['AAPL'],
    action_types=['split', 'dividend'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Add new event
new_event = await api.add_event({
    'event_type': 'earnings',
    'symbol': 'AAPL',
    'event_date': '2024-02-01T16:30:00Z',
    'data': {
        'eps_estimate': 2.10,
        'revenue_estimate': 118000000000,
        'source': 'manual_entry'
    }
})
```

**Key Features:**
- **RESTful Interface**: Standard REST API for event access
- **Flexible Filtering**: Filter by symbol, date, type, importance
- **Bulk Operations**: Efficient bulk event retrieval
- **Event Creation**: API for adding custom events
- **Data Validation**: Automatic schema validation
- **Rate Limiting**: Built-in rate limiting and throttling

### 🗄️ **Database Integration** (`db.py`)

Efficient event storage and retrieval with optimization:

```python
from events.db import EventDatabase, EventQuery

# Initialize database connection
db = EventDatabase()

# Store events efficiently
await db.bulk_insert_events(events_list)

# Complex event queries
query = EventQuery()
results = await db.query_events(
    query.symbols(['AAPL', 'MSFT'])
         .event_types(['earnings', 'guidance'])
         .date_range('2024-01-01', '2024-12-31')
         .order_by('event_date')
         .limit(1000)
)

# Event aggregation
stats = await db.get_event_statistics(
    symbols=['AAPL'],
    timeframe='quarterly'
)

# Real-time event streaming
async for event in db.stream_events(
    event_types=['earnings'],
    real_time=True
):
    await process_event(event)
```

**Key Features:**
- **Optimized Storage**: Efficient storage with proper indexing
- **Complex Queries**: Advanced querying with builder pattern
- **Streaming Support**: Real-time event streaming
- **Aggregation**: Built-in event statistics and aggregation
- **Performance**: Optimized for high-volume event processing
- **ACID Compliance**: Transactional consistency for event data

### 📋 **Event Schemas** (`schemas.py`)

Standardized event data schemas and validation:

```python
from events.schemas import EarningsEvent, EconomicEvent, CorporateActionEvent

# Earnings event schema
earnings = EarningsEvent(
    symbol='AAPL',
    event_date='2024-02-01T16:30:00Z',
    event_type='earnings_release',
    fiscal_quarter='Q1',
    fiscal_year=2024,
    eps_estimate=2.10,
    eps_actual=2.15,
    revenue_estimate=118000000000,
    revenue_actual=119500000000,
    guidance_raised=True,
    conference_call_time='2024-02-01T17:00:00Z'
)

# Economic event schema
economic = EconomicEvent(
    event_name='Non-Farm Payrolls',
    event_date='2024-02-02T08:30:00Z',
    country='US',
    importance='high',
    category='employment',
    forecast_value=200000,
    actual_value=215000,
    previous_value=190000,
    impact='positive'
)

# Corporate action schema
corporate_action = CorporateActionEvent(
    symbol='AAPL',
    action_type='stock_split',
    announcement_date='2024-01-15',
    effective_date='2024-02-01',
    split_ratio=2.0,
    description='2-for-1 stock split'
)

# Validation
earnings.validate()  # Raises ValidationError if invalid
```

**Key Features:**
- **Type Safety**: Strongly typed event schemas
- **Validation**: Automatic data validation and sanitization
- **Standardization**: Consistent data format across all sources
- **Extensibility**: Easy to add new event types
- **Documentation**: Self-documenting schema with field descriptions
- **Serialization**: JSON serialization support

## Event Sources

### 📡 **Primary Data Sources**

#### **Polygon.io** - Comprehensive Market Data
```python
from events.ingest.polygon_earnings import PolygonEarningsIngestor
from events.ingest.polygon_economic_calendar import PolygonEconomicIngestor

# Polygon earnings ingestion
polygon_earnings = PolygonEarningsIngestor()
earnings_events = await polygon_earnings.fetch_earnings(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Polygon economic calendar
polygon_economic = PolygonEconomicIngestor()
economic_events = await polygon_economic.fetch_economic_events(
    importance=['high'],
    countries=['US']
)
```

#### **Finnhub** - Financial Data
```python
from events.ingest.finnhub_earnings import FinnhubEarningsIngestor

finnhub = FinnhubEarningsIngestor()
earnings = await finnhub.fetch_earnings_calendar(
    start_date='2024-01-01',
    end_date='2024-12-31'
)
```

#### **Financial Modeling Prep (FMP)** - Fundamental Data
```python
from events.ingest.fmp_earnings import FMPEarningsIngestor

fmp = FMPEarningsIngestor()
earnings = await fmp.fetch_earnings_calendar(
    symbols=['AAPL', 'MSFT'],
    quarter='Q1',
    year=2024
)
```

### 🔌 **Easy Source Integration**

Adding new event sources follows a standard pattern:

```python
# 1. Create source adapter
class NewSourceEarningsIngestor(BaseEarningsIngestor):
    async def fetch_earnings(self, symbols, start_date, end_date):
        # Implement source-specific logic
        raw_data = await self.client.get_earnings(symbols, start_date, end_date)
        
        # Transform to standard schema
        events = [self.transform_to_schema(item) for item in raw_data]
        
        return events
    
    def transform_to_schema(self, raw_event):
        return EarningsEvent(
            symbol=raw_event['ticker'],
            event_date=raw_event['date'],
            eps_estimate=raw_event['eps_est'],
            # ... map other fields
        )

# 2. Register with pipeline
pipeline.register_source('new_source', NewSourceEarningsIngestor())

# 3. Use in unified ingestion
config.sources.append('new_source')
```

## Event Types & Coverage

### 📈 **Earnings Events**
- **Earnings Releases**: EPS, revenue, guidance
- **Conference Calls**: Timing and dial-in information
- **Earnings Surprises**: Actual vs. estimate analysis
- **Guidance Updates**: Forward-looking guidance changes
- **Analyst Updates**: Post-earnings analyst revisions

### 🏛️ **Economic Events**
- **Employment Data**: Non-farm payrolls, unemployment rate
- **Inflation Data**: CPI, PPI, core inflation measures
- **GDP Data**: GDP growth, GDP deflator
- **Federal Reserve**: FOMC meetings, interest rate decisions
- **International**: ECB, BOJ, BOE policy decisions

### 💼 **Corporate Actions**
- **Stock Splits**: Split ratios and effective dates
- **Dividends**: Regular and special dividend payments
- **Mergers & Acquisitions**: M&A announcements and closings
- **Spin-offs**: Corporate spin-off events
- **Name Changes**: Ticker and company name changes

### 📰 **News Events**
- **Breaking News**: Market-moving news in real-time
- **Analyst Reports**: Buy/sell/hold recommendations
- **Regulatory Filings**: SEC filings and regulatory updates
- **Management Changes**: CEO, CFO, and board changes
- **Product Launches**: New product announcements

## Real-Time Processing

### ⚡ **Async Event Processing**
```python
import asyncio
from events.ingest.unified_pipeline import RealTimeProcessor

async def real_time_event_processing():
    processor = RealTimeProcessor()
    
    # Start multiple source streams
    tasks = [
        processor.start_polygon_stream(),
        processor.start_finnhub_stream(),
        processor.start_fmp_stream()
    ]
    
    # Process events as they arrive
    async def event_handler(event):
        # Validate event
        validated_event = await processor.validate_event(event)
        
        # Store in database
        await processor.store_event(validated_event)
        
        # Trigger downstream processing
        await processor.trigger_portfolio_update(validated_event)
        
        # Send alerts if necessary
        if validated_event.importance == 'high':
            await processor.send_alert(validated_event)
    
    # Run all streams concurrently
    await asyncio.gather(*tasks)

# Start real-time processing
asyncio.run(real_time_event_processing())
```

### 🔄 **Event Queue Management**
```python
from events.processing import EventQueue, EventProcessor

# High-throughput event queue
queue = EventQueue(
    max_size=10000,
    batch_size=100,
    flush_interval=1.0  # seconds
)

# Event processor with automatic batching
processor = EventProcessor(queue)

# Add events to queue
await queue.put(earnings_event)
await queue.put(economic_event)

# Process events in batches
await processor.start_batch_processing()
```

## Data Reconciliation

### 🔍 **Multi-Source Reconciliation**
```python
from events.reconciliation import EventReconciler, ReconciliationRules

# Configure reconciliation rules
rules = ReconciliationRules(
    primary_source='polygon',
    fallback_sources=['finnhub', 'fmp'],
    confidence_weights={
        'polygon': 0.5,
        'finnhub': 0.3,
        'fmp': 0.2
    },
    reconciliation_threshold=0.1  # 10% tolerance
)

# Reconcile earnings data
reconciler = EventReconciler(rules)
reconciled_events = await reconciler.reconcile_earnings(
    events_by_source={
        'polygon': polygon_earnings,
        'finnhub': finnhub_earnings,
        'fmp': fmp_earnings
    }
)

# Handle conflicts
conflicts = reconciler.get_conflicts()
for conflict in conflicts:
    print(f"Conflict in {conflict.field}: {conflict.values}")
    resolved = await reconciler.resolve_conflict(conflict)
    print(f"Resolved to: {resolved}")
```

### 📊 **Data Quality Metrics**
```python
from events.quality import DataQualityAnalyzer

analyzer = DataQualityAnalyzer()

# Analyze data quality by source
quality_report = await analyzer.analyze_source_quality(
    source='polygon',
    event_types=['earnings'],
    timeframe='2024-01-01:2024-12-31'
)

print(f"Completeness: {quality_report.completeness:.1%}")
print(f"Accuracy: {quality_report.accuracy:.1%}")
print(f"Timeliness: {quality_report.timeliness:.1%}")
print(f"Consistency: {quality_report.consistency:.1%}")

# Cross-source comparison
comparison = await analyzer.compare_sources(['polygon', 'finnhub'])
```

## Performance & Optimization

### ⚡ **High-Performance Ingestion**
```python
# Parallel ingestion from multiple sources
async def parallel_ingestion():
    sources = ['polygon', 'finnhub', 'fmp', 'iex']
    
    # Create tasks for each source
    tasks = []
    for source in sources:
        ingestor = get_ingestor(source)
        task = asyncio.create_task(
            ingestor.fetch_earnings('2024-01-01', '2024-12-31')
        )
        tasks.append(task)
    
    # Wait for all sources to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    all_events = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Source {sources[i]} failed: {result}")
        else:
            all_events.extend(result)
    
    return all_events

# Batch processing optimization
async def batch_event_processing(events, batch_size=1000):
    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        await process_event_batch(batch)
```

### 📏 **Scalability Features**
- **Horizontal Scaling**: Process events across multiple workers
- **Database Partitioning**: Partition events by date and type
- **Caching**: Cache frequently accessed events
- **Compression**: Compress historical event data
- **Archival**: Archive old events to cold storage

## Monitoring & Alerting

### 📊 **Event System Monitoring**
```python
from events.monitoring import EventMonitor

monitor = EventMonitor()

# Real-time metrics
metrics = await monitor.get_real_time_metrics()
print(f"Events per second: {metrics.events_per_second}")
print(f"Processing latency: {metrics.avg_processing_latency_ms}ms")
print(f"Error rate: {metrics.error_rate:.2%}")

# Source availability monitoring
availability = await monitor.check_source_availability()
for source, status in availability.items():
    print(f"{source}: {'✓' if status.available else '✗'}")

# Alert on critical events
await monitor.setup_alerts([
    AlertRule(
        condition="earnings_event.eps_surprise > 0.1",
        action="send_email",
        recipients=["traders@company.com"]
    ),
    AlertRule(
        condition="economic_event.importance == 'high'",
        action="send_slack_message",
        channel="#trading-alerts"
    )
])
```

### 🚨 **Error Handling & Recovery**
```python
from events.resilience import ErrorHandler, RecoveryManager

# Automatic error recovery
error_handler = ErrorHandler(
    max_retries=3,
    backoff_factor=2.0,
    circuit_breaker_threshold=10
)

recovery_manager = RecoveryManager()

# Handle source failures gracefully
try:
    events = await fetch_earnings_from_source('polygon')
except SourceUnavailableError:
    # Fallback to alternative source
    events = await fetch_earnings_from_source('finnhub')
    await recovery_manager.log_fallback('polygon', 'finnhub')

# Data gap detection and filling
gaps = await recovery_manager.detect_data_gaps()
for gap in gaps:
    await recovery_manager.fill_gap(gap)
```

## Configuration

### ⚙️ **Event System Configuration**
```python
# Event system settings
EVENT_CONFIG = {
    'sources': {
        'polygon': {
            'enabled': True,
            'priority': 1,
            'rate_limit': 5,  # requests per second
            'timeout': 30
        },
        'finnhub': {
            'enabled': True,
            'priority': 2,
            'rate_limit': 10,
            'timeout': 15
        }
    },
    'processing': {
        'batch_size': 1000,
        'max_concurrent': 10,
        'reconciliation_enabled': True,
        'deduplication_enabled': True
    },
    'storage': {
        'compression_enabled': True,
        'archival_after_days': 365,
        'partition_by': 'month'
    }
}
```

### 🔧 **Environment Variables**
```bash
# Event ingestion settings
EVENT_SOURCES=polygon,finnhub,fmp
EVENT_BATCH_SIZE=1000
EVENT_MAX_CONCURRENT=10

# Source-specific API keys
POLYGON_API_KEY=your_polygon_key
FINNHUB_API_KEY=your_finnhub_key
FMP_API_KEY=your_fmp_key

# Processing settings
ENABLE_RECONCILIATION=true
ENABLE_DEDUPLICATION=true
ENABLE_REAL_TIME=true

# Storage settings
EVENT_COMPRESSION=true
EVENT_ARCHIVAL_DAYS=365
```

## Best Practices

### 📋 **Event Ingestion Guidelines**
1. **Data Validation**: Always validate events before storage
2. **Error Handling**: Implement robust error handling and retry logic
3. **Rate Limiting**: Respect API rate limits of data sources
4. **Monitoring**: Monitor ingestion performance and data quality
5. **Reconciliation**: Always reconcile data from multiple sources

### 🔧 **Performance Guidelines**
1. **Batch Processing**: Process events in batches for efficiency
2. **Async Operations**: Use async/await for high concurrency
3. **Database Optimization**: Use proper indexing and partitioning
4. **Caching**: Cache frequently accessed events
5. **Compression**: Compress historical data to save storage

### 🛡️ **Data Quality Guidelines**
1. **Source Diversity**: Use multiple sources for critical events
2. **Quality Metrics**: Track data quality metrics continuously
3. **Anomaly Detection**: Detect and handle data anomalies
4. **Historical Validation**: Validate events against historical patterns
5. **Manual Review**: Have processes for manual review of critical events

---

**📅 This directory provides a comprehensive, scalable event system capable of ingesting and processing millions of events daily from multiple sources with enterprise-grade reliability and performance.**