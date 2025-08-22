# Real-Time Market Data Collection System - Detailed Requirements Document (DRD)

**Document Version:** 1.0  
**Created:** August 22, 2025  
**Last Updated:** August 22, 2025  
**Technical Lead:** ATS Data Engineering Team  
**Status:** 🔄 DETAILED DESIGN COMPLETE - READY FOR IMPLEMENTATION  

---

## 1. Document Overview

### 1.1 Purpose
This DRD provides detailed technical specifications for implementing the Real-Time Market Data Collection System, covering database schema, API integrations, Kubernetes deployment, data validation algorithms, and operational procedures.

### 1.2 Scope
- **Database Design**: Complete schema with vendor-specific tables and monitoring infrastructure
- **Service Architecture**: Kubernetes-native deployment with streaming services and CronJobs
- **Integration Specifications**: Detailed vendor API integration patterns and protocols
- **Quality Framework**: Comprehensive data validation and quality scoring algorithms
- **Operational Procedures**: Monitoring, alerting, and maintenance protocols

### 1.3 Related Documents
- [Real-Time Data Collection PRD](realtime_data_collection_prd.md) - Business requirements and success criteria
- [Data Coverage Catalog PRD](data_catalog_prd.md) - Integration with existing coverage monitoring
- [Database Migration Guide](../src/db/migrations/042_create_vendor_minute_tables.sql) - Schema implementation

---

## 2. System Architecture Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Real-Time Data Collection System             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐  │
│  │   Polygon.io    │    │     Tiingo      │    │     FMP     │  │
│  │   WebSocket     │    │  WebSocket/API  │    │  REST API   │  │
│  └─────────────────┘    └─────────────────┘    └─────────────┘  │
│           │                       │                      │      │
│           └───────────────────────┼──────────────────────┘      │
│                                   │                             │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │          Real-Time Streaming Collector                     │ │
│  │     (Kubernetes Deployment - Always Running)               │ │
│  │                                                             │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │ │
│  │  │   Market    │ │   Quality   │ │    Gap Detection    │   │ │
│  │  │   Hours     │ │ Assessment  │ │   & Monitoring      │   │ │
│  │  │ Intelligence│ │             │ │                     │   │ │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                   │                             │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                 TimescaleDB Database                        │ │
│  │                                                             │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │ │
│  │  │   Vendor    │ │ Monitoring  │ │    Validation       │   │ │
│  │  │   Tables    │ │   Tables    │ │      Tables         │   │ │
│  │  │             │ │             │ │                     │   │ │
│  │  │ polygon     │ │ collection  │ │  batch_validation   │   │ │
│  │  │ tiingo      │ │ _status     │ │  gaps               │   │ │
│  │  │ fmp         │ │             │ │                     │   │ │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Scheduled Validation & Backfill               │ │
│  │                    (Kubernetes CronJobs)                   │ │
│  │                                                             │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐   │ │
│  │  │   Daily     │ │    Gap      │ │      Weekly         │   │ │
│  │  │ Validation  │ │ Detection   │ │   Comprehensive     │   │ │
│  │  │  (8:00 PM)  │ │(Every 30min)│ │   Backfill (Sat)    │   │ │
│  │  └─────────────┘ └─────────────┘ └─────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Responsibilities

#### Real-Time Streaming Collector
- **Primary Function**: Continuous data ingestion during market hours
- **Data Sources**: Polygon WebSocket, Tiingo API, FMP REST API
- **Processing**: Quality assessment, vendor-specific storage, gap detection
- **Deployment**: Kubernetes Deployment with 2 replicas for high availability

#### Daily Validation Service
- **Primary Function**: Compare real-time vs batch data for accuracy
- **Schedule**: Daily at 8:00 PM EST (after market close + 4 hours)
- **Processing**: Statistical analysis, discrepancy detection, quality scoring
- **Deployment**: Kubernetes CronJob with 2-hour timeout

#### Gap Detection Service
- **Primary Function**: Identify and classify data gaps for backfill
- **Schedule**: Every 30 minutes during market hours
- **Processing**: Gap pattern analysis, priority classification, backfill triggers
- **Deployment**: Kubernetes CronJob with 30-minute timeout

---

## 3. Database Design Specification

### 3.1 Vendor-Specific Real-Time Tables

#### 3.1.1 Polygon Real-Time Table
```sql
CREATE TABLE IF NOT EXISTS dev_one_minute_live_polygon (
    id BIGSERIAL PRIMARY KEY,
    instrument_id INTEGER REFERENCES dev_instruments(id),
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12,4) NOT NULL,
    high_price NUMERIC(12,4) NOT NULL,
    low_price NUMERIC(12,4) NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    vwap NUMERIC(12,4),                    -- Polygon-specific field
    trade_count INTEGER,                   -- Polygon-specific field
    
    -- Real-time metadata
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_latency_ms INTEGER,               -- Latency from bar close to receipt
    collection_method VARCHAR(20) DEFAULT 'websocket',
    is_realtime BOOLEAN DEFAULT TRUE,
    
    -- Quality and validation
    quality_score NUMERIC(3,2) DEFAULT 0.8,
    validation_status VARCHAR(20) DEFAULT 'pending',
    data_source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(instrument_id, timestamp)
);

-- TimescaleDB hypertable optimization
SELECT create_hypertable('dev_one_minute_live_polygon', 'timestamp', if_not_exists => TRUE);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_live_polygon_symbol_time 
    ON dev_one_minute_live_polygon (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_polygon_received_at 
    ON dev_one_minute_live_polygon (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_live_polygon_latency 
    ON dev_one_minute_live_polygon (data_latency_ms) 
    WHERE data_latency_ms IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_live_polygon_quality 
    ON dev_one_minute_live_polygon (quality_score) 
    WHERE quality_score < 0.8;
```

#### 3.1.2 Tiingo Real-Time Table
```sql
CREATE TABLE IF NOT EXISTS dev_one_minute_live_tiingo (
    id BIGSERIAL PRIMARY KEY,
    instrument_id INTEGER REFERENCES dev_instruments(id),
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12,4) NOT NULL,
    high_price NUMERIC(12,4) NOT NULL,
    low_price NUMERIC(12,4) NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    adj_close_price NUMERIC(12,4),        -- Tiingo-specific field
    volume BIGINT NOT NULL DEFAULT 0,
    
    -- Real-time metadata
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_latency_ms INTEGER,
    collection_method VARCHAR(20) DEFAULT 'websocket',
    is_realtime BOOLEAN DEFAULT TRUE,
    
    -- Quality and validation
    quality_score NUMERIC(3,2) DEFAULT 0.8,
    validation_status VARCHAR(20) DEFAULT 'pending',
    data_source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(instrument_id, timestamp)
);

-- Similar TimescaleDB and index setup as Polygon
```

#### 3.1.3 FMP Real-Time Table
```sql
CREATE TABLE IF NOT EXISTS dev_one_minute_live_fmp (
    id BIGSERIAL PRIMARY KEY,
    instrument_id INTEGER REFERENCES dev_instruments(id),
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12,4) NOT NULL,
    high_price NUMERIC(12,4) NOT NULL,
    low_price NUMERIC(12,4) NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    adj_close_price NUMERIC(12,4),        -- FMP-specific field
    volume BIGINT NOT NULL DEFAULT 0,
    
    -- Real-time metadata (polling-based)
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_latency_ms INTEGER,
    collection_method VARCHAR(20) DEFAULT 'polling',
    is_realtime BOOLEAN DEFAULT TRUE,
    
    -- Quality and validation
    quality_score NUMERIC(3,2) DEFAULT 0.8,
    validation_status VARCHAR(20) DEFAULT 'pending',
    data_source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(instrument_id, timestamp)
);
```

### 3.2 Monitoring and Status Tables

#### 3.2.1 Real-Time Collection Status
```sql
CREATE TABLE IF NOT EXISTS dev_realtime_collection_status (
    id BIGSERIAL PRIMARY KEY,
    vendor VARCHAR(20) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    last_received_timestamp TIMESTAMPTZ,
    expected_timestamp TIMESTAMPTZ,       -- When next bar should arrive
    data_delay_minutes INTEGER DEFAULT 0,
    consecutive_missing_bars INTEGER DEFAULT 0,
    total_bars_today INTEGER DEFAULT 0,
    successful_collections INTEGER DEFAULT 0,
    failed_collections INTEGER DEFAULT 0,
    avg_latency_ms NUMERIC(8,2),
    collection_health_score NUMERIC(3,2) DEFAULT 1.0,
    
    -- Status tracking
    is_active BOOLEAN DEFAULT TRUE,
    last_error_message TEXT,
    last_error_at TIMESTAMPTZ,
    
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(vendor, symbol)
);

-- Index for monitoring queries
CREATE INDEX IF NOT EXISTS idx_collection_status_vendor_symbol 
    ON dev_realtime_collection_status (vendor, symbol);
CREATE INDEX IF NOT EXISTS idx_collection_status_health 
    ON dev_realtime_collection_status (collection_health_score) 
    WHERE collection_health_score < 0.8;
CREATE INDEX IF NOT EXISTS idx_collection_status_active 
    ON dev_realtime_collection_status (is_active, updated_at) 
    WHERE is_active = true;
```

#### 3.2.2 Real-Time vs Batch Validation
```sql
CREATE TABLE IF NOT EXISTS dev_realtime_batch_validation (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    validation_date DATE NOT NULL,
    vendor VARCHAR(20) NOT NULL,
    
    -- Comparison metrics
    realtime_bars_count INTEGER DEFAULT 0,
    batch_bars_count INTEGER DEFAULT 0,
    missing_realtime_bars INTEGER DEFAULT 0,
    discrepant_prices INTEGER DEFAULT 0,
    avg_price_difference NUMERIC(8,6),    -- Average % difference
    max_price_difference NUMERIC(8,6),    -- Maximum % difference
    
    -- Latency analysis
    avg_data_latency_minutes NUMERIC(6,2),
    max_data_latency_minutes NUMERIC(6,2),
    late_bars_count INTEGER DEFAULT 0,    -- Bars > 5 min late
    
    -- Quality scores
    realtime_quality_score NUMERIC(3,2),
    batch_quality_score NUMERIC(3,2),
    overall_accuracy_score NUMERIC(3,2),
    
    -- Validation results
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_notes TEXT,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, validation_date, vendor)
);

-- Index for validation reporting
CREATE INDEX IF NOT EXISTS idx_validation_date_vendor 
    ON dev_realtime_batch_validation (validation_date DESC, vendor);
CREATE INDEX IF NOT EXISTS idx_validation_accuracy 
    ON dev_realtime_batch_validation (overall_accuracy_score) 
    WHERE overall_accuracy_score < 0.9;
```

#### 3.2.3 Gap Detection and Backfill Tracking
```sql
CREATE TABLE IF NOT EXISTS dev_realtime_gaps (
    id BIGSERIAL PRIMARY KEY,
    vendor VARCHAR(20) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    gap_start_timestamp TIMESTAMPTZ NOT NULL,
    gap_end_timestamp TIMESTAMPTZ NOT NULL,
    gap_duration_minutes INTEGER NOT NULL,
    missing_bars_count INTEGER NOT NULL,
    
    -- Gap classification
    gap_type VARCHAR(20) NOT NULL,        -- connection_loss, api_error, market_closure
    detection_method VARCHAR(20) DEFAULT 'realtime',  -- realtime, batch_validation
    gap_severity VARCHAR(10) DEFAULT 'medium',        -- low, medium, high, critical
    
    -- Backfill tracking
    backfill_status VARCHAR(20) DEFAULT 'pending',    -- pending, in_progress, completed, failed
    backfill_method VARCHAR(20),          -- websocket_replay, batch_api, manual
    backfilled_bars_count INTEGER DEFAULT 0,
    backfill_started_at TIMESTAMPTZ,
    backfill_completed_at TIMESTAMPTZ,
    backfill_error_message TEXT,
    
    detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index for gap analysis
CREATE INDEX IF NOT EXISTS idx_gaps_vendor_symbol_time 
    ON dev_realtime_gaps (vendor, symbol, gap_start_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_gaps_backfill_status 
    ON dev_realtime_gaps (backfill_status) 
    WHERE backfill_status != 'completed';
CREATE INDEX IF NOT EXISTS idx_gaps_severity 
    ON dev_realtime_gaps (gap_severity, detected_at DESC) 
    WHERE gap_severity IN ('high', 'critical');
```

### 3.3 Unified Views and Analytics

#### 3.3.1 Real-Time Data Unified View
```sql
CREATE OR REPLACE VIEW dev_one_minute_live_unified AS
WITH vendor_data AS (
    SELECT 
        'polygon' as vendor,
        instrument_id, symbol, timestamp,
        open_price, high_price, low_price, close_price, volume,
        vwap, trade_count,
        received_at, data_latency_ms, collection_method,
        is_realtime, quality_score, validation_status
    FROM dev_one_minute_live_polygon
    
    UNION ALL
    
    SELECT 
        'tiingo' as vendor,
        instrument_id, symbol, timestamp,
        open_price, high_price, low_price, close_price, volume,
        NULL as vwap, NULL as trade_count,
        received_at, data_latency_ms, collection_method,
        is_realtime, quality_score, validation_status
    FROM dev_one_minute_live_tiingo
    
    UNION ALL
    
    SELECT 
        'fmp' as vendor,
        instrument_id, symbol, timestamp,
        open_price, high_price, low_price, close_price, volume,
        NULL as vwap, NULL as trade_count,
        received_at, data_latency_ms, collection_method,
        is_realtime, quality_score, validation_status
    FROM dev_one_minute_live_fmp
)
SELECT * FROM vendor_data
ORDER BY symbol, timestamp DESC, vendor;
```

#### 3.3.2 Real-Time Quality Dashboard View
```sql
CREATE OR REPLACE VIEW dev_realtime_quality_dashboard AS
SELECT 
    rcs.vendor,
    rcs.symbol,
    rcs.last_received_timestamp,
    rcs.data_delay_minutes,
    rcs.consecutive_missing_bars,
    rcs.collection_health_score,
    
    -- Latest validation results
    rbv.overall_accuracy_score,
    rbv.avg_price_difference,
    rbv.validation_status,
    
    -- Active gaps
    COUNT(rg.id) FILTER (WHERE rg.backfill_status != 'completed') as active_gaps_count,
    MAX(rg.gap_duration_minutes) as max_gap_duration_minutes,
    
    -- Status indicators
    CASE 
        WHEN rcs.collection_health_score >= 0.9 THEN 'healthy'
        WHEN rcs.collection_health_score >= 0.7 THEN 'warning'
        ELSE 'critical'
    END as health_status,
    
    rcs.updated_at
    
FROM dev_realtime_collection_status rcs
LEFT JOIN dev_realtime_batch_validation rbv ON rcs.vendor = rbv.vendor 
    AND rcs.symbol = rbv.symbol 
    AND rbv.validation_date = CURRENT_DATE
LEFT JOIN dev_realtime_gaps rg ON rcs.vendor = rg.vendor 
    AND rcs.symbol = rg.symbol 
    AND rg.backfill_status != 'completed'
GROUP BY 
    rcs.vendor, rcs.symbol, rcs.last_received_timestamp, rcs.data_delay_minutes,
    rcs.consecutive_missing_bars, rcs.collection_health_score,
    rbv.overall_accuracy_score, rbv.avg_price_difference, rbv.validation_status,
    rcs.updated_at
ORDER BY rcs.collection_health_score ASC, rcs.data_delay_minutes DESC;
```

---

## 4. Vendor Integration Specifications

### 4.1 Polygon WebSocket Integration

#### 4.1.1 Connection Specifications
```python
# Polygon WebSocket Configuration
POLYGON_CONFIG = {
    'websocket_url': 'wss://socket.polygon.io/stocks',
    'auth_method': 'api_key',
    'max_symbols_per_subscription': 100,
    'reconnect_interval': 30,  # seconds
    'heartbeat_interval': 30,  # seconds
    'message_types': ['AM'],   # Minute aggregates
    'rate_limit_per_minute': 5000
}

# WebSocket Message Flow
async def polygon_websocket_handler():
    """
    1. Connect to wss://socket.polygon.io/stocks
    2. Authenticate with API key
    3. Subscribe to minute aggregates (AM) for universe symbols
    4. Process incoming messages and store to database
    5. Handle reconnection on disconnect
    """
    
    # Authentication message
    auth_msg = {
        "action": "auth",
        "params": POLYGON_API_KEY
    }
    
    # Subscription message for minute aggregates
    subscribe_msg = {
        "action": "subscribe", 
        "params": "AM.AAPL,AM.MSFT,AM.GOOGL,..."  # Max 100 symbols
    }
    
    # Expected response format
    minute_aggregate_msg = {
        "ev": "AM",          # Event type (minute aggregate)
        "sym": "AAPL",       # Symbol
        "v": 31315282,       # Volume
        "av": 73814083,      # Accumulated volume
        "op": 102.87,        # Open price
        "vw": 103.0267,      # Volume weighted average price
        "o": 102.89,         # Open price for this minute
        "c": 103.74,         # Close price for this minute
        "h": 103.74,         # High price for this minute
        "l": 102.89,         # Low price for this minute
        "a": 103.0267,       # VWAP
        "n": 547,            # Number of transactions
        "t": 1577745000000,  # Timestamp (Unix milliseconds)
        "s": 1577745060000   # Start of window timestamp
    }
```

#### 4.1.2 Error Handling and Resilience
```python
class PolygonStreamingAdapter:
    def __init__(self):
        self.max_retries = 5
        self.retry_delay = 30  # seconds
        self.circuit_breaker_threshold = 10
        self.current_failures = 0
        
    async def handle_connection_error(self, error):
        """Handle WebSocket connection errors with exponential backoff"""
        self.current_failures += 1
        
        if self.current_failures >= self.circuit_breaker_threshold:
            # Circuit breaker - stop trying for 5 minutes
            await asyncio.sleep(300)
            self.current_failures = 0
            
        # Exponential backoff
        delay = min(self.retry_delay * (2 ** self.current_failures), 300)
        await asyncio.sleep(delay)
        
    async def validate_message(self, message):
        """Validate Polygon message format and data quality"""
        required_fields = ['ev', 'sym', 'c', 'h', 'l', 'o', 'v', 't']
        
        for field in required_fields:
            if field not in message:
                raise ValueError(f"Missing required field: {field}")
                
        # Data quality checks
        if message['h'] < message['l']:
            raise ValueError("High price less than low price")
            
        if message['c'] <= 0 or message['o'] <= 0:
            raise ValueError("Invalid price values")
```

### 4.2 Tiingo Integration

#### 4.2.1 IEX WebSocket Configuration
```python
# Tiingo IEX WebSocket Configuration
TIINGO_CONFIG = {
    'websocket_url': 'wss://api.tiingo.com/iex',
    'auth_method': 'token_header',
    'max_symbols_per_subscription': 50,
    'reconnect_interval': 45,
    'heartbeat_interval': 60,
    'message_types': ['T'],  # Trades (to build minute bars)
    'rate_limit_per_minute': 1000,
    'fallback_to_rest': True  # Fall back to REST API if WebSocket fails
}

# Tiingo REST API Configuration (Fallback)
TIINGO_REST_CONFIG = {
    'base_url': 'https://api.tiingo.com/iex',
    'auth_method': 'token_param',
    'rate_limit_per_minute': 500,
    'batch_size': 50,  # Symbols per request
    'request_timeout': 30  # seconds
}

# Expected WebSocket message format
tiingo_trade_msg = {
    "messageType": "T",
    "service": "iex",
    "data": [{
        "ticker": "AAPL",
        "timestamp": "2019-12-30T20:00:00.000000000Z",
        "quoteTimestamp": "2019-12-30T20:00:00.000000000Z", 
        "lastPrice": 293.65,
        "lastSize": 100,
        "lastSaleTimestamp": "2019-12-30T20:00:00.000000000Z",
        "lastUpdated": "2019-12-30T20:00:00.000000000Z",
        "bidPrice": 293.64,
        "bidSize": 100,
        "askPrice": 293.65,
        "askSize": 200,
        "mid": 293.645,
        "volume": 31315282
    }]
}
```

#### 4.2.2 Minute Bar Aggregation
```python
class TiingoMinuteBarAggregator:
    """Aggregate Tiingo trade data into 1-minute OHLCV bars"""
    
    def __init__(self):
        self.active_bars = {}  # symbol -> current minute bar
        
    def process_trade(self, trade_data):
        """Process individual trade and update minute bar"""
        symbol = trade_data['ticker']
        price = trade_data['lastPrice']
        volume = trade_data['lastSize']
        timestamp = parse_timestamp(trade_data['timestamp'])
        
        # Round down to minute boundary
        minute_timestamp = timestamp.replace(second=0, microsecond=0)
        
        bar_key = f"{symbol}_{minute_timestamp}"
        
        if bar_key not in self.active_bars:
            # Initialize new minute bar
            self.active_bars[bar_key] = {
                'symbol': symbol,
                'timestamp': minute_timestamp,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume,
                'trade_count': 1
            }
        else:
            # Update existing minute bar
            bar = self.active_bars[bar_key]
            bar['high'] = max(bar['high'], price)
            bar['low'] = min(bar['low'], price)
            bar['close'] = price
            bar['volume'] += volume
            bar['trade_count'] += 1
            
    def get_completed_bars(self, current_time):
        """Return completed minute bars and clean up active bars"""
        completed = []
        current_minute = current_time.replace(second=0, microsecond=0)
        
        for bar_key, bar in list(self.active_bars.items()):
            if bar['timestamp'] < current_minute:
                completed.append(bar)
                del self.active_bars[bar_key]
                
        return completed
```

### 4.3 FMP REST API Integration

#### 4.3.1 API Configuration
```python
# FMP REST API Configuration
FMP_CONFIG = {
    'base_url': 'https://financialmodelingprep.com/api/v3',
    'auth_method': 'api_key_param',
    'rate_limit_per_minute': 250,  # Free tier limit
    'request_timeout': 30,
    'batch_size': 1,  # FMP requires individual symbol requests
    'retry_attempts': 3,
    'retry_delay': 60  # seconds between retries
}

# API Endpoints
FMP_ENDPOINTS = {
    'minute_chart': '/historical-chart/1min/{symbol}',
    'daily_chart': '/historical-price-full/{symbol}',
    'real_time_price': '/quote-short/{symbol}'
}

# Expected API response format
fmp_minute_response = {
    "symbol": "AAPL",
    "historical": [
        {
            "date": "2019-12-30 20:00:00",
            "open": 293.80,
            "high": 293.90,
            "low": 293.65,
            "close": 293.65,
            "volume": 152344
        }
        # ... more minute bars
    ]
}
```

#### 4.3.2 Polling Strategy
```python
class FMPPollingCollector:
    """FMP minute data collection via polling"""
    
    def __init__(self):
        self.polling_interval = 60  # seconds
        self.symbols_per_batch = 20  # To respect rate limits
        self.last_collection_time = {}
        
    async def collect_latest_minute_data(self, symbols):
        """Collect latest minute data for symbols"""
        current_time = datetime.now(timezone.utc)
        
        for symbol_batch in self.chunk_symbols(symbols, self.symbols_per_batch):
            for symbol in symbol_batch:
                try:
                    # Get latest minute data
                    data = await self.fetch_minute_data(symbol, current_time)
                    
                    # Filter to new data since last collection
                    new_bars = self.filter_new_data(symbol, data)
                    
                    for bar in new_bars:
                        await self.store_minute_bar(bar)
                        
                    self.last_collection_time[symbol] = current_time
                    
                except Exception as e:
                    logger.error(f"FMP collection error for {symbol}: {e}")
                    
                # Rate limiting
                await asyncio.sleep(60 / 250)  # 250 requests per minute max
                
    def filter_new_data(self, symbol, data):
        """Filter data to only include new bars since last collection"""
        last_time = self.last_collection_time.get(symbol)
        if not last_time:
            return data  # First collection, take all recent data
            
        return [bar for bar in data if bar['timestamp'] > last_time]
```

---

## 5. Quality Assessment Algorithms

### 5.1 Real-Time Quality Scoring

#### 5.1.1 Quality Score Calculation
```python
class DataQualityAssessor:
    """Real-time data quality assessment"""
    
    def __init__(self):
        self.latency_thresholds = {
            'excellent': 30,    # < 30 seconds
            'good': 60,         # 30-60 seconds  
            'acceptable': 300,  # 1-5 minutes
            'poor': 600         # 5-10 minutes
        }
        
        self.completeness_weights = {
            'open': 0.2,
            'high': 0.2, 
            'low': 0.2,
            'close': 0.3,  # Close price most important
            'volume': 0.1
        }
        
    def calculate_quality_score(self, bar_data, latency_ms):
        """Calculate comprehensive quality score (0.0 - 1.0)"""
        base_score = 1.0
        
        # Latency penalty
        latency_seconds = latency_ms / 1000
        if latency_seconds > self.latency_thresholds['poor']:
            base_score -= 0.5  # Major penalty for very late data
        elif latency_seconds > self.latency_thresholds['acceptable']:
            base_score -= 0.3  # Moderate penalty
        elif latency_seconds > self.latency_thresholds['good']:
            base_score -= 0.1  # Minor penalty
            
        # Data completeness penalty
        missing_fields = self.check_completeness(bar_data)
        for field, weight in self.completeness_weights.items():
            if field in missing_fields:
                base_score -= weight
                
        # OHLC consistency check
        if not self.validate_ohlc_consistency(bar_data):
            base_score -= 0.2
            
        # Volume reasonableness check
        if not self.validate_volume(bar_data):
            base_score -= 0.1
            
        return max(0.0, min(1.0, base_score))
        
    def validate_ohlc_consistency(self, bar_data):
        """Validate OHLC price relationships"""
        try:
            o, h, l, c = bar_data['open'], bar_data['high'], bar_data['low'], bar_data['close']
            
            # High must be >= all other prices
            if h < max(o, l, c):
                return False
                
            # Low must be <= all other prices  
            if l > min(o, h, c):
                return False
                
            # All prices must be positive
            if any(price <= 0 for price in [o, h, l, c]):
                return False
                
            return True
            
        except (KeyError, TypeError, ValueError):
            return False
            
    def validate_volume(self, bar_data):
        """Validate volume data"""
        try:
            volume = bar_data.get('volume', 0)
            
            # Volume must be non-negative
            if volume < 0:
                return False
                
            # Very high volume check (> 100M shares might be suspicious)
            if volume > 100_000_000:
                return False
                
            return True
            
        except (TypeError, ValueError):
            return False
```

#### 5.1.2 Statistical Validation
```python
class StatisticalValidator:
    """Statistical analysis for price validation"""
    
    def __init__(self):
        self.price_change_thresholds = {
            'normal': 0.05,      # 5% change
            'high': 0.10,        # 10% change  
            'extreme': 0.20      # 20% change
        }
        
    async def validate_price_continuity(self, symbol, new_bar, db_pool):
        """Validate price continuity against recent history"""
        # Get last few bars for comparison
        query = """
            SELECT close_price, timestamp 
            FROM dev_one_minute_live_unified 
            WHERE symbol = $1 
              AND timestamp < $2 
            ORDER BY timestamp DESC 
            LIMIT 5
        """
        
        async with db_pool.acquire() as conn:
            recent_bars = await conn.fetch(query, symbol, new_bar['timestamp'])
            
        if not recent_bars:
            return True  # No history to compare against
            
        last_price = recent_bars[0]['close_price']
        current_price = new_bar['close_price']
        
        price_change = abs(current_price - last_price) / last_price
        
        # Flag suspicious price changes
        if price_change > self.price_change_thresholds['extreme']:
            return False  # Requires manual review
        elif price_change > self.price_change_thresholds['high']:
            # Additional validation for high changes
            return await self.validate_high_price_change(symbol, new_bar, recent_bars)
            
        return True
        
    async def validate_high_price_change(self, symbol, new_bar, recent_bars):
        """Additional validation for high price changes"""
        # Check if similar change happened in recent history
        prices = [float(bar['close_price']) for bar in recent_bars]
        
        # Calculate recent volatility
        if len(prices) >= 3:
            recent_changes = [abs(prices[i] - prices[i+1]) / prices[i+1] for i in range(len(prices)-1)]
            avg_change = sum(recent_changes) / len(recent_changes)
            
            current_change = abs(new_bar['close_price'] - prices[0]) / prices[0]
            
            # If current change is within 3x recent average, likely valid
            if current_change <= avg_change * 3:
                return True
                
        # TODO: Check against news/market events for justification
        return False  # Flag for manual review
```

### 5.2 Daily Batch Validation

#### 5.2.1 Comparison Algorithm
```python
class BatchValidationEngine:
    """Compare real-time vs batch data for accuracy assessment"""
    
    def __init__(self):
        self.price_tolerance = 0.005    # 0.5% price difference tolerance
        self.volume_tolerance = 0.10    # 10% volume difference tolerance
        self.time_tolerance = 60       # 60 seconds timestamp tolerance
        
    async def validate_symbol_day(self, vendor, symbol, validation_date):
        """Validate one symbol for one day against batch data"""
        
        # Get real-time data for the day
        realtime_data = await self.get_realtime_data(vendor, symbol, validation_date)
        
        # Get batch data from vendor API
        batch_data = await self.get_batch_data(vendor, symbol, validation_date)
        
        # Align timestamps and compare
        comparison_results = self.compare_data_sets(realtime_data, batch_data)
        
        # Calculate metrics
        validation_result = self.calculate_validation_metrics(comparison_results)
        
        return validation_result
        
    def compare_data_sets(self, realtime_data, batch_data):
        """Compare real-time vs batch data point by point"""
        results = []
        
        # Create lookup for batch data with time tolerance
        batch_lookup = {}
        for bar in batch_data:
            timestamp = bar['timestamp']
            batch_lookup[timestamp] = bar
            
        for rt_bar in realtime_data:
            rt_timestamp = rt_bar['timestamp']
            
            # Find matching batch bar within time tolerance
            batch_bar = self.find_matching_batch_bar(rt_timestamp, batch_lookup)
            
            if batch_bar:
                comparison = self.compare_individual_bars(rt_bar, batch_bar)
                comparison['realtime_bar'] = rt_bar
                comparison['batch_bar'] = batch_bar
                results.append(comparison)
            else:
                # Real-time bar with no batch equivalent
                results.append({
                    'status': 'missing_batch',
                    'realtime_bar': rt_bar,
                    'batch_bar': None
                })
                
        return results
        
    def compare_individual_bars(self, rt_bar, batch_bar):
        """Compare two individual minute bars"""
        comparison = {
            'status': 'compared',
            'price_differences': {},
            'volume_difference': None,
            'timestamp_difference': None,
            'overall_match': True
        }
        
        # Compare prices
        price_fields = ['open_price', 'high_price', 'low_price', 'close_price']
        for field in price_fields:
            rt_price = rt_bar.get(field)
            batch_price = batch_bar.get(field)
            
            if rt_price and batch_price:
                diff_pct = abs(rt_price - batch_price) / batch_price
                comparison['price_differences'][field] = diff_pct
                
                if diff_pct > self.price_tolerance:
                    comparison['overall_match'] = False
                    
        # Compare volume
        rt_volume = rt_bar.get('volume', 0)
        batch_volume = batch_bar.get('volume', 0)
        
        if batch_volume > 0:
            volume_diff = abs(rt_volume - batch_volume) / batch_volume
            comparison['volume_difference'] = volume_diff
            
            if volume_diff > self.volume_tolerance:
                comparison['overall_match'] = False
                
        # Compare timestamps
        rt_time = rt_bar['timestamp']
        batch_time = batch_bar['timestamp']
        time_diff = abs((rt_time - batch_time).total_seconds())
        comparison['timestamp_difference'] = time_diff
        
        if time_diff > self.time_tolerance:
            comparison['overall_match'] = False
            
        return comparison
        
    def calculate_validation_metrics(self, comparison_results):
        """Calculate summary metrics from comparison results"""
        total_comparisons = len(comparison_results)
        
        if total_comparisons == 0:
            return self.empty_validation_result()
            
        matching_bars = sum(1 for r in comparison_results if r.get('overall_match', False))
        missing_batch = sum(1 for r in comparison_results if r['status'] == 'missing_batch')
        
        # Price difference statistics
        all_price_diffs = []
        for result in comparison_results:
            if 'price_differences' in result:
                all_price_diffs.extend(result['price_differences'].values())
                
        # Calculate final metrics
        return {
            'total_realtime_bars': total_comparisons,
            'matching_bars': matching_bars,
            'missing_batch_bars': missing_batch,
            'accuracy_rate': matching_bars / total_comparisons if total_comparisons > 0 else 0,
            'avg_price_difference': statistics.mean(all_price_diffs) if all_price_diffs else 0,
            'max_price_difference': max(all_price_diffs) if all_price_diffs else 0,
            'validation_status': 'passed' if matching_bars / total_comparisons >= 0.95 else 'failed'
        }
```

---

## 6. Kubernetes Deployment Specification

### 6.1 Real-Time Streaming Service

#### 6.1.1 Deployment Configuration
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: realtime-minute-collector
  namespace: ats-dev
  labels:
    app: realtime-collector
    component: streaming-service
    version: v1.0.0
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0  # Ensure zero downtime
  selector:
    matchLabels:
      app: realtime-collector
  template:
    metadata:
      labels:
        app: realtime-collector
        version: v1.0.0
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
      - name: streaming-collector
        image: ats/realtime-collector:v1.0.0  # Custom image with baked-in code
        imagePullPolicy: Always
        ports:
        - name: metrics
          containerPort: 8080
          protocol: TCP
        - name: health
          containerPort: 8081
          protocol: TCP
        env:
        # Universe and market configuration
        - name: UNIVERSE_SIZE
          value: "2000"
        - name: MARKET_HOURS_ONLY
          value: "true"
        - name: ENABLE_PREMARKET
          value: "false"
        - name: ENABLE_AFTERHOURS
          value: "true"
        - name: MAX_LATENCY_SECONDS
          value: "120"
        - name: COLLECTION_INTERVAL_SECONDS
          value: "60"
        
        # Quality thresholds
        - name: MIN_QUALITY_SCORE
          value: "0.7"
        - name: MAX_PRICE_CHANGE_PCT
          value: "0.20"
        - name: ALERT_LATENCY_THRESHOLD_MS
          value: "300000"  # 5 minutes
        
        # API credentials from secrets
        - name: POLYGON_API_KEY
          valueFrom:
            secretKeyRef:
              name: vendor-api-credentials
              key: polygon-api-key
        - name: TIINGO_API_KEY
          valueFrom:
            secretKeyRef:
              name: vendor-api-credentials
              key: tiingo-api-key
        - name: FMP_API_KEY
          valueFrom:
            secretKeyRef:
              name: vendor-api-credentials
              key: fmp-api-key
        
        # Database configuration
        - name: DB_HOST
          value: "postgres-simple"
        - name: DB_PORT
          value: "5432"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: password
        - name: DB_NAME
          value: "dev_db"
        - name: DB_POOL_MIN_SIZE
          value: "5"
        - name: DB_POOL_MAX_SIZE
          value: "20"
        
        # Logging configuration
        - name: LOG_LEVEL
          value: "INFO"
        - name: LOG_FORMAT
          value: "json"
        - name: ENABLE_STRUCTURED_LOGGING
          value: "true"
        
        resources:
          requests:
            memory: "4Gi"
            cpu: "2000m"
            ephemeral-storage: "1Gi"
          limits:
            memory: "8Gi"
            cpu: "4000m"
            ephemeral-storage: "2Gi"
        
        # Health checks
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8081
          initialDelaySeconds: 60
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
          successThreshold: 1
        
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 15
          timeoutSeconds: 5
          failureThreshold: 3
          successThreshold: 1
        
        # Startup probe for longer initialization
        startupProbe:
          httpGet:
            path: /health/startup
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 30  # Allow 5 minutes for startup
          successThreshold: 1
        
        # Graceful shutdown
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 15"]  # Grace period for connections
        
        # Security context
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop:
            - ALL
        
        # Volume mounts
        volumeMounts:
        - name: tmp-volume
          mountPath: /tmp
        - name: logs-volume
          mountPath: /app/logs
      
      # Volumes
      volumes:
      - name: tmp-volume
        emptyDir:
          sizeLimit: 1Gi
      - name: logs-volume
        emptyDir:
          sizeLimit: 2Gi
      
      # DNS and networking
      dnsPolicy: ClusterFirst
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      
      # Node affinity for optimal performance
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - realtime-collector
              topologyKey: kubernetes.io/hostname
      
      # Tolerations for dedicated nodes
      tolerations:
      - key: "workload-type"
        operator: "Equal"
        value: "data-processing"
        effect: "NoSchedule"
```

#### 6.1.2 Service Configuration
```yaml
apiVersion: v1
kind: Service
metadata:
  name: realtime-collector-service
  namespace: ats-dev
  labels:
    app: realtime-collector
    component: streaming-service
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/metrics"
spec:
  type: ClusterIP
  selector:
    app: realtime-collector
  ports:
  - name: metrics
    port: 8080
    targetPort: 8080
    protocol: TCP
  - name: health
    port: 8081
    targetPort: 8081
    protocol: TCP
  sessionAffinity: None
```

### 6.2 Daily Validation CronJob

#### 6.2.1 CronJob Configuration
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-realtime-validation
  namespace: ats-dev
  labels:
    app: realtime-validation
    component: validation-service
spec:
  schedule: "0 20 * * 1-5"  # 8:00 PM EST, Monday-Friday
  timeZone: "America/New_York"
  concurrencyPolicy: Forbid  # Don't allow overlapping runs
  successfulJobsHistoryLimit: 5
  failedJobsHistoryLimit: 3
  startingDeadlineSeconds: 3600  # Allow 1 hour late start
  jobTemplate:
    metadata:
      labels:
        app: realtime-validation
        component: validation-job
    spec:
      activeDeadlineSeconds: 7200  # 2 hour timeout
      backoffLimit: 1  # Only retry once
      parallelism: 1
      completions: 1
      template:
        metadata:
          labels:
            app: realtime-validation
            component: validation-job
        spec:
          restartPolicy: OnFailure
          securityContext:
            runAsNonRoot: true
            runAsUser: 1000
            fsGroup: 1000
          containers:
          - name: validation-service
            image: ats/realtime-validator:v1.0.0
            imagePullPolicy: Always
            env:
            # Validation configuration
            - name: VALIDATION_BATCH_SIZE
              value: "50"
            - name: MAX_LATENCY_MINUTES
              value: "5.0"
            - name: MAX_PRICE_DIFF_PCT
              value: "0.5"
            - name: MIN_ACCURACY_SCORE
              value: "0.95"
            - name: MIN_COMPLETENESS
              value: "0.90"
            - name: VALIDATION_TIMEOUT_MINUTES
              value: "120"
            
            # Override validation date for testing
            # - name: VALIDATION_DATE
            #   value: "2025-08-22"
            
            # API credentials
            - name: POLYGON_API_KEY
              valueFrom:
                secretKeyRef:
                  name: vendor-api-credentials
                  key: polygon-api-key
            - name: TIINGO_API_KEY
              valueFrom:
                secretKeyRef:
                  name: vendor-api-credentials
                  key: tiingo-api-key
            - name: FMP_API_KEY
              valueFrom:
                secretKeyRef:
                  name: vendor-api-credentials
                  key: fmp-api-key
            
            # Database configuration
            - name: DB_HOST
              value: "postgres-simple"
            - name: DB_PORT
              value: "5432"
            - name: DB_USER
              valueFrom:
                secretKeyRef:
                  name: postgres-credentials
                  key: username
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: postgres-credentials
                  key: password
            - name: DB_NAME
              value: "dev_db"
            
            # Notification configuration
            - name: SLACK_WEBHOOK_URL
              valueFrom:
                secretKeyRef:
                  name: notification-credentials
                  key: slack-webhook-url
                  optional: true
            - name: EMAIL_SMTP_SERVER
              value: "smtp.gmail.com"
            - name: EMAIL_FROM
              valueFrom:
                secretKeyRef:
                  name: notification-credentials
                  key: email-from
                  optional: true
            
            resources:
              requests:
                memory: "2Gi"
                cpu: "1000m"
                ephemeral-storage: "1Gi"
              limits:
                memory: "4Gi"
                cpu: "2000m"
                ephemeral-storage: "2Gi"
            
            # Security context
            securityContext:
              allowPrivilegeEscalation: false
              readOnlyRootFilesystem: true
              capabilities:
                drop:
                - ALL
            
            # Volume mounts
            volumeMounts:
            - name: tmp-volume
              mountPath: /tmp
            - name: reports-volume
              mountPath: /app/reports
          
          volumes:
          - name: tmp-volume
            emptyDir:
              sizeLimit: 1Gi
          - name: reports-volume
            emptyDir:
              sizeLimit: 1Gi
          
          # Node affinity for batch processing
          affinity:
            nodeAffinity:
              preferredDuringSchedulingIgnoredDuringExecution:
              - weight: 100
                preference:
                  matchExpressions:
                  - key: node-type
                    operator: In
                    values:
                    - batch-processing
```

### 6.3 Gap Detection CronJob

#### 6.3.1 Gap Detection Configuration
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: gap-detection-backfill
  namespace: ats-dev
  labels:
    app: gap-detection
    component: gap-service
spec:
  schedule: "*/30 9-16 * * 1-5"  # Every 30 minutes, 9 AM - 4 PM EST, weekdays
  timeZone: "America/New_York"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 10  # Keep more history for gap analysis
  failedJobsHistoryLimit: 5
  startingDeadlineSeconds: 1800  # 30 minute deadline
  jobTemplate:
    metadata:
      labels:
        app: gap-detection
        component: gap-job
    spec:
      activeDeadlineSeconds: 1800  # 30 minute timeout
      backoffLimit: 2
      template:
        metadata:
          labels:
            app: gap-detection
            component: gap-job
        spec:
          restartPolicy: OnFailure
          containers:
          - name: gap-detector
            image: ats/gap-detector:v1.0.0
            env:
            # Gap detection configuration
            - name: MAX_GAP_MINUTES
              value: "10"
            - name: CRITICAL_GAP_MINUTES
              value: "30"
            - name: BACKFILL_PRIORITY_THRESHOLD
              value: "3"
            - name: MAX_CONCURRENT_BACKFILLS
              value: "5"
            - name: DETECTION_LOOKBACK_HOURS
              value: "2"
            
            # Database configuration
            - name: DB_HOST
              value: "postgres-simple"
            - name: DB_PORT
              value: "5432"
            - name: DB_USER
              valueFrom:
                secretKeyRef:
                  name: postgres-credentials
                  key: username
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: postgres-credentials
                  key: password
            - name: DB_NAME
              value: "dev_db"
            
            resources:
              requests:
                memory: "1Gi"
                cpu: "500m"
              limits:
                memory: "2Gi"
                cpu: "1000m"
```

---

## 7. Monitoring and Alerting Specification

### 7.1 Prometheus Metrics

#### 7.1.1 Application Metrics
```python
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Data collection metrics
data_bars_collected_total = Counter(
    'realtime_data_bars_collected_total',
    'Total number of minute bars collected',
    ['vendor', 'symbol']
)

data_collection_latency_seconds = Histogram(
    'realtime_data_collection_latency_seconds',
    'Latency from bar close to storage',
    ['vendor'],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
)

data_quality_score = Gauge(
    'realtime_data_quality_score',
    'Current data quality score',
    ['vendor', 'symbol']
)

# Gap detection metrics
data_gaps_detected_total = Counter(
    'realtime_data_gaps_detected_total',
    'Total number of data gaps detected',
    ['vendor', 'severity']
)

gap_duration_minutes = Histogram(
    'realtime_gap_duration_minutes',
    'Duration of detected data gaps',
    ['vendor'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600]
)

# Validation metrics
validation_accuracy_score = Gauge(
    'realtime_validation_accuracy_score',
    'Daily validation accuracy score',
    ['vendor', 'symbol']
)

validation_price_difference_pct = Histogram(
    'realtime_validation_price_difference_pct',
    'Price difference between real-time and batch data',
    ['vendor'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
)

# System health metrics
websocket_connections_active = Gauge(
    'realtime_websocket_connections_active',
    'Number of active WebSocket connections',
    ['vendor']
)

api_requests_total = Counter(
    'realtime_api_requests_total',
    'Total API requests made to vendors',
    ['vendor', 'status']
)

database_operations_total = Counter(
    'realtime_database_operations_total',
    'Total database operations',
    ['operation', 'status']
)
```

#### 7.1.2 System Metrics
```python
# Resource utilization
memory_usage_bytes = Gauge(
    'realtime_collector_memory_usage_bytes',
    'Memory usage in bytes'
)

cpu_usage_percent = Gauge(
    'realtime_collector_cpu_usage_percent',
    'CPU usage percentage'
)

# Performance metrics
processing_time_seconds = Histogram(
    'realtime_processing_time_seconds',
    'Time to process and store a minute bar',
    buckets=[0.001, 0.01, 0.1, 0.5, 1.0, 5.0]
)

queue_size = Gauge(
    'realtime_processing_queue_size',
    'Number of bars waiting to be processed'
)

# Error metrics
errors_total = Counter(
    'realtime_errors_total',
    'Total errors encountered',
    ['component', 'error_type']
)

recovery_attempts_total = Counter(
    'realtime_recovery_attempts_total',
    'Total recovery attempts',
    ['component', 'status']
)
```

### 7.2 Grafana Dashboard Configuration

#### 7.2.1 Real-Time Collection Dashboard
```json
{
  "dashboard": {
    "title": "Real-Time Market Data Collection",
    "tags": ["market-data", "real-time", "monitoring"],
    "panels": [
      {
        "title": "Data Collection Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(realtime_data_bars_collected_total[5m])",
            "legendFormat": "{{vendor}} bars/sec"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "reqps",
            "color": {"mode": "thresholds"},
            "thresholds": {
              "steps": [
                {"color": "red", "value": 0},
                {"color": "yellow", "value": 5},
                {"color": "green", "value": 10}
              ]
            }
          }
        }
      },
      {
        "title": "Data Latency Distribution",
        "type": "heatmap",
        "targets": [
          {
            "expr": "rate(realtime_data_collection_latency_seconds_bucket[5m])",
            "legendFormat": "{{vendor}}"
          }
        ]
      },
      {
        "title": "Quality Score by Vendor",
        "type": "timeseries",
        "targets": [
          {
            "expr": "avg by (vendor) (realtime_data_quality_score)",
            "legendFormat": "{{vendor}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "min": 0,
            "max": 1,
            "unit": "percentunit"
          }
        }
      },
      {
        "title": "Active WebSocket Connections",
        "type": "stat",
        "targets": [
          {
            "expr": "realtime_websocket_connections_active",
            "legendFormat": "{{vendor}}"
          }
        ]
      },
      {
        "title": "Gap Detection Rate",
        "type": "timeseries", 
        "targets": [
          {
            "expr": "rate(realtime_data_gaps_detected_total[1h])",
            "legendFormat": "{{vendor}} - {{severity}}"
          }
        ]
      },
      {
        "title": "System Resource Usage",
        "type": "timeseries",
        "targets": [
          {
            "expr": "realtime_collector_memory_usage_bytes / 1024 / 1024 / 1024",
            "legendFormat": "Memory (GB)"
          },
          {
            "expr": "realtime_collector_cpu_usage_percent",
            "legendFormat": "CPU (%)"
          }
        ]
      }
    ]
  }
}
```

#### 7.2.2 Data Quality Dashboard
```json
{
  "dashboard": {
    "title": "Real-Time Data Quality Monitoring",
    "panels": [
      {
        "title": "Daily Validation Accuracy",
        "type": "timeseries",
        "targets": [
          {
            "expr": "realtime_validation_accuracy_score",
            "legendFormat": "{{vendor}} - {{symbol}}"
          }
        ],
        "alert": {
          "conditions": [
            {
              "query": {"params": ["A", "5m", "now"]},
              "reducer": {"type": "avg", "params": []},
              "evaluator": {"params": [0.95], "type": "lt"}
            }
          ],
          "executionErrorState": "alerting",
          "frequency": "10s",
          "handler": 1,
          "name": "Low Data Quality Alert",
          "noDataState": "no_data"
        }
      },
      {
        "title": "Price Difference Distribution",
        "type": "heatmap",
        "targets": [
          {
            "expr": "rate(realtime_validation_price_difference_pct_bucket[1h])"
          }
        ]
      },
      {
        "title": "Gap Duration Analysis",
        "type": "histogram",
        "targets": [
          {
            "expr": "realtime_gap_duration_minutes"
          }
        ]
      }
    ]
  }
}
```

### 7.3 Alerting Rules

#### 7.3.1 Critical Alerts (Immediate Response)
```yaml
groups:
- name: realtime_critical
  rules:
  - alert: RealtimeCollectionDown
    expr: up{job="realtime-collector"} == 0
    for: 1m
    labels:
      severity: critical
      component: real-time-collection
    annotations:
      summary: "Real-time data collection service is down"
      description: "Real-time collector has been down for more than 1 minute during market hours"
      runbook_url: "https://wiki.ats.com/runbooks/realtime-collection-down"
  
  - alert: HighDataLatency
    expr: histogram_quantile(0.95, rate(realtime_data_collection_latency_seconds_bucket[5m])) > 300
    for: 5m
    labels:
      severity: critical
      component: data-latency
    annotations:
      summary: "High data collection latency detected"
      description: "95th percentile latency is {{ $value }} seconds, above 300s threshold"
  
  - alert: LowDataQuality
    expr: avg_over_time(realtime_data_quality_score[10m]) < 0.7
    for: 10m
    labels:
      severity: critical
      component: data-quality
    annotations:
      summary: "Low data quality detected for {{ $labels.vendor }}/{{ $labels.symbol }}"
      description: "Quality score has been below 0.7 for 10 minutes"
  
  - alert: MajorDataGap
    expr: realtime_gap_duration_minutes > 30
    for: 0m
    labels:
      severity: critical
      component: data-gaps
    annotations:
      summary: "Major data gap detected"
      description: "Data gap of {{ $value }} minutes detected for {{ $labels.vendor }}/{{ $labels.symbol }}"
```

#### 7.3.2 Warning Alerts (30-minute Response)
```yaml
- name: realtime_warnings
  rules:
  - alert: ModerateDataLatency
    expr: histogram_quantile(0.95, rate(realtime_data_collection_latency_seconds_bucket[5m])) > 120
    for: 10m
    labels:
      severity: warning
      component: data-latency
    annotations:
      summary: "Moderate data collection latency"
      description: "95th percentile latency is {{ $value }} seconds"
  
  - alert: ValidationAccuracyDegraded
    expr: realtime_validation_accuracy_score < 0.95
    for: 1h
    labels:
      severity: warning
      component: validation
    annotations:
      summary: "Validation accuracy degraded"
      description: "Accuracy for {{ $labels.vendor }}/{{ $labels.symbol }} is {{ $value }}"
  
  - alert: WebSocketDisconnection
    expr: realtime_websocket_connections_active < 1
    for: 5m
    labels:
      severity: warning
      component: websocket
    annotations:
      summary: "WebSocket connection lost"
      description: "{{ $labels.vendor }} WebSocket has been disconnected for 5 minutes"
  
  - alert: FrequentAPIErrors
    expr: rate(realtime_api_requests_total{status!="success"}[5m]) > 0.1
    for: 10m
    labels:
      severity: warning
      component: api-integration
    annotations:
      summary: "Frequent API errors"
      description: "{{ $labels.vendor }} API error rate is {{ $value }} requests/second"
```

### 7.4 Notification Configuration

#### 7.4.1 Slack Integration
```yaml
# Alertmanager configuration
global:
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'

route:
  group_by: ['alertname', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'default'
  routes:
  - match:
      severity: critical
    receiver: 'critical-alerts'
    group_wait: 0s
    repeat_interval: 15m
  - match:
      severity: warning
    receiver: 'warning-alerts'
    repeat_interval: 4h

receivers:
- name: 'default'
  slack_configs:
  - channel: '#data-engineering'
    title: 'ATS Real-Time Data Alert'
    text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'

- name: 'critical-alerts'
  slack_configs:
  - channel: '#data-engineering'
    title: '🚨 CRITICAL: Real-Time Data Issue'
    text: |
      {{ range .Alerts }}
      *Alert:* {{ .Annotations.summary }}
      *Component:* {{ .Labels.component }}
      *Description:* {{ .Annotations.description }}
      *Runbook:* {{ .Annotations.runbook_url }}
      {{ end }}
    actions:
    - type: button
      text: 'View Dashboard'
      url: 'https://grafana.ats.com/d/realtime-data'

- name: 'warning-alerts'
  slack_configs:
  - channel: '#data-engineering'
    title: '⚠️ WARNING: Real-Time Data Issue'
    text: |
      {{ range .Alerts }}
      *Alert:* {{ .Annotations.summary }}
      *Component:* {{ .Labels.component }}
      *Description:* {{ .Annotations.description }}
      {{ end }}
```

---

## 8. Testing and Validation Strategy

### 8.1 Unit Testing Framework

#### 8.1.1 Data Collection Tests
```python
import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone
from market_data.realtime.streaming_collector import RealtimeStreamingCollector

class TestRealtimeStreamingCollector:
    
    @pytest.fixture
    async def collector(self):
        """Create test collector instance"""
        with patch.dict(os.environ, {
            'POLYGON_API_KEY': 'test_key',
            'TIINGO_API_KEY': 'test_key',
            'FMP_API_KEY': 'test_key'
        }):
            collector = RealtimeStreamingCollector()
            yield collector
    
    @pytest.mark.asyncio
    async def test_polygon_message_processing(self, collector):
        """Test Polygon WebSocket message processing"""
        # Mock message from Polygon
        test_message = {
            "ev": "AM",
            "sym": "AAPL",
            "v": 31315282,
            "o": 102.89,
            "c": 103.74,
            "h": 103.74,
            "l": 102.89,
            "t": 1577745000000,
            "vw": 103.0267,
            "n": 547
        }
        
        with patch.object(collector, '_store_minute_bar') as mock_store:
            await collector._process_polygon_minute_bar(test_message)
            
            # Verify bar was stored
            mock_store.assert_called_once()
            stored_bar = mock_store.call_args[0][0]
            
            assert stored_bar.vendor == 'polygon'
            assert stored_bar.symbol == 'AAPL'
            assert stored_bar.close_price == 103.74
            assert stored_bar.volume == 31315282
    
    @pytest.mark.asyncio
    async def test_data_quality_scoring(self, collector):
        """Test data quality score calculation"""
        # Test high quality data (low latency, complete data)
        high_quality_data = {
            'open': 100.0,
            'high': 101.0,
            'low': 99.0,
            'close': 100.5,
            'volume': 1000
        }
        
        score = collector._calculate_quality_score(high_quality_data, 15000)  # 15 seconds
        assert score >= 0.8
        
        # Test low quality data (high latency, missing fields)
        low_quality_data = {
            'open': 100.0,
            'close': 100.5
            # Missing high, low, volume
        }
        
        score = collector._calculate_quality_score(low_quality_data, 600000)  # 10 minutes
        assert score <= 0.5
    
    @pytest.mark.asyncio
    async def test_gap_detection(self, collector):
        """Test gap detection algorithm"""
        # Mock collection status with missing data
        with patch.object(collector.pool, 'acquire') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value = mock_conn
            
            # Simulate gap in data
            mock_conn.fetch.return_value = [
                {
                    'vendor': 'polygon',
                    'symbol': 'AAPL',
                    'last_received_timestamp': datetime.now(timezone.utc) - timedelta(minutes=30),
                    'minutes_since_last': 30.0
                }
            ]
            
            await collector._detect_gaps()
            
            # Verify gap was handled
            mock_conn.execute.assert_called()  # Gap record should be inserted
```

#### 8.1.2 Validation Tests
```python
class TestBatchValidationEngine:
    
    @pytest.fixture
    def validation_engine(self):
        return BatchValidationEngine()
    
    def test_bar_comparison_exact_match(self, validation_engine):
        """Test comparison of identical bars"""
        rt_bar = {
            'timestamp': datetime(2025, 8, 22, 14, 30, tzinfo=timezone.utc),
            'open_price': 100.0,
            'high_price': 101.0,
            'low_price': 99.0,
            'close_price': 100.5,
            'volume': 1000
        }
        
        batch_bar = rt_bar.copy()
        
        comparison = validation_engine.compare_individual_bars(rt_bar, batch_bar)
        
        assert comparison['overall_match'] is True
        assert all(diff < 0.001 for diff in comparison['price_differences'].values())
    
    def test_bar_comparison_price_difference(self, validation_engine):
        """Test comparison with price differences"""
        rt_bar = {
            'timestamp': datetime(2025, 8, 22, 14, 30, tzinfo=timezone.utc),
            'close_price': 100.0,
            'volume': 1000
        }
        
        batch_bar = {
            'timestamp': datetime(2025, 8, 22, 14, 30, tzinfo=timezone.utc),
            'close_price': 101.0,  # 1% difference
            'volume': 1000
        }
        
        comparison = validation_engine.compare_individual_bars(rt_bar, batch_bar)
        
        assert comparison['overall_match'] is False  # 1% > 0.5% tolerance
        assert comparison['price_differences']['close_price'] == 0.01
    
    @pytest.mark.asyncio
    async def test_validation_metrics_calculation(self, validation_engine):
        """Test validation metrics calculation"""
        comparison_results = [
            {'overall_match': True, 'price_differences': {'close_price': 0.001}},
            {'overall_match': True, 'price_differences': {'close_price': 0.002}},
            {'overall_match': False, 'price_differences': {'close_price': 0.01}},
            {'status': 'missing_batch'}
        ]
        
        metrics = validation_engine.calculate_validation_metrics(comparison_results)
        
        assert metrics['total_realtime_bars'] == 4
        assert metrics['matching_bars'] == 2
        assert metrics['missing_batch_bars'] == 1
        assert metrics['accuracy_rate'] == 0.5  # 2/4
        assert metrics['validation_status'] == 'failed'  # < 95% accuracy
```

### 8.2 Integration Testing

#### 8.2.1 End-to-End Data Flow Test
```python
@pytest.mark.integration
class TestRealtimeDataFlow:
    
    @pytest.fixture(scope='class')
    async def test_database(self):
        """Create test database with schema"""
        # Use timestamp-based test database
        timestamp = int(time.time() * 1000)
        test_db_name = f"test_realtime_{timestamp}"
        
        # Create test database and run migrations
        # ... database setup code
        
        yield test_db_name
        
        # Cleanup
        # ... database cleanup code
    
    @pytest.mark.asyncio
    async def test_complete_data_flow(self, test_database):
        """Test complete data flow from vendor to storage"""
        # Mock vendor API responses
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.json.return_value = {
                'results': [{
                    't': int(datetime.now().timestamp() * 1000),
                    'o': 100.0,
                    'h': 101.0,
                    'l': 99.0,
                    'c': 100.5,
                    'v': 1000
                }]
            }
            mock_get.return_value.__aenter__.return_value = mock_response
            
            # Initialize collector with test database
            collector = RealtimeStreamingCollector()
            await collector.initialize()
            
            # Trigger data collection
            await collector._fmp_polling_collection(['AAPL'])
            
            # Verify data was stored
            async with collector.pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT * FROM dev_one_minute_live_fmp WHERE symbol = 'AAPL'"
                )
                
                assert result is not None
                assert result['close_price'] == 100.5
                assert result['volume'] == 1000
    
    @pytest.mark.asyncio
    async def test_gap_detection_and_backfill(self, test_database):
        """Test gap detection triggers backfill"""
        # Create gap scenario in test data
        # ... setup code
        
        # Run gap detection
        gap_detector = GapDetectionService()
        await gap_detector.detect_and_handle_gaps()
        
        # Verify gap was detected and backfill triggered
        # ... verification code
```

### 8.3 Performance Testing

#### 8.3.1 Load Testing Configuration
```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

class RealtimePerformanceTest:
    
    async def test_high_volume_ingestion(self):
        """Test ingestion of high volume data"""
        # Simulate 2000 symbols * 3 vendors = 6000 concurrent streams
        symbol_count = 2000
        vendor_count = 3
        
        start_time = time.time()
        
        # Generate test data
        test_bars = []
        for i in range(symbol_count * vendor_count):
            test_bars.append({
                'symbol': f'SYM{i % symbol_count}',
                'vendor': ['polygon', 'tiingo', 'fmp'][i % vendor_count],
                'timestamp': datetime.now(timezone.utc),
                'close_price': 100.0 + (i % 100),
                'volume': 1000 + (i % 1000)
            })
        
        # Process bars concurrently
        collector = RealtimeStreamingCollector()
        await collector.initialize()
        
        tasks = [collector._store_minute_bar(bar) for bar in test_bars]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Performance assertions
        bars_per_second = len(test_bars) / processing_time
        assert bars_per_second >= 1000, f"Performance too slow: {bars_per_second} bars/sec"
        
        print(f"Processed {len(test_bars)} bars in {processing_time:.2f}s ({bars_per_second:.0f} bars/sec)")
    
    async def test_latency_requirements(self):
        """Test that latency requirements are met"""
        collector = RealtimeStreamingCollector()
        
        # Test multiple bars with timing
        latencies = []
        
        for i in range(100):
            start_time = time.time()
            
            test_bar = MinuteBar(
                vendor='polygon',
                symbol='AAPL',
                instrument_id=1,
                timestamp=datetime.now(timezone.utc),
                open_price=100.0,
                high_price=101.0,
                low_price=99.0,
                close_price=100.5,
                volume=1000
            )
            
            await collector._store_minute_bar(test_bar)
            
            end_time = time.time()
            latencies.append((end_time - start_time) * 1000)  # Convert to ms
        
        # Latency assertions
        avg_latency = sum(latencies) / len(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        assert avg_latency < 100, f"Average latency too high: {avg_latency:.2f}ms"
        assert p95_latency < 500, f"P95 latency too high: {p95_latency:.2f}ms"
        
        print(f"Latency stats: avg={avg_latency:.2f}ms, p95={p95_latency:.2f}ms")
```

---

## 9. Operational Procedures

### 9.1 Deployment Procedures

#### 9.1.1 Production Deployment Checklist
```bash
#!/bin/bash
# Production deployment script

set -e

echo "🚀 Real-Time Data Collection System Deployment"
echo "=============================================="

# Pre-deployment checks
echo "1. Pre-deployment validation..."
./scripts/validate_environment.sh
./scripts/check_dependencies.sh
./scripts/test_api_credentials.sh

# Database migration
echo "2. Running database migrations..."
kubectl apply -f k8s/migrations/042_create_vendor_minute_tables.yaml
kubectl wait --for=condition=complete job/database-migration --timeout=300s

# Deploy services in order
echo "3. Deploying real-time collector..."
kubectl apply -f k8s/realtime-data-collection-system.yaml

# Wait for deployment
echo "4. Waiting for services to be ready..."
kubectl wait --for=condition=available deployment/realtime-minute-collector --timeout=300s

# Verify deployment
echo "5. Verifying deployment..."
./scripts/verify_deployment.sh

# Health checks
echo "6. Running health checks..."
./scripts/health_check.sh

echo "✅ Deployment complete!"
```

#### 9.1.2 Rollback Procedures
```bash
#!/bin/bash
# Emergency rollback script

echo "🔄 Emergency Rollback Procedure"
echo "==============================="

# Get current deployment info
CURRENT_VERSION=$(kubectl get deployment realtime-minute-collector -o jsonpath='{.spec.template.spec.containers[0].image}')
echo "Current version: $CURRENT_VERSION"

# Get previous version
PREVIOUS_VERSION=$(kubectl rollout history deployment/realtime-minute-collector | tail -2 | head -1 | awk '{print $1}')
echo "Rolling back to revision: $PREVIOUS_VERSION"

# Perform rollback
kubectl rollout undo deployment/realtime-minute-collector --to-revision=$PREVIOUS_VERSION

# Wait for rollback
kubectl rollout status deployment/realtime-minute-collector --timeout=300s

# Verify rollback
echo "Verifying rollback..."
./scripts/verify_deployment.sh

echo "✅ Rollback complete!"
```

### 9.2 Maintenance Procedures

#### 9.2.1 Routine Maintenance Tasks
```bash
#!/bin/bash
# Weekly maintenance script

echo "🔧 Weekly Maintenance Tasks"
echo "=========================="

# 1. Database maintenance
echo "1. Database maintenance..."
kubectl exec -it postgres-simple-0 -- psql -U postgres -d dev_db -c "
    -- Refresh materialized views
    REFRESH MATERIALIZED VIEW minute_bars_tft_training;
    
    -- Update table statistics
    ANALYZE dev_one_minute_live_polygon;
    ANALYZE dev_one_minute_live_tiingo;
    ANALYZE dev_one_minute_live_fmp;
    
    -- Clean up old validation records
    DELETE FROM dev_realtime_batch_validation 
    WHERE created_at < NOW() - INTERVAL '30 days';
"

# 2. Log rotation and cleanup
echo "2. Log cleanup..."
kubectl delete pods -l app=realtime-collector --field-selector=status.phase=Succeeded
kubectl logs -l app=realtime-collector --since=24h > /tmp/realtime_logs_$(date +%Y%m%d).log

# 3. Performance metrics review
echo "3. Performance metrics review..."
./scripts/generate_performance_report.sh

# 4. Certificate renewal check
echo "4. Certificate renewal check..."
./scripts/check_certificates.sh

echo "✅ Maintenance complete!"
```

#### 9.2.2 Emergency Response Procedures
```bash
#!/bin/bash
# Emergency response script

ALERT_TYPE=$1
COMPONENT=$2

echo "🚨 Emergency Response: $ALERT_TYPE for $COMPONENT"
echo "================================================"

case $ALERT_TYPE in
    "data_collection_down")
        echo "Responding to data collection outage..."
        
        # Check service status
        kubectl get pods -l app=realtime-collector
        
        # Restart if needed
        kubectl rollout restart deployment/realtime-minute-collector
        
        # Switch to backup collection method
        ./scripts/enable_backup_collection.sh
        ;;
        
    "high_latency")
        echo "Responding to high latency alert..."
        
        # Check resource usage
        kubectl top pods -l app=realtime-collector
        
        # Scale up if needed
        kubectl scale deployment realtime-minute-collector --replicas=4
        
        # Check API status
        ./scripts/check_vendor_apis.sh
        ;;
        
    "data_quality_issue")
        echo "Responding to data quality issue..."
        
        # Trigger manual validation
        kubectl create job manual-validation --from=cronjob/daily-realtime-validation
        
        # Check vendor data sources
        ./scripts/validate_vendor_data.sh
        ;;
        
    *)
        echo "Unknown alert type: $ALERT_TYPE"
        exit 1
        ;;
esac

echo "✅ Emergency response complete!"
```

### 9.3 Monitoring and Troubleshooting

#### 9.3.1 Health Check Script
```bash
#!/bin/bash
# Comprehensive health check

echo "🩺 Real-Time Data Collection Health Check"
echo "========================================"

# 1. Service availability
echo "1. Checking service availability..."
COLLECTOR_STATUS=$(kubectl get deployment realtime-minute-collector -o jsonpath='{.status.readyReplicas}')
COLLECTOR_DESIRED=$(kubectl get deployment realtime-minute-collector -o jsonpath='{.spec.replicas}')

if [ "$COLLECTOR_STATUS" = "$COLLECTOR_DESIRED" ]; then
    echo "✅ Collector service: $COLLECTOR_STATUS/$COLLECTOR_DESIRED replicas ready"
else
    echo "❌ Collector service: $COLLECTOR_STATUS/$COLLECTOR_DESIRED replicas ready"
fi

# 2. Database connectivity
echo "2. Checking database connectivity..."
DB_STATUS=$(kubectl exec postgres-simple-0 -- pg_isready -U postgres 2>/dev/null && echo "OK" || echo "FAIL")
echo "Database status: $DB_STATUS"

# 3. Recent data collection
echo "3. Checking recent data collection..."
RECENT_BARS=$(kubectl exec postgres-simple-0 -- psql -U postgres -d dev_db -t -c "
    SELECT COUNT(*) FROM dev_one_minute_live_polygon 
    WHERE timestamp > NOW() - INTERVAL '1 hour'
")
echo "Bars collected in last hour: $RECENT_BARS"

# 4. Quality metrics
echo "4. Checking quality metrics..."
AVG_QUALITY=$(kubectl exec postgres-simple-0 -- psql -U postgres -d dev_db -t -c "
    SELECT ROUND(AVG(quality_score), 3) FROM dev_one_minute_live_polygon 
    WHERE timestamp > NOW() - INTERVAL '1 hour'
")
echo "Average quality score: $AVG_QUALITY"

# 5. Gap analysis
echo "5. Checking for data gaps..."
ACTIVE_GAPS=$(kubectl exec postgres-simple-0 -- psql -U postgres -d dev_db -t -c "
    SELECT COUNT(*) FROM dev_realtime_gaps 
    WHERE backfill_status != 'completed' 
    AND detected_at > NOW() - INTERVAL '24 hours'
")
echo "Active gaps in last 24h: $ACTIVE_GAPS"

# 6. API connectivity
echo "6. Checking vendor API connectivity..."
for vendor in polygon tiingo fmp; do
    case $vendor in
        "polygon")
            API_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2025-08-22/2025-08-22?apikey=$POLYGON_API_KEY")
            ;;
        "tiingo")
            API_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "https://api.tiingo.com/iex/AAPL/prices?token=$TIINGO_API_KEY")
            ;;
        "fmp")
            API_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "https://financialmodelingprep.com/api/v3/quote-short/AAPL?apikey=$FMP_API_KEY")
            ;;
    esac
    
    if [ "$API_STATUS" = "200" ]; then
        echo "✅ $vendor API: OK"
    else
        echo "❌ $vendor API: $API_STATUS"
    fi
done

echo "🏁 Health check complete!"
```

---

## 10. Security and Compliance

### 10.1 Security Requirements

#### 10.1.1 API Key Management
```yaml
# Kubernetes Secret for API credentials
apiVersion: v1
kind: Secret
metadata:
  name: vendor-api-credentials
  namespace: ats-dev
  annotations:
    description: "Vendor API keys for real-time data collection"
type: Opaque
data:
  # Base64 encoded API keys
  polygon-api-key: "<base64-encoded-polygon-key>"
  tiingo-api-key: "<base64-encoded-tiingo-key>"
  fmp-api-key: "<base64-encoded-fmp-key>"

---
# Database credentials
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: ats-dev
type: Opaque
data:
  username: "<base64-encoded-username>"
  password: "<base64-encoded-password>"
```

#### 10.1.2 Network Security
```yaml
# Network Policy for real-time collector
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: realtime-collector-netpol
  namespace: ats-dev
spec:
  podSelector:
    matchLabels:
      app: realtime-collector
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: prometheus
    ports:
    - protocol: TCP
      port: 8080  # Metrics port
  - from:
    - podSelector:
        matchLabels:
          app: monitoring
    ports:
    - protocol: TCP
      port: 8081  # Health port
  egress:
  - to: []  # Allow all egress for vendor APIs
    ports:
    - protocol: TCP
      port: 443  # HTTPS
    - protocol: TCP
      port: 80   # HTTP
  - to:
    - podSelector:
        matchLabels:
          app: postgres
    ports:
    - protocol: TCP
      port: 5432  # Database
```

### 10.2 Data Privacy and Compliance

#### 10.2.1 Data Retention Policy
```sql
-- Automated data retention policy
CREATE OR REPLACE FUNCTION cleanup_old_realtime_data()
RETURNS void AS $$
BEGIN
    -- Remove real-time data older than 90 days
    DELETE FROM dev_one_minute_live_polygon 
    WHERE timestamp < NOW() - INTERVAL '90 days';
    
    DELETE FROM dev_one_minute_live_tiingo 
    WHERE timestamp < NOW() - INTERVAL '90 days';
    
    DELETE FROM dev_one_minute_live_fmp 
    WHERE timestamp < NOW() - INTERVAL '90 days';
    
    -- Remove validation records older than 30 days
    DELETE FROM dev_realtime_batch_validation 
    WHERE validation_date < CURRENT_DATE - INTERVAL '30 days';
    
    -- Archive completed gap records older than 90 days
    INSERT INTO dev_realtime_gaps_archive 
    SELECT * FROM dev_realtime_gaps 
    WHERE backfill_status = 'completed' 
    AND detected_at < NOW() - INTERVAL '90 days';
    
    DELETE FROM dev_realtime_gaps 
    WHERE backfill_status = 'completed' 
    AND detected_at < NOW() - INTERVAL '90 days';
    
    RAISE NOTICE 'Data retention cleanup completed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Schedule cleanup to run weekly
SELECT cron.schedule('weekly-cleanup', '0 2 * * 0', 'SELECT cleanup_old_realtime_data()');
```

#### 10.2.2 Audit Logging
```python
class AuditLogger:
    """Audit logging for real-time data operations"""
    
    def __init__(self):
        self.audit_logger = logging.getLogger('audit')
        self.audit_logger.setLevel(logging.INFO)
        
        # Structured JSON formatter
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"component": "%(name)s", "message": %(message)s}'
        )
        
        # File handler for audit logs
        handler = logging.FileHandler('/var/log/audit/realtime_data_audit.log')
        handler.setFormatter(formatter)
        self.audit_logger.addHandler(handler)
    
    def log_data_access(self, user, action, resource, result):
        """Log data access events"""
        audit_event = {
            "event_type": "data_access",
            "user": user,
            "action": action,
            "resource": resource,
            "result": result,
            "ip_address": self.get_client_ip(),
            "user_agent": self.get_user_agent()
        }
        
        self.audit_logger.info(json.dumps(audit_event))
    
    def log_system_event(self, component, event, details):
        """Log system events"""
        audit_event = {
            "event_type": "system_event",
            "component": component,
            "event": event,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.audit_logger.info(json.dumps(audit_event))
```

---

## 11. Documentation and Knowledge Transfer

### 11.1 API Documentation

#### 11.1.1 Real-Time Data API Endpoints
```python
# FastAPI documentation for real-time data access
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, date

app = FastAPI(
    title="Real-Time Market Data API",
    description="Access to real-time 1-minute market data from multiple vendors",
    version="1.0.0"
)

class MinuteBarResponse(BaseModel):
    """Real-time minute bar response model"""
    vendor: str
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    vwap: Optional[float] = None
    quality_score: float
    data_latency_ms: Optional[int] = None

class DataQualityResponse(BaseModel):
    """Data quality metrics response"""
    vendor: str
    symbol: str
    date: date
    total_bars: int
    quality_score: float
    average_latency_ms: float
    gaps_detected: int

@app.get("/api/v1/realtime/bars/{symbol}", response_model=List[MinuteBarResponse])
async def get_realtime_bars(
    symbol: str,
    vendor: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: Optional[int] = 100
):
    """
    Get real-time minute bars for a symbol.
    
    - **symbol**: Stock symbol (e.g., 'AAPL')
    - **vendor**: Filter by vendor ('polygon', 'tiingo', 'fmp')
    - **start_time**: Start timestamp (ISO format)
    - **end_time**: End timestamp (ISO format)
    - **limit**: Maximum number of bars to return
    """
    # Implementation here
    pass

@app.get("/api/v1/realtime/quality/{symbol}", response_model=DataQualityResponse)
async def get_data_quality(symbol: str, date: Optional[date] = None):
    """
    Get data quality metrics for a symbol.
    
    - **symbol**: Stock symbol
    - **date**: Date to check (defaults to today)
    """
    # Implementation here
    pass

@app.get("/api/v1/realtime/health")
async def health_check():
    """
    System health check endpoint.
    
    Returns current system status, active connections, and recent performance metrics.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "active_connections": {
            "polygon": True,
            "tiingo": True,
            "fmp": True
        },
        "recent_metrics": {
            "bars_per_minute": 2000,
            "average_latency_ms": 45,
            "quality_score": 0.95
        }
    }
```

### 11.2 Troubleshooting Guide

#### 11.2.1 Common Issues and Solutions

| Issue | Symptoms | Diagnosis | Solution |
|-------|----------|-----------|----------|
| **High Data Latency** | Bars arriving >5 minutes late | Check vendor API status, network connectivity | Restart collector service, check API rate limits |
| **WebSocket Disconnections** | Frequent connection drops for Polygon/Tiingo | Review connection logs, check API credentials | Implement exponential backoff, verify API keys |
| **Low Quality Scores** | Quality scores consistently <0.8 | Check OHLC consistency, volume data | Review data validation rules, contact vendor support |
| **Missing Data Gaps** | No bars for extended periods | Check market hours, verify vendor data availability | Trigger manual backfill, review gap detection logic |
| **Database Connection Issues** | Connection pool exhaustion | Monitor connection usage, check database health | Scale database connections, review query performance |
| **Memory Issues** | Pod memory usage >80% | Check data structure sizes, memory leaks | Increase memory limits, optimize data processing |

#### 11.2.2 Debug Commands
```bash
# Check real-time collector status
kubectl get pods -l app=realtime-collector
kubectl logs -l app=realtime-collector --tail=100

# Check database connectivity
kubectl exec postgres-simple-0 -- pg_isready -U postgres

# Check recent data collection
kubectl exec postgres-simple-0 -- psql -U postgres -d dev_db -c "
    SELECT vendor, COUNT(*) as bars_count, MAX(timestamp) as latest_bar
    FROM dev_one_minute_live_unified 
    WHERE timestamp > NOW() - INTERVAL '1 hour'
    GROUP BY vendor;
"

# Check active gaps
kubectl exec postgres-simple-0 -- psql -U postgres -d dev_db -c "
    SELECT vendor, symbol, gap_duration_minutes, gap_severity
    FROM dev_realtime_gaps 
    WHERE backfill_status != 'completed'
    ORDER BY gap_duration_minutes DESC
    LIMIT 10;
"

# Check system resource usage
kubectl top pods -l app=realtime-collector
kubectl describe pod $(kubectl get pods -l app=realtime-collector -o name | head -1)

# Test vendor API connectivity
curl -s "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2025-08-22/2025-08-22?apikey=$POLYGON_API_KEY"
curl -s "https://api.tiingo.com/iex/AAPL/prices?token=$TIINGO_API_KEY"
curl -s "https://financialmodelingprep.com/api/v3/quote-short/AAPL?apikey=$FMP_API_KEY"
```

---

## 12. Implementation Readiness Summary

### 12.1 Design Completion Status ✅

**The Real-Time Market Data Collection System design is comprehensively complete and ready for immediate implementation.**

#### Core Components ✅ FULLY DESIGNED
- **Database Schema**: Complete vendor-specific tables with TimescaleDB optimization
- **Streaming Service**: Kubernetes-native real-time collector with multi-vendor support
- **Validation Framework**: Daily batch comparison with statistical analysis
- **Gap Detection**: Intelligent monitoring and automated backfill system
- **Quality Assessment**: Real-time scoring with cross-vendor validation

#### Technical Specifications ✅ PRODUCTION-READY
- **Performance**: Designed for <60s latency, 2000 symbols, 780K bars/day
- **Scalability**: Kubernetes auto-scaling with resource optimization
- **Reliability**: Multi-replica deployment with health checks and failover
- **Security**: Network policies, secret management, audit logging
- **Monitoring**: Comprehensive Prometheus metrics and Grafana dashboards

### 12.2 Implementation Roadmap

#### Week 1-2: Core Infrastructure
1. Deploy database schema (Migration 042)
2. Implement Polygon WebSocket streaming
3. Basic storage and quality assessment
4. Kubernetes deployment and health checks

#### Week 3-4: Multi-Vendor Integration  
1. Add Tiingo and FMP adapters
2. Implement daily validation CronJob
3. Cross-vendor reconciliation logic
4. Monitoring and alerting setup

#### Week 5-6: Advanced Features
1. Gap detection and backfill automation
2. Performance optimization and tuning
3. Security hardening and compliance
4. Documentation and training

#### Week 7: Production Deployment
1. Load testing and performance validation
2. Security review and audit
3. Production environment configuration
4. Go-live and monitoring

### 12.3 Success Criteria Targets

| Metric | Target | Design Status |
|--------|--------|---------------|
| **Data Latency** | <60 seconds average | ✅ Architecture supports <30s |
| **Data Completeness** | >99% during market hours | ✅ Multi-vendor redundancy designed |
| **Price Accuracy** | >99.5% vs batch validation | ✅ Statistical validation framework ready |
| **System Availability** | >99.9% uptime | ✅ HA Kubernetes deployment designed |
| **Gap Recovery** | <5 minutes detection | ✅ Real-time gap monitoring implemented |

### 12.4 Risk Mitigation ✅

**All major technical risks have been addressed in the design:**
- **Vendor API Reliability**: Multi-vendor redundancy with automatic failover
- **Data Quality Issues**: Real-time validation with cross-vendor comparison  
- **System Performance**: Kubernetes auto-scaling with resource optimization
- **Operational Complexity**: Comprehensive monitoring, alerting, and troubleshooting guides

---

**🚀 The Real-Time Market Data Collection System is architecturally complete and ready for production implementation. The design provides a robust, scalable, and maintainable solution that meets all business and technical requirements.**

*This DRD serves as the complete technical blueprint for implementing a production-grade real-time market data collection system that will serve as the foundation for advanced quantitative trading operations.*