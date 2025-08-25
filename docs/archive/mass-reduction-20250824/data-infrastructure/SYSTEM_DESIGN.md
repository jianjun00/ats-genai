# 🏗️ Data Infrastructure System Design

**Architecture, Data Flows, and Storage Patterns**

---

## 🎯 Architecture Overview

### **High-Level Data Architecture**
```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INFRASTRUCTURE                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Market Data   │  │   Economic      │  │  Alternative    │
│   Vendors       │  │   Data Sources  │  │  Data Sources   │
│                 │  │                 │  │                 │
│ • Polygon       │  │ • FRED API      │  │ • News APIs     │
│ • Tiingo        │  │ • Treasury      │  │ • Sentiment     │
│ • Alpha Vantage │  │ • BLS Data      │  │ • Social Media  │
│ • FMP           │  │ • FOMC Releases │  │ • Analyst Recs  │
│ • Finnhub       │  │                 │  │                 │
└─────────┬───────┘  └─────────┬───────┘  └─────────┬───────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                             │
│                                                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│ │ Real-Time       │ │ Batch ETL       │ │ Event-Driven    │    │
│ │ Collectors      │ │ Processors      │ │ Streaming       │    │
│ │                 │ │                 │ │                 │    │
│ │ • Live Feeds    │ │ • Daily Jobs    │ │ • Market Events │    │
│ │ • Rate Limited  │ │ • Backfills     │ │ • Corporate     │    │
│ │ • WebSockets    │ │ • Historical    │ │   Actions       │    │
│ │ • REST APIs     │ │ • Reconciliation│ │ • Earnings      │    │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘    │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DATA PROCESSING LAYER                        │
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │                DATA QUALITY PIPELINE                       │ │
│ │                                                             │ │
│ │ Schema        Business      Anomaly        Cross-Vendor    │ │
│ │ Validation -> Rules Check -> Detection  -> Reconciliation  │ │
│ │    │             │             │              │            │ │
│ │    ▼             ▼             ▼              ▼            │ │
│ │ Type Check   Range Check   Outliers    Price Variance     │ │
│ │ Format       Null Check    Patterns    Volume Check       │ │
│ │ Completeness Logic Rules   ML Models   Correlation        │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                │                                │
│                                ▼                                │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │              TRANSFORMATION PIPELINE                        │ │
│ │                                                             │ │
│ │ Corporate      Price         Technical      Data            │ │
│ │ Actions   ->   Adjustments -> Indicators -> Normalization  │ │
│ │    │              │             │             │             │ │
│ │    ▼              ▼             ▼             ▼             │ │
│ │ Splits         Split Adj    RSI/MACD     Standard Format   │ │
│ │ Dividends      Dividend Adj  Bollinger   Time Zones       │ │
│ │ Spin-offs      Lookback     Moving Avg   Decimal Places   │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                               │
│                                                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│ │ TimescaleDB     │ │ PostgreSQL      │ │ Redis Cache     │    │
│ │                 │ │                 │ │                 │    │
│ │ • Time Series   │ │ • Metadata      │ │ • Hot Data      │    │
│ │ • OHLCV Data    │ │ • Instruments   │ │ • API Results   │    │
│ │ • Indicators    │ │ • Corporate     │ │ • Sessions      │    │
│ │ • Compressed    │ │   Actions       │ │ • Rate Limits   │    │
│ │ • Indexed       │ │ • Data Lineage  │ │                 │    │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘    │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ACCESS LAYER                                │
│                                                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│ │ Data APIs       │ │ Analytics APIs  │ │ Direct Access   │    │
│ │                 │ │                 │ │                 │    │
│ │ • REST Endpoints│ │ • Aggregations  │ │ • SQL Queries   │    │
│ │ • GraphQL       │ │ • Calculations  │ │ • Data Exports  │    │
│ │ • Streaming     │ │ • Derived Metrics│ │ • Admin Tools   │    │
│ │ • Cached        │ │ • Performance   │ │                 │    │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Data Flow Patterns

### **Real-Time Data Flow**
```
Market Open
    │
    ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Vendor APIs     │────▶│ Rate Limiters   │────▶│ Data Collectors │
│                 │     │                 │     │                 │
│ • Polygon WS    │     │ • 3 req/sec     │     │ • Python        │
│ • Tiingo Stream │     │ • 1 req/sec     │     │ • Async I/O     │
│ • AV Real-time  │     │ • Token Bucket  │     │ • Error Handling│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Quality Gates   │────▶│ TimescaleDB     │────▶│ Notification    │
│                 │     │                 │     │                 │
│ • Schema Check  │     │ • Compressed    │     │ • Alerts        │
│ • Range Valid   │     │ • Indexed       │     │ • Monitoring    │
│ • Duplicate Det │     │ • Partitioned   │     │ • Dashboards    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### **Batch Processing Flow**
```
Scheduled Job (Daily 8PM)
    │
    ▼
┌─────────────────┐     ┌─────────────────┐
│ Data Discovery  │────▶│ Vendor Polling  │
│                 │     │                 │
│ • Missing Dates │     │ • All Vendors   │
│ • Symbol Lists  │     │ • Parallel Exec │
│ • Priority Queue│     │ • Checkpoints   │
└─────────────────┘     └─────────────────┘
                                 │
                                 ▼
┌─────────────────┐     ┌─────────────────┐
│ Data Validation │────▶│ Reconciliation  │
│                 │     │                 │
│ • Cross-Vendor  │     │ • Price Variance│
│ • Anomaly Detect│     │ • Volume Check  │
│ • Completeness  │     │ • Quality Score │
└─────────────────┘     └─────────────────┘
                                 │
                                 ▼
┌─────────────────┐     ┌─────────────────┐
│ Storage Update  │────▶│ Cache Refresh   │
│                 │     │                 │
│ • Bulk Insert   │     │ • Hot Data      │
│ • Index Rebuild │     │ • API Results   │
│ • Statistics    │     │ • Invalidation  │
└─────────────────┘     └─────────────────┘
```

---

## 🗄️ Data Storage Design

### **TimescaleDB Schema Design**

#### **Core Time-Series Tables**
```sql
-- Daily price data with time-series optimization
CREATE TABLE dev_daily_prices (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DECIMAL(12,4) NOT NULL,
    high DECIMAL(12,4) NOT NULL,
    low DECIMAL(12,4) NOT NULL,
    close DECIMAL(12,4) NOT NULL,
    volume BIGINT NOT NULL,
    adjusted_close DECIMAL(12,4),
    vendor VARCHAR(20) NOT NULL,
    quality_score DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('dev_daily_prices', 'time');

-- Compression policy for older data
SELECT add_compression_policy('dev_daily_prices', INTERVAL '90 days');
```

#### **Optimized Indexes**
```sql
-- Primary access patterns
CREATE INDEX idx_daily_prices_symbol_time 
ON dev_daily_prices (symbol, time DESC);

-- Multi-vendor queries
CREATE INDEX idx_daily_prices_vendor_time 
ON dev_daily_prices (vendor, time DESC);

-- Quality-based filtering
CREATE INDEX idx_daily_prices_quality 
ON dev_daily_prices (quality_score, time DESC) 
WHERE quality_score >= 0.95;
```

### **Data Partitioning Strategy**
```sql
-- Time-based partitioning (TimescaleDB chunks)
CHUNK_TIME_INTERVAL: 7 days
COMPRESSION_AFTER: 90 days
RETENTION_POLICY: 10 years

-- Symbol-based partitioning for large tables
CREATE TABLE dev_minute_prices_partition (
    LIKE dev_minute_prices
) PARTITION BY HASH (symbol);

-- Create partitions for balanced distribution
CREATE TABLE dev_minute_prices_0 PARTITION OF dev_minute_prices_partition
FOR VALUES WITH (modulus 8, remainder 0);
-- ... repeat for remainder 1-7
```

---

## 🔄 Multi-Vendor Reconciliation

### **Data Reconciliation Algorithm**
```python
class DataReconciliationEngine:
    def reconcile_daily_prices(self, symbol: str, date: datetime) -> ReconciledPrice:
        """
        Multi-vendor price reconciliation with quality scoring
        """
        vendor_prices = self.get_vendor_prices(symbol, date)
        
        if len(vendor_prices) == 1:
            return vendor_prices[0]  # Single source
        
        # Statistical reconciliation
        close_prices = [p.close for p in vendor_prices]
        median_close = statistics.median(close_prices)
        std_dev = statistics.stdev(close_prices)
        
        # Quality scoring based on variance
        quality_scores = []
        for price in vendor_prices:
            variance = abs(price.close - median_close)
            score = max(0.0, 1.0 - (variance / (2 * std_dev)))
            quality_scores.append(score)
        
        # Weighted average with quality scores
        weighted_price = self._calculate_weighted_average(
            vendor_prices, quality_scores
        )
        
        return ReconciledPrice(
            symbol=symbol,
            date=date,
            open=weighted_price.open,
            high=max(p.high for p in vendor_prices),
            low=min(p.low for p in vendor_prices),
            close=weighted_price.close,
            volume=sum(p.volume for p in vendor_prices) // len(vendor_prices),
            quality_score=max(quality_scores),
            source_count=len(vendor_prices)
        )
```

### **Reconciliation Rules**
```yaml
reconciliation_rules:
  price_variance_threshold: 0.05    # 5% max variance
  volume_variance_threshold: 0.20   # 20% max variance
  
  quality_weights:
    polygon: 0.40      # Premium source
    tiingo: 0.30       # High quality
    fmp: 0.20          # Medium quality  
    alpha_vantage: 0.10 # Lower priority
  
  conflict_resolution:
    high_variance: "flag_for_review"
    missing_vendor: "use_best_available"
    all_vendors_agree: "auto_accept"
```

---

## 📈 Data Quality Framework

### **Quality Dimensions**
```python
@dataclass
class DataQualityMetrics:
    completeness: float     # % of expected records
    accuracy: float         # Cross-vendor price accuracy
    timeliness: float       # Data freshness score
    consistency: float      # Schema compliance
    uniqueness: float       # Duplicate detection
    validity: float         # Business rule compliance
    
    def overall_score(self) -> float:
        """Weighted average of all dimensions"""
        weights = [0.25, 0.20, 0.15, 0.15, 0.10, 0.15]
        scores = [self.completeness, self.accuracy, self.timeliness,
                 self.consistency, self.uniqueness, self.validity]
        return sum(w * s for w, s in zip(weights, scores))
```

### **Automated Quality Checks**
```python
class DataQualityValidator:
    def validate_daily_prices(self, prices: List[DailyPrice]) -> ValidationResult:
        """Comprehensive data quality validation"""
        issues = []
        
        # Schema validation
        for price in prices:
            if not self.validate_schema(price):
                issues.append(f"Schema error for {price.symbol}")
        
        # Business rule validation
        for price in prices:
            if price.high < price.low:
                issues.append(f"High < Low for {price.symbol}")
            if price.close < 0:
                issues.append(f"Negative close for {price.symbol}")
            if price.volume < 0:
                issues.append(f"Negative volume for {price.symbol}")
        
        # Statistical anomaly detection
        anomalies = self.detect_anomalies(prices)
        issues.extend(anomalies)
        
        return ValidationResult(
            passed=len(issues) == 0,
            issues=issues,
            quality_score=self.calculate_quality_score(prices, issues)
        )
```

---

## 🔄 Event-Driven Architecture

### **Event Processing Pipeline**
```
Corporate Action Event
    │
    ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Event Detection │────▶│ Impact Analysis │────▶│ Price Adjustment│
│                 │     │                 │     │                 │
│ • Split Ratio   │     │ • Affected Dates│     │ • Historical    │
│ • Ex-Date       │     │ • Price Impact  │     │ • Forward Adj   │
│ • Record Date   │     │ • Volume Impact │     │ • Lookback      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Notification    │◀────│ Database Update │◀────│ Validation      │
│                 │     │                 │     │                 │
│ • ML Platform   │     │ • Bulk Update   │     │ • Consistency   │
│ • Analytics     │     │ • Index Rebuild │     │ • Completeness  │
│ • Users         │     │ • Cache Clear   │     │ • Accuracy      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### **Event Types**
```python
class EventType(Enum):
    STOCK_SPLIT = "stock_split"
    DIVIDEND = "dividend"
    SPIN_OFF = "spin_off"
    SYMBOL_CHANGE = "symbol_change"
    DELISTING = "delisting"
    EARNINGS = "earnings"
    ECONOMIC_RELEASE = "economic_release"
```

---

## 🚀 Performance Optimization

### **Ingestion Performance**
```python
# Async batch processing for high throughput
async def batch_ingest_prices(self, prices: List[DailyPrice], batch_size: int = 1000):
    """Optimized batch ingestion with connection pooling"""
    
    async with self.get_db_pool() as pool:
        tasks = []
        for i in range(0, len(prices), batch_size):
            batch = prices[i:i + batch_size]
            task = self._ingest_batch(pool, batch)
            tasks.append(task)
        
        # Process batches concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any failed batches
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                await self._retry_failed_batch(prices[i*batch_size:(i+1)*batch_size])
```

### **Query Optimization**
```sql
-- Continuous aggregate for common analytics
CREATE MATERIALIZED VIEW daily_portfolio_performance
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', time) AS day,
    symbol,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume
FROM dev_minute_prices
GROUP BY day, symbol;

-- Compression for historical data
SELECT add_compression_policy('dev_daily_prices', INTERVAL '90 days');
SELECT add_retention_policy('dev_daily_prices', INTERVAL '10 years');
```

---

## 💾 Backup & Recovery

### **Backup Strategy**
```yaml
backup_schedule:
  incremental: "every 4 hours"
  full: "daily at 2AM"
  archive: "weekly to cold storage"
  
retention:
  incremental: "7 days"
  full: "30 days"  
  archive: "10 years"
  
recovery_targets:
  rto: "< 1 hour"      # Recovery Time Objective
  rpo: "< 15 minutes"  # Recovery Point Objective
```

### **Disaster Recovery**
- **Multi-AZ Deployment**: Primary/replica setup
- **Cross-Region Backup**: Weekly full backups to different region
- **Point-in-Time Recovery**: Continuous WAL archiving
- **Data Validation**: Post-recovery integrity checks

---

*This data infrastructure design supports petabyte-scale financial data processing with enterprise-grade reliability and performance.*