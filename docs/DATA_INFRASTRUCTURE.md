# 📊 Data Infrastructure

**Multi-Vendor Data Pipelines, Storage, ETL, and Quality Management**

Complete data infrastructure documentation covering all data ingestion, processing, storage, quality management, and multi-vendor reconciliation systems.

---

## 🎯 Infrastructure Overview

The Data Infrastructure provides comprehensive data ingestion, processing, storage, and quality management for the ATS platform. It handles multi-vendor data reconciliation, real-time streaming, historical backfills, and data catalog management.

### **Core Capabilities**
- **Multi-Vendor Data Ingestion** - Polygon, Tiingo, Alpha Vantage, FMP, Finnhub
- **Real-Time Streaming** - Live market data with sub-second latency
- **Historical Backfill** - Efficient 30-year data loading across all vendors
- **Data Reconciliation** - Cross-vendor validation and conflict resolution
- **Data Quality Management** - Automated validation, cleansing, and monitoring
- **Event Processing** - Earnings, economic events, corporate actions

### **Key Technologies**
- **TimescaleDB** - Time-series optimized PostgreSQL
- **Redis** - Caching and temporary storage
- **Python** - Data processing and ETL pipelines
- **Kubernetes** - Containerized job execution
- **Prometheus/Grafana** - Monitoring and alerting

---

## 🚀 Quick Start

### **Data Pipeline Execution**
```bash
# Run real-time data collection
python scripts/run_dev.py deploy --file k8s/realtime-data-collection-system.yaml

# Execute historical backfill
python scripts/run_dev.py deploy --file k8s/polygon-10year-backfill-job.yaml

# Data quality validation
python scripts/run_dev.py deploy --file k8s/data-quality-validation-job.yaml

# Multi-vendor reconciliation
python scripts/run_dev.py deploy --file k8s/cross-vendor-reconciliation-job.yaml
```

### **Data Access Patterns**
```python
# Query time-series data
query = """
SELECT symbol, date, close, volume 
FROM dev_daily_prices 
WHERE symbol IN ('AAPL', 'MSFT') 
  AND date >= '2024-01-01'
ORDER BY symbol, date DESC
"""

# Cross-vendor reconciliation query
reconciliation_query = """
SELECT symbol, date, 
       COUNT(DISTINCT vendor) as vendor_count,
       AVG(close) as avg_close,
       STDDEV(close) as price_variance
FROM dev_multi_vendor_prices 
WHERE date = '2024-01-15'
GROUP BY symbol, date
HAVING COUNT(DISTINCT vendor) > 1
"""
```

---

## 🌐 Multi-Vendor Architecture

### **Vendor Capabilities Matrix**
| Vendor | Real-time | Historical | Fundamentals | Options | International | Cost |
|--------|-----------|------------|--------------|---------|---------------|------|
| **Polygon** | ✅ | ✅ 30yr | ✅ | ✅ | Limited | High |
| **Tiingo** | ✅ | ✅ 30yr | ❌ | ❌ | ✅ | Medium |
| **Alpha Vantage** | ✅ | ✅ 20yr | ✅ | ❌ | Limited | Low |
| **FMP** | ✅ | ✅ 30yr | ✅ | ❌ | ✅ | Medium |
| **Finnhub** | ✅ | Limited | ✅ | ❌ | ✅ | Medium |

### **Data Source Integration**
```python
class MultiVendorDataManager:
    def __init__(self):
        self.vendors = {
            'polygon': PolygonAdapter(),
            'tiingo': TiingoAdapter(), 
            'alphavantage': AlphaVantageAdapter(),
            'fmp': FMPAdapter(),
            'finnhub': FinnhubAdapter()
        }
    
    async def collect_market_data(
        self, 
        symbols: List[str], 
        date: datetime,
        vendors: List[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Collect data from multiple vendors in parallel
        """
        vendors = vendors or list(self.vendors.keys())
        tasks = []
        
        for vendor_name in vendors:
            vendor = self.vendors[vendor_name]
            task = vendor.get_daily_prices(symbols, date)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            vendor: result 
            for vendor, result in zip(vendors, results)
            if not isinstance(result, Exception)
        }
```

---

## 🔄 Data Processing Workflows

### **Real-Time Processing Pipeline**
```
Market Open → Stream Collection → Rate Limiting → Quality Gates → Storage → Alerts
    ↓              ↓                  ↓              ↓            ↓        ↓
 [Vendors]    [Collectors]      [API Limits]   [Validation]  [TimescaleDB] [Users]
              [Resilient]       [Backoff]      [Business     [Partitioned] [Slack]
              [Monitored]       [Circuit       Rules]        [Indexed]     [Email]
                               Breakers]
```

### **Batch Processing Architecture**
```
Scheduled Job → Data Extraction → Transformation → Quality Check → Load → Verification
      ↓              ↓               ↓              ↓           ↓         ↓
  [Cron/K8s]    [Parallel API]   [Cleansing]   [Validation]  [Bulk     [QA Reports]
  [Resource     [Calls Rate      [Format       [Schema       Insert]   [Metrics]
   Limited]      Limited]         Standard]     Check]       [UPSERT]  [Alerts]
```

### **Data Quality Pipeline**
```python
class DataQualityEngine:
    def validate_market_data(self, data: pd.DataFrame, vendor: str) -> QualityReport:
        """
        Comprehensive data quality validation
        """
        checks = []
        
        # Schema validation
        checks.append(self.validate_schema(data))
        
        # Business rule validation
        checks.append(self.validate_business_rules(data))
        
        # Cross-vendor consistency check
        checks.append(self.validate_cross_vendor_consistency(data, vendor))
        
        # Anomaly detection
        checks.append(self.detect_anomalies(data))
        
        # Data completeness check
        checks.append(self.validate_completeness(data))
        
        overall_score = sum(check.score for check in checks) / len(checks)
        
        return QualityReport(
            vendor=vendor,
            timestamp=datetime.utcnow(),
            checks=checks,
            overall_score=overall_score,
            passed=overall_score >= 0.95
        )
```

---

## 🗄️ Storage Architecture

### **TimescaleDB Optimization**
```sql
-- Hypertable creation for time-series data
CREATE TABLE dev_daily_prices (
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL, 
    open DECIMAL(10,4),
    high DECIMAL(10,4),
    low DECIMAL(10,4),
    close DECIMAL(10,4),
    volume BIGINT,
    vendor VARCHAR(20),
    quality_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('dev_daily_prices', 'date', chunk_time_interval => interval '1 month');

-- Create indexes for fast queries
CREATE INDEX ON dev_daily_prices (symbol, date DESC);
CREATE INDEX ON dev_daily_prices (vendor, date DESC);
CREATE INDEX ON dev_daily_prices (date DESC, symbol) WHERE vendor = 'polygon';
```

### **Data Partitioning Strategy**
```sql
-- Automatic compression for older data
SELECT add_compression_policy('dev_daily_prices', INTERVAL '7 days');

-- Retention policy for dev environment 
SELECT add_retention_policy('dev_daily_prices', INTERVAL '2 years');

-- Continuous aggregates for common queries
CREATE MATERIALIZED VIEW daily_ohlcv_summary
WITH (timescaledb.continuous) AS
SELECT symbol, 
       time_bucket('1 day', date) as day,
       first(open, date) as open,
       max(high) as high,
       min(low) as low, 
       last(close, date) as close,
       sum(volume) as volume,
       count(*) as vendor_count
FROM dev_daily_prices 
GROUP BY symbol, day;
```

### **Multi-Vendor Data Model**
```python
# Unified data model across vendors
@dataclass
class MarketDataRecord:
    symbol: str
    date: datetime
    open: Decimal
    high: Decimal  
    low: Decimal
    close: Decimal
    volume: int
    vendor: str
    quality_score: float
    raw_data: Dict[str, Any]  # Vendor-specific fields
    
    def validate(self) -> bool:
        """Validate business rules"""
        return all([
            self.high >= self.low,
            self.high >= self.open,
            self.high >= self.close,
            self.low <= self.open,
            self.low <= self.close,
            self.volume >= 0,
            0 <= self.quality_score <= 1.0
        ])
```

---

## 🔍 Data Quality & Monitoring

### **Quality Metrics Dashboard**
```python
class DataQualityDashboard:
    def generate_quality_report(self, date: datetime) -> QualityDashboard:
        """
        Generate comprehensive data quality dashboard
        """
        return QualityDashboard(
            date=date,
            metrics={
                'completeness': self.calculate_completeness(date),
                'accuracy': self.calculate_cross_vendor_accuracy(date), 
                'timeliness': self.calculate_ingestion_latency(date),
                'consistency': self.calculate_schema_consistency(date),
                'uniqueness': self.calculate_duplicate_rate(date)
            },
            vendor_scores={
                vendor: self.calculate_vendor_score(vendor, date)
                for vendor in ['polygon', 'tiingo', 'alphavantage', 'fmp']
            },
            alerts=self.get_quality_alerts(date),
            recommendations=self.generate_recommendations(date)
        )
```

### **Cross-Vendor Reconciliation**
```python
class CrossVendorReconciler:
    def reconcile_daily_prices(self, symbol: str, date: datetime) -> ReconciliationResult:
        """
        Reconcile price data across multiple vendors
        """
        vendor_data = {}
        for vendor in self.active_vendors:
            data = self.get_vendor_data(vendor, symbol, date)
            if data:
                vendor_data[vendor] = data
        
        if len(vendor_data) < 2:
            return ReconciliationResult(status="insufficient_data")
            
        # Calculate price variance
        prices = [data.close for data in vendor_data.values()]
        mean_price = statistics.mean(prices)
        std_dev = statistics.stdev(prices) if len(prices) > 1 else 0
        variance_pct = (std_dev / mean_price) * 100
        
        # Flag significant discrepancies  
        if variance_pct > 0.5:  # 0.5% threshold
            return ReconciliationResult(
                status="discrepancy_detected",
                variance_pct=variance_pct,
                vendor_data=vendor_data,
                recommended_action="manual_review"
            )
            
        # Use majority voting or highest quality source
        best_source = max(
            vendor_data.items(), 
            key=lambda x: x[1].quality_score
        )
        
        return ReconciliationResult(
            status="reconciled",
            canonical_data=best_source[1],
            variance_pct=variance_pct,
            confidence=min(variance_pct / 0.5, 1.0)
        )
```

---

## 📈 Historical Backfill System

### **30-Year Backfill Architecture**
```python
class HistoricalBackfillOrchestrator:
    def __init__(self):
        self.checkpoint_manager = CheckpointManager()
        self.rate_limiter = RateLimiter()
        self.quality_validator = DataQualityEngine()
    
    async def execute_backfill(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        vendors: List[str]
    ) -> BackfillResult:
        """
        Execute large-scale historical backfill with checkpoints
        """
        total_days = (end_date - start_date).days
        completed_days = 0
        
        for current_date in self.date_range(start_date, end_date):
            # Check if already completed
            if self.checkpoint_manager.is_completed(current_date, vendors):
                completed_days += 1
                continue
                
            # Rate limit API calls
            await self.rate_limiter.acquire(vendors)
            
            try:
                # Collect data from all vendors in parallel
                vendor_data = await self.collect_data_parallel(
                    symbols, current_date, vendors
                )
                
                # Quality validation
                quality_results = {}
                for vendor, data in vendor_data.items():
                    quality_results[vendor] = self.quality_validator.validate_market_data(
                        data, vendor
                    )
                
                # Store high-quality data only
                await self.store_validated_data(vendor_data, quality_results)
                
                # Update checkpoint
                self.checkpoint_manager.mark_completed(current_date, vendors)
                completed_days += 1
                
                # Progress reporting
                progress = completed_days / total_days * 100
                await self.report_progress(progress, current_date)
                
            except Exception as e:
                await self.handle_backfill_error(e, current_date, vendors)
                
        return BackfillResult(
            total_days=total_days,
            completed_days=completed_days,
            success_rate=completed_days / total_days,
            quality_summary=self.generate_quality_summary()
        )
```

---

## 🔄 Real-Time Data Collection

### **Streaming Data Architecture**
```python
class RealTimeCollector:
    def __init__(self):
        self.websocket_connections = {}
        self.message_queue = asyncio.Queue(maxsize=10000)
        self.batch_processor = BatchProcessor()
    
    async def start_real_time_collection(self, symbols: List[str]):
        """
        Start real-time data collection from multiple vendors
        """
        # Start WebSocket connections
        await self.connect_polygon_websocket(symbols)
        await self.connect_tiingo_websocket(symbols)
        
        # Start message processing
        asyncio.create_task(self.process_message_queue())
        asyncio.create_task(self.batch_insert_worker())
        
        # Start health monitoring
        asyncio.create_task(self.monitor_connection_health())
    
    async def handle_market_data_message(self, message: Dict, vendor: str):
        """
        Handle incoming real-time market data
        """
        try:
            # Parse and validate message
            parsed_data = self.parse_vendor_message(message, vendor)
            
            # Quality check
            if not parsed_data.validate():
                await self.log_quality_issue(parsed_data, vendor)
                return
            
            # Add to processing queue
            await self.message_queue.put({
                'data': parsed_data,
                'vendor': vendor,
                'timestamp': datetime.utcnow()
            })
            
        except Exception as e:
            await self.handle_streaming_error(e, vendor)
```

---

## 📊 Data Catalog & Lineage

### **Dataset Inventory**
```python
datasets = {
    "dev_daily_prices": {
        "description": "End-of-day OHLCV data from all vendors",
        "schema": "symbol, date, ohlcv, volume, vendor, quality_score",
        "partitioning": "Monthly by date",
        "retention": "2 years",
        "sources": ["polygon", "tiingo", "alphavantage", "fmp"],
        "update_frequency": "Daily after market close"
    },
    "dev_minute_prices": {
        "description": "Intraday minute-level OHLCV data", 
        "schema": "symbol, timestamp, ohlcv, volume, vendor",
        "partitioning": "Daily by timestamp",
        "retention": "90 days", 
        "sources": ["polygon", "tiingo"],
        "update_frequency": "Real-time during market hours"
    },
    "dev_splits_dividends": {
        "description": "Corporate action events",
        "schema": "symbol, date, type, ratio, amount, vendor",
        "partitioning": "Yearly by date",
        "retention": "10 years",
        "sources": ["polygon", "tiingo"],
        "update_frequency": "Daily"
    }
}
```

### **Data Lineage Tracking**
```python
class DataLineageTracker:
    def record_data_lineage(
        self, 
        dataset: str,
        symbol: str, 
        date: datetime,
        source_info: Dict
    ):
        """
        Record complete data lineage for audit trails
        """
        lineage_record = {
            "dataset": dataset,
            "symbol": symbol,
            "date": date,
            "sources": [
                {
                    "vendor": source_info["vendor"],
                    "api_endpoint": source_info["endpoint"],
                    "ingested_at": source_info["timestamp"],
                    "raw_response_hash": hashlib.md5(
                        json.dumps(source_info["raw_data"]).encode()
                    ).hexdigest()
                }
            ],
            "transformations": [
                {
                    "type": "schema_normalization",
                    "applied_at": datetime.utcnow(),
                    "version": "1.0"
                },
                {
                    "type": "quality_validation", 
                    "applied_at": datetime.utcnow(),
                    "score": source_info["quality_score"]
                }
            ],
            "quality_score": source_info["quality_score"],
            "created_at": datetime.utcnow()
        }
        
        return self.store_lineage_record(lineage_record)
```

---

## 🚀 Deployment & Operations

### **Data Pipeline Deployment**
```yaml
# k8s/data-infrastructure.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-price-collection
  namespace: ats-dev
spec:
  schedule: "0 17 * * MON-FRI"  # 5 PM EST weekdays
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: price-collector
            image: dragonflyer762/ats-genai:latest
            command: ["python"]
            args: ["scripts/k8s-extracted/daily_price_collection.py"]
            env:
            - name: ENVIRONMENT
              value: "dev"
            - name: POLYGON_API_KEY
              valueFrom:
                secretKeyRef:
                  name: market-data-secrets
                  key: polygon-api-key
            resources:
              requests:
                memory: "1Gi"
                cpu: "500m"
              limits:
                memory: "2Gi" 
                cpu: "1000m"
          restartPolicy: OnFailure
```

---

## 📊 Performance Metrics & SLAs

### **Data Infrastructure KPIs**
- **Data Freshness**: < 5 minutes for real-time feeds
- **Historical Accuracy**: 99.9% price accuracy vs exchange data
- **Pipeline Uptime**: 99.5% availability during market hours  
- **Recovery Time**: < 15 minutes for critical pipeline failures
- **Storage Efficiency**: 70% compression ratio for time-series data
- **Cross-Vendor Consistency**: < 0.5% price variance tolerance

### **Monitoring & Alerting**
```python
# Critical alerts
alerts = {
    "data_pipeline_failure": {
        "condition": "No data received for > 15 minutes during market hours",
        "severity": "critical",
        "notification": ["slack", "email", "pagerduty"]
    },
    "quality_score_degradation": {
        "condition": "Average quality score < 0.90 for any vendor",
        "severity": "warning", 
        "notification": ["slack"]
    },
    "cross_vendor_discrepancy": {
        "condition": "Price variance > 1% between vendors",
        "severity": "warning",
        "notification": ["slack"]
    }
}
```

---

**🎯 The Data Infrastructure provides enterprise-grade data management with multi-vendor reconciliation, real-time processing, and comprehensive quality assurance for algorithmic trading operations.**