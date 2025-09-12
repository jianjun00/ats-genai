# ATS Comprehensive Monitoring Dashboard

## 🚀 Overview

This SignOZ dashboard provides complete visibility into the ATS fintech platform infrastructure, services, and business metrics. It monitors all critical components across development and integration environments.

## 📊 Dashboard Panels Documentation

### **Panel 1: 🚀 Service Health Overview**
- **Type**: Stat
- **Purpose**: Shows real-time up/down status of all ATS services
- **Metrics**: `up{job=~"ats-.*"}`
- **Key Insights**:
  - Immediate visibility into service availability
  - Red = DOWN, Green = UP status indicators
  - Critical for operations team to identify outages

### **Panel 2: 📊 Instruments by Environment**
- **Type**: Stat
- **Purpose**: Count of financial instruments loaded per environment
- **Metrics**: `ats_instruments_total`
- **Key Insights**:
  - Tracks data completeness across dev/intg environments
  - Validates instrument loading after database migrations
  - Business metric for data coverage

### **Panel 3: ⚡ Minute Bars Collected (24h)**
- **Type**: Stat
- **Purpose**: Volume of minute-level price data collected in last 24 hours
- **Metrics**: `increase(ats_minute_bars_collected_total[24h])`
- **Key Insights**:
  - Shows data ingestion velocity by vendor (Polygon, Tiingo, EODHD)
  - Critical for real-time trading data pipeline health
  - Identifies collection gaps or vendor issues

### **Panel 4: 📈 Daily Prices Processed (24h)**
- **Type**: Stat
- **Purpose**: Count of daily price records synced in last 24 hours
- **Metrics**: `increase(ats_daily_prices_sync_prices_processed_total[24h])`
- **Key Insights**:
  - Tracks daily data backfill completion
  - Validates end-of-day processing workflows
  - Shows vendor performance comparison

### **Panel 5: 🔄 Real-Time Data Collection Rate**
- **Type**: Time Series
- **Purpose**: Real-time visualization of data ingestion rates
- **Metrics**:
  - `rate(ats_minute_bars_collected_total[5m])` - Minute bars per second
  - `rate(ats_daily_prices_sync_prices_processed_total[5m])` - Prices per second
- **Key Insights**:
  - Identifies peak collection periods
  - Shows processing capacity and bottlenecks
  - Monitors real-time system performance

### **Panel 6: 🗄️ Database Health - Connections & Performance**
- **Type**: Time Series
- **Purpose**: PostgreSQL/TimescaleDB performance monitoring
- **Metrics**:
  - `postgresql_connections_active` - Active database connections
  - `postgresql_connections_idle` - Idle connections
  - `postgresql_queries_per_second` - Query throughput
  - `postgresql_cache_hit_ratio * 100` - Buffer cache efficiency
- **Key Insights**:
  - Database connection pooling health
  - Query performance optimization opportunities
  - Cache efficiency for read-heavy workloads

### **Panel 7: 💾 System Resources (CPU, Memory, Disk)**
- **Type**: Time Series
- **Purpose**: Infrastructure resource utilization monitoring
- **Metrics**:
  - `postgresql_cpu_percent` - Database CPU usage
  - `postgresql_memory_mb` - Database memory consumption
  - `postgresql_disk_usage_percent` - Storage utilization
  - `ats_memory_total_bytes / 1024 / 1024 / 1024` - System memory
- **Key Insights**:
  - Capacity planning and resource allocation
  - Performance bottleneck identification
  - Early warning for resource exhaustion

### **Panel 8: 📡 API Success Rates & Error Monitoring**
- **Type**: Time Series
- **Purpose**: External API health and reliability tracking
- **Metrics**:
  - `rate(ats_daily_prices_backfill_api_calls_total{status="200"}[5m])` - Successful API calls
  - `rate(ats_daily_prices_backfill_api_calls_total{status!="200"}[5m])` - API errors
  - `rate(api_errors_total[5m])` - General API failures
  - `rate(ats_news_api_calls_total{status="200"}[5m])` - News API success
- **Key Insights**:
  - Vendor API reliability assessment
  - Rate limiting and quota management
  - Data collection pipeline stability

### **Panel 9: 🔍 Data Quality & Coverage Metrics**
- **Type**: Gauge
- **Purpose**: Business-critical data quality indicators
- **Metrics**:
  - `ats_price_coverage_percentage` - Price data completeness
  - `ats_daily_prices_sync_success_rate * 100` - Sync operation success
  - `ats_daily_prices_backfill_success_rate * 100` - Backfill success
- **Key Insights**:
  - Data completeness for trading algorithms
  - Process reliability measurements
  - SLA compliance tracking

### **Panel 10: 📊 Data Volume by Vendor & Type**
- **Type**: Pie Chart
- **Purpose**: Distribution analysis of data collection by source
- **Metrics**:
  - `sum by (vendor) (ats_minute_bars_collected_total)` - Minute bars by vendor
  - `sum by (vendor) (ats_daily_prices_sync_prices_processed_total)` - Daily prices by vendor
  - `sum by (vendor) (ats_news_articles_collected_total)` - News articles by vendor
- **Key Insights**:
  - Vendor contribution analysis
  - Data source diversification assessment
  - Contract and cost optimization opportunities

### **Panel 11: ⏱️ Processing Duration & Performance**
- **Type**: Time Series
- **Purpose**: Performance analysis of data processing operations
- **Metrics**:
  - `histogram_quantile(0.95, rate(ats_daily_prices_sync_duration_seconds_bucket[5m]))` - P95 sync time
  - `histogram_quantile(0.95, rate(ats_daily_prices_backfill_duration_seconds_bucket[5m]))` - P95 backfill time
  - `histogram_quantile(0.95, rate(collection_duration_seconds_bucket[5m]))` - P95 collection time
  - `histogram_quantile(0.50, rate(ats_daily_prices_sync_duration_seconds_bucket[5m]))` - P50 sync time
- **Key Insights**:
  - Performance regression detection
  - Optimization target identification
  - SLA compliance monitoring

### **Panel 12: 🚨 Alerts & Issues Summary**
- **Type**: Table
- **Purpose**: Centralized alert dashboard for critical issues
- **Metrics**:
  - `postgresql_blocked_queries > 0` - Database lock issues
  - `postgresql_long_running_queries > 0` - Performance problems
  - `up{job=~"ats-.*"} == 0` - Service outages
  - `ats_price_coverage_percentage < 90` - Data quality issues
- **Key Insights**:
  - Operations team action items
  - Critical issue prioritization
  - System health at-a-glance

### **Panel 13: 📈 Training Dataset Creation Metrics**
- **Type**: Time Series
- **Purpose**: Machine learning pipeline monitoring
- **Metrics**:
  - `increase(ats_training_datasets_created_total[24h])` - Dataset creation rate
  - `ats_training_dataset_size_mb` - Dataset sizes
  - `ats_training_dataset_quality_score` - Data quality scores
- **Key Insights**:
  - ML pipeline health and productivity
  - Data quality trends for training
  - Resource usage for ML workflows

### **Panel 14: 🌐 Active Symbols & Coverage by Exchange**
- **Type**: Bar Gauge
- **Purpose**: Market coverage and symbol distribution analysis
- **Metrics**:
  - `active_symbols` - Currently monitored symbols
  - `sum by (instrument_type) (ats_daily_minute_backfill_symbols_by_type)` - Coverage by type
  - `sum by (letter) (ats_daily_minute_backfill_symbols_by_letter)` - Alphabetical distribution
- **Key Insights**:
  - Market coverage completeness
  - Symbol distribution patterns
  - Trading universe scope

## 🔧 Dashboard Features

### **Template Variables**
- **Environment**: Filter by `dev`, `intg` environments
- **Vendor**: Filter by `polygon`, `tiingo`, `eodhd` data providers
- **Service**: Filter by specific ATS services

### **Annotations**
- **Deployment Events**: Shows deployment timestamps
- **Service Restarts**: Highlights service restart events

### **Time Controls**
- **Default Range**: Last 1 hour with 30-second auto-refresh
- **Configurable**: Adjustable time ranges from 5 minutes to 30 days

## 🎯 Use Cases

### **Operations Team**
- **Daily Health Checks**: Panel 1, 12 for service status and alerts
- **Capacity Planning**: Panel 7 for resource utilization trends
- **Incident Response**: Panel 8, 12 for error identification and resolution

### **Data Engineering Team**
- **Pipeline Monitoring**: Panel 3, 4, 5 for data flow analysis
- **Quality Assurance**: Panel 9 for data completeness validation
- **Performance Optimization**: Panel 6, 11 for processing efficiency

### **Business Team**
- **Market Coverage**: Panel 2, 14 for instrument and symbol coverage
- **Vendor Analysis**: Panel 10 for data source distribution
- **SLA Compliance**: Panel 9 for quality and availability metrics

### **ML Engineering Team**
- **Training Data Pipeline**: Panel 13 for dataset creation monitoring
- **Data Quality**: Panel 9 for training data reliability
- **Resource Usage**: Panel 7 for ML workload resource consumption

## 🔗 Quick Links

- [ATS Operations Guide](/docs/OPERATIONS.md)
- [SignOZ Services](http://localhost:8080/services)
- [Infrastructure Documentation](/docs/INFRASTRUCTURE.md)

---

**🚨 Critical Monitoring Areas:**
- Service availability (Panel 1, 12)
- Data collection rates (Panel 3, 4, 5)
- Database health (Panel 6, 7)
- API reliability (Panel 8)
- Data quality (Panel 9)