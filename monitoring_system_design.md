# Data Coverage Monitoring System Architecture

## 🎯 **Goal**: Continuous monitoring to identify which instruments and dates need backfill

## 📊 **Database Schema for Coverage Tracking**

### Core Tables

```sql
-- 1. Data Coverage Tracking
CREATE TABLE dev_data_coverage_tracking (
    id SERIAL PRIMARY KEY,
    vendor VARCHAR(20) NOT NULL,           -- 'polygon', 'tiingo', 'firstrate', 'eodhd'
    data_type VARCHAR(20) NOT NULL,        -- 'daily_prices', 'minute_bars'
    symbol VARCHAR(20) NOT NULL,
    trading_date DATE NOT NULL,
    coverage_status VARCHAR(20) NOT NULL,  -- 'complete', 'partial', 'missing', 'stale'
    data_quality_score DECIMAL(5,2),       -- 0-100 quality score
    record_count INTEGER,                  -- Number of records for this symbol/date
    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
    file_path TEXT,                        -- Path to data file if applicable
    file_size_bytes BIGINT,                -- File size for storage tracking
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(vendor, data_type, symbol, trading_date)
);

-- 2. Coverage Gaps (Actionable Backfill Queue)
CREATE TABLE dev_coverage_gaps (
    id SERIAL PRIMARY KEY,
    vendor VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    gap_start_date DATE NOT NULL,
    gap_end_date DATE NOT NULL,
    gap_days INTEGER NOT NULL,
    priority_score INTEGER NOT NULL,       -- 1-10 priority for backfill
    backfill_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'in_progress', 'completed', 'failed'
    estimated_effort_minutes INTEGER,      -- Time estimate for backfill
    assigned_worker VARCHAR(50),           -- Ray worker assignment
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- 3. Daily Coverage Metrics (Trending)
CREATE TABLE dev_daily_coverage_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    vendor VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    total_expected_instruments INTEGER,
    instruments_with_data INTEGER,
    coverage_percentage DECIMAL(5,2),
    total_expected_files INTEGER,
    files_found INTEGER,
    files_missing INTEGER,
    files_stale INTEGER,                   -- Files older than expected
    avg_quality_score DECIMAL(5,2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(metric_date, vendor, data_type)
);

-- 4. Backfill Operations Log
CREATE TABLE dev_backfill_operations (
    id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL,   -- 'manual', 'scheduled', 'urgent'
    vendor VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    symbols_requested TEXT[],              -- Array of symbols
    date_range_start DATE NOT NULL,
    date_range_end DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    symbols_completed TEXT[],
    symbols_failed TEXT[],
    total_files_updated INTEGER DEFAULT 0,
    total_records_added BIGINT DEFAULT 0,
    worker_configuration JSONB,           -- Ray worker config used
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    error_log TEXT,
    created_by VARCHAR(50) DEFAULT 'system',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

## 🔄 **Daily Monitoring Workflow**

### 1. Coverage Scanner (Runs Daily at 6 AM)
```python
async def daily_coverage_scan():
    for vendor in ['firstrate', 'polygon', 'tiingo', 'eodhd']:
        for data_type in ['daily_prices', 'minute_bars']:
            # Scan last 90 days
            coverage_data = await scan_vendor_coverage(vendor, data_type, 90)
            await update_coverage_tracking(coverage_data)
            await calculate_daily_metrics(vendor, data_type)
```

### 2. Gap Detection Algorithm
```python
async def detect_coverage_gaps():
    # Find missing data patterns
    gaps = await identify_gaps(
        lookback_days=90,
        min_gap_size=1,     # 1+ missing days
        priority_symbols=get_priority_symbols()  # SPY, QQQ, AAPL, etc.
    )
    
    # Prioritize gaps
    for gap in gaps:
        priority = calculate_gap_priority(gap)
        await queue_backfill_task(gap, priority)
```

### 3. Priority Scoring Algorithm
```python
def calculate_gap_priority(gap):
    base_score = 1
    
    # Symbol importance (1-4x multiplier)
    if gap.symbol in ['SPY', 'QQQ', 'AAPL', 'MSFT', 'GOOGL']:
        base_score *= 4  # Critical instruments
    elif gap.symbol in get_sp500_symbols():
        base_score *= 2  # Important instruments
    
    # Recency (1-3x multiplier)
    days_ago = (date.today() - gap.gap_end_date).days
    if days_ago <= 7:
        base_score *= 3  # Very recent gaps
    elif days_ago <= 30:
        base_score *= 2  # Recent gaps
    
    # Gap size (1-2x multiplier)
    if gap.gap_days >= 10:
        base_score *= 2  # Large gaps
    
    # Data type importance
    if gap.data_type == 'minute_bars':
        base_score *= 1.5  # Minute bars more critical
    
    return min(base_score, 10)  # Cap at 10
```

## 🤖 **Automated Backfill Orchestration**

### 1. Smart Backfill Scheduler (Runs Every 2 Hours)
```python
async def smart_backfill_scheduler():
    # Get high-priority gaps
    urgent_gaps = await get_priority_gaps(min_priority=7)
    
    if urgent_gaps:
        # Schedule immediate Ray backfill
        await schedule_ray_backfill(urgent_gaps, urgency='high')
    
    # Get moderate priority gaps for batch processing
    batch_gaps = await get_priority_gaps(min_priority=4, max_priority=6)
    
    if len(batch_gaps) >= 10:  # Batch efficiency threshold
        await schedule_ray_backfill(batch_gaps, urgency='normal')
```

### 2. Adaptive Ray Configuration
```python
def configure_ray_for_backfill(gap_count, urgency):
    if urgency == 'high':
        return {
            'num_workers': min(32, os.cpu_count() * 4),
            'symbols_per_batch': 5,  # Smaller batches for speed
            'timeout_minutes': 30
        }
    else:
        return {
            'num_workers': min(16, os.cpu_count() * 2), 
            'symbols_per_batch': 10,
            'timeout_minutes': 60
        }
```

## 📈 **Monitoring Dashboard Components**

### 1. Real-Time Coverage Dashboard
- **Coverage Heatmap**: Visual grid showing coverage by vendor/date
- **Gap Count Trending**: Track gaps over time
- **Backfill Queue Status**: Pending, in-progress, completed operations
- **Data Quality Scores**: Trending quality metrics

### 2. Alert Thresholds
```python
ALERT_THRESHOLDS = {
    'critical_symbol_gap': {
        'symbols': ['SPY', 'QQQ', 'AAPL', 'MSFT'],
        'max_gap_days': 1,  # Alert if missing > 1 day
        'max_response_time': 2  # Must backfill within 2 hours
    },
    'coverage_degradation': {
        'min_coverage_pct': 85,  # Alert if overall coverage < 85%
        'trending_window': 7     # Days to analyze trend
    },
    'backfill_failures': {
        'max_consecutive_failures': 3,
        'max_failure_rate': 0.1  # 10% failure rate threshold
    }
}
```

## 🔧 **Implementation Components**

### 1. Coverage Scanner Module
- File system scanners for each vendor
- Database connectivity validators
- Data quality assessment functions
- Parallel processing for large-scale scanning

### 2. Gap Detection Engine
- SQL-based gap analysis queries
- Priority scoring algorithms
- Backfill cost estimation
- Smart batching logic

### 3. Automated Backfill Engine
- Ray-based parallel processing
- Fault tolerance and retry logic
- Progress tracking and reporting
- Resource optimization

### 4. Monitoring & Alerting
- Prometheus metrics export
- Grafana dashboard integration
- Slack/email alerting
- Historical trend analysis

## 🎯 **Key Benefits**

1. **Proactive Gap Detection**: Identify missing data before it impacts trading
2. **Intelligent Prioritization**: Focus on critical instruments and recent gaps
3. **Automated Resolution**: Reduce manual backfill operations by 90%
4. **Performance Optimization**: Use Ray parallelization efficiently
5. **Historical Tracking**: Understand data quality trends over time
6. **Cost Optimization**: Batch operations and avoid redundant processing

## 📋 **Implementation Phases**

**Phase 1** (Week 1): Database schema and basic coverage tracking
**Phase 2** (Week 2): Gap detection and prioritization algorithms  
**Phase 3** (Week 3): Automated backfill orchestration
**Phase 4** (Week 4): Monitoring dashboard and alerting integration

This system transforms reactive "discover and fix" into proactive "predict and prevent" data management.