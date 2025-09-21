# Feature Extraction System - Database Requirements Document (DRD)

## Overview

Comprehensive database and storage design for transforming training data generation into scalable feature extraction system supporting thousands of instruments with monthly ArrayRecord storage.

## Database Schema Design

### Core Tables

#### 1. feature_extraction_runs
```sql
CREATE TABLE {env}_feature_extraction_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) UNIQUE NOT NULL,           -- External UUID for tracking
    run_type VARCHAR(50) NOT NULL DEFAULT 'feature_extraction',
    status VARCHAR(50) NOT NULL DEFAULT 'running', -- running, completed, failed
    feature_groups TEXT[] NOT NULL,                -- Array of feature group names
    date_range_start DATE NOT NULL,
    date_range_end DATE NOT NULL,
    total_instruments INTEGER NOT NULL DEFAULT 0,
    total_features_generated INTEGER DEFAULT 0,
    execution_duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    command_line TEXT,
    git_commit_hash VARCHAR(40),
    environment VARCHAR(20),
    parameters JSONB,                              -- Extraction parameters
    results JSONB,                                 -- Execution results
    error_message TEXT,
    
    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT valid_date_range CHECK (date_range_end >= date_range_start)
);

-- Indexes
CREATE INDEX idx_{env}_feature_extraction_runs_status ON {env}_feature_extraction_runs(status);
CREATE INDEX idx_{env}_feature_extraction_runs_date_range ON {env}_feature_extraction_runs(date_range_start, date_range_end);
CREATE INDEX idx_{env}_feature_extraction_runs_created_at ON {env}_feature_extraction_runs(created_at);
CREATE INDEX idx_{env}_feature_extraction_runs_feature_groups ON {env}_feature_extraction_runs USING GIN(feature_groups);
```

#### 2. feature_extraction_instruments
```sql
CREATE TABLE {env}_feature_extraction_instruments (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES {env}_feature_extraction_runs(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL,                -- Foreign key to instruments table
    symbol VARCHAR(20) NOT NULL,                   -- Denormalized for performance
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, processing, completed, failed
    features_generated INTEGER DEFAULT 0,
    processing_duration_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT unique_run_instrument UNIQUE(run_id, instrument_id),
    CONSTRAINT valid_instrument_status CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

-- Indexes
CREATE INDEX idx_{env}_feature_extraction_instruments_run_id ON {env}_feature_extraction_instruments(run_id);
CREATE INDEX idx_{env}_feature_extraction_instruments_instrument_id ON {env}_feature_extraction_instruments(instrument_id);
CREATE INDEX idx_{env}_feature_extraction_instruments_symbol ON {env}_feature_extraction_instruments(symbol);
CREATE INDEX idx_{env}_feature_extraction_instruments_status ON {env}_feature_extraction_instruments(status);
```

#### 3. feature_catalog
```sql
CREATE TABLE {env}_feature_catalog (
    feature_id SERIAL PRIMARY KEY,                -- Unique feature identifier
    feature_name VARCHAR(100) NOT NULL UNIQUE,    -- e.g., 'rsi_14', 'sma_20'
    feature_group_id INTEGER NOT NULL REFERENCES {env}_feature_groups(id),
    data_type VARCHAR(20) NOT NULL DEFAULT 'FLOAT64', -- FLOAT64, INT64, STRING
    column_position INTEGER NOT NULL,             -- Position in ArrayRecord schema (0-based)
    description TEXT,
    computation_method TEXT,                       -- Formula or algorithm description
    dependencies TEXT[],                           -- Array of required input features
    validation_rules JSONB,                        -- Min/max bounds, null handling
    is_active BOOLEAN DEFAULT true,               -- Feature enabled for generation
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_data_type CHECK (data_type IN ('FLOAT64', 'INT64', 'STRING', 'BOOLEAN', 'TIMESTAMP')),
    CONSTRAINT unique_group_position UNIQUE(feature_group_id, column_position),
    CONSTRAINT unique_group_feature UNIQUE(feature_group_id, feature_name)
);

-- Indexes
CREATE INDEX idx_{env}_feature_catalog_feature_group_id ON {env}_feature_catalog(feature_group_id);
CREATE INDEX idx_{env}_feature_catalog_feature_name ON {env}_feature_catalog(feature_name);
CREATE INDEX idx_{env}_feature_catalog_dependencies ON {env}_feature_catalog USING GIN(dependencies);
CREATE INDEX idx_{env}_feature_catalog_active ON {env}_feature_catalog(is_active);
```

#### 4. feature_groups
```sql
CREATE TABLE {env}_feature_groups (
    id SERIAL PRIMARY KEY,
    group_name VARCHAR(100) NOT NULL UNIQUE,      -- e.g., 'ohlcv_basic', 'technical_momentum'
    display_name VARCHAR(200) NOT NULL,           -- Human-readable name
    description TEXT,
    category VARCHAR(50) NOT NULL,                -- basic, technical, fundamental, alternative
    update_frequency VARCHAR(20) NOT NULL,        -- daily, intraday, weekly, monthly
    computation_lag_minutes INTEGER DEFAULT 0,    -- Processing delay after market close
    dependencies TEXT[],                           -- Required feature groups
    storage_format VARCHAR(20) DEFAULT 'arrayrecord',
    retention_months INTEGER DEFAULT 60,          -- Archive after N months
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_category CHECK (category IN ('basic', 'technical', 'fundamental', 'alternative')),
    CONSTRAINT valid_frequency CHECK (update_frequency IN ('intraday', 'daily', 'weekly', 'monthly'))
);

-- Indexes
CREATE INDEX idx_{env}_feature_groups_category ON {env}_feature_groups(category);
CREATE INDEX idx_{env}_feature_groups_update_frequency ON {env}_feature_groups(update_frequency);
CREATE INDEX idx_{env}_feature_groups_is_active ON {env}_feature_groups(is_active);
```

#### 5. feature_availability
```sql
CREATE TABLE {env}_feature_availability (
    id SERIAL PRIMARY KEY,
    feature_group_id INTEGER NOT NULL REFERENCES {env}_feature_groups(id),
    instrument_id INTEGER NOT NULL,
    symbol VARCHAR(20) NOT NULL,                   -- Denormalized for performance
    year_month DATE NOT NULL,                      -- First day of month (2024-01-01)
    file_path TEXT NOT NULL,                       -- ArrayRecord file location
    file_size_bytes BIGINT,
    record_count INTEGER,
    date_range_start TIMESTAMP NOT NULL,
    date_range_end TIMESTAMP NOT NULL,
    quality_score DECIMAL(5,4),                    -- 0.0000 to 1.0000
    validation_status VARCHAR(20) DEFAULT 'pending', -- pending, passed, failed
    validation_errors JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT unique_feature_availability UNIQUE(feature_group_id, instrument_id, year_month),
    CONSTRAINT valid_quality_score CHECK (quality_score >= 0.0 AND quality_score <= 1.0),
    CONSTRAINT valid_validation_status CHECK (validation_status IN ('pending', 'passed', 'failed')),
    CONSTRAINT valid_date_range CHECK (date_range_end >= date_range_start)
);

-- Indexes (Partitioned by year_month for performance)
CREATE INDEX idx_{env}_feature_availability_feature_group_id ON {env}_feature_availability(feature_group_id);
CREATE INDEX idx_{env}_feature_availability_instrument_id ON {env}_feature_availability(instrument_id);
CREATE INDEX idx_{env}_feature_availability_year_month ON {env}_feature_availability(year_month);
CREATE INDEX idx_{env}_feature_availability_quality_score ON {env}_feature_availability(quality_score);
CREATE INDEX idx_{env}_feature_availability_validation_status ON {env}_feature_availability(validation_status);

-- Partitioning by year for scalability
-- CREATE TABLE {env}_feature_availability_2024 PARTITION OF {env}_feature_availability
-- FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

#### 6. feature_tags
```sql
CREATE TABLE {env}_feature_tags (
    id SERIAL PRIMARY KEY,
    feature_group_id INTEGER NOT NULL REFERENCES {env}_feature_groups(id),
    tag_name VARCHAR(50) NOT NULL,                 -- prod, experimental, validated, deprecated
    tag_value VARCHAR(200),                        -- Optional tag value
    applied_by VARCHAR(100) NOT NULL,              -- User who applied tag
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,                          -- Optional expiration
    metadata JSONB,                                -- Additional tag metadata
    
    -- Constraints
    CONSTRAINT unique_feature_group_tag UNIQUE(feature_group_id, tag_name),
    CONSTRAINT valid_tag_name CHECK (tag_name IN ('experimental', 'validated', 'prod-candidate', 'prod', 'deprecated'))
);

-- Indexes
CREATE INDEX idx_{env}_feature_tags_feature_group_id ON {env}_feature_tags(feature_group_id);
CREATE INDEX idx_{env}_feature_tags_tag_name ON {env}_feature_tags(tag_name);
CREATE INDEX idx_{env}_feature_tags_applied_at ON {env}_feature_tags(applied_at);
```

#### 7. feature_quality_metrics
```sql
CREATE TABLE {env}_feature_quality_metrics (
    id SERIAL PRIMARY KEY,
    feature_group_id INTEGER NOT NULL REFERENCES {env}_feature_groups(id),
    instrument_id INTEGER NOT NULL,
    year_month DATE NOT NULL,
    metric_name VARCHAR(50) NOT NULL,              -- completeness, accuracy, consistency, timeliness
    metric_value DECIMAL(10,6) NOT NULL,
    threshold_value DECIMAL(10,6),
    status VARCHAR(20) NOT NULL,                   -- pass, fail, warning
    details JSONB,                                 -- Detailed metric breakdown
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_metric_name CHECK (metric_name IN ('completeness', 'accuracy', 'consistency', 'timeliness', 'uniqueness')),
    CONSTRAINT valid_status CHECK (status IN ('pass', 'fail', 'warning'))
);

-- Indexes
CREATE INDEX idx_{env}_feature_quality_metrics_feature_group_id ON {env}_feature_quality_metrics(feature_group_id);
CREATE INDEX idx_{env}_feature_quality_metrics_year_month ON {env}_feature_quality_metrics(year_month);
CREATE INDEX idx_{env}_feature_quality_metrics_status ON {env}_feature_quality_metrics(status);
```

## Data Flow Architecture

### 1. Input Data Sources
```
Minute Bar Files: /mnt/d/ats-data/minute-bars/firstrate/T/{SYMBOL}/{YEAR}/{MONTH}/
├── AAPL_20240101_20240131_minute_bars.parquet
├── MSFT_20240101_20240131_minute_bars.parquet
└── TSLA_20240101_20240131_minute_bars.parquet

Schema: timestamp, symbol, open, high, low, close, volume, vwap, trade_count
```

### 2. Processing Pipeline
```
1. Feature Extraction Runner
   ├── Read minute bar files for date range
   ├── Compute feature groups (OHLCV → Technical → Fundamental)
   ├── Validate feature values against rules
   ├── Generate monthly ArrayRecord files
   └── Update database metadata

2. Quality Assessment
   ├── Completeness: % of expected records present
   ├── Accuracy: Values within expected ranges
   ├── Consistency: Cross-validation with other sources
   └── Timeliness: Processing delay metrics

3. File Organization
   ├── Create monthly ArrayRecord files per instrument/feature group
   ├── Generate metadata.json with schema information
   ├── Update feature_availability table
   └── Calculate quality scores
```

### 3. Output Storage Structure
```
/data/features/
├── 2024/
│   ├── 01/                                    # January 2024
│   │   ├── ohlcv_basic/
│   │   │   ├── AAPL_202401.arrayrecord        # 24x7x31 = ~5200 records
│   │   │   ├── MSFT_202401.arrayrecord
│   │   │   ├── metadata.json                  # Schema + stats
│   │   │   └── quality_report.json            # Quality metrics
│   │   ├── technical_momentum/
│   │   │   ├── AAPL_202401.arrayrecord        # RSI, MACD, SMA, EMA
│   │   │   ├── MSFT_202401.arrayrecord
│   │   │   └── metadata.json
│   │   └── technical_volatility/
│   │       ├── AAPL_202401.arrayrecord        # Bollinger, ATR, Vol
│   │       └── metadata.json
│   └── 02/                                    # February 2024
├── 2025/
└── archive/                                   # Files older than retention period
```

## ArrayRecord File Format

### Schema Definition
Schemas are dynamically generated from feature_catalog based on feature_group_id:

```python
def get_feature_group_schema(feature_group_id: int) -> List[Tuple[str, str]]:
    """Generate ArrayRecord schema from feature catalog."""
    query = """
    SELECT feature_name, data_type, column_position
    FROM feature_catalog 
    WHERE feature_group_id = %s AND is_active = true
    ORDER BY column_position
    """
    
    # Example result for ohlcv_basic (feature_group_id=1):
    return [
        ('timestamp', 'int64'),    # column_position=0
        ('symbol', 'string'),      # column_position=1  
        ('open', 'float64'),       # column_position=2
        ('high', 'float64'),       # column_position=3
        ('low', 'float64'),        # column_position=4
        ('close', 'float64'),      # column_position=5
        ('volume', 'int64'),       # column_position=6
        ('vwap', 'float64'),       # column_position=7
    ]

# Example feature catalog data:
INSERT INTO feature_catalog (feature_name, feature_group_id, data_type, column_position) VALUES
-- OHLCV Basic (group_id=1)
('timestamp', 1, 'INT64', 0),
('symbol', 1, 'STRING', 1),
('open', 1, 'FLOAT64', 2),
('high', 1, 'FLOAT64', 3),
('low', 1, 'FLOAT64', 4),
('close', 1, 'FLOAT64', 5),
('volume', 1, 'INT64', 6),
('vwap', 1, 'FLOAT64', 7),

-- Technical Momentum (group_id=2)
('timestamp', 2, 'INT64', 0),
('symbol', 2, 'STRING', 1),
('sma_20', 2, 'FLOAT64', 2),
('ema_12', 2, 'FLOAT64', 3),
('rsi_14', 2, 'FLOAT64', 4),
('macd', 2, 'FLOAT64', 5),
('macd_signal', 2, 'FLOAT64', 6),
('momentum_1d', 2, 'FLOAT64', 7),
('momentum_5d', 2, 'FLOAT64', 8);
```

### File Naming Convention
```
{SYMBOL}_{YYYYMM}.arrayrecord
Examples:
- AAPL_202401.arrayrecord    # Apple, January 2024
- MSFT_202401.arrayrecord    # Microsoft, January 2024
- TSLA_202402.arrayrecord    # Tesla, February 2024
```

### Metadata File Structure
```json
{
    "feature_group": "ohlcv_basic",
    "symbol": "AAPL",
    "year_month": "2024-01",
    "file_path": "/data/features/2024/01/ohlcv_basic/AAPL_202401.arrayrecord",
    "schema": [
        {"name": "timestamp", "type": "int64"},
        {"name": "symbol", "type": "string"},
        {"name": "open", "type": "float64"}
    ],
    "record_count": 5184,
    "file_size_bytes": 415360,
    "date_range": {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-31T23:59:59Z"
    },
    "quality_metrics": {
        "completeness": 0.9985,
        "accuracy": 0.9998,
        "consistency": 0.9995,
        "overall_score": 0.9993
    },
    "generation_info": {
        "run_id": "run_20241201_143052_abc123",
        "generated_at": "2024-12-01T14:30:52Z",
        "processing_duration_seconds": 145,
        "source_files": [
            "/mnt/d/ats-data/minute-bars/firstrate/T/AAPL/2024/01/AAPL_20240101_20240131_minute_bars.parquet"
        ]
    }
}
```

## Performance Requirements

### Query Patterns & Optimization

#### 1. Feature Discovery Queries
```sql
-- Find available features for specific instruments with feature details
SELECT DISTINCT 
    fg.group_name, 
    fg.display_name, 
    fg.category,
    fc.feature_name,
    fc.data_type,
    fc.description
FROM {env}_feature_groups fg
JOIN {env}_feature_availability fa ON fg.id = fa.feature_group_id
JOIN {env}_feature_catalog fc ON fg.id = fc.feature_group_id
WHERE fa.symbol IN ('AAPL', 'MSFT', 'GOOGL')
  AND fa.year_month >= '2024-01-01'
  AND fa.validation_status = 'passed'
  AND fc.is_active = true
ORDER BY fg.group_name, fc.column_position;

-- Get feature count per group for an instrument
SELECT 
    fg.group_name,
    COUNT(fc.feature_id) as feature_count,
    STRING_AGG(fc.feature_name, ', ' ORDER BY fc.column_position) as features
FROM {env}_feature_groups fg
JOIN {env}_feature_catalog fc ON fg.id = fc.feature_group_id
JOIN {env}_feature_availability fa ON fg.id = fa.feature_group_id
WHERE fa.symbol = 'AAPL' 
  AND fc.is_active = true
GROUP BY fg.id, fg.group_name;

-- Performance Target: <2 seconds for 100 instruments
-- Optimization: Composite index on (symbol, year_month, validation_status)
```

#### 2. Coverage Analysis Queries
```sql
-- Coverage matrix for feature groups across instruments/time
SELECT 
    fa.symbol,
    fa.year_month,
    fg.group_name,
    fa.quality_score,
    fa.record_count
FROM {env}_feature_availability fa
JOIN {env}_feature_groups fg ON fa.feature_group_id = fg.id
WHERE fa.symbol IN (SELECT symbol FROM sp500_universe)  -- 500 instruments
  AND fa.year_month BETWEEN '2020-01-01' AND '2024-12-01'
  AND fg.group_name IN ('ohlcv_basic', 'technical_momentum')
ORDER BY fa.symbol, fa.year_month, fg.group_name;

-- Performance Target: <30 seconds for 500 instruments, 5 years
-- Optimization: Partitioning by year_month, covering indexes
```

#### 3. Quality Monitoring Queries
```sql
-- Quality trend analysis
SELECT 
    fg.group_name,
    fqm.year_month,
    AVG(fqm.metric_value) as avg_quality,
    COUNT(CASE WHEN fqm.status = 'fail' THEN 1 END) as failure_count
FROM {env}_feature_quality_metrics fqm
JOIN {env}_feature_groups fg ON fqm.feature_group_id = fg.id
WHERE fqm.year_month >= CURRENT_DATE - INTERVAL '90 days'
  AND fqm.metric_name = 'completeness'
GROUP BY fg.group_name, fqm.year_month
ORDER BY fg.group_name, fqm.year_month;

-- Performance Target: <5 seconds for 90-day analysis
-- Optimization: Partial indexes on recent data
```

### Indexing Strategy

#### Primary Indexes
```sql
-- Feature availability lookups (most critical)
CREATE INDEX idx_{env}_feature_availability_coverage 
ON {env}_feature_availability(symbol, year_month, feature_group_id, validation_status);

-- Quality score filtering
CREATE INDEX idx_{env}_feature_availability_quality 
ON {env}_feature_availability(feature_group_id, quality_score DESC, year_month);

-- Tag-based filtering
CREATE INDEX idx_{env}_feature_tags_prod 
ON {env}_feature_tags(tag_name, feature_group_id) 
WHERE tag_name IN ('prod', 'prod-candidate');
```

#### Partitioning Strategy
```sql
-- Partition feature_availability by year for query performance
CREATE TABLE {env}_feature_availability_2024 PARTITION OF {env}_feature_availability
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE {env}_feature_availability_2025 PARTITION OF {env}_feature_availability
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Automatic partition creation for future years
```

## Data Quality Framework

### Validation Rules
```json
{
    "ohlcv_basic": {
        "open": {"min": 0.01, "max": 10000, "null_allowed": false},
        "high": {"min": 0.01, "max": 10000, "null_allowed": false},
        "low": {"min": 0.01, "max": 10000, "null_allowed": false},
        "close": {"min": 0.01, "max": 10000, "null_allowed": false},
        "volume": {"min": 0, "max": 1000000000, "null_allowed": false},
        "relationships": [
            {"rule": "high >= open", "severity": "error"},
            {"rule": "high >= close", "severity": "error"},
            {"rule": "low <= open", "severity": "error"},
            {"rule": "low <= close", "severity": "error"}
        ]
    },
    "technical_momentum": {
        "rsi_14": {"min": 0, "max": 100, "null_allowed": true},
        "sma_20": {"min": 0.01, "max": 10000, "null_allowed": true},
        "ema_12": {"min": 0.01, "max": 10000, "null_allowed": true}
    }
}
```

### Quality Scoring Algorithm
```python
def calculate_quality_score(feature_data, validation_rules):
    """
    Quality Score = (Completeness * 0.4) + (Accuracy * 0.3) + (Consistency * 0.2) + (Timeliness * 0.1)
    """
    completeness = 1.0 - (null_count / total_count)
    accuracy = 1.0 - (validation_failures / total_count)
    consistency = cross_validation_score()  # Compare with other data sources
    timeliness = 1.0 - min(processing_delay_hours / 24, 1.0)
    
    return (completeness * 0.4) + (accuracy * 0.3) + (consistency * 0.2) + (timeliness * 0.1)
```

## Scalability Design

### Horizontal Scaling
- **Database**: Read replicas for query-heavy operations
- **File Storage**: Distributed across multiple mount points
- **Processing**: Parallel feature extraction by instrument batches

### Storage Growth Projections
```
Assumptions:
- 3000 instruments
- 4 feature groups
- 12 months/year
- 50KB average file size

Annual Storage:
3000 instruments × 4 groups × 12 months × 50KB = ~7.2GB/year

5-Year Projection: ~36GB
10-Year Projection: ~72GB

Database Growth:
- feature_availability: ~144K rows/year (manageable)
- feature_quality_metrics: ~576K rows/year (requires partitioning)
```

### Archive Strategy
```sql
-- Move files older than 60 months to archive storage
-- Keep database metadata for discovery but mark files as archived
UPDATE {env}_feature_availability 
SET file_path = REPLACE(file_path, '/data/features/', '/data/archive/')
WHERE year_month < CURRENT_DATE - INTERVAL '60 months';
```

## Migration Strategy

### Phase 1: Schema Creation (Week 1)
```sql
-- Create new feature extraction tables
-- Migrate existing runs data where applicable
-- Set up partitioning and indexes

-- Migration script example:
INSERT INTO {env}_feature_extraction_runs (run_id, run_type, status, created_at)
SELECT run_id, 'feature_extraction', status, created_at 
FROM {env}_training_runs 
WHERE created_at >= '2024-01-01';
```

### Phase 2: Data Migration (Week 2-3)
```python
# Convert existing training datasets to feature format
# Extract feature metadata from training files
# Populate feature_availability table
# Generate quality metrics for existing data
```

### Phase 3: Service Integration (Week 4)
```python
# Update analytics service to use new schema
# Implement Feature Explorer dashboard
# Create migration tools for ongoing data
```

## Data Retention Policy

### File Retention
- **Active Storage**: 24 months (recent data for analysis)
- **Archive Storage**: 60 months (compliance and backtesting)
- **Purge**: >60 months (permanent deletion)

### Database Retention
- **Metadata**: Permanent retention (small footprint)
- **Quality Metrics**: 36 months (performance trends)
- **Run Logs**: 12 months (operational debugging)

## Backup & Recovery

### File Backup
```bash
# Daily incremental backup of active features
rsync -av /data/features/ /mnt/d/ats-backup/features/

# Weekly full backup to external storage
tar -czf /backup/features_$(date +%Y%m%d).tar.gz /data/features/
```

### Database Backup
```sql
-- Daily logical backup of feature metadata
pg_dump -h localhost -U postgres -d {env}_db \
  --table='{env}_feature_*' \
  --file=/backup/feature_metadata_$(date +%Y%m%d).sql
```

This DRD provides the complete technical foundation for implementing the feature extraction system with proper scalability, performance, and data quality controls.