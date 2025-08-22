-- Migration 035: Create Data Coverage Catalog Schema
-- Comprehensive coverage tracking for daily prices and minute bars across all vendors
-- Designed for 100M-2B row scale with TimescaleDB optimization

-- =====================================================
-- Core Coverage Intervals Table
-- Tracks contiguous periods of data availability
-- =====================================================
CREATE TABLE IF NOT EXISTS coverage_intervals (
    interval_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(20) NOT NULL, -- 'daily', 'minute'
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    record_count BIGINT NOT NULL,
    expected_count BIGINT NOT NULL,
    completeness_ratio NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    avg_quality_score NUMERIC(3,2),
    min_quality_score NUMERIC(3,2),
    has_gaps BOOLEAN DEFAULT FALSE,
    gap_count INTEGER DEFAULT 0,
    total_gap_duration_minutes INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure intervals don't overlap for same symbol/vendor/type
    CONSTRAINT coverage_intervals_no_overlap 
        EXCLUDE USING gist (
            symbol WITH =, 
            vendor WITH =, 
            data_type WITH =,
            tstzrange(start_time, end_time) WITH &&
        )
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('coverage_intervals', 'start_time', if_not_exists => TRUE);

-- Configure chunk time intervals for optimal performance
SELECT set_chunk_time_interval('coverage_intervals', INTERVAL '1 day');

-- =====================================================
-- Pre-computed Coverage Statistics
-- Multi-level aggregation for instant queries
-- =====================================================
CREATE TABLE IF NOT EXISTS coverage_stats (
    stat_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    aggregation_level VARCHAR(10) NOT NULL, -- 'hour', 'day', 'week', 'month', 'quarter'
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    
    -- Core metrics
    total_expected BIGINT NOT NULL,
    total_actual BIGINT NOT NULL,
    coverage_percentage NUMERIC(5,2) NOT NULL, -- 0.00 to 100.00
    completeness_score NUMERIC(5,4) NOT NULL, -- 0.0000 to 1.0000
    
    -- Quality metrics
    avg_quality_score NUMERIC(3,2),
    min_quality_score NUMERIC(3,2),
    max_quality_score NUMERIC(3,2),
    quality_std_dev NUMERIC(6,4),
    
    -- Gap metrics
    gap_count INTEGER DEFAULT 0,
    total_gap_duration_minutes INTEGER DEFAULT 0,
    largest_gap_minutes INTEGER DEFAULT 0,
    avg_gap_duration_minutes NUMERIC(8,2) DEFAULT 0,
    
    -- Performance metrics
    first_record_time TIMESTAMPTZ,
    last_record_time TIMESTAMPTZ,
    records_per_minute NUMERIC(10,2),
    
    -- Metadata
    computation_time_ms INTEGER,
    last_computed_at TIMESTAMPTZ DEFAULT NOW(),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint for hierarchical aggregation
    UNIQUE(symbol, vendor, data_type, aggregation_level, period_start)
);

-- TimescaleDB hypertable for stats
SELECT create_hypertable('coverage_stats', 'period_start', if_not_exists => TRUE);
SELECT set_chunk_time_interval('coverage_stats', INTERVAL '1 week');

-- =====================================================
-- Coverage Gaps for Detailed Analysis
-- Track missing data periods and their characteristics
-- =====================================================
CREATE TABLE IF NOT EXISTS coverage_gaps (
    gap_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    gap_start TIMESTAMPTZ NOT NULL,
    gap_end TIMESTAMPTZ NOT NULL,
    gap_duration_minutes INTEGER NOT NULL,
    expected_records INTEGER NOT NULL,
    actual_records INTEGER DEFAULT 0,
    
    -- Gap classification
    gap_type VARCHAR(20) NOT NULL, -- 'missing', 'partial', 'low_quality', 'outlier'
    gap_severity VARCHAR(10) NOT NULL, -- 'low', 'medium', 'high', 'critical'
    
    -- Context
    trading_day DATE NOT NULL,
    is_market_hours BOOLEAN DEFAULT TRUE,
    is_trading_day BOOLEAN DEFAULT TRUE,
    
    -- Detection metadata
    detection_method VARCHAR(50), -- 'streaming', 'batch', 'manual'
    detection_confidence NUMERIC(3,2), -- 0.00 to 1.00
    
    -- Resolution tracking
    is_resolved BOOLEAN DEFAULT FALSE,
    resolution_method VARCHAR(50),
    resolved_at TIMESTAMPTZ,
    resolution_notes TEXT,
    
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Prevent overlapping gaps
    CONSTRAINT coverage_gaps_no_overlap 
        EXCLUDE USING gist (
            symbol WITH =, 
            vendor WITH =, 
            data_type WITH =,
            tstzrange(gap_start, gap_end) WITH &&
        )
);

-- TimescaleDB hypertable for gaps
SELECT create_hypertable('coverage_gaps', 'gap_start', if_not_exists => TRUE);

-- =====================================================
-- Real-time Coverage Summary
-- Current state for dashboard displays
-- =====================================================
CREATE TABLE IF NOT EXISTS coverage_summary (
    summary_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    
    -- Current state
    latest_data_time TIMESTAMPTZ,
    hours_since_update NUMERIC(8,2),
    current_status VARCHAR(20), -- 'active', 'stale', 'missing', 'degraded'
    
    -- 24-hour metrics
    coverage_24h NUMERIC(5,2),
    quality_24h NUMERIC(3,2),
    gaps_24h INTEGER,
    records_24h BIGINT,
    
    -- 7-day metrics
    coverage_7d NUMERIC(5,2),
    quality_7d NUMERIC(3,2),
    gaps_7d INTEGER,
    records_7d BIGINT,
    
    -- 30-day metrics
    coverage_30d NUMERIC(5,2),
    quality_30d NUMERIC(3,2),
    gaps_30d INTEGER,
    records_30d BIGINT,
    
    -- Trend indicators
    coverage_trend VARCHAR(10), -- 'improving', 'stable', 'degrading'
    quality_trend VARCHAR(10),
    
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (symbol, vendor, data_type)
);

-- =====================================================
-- Coverage SLA and Benchmark Tracking
-- Define expected coverage and track against SLAs
-- =====================================================
CREATE TABLE IF NOT EXISTS coverage_sla (
    sla_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    vendor VARCHAR(50) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    
    -- SLA definitions (null symbol = applies to all symbols)
    min_coverage_percentage NUMERIC(5,2) DEFAULT 95.00,
    max_acceptable_gap_minutes INTEGER DEFAULT 5,
    max_quality_degradation NUMERIC(3,2) DEFAULT 0.20,
    
    -- Market hours definitions
    market_open_utc TIME NOT NULL DEFAULT '13:30:00', -- 9:30 AM EST
    market_close_utc TIME NOT NULL DEFAULT '20:00:00', -- 4:00 PM EST
    
    -- Alerting thresholds
    warning_threshold NUMERIC(5,2) DEFAULT 90.00,
    critical_threshold NUMERIC(5,2) DEFAULT 80.00,
    
    -- Business rules
    apply_to_market_hours_only BOOLEAN DEFAULT TRUE,
    exclude_weekends BOOLEAN DEFAULT TRUE,
    exclude_holidays BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(symbol, vendor, data_type)
);

-- =====================================================
-- Performance Indexes for Coverage Tables
-- =====================================================

-- Coverage Intervals Indexes
CREATE INDEX IF NOT EXISTS idx_coverage_intervals_symbol_vendor_type 
    ON coverage_intervals(symbol, vendor, data_type, start_time DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_intervals_time_range 
    ON coverage_intervals(start_time, end_time);

CREATE INDEX IF NOT EXISTS idx_coverage_intervals_vendor_time 
    ON coverage_intervals(vendor, data_type, start_time DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_intervals_completeness 
    ON coverage_intervals(completeness_ratio DESC) 
    WHERE completeness_ratio < 1.0;

CREATE INDEX IF NOT EXISTS idx_coverage_intervals_quality 
    ON coverage_intervals(avg_quality_score DESC) 
    WHERE avg_quality_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_coverage_intervals_gaps 
    ON coverage_intervals(symbol, has_gaps, gap_count DESC) 
    WHERE has_gaps = TRUE;

-- Coverage Stats Indexes
CREATE INDEX IF NOT EXISTS idx_coverage_stats_symbol_vendor_level_period 
    ON coverage_stats(symbol, vendor, data_type, aggregation_level, period_start DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_stats_period_coverage 
    ON coverage_stats(period_start DESC, coverage_percentage DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_stats_recent_summary 
    ON coverage_stats(aggregation_level, period_start DESC) 
    WHERE period_start >= CURRENT_DATE - INTERVAL '30 days';

-- Coverage Gaps Indexes
CREATE INDEX IF NOT EXISTS idx_coverage_gaps_symbol_vendor_type_time 
    ON coverage_gaps(symbol, vendor, data_type, gap_start DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_gaps_unresolved 
    ON coverage_gaps(symbol, vendor, is_resolved, gap_start DESC) 
    WHERE is_resolved = FALSE;

CREATE INDEX IF NOT EXISTS idx_coverage_gaps_severity_duration 
    ON coverage_gaps(gap_severity, gap_duration_minutes DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_gaps_trading_day 
    ON coverage_gaps(trading_day, is_market_hours) 
    WHERE is_market_hours = TRUE;

-- Coverage Summary Indexes
CREATE INDEX IF NOT EXISTS idx_coverage_summary_status 
    ON coverage_summary(current_status, hours_since_update);

CREATE INDEX IF NOT EXISTS idx_coverage_summary_coverage_24h 
    ON coverage_summary(coverage_24h DESC, quality_24h DESC);

CREATE INDEX IF NOT EXISTS idx_coverage_summary_trends 
    ON coverage_summary(coverage_trend, quality_trend);

-- =====================================================
-- TimescaleDB Compression and Retention Policies
-- =====================================================

-- Enable compression for older data (>7 days old)
ALTER TABLE coverage_intervals SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, vendor, data_type',
    timescaledb.compress_orderby = 'start_time'
);

ALTER TABLE coverage_stats SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, vendor, data_type, aggregation_level',
    timescaledb.compress_orderby = 'period_start'
);

-- Add compression policies
SELECT add_compression_policy('coverage_intervals', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('coverage_stats', INTERVAL '7 days', if_not_exists => TRUE);

-- Data retention policies
SELECT add_retention_policy('coverage_gaps', INTERVAL '1 year', if_not_exists => TRUE);

-- =====================================================
-- Continuous Aggregates for Real-Time Performance
-- =====================================================

-- Hourly coverage continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS coverage_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket(INTERVAL '1 hour', start_time) AS bucket,
    symbol,
    vendor,
    data_type,
    COUNT(*) as interval_count,
    SUM(record_count) as total_records,
    SUM(expected_count) as total_expected,
    AVG(completeness_ratio) as avg_completeness,
    AVG(avg_quality_score) as avg_quality,
    SUM(gap_count) as total_gaps
FROM coverage_intervals
GROUP BY bucket, symbol, vendor, data_type;

-- Daily coverage continuous aggregate  
CREATE MATERIALIZED VIEW IF NOT EXISTS coverage_daily
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket(INTERVAL '1 day', start_time) AS bucket,
    symbol,
    vendor,
    data_type,
    COUNT(*) as interval_count,
    SUM(record_count) as total_records,
    SUM(expected_count) as total_expected,
    AVG(completeness_ratio) as avg_completeness,
    AVG(avg_quality_score) as avg_quality,
    SUM(gap_count) as total_gaps,
    
    -- Performance metrics
    SUM(record_count)::NUMERIC / 
        NULLIF(EXTRACT(EPOCH FROM INTERVAL '1 day') / 60, 0) as records_per_minute,
    
    -- Coverage classification
    CASE 
        WHEN AVG(completeness_ratio) >= 0.95 THEN 'excellent'
        WHEN AVG(completeness_ratio) >= 0.90 THEN 'good'
        WHEN AVG(completeness_ratio) >= 0.80 THEN 'fair'
        ELSE 'poor'
    END as coverage_grade
    
FROM coverage_intervals
GROUP BY bucket, symbol, vendor, data_type;

-- Add refresh policies for real-time updates
SELECT add_continuous_aggregate_policy('coverage_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('coverage_daily',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- =====================================================
-- Default SLA Configuration
-- =====================================================

-- Insert default SLA configurations for major vendors
INSERT INTO coverage_sla (vendor, data_type, min_coverage_percentage, max_acceptable_gap_minutes, warning_threshold, critical_threshold)
VALUES 
    ('polygon', 'minute', 95.0, 2, 90.0, 80.0),
    ('polygon', 'daily', 99.0, 1440, 95.0, 90.0),
    ('tiingo', 'minute', 90.0, 5, 85.0, 75.0),
    ('tiingo', 'daily', 98.0, 1440, 95.0, 90.0),
    ('fmp', 'daily', 95.0, 1440, 90.0, 85.0),
    ('alphavantage', 'daily', 90.0, 1440, 85.0, 80.0)
ON CONFLICT (symbol, vendor, data_type) DO NOTHING;

-- =====================================================
-- Data Quality Constraints
-- =====================================================

-- Constraints for data integrity
ALTER TABLE coverage_intervals ADD CONSTRAINT coverage_intervals_completeness_check 
    CHECK (completeness_ratio >= 0.0 AND completeness_ratio <= 1.0);

ALTER TABLE coverage_intervals ADD CONSTRAINT coverage_intervals_quality_check 
    CHECK (avg_quality_score IS NULL OR (avg_quality_score >= 0.0 AND avg_quality_score <= 1.0));

ALTER TABLE coverage_intervals ADD CONSTRAINT coverage_intervals_time_order_check 
    CHECK (start_time < end_time);

ALTER TABLE coverage_intervals ADD CONSTRAINT coverage_intervals_record_count_check 
    CHECK (record_count >= 0 AND expected_count >= 0);

ALTER TABLE coverage_gaps ADD CONSTRAINT coverage_gaps_duration_check 
    CHECK (gap_duration_minutes > 0);

ALTER TABLE coverage_gaps ADD CONSTRAINT coverage_gaps_time_order_check 
    CHECK (gap_start < gap_end);

ALTER TABLE coverage_gaps ADD CONSTRAINT coverage_gaps_confidence_check 
    CHECK (detection_confidence IS NULL OR (detection_confidence >= 0.0 AND detection_confidence <= 1.0));

-- =====================================================
-- Update Triggers for Timestamps
-- =====================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_coverage_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply update triggers
CREATE TRIGGER trigger_coverage_intervals_updated_at
    BEFORE UPDATE ON coverage_intervals
    FOR EACH ROW
    EXECUTE FUNCTION update_coverage_updated_at();

CREATE TRIGGER trigger_coverage_stats_updated_at
    BEFORE UPDATE ON coverage_stats
    FOR EACH ROW
    EXECUTE FUNCTION update_coverage_updated_at();

CREATE TRIGGER trigger_coverage_sla_updated_at
    BEFORE UPDATE ON coverage_sla
    FOR EACH ROW
    EXECUTE FUNCTION update_coverage_updated_at();

-- =====================================================
-- Comments for Documentation
-- =====================================================

COMMENT ON TABLE coverage_intervals IS 'Core coverage tracking - contiguous data availability periods';
COMMENT ON TABLE coverage_stats IS 'Pre-computed coverage statistics for fast dashboard queries';
COMMENT ON TABLE coverage_gaps IS 'Detailed tracking of data gaps for analysis and remediation';
COMMENT ON TABLE coverage_summary IS 'Real-time summary metrics for dashboard displays';
COMMENT ON TABLE coverage_sla IS 'SLA definitions and thresholds for coverage monitoring';

COMMENT ON COLUMN coverage_intervals.completeness_ratio IS 'Ratio of actual records to expected records (0.0-1.0)';
COMMENT ON COLUMN coverage_intervals.metadata IS 'JSON metadata for additional coverage context';
COMMENT ON COLUMN coverage_gaps.gap_type IS 'Classification: missing, partial, low_quality, outlier';
COMMENT ON COLUMN coverage_gaps.gap_severity IS 'Severity level: low, medium, high, critical';
COMMENT ON COLUMN coverage_summary.current_status IS 'Real-time status: active, stale, missing, degraded';

-- =====================================================
-- Initial Data Population
-- =====================================================

-- Create initial coverage summary entries for existing data
INSERT INTO coverage_summary (symbol, vendor, data_type, current_status, coverage_24h, quality_24h, records_24h, last_updated)
SELECT DISTINCT 
    symbol,
    vendor,
    'minute' as data_type,
    CASE 
        WHEN MAX(timestamp) >= NOW() - INTERVAL '5 minutes' THEN 'active'
        WHEN MAX(timestamp) >= NOW() - INTERVAL '1 hour' THEN 'stale'
        ELSE 'missing'
    END as current_status,
    LEAST(100.0, (COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE)::NUMERIC / (6.5 * 60)) * 100.0) as coverage_24h,
    AVG(quality_score) FILTER (WHERE timestamp >= CURRENT_DATE) as quality_24h,
    COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE) as records_24h,
    NOW() as last_updated
FROM minute_bars
WHERE timestamp >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY symbol, vendor
ON CONFLICT (symbol, vendor, data_type) DO NOTHING;

-- Initialize daily prices coverage summary
INSERT INTO coverage_summary (symbol, vendor, data_type, current_status, coverage_24h, quality_24h, records_24h, last_updated)
SELECT DISTINCT 
    symbol,
    COALESCE(source, 'unknown') as vendor,
    'daily' as data_type,
    CASE 
        WHEN MAX(date) >= CURRENT_DATE - INTERVAL '1 day' THEN 'active'
        WHEN MAX(date) >= CURRENT_DATE - INTERVAL '7 days' THEN 'stale'
        ELSE 'missing'
    END as current_status,
    CASE 
        WHEN COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '1 day') > 0 THEN 100.0
        ELSE 0.0
    END as coverage_24h,
    NULL as quality_24h, -- No quality score for daily prices yet
    COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '1 day') as records_24h,
    NOW() as last_updated
FROM daily_prices
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY symbol, source
ON CONFLICT (symbol, vendor, data_type) DO NOTHING;