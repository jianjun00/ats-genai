-- Coverage Monitoring System Database Schema
-- Migration: Add comprehensive data coverage tracking tables

-- 1. Data Coverage Tracking - Core table for tracking what data we have
CREATE TABLE IF NOT EXISTS dev_data_coverage_tracking (
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

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_coverage_tracking_lookup 
ON dev_data_coverage_tracking(vendor, data_type, trading_date);

CREATE INDEX IF NOT EXISTS idx_coverage_tracking_symbol 
ON dev_data_coverage_tracking(symbol, trading_date);

CREATE INDEX IF NOT EXISTS idx_coverage_tracking_status 
ON dev_data_coverage_tracking(coverage_status, vendor);

-- 2. Coverage Gaps - Actionable queue for backfill operations
CREATE TABLE IF NOT EXISTS dev_coverage_gaps (
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

-- Index for gap processing
CREATE INDEX IF NOT EXISTS idx_coverage_gaps_priority 
ON dev_coverage_gaps(priority_score DESC, backfill_status, created_at);

CREATE INDEX IF NOT EXISTS idx_coverage_gaps_vendor 
ON dev_coverage_gaps(vendor, data_type, backfill_status);

-- 3. Daily Coverage Metrics - High-level trending data
CREATE TABLE IF NOT EXISTS dev_daily_coverage_metrics (
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

-- Index for trending analysis
CREATE INDEX IF NOT EXISTS idx_daily_metrics_trending 
ON dev_daily_coverage_metrics(vendor, data_type, metric_date);

-- 4. Backfill Operations Log - Track all backfill operations
CREATE TABLE IF NOT EXISTS dev_backfill_operations (
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

-- Index for operations tracking
CREATE INDEX IF NOT EXISTS idx_backfill_ops_status 
ON dev_backfill_operations(status, vendor, created_at);

-- 5. Priority Symbols Configuration - Define which symbols are most important
CREATE TABLE IF NOT EXISTS dev_priority_symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    priority_tier INTEGER NOT NULL,        -- 1=Critical, 2=High, 3=Medium, 4=Low
    priority_multiplier DECIMAL(3,1) NOT NULL DEFAULT 1.0,
    reason VARCHAR(100),                   -- Why this symbol is prioritized
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for priority lookups
CREATE INDEX IF NOT EXISTS idx_priority_symbols_tier 
ON dev_priority_symbols(priority_tier, active);

-- Insert default priority symbols
INSERT INTO dev_priority_symbols (symbol, priority_tier, priority_multiplier, reason) VALUES
-- Tier 1: Critical (4x multiplier)
('SPY', 1, 4.0, 'S&P 500 ETF - Market benchmark'),
('QQQ', 1, 4.0, 'NASDAQ 100 ETF - Tech benchmark'), 
('AAPL', 1, 4.0, 'Apple - Largest market cap'),
('MSFT', 1, 4.0, 'Microsoft - Major tech stock'),
('GOOGL', 1, 4.0, 'Google - Major tech stock'),
('AMZN', 1, 4.0, 'Amazon - Major tech stock'),
('NVDA', 1, 4.0, 'NVIDIA - AI/Chip leader'),
('TSLA', 1, 4.0, 'Tesla - EV leader'),

-- Tier 2: High Priority (2x multiplier)
('META', 2, 2.0, 'Meta - Social media giant'),
('NFLX', 2, 2.0, 'Netflix - Streaming leader'),
('IWM', 2, 2.0, 'Russell 2000 ETF - Small cap benchmark'),
('VTI', 2, 2.0, 'Total Stock Market ETF'),
('JPM', 2, 2.0, 'JPMorgan - Banking leader'),
('UNH', 2, 2.0, 'UnitedHealth - Healthcare leader'),

-- Tier 3: Medium Priority (1.5x multiplier)
('XLF', 3, 1.5, 'Financial sector ETF'),
('XLK', 3, 1.5, 'Technology sector ETF'),
('XLE', 3, 1.5, 'Energy sector ETF'),
('GLD', 3, 1.5, 'Gold ETF - Safe haven'),
('TLT', 3, 1.5, 'Treasury bond ETF')

ON CONFLICT (symbol) DO UPDATE SET
    priority_tier = EXCLUDED.priority_tier,
    priority_multiplier = EXCLUDED.priority_multiplier,
    reason = EXCLUDED.reason,
    updated_at = NOW();

-- 6. Coverage Alert Thresholds - Configuration for alerting
CREATE TABLE IF NOT EXISTS dev_coverage_alert_thresholds (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,       -- 'critical_gap', 'coverage_degradation', 'backfill_failure'
    vendor VARCHAR(20),                    -- NULL means applies to all vendors
    data_type VARCHAR(20),                 -- NULL means applies to all data types
    threshold_config JSONB NOT NULL,       -- JSON configuration for thresholds
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Insert default alert thresholds
INSERT INTO dev_coverage_alert_thresholds (alert_type, threshold_config) VALUES
('critical_symbol_gap', '{"max_gap_days": 1, "max_response_hours": 2, "apply_to_tier": 1}'),
('coverage_degradation', '{"min_coverage_pct": 85, "trending_window_days": 7}'),
('backfill_failures', '{"max_consecutive_failures": 3, "max_failure_rate": 0.1}');

-- Create views for common queries

-- View: Current coverage summary by vendor
CREATE OR REPLACE VIEW v_current_coverage_summary AS
SELECT 
    vendor,
    data_type,
    COUNT(DISTINCT symbol) as total_symbols,
    COUNT(DISTINCT CASE WHEN coverage_status = 'complete' THEN symbol END) as symbols_complete,
    COUNT(DISTINCT CASE WHEN coverage_status = 'missing' THEN symbol END) as symbols_missing,
    ROUND(
        COUNT(DISTINCT CASE WHEN coverage_status = 'complete' THEN symbol END) * 100.0 / 
        COUNT(DISTINCT symbol), 2
    ) as coverage_percentage,
    MAX(last_updated) as last_scan_time
FROM dev_data_coverage_tracking 
WHERE trading_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY vendor, data_type;

-- View: Active gaps requiring backfill
CREATE OR REPLACE VIEW v_active_backfill_queue AS
SELECT 
    g.*,
    ps.priority_tier,
    ps.priority_multiplier,
    g.priority_score * COALESCE(ps.priority_multiplier, 1.0) as adjusted_priority
FROM dev_coverage_gaps g
LEFT JOIN dev_priority_symbols ps ON g.symbol = ps.symbol AND ps.active = true
WHERE g.backfill_status = 'pending'
ORDER BY adjusted_priority DESC, g.created_at ASC;

-- View: Daily coverage trending
CREATE OR REPLACE VIEW v_coverage_trending AS
SELECT 
    metric_date,
    vendor,
    data_type,
    coverage_percentage,
    LAG(coverage_percentage) OVER (
        PARTITION BY vendor, data_type 
        ORDER BY metric_date
    ) as prev_day_coverage,
    coverage_percentage - LAG(coverage_percentage) OVER (
        PARTITION BY vendor, data_type 
        ORDER BY metric_date
    ) as coverage_change
FROM dev_daily_coverage_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY vendor, data_type, metric_date;

-- Add comments for documentation
COMMENT ON TABLE dev_data_coverage_tracking IS 'Tracks actual data coverage for each vendor/symbol/date combination';
COMMENT ON TABLE dev_coverage_gaps IS 'Actionable queue of data gaps requiring backfill operations';
COMMENT ON TABLE dev_daily_coverage_metrics IS 'Daily aggregated coverage metrics for trending analysis';
COMMENT ON TABLE dev_backfill_operations IS 'Log of all backfill operations performed by the system';
COMMENT ON TABLE dev_priority_symbols IS 'Configuration of symbol priorities for backfill scheduling';
COMMENT ON VIEW v_current_coverage_summary IS 'Real-time coverage summary by vendor and data type';
COMMENT ON VIEW v_active_backfill_queue IS 'Priority-ordered queue of gaps requiring backfill';

-- ================================================================
-- VIEW: Recent gaps requiring immediate attention
-- ================================================================
CREATE OR REPLACE VIEW v_recent_gaps AS
SELECT 
    g.id as gap_id,
    g.vendor,
    g.data_type,
    g.symbol,
    g.gap_start_date,
    g.gap_end_date,
    g.gap_days,
    g.priority_score,
    g.estimated_effort_minutes,
    g.created_at,
    ps.priority_level,
    ps.business_impact,
    CASE 
        WHEN g.priority_score >= 8 THEN 'critical'
        WHEN g.priority_score >= 6 THEN 'high'
        WHEN g.priority_score >= 4 THEN 'medium'
        ELSE 'low'
    END as urgency_level,
    -- Days since gap was detected
    EXTRACT(days FROM CURRENT_TIMESTAMP - g.created_at) as days_since_detected
FROM dev_coverage_gaps g
LEFT JOIN dev_priority_symbols ps ON g.symbol = ps.symbol
WHERE g.backfill_status = 'pending'
    AND g.created_at >= CURRENT_DATE - INTERVAL '30 days'  -- Recent gaps only
ORDER BY g.priority_score DESC, g.gap_days DESC, g.created_at DESC;

COMMENT ON VIEW v_recent_gaps IS 'Recent data gaps requiring immediate attention, ordered by priority';