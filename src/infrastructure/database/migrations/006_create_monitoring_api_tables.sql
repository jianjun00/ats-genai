-- Migration 006: Create monitoring and API tracking tables from intg schema
-- Generated from current intg database schema on 2025-09-15

-- Update db_version
INSERT INTO db_version (version, description) VALUES 
(6, 'Monitoring and API tracking tables - health, metrics, quality')
ON CONFLICT (version) DO NOTHING;

-- API health and tracking
CREATE TABLE IF NOT EXISTS vendor_api_health (
    id SERIAL PRIMARY KEY,
    vendor_name TEXT NOT NULL,
    endpoint TEXT,
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'down')),
    response_time_ms INTEGER,
    error_count INTEGER DEFAULT 0,
    success_rate DOUBLE PRECISION,
    last_check TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_calls (
    id SERIAL PRIMARY KEY,
    vendor TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    method TEXT DEFAULT 'GET',
    status_code INTEGER,
    response_time_ms INTEGER,
    request_size_bytes INTEGER,
    response_size_bytes INTEGER,
    error_message TEXT,
    called_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Specific API call tracking tables
CREATE TABLE IF NOT EXISTS minute_bar_api_calls (
    id SERIAL PRIMARY KEY,
    vendor TEXT NOT NULL,
    symbol TEXT NOT NULL,
    start_date DATE,
    end_date DATE,
    status_code INTEGER,
    response_time_ms INTEGER,
    records_returned INTEGER,
    error_message TEXT,
    called_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_api_calls (
    id SERIAL PRIMARY KEY,
    vendor TEXT NOT NULL,
    endpoint TEXT,
    symbols TEXT[],
    start_date DATE,
    end_date DATE,
    status_code INTEGER,
    response_time_ms INTEGER,
    articles_returned INTEGER,
    error_message TEXT,
    called_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data quality monitoring
CREATE TABLE IF NOT EXISTS data_quality_issues (
    id SERIAL PRIMARY KEY,
    issue_type TEXT NOT NULL, -- 'missing_data', 'duplicate', 'outlier', 'inconsistent'
    severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    table_name TEXT NOT NULL,
    column_name TEXT,
    symbol TEXT,
    date_range_start DATE,
    date_range_end DATE,
    description TEXT,
    affected_records INTEGER,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE,
    status TEXT DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved', 'ignored'))
);

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    metric_name TEXT NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'timeliness'
    metric_value DOUBLE PRECISION,
    target_value DOUBLE PRECISION,
    measurement_date DATE NOT NULL,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, metric_name, measurement_date)
);

-- Data quality agent operations tracking
CREATE TABLE IF NOT EXISTS data_quality_agent_operations (
    id SERIAL PRIMARY KEY,
    operation_type TEXT NOT NULL, -- 'scan', 'validate', 'repair', 'alert'
    target_table TEXT,
    target_date_range DATERANGE,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    issues_found INTEGER DEFAULT 0,
    issues_resolved INTEGER DEFAULT 0,
    execution_time_seconds INTEGER,
    details JSONB,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Data quality alert configuration
CREATE TABLE IF NOT EXISTS data_quality_alert_config (
    id SERIAL PRIMARY KEY,
    alert_name TEXT NOT NULL UNIQUE,
    alert_type TEXT NOT NULL, -- 'threshold', 'anomaly', 'missing_data'
    table_name TEXT NOT NULL,
    column_name TEXT,
    condition_sql TEXT,
    threshold_value DOUBLE PRECISION,
    severity TEXT DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high')),
    notification_channels TEXT[], -- 'email', 'slack', 'webhook'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Collection metrics
CREATE TABLE IF NOT EXISTS minute_bar_collection_metrics (
    id SERIAL PRIMARY KEY,
    collection_date DATE NOT NULL,
    vendor TEXT NOT NULL,
    symbols_requested INTEGER,
    symbols_collected INTEGER,
    total_records INTEGER,
    success_rate DOUBLE PRECISION,
    avg_response_time_ms INTEGER,
    errors_count INTEGER,
    collection_duration_seconds INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(collection_date, vendor)
);

CREATE TABLE IF NOT EXISTS news_collection_metrics (
    id SERIAL PRIMARY KEY,
    collection_date DATE NOT NULL,
    vendor TEXT NOT NULL,
    articles_collected INTEGER,
    unique_symbols INTEGER,
    avg_response_time_ms INTEGER,
    errors_count INTEGER,
    collection_duration_seconds INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(collection_date, vendor)
);

-- Real-time minute bars tracking (for live data)
CREATE TABLE IF NOT EXISTS realtime_minute_bars (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    trade_count INTEGER,
    vwap DOUBLE PRECISION,
    source TEXT NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, datetime, source)
);

-- Live vendor data tracking
CREATE TABLE IF NOT EXISTS one_minute_live_polygon (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    vwap DOUBLE PRECISION,
    trade_count INTEGER,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, datetime)
);

CREATE TABLE IF NOT EXISTS one_minute_live_tiingo (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, datetime)
);

CREATE TABLE IF NOT EXISTS one_minute_live_eodhd (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, datetime)
);

-- Monitoring and API indexes
CREATE INDEX IF NOT EXISTS idx_vendor_api_health_vendor ON vendor_api_health(vendor_name);
CREATE INDEX IF NOT EXISTS idx_vendor_api_health_status ON vendor_api_health(status);
CREATE INDEX IF NOT EXISTS idx_api_calls_vendor_endpoint ON api_calls(vendor, endpoint);
CREATE INDEX IF NOT EXISTS idx_api_calls_called_at ON api_calls(called_at);
CREATE INDEX IF NOT EXISTS idx_data_quality_issues_severity ON data_quality_issues(severity);
CREATE INDEX IF NOT EXISTS idx_data_quality_issues_status ON data_quality_issues(status);
CREATE INDEX IF NOT EXISTS idx_data_quality_issues_table_name ON data_quality_issues(table_name);
CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_table_metric ON data_quality_metrics(table_name, metric_name);
CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_date ON data_quality_metrics(measurement_date);
CREATE INDEX IF NOT EXISTS idx_minute_bar_collection_metrics_date ON minute_bar_collection_metrics(collection_date);
CREATE INDEX IF NOT EXISTS idx_news_collection_metrics_date ON news_collection_metrics(collection_date);
CREATE INDEX IF NOT EXISTS idx_realtime_minute_bars_symbol_datetime ON realtime_minute_bars(symbol, datetime);
CREATE INDEX IF NOT EXISTS idx_one_minute_live_polygon_symbol_datetime ON one_minute_live_polygon(symbol, datetime);
CREATE INDEX IF NOT EXISTS idx_one_minute_live_tiingo_symbol_datetime ON one_minute_live_tiingo(symbol, datetime);
CREATE INDEX IF NOT EXISTS idx_one_minute_live_eodhd_symbol_datetime ON one_minute_live_eodhd(symbol, datetime);

-- Insert basic data quality alert configurations
INSERT INTO data_quality_alert_config (alert_name, alert_type, table_name, condition_sql, severity) VALUES 
    ('missing_daily_prices', 'missing_data', 'daily_price_tiingo', 'SELECT COUNT(*) FROM daily_price_tiingo WHERE date = CURRENT_DATE - 1', 'high'),
    ('duplicate_daily_prices', 'threshold', 'daily_price_tiingo', 'SELECT COUNT(*) - COUNT(DISTINCT date, symbol) FROM daily_price_tiingo WHERE date >= CURRENT_DATE - 7', 'medium'),
    ('zero_volume_threshold', 'threshold', 'daily_price_tiingo', 'SELECT COUNT(*) FROM daily_price_tiingo WHERE volume = 0 AND date >= CURRENT_DATE - 1', 'low'),
    ('price_outliers', 'anomaly', 'daily_price_tiingo', 'SELECT COUNT(*) FROM daily_price_tiingo WHERE ABS(close - open) / open > 0.5 AND date >= CURRENT_DATE - 1', 'medium')
ON CONFLICT (alert_name) DO NOTHING;