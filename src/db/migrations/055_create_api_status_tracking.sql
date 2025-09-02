-- Migration: Create API status tracking tables for monitoring vendor API performance
-- This enables comprehensive tracking of API calls, status codes, and performance metrics

-- API call tracking table
CREATE TABLE IF NOT EXISTS intg_api_calls (
    id BIGSERIAL PRIMARY KEY,
    vendor VARCHAR(50) NOT NULL,
    endpoint VARCHAR(200) NOT NULL,
    method VARCHAR(10) NOT NULL DEFAULT 'GET',
    status_code INTEGER NOT NULL,
    response_time_ms INTEGER,
    request_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    response_size_bytes INTEGER,
    error_message TEXT,
    symbols_requested TEXT[], -- For batch requests
    symbols_count INTEGER DEFAULT 1,
    rate_limit_remaining INTEGER,
    rate_limit_reset TIMESTAMP WITH TIME ZONE
);

-- Minute bar collection metrics table
CREATE TABLE IF NOT EXISTS intg_minute_bar_collection_metrics (
    id BIGSERIAL PRIMARY KEY,
    vendor VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    collection_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    records_collected INTEGER NOT NULL DEFAULT 0,
    collection_success BOOLEAN NOT NULL DEFAULT true,
    api_calls_made INTEGER NOT NULL DEFAULT 1,
    total_response_time_ms INTEGER,
    error_details TEXT,
    data_quality_score DECIMAL(3,3) DEFAULT 0.000
);

-- Vendor API health summary table (updated periodically)
CREATE TABLE IF NOT EXISTS intg_vendor_api_health (
    id BIGSERIAL PRIMARY KEY,
    vendor VARCHAR(50) NOT NULL,
    period_start TIMESTAMP WITH TIME ZONE NOT NULL,
    period_end TIMESTAMP WITH TIME ZONE NOT NULL,
    total_calls INTEGER NOT NULL DEFAULT 0,
    successful_calls INTEGER NOT NULL DEFAULT 0,
    failed_calls INTEGER NOT NULL DEFAULT 0,
    avg_response_time_ms DECIMAL(8,2),
    success_rate DECIMAL(5,4),
    rate_limit_hits INTEGER DEFAULT 0,
    most_common_error TEXT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_api_calls_vendor_timestamp ON intg_api_calls(vendor, request_timestamp);
CREATE INDEX IF NOT EXISTS idx_api_calls_status_code ON intg_api_calls(status_code);
CREATE INDEX IF NOT EXISTS idx_api_calls_timestamp ON intg_api_calls(request_timestamp);

CREATE INDEX IF NOT EXISTS idx_minute_bar_vendor_symbol_timestamp ON intg_minute_bar_collection_metrics(vendor, symbol, collection_timestamp);
CREATE INDEX IF NOT EXISTS idx_minute_bar_collection_timestamp ON intg_minute_bar_collection_metrics(collection_timestamp);

CREATE INDEX IF NOT EXISTS idx_vendor_health_vendor_period ON intg_vendor_api_health(vendor, period_start, period_end);

-- Unique constraint for vendor health periods (prevent duplicates)
CREATE UNIQUE INDEX IF NOT EXISTS idx_vendor_health_unique_period 
ON intg_vendor_api_health(vendor, period_start, period_end);

COMMENT ON TABLE intg_api_calls IS 'Tracks individual API calls to vendor services with status codes and performance metrics';
COMMENT ON TABLE intg_minute_bar_collection_metrics IS 'Tracks minute bar data collection events and success rates by vendor';  
COMMENT ON TABLE intg_vendor_api_health IS 'Periodic summary of vendor API health and performance metrics';