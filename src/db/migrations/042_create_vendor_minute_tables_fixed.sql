-- Migration 042 (Fixed): Create vendor-specific minute bar tables for real-time collection
-- Supports 1-minute latency streaming with delay detection and validation

-- Polygon real-time minute bars
CREATE TABLE IF NOT EXISTS dev_one_minute_live_polygon (
    id BIGSERIAL,
    instrument_id INTEGER REFERENCES dev_instruments(id),
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12,4) NOT NULL,
    high_price NUMERIC(12,4) NOT NULL,
    low_price NUMERIC(12,4) NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    vwap NUMERIC(12,4),
    trade_count INTEGER,
    
    -- Real-time specific fields
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_latency_ms INTEGER,
    collection_method TEXT DEFAULT 'websocket',
    is_realtime BOOLEAN DEFAULT TRUE,
    
    -- Data quality and validation
    quality_score NUMERIC(3,2) DEFAULT 0.8,
    validation_status TEXT DEFAULT 'pending',
    data_source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Tiingo real-time minute bars  
CREATE TABLE IF NOT EXISTS dev_one_minute_live_tiingo (
    id BIGSERIAL,
    instrument_id INTEGER REFERENCES dev_instruments(id),
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12,4) NOT NULL,
    high_price NUMERIC(12,4) NOT NULL,
    low_price NUMERIC(12,4) NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    adj_close_price NUMERIC(12,4),
    volume BIGINT NOT NULL DEFAULT 0,
    
    -- Real-time specific fields
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_latency_ms INTEGER,
    collection_method TEXT DEFAULT 'websocket',
    is_realtime BOOLEAN DEFAULT TRUE,
    
    -- Data quality and validation
    quality_score NUMERIC(3,2) DEFAULT 0.8,
    validation_status TEXT DEFAULT 'pending',
    data_source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- FMP real-time minute bars
CREATE TABLE IF NOT EXISTS dev_one_minute_live_fmp (
    id BIGSERIAL,
    instrument_id INTEGER REFERENCES dev_instruments(id),
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(12,4) NOT NULL,
    high_price NUMERIC(12,4) NOT NULL,
    low_price NUMERIC(12,4) NOT NULL,
    close_price NUMERIC(12,4) NOT NULL,
    adj_close_price NUMERIC(12,4),
    volume BIGINT NOT NULL DEFAULT 0,
    
    -- Real-time specific fields
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_latency_ms INTEGER,
    collection_method TEXT DEFAULT 'polling',
    is_realtime BOOLEAN DEFAULT TRUE,
    
    -- Data quality and validation
    quality_score NUMERIC(3,2) DEFAULT 0.8,
    validation_status TEXT DEFAULT 'pending',
    data_source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Real-time data collection status tracking
CREATE TABLE IF NOT EXISTS dev_realtime_collection_status (
    id BIGSERIAL PRIMARY KEY,
    vendor TEXT NOT NULL,
    symbol TEXT NOT NULL,
    last_received_timestamp TIMESTAMPTZ,
    expected_timestamp TIMESTAMPTZ,
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

-- Real-time vs batch validation results
CREATE TABLE IF NOT EXISTS dev_realtime_batch_validation (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    validation_date DATE NOT NULL,
    vendor TEXT NOT NULL,
    
    -- Comparison metrics
    realtime_bars_count INTEGER DEFAULT 0,
    batch_bars_count INTEGER DEFAULT 0,
    missing_realtime_bars INTEGER DEFAULT 0,
    discrepant_prices INTEGER DEFAULT 0,
    avg_price_difference NUMERIC(8,6),
    max_price_difference NUMERIC(8,6),
    
    -- Latency analysis
    avg_data_latency_minutes NUMERIC(6,2),
    max_data_latency_minutes NUMERIC(6,2),
    late_bars_count INTEGER DEFAULT 0,
    
    -- Quality scores
    realtime_quality_score NUMERIC(3,2),
    batch_quality_score NUMERIC(3,2),
    overall_accuracy_score NUMERIC(3,2),
    
    -- Validation status
    validation_status TEXT DEFAULT 'pending',
    validation_notes TEXT,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, validation_date, vendor)
);

-- Data gap detection and backfill tracking
CREATE TABLE IF NOT EXISTS dev_realtime_gaps (
    id BIGSERIAL PRIMARY KEY,
    vendor TEXT NOT NULL,
    symbol TEXT NOT NULL,
    gap_start_timestamp TIMESTAMPTZ NOT NULL,
    gap_end_timestamp TIMESTAMPTZ NOT NULL,
    gap_duration_minutes INTEGER NOT NULL,
    missing_bars_count INTEGER NOT NULL,
    
    -- Gap analysis
    gap_type TEXT NOT NULL,
    detection_method TEXT DEFAULT 'realtime',
    gap_severity TEXT DEFAULT 'medium',
    
    -- Backfill status
    backfill_status TEXT DEFAULT 'pending',
    backfill_method TEXT,
    backfilled_bars_count INTEGER DEFAULT 0,
    backfill_started_at TIMESTAMPTZ,
    backfill_completed_at TIMESTAMPTZ,
    backfill_error_message TEXT,
    
    detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create TimescaleDB hypertables for time-series optimization
-- Note: Adding timestamp to PRIMARY KEY for TimescaleDB compatibility
ALTER TABLE dev_one_minute_live_polygon ADD PRIMARY KEY (id, timestamp);
ALTER TABLE dev_one_minute_live_tiingo ADD PRIMARY KEY (id, timestamp);
ALTER TABLE dev_one_minute_live_fmp ADD PRIMARY KEY (id, timestamp);

-- Create hypertables
SELECT create_hypertable('dev_one_minute_live_polygon', 'timestamp', if_not_exists => TRUE);
SELECT create_hypertable('dev_one_minute_live_tiingo', 'timestamp', if_not_exists => TRUE);  
SELECT create_hypertable('dev_one_minute_live_fmp', 'timestamp', if_not_exists => TRUE);

-- Add unique constraints (now compatible with hypertables)
ALTER TABLE dev_one_minute_live_polygon ADD CONSTRAINT unique_polygon_instrument_time 
    UNIQUE (instrument_id, timestamp);
ALTER TABLE dev_one_minute_live_tiingo ADD CONSTRAINT unique_tiingo_instrument_time 
    UNIQUE (instrument_id, timestamp);
ALTER TABLE dev_one_minute_live_fmp ADD CONSTRAINT unique_fmp_instrument_time 
    UNIQUE (instrument_id, timestamp);

-- Indexes for real-time performance
CREATE INDEX IF NOT EXISTS idx_live_polygon_symbol_time ON dev_one_minute_live_polygon (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_polygon_received_at ON dev_one_minute_live_polygon (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_live_polygon_latency ON dev_one_minute_live_polygon (data_latency_ms) WHERE data_latency_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_live_tiingo_symbol_time ON dev_one_minute_live_tiingo (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_tiingo_received_at ON dev_one_minute_live_tiingo (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_live_tiingo_latency ON dev_one_minute_live_tiingo (data_latency_ms) WHERE data_latency_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_live_fmp_symbol_time ON dev_one_minute_live_fmp (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_fmp_received_at ON dev_one_minute_live_fmp (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_live_fmp_latency ON dev_one_minute_live_fmp (data_latency_ms) WHERE data_latency_ms IS NOT NULL;

-- Indexes for monitoring and validation
CREATE INDEX IF NOT EXISTS idx_collection_status_vendor_symbol ON dev_realtime_collection_status (vendor, symbol);
CREATE INDEX IF NOT EXISTS idx_collection_status_health ON dev_realtime_collection_status (collection_health_score) WHERE collection_health_score < 0.8;

CREATE INDEX IF NOT EXISTS idx_validation_date_vendor ON dev_realtime_batch_validation (validation_date DESC, vendor);
CREATE INDEX IF NOT EXISTS idx_validation_accuracy ON dev_realtime_batch_validation (overall_accuracy_score) WHERE overall_accuracy_score < 0.9;

CREATE INDEX IF NOT EXISTS idx_gaps_vendor_symbol_time ON dev_realtime_gaps (vendor, symbol, gap_start_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_gaps_backfill_status ON dev_realtime_gaps (backfill_status) WHERE backfill_status != 'completed';

-- Data retention policies (keep 90 days of real-time data)
SELECT add_retention_policy('dev_one_minute_live_polygon', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('dev_one_minute_live_tiingo', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('dev_one_minute_live_fmp', INTERVAL '90 days', if_not_exists => TRUE);

-- Compression policies (compress data older than 7 days)
SELECT add_compression_policy('dev_one_minute_live_polygon', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('dev_one_minute_live_tiingo', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('dev_one_minute_live_fmp', INTERVAL '7 days', if_not_exists => TRUE);

-- Unified real-time view across all vendors
CREATE OR REPLACE VIEW dev_one_minute_live_unified AS
WITH vendor_data AS (
    SELECT 
        'polygon' as vendor,
        instrument_id,
        symbol,
        timestamp,
        open_price,
        high_price,
        low_price, 
        close_price,
        volume,
        vwap,
        received_at,
        data_latency_ms,
        collection_method,
        is_realtime,
        quality_score,
        validation_status
    FROM dev_one_minute_live_polygon
    
    UNION ALL
    
    SELECT 
        'tiingo' as vendor,
        instrument_id,
        symbol,
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        NULL as vwap,
        received_at,
        data_latency_ms,
        collection_method,
        is_realtime,
        quality_score,
        validation_status
    FROM dev_one_minute_live_tiingo
    
    UNION ALL
    
    SELECT 
        'fmp' as vendor,
        instrument_id,
        symbol,
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        NULL as vwap,
        received_at,
        data_latency_ms,
        collection_method,
        is_realtime,
        quality_score,
        validation_status
    FROM dev_one_minute_live_fmp
)
SELECT * FROM vendor_data
ORDER BY symbol, timestamp DESC, vendor;

-- Real-time data quality monitoring view
CREATE OR REPLACE VIEW dev_realtime_quality_dashboard AS
SELECT 
    rcs.vendor,
    rcs.symbol,
    rcs.last_received_timestamp,
    rcs.data_delay_minutes,
    rcs.consecutive_missing_bars,
    rcs.collection_health_score,
    
    -- Latest validation results
    rbv.realtime_quality_score,
    rbv.overall_accuracy_score,
    rbv.avg_price_difference,
    
    -- Active gaps
    COUNT(rg.id) FILTER (WHERE rg.backfill_status != 'completed') as active_gaps_count,
    MAX(rg.gap_duration_minutes) as max_gap_duration_minutes,
    
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
    rbv.realtime_quality_score, rbv.overall_accuracy_score, rbv.avg_price_difference,
    rcs.updated_at
ORDER BY rcs.collection_health_score ASC, rcs.data_delay_minutes DESC;

-- Update triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_realtime_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_live_polygon_updated_at
    BEFORE UPDATE ON dev_one_minute_live_polygon
    FOR EACH ROW EXECUTE FUNCTION update_realtime_updated_at();

CREATE TRIGGER trigger_live_tiingo_updated_at
    BEFORE UPDATE ON dev_one_minute_live_tiingo
    FOR EACH ROW EXECUTE FUNCTION update_realtime_updated_at();

CREATE TRIGGER trigger_live_fmp_updated_at
    BEFORE UPDATE ON dev_one_minute_live_fmp
    FOR EACH ROW EXECUTE FUNCTION update_realtime_updated_at();

CREATE TRIGGER trigger_collection_status_updated_at
    BEFORE UPDATE ON dev_realtime_collection_status
    FOR EACH ROW EXECUTE FUNCTION update_realtime_updated_at();

CREATE TRIGGER trigger_gaps_updated_at
    BEFORE UPDATE ON dev_realtime_gaps
    FOR EACH ROW EXECUTE FUNCTION update_realtime_updated_at();

-- Comments for documentation
COMMENT ON TABLE dev_one_minute_live_polygon IS 'Real-time 1-minute bars from Polygon with latency tracking';
COMMENT ON TABLE dev_one_minute_live_tiingo IS 'Real-time 1-minute bars from Tiingo with latency tracking';
COMMENT ON TABLE dev_one_minute_live_fmp IS 'Real-time 1-minute bars from FMP with latency tracking';
COMMENT ON TABLE dev_realtime_collection_status IS 'Monitoring status for real-time data collection per vendor/symbol';
COMMENT ON TABLE dev_realtime_batch_validation IS 'Daily validation comparing real-time vs batch data quality';
COMMENT ON TABLE dev_realtime_gaps IS 'Detected data gaps and backfill tracking for real-time streams';

COMMENT ON COLUMN dev_one_minute_live_polygon.data_latency_ms IS 'Milliseconds between bar close time and when we received it';
COMMENT ON COLUMN dev_one_minute_live_polygon.collection_method IS 'How the data was collected: websocket, polling, backfill';
COMMENT ON COLUMN dev_realtime_collection_status.collection_health_score IS 'Overall health score 0.0-1.0 based on latency, gaps, and accuracy';