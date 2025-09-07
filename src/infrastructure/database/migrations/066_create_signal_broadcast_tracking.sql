-- Migration 066: Create signal broadcast tracking table
-- This migration creates the table for tracking signal broadcast delivery status and performance

CREATE TABLE dev_signal_broadcasts (
    id BIGSERIAL PRIMARY KEY,
    
    -- Signal Reference
    signal_id BIGINT NOT NULL REFERENCES dev_critical_news_signals(id) ON DELETE CASCADE,
    
    -- Broadcast Target Information
    target_name VARCHAR(100) NOT NULL,
    channel VARCHAR(50) NOT NULL CHECK (channel IN ('websocket', 'rest_api', 'message_queue', 'email_alert', 'slack_alert', 'portfolio_system', 'analytics_dashboard')),
    
    -- Broadcast Result
    success BOOLEAN NOT NULL,
    broadcast_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Performance Metrics
    latency_ms INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    
    -- Error Details
    error_message TEXT,
    error_code VARCHAR(50),
    
    -- Additional Metadata
    target_endpoint VARCHAR(500),
    payload_size_bytes INTEGER,
    response_status_code INTEGER,
    
    -- Standard Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Performance indexes for broadcast tracking
CREATE INDEX idx_signal_broadcasts_signal_id ON dev_signal_broadcasts(signal_id);
CREATE INDEX idx_signal_broadcasts_timestamp ON dev_signal_broadcasts(broadcast_timestamp DESC);
CREATE INDEX idx_signal_broadcasts_success ON dev_signal_broadcasts(success, broadcast_timestamp DESC);
CREATE INDEX idx_signal_broadcasts_channel ON dev_signal_broadcasts(channel, broadcast_timestamp DESC);
CREATE INDEX idx_signal_broadcasts_target ON dev_signal_broadcasts(target_name, broadcast_timestamp DESC);

-- Composite indexes for analytics queries
CREATE INDEX idx_signal_broadcasts_success_channel ON dev_signal_broadcasts(success, channel, broadcast_timestamp DESC);
CREATE INDEX idx_signal_broadcasts_signal_success ON dev_signal_broadcasts(signal_id, success);
CREATE INDEX idx_signal_broadcasts_latency_performance ON dev_signal_broadcasts(latency_ms, broadcast_timestamp DESC) WHERE success = TRUE;

-- Comments for documentation
COMMENT ON TABLE dev_signal_broadcasts IS 'Tracking table for signal broadcast delivery status and performance';
COMMENT ON COLUMN dev_signal_broadcasts.channel IS 'Broadcast channel type: websocket, rest_api, email_alert, etc.';
COMMENT ON COLUMN dev_signal_broadcasts.latency_ms IS 'Time taken to deliver the broadcast in milliseconds';
COMMENT ON COLUMN dev_signal_broadcasts.retry_count IS 'Number of retry attempts made for this broadcast';
COMMENT ON COLUMN dev_signal_broadcasts.payload_size_bytes IS 'Size of the broadcast payload in bytes';

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_signal_broadcasts TO postgres;
GRANT USAGE ON SEQUENCE dev_signal_broadcasts_id_seq TO postgres;

-- Create function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_signal_broadcasts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update the updated_at field
CREATE TRIGGER trigger_update_signal_broadcasts_updated_at
    BEFORE UPDATE ON dev_signal_broadcasts
    FOR EACH ROW
    EXECUTE FUNCTION update_signal_broadcasts_updated_at();

-- Create view for broadcast performance analytics
CREATE VIEW dev_signal_broadcast_performance AS
SELECT 
    channel,
    target_name,
    COUNT(*) as total_broadcasts,
    COUNT(CASE WHEN success THEN 1 END) as successful_broadcasts,
    COUNT(CASE WHEN NOT success THEN 1 END) as failed_broadcasts,
    ROUND(
        100.0 * COUNT(CASE WHEN success THEN 1 END) / NULLIF(COUNT(*), 0), 
        2
    ) as success_rate_pct,
    
    -- Performance metrics (for successful broadcasts)
    AVG(CASE WHEN success THEN latency_ms END) as avg_latency_ms,
    MIN(CASE WHEN success THEN latency_ms END) as min_latency_ms,
    MAX(CASE WHEN success THEN latency_ms END) as max_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN success THEN latency_ms END) as p95_latency_ms,
    
    -- Retry analysis
    AVG(retry_count) as avg_retry_count,
    MAX(retry_count) as max_retry_count,
    COUNT(CASE WHEN retry_count > 0 THEN 1 END) as broadcasts_with_retries,
    
    -- Error analysis
    COUNT(DISTINCT error_code) as unique_error_codes,
    MODE() WITHIN GROUP (ORDER BY error_code) as most_common_error_code,
    
    -- Time analysis
    MIN(broadcast_timestamp) as first_broadcast,
    MAX(broadcast_timestamp) as latest_broadcast,
    COUNT(DISTINCT DATE(broadcast_timestamp)) as active_days

FROM dev_signal_broadcasts
WHERE broadcast_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY channel, target_name
ORDER BY total_broadcasts DESC;

GRANT SELECT ON dev_signal_broadcast_performance TO postgres;

-- Create view for real-time broadcast monitoring
CREATE VIEW dev_signal_broadcast_monitoring AS
SELECT 
    s.id as signal_id,
    s.symbol,
    s.signal_type,
    s.urgency_level,
    s.signal_strength,
    s.signal_confidence,
    s.signal_timestamp,
    
    -- Broadcast summary
    COUNT(b.id) as total_broadcast_attempts,
    COUNT(CASE WHEN b.success THEN 1 END) as successful_broadcasts,
    COUNT(CASE WHEN NOT b.success THEN 1 END) as failed_broadcasts,
    
    -- Performance metrics
    AVG(b.latency_ms) as avg_broadcast_latency_ms,
    MAX(b.latency_ms) as max_broadcast_latency_ms,
    
    -- Status flags
    BOOL_AND(b.success) as all_broadcasts_successful,
    MAX(b.broadcast_timestamp) as latest_broadcast_time,
    
    -- Channel breakdown
    STRING_AGG(DISTINCT b.channel, ', ') as broadcast_channels,
    STRING_AGG(
        DISTINCT CASE WHEN NOT b.success THEN b.channel || '(' || COALESCE(b.error_code, 'unknown') || ')' END, 
        ', '
    ) as failed_channels

FROM dev_critical_news_signals s
LEFT JOIN dev_signal_broadcasts b ON s.id = b.signal_id
WHERE s.signal_timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
GROUP BY s.id, s.symbol, s.signal_type, s.urgency_level, s.signal_strength, s.signal_confidence, s.signal_timestamp
ORDER BY s.signal_timestamp DESC;

GRANT SELECT ON dev_signal_broadcast_monitoring TO postgres;

-- Create materialized view for broadcast analytics dashboard
CREATE MATERIALIZED VIEW dev_broadcast_analytics_dashboard AS
SELECT 
    -- Time buckets
    DATE_TRUNC('hour', broadcast_timestamp) as hour_bucket,
    DATE_TRUNC('day', broadcast_timestamp) as day_bucket,
    
    -- Broadcast metrics by channel
    channel,
    COUNT(*) as total_broadcasts,
    COUNT(CASE WHEN success THEN 1 END) as successful_broadcasts,
    ROUND(
        100.0 * COUNT(CASE WHEN success THEN 1 END) / NULLIF(COUNT(*), 0),
        2
    ) as success_rate_pct,
    
    -- Performance metrics
    AVG(latency_ms) as avg_latency_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) as median_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency_ms,
    
    -- Volume metrics
    COUNT(DISTINCT signal_id) as unique_signals_broadcast,
    AVG(payload_size_bytes) as avg_payload_size_bytes,
    
    -- Error metrics
    COUNT(CASE WHEN NOT success THEN 1 END) as error_count,
    COUNT(CASE WHEN retry_count > 0 THEN 1 END) as retries_needed,
    AVG(retry_count) as avg_retry_count,
    
    -- Top error codes for this time bucket and channel
    MODE() WITHIN GROUP (ORDER BY error_code) FILTER (WHERE error_code IS NOT NULL) as most_common_error
    
FROM dev_signal_broadcasts
WHERE broadcast_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY 
    DATE_TRUNC('hour', broadcast_timestamp),
    DATE_TRUNC('day', broadcast_timestamp),
    channel
ORDER BY hour_bucket DESC, channel;

-- Create unique index for materialized view
CREATE UNIQUE INDEX idx_broadcast_analytics_dashboard_unique 
ON dev_broadcast_analytics_dashboard(hour_bucket, channel);

-- Grant permissions on materialized view
GRANT SELECT ON dev_broadcast_analytics_dashboard TO postgres;

-- Create function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_broadcast_analytics_dashboard()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW dev_broadcast_analytics_dashboard;
END;
$$ LANGUAGE plpgsql;

-- Create view for broadcast delivery status by signal
CREATE VIEW dev_signal_delivery_status AS
SELECT 
    s.id,
    s.symbol,
    s.signal_type,
    s.urgency_level,
    s.signal_timestamp,
    
    -- Delivery status summary
    CASE 
        WHEN COUNT(b.id) = 0 THEN 'not_broadcast'
        WHEN COUNT(b.id) > 0 AND COUNT(CASE WHEN b.success THEN 1 END) = 0 THEN 'all_failed'
        WHEN COUNT(b.id) > 0 AND COUNT(CASE WHEN b.success THEN 1 END) = COUNT(b.id) THEN 'all_successful' 
        ELSE 'partial_success'
    END as delivery_status,
    
    -- Delivery metrics
    COUNT(b.id) as broadcast_attempts,
    COUNT(CASE WHEN b.success THEN 1 END) as successful_deliveries,
    COUNT(DISTINCT b.channel) as channels_attempted,
    COUNT(DISTINCT CASE WHEN b.success THEN b.channel END) as channels_successful,
    
    -- Timing
    MIN(b.broadcast_timestamp) as first_broadcast_time,
    MAX(b.broadcast_timestamp) as last_broadcast_time,
    EXTRACT(EPOCH FROM (MAX(b.broadcast_timestamp) - MIN(b.broadcast_timestamp))) as broadcast_duration_seconds,
    
    -- Performance
    AVG(CASE WHEN b.success THEN b.latency_ms END) as avg_successful_latency_ms
    
FROM dev_critical_news_signals s
LEFT JOIN dev_signal_broadcasts b ON s.id = b.signal_id
WHERE s.signal_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY s.id, s.symbol, s.signal_type, s.urgency_level, s.signal_timestamp
ORDER BY s.signal_timestamp DESC;

GRANT SELECT ON dev_signal_delivery_status TO postgres;