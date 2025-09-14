-- Migration 028: Create Authentication Tables
-- Created: 2025-01-16
-- Purpose: Add API key management and usage tracking for Portfolio GPT MVP

-- Drop existing tables if they exist (for clean reruns)
DROP TABLE IF EXISTS api_usage CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;

-- API Keys table for authentication and tier management
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    key_hash VARCHAR(255) UNIQUE NOT NULL,
    key_prefix VARCHAR(8) NOT NULL, -- First 8 chars for identification
    tier VARCHAR(20) NOT NULL CHECK (tier IN ('free', 'premium')),
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'revoked')),
    name VARCHAR(100), -- Human-readable name for the key
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP, -- Optional expiration
    created_by VARCHAR(100) DEFAULT 'system'
);

-- Index for fast key lookup
CREATE INDEX idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
CREATE INDEX idx_api_keys_tier ON api_keys(tier);

-- API Usage tracking for rate limiting and analytics
CREATE TABLE api_usage (
    id SERIAL PRIMARY KEY,
    api_key_id INTEGER REFERENCES api_keys(id) ON DELETE CASCADE,
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL DEFAULT 'GET',
    status_code INTEGER NOT NULL,
    request_count INTEGER DEFAULT 1,
    date DATE DEFAULT CURRENT_DATE,
    hour INTEGER DEFAULT EXTRACT(hour FROM NOW()), -- For hourly tracking
    response_time_ms INTEGER, -- Performance tracking
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Composite index for efficient usage queries
CREATE UNIQUE INDEX idx_api_usage_unique ON api_usage(api_key_id, endpoint, date, hour);
CREATE INDEX idx_api_usage_date ON api_usage(date);
CREATE INDEX idx_api_usage_key_date ON api_usage(api_key_id, date);

-- Rate limiting view for quick access to current usage
CREATE OR REPLACE VIEW daily_api_usage AS
SELECT 
    api_key_id,
    endpoint,
    date,
    SUM(request_count) as total_requests,
    AVG(response_time_ms) as avg_response_time_ms,
    MAX(updated_at) as last_request_at
FROM api_usage 
GROUP BY api_key_id, endpoint, date;

-- Insert default admin API key (premium tier)
-- Note: In production, this should be created through proper admin interface
INSERT INTO api_keys (key_hash, key_prefix, tier, name, description, created_by) 
VALUES (
    -- This is hash of 'dev_admin_key_12345678901234567890123' - change in production!
    '$2b$12$LQv3c1yqBwLVFgDJ2n/jWe.V5kB3Z9h0O8K7D.h0U2Tl9Q8K7j3Y2',
    'dev_admi',
    'premium',
    'Development Admin Key',
    'Default admin API key for development environment - CHANGE IN PRODUCTION!',
    'migration_028'
);

-- Create function to automatically update usage statistics
CREATE OR REPLACE FUNCTION update_api_usage(
    p_api_key_id INTEGER,
    p_endpoint VARCHAR(255),
    p_method VARCHAR(10),
    p_status_code INTEGER,
    p_response_time_ms INTEGER DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO api_usage (
        api_key_id, 
        endpoint, 
        method,
        status_code,
        response_time_ms,
        date,
        hour
    ) VALUES (
        p_api_key_id,
        p_endpoint,
        p_method,
        p_status_code,
        p_response_time_ms,
        CURRENT_DATE,
        EXTRACT(hour FROM NOW())
    )
    ON CONFLICT (api_key_id, endpoint, date, hour) 
    DO UPDATE SET 
        request_count = api_usage.request_count + 1,
        updated_at = NOW(),
        response_time_ms = CASE 
            WHEN p_response_time_ms IS NOT NULL THEN 
                (api_usage.response_time_ms + p_response_time_ms) / 2
            ELSE api_usage.response_time_ms
        END;
        
    -- Update last_used_at for the API key
    UPDATE api_keys 
    SET last_used_at = NOW() 
    WHERE id = p_api_key_id;
END;
$$ LANGUAGE plpgsql;

-- Create function to check rate limits
CREATE OR REPLACE FUNCTION check_rate_limit(
    p_api_key_id INTEGER,
    p_tier VARCHAR(20)
) RETURNS BOOLEAN AS $$
DECLARE
    daily_usage INTEGER;
BEGIN
    -- Premium tier has no limits
    IF p_tier = 'premium' THEN
        RETURN TRUE;
    END IF;
    
    -- Check daily usage for free tier
    SELECT COALESCE(SUM(request_count), 0) INTO daily_usage
    FROM api_usage
    WHERE api_key_id = p_api_key_id 
    AND date = CURRENT_DATE;
    
    -- Free tier limit: 24 requests per day
    RETURN daily_usage < 24;
END;
$$ LANGUAGE plpgsql;