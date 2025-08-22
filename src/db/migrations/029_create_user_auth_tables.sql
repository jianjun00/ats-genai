-- Migration 029: Create User Authentication Tables (Google OAuth2)
-- Created: 2025-01-16
-- Purpose: Add user accounts, sessions, and user-based API keys for Portfolio GPT MVP

-- Drop existing tables if they exist (for clean reruns)
DROP TABLE IF EXISTS user_preferences CASCADE;
DROP TABLE IF EXISTS api_usage CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;
DROP TABLE IF EXISTS user_sessions CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Users table for Google OAuth2 authentication
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    google_id VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    given_name VARCHAR(255),
    family_name VARCHAR(255),
    picture_url TEXT,
    subscription_tier VARCHAR(20) DEFAULT 'free' CHECK (subscription_tier IN ('free', 'premium')),
    subscription_status VARCHAR(20) DEFAULT 'active' CHECK (subscription_status IN ('active', 'inactive', 'cancelled')),
    stripe_customer_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_login_at TIMESTAMP,
    email_verified BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true
);

-- Indexes for users table
CREATE INDEX idx_users_google_id ON users(google_id);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_subscription_tier ON users(subscription_tier);

-- User sessions for dashboard authentication
CREATE TABLE user_sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    session_token VARCHAR(255) UNIQUE NOT NULL,
    refresh_token VARCHAR(255),
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP DEFAULT NOW(),
    user_agent TEXT,
    ip_address INET,
    is_active BOOLEAN DEFAULT true
);

-- Indexes for sessions table
CREATE INDEX idx_user_sessions_token ON user_sessions(session_token);
CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_expires_at ON user_sessions(expires_at);

-- Updated API Keys table (user-based instead of standalone)
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    key_hash VARCHAR(255) UNIQUE NOT NULL,
    key_prefix VARCHAR(8) NOT NULL,
    name VARCHAR(100) NOT NULL, -- User-provided name for the key
    description TEXT,
    permissions JSONB DEFAULT '{"recommendations": true, "usage": true}',
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'revoked')),
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP, -- Optional expiration
    usage_count INTEGER DEFAULT 0
);

-- Indexes for API keys table
CREATE INDEX idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);

-- Updated API Usage table (user-based tracking)
CREATE TABLE api_usage (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL DEFAULT 'GET',
    status_code INTEGER NOT NULL,
    request_count INTEGER DEFAULT 1,
    date DATE DEFAULT CURRENT_DATE,
    hour INTEGER DEFAULT EXTRACT(hour FROM NOW()),
    response_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Composite index for efficient usage queries
CREATE UNIQUE INDEX idx_api_usage_unique ON api_usage(user_id, endpoint, date, hour);
CREATE INDEX idx_api_usage_user_date ON api_usage(user_id, date);
CREATE INDEX idx_api_usage_api_key ON api_usage(api_key_id);

-- User preferences and settings
CREATE TABLE user_preferences (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    watchlist JSONB DEFAULT '[]', -- Array of stock symbols
    notification_settings JSONB DEFAULT '{"email": true, "push": false}',
    dashboard_layout JSONB DEFAULT '{"view": "grid", "refresh_interval": 3600}',
    timezone VARCHAR(50) DEFAULT 'UTC',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for user preferences
CREATE UNIQUE INDEX idx_user_preferences_user_id ON user_preferences(user_id);

-- Create updated functions for user-based usage tracking
CREATE OR REPLACE FUNCTION update_user_api_usage(
    p_user_id INTEGER,
    p_api_key_id INTEGER,
    p_endpoint VARCHAR(255),
    p_method VARCHAR(10),
    p_status_code INTEGER,
    p_response_time_ms INTEGER DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO api_usage (
        user_id,
        api_key_id,
        endpoint, 
        method,
        status_code,
        response_time_ms,
        date,
        hour
    ) VALUES (
        p_user_id,
        p_api_key_id,
        p_endpoint,
        p_method,
        p_status_code,
        p_response_time_ms,
        CURRENT_DATE,
        EXTRACT(hour FROM NOW())
    )
    ON CONFLICT (user_id, endpoint, date, hour) 
    DO UPDATE SET 
        request_count = api_usage.request_count + 1,
        updated_at = NOW(),
        response_time_ms = CASE 
            WHEN p_response_time_ms IS NOT NULL THEN 
                (api_usage.response_time_ms + p_response_time_ms) / 2
            ELSE api_usage.response_time_ms
        END;
        
    -- Update last_used_at for the API key if provided
    IF p_api_key_id IS NOT NULL THEN
        UPDATE api_keys 
        SET last_used_at = NOW(), usage_count = usage_count + 1
        WHERE id = p_api_key_id;
    END IF;
    
    -- Update user's last activity
    UPDATE users 
    SET last_login_at = NOW() 
    WHERE id = p_user_id;
END;
$$ LANGUAGE plpgsql;

-- Create function to check user rate limits
CREATE OR REPLACE FUNCTION check_user_rate_limit(
    p_user_id INTEGER,
    p_subscription_tier VARCHAR(20)
) RETURNS BOOLEAN AS $$
DECLARE
    daily_usage INTEGER;
BEGIN
    -- Premium tier has no limits
    IF p_subscription_tier = 'premium' THEN
        RETURN TRUE;
    END IF;
    
    -- Check daily usage for free tier
    SELECT COALESCE(SUM(request_count), 0) INTO daily_usage
    FROM api_usage
    WHERE user_id = p_user_id 
    AND date = CURRENT_DATE;
    
    -- Free tier limit: 24 requests per day
    RETURN daily_usage < 24;
END;
$$ LANGUAGE plpgsql;

-- Create function to get user daily usage
CREATE OR REPLACE FUNCTION get_user_daily_usage(p_user_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    daily_usage INTEGER;
BEGIN
    SELECT COALESCE(SUM(request_count), 0) INTO daily_usage
    FROM api_usage
    WHERE user_id = p_user_id 
    AND date = CURRENT_DATE;
    
    RETURN daily_usage;
END;
$$ LANGUAGE plpgsql;

-- Create view for user usage analytics (simplified to avoid prefixing conflicts)
-- Full view can be created later via application code

-- Insert a test user for development (remove in production)
INSERT INTO users (google_id, email, name, subscription_tier, email_verified) 
VALUES (
    'dev_test_user_123456789',
    'dev@portfoliogpt.com',
    'Development User',
    'premium',
    true
) ON CONFLICT (google_id) DO NOTHING;

-- Create default preferences for the test user
INSERT INTO user_preferences (user_id, watchlist)
SELECT id, '["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]'::jsonb
FROM users 
WHERE google_id = 'dev_test_user_123456789'
ON CONFLICT (user_id) DO NOTHING;