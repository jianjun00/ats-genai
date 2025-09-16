-- Migration 005: Create ML and training data tables from intg schema
-- Generated from current intg database schema on 2025-09-15

-- Update db_version
INSERT INTO db_version (version, description) VALUES 
(5, 'ML and training data tables - datasets, model tracking')
ON CONFLICT (version) DO NOTHING;

-- Training datasets table
CREATE TABLE IF NOT EXISTS training_dataset (
    id SERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    symbols TEXT[],
    date_range_start DATE,
    date_range_end DATE,
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    total_sequences INTEGER,
    feature_count INTEGER,
    status TEXT DEFAULT 'created', -- 'created', 'training', 'completed', 'failed'
    data_quality_score DOUBLE PRECISION,
    file_path TEXT,
    file_size_mb DOUBLE PRECISION,
    config JSONB,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Monthly training data tracking table
CREATE TABLE IF NOT EXISTS monthly_training_data (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER REFERENCES instrument(id) ON DELETE SET NULL,
    symbol TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_sequences INTEGER DEFAULT 0,
    data_quality_score DOUBLE PRECISION,
    file_path TEXT,
    file_size_mb DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, year, month)
);

-- Model comparisons table for tracking different model configurations
CREATE TABLE IF NOT EXISTS model_comparisons (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_config JSONB,
    training_dataset_id INTEGER REFERENCES training_dataset(id),
    validation_score DOUBLE PRECISION,
    test_score DOUBLE PRECISION,
    training_time_seconds INTEGER,
    model_size_mb DOUBLE PRECISION,
    feature_importance JSONB,
    hyperparameters JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Support resistance events and levels
CREATE TABLE IF NOT EXISTS sr_levels (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    level_type TEXT NOT NULL CHECK (level_type IN ('support', 'resistance')),
    price_level DOUBLE PRECISION NOT NULL,
    strength DOUBLE PRECISION DEFAULT 1.0,
    first_touch DATE,
    last_touch DATE,
    touch_count INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sr_events (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    event_date DATE NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('bounce', 'break', 'test')),
    level_id INTEGER REFERENCES sr_levels(id),
    price DOUBLE PRECISION NOT NULL,
    volume BIGINT,
    significance DOUBLE PRECISION DEFAULT 1.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sr_tests (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    test_date DATE NOT NULL,
    level_type TEXT NOT NULL,
    expected_level DOUBLE PRECISION,
    actual_price DOUBLE PRECISION,
    test_result TEXT CHECK (test_result IN ('hold', 'break')),
    confidence DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Gap events table
CREATE TABLE IF NOT EXISTS gap_events (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    gap_date DATE NOT NULL,
    gap_type TEXT NOT NULL CHECK (gap_type IN ('up', 'down', 'common', 'breakaway', 'runaway', 'exhaustion')),
    previous_close DOUBLE PRECISION NOT NULL,
    current_open DOUBLE PRECISION NOT NULL,
    gap_size DOUBLE PRECISION NOT NULL,
    gap_percent DOUBLE PRECISION NOT NULL,
    volume BIGINT,
    filled_date DATE,
    is_filled BOOLEAN DEFAULT FALSE,
    significance DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, gap_date)
);

-- Market regimes table for different market conditions
CREATE TABLE IF NOT EXISTS market_regimes (
    id SERIAL PRIMARY KEY,
    regime_date DATE NOT NULL,
    regime_type TEXT NOT NULL, -- 'bull', 'bear', 'sideways', 'volatile', 'low_vol'
    confidence DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    trend_strength DOUBLE PRECISION,
    market_breadth DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(regime_date)
);

-- Symbol performance tracking
CREATE TABLE IF NOT EXISTS symbol_performance (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    daily_return DOUBLE PRECISION,
    volatility_5d DOUBLE PRECISION,
    volatility_20d DOUBLE PRECISION,
    rsi DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION, -- Current volume / 20-day avg volume
    relative_strength DOUBLE PRECISION, -- vs SPY
    sector_relative_strength DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, date)
);

-- User preferences and dashboard configurations
CREATE TABLE IF NOT EXISTS user_preferences (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    preference_key TEXT NOT NULL,
    preference_value JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_id, preference_key)
);

CREATE TABLE IF NOT EXISTS dashboard_configs (
    id SERIAL PRIMARY KEY,
    config_name TEXT NOT NULL UNIQUE,
    config_type TEXT NOT NULL, -- 'trading', 'analytics', 'risk'
    layout JSONB,
    widgets JSONB,
    filters JSONB,
    is_default BOOLEAN DEFAULT FALSE,
    created_by TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ML and training indexes
CREATE INDEX IF NOT EXISTS idx_training_dataset_name ON training_dataset(dataset_name);
CREATE INDEX IF NOT EXISTS idx_training_dataset_status ON training_dataset(status);
CREATE INDEX IF NOT EXISTS idx_training_dataset_date_range ON training_dataset(date_range_start, date_range_end);
CREATE INDEX IF NOT EXISTS idx_monthly_training_data_symbol_date ON monthly_training_data(symbol, year, month);
CREATE INDEX IF NOT EXISTS idx_model_comparisons_run_id ON model_comparisons(run_id);
CREATE INDEX IF NOT EXISTS idx_model_comparisons_model_name ON model_comparisons(model_name);
CREATE INDEX IF NOT EXISTS idx_sr_levels_symbol ON sr_levels(symbol);
CREATE INDEX IF NOT EXISTS idx_sr_levels_price ON sr_levels(price_level);
CREATE INDEX IF NOT EXISTS idx_sr_levels_active ON sr_levels(is_active);
CREATE INDEX IF NOT EXISTS idx_sr_events_symbol_date ON sr_events(symbol, event_date);
CREATE INDEX IF NOT EXISTS idx_gap_events_symbol_date ON gap_events(symbol, gap_date);
CREATE INDEX IF NOT EXISTS idx_gap_events_filled ON gap_events(is_filled);
CREATE INDEX IF NOT EXISTS idx_market_regimes_date ON market_regimes(regime_date);
CREATE INDEX IF NOT EXISTS idx_symbol_performance_symbol_date ON symbol_performance(symbol, date);
CREATE INDEX IF NOT EXISTS idx_user_preferences_user_id ON user_preferences(user_id);
CREATE INDEX IF NOT EXISTS idx_dashboard_configs_type ON dashboard_configs(config_type);

-- Insert default dashboard configurations
INSERT INTO dashboard_configs (config_name, config_type, layout, widgets, is_default, created_by) VALUES 
    ('default_trading', 'trading', '{"columns": 3, "rows": 4}', '["price_chart", "volume", "indicators", "news"]', TRUE, 'system'),
    ('default_analytics', 'analytics', '{"columns": 2, "rows": 3}', '["performance", "risk_metrics", "attribution"]', TRUE, 'system'),
    ('default_risk', 'risk', '{"columns": 2, "rows": 2}', '["var", "stress_test", "correlation", "exposure"]', TRUE, 'system')
ON CONFLICT (config_name) DO NOTHING;