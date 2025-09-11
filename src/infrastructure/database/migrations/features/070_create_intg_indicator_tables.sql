-- Migration 070: Create missing INTG indicator tables
-- These tables are required for training data generation with technical indicators

-- Create instrument_interval table for INTG environment
CREATE TABLE IF NOT EXISTS intg_instrument_interval (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES intg_instruments(id) ON DELETE CASCADE,
    interval_start TIMESTAMPTZ NOT NULL,
    interval_end TIMESTAMPTZ NOT NULL,
    interval_duration VARCHAR(16) NOT NULL, -- e.g., '1m', '5m', '15m', '1h', '1d'
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_id, interval_start, interval_duration)
);

-- Create instrument_indicator_interval table for INTG environment
CREATE TABLE IF NOT EXISTS intg_instrument_indicator_interval (
    id SERIAL PRIMARY KEY,
    instrument_interval_id INTEGER NOT NULL REFERENCES intg_instrument_interval(id) ON DELETE CASCADE,
    indicator_name VARCHAR(64) NOT NULL,
    indicator_value DOUBLE PRECISION,
    indicator_status VARCHAR(16),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_interval_id, indicator_name)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_intg_instrument_interval_instrument_time 
    ON intg_instrument_interval (instrument_id, interval_start);
    
CREATE INDEX IF NOT EXISTS idx_intg_instrument_interval_duration 
    ON intg_instrument_interval (interval_duration);
    
CREATE INDEX IF NOT EXISTS idx_intg_instrument_indicator_interval_name 
    ON intg_instrument_indicator_interval (indicator_name);
    
CREATE INDEX IF NOT EXISTS idx_intg_instrument_indicator_interval_value 
    ON intg_instrument_indicator_interval (indicator_value);

-- Set version
INSERT INTO intg_db_version (version, description, applied_at) VALUES (70, 'Create INTG indicator tables for training data generation', NOW()) 
ON CONFLICT (version) DO NOTHING;