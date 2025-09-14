-- Migration 021: Create universe_state_interval table for binary-proto storage of UniverseStateInterval

CREATE TABLE IF NOT EXISTS universe_state_interval (
    id SERIAL PRIMARY KEY,
    universe_id INTEGER NOT NULL,
    duration VARCHAR(16) NOT NULL,
    start_date_time TIMESTAMP NOT NULL,
    end_date_time TIMESTAMP NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (universe_id, duration, start_date_time)
);

-- Indexes for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_universe_state_interval_start_date_time ON universe_state_interval (start_date_time);
CREATE INDEX IF NOT EXISTS idx_universe_state_interval_end_date_time ON universe_state_interval (end_date_time);
