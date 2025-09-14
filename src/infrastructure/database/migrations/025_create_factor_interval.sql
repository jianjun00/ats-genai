-- Migration 022d: Create factor_interval table only

CREATE TABLE IF NOT EXISTS factor_interval (
    id SERIAL PRIMARY KEY,
    universe_state_interval_id INTEGER NOT NULL REFERENCES universe_state_interval(id) ON DELETE CASCADE,
    factor_name VARCHAR(64) NOT NULL,
    factor_value DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (universe_state_interval_id, factor_name)
);
