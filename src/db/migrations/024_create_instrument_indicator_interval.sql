-- Migration 022c: Create instrument_indicator_interval table only

CREATE TABLE IF NOT EXISTS instrument_indicator_interval (
    id SERIAL PRIMARY KEY,
    instrument_interval_id INTEGER NOT NULL REFERENCES instrument_interval(id) ON DELETE CASCADE,
    indicator_name VARCHAR(64) NOT NULL,
    indicator_value DOUBLE PRECISION,
    indicator_status VARCHAR(16),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_interval_id, indicator_name)
);
