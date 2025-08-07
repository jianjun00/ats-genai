-- Migration 022b: Create instrument_interval table only

CREATE TABLE IF NOT EXISTS instrument_interval (
    id SERIAL PRIMARY KEY,
    universe_state_interval_id INTEGER NOT NULL REFERENCES universe_state_interval(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    traded_volume DOUBLE PRECISION,
    traded_dollar DOUBLE PRECISION,
    status VARCHAR(16),
    market_cap DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (universe_state_interval_id, instrument_id)
);
