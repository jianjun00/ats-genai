-- Migration 020: Create universe_state table for TimescaleDB persistence of universe state

CREATE TABLE IF NOT EXISTS universe_state (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id),
    as_of_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,
    exchange TEXT,
    market_cap DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    is_active BOOLEAN,
    sector TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_id, as_of_date)
);

-- TimescaleDB hypertable for as_of_date
SELECT create_hypertable('universe_state', 'as_of_date', if_not_exists => TRUE);

-- Index for fast queries by instrument and date
CREATE INDEX IF NOT EXISTS idx_universe_state_instrument_date ON universe_state (instrument_id, as_of_date DESC);
