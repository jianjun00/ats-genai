-- Migration: Create tables for multi-universe membership modeling

CREATE TABLE IF NOT EXISTS universe (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS universe_membership (
    id SERIAL PRIMARY KEY,
    universe_id INTEGER NOT NULL REFERENCES universe(id),
    symbol TEXT NOT NULL,
    start_at DATE NOT NULL,
    end_at DATE,
    meta JSONB,
    created_at TIMESTAMP DEFAULT now(),
    UNIQUE (universe_id, symbol, start_at)
    -- Optionally, add an exclusion constraint to prevent overlapping periods for the same (universe_id, symbol)
);

-- Example index for fast membership queries by date
CREATE INDEX IF NOT EXISTS idx_universe_membership_universe_date ON universe_membership (universe_id, start_at, end_at);
