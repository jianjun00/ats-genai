-- Migration 001: Initial schema from existing schema.sql
-- This migration creates all the base tables for the trading system


-- Daily prices table
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    adjusted_price DOUBLE PRECISION,
    source TEXT,
    status TEXT,
    note TEXT,
    PRIMARY KEY (date, symbol)
);

-- Vendors table
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    website TEXT,
    api_key_env_var TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Instruments table
CREATE TABLE IF NOT EXISTS instruments (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT,
    exchange TEXT,
    type TEXT,
    currency TEXT,
    figi TEXT,
    isin TEXT,
    cusip TEXT,
    composite_figi TEXT,
    active BOOLEAN,
    list_date DATE,
    delist_date DATE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol),
    UNIQUE (figi),
    UNIQUE (isin),
    UNIQUE (cusip)
);

-- Instrument aliases
CREATE TABLE IF NOT EXISTS instrument_aliases (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    source TEXT,
    UNIQUE (instrument_id, alias)
);

-- Instrument metadata
CREATE TABLE IF NOT EXISTS instrument_metadata (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT,
    source TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_id, key, source)
);

-- Polygon-specific instrument data
CREATE TABLE IF NOT EXISTS instrument_polygon (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    exchange TEXT,
    type TEXT,
    currency TEXT,
    figi TEXT,
    isin TEXT,
    cusip TEXT,
    composite_figi TEXT,
    active BOOLEAN,
    list_date DATE,
    delist_date DATE,
    raw JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Status codes for data quality tracking
CREATE TABLE IF NOT EXISTS status_code (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    description TEXT
);
ALTER TABLE status_code DROP CONSTRAINT IF EXISTS status_code_code_key;
ALTER TABLE status_code ADD CONSTRAINT status_code_code_key UNIQUE (code);

-- Insert default statuses
INSERT INTO status_code (code, description) VALUES
    ('OK', 'Data available and inserted'),
    ('NO_DATA', 'No data returned for this date/ticker')
ON CONFLICT (code) DO NOTHING;

-- Tiingo daily prices
CREATE TABLE IF NOT EXISTS daily_prices_tiingo (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjClose DOUBLE PRECISION,
    volume BIGINT,
    status_id INTEGER REFERENCES status_code(id) DEFAULT NULL,
    PRIMARY KEY (date, symbol)
);

-- Polygon daily prices
CREATE TABLE IF NOT EXISTS daily_prices_polygon (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);

-- Daily market cap
CREATE TABLE IF NOT EXISTS daily_market_cap (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);

-- Universe definitions
CREATE TABLE IF NOT EXISTS universe (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT
);

-- Universe membership tracking
CREATE TABLE IF NOT EXISTS universe_membership (
    universe_id INTEGER NOT NULL REFERENCES universe(id),
    symbol TEXT NOT NULL,
    start_at DATE NOT NULL,
    end_at DATE,
    PRIMARY KEY (universe_id, symbol, start_at)
);

-- Insert default universe
INSERT INTO universe (id, name, description)
VALUES (1, 'default', 'Default universe for daily screening')
ON CONFLICT (id) DO NOTHING;

-- Fundamentals data
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);
