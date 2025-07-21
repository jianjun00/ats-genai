-- Schema for all required tables for market-forecast-app tests

CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    symbol TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    reported_time TIMESTAMPTZ,
    source TEXT,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_symbol_time ON events(symbol, event_time);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, event_time);

-- Unified daily_prices table: merges Polygon & Tiingo, tracks source, status, and notes
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    adjusted_price DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
-- Add new columns if they do not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='daily_prices' AND column_name='source') THEN
        ALTER TABLE daily_prices ADD COLUMN source TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='daily_prices' AND column_name='status') THEN
        ALTER TABLE daily_prices ADD COLUMN status TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='daily_prices' AND column_name='note') THEN
        ALTER TABLE daily_prices ADD COLUMN note TEXT;
    END IF;
END$$;

-- Vendor table for all data providers (e.g., Tiingo, Polygon, Quandl, Norgate)
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    website TEXT,
    api_key_env_var TEXT, -- e.g. 'TIINGO_API_KEY'
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Insert initial vendors
INSERT INTO vendors (name, description, website, api_key_env_var)
VALUES
    ('Tiingo', 'US equities, FX, and crypto market data', 'https://www.tiingo.com/', 'TIINGO_API_KEY'),
    ('Polygon', 'Real-time and historical market data APIs', 'https://polygon.io/', 'POLYGON_API_KEY'),
    ('Quandl', 'Alternative and core financial data', 'https://www.quandl.com/', 'QUANDL_API_KEY'),
    ('Norgate', 'Premium end-of-day stock market data', 'https://norgatedata.com/', NULL)
ON CONFLICT (name) DO NOTHING;


CREATE TABLE IF NOT EXISTS daily_prices_tiingo (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjClose DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS stock_splits (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    split_date DATE NOT NULL,
    numerator DOUBLE PRECISION NOT NULL,
    denominator DOUBLE PRECISION NOT NULL,
    split_ratio DOUBLE PRECISION GENERATED ALWAYS AS (numerator/denominator) STORED
);

CREATE TABLE IF NOT EXISTS dividends (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ex_date DATE NOT NULL,
    amount DOUBLE PRECISION NOT NULL
);

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

CREATE TABLE IF NOT EXISTS daily_prices_norgate (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS daily_prices_quandl (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);


CREATE TABLE IF NOT EXISTS spy_membership (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    effective_date DATE NOT NULL,
    removal_date DATE
);
CREATE INDEX IF NOT EXISTS idx_spy_membership_symbol ON spy_membership(symbol);
CREATE INDEX IF NOT EXISTS idx_spy_membership_effective ON spy_membership(effective_date);

CREATE TABLE IF NOT EXISTS spy_membership_change (
    id SERIAL PRIMARY KEY,
    change_date DATE NOT NULL,
    added TEXT,
    removed TEXT
);
