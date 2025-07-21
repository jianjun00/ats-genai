-- Unified schema for trading and test databases
-- Includes all tables: daily_prices, instrument_polygon, vendors, etc.

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
ALTER TABLE daily_prices ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE daily_prices ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE daily_prices ADD COLUMN IF NOT EXISTS note TEXT;

CREATE TABLE IF NOT EXISTS vendors (
    vendor_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    website TEXT,
    api_key_env_var TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT,
    exchange TEXT,
    type TEXT,
    currency TEXT,
    figi TEXT,
    isin TEXT,
    cusip TEXT,
    composite_figi TEXT,
    UNIQUE (symbol),
    UNIQUE (figi),
    UNIQUE (isin),
    UNIQUE (cusip),
    active BOOLEAN,
    list_date DATE,
    delist_date DATE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instrument_aliases (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    source TEXT,
    UNIQUE (instrument_id, alias)
);

CREATE TABLE IF NOT EXISTS instrument_metadata (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT,
    source TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_id, key, source)
);


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

CREATE TABLE IF NOT EXISTS status_code (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,    -- e.g. 'OK', 'NO_DATA'
    description TEXT
);

-- Insert default statuses
INSERT INTO status_code (code, description) VALUES
    ('OK', 'Data available and inserted'),
    ('NO_DATA', 'No data returned for this date/ticker')
ON CONFLICT (code) DO NOTHING;

ALTER TABLE daily_prices_tiingo
ADD COLUMN IF NOT EXISTS status_id INTEGER REFERENCES status_code(id) DEFAULT NULL;

-- Add all other required tables (daily_prices_polygon, stock_splits, fundamentals, etc.)
-- ... (copy from init_test_schema.sql and setup_trading_db.py)

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

CREATE TABLE IF NOT EXISTS daily_market_cap (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);

-- Add any other tables from both sources as needed
-- ...
