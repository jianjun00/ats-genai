-- Migration 002: Create market data tables from intg schema
-- Generated from current intg database schema on 2025-09-15

-- Update db_version
INSERT INTO db_version (version, description) VALUES 
(2, 'Market data tables - daily prices, market cap, fundamentals')
ON CONFLICT (version) DO NOTHING;

-- Daily price tables by vendor
CREATE TABLE IF NOT EXISTS daily_price_tiingo (
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

CREATE TABLE IF NOT EXISTS daily_price_polygon (
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

CREATE TABLE IF NOT EXISTS daily_price_eodhd (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjusted_close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);

-- Daily market cap table (based on intg_daily_market_cap schema)
CREATE TABLE IF NOT EXISTS daily_market_cap (
    date DATE NOT NULL,
    symbol TEXT,
    market_cap DOUBLE PRECISION,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument_id, date)
);

-- Fundamentals table
CREATE TABLE IF NOT EXISTS fundamental (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    period TEXT, -- 'Q1', 'Q2', 'Q3', 'Q4', 'annual'
    fiscal_year INTEGER,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(instrument_id, date, metric_name, period)
);

-- Corporate actions - dividends
CREATE TABLE IF NOT EXISTS dividend (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    ex_date DATE NOT NULL,
    pay_date DATE,
    amount DOUBLE PRECISION NOT NULL,
    frequency TEXT,
    type TEXT, -- 'regular', 'special', 'stock'
    currency TEXT DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(instrument_id, ex_date, amount)
);

-- Corporate actions - stock splits
CREATE TABLE IF NOT EXISTS stock_splits (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    ex_date DATE NOT NULL,
    split_ratio DOUBLE PRECISION NOT NULL, -- e.g., 2.0 for 2:1 split
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(instrument_id, ex_date, split_ratio)
);

-- Vendor-specific dividend tables
CREATE TABLE IF NOT EXISTS dividend_polygon (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ex_date DATE NOT NULL,
    pay_date DATE,
    amount DOUBLE PRECISION NOT NULL,
    frequency TEXT,
    raw JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, ex_date, amount)
);

CREATE TABLE IF NOT EXISTS stock_splits_polygon (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ex_date DATE NOT NULL,
    split_ratio DOUBLE PRECISION NOT NULL,
    raw JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, ex_date, split_ratio)
);

-- Market data indexes for performance
CREATE INDEX IF NOT EXISTS idx_daily_price_tiingo_date ON daily_price_tiingo(date);
CREATE INDEX IF NOT EXISTS idx_daily_price_tiingo_symbol ON daily_price_tiingo(symbol);
CREATE INDEX IF NOT EXISTS idx_daily_price_polygon_date ON daily_price_polygon(date);
CREATE INDEX IF NOT EXISTS idx_daily_price_polygon_symbol ON daily_price_polygon(symbol);
CREATE INDEX IF NOT EXISTS idx_daily_price_eodhd_date ON daily_price_eodhd(date);
CREATE INDEX IF NOT EXISTS idx_daily_price_eodhd_symbol ON daily_price_eodhd(symbol);
CREATE INDEX IF NOT EXISTS idx_daily_market_cap_date ON daily_market_cap(date);
CREATE INDEX IF NOT EXISTS idx_daily_market_cap_symbol ON daily_market_cap(symbol);
CREATE INDEX IF NOT EXISTS idx_fundamental_date ON fundamental(date);
CREATE INDEX IF NOT EXISTS idx_fundamental_instrument_metric ON fundamental(instrument_id, metric_name);
CREATE INDEX IF NOT EXISTS idx_dividend_ex_date ON dividend(ex_date);
CREATE INDEX IF NOT EXISTS idx_stock_splits_ex_date ON stock_splits(ex_date);