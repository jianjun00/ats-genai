-- Migration 002: Add stock splits tracking table
-- This migration adds support for tracking stock splits and dividends

CREATE TABLE IF NOT EXISTS stock_splits (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ex_date DATE NOT NULL,
    split_ratio DOUBLE PRECISION NOT NULL,
    split_from INTEGER NOT NULL,
    split_to INTEGER NOT NULL,
    source TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_splits_symbol_date ON stock_splits(symbol, ex_date);

-- Add dividends table
CREATE TABLE IF NOT EXISTS dividends (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ex_date DATE NOT NULL,
    pay_date DATE,
    record_date DATE,
    amount DOUBLE PRECISION NOT NULL,
    currency TEXT DEFAULT 'USD',
    dividend_type TEXT DEFAULT 'regular',
    source TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, ex_date, dividend_type)
);

CREATE INDEX IF NOT EXISTS idx_dividends_symbol_date ON dividends(symbol, ex_date);
