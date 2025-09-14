-- Migration 003: Add Polygon splits and dividends tables

CREATE TABLE IF NOT EXISTS stock_splits_polygon (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    execution_date DATE NOT NULL,
    split_from INTEGER NOT NULL,
    split_to INTEGER NOT NULL,
    cash_amount DOUBLE PRECISION,
    declaration_date DATE,
    payment_date DATE,
    record_date DATE,
    description TEXT,
    refid TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, execution_date, refid)
);

CREATE INDEX IF NOT EXISTS idx_stock_splits_polygon_symbol_date ON stock_splits_polygon(symbol, execution_date);

CREATE TABLE IF NOT EXISTS dividend_polygon (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ex_dividend_date DATE NOT NULL,
    cash_amount DOUBLE PRECISION NOT NULL,
    declaration_date DATE,
    payment_date DATE,
    record_date DATE,
    description TEXT,
    refid TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, ex_dividend_date, refid)
);

CREATE INDEX IF NOT EXISTS idx_dividend_polygon_symbol_date ON dividend_polygon(symbol, ex_dividend_date);
