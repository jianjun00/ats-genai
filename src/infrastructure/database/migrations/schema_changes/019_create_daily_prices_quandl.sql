-- 019_create_daily_prices_quandl.sql
-- Migration to create the daily_prices_quandl table for integration and production environments

CREATE TABLE IF NOT EXISTS daily_prices_quandl (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_prices_quandl_symbol_date
    ON daily_prices_quandl (symbol, date);

