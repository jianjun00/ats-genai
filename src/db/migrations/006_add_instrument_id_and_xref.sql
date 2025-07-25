-- Migration 006: Add instrument_xref and instrument_id columns to core tables

-- 1. Create instrument_xref table
CREATE TABLE IF NOT EXISTS instrument_xrefs (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id),
    vendor_id INTEGER NOT NULL REFERENCES vendors(vendor_id),
    symbol TEXT NOT NULL,
    type TEXT,
    start_at DATE NOT NULL,
    end_at DATE,
    UNIQUE (instrument_id, vendor_id, symbol, start_at)
);
CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_symbol_vendor ON instrument_xrefs(symbol, vendor_id);

-- 2. Add instrument_id columns to all tables using symbol
ALTER TABLE daily_prices ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE daily_prices_tiingo ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE daily_prices_polygon ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE stock_splits ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE dividends ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE universe_membership ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE universe_membership_changes ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS instrument_id INTEGER REFERENCES instruments(id);

-- 3. (Optional) Add indexes for instrument_id usage
CREATE INDEX IF NOT EXISTS idx_daily_prices_instrument_id ON daily_prices(instrument_id);
CREATE INDEX IF NOT EXISTS idx_daily_prices_tiingo_instrument_id ON daily_prices_tiingo(instrument_id);
CREATE INDEX IF NOT EXISTS idx_daily_prices_polygon_instrument_id ON daily_prices_polygon(instrument_id);
CREATE INDEX IF NOT EXISTS idx_stock_splits_instrument_id ON stock_splits(instrument_id);
CREATE INDEX IF NOT EXISTS idx_dividends_instrument_id ON dividends(instrument_id);
CREATE INDEX IF NOT EXISTS idx_universe_membership_instrument_id ON universe_membership(instrument_id);
CREATE INDEX IF NOT EXISTS idx_universe_membership_changes_instrument_id ON universe_membership_changes(instrument_id);
CREATE INDEX IF NOT EXISTS idx_fundamentals_instrument_id ON fundamentals(instrument_id);
