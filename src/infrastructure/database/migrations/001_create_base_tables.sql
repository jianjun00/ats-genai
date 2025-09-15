-- Migration 001: Create base tables from intg schema
-- Generated from current intg database schema on 2025-09-15

-- Create db_version table for migration tracking
CREATE TABLE IF NOT EXISTS db_version (
    id SERIAL PRIMARY KEY,
    version INTEGER NOT NULL UNIQUE,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    description TEXT
);

-- Insert initial version
INSERT INTO db_version (version, description) VALUES 
(1, 'Initial schema with base tables')
ON CONFLICT (version) DO NOTHING;

-- Vendors table
CREATE TABLE IF NOT EXISTS vendors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    website TEXT,
    api_key_env_var TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Status codes table
CREATE TABLE IF NOT EXISTS status_code (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    description TEXT
);

-- Insert default status codes
INSERT INTO status_code (code, description) VALUES
    ('OK', 'Data available and inserted'),
    ('NO_DATA', 'No data returned for this date/ticker'),
    ('ERROR', 'Error occurred during data collection'),
    ('PENDING', 'Data collection pending')
ON CONFLICT (code) DO NOTHING;

-- Instruments table (based on intg_instrument schema)
CREATE TABLE IF NOT EXISTS instrument (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT,
    exchange TEXT,
    type TEXT,
    currency TEXT,
    figi TEXT UNIQUE,
    isin TEXT UNIQUE,
    cusip TEXT UNIQUE,
    composite_figi TEXT,
    active BOOLEAN,
    list_date DATE,
    delist_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sector TEXT
);

-- Instrument aliases table
CREATE TABLE IF NOT EXISTS instrument_aliases (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    source TEXT,
    UNIQUE (instrument_id, alias)
);

-- Instrument metadata table
CREATE TABLE IF NOT EXISTS instrument_metadata (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (instrument_id, key, source)
);

-- Instrument cross-references table
CREATE TABLE IF NOT EXISTS instrument_xrefs (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id) ON DELETE CASCADE,
    vendor_id INTEGER NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    type TEXT,
    start_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    end_at TIMESTAMP WITHOUT TIME ZONE,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(instrument_id, vendor_id, start_at)
);

-- Universe table
CREATE TABLE IF NOT EXISTS universe (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Universe membership table
CREATE TABLE IF NOT EXISTS universe_membership (
    id SERIAL PRIMARY KEY,
    universe_id INTEGER NOT NULL REFERENCES universe(id) ON DELETE CASCADE,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    entered_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    exited_at TIMESTAMP WITHOUT TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(universe_id, instrument_id, entered_at)
);

-- Universe membership changes table
CREATE TABLE IF NOT EXISTS universe_membership_changes (
    id SERIAL PRIMARY KEY,
    universe_id INTEGER NOT NULL REFERENCES universe(id),
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    change_type TEXT NOT NULL CHECK (change_type IN ('ENTRY', 'EXIT')),
    change_date DATE NOT NULL,
    reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Basic indexes for performance
CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name);
CREATE INDEX IF NOT EXISTS idx_instrument_symbol ON instrument(symbol);
CREATE INDEX IF NOT EXISTS idx_instrument_active ON instrument(active);
CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_symbol_vendor ON instrument_xrefs(symbol, vendor_id);
CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_active ON instrument_xrefs(active);
CREATE INDEX IF NOT EXISTS idx_universe_membership_active ON universe_membership(is_active);
CREATE INDEX IF NOT EXISTS idx_universe_membership_changes_date ON universe_membership_changes(change_date);

-- Insert basic vendor data
INSERT INTO vendors (name, description) VALUES 
    ('polygon', 'Polygon.io financial data provider'),
    ('tiingo', 'Tiingo financial data provider'),
    ('eodhd', 'EODHD financial data provider'),
    ('ticker', 'Generic ticker symbol vendor')
ON CONFLICT (name) DO NOTHING;

-- Insert basic universe
INSERT INTO universe (name, description) VALUES 
    ('default', 'Default universe for all instruments'),
    ('sp500', 'S&P 500 universe'),
    ('nasdaq100', 'NASDAQ 100 universe')
ON CONFLICT (name) DO NOTHING;