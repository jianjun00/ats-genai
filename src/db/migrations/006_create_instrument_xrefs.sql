-- Migration 006: Create instrument_xrefs table

CREATE TABLE IF NOT EXISTS instrument_xrefs (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    vendor_id INTEGER NOT NULL REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    type TEXT,
    start_at TIMESTAMP NOT NULL DEFAULT now(),
    end_at TIMESTAMP,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_id, vendor_id, symbol, start_at)
);

-- For environment-specific tables (integration, test, prod), the migration runner should apply prefixing automatically.
