-- Migration 018: Create reconciled_records table for audit-traceable unified market data

CREATE TABLE reconciled_records (
    id SERIAL PRIMARY KEY,
    instrument_id VARCHAR(32) NOT NULL,
    as_of DATE NOT NULL,
    data_type VARCHAR(16) NOT NULL, -- e.g. 'eod', 'tick', etc.
    value JSONB NOT NULL,           -- canonical data fields (close, open, etc.)
    quality_score DOUBLE PRECISION NOT NULL,
    sources TEXT[] NOT NULL,
    rationale TEXT NOT NULL,
    provenance JSONB NOT NULL,      -- full audit/provenance log
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    UNIQUE (instrument_id, as_of, data_type)
);

-- Index for fast queries by instrument and date
CREATE INDEX reconciled_records_instrument_date_idx
    ON reconciled_records (instrument_id, as_of DESC, data_type);
