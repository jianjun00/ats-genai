-- Migration 022c: Create instrument_indicator_interval table only

CREATE TABLE IF NOT EXISTS instrument_indicator_interval (
    id SERIAL PRIMARY KEY,
    instrument_interval_id INTEGER NOT NULL REFERENCES instrument_interval(id) ON DELETE CASCADE,
    indicator_name VARCHAR(64) NOT NULL,
    indicator_value DOUBLE PRECISION,
    indicator_status VARCHAR(16),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (instrument_interval_id, indicator_name)
);

-- Migration 024: Remove legacy interval_blob column from universe_state_interval
-- Context: Older schemas stored a serialized protobuf in interval_blob BYTEA NOT NULL.
-- The project has migrated to a normalized schema (021/022+), so this column is obsolete
-- and causes NOT NULL violations when inserting rows without the blob.

-- Make operation idempotent: only drop if the column exists.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'universe_state_interval' AND column_name = 'interval_blob'
    ) THEN
        -- If any constraint enforces NOT NULL, dropping the column removes it implicitly.
        ALTER TABLE universe_state_interval DROP COLUMN interval_blob;
    END IF;
END $$;
