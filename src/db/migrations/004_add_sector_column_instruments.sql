-- Migration 004: Add sector column to instrument_polygon and instruments tables if missing

ALTER TABLE instrument_polygon ADD COLUMN IF NOT EXISTS sector TEXT;
ALTER TABLE instruments ADD COLUMN IF NOT EXISTS sector TEXT;

-- For environment-specific tables (integration, test, prod), the migration runner should apply prefixing automatically.
