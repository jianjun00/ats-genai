-- Migration 083: Fix instrument_indicator_interval schema to match DAO expectations
-- 
-- Problem: The table has instrument_id + datetime but DAO expects instrument_interval_id + indicator_status
-- Solution: Add missing columns to match DAO interface

-- Add the missing columns
ALTER TABLE instrument_indicator_interval 
ADD COLUMN IF NOT EXISTS instrument_interval_id INTEGER;

ALTER TABLE instrument_indicator_interval 
ADD COLUMN IF NOT EXISTS indicator_status TEXT;

-- Remove NOT NULL constraint from instrument_id since we're transitioning to instrument_interval_id
ALTER TABLE instrument_indicator_interval 
ALTER COLUMN instrument_id DROP NOT NULL;

-- Remove NOT NULL constraint from datetime since we're transitioning to instrument_interval_id
ALTER TABLE instrument_indicator_interval 
ALTER COLUMN datetime DROP NOT NULL;

-- Add foreign key constraint to instrument_interval table
-- Note: This assumes instrument_interval table exists
ALTER TABLE instrument_indicator_interval 
ADD CONSTRAINT fk_instrument_indicator_interval_instrument_interval 
FOREIGN KEY (instrument_interval_id) REFERENCES instrument_interval(id) ON DELETE CASCADE;

-- Drop the old unique constraint
ALTER TABLE instrument_indicator_interval 
DROP CONSTRAINT IF EXISTS instrument_indicator_interval_run_id_instrument_id_datetime_ind;

-- Add new unique constraint with instrument_interval_id
ALTER TABLE instrument_indicator_interval 
ADD CONSTRAINT instrument_indicator_interval_unique 
UNIQUE(instrument_interval_id, indicator_name);

-- Update existing indexes
DROP INDEX IF EXISTS idx_instrument_indicator_interval_instrument;
CREATE INDEX IF NOT EXISTS idx_instrument_indicator_interval_instrument_interval 
ON instrument_indicator_interval(instrument_interval_id);

-- Note: We keep instrument_id and datetime for backward compatibility
-- but new code should use instrument_interval_id