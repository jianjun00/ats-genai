-- Migration 010: Standardize Daily Price Vendor Table Schemas
-- 
-- Problem: EODHD table is missing updated_at column while Polygon and Tiingo have it
-- Solution: Add updated_at column to EODHD table for schema consistency
--
-- This ensures all vendor daily price tables have identical schema structure

-- Add updated_at column to EODHD table
ALTER TABLE intg_daily_price_eodhd 
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

-- Add updated_at column to dev environment if it exists (will be prefixed automatically)
ALTER TABLE dev_daily_price_eodhd 
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

-- Backfill existing records with created_at value
UPDATE intg_daily_price_eodhd 
SET updated_at = created_at 
WHERE updated_at IS NULL;

-- Create index for performance
CREATE INDEX IF NOT EXISTS intg_daily_price_eodhd_updated_at_idx 
ON intg_daily_price_eodhd(updated_at);

-- Verification query
-- SELECT 
--   'eodhd' as vendor,
--   COUNT(*) as total_records,
--   COUNT(updated_at) as records_with_updated_at
-- FROM intg_daily_price_eodhd;