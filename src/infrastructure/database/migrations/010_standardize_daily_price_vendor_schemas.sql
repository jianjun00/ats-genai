-- Migration 010: Standardize Daily Price Vendor Table Schemas
-- 
-- Problem: EODHD table is missing updated_at column while Polygon and Tiingo have it
-- Solution: Add updated_at column to EODHD table for schema consistency
--
-- This ensures all vendor daily price tables have identical schema structure

DO $$
BEGIN
    -- Skip this migration for test environments (no vendor tables exist)
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'intg_daily_price_eodhd') 
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_daily_price_eodhd') THEN
        RAISE NOTICE 'Migration 010 skipped: No daily price vendor tables found (test environment detected)';
        RETURN;
    END IF;

    -- Add updated_at column to EODHD table (intg environment)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'intg_daily_price_eodhd') THEN
        ALTER TABLE intg_daily_price_eodhd 
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
        
        -- Backfill existing records with created_at value
        UPDATE intg_daily_price_eodhd 
        SET updated_at = created_at 
        WHERE updated_at IS NULL;
        
        -- Create index for performance
        CREATE INDEX IF NOT EXISTS intg_daily_price_eodhd_updated_at_idx 
        ON intg_daily_price_eodhd(updated_at);
        
        RAISE NOTICE 'Migration 010 applied to intg_daily_price_eodhd';
    END IF;

    -- Add updated_at column to dev environment if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_daily_price_eodhd') THEN
        ALTER TABLE dev_daily_price_eodhd 
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
        
        -- Backfill existing records with created_at value  
        UPDATE dev_daily_price_eodhd 
        SET updated_at = created_at 
        WHERE updated_at IS NULL;
        
        -- Create index for performance
        CREATE INDEX IF NOT EXISTS dev_daily_price_eodhd_updated_at_idx 
        ON dev_daily_price_eodhd(updated_at);
        
        RAISE NOTICE 'Migration 010 applied to dev_daily_price_eodhd';
    END IF;

    RAISE NOTICE 'Migration 010 completed successfully';
END $$;

-- Verification query
-- SELECT 
--   'eodhd' as vendor,
--   COUNT(*) as total_records,
--   COUNT(updated_at) as records_with_updated_at
-- FROM intg_daily_price_eodhd;