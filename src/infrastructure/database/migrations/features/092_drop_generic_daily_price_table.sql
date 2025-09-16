-- Migration 092: Drop generic daily_price table
-- This migration removes the generic daily_price table and keeps only vendor-specific tables:
-- - daily_price_polygon
-- - daily_price_tiingo  
-- - daily_price_eodhd
--
-- The generic daily_price table is being replaced by vendor-specific tables for better data management

-- Drop the generic daily_price table and its audit table
DROP TABLE IF EXISTS daily_price CASCADE;
DROP TABLE IF EXISTS daily_price_audit CASCADE;

-- Verify that vendor-specific tables still exist
DO $$
DECLARE
    polygon_exists BOOLEAN;
    tiingo_exists BOOLEAN;
    eodhd_exists BOOLEAN;
BEGIN
    -- Check if vendor-specific tables exist
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'daily_price_polygon'
    ) INTO polygon_exists;
    
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'daily_price_tiingo'
    ) INTO tiingo_exists;
    
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'daily_price_eodhd'
    ) INTO eodhd_exists;
    
    -- Log the status
    RAISE NOTICE 'Vendor-specific table status after dropping generic daily_price:';
    RAISE NOTICE '  daily_price_polygon exists: %', polygon_exists;
    RAISE NOTICE '  daily_price_tiingo exists: %', tiingo_exists;
    RAISE NOTICE '  daily_price_eodhd exists: %', eodhd_exists;
    
    IF NOT (polygon_exists OR tiingo_exists OR eodhd_exists) THEN
        RAISE WARNING 'No vendor-specific daily_price tables found after dropping generic table';
    END IF;
END;
$$;

-- Log successful completion
SELECT 'Migration 092: Successfully dropped generic daily_price table' AS migration_status;