-- Database Naming Standardization - Rollback Script
-- Migration: 058_rollback_naming_standardization.sql
-- Created: 2025-09-05
-- Purpose: Rollback migrations 056 and 057 if issues occur

-- =====================================================
-- ROLLBACK PROCEDURES FOR NAMING STANDARDIZATION
-- =====================================================

\echo 'Starting Rollback for Database Naming Standardization...'
\echo '⚠️  WARNING: This will revert column names to original state'
\echo '⚠️  Ensure applications are updated to handle old column names'

-- Check if rollback is needed
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dev_migration_log WHERE migration_name LIKE '%_standardize_naming' AND status = 'completed') THEN
        RAISE EXCEPTION 'No completed naming standardization migrations found to rollback';
    END IF;
END
$$;

BEGIN;

-- Log rollback start
INSERT INTO dev_migration_log (migration_name, phase, notes) 
VALUES ('058_rollback_naming', 'rollback', 'Rolling back naming standardization migrations 056-057');

-- =====================================================
-- ROLLBACK PHASE 2 (057) - PRICE COLUMNS AND VENDOR IDS
-- =====================================================

\echo 'Rolling back Phase 2: Price columns and vendor IDs...'

-- Rollback minute bar price columns
DO $$
BEGIN
    -- Rollback polygon minute bars (open -> open_price, etc.)
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_polygon' AND column_name = 'open') THEN
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN open TO open_price;
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN high TO high_price;
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN low TO low_price;
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN close TO close_price;
        RAISE NOTICE 'Rolled back polygon minute bar column names';
    END IF;
END
$$;

DO $$
BEGIN
    -- Rollback tiingo minute bars
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_tiingo' AND column_name = 'open') THEN
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN open TO open_price;
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN high TO high_price;
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN low TO low_price;
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN close TO close_price;
    END IF;
    
    -- Handle adjusted close rollback
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_tiingo' AND column_name = 'adjusted_close') THEN
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN adjusted_close TO adj_close_price;
    END IF;
    
    RAISE NOTICE 'Rolled back tiingo minute bar column names';
END
$$;

DO $$
BEGIN
    -- Rollback FMP minute bars
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_fmp' AND column_name = 'open') THEN
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN open TO open_price;
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN high TO high_price;
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN low TO low_price;
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN close TO close_price;
        RAISE NOTICE 'Rolled back FMP minute bar column names';
    END IF;
END
$$;

-- Rollback vendor ID columns
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_news_polygon' AND column_name = 'vendor_id') THEN
        ALTER TABLE dev_news_polygon RENAME COLUMN vendor_id TO polygon_id;
        RAISE NOTICE 'Rolled back vendor_id to polygon_id in dev_news_polygon';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_news_tiingo' AND column_name = 'vendor_id') THEN
        ALTER TABLE dev_news_tiingo RENAME COLUMN vendor_id TO tiingo_id;
        RAISE NOTICE 'Rolled back vendor_id to tiingo_id in dev_news_tiingo';
    END IF;
END
$$;

\echo 'Phase 2 rollback completed ✅'

-- =====================================================
-- ROLLBACK PHASE 1 (056) - ADJUSTED CLOSE AND TIMESTAMPS
-- =====================================================

\echo 'Rolling back Phase 1: Adjusted close columns...'

-- Rollback adjusted_close columns
ALTER TABLE dev_daily_prices 
RENAME COLUMN adjusted_close TO adjusted_price;

ALTER TABLE dev_daily_prices_tiingo 
RENAME COLUMN adjusted_close TO adjclose;

\echo 'Adjusted close columns rolled back ✅'

-- Note: Timestamp rollback and foreign key rollback are more complex
-- We document what would need to be done but don't execute automatically

\echo '⚠️  Manual actions needed for complete rollback:'
\echo '   1. Timestamp columns: Convert back to "timestamp without time zone" if needed'
\echo '   2. Foreign keys: Revert instrument_id data types if needed'
\echo '   3. Check backup tables for data recovery:'
\echo '      - dev_daily_prices_backup_20250905'
\echo '      - dev_daily_prices_tiingo_backup_20250905'
\echo '      - All minute bar backup tables'

-- =====================================================
-- VALIDATION AND CLEANUP
-- =====================================================

\echo 'Validating rollback...'

-- Verify rollback worked
SELECT 
    'Rollback Verification' as check_type,
    table_name,
    column_name
FROM information_schema.columns 
WHERE (table_name = 'dev_daily_prices' AND column_name = 'adjusted_price')
   OR (table_name = 'dev_daily_prices_tiingo' AND column_name = 'adjclose')
   OR (table_name = 'dev_news_polygon' AND column_name = 'polygon_id')
   OR (table_name = 'dev_news_tiingo' AND column_name = 'tiingo_id')
ORDER BY table_name, column_name;

-- Mark rollback as completed
UPDATE dev_migration_log 
SET status = 'completed', completed_at = NOW(),
    notes = 'Rollback completed - reverted to original column names'
WHERE migration_name = '058_rollback_naming' AND phase = 'rollback';

-- Mark original migrations as rolled back
UPDATE dev_migration_log 
SET status = 'rolled_back', notes = 'Migration rolled back by 058_rollback_naming_standardization'
WHERE migration_name IN ('056_standardize_naming', '057_standardize_naming');

\echo 'Rollback completed successfully! ✅'
\echo ''
\echo '📋 Summary of rollback actions:'
\echo '   - adjusted_close -> adjusted_price (dev_daily_prices)'
\echo '   - adjusted_close -> adjclose (dev_daily_prices_tiingo)'
\echo '   - open/high/low/close -> open_price/high_price/low_price/close_price (minute tables)'
\echo '   - vendor_id -> polygon_id/tiingo_id (news tables)'
\echo ''
\echo '⚠️  Important next steps:'
\echo '   1. Update application code to use original column names'
\echo '   2. Test all data access functionality'
\echo '   3. Review backup tables if data issues occur'

COMMIT;

-- =====================================================
-- EMERGENCY RESTORE PROCEDURES
-- =====================================================

\echo ''
\echo '🆘 EMERGENCY DATA RESTORE (if data corruption occurred):'
\echo '   -- Restore from backups (use with extreme caution):'
\echo '   -- DROP TABLE dev_daily_prices;'
\echo '   -- ALTER TABLE dev_daily_prices_backup_20250905 RENAME TO dev_daily_prices;'
\echo '   -- (Repeat for other tables as needed)'
\echo ''
\echo '✅ Rollback script execution completed'