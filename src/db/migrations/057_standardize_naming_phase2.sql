-- Database Naming Standardization - Phase 2 (Medium Risk)
-- Migration: 057_standardize_naming_phase2.sql
-- Created: 2025-09-05
-- Purpose: Standardize price columns and vendor-specific naming patterns

-- =====================================================
-- PHASE 2: PRICE COLUMN AND VENDOR STANDARDIZATION
-- =====================================================

\echo 'Starting Migration 057: Database naming standardization (Phase 2)...'

BEGIN;

-- Log start of migration
INSERT INTO dev_migration_log (migration_name, phase, notes) 
VALUES ('057_standardize_naming', 'phase2', 'Price columns and vendor naming standardization - medium risk');

-- =====================================================
-- 2.1 STANDARDIZE MINUTE BAR PRICE COLUMNS
-- =====================================================

\echo 'Step 2.1: Creating backup tables for minute data...'

-- Create backups of minute bar tables
CREATE TABLE dev_one_minute_live_polygon_backup_20250905 AS 
SELECT * FROM dev_one_minute_live_polygon;

CREATE TABLE dev_one_minute_live_tiingo_backup_20250905 AS 
SELECT * FROM dev_one_minute_live_tiingo;

CREATE TABLE dev_one_minute_live_fmp_backup_20250905 AS 
SELECT * FROM dev_one_minute_live_fmp;

\echo 'Step 2.1: Standardizing minute bar price column naming...'

-- Option: Rename minute bar columns to match daily price pattern (open, high, low, close)
-- Polygon minute bars
DO $$
BEGIN
    -- Check if columns exist before renaming
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_polygon' AND column_name = 'open_price') THEN
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN open_price TO open;
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN high_price TO high;
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN low_price TO low;
        ALTER TABLE dev_one_minute_live_polygon RENAME COLUMN close_price TO close;
        RAISE NOTICE 'Renamed polygon minute bar price columns';
    END IF;
END
$$;

-- Tiingo minute bars
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_tiingo' AND column_name = 'open_price') THEN
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN open_price TO open;
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN high_price TO high;
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN low_price TO low;
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN close_price TO close;
    END IF;
    
    -- Handle adjusted close if it exists
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_tiingo' AND column_name = 'adj_close_price') THEN
        ALTER TABLE dev_one_minute_live_tiingo RENAME COLUMN adj_close_price TO adjusted_close;
    END IF;
    
    RAISE NOTICE 'Renamed tiingo minute bar price columns';
END
$$;

-- FMP minute bars
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_one_minute_live_fmp' AND column_name = 'open_price') THEN
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN open_price TO open;
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN high_price TO high;
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN low_price TO low;
        ALTER TABLE dev_one_minute_live_fmp RENAME COLUMN close_price TO close;
        RAISE NOTICE 'Renamed FMP minute bar price columns';
    END IF;
END
$$;

\echo 'Step 2.1: Minute bar price columns standardized ✅'

-- =====================================================
-- 2.2 STANDARDIZE VENDOR ID COLUMNS
-- =====================================================

\echo 'Step 2.2: Standardizing vendor-specific ID columns...'

-- Create backups
CREATE TABLE dev_news_polygon_backup_20250905 AS 
SELECT * FROM dev_news_polygon;

CREATE TABLE dev_news_tiingo_backup_20250905 AS 
SELECT * FROM dev_news_tiingo;

-- Standardize polygon news ID column
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_news_polygon' AND column_name = 'polygon_id') THEN
        ALTER TABLE dev_news_polygon RENAME COLUMN polygon_id TO vendor_id;
        RAISE NOTICE 'Renamed polygon_id to vendor_id in dev_news_polygon';
    END IF;
END
$$;

-- Standardize tiingo news ID column
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_news_tiingo' AND column_name = 'tiingo_id') THEN
        ALTER TABLE dev_news_tiingo RENAME COLUMN tiingo_id TO vendor_id;
        RAISE NOTICE 'Renamed tiingo_id to vendor_id in dev_news_tiingo';
    END IF;
END
$$;

\echo 'Step 2.2: Vendor ID columns standardized ✅'

-- =====================================================
-- 2.3 STANDARDIZE DATE COLUMN NAMING
-- =====================================================

\echo 'Step 2.3: Standardizing date column naming...'

-- Create backups for corporate action tables
CREATE TABLE dev_dividends_backup_20250905 AS 
SELECT * FROM dev_dividends;

CREATE TABLE dev_dividend_polygon_backup_20250905 AS 
SELECT * FROM dev_dividend_polygon;

CREATE TABLE dev_stock_splits_backup_20250905 AS 
SELECT * FROM dev_stock_splits;

-- Note: This step is more complex and should be carefully reviewed
-- For now, we document the inconsistencies but don't rename to avoid breaking changes

\echo 'Step 2.3: Date columns documented (manual review needed) ⚠️'

-- =====================================================
-- 2.4 STANDARDIZE POLYGON REFID COLUMNS
-- =====================================================

\echo 'Step 2.4: Documenting refid column inconsistencies...'

-- Create backups for refid tables
CREATE TABLE dev_stock_splits_polygon_backup_20250905 AS 
SELECT * FROM dev_stock_splits_polygon;

-- Note: refid columns are text vs integer - this needs careful analysis
-- We document but don't change in this phase

\echo 'Step 2.4: Refid columns documented (data analysis needed) ⚠️'

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

\echo 'Running Phase 2 validation queries...'

-- Validate minute bar column renames
SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns 
WHERE table_name IN ('dev_one_minute_live_polygon', 'dev_one_minute_live_tiingo', 'dev_one_minute_live_fmp')
AND column_name IN ('open', 'high', 'low', 'close', 'adjusted_close')
ORDER BY table_name, column_name;

-- Validate vendor ID column renames
SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns 
WHERE table_name IN ('dev_news_polygon', 'dev_news_tiingo')
AND column_name = 'vendor_id'
ORDER BY table_name;

-- Store rollback information
UPDATE dev_migration_log 
SET rollback_info = jsonb_build_object(
    'backup_tables', jsonb_build_array(
        'dev_one_minute_live_polygon_backup_20250905',
        'dev_one_minute_live_tiingo_backup_20250905',
        'dev_one_minute_live_fmp_backup_20250905',
        'dev_news_polygon_backup_20250905',
        'dev_news_tiingo_backup_20250905',
        'dev_dividends_backup_20250905',
        'dev_dividend_polygon_backup_20250905',
        'dev_stock_splits_backup_20250905',
        'dev_stock_splits_polygon_backup_20250905'
    ),
    'column_changes', jsonb_build_array(
        jsonb_build_object('table', 'dev_one_minute_live_polygon', 'columns', 'open_price->open, high_price->high, low_price->low, close_price->close'),
        jsonb_build_object('table', 'dev_one_minute_live_tiingo', 'columns', 'open_price->open, high_price->high, low_price->low, close_price->close, adj_close_price->adjusted_close'),
        jsonb_build_object('table', 'dev_one_minute_live_fmp', 'columns', 'open_price->open, high_price->high, low_price->low, close_price->close'),
        jsonb_build_object('table', 'dev_news_polygon', 'column', 'polygon_id->vendor_id'),
        jsonb_build_object('table', 'dev_news_tiingo', 'column', 'tiingo_id->vendor_id')
    )
)
WHERE migration_name = '057_standardize_naming' AND phase = 'phase2';

-- Mark migration as completed
UPDATE dev_migration_log 
SET status = 'completed', completed_at = NOW()
WHERE migration_name = '057_standardize_naming' AND phase = 'phase2';

\echo 'Migration 057 Phase 2 completed successfully! ✅'
\echo 'Backup tables created for rollback if needed'

COMMIT;

-- =====================================================
-- POST-MIGRATION VERIFICATION
-- =====================================================

\echo 'Running post-migration verification...'

-- Verify price column standardization
SELECT 
    'Minute Bar Price Columns' as check_type,
    table_name,
    COUNT(*) as column_count
FROM information_schema.columns 
WHERE table_name LIKE 'dev_one_minute_live_%'
AND column_name IN ('open', 'high', 'low', 'close', 'adjusted_close')
GROUP BY table_name
ORDER BY table_name;

-- Verify vendor ID standardization
SELECT 
    'Vendor ID Columns' as check_type,
    table_name,
    column_name,
    data_type
FROM information_schema.columns 
WHERE table_name IN ('dev_news_polygon', 'dev_news_tiingo')
AND column_name = 'vendor_id'
ORDER BY table_name;

\echo 'Migration 057 verification completed ✅'
\echo ''
\echo '⚠️  IMPORTANT: Test applications with minute bar data access'
\echo '⚠️  REVIEW: Date column standardization needs manual review'
\echo '⚠️  ANALYZE: refid vs integer ID patterns need data analysis'