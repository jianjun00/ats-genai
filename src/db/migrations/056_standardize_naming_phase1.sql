-- Database Naming Standardization - Phase 1 (Low Risk)
-- Migration: 056_standardize_naming_phase1.sql
-- Created: 2025-09-05
-- Purpose: Standardize critical column names while preserving all data

-- =====================================================
-- PHASE 1: CRITICAL DATA INTEGRITY FIXES
-- =====================================================

\echo 'Starting Migration 056: Database naming standardization (Phase 1)...'

BEGIN;

-- Create migration log table if it doesn't exist
CREATE TABLE IF NOT EXISTS dev_migration_log (
    id SERIAL PRIMARY KEY,
    migration_name TEXT NOT NULL,
    phase TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'in_progress',
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    rollback_info JSONB,
    notes TEXT
);

-- Log start of migration
INSERT INTO dev_migration_log (migration_name, phase, notes) 
VALUES ('056_standardize_naming', 'phase1', 'Critical naming standardization - low risk changes');

-- =====================================================
-- 1.1 STANDARDIZE ADJUSTED CLOSE COLUMN NAMING
-- =====================================================

\echo 'Step 1.1: Creating backup tables...'

-- Create backup tables for safety
CREATE TABLE dev_daily_prices_backup_20250905 AS 
SELECT * FROM dev_daily_prices;

CREATE TABLE dev_daily_prices_tiingo_backup_20250905 AS 
SELECT * FROM dev_daily_prices_tiingo;

\echo 'Step 1.1: Renaming adjusted close columns...'

-- Rename adjclose to adjusted_close in tiingo table
ALTER TABLE dev_daily_prices_tiingo 
RENAME COLUMN adjclose TO adjusted_close;

-- Rename adjusted_price to adjusted_close in main table for consistency
ALTER TABLE dev_daily_prices 
RENAME COLUMN adjusted_price TO adjusted_close;

\echo 'Step 1.1: Adjusted close columns standardized ✅'

-- =====================================================
-- 1.2 FIX CRITICAL FOREIGN KEY DATA TYPE MISMATCHES
-- =====================================================

\echo 'Step 1.2: Fixing instrument_id data types...'

-- Create backup of reconciled records table
CREATE TABLE dev_reconciled_records_backup_20250905 AS 
SELECT * FROM dev_reconciled_records;

-- Check if instrument_id column needs fixing (text -> integer conversion)
DO $$
BEGIN
    -- Only attempt conversion if column is text type
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'dev_reconciled_records' 
        AND column_name = 'instrument_id' 
        AND data_type IN ('text', 'character varying')
    ) THEN
        -- First, ensure all values are valid integers or NULL
        UPDATE dev_reconciled_records 
        SET instrument_id = NULL 
        WHERE instrument_id IS NOT NULL 
        AND instrument_id !~ '^[0-9]+$';
        
        -- Convert to integer type
        ALTER TABLE dev_reconciled_records 
        ALTER COLUMN instrument_id TYPE integer USING instrument_id::integer;
        
        -- Add foreign key constraint if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE constraint_name = 'dev_reconciled_records_instrument_id_fkey'
        ) THEN
            ALTER TABLE dev_reconciled_records 
            ADD CONSTRAINT dev_reconciled_records_instrument_id_fkey 
            FOREIGN KEY (instrument_id) REFERENCES dev_instruments(id);
        END IF;
        
        RAISE NOTICE 'Fixed instrument_id data type in dev_reconciled_records';
    ELSE
        RAISE NOTICE 'instrument_id already correct type in dev_reconciled_records';
    END IF;
END
$$;

\echo 'Step 1.2: Foreign key data types fixed ✅'

-- =====================================================
-- 1.3 STANDARDIZE TIMESTAMP COLUMNS
-- =====================================================

\echo 'Step 1.3: Standardizing timestamp columns to include timezone...'

-- Function to safely convert timestamp columns to timestamp with time zone
CREATE OR REPLACE FUNCTION standardize_timestamp_columns()
RETURNS void AS $$
DECLARE
    rec RECORD;
    sql_cmd TEXT;
BEGIN
    -- Find all timestamp without time zone columns that should be with time zone
    FOR rec IN 
        SELECT table_name, column_name
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        AND data_type = 'timestamp without time zone'
        AND column_name IN ('created_at', 'updated_at', 'timestamp', 'last_updated')
        AND table_name LIKE 'dev_%'
        ORDER BY table_name, column_name
    LOOP
        sql_cmd := format('ALTER TABLE %I ALTER COLUMN %I TYPE timestamp with time zone', 
                         rec.table_name, rec.column_name);
        
        RAISE NOTICE 'Executing: %', sql_cmd;
        EXECUTE sql_cmd;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Execute timestamp standardization
SELECT standardize_timestamp_columns();

-- Drop the temporary function
DROP FUNCTION standardize_timestamp_columns();

\echo 'Step 1.3: Timestamp columns standardized ✅'

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

\echo 'Running validation queries...'

-- Validate adjusted_close column rename
SELECT 
    'dev_daily_prices' as table_name,
    COUNT(*) as row_count,
    COUNT(adjusted_close) as adjusted_close_count
FROM dev_daily_prices
UNION ALL
SELECT 
    'dev_daily_prices_tiingo' as table_name,
    COUNT(*) as row_count,
    COUNT(adjusted_close) as adjusted_close_count
FROM dev_daily_prices_tiingo;

-- Validate instrument_id foreign key
SELECT 
    COUNT(*) as total_reconciled_records,
    COUNT(r.instrument_id) as with_instrument_id,
    COUNT(i.id) as valid_instrument_refs
FROM dev_reconciled_records r
LEFT JOIN dev_instruments i ON r.instrument_id = i.id;

-- Store rollback information
UPDATE dev_migration_log 
SET rollback_info = jsonb_build_object(
    'backup_tables', jsonb_build_array(
        'dev_daily_prices_backup_20250905',
        'dev_daily_prices_tiingo_backup_20250905', 
        'dev_reconciled_records_backup_20250905'
    ),
    'column_changes', jsonb_build_array(
        jsonb_build_object('table', 'dev_daily_prices', 'column', 'adjusted_price', 'renamed_to', 'adjusted_close'),
        jsonb_build_object('table', 'dev_daily_prices_tiingo', 'column', 'adjclose', 'renamed_to', 'adjusted_close'),
        jsonb_build_object('table', 'dev_reconciled_records', 'column', 'instrument_id', 'type_change', 'text->integer')
    )
)
WHERE migration_name = '056_standardize_naming' AND phase = 'phase1';

-- Mark migration as completed
UPDATE dev_migration_log 
SET status = 'completed', completed_at = NOW()
WHERE migration_name = '056_standardize_naming' AND phase = 'phase1';

\echo 'Migration 056 Phase 1 completed successfully! ✅'
\echo 'Backup tables created for rollback if needed:'
\echo '  - dev_daily_prices_backup_20250905'
\echo '  - dev_daily_prices_tiingo_backup_20250905' 
\echo '  - dev_reconciled_records_backup_20250905'

COMMIT;

-- =====================================================
-- POST-MIGRATION VERIFICATION
-- =====================================================

\echo 'Running post-migration verification...'

-- Check that all expected columns exist with correct names
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_name IN ('dev_daily_prices', 'dev_daily_prices_tiingo', 'dev_reconciled_records')
AND column_name IN ('adjusted_close', 'instrument_id', 'created_at', 'updated_at')
ORDER BY table_name, column_name;

\echo 'Migration 056 verification completed ✅'
\echo ''
\echo '⚠️  IMPORTANT: Test your applications to ensure they work with renamed columns'
\echo '⚠️  Next: Review Phase 2 migration script (057) before proceeding'