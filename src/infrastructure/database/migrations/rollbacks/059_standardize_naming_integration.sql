-- Database Naming Standardization - Integration Environment
-- Migration: 059_standardize_naming_integration.sql
-- Created: 2025-09-05
-- Purpose: Apply naming standardization to integration environment (intg_ prefixed tables)

-- =====================================================
-- INTEGRATION ENVIRONMENT NAMING STANDARDIZATION
-- =====================================================

\echo 'Starting Migration 059: Integration environment naming standardization...'

BEGIN;

-- Create migration log table if it doesn't exist
CREATE TABLE IF NOT EXISTS intg_migration_log (
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
INSERT INTO intg_migration_log (migration_name, phase, notes) 
VALUES ('059_standardize_naming_integration', 'phase1', 'Integration environment naming standardization');

-- =====================================================
-- 1.1 STANDARDIZE ADJUSTED CLOSE COLUMN NAMING
-- =====================================================

\echo 'Step 1.1: Creating backup tables for integration environment...'

-- Create backup tables for integration environment
CREATE TABLE intg_daily_price_tiingo_backup_20250905 AS 
SELECT * FROM intg_daily_price_tiingo;

\echo 'Step 1.1: Renaming adjusted close columns in integration environment...'

-- Rename adjclose to adjusted_close in integration tiingo table
ALTER TABLE intg_daily_price_tiingo 
RENAME COLUMN adjclose TO adjusted_close;

\echo 'Step 1.1: Integration adjusted close columns standardized ✅'

-- =====================================================
-- 1.2 STANDARDIZE TIMESTAMP COLUMNS (INTEGRATION)
-- =====================================================

\echo 'Step 1.2: Standardizing timestamp columns in integration environment...'

-- Function to safely convert timestamp columns to timestamp with time zone (integration)
CREATE OR REPLACE FUNCTION standardize_intg_timestamp_columns()
RETURNS void AS $$
DECLARE
    rec RECORD;
    sql_cmd TEXT;
BEGIN
    -- Find all timestamp without time zone columns in integration tables
    FOR rec IN 
        SELECT table_name, column_name
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        AND data_type = 'timestamp without time zone'
        AND column_name IN ('created_at', 'updated_at', 'timestamp', 'last_updated')
        AND table_name LIKE 'intg_%'
        ORDER BY table_name, column_name
    LOOP
        sql_cmd := format('ALTER TABLE %I ALTER COLUMN %I TYPE timestamp with time zone', 
                         rec.table_name, rec.column_name);
        
        RAISE NOTICE 'Executing: %', sql_cmd;
        EXECUTE sql_cmd;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Execute timestamp standardization for integration
SELECT standardize_intg_timestamp_columns();

-- Drop the temporary function
DROP FUNCTION standardize_intg_timestamp_columns();

\echo 'Step 1.2: Integration timestamp columns standardized ✅'

-- =====================================================
-- VALIDATION QUERIES (INTEGRATION)
-- =====================================================

\echo 'Running integration environment validation queries...'

-- Validate adjusted_close column rename in integration
SELECT 
    'intg_daily_price_tiingo' as table_name,
    COUNT(*) as row_count,
    COUNT(adjusted_close) as adjusted_close_count
FROM intg_daily_price_tiingo;

-- Store rollback information
UPDATE intg_migration_log 
SET rollback_info = jsonb_build_object(
    'backup_tables', jsonb_build_array(
        'intg_daily_price_tiingo_backup_20250905'
    ),
    'column_changes', jsonb_build_array(
        jsonb_build_object('table', 'intg_daily_price_tiingo', 'column', 'adjclose', 'renamed_to', 'adjusted_close')
    )
)
WHERE migration_name = '059_standardize_naming_integration' AND phase = 'phase1';

-- Mark migration as completed
UPDATE intg_migration_log 
SET status = 'completed', completed_at = NOW()
WHERE migration_name = '059_standardize_naming_integration' AND phase = 'phase1';

\echo 'Migration 059 Integration completed successfully! ✅'
\echo 'Backup table created for rollback if needed:'
\echo '  - intg_daily_price_tiingo_backup_20250905'

COMMIT;

-- =====================================================
-- POST-MIGRATION VERIFICATION (INTEGRATION)
-- =====================================================

\echo 'Running integration post-migration verification...'

-- Check that expected columns exist with correct names in integration
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_name = 'intg_daily_price_tiingo'
AND column_name IN ('adjusted_close', 'created_at', 'updated_at')
ORDER BY table_name, column_name;

\echo 'Integration Migration 059 verification completed ✅'
\echo ''
\echo '⚠️  IMPORTANT: Test integration environment applications'
\echo '⚠️  Next: Apply Phase 2 changes if needed'