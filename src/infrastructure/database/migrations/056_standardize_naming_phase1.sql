-- Database Naming Standardization - Phase 1 (Low Risk)
-- Migration: 056_standardize_naming_phase1.sql
-- Created: 2025-09-05
-- Purpose: Standardize critical column names while preserving all data
-- Environment: Auto-detects dev_ or intg_ table prefixes

-- =====================================================
-- ENVIRONMENT DETECTION AND SETUP
-- =====================================================

\echo 'Starting Migration 056: Database naming standardization (Phase 1)...'

-- Detect environment based on existing tables
DO $$
DECLARE
    env_prefix TEXT;
BEGIN
    -- Check if dev_ tables exist
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name LIKE 'dev_%' LIMIT 1) THEN
        env_prefix := 'dev';
    -- Check if intg_ tables exist  
    ELSIF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name LIKE 'intg_%' LIMIT 1) THEN
        env_prefix := 'intg';
    ELSE
        RAISE EXCEPTION 'No dev_ or intg_ tables found - cannot determine environment';
    END IF;
    
    -- Store environment prefix for use in migration
    CREATE TEMP TABLE migration_env (prefix TEXT);
    INSERT INTO migration_env VALUES (env_prefix);
    
    RAISE NOTICE 'Detected environment: %', env_prefix;
END
$$;

BEGIN;

-- Create migration log table with environment-specific name
DO $$
DECLARE
    env_prefix TEXT;
    log_table_name TEXT;
    sql_cmd TEXT;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    log_table_name := env_prefix || '_migration_log';
    
    sql_cmd := format('CREATE TABLE IF NOT EXISTS %I (
        id SERIAL PRIMARY KEY,
        migration_name TEXT NOT NULL,
        phase TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT ''in_progress'',
        started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        completed_at TIMESTAMP WITH TIME ZONE,
        rollback_info JSONB,
        notes TEXT
    )', log_table_name);
    
    EXECUTE sql_cmd;
    
    -- Log start of migration
    sql_cmd := format('INSERT INTO %I (migration_name, phase, notes) VALUES ($1, $2, $3)', log_table_name);
    EXECUTE sql_cmd USING '056_standardize_naming', 'phase1', 'Critical naming standardization - low risk changes';
END
$$;

-- =====================================================
-- 1.1 STANDARDIZE ADJUSTED CLOSE COLUMN NAMING
-- =====================================================

\echo 'Step 1.1: Creating backup tables...'

-- Create backup tables for safety (environment-aware)
DO $$
DECLARE
    env_prefix TEXT;
    sql_cmd TEXT;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    
    -- Create backup for daily_prices table if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = env_prefix || '_daily_prices') THEN
        sql_cmd := format('CREATE TABLE %I AS SELECT * FROM %I', 
                         env_prefix || '_daily_prices_backup_20250905',
                         env_prefix || '_daily_prices');
        EXECUTE sql_cmd;
        RAISE NOTICE 'Created backup: %_daily_prices_backup_20250905', env_prefix;
    END IF;
    
    -- Create backup for daily_prices_tiingo table if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = env_prefix || '_daily_prices_tiingo') THEN
        sql_cmd := format('CREATE TABLE %I AS SELECT * FROM %I', 
                         env_prefix || '_daily_prices_tiingo_backup_20250905',
                         env_prefix || '_daily_prices_tiingo');
        EXECUTE sql_cmd;
        RAISE NOTICE 'Created backup: %_daily_prices_tiingo_backup_20250905', env_prefix;
    END IF;
END
$$;

\echo 'Step 1.1: Renaming adjusted close columns...'

-- Rename adjusted close columns (environment-aware)
DO $$
DECLARE
    env_prefix TEXT;
    sql_cmd TEXT;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    
    -- Rename adjclose to adjusted_close in tiingo table if column exists
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = env_prefix || '_daily_prices_tiingo' 
               AND column_name = 'adjclose') THEN
        sql_cmd := format('ALTER TABLE %I RENAME COLUMN adjclose TO adjusted_close', 
                         env_prefix || '_daily_prices_tiingo');
        EXECUTE sql_cmd;
        RAISE NOTICE 'Renamed adjclose to adjusted_close in %_daily_prices_tiingo', env_prefix;
    END IF;
    
    -- Rename adjusted_price to adjusted_close in main table if column exists
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = env_prefix || '_daily_prices' 
               AND column_name = 'adjusted_price') THEN
        sql_cmd := format('ALTER TABLE %I RENAME COLUMN adjusted_price TO adjusted_close', 
                         env_prefix || '_daily_prices');
        EXECUTE sql_cmd;
        RAISE NOTICE 'Renamed adjusted_price to adjusted_close in %_daily_prices', env_prefix;
    END IF;
END
$$;

\echo 'Step 1.1: Adjusted close columns standardized ✅'

-- =====================================================
-- 1.2 FIX CRITICAL FOREIGN KEY DATA TYPE MISMATCHES
-- =====================================================

\echo 'Step 1.2: Fixing instrument_id data types...'

-- Create backup and fix reconciled records table (environment-aware)
DO $$
DECLARE
    env_prefix TEXT;
    reconciled_table TEXT;
    instruments_table TEXT;
    constraint_name TEXT;
    sql_cmd TEXT;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    reconciled_table := env_prefix || '_reconciled_records';
    instruments_table := env_prefix || '_instruments';
    constraint_name := reconciled_table || '_instrument_id_fkey';
    
    -- Create backup if table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = reconciled_table) THEN
        sql_cmd := format('CREATE TABLE %I AS SELECT * FROM %I', 
                         reconciled_table || '_backup_20250905', reconciled_table);
        EXECUTE sql_cmd;
        RAISE NOTICE 'Created backup: %_backup_20250905', reconciled_table;
        
        -- Check if instrument_id column needs fixing (text -> integer conversion)
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = reconciled_table 
            AND column_name = 'instrument_id' 
            AND data_type IN ('text', 'character varying')
        ) THEN
            -- First, ensure all values are valid integers or NULL
            sql_cmd := format('UPDATE %I SET instrument_id = NULL WHERE instrument_id IS NOT NULL AND instrument_id !~ ''^[0-9]+$''', reconciled_table);
            EXECUTE sql_cmd;
            
            -- Convert to integer type
            sql_cmd := format('ALTER TABLE %I ALTER COLUMN instrument_id TYPE integer USING instrument_id::integer', reconciled_table);
            EXECUTE sql_cmd;
            
            -- Add foreign key constraint if it doesn't exist and instruments table exists
            IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = constraint_name) 
               AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = instruments_table) THEN
                sql_cmd := format('ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (instrument_id) REFERENCES %I(id)', 
                                 reconciled_table, constraint_name, instruments_table);
                EXECUTE sql_cmd;
            END IF;
            
            RAISE NOTICE 'Fixed instrument_id data type in %', reconciled_table;
        ELSE
            RAISE NOTICE 'instrument_id already correct type in %', reconciled_table;
        END IF;
    END IF;
END
$$;

\echo 'Step 1.2: Foreign key data types fixed ✅'

-- =====================================================
-- 1.3 STANDARDIZE TIMESTAMP COLUMNS
-- =====================================================

\echo 'Step 1.3: Standardizing timestamp columns to include timezone...'

-- Environment-aware timestamp standardization
DO $$
DECLARE
    env_prefix TEXT;
    rec RECORD;
    sql_cmd TEXT;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    
    -- Find all timestamp without time zone columns that should be with time zone
    FOR rec IN 
        SELECT table_name, column_name
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        AND data_type = 'timestamp without time zone'
        AND column_name IN ('created_at', 'updated_at', 'timestamp', 'last_updated')
        AND table_name LIKE env_prefix || '_%'
        ORDER BY table_name, column_name
    LOOP
        sql_cmd := format('ALTER TABLE %I ALTER COLUMN %I TYPE timestamp with time zone', 
                         rec.table_name, rec.column_name);
        
        RAISE NOTICE 'Executing: %', sql_cmd;
        EXECUTE sql_cmd;
    END LOOP;
END
$$;

\echo 'Step 1.3: Timestamp columns standardized ✅'

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

\echo 'Running validation queries...'

-- Environment-aware validation queries
DO $$
DECLARE
    env_prefix TEXT;
    sql_cmd TEXT;
    result_row RECORD;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    
    -- Validate adjusted_close column rename for daily_prices tables
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = env_prefix || '_daily_prices') THEN
        sql_cmd := format('SELECT ''%s'' as table_name, COUNT(*) as row_count, COUNT(adjusted_close) as adjusted_close_count FROM %I',
                         env_prefix || '_daily_prices', env_prefix || '_daily_prices');
        FOR result_row IN EXECUTE sql_cmd LOOP
            RAISE NOTICE 'Validation: % - % rows, % adjusted_close values', 
                        result_row.table_name, result_row.row_count, result_row.adjusted_close_count;
        END LOOP;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = env_prefix || '_daily_prices_tiingo') THEN
        sql_cmd := format('SELECT ''%s'' as table_name, COUNT(*) as row_count, COUNT(adjusted_close) as adjusted_close_count FROM %I',
                         env_prefix || '_daily_prices_tiingo', env_prefix || '_daily_prices_tiingo');
        FOR result_row IN EXECUTE sql_cmd LOOP
            RAISE NOTICE 'Validation: % - % rows, % adjusted_close values', 
                        result_row.table_name, result_row.row_count, result_row.adjusted_close_count;
        END LOOP;
    END IF;
    
    -- Validate instrument_id foreign key if both tables exist
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = env_prefix || '_reconciled_records')
       AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = env_prefix || '_instruments') THEN
        sql_cmd := format('SELECT COUNT(*) as total_reconciled_records, COUNT(r.instrument_id) as with_instrument_id, COUNT(i.id) as valid_instrument_refs FROM %I r LEFT JOIN %I i ON r.instrument_id = i.id',
                         env_prefix || '_reconciled_records', env_prefix || '_instruments');
        FOR result_row IN EXECUTE sql_cmd LOOP
            RAISE NOTICE 'Foreign key validation: % total, % with instrument_id, % valid refs',
                        result_row.total_reconciled_records, result_row.with_instrument_id, result_row.valid_instrument_refs;
        END LOOP;
    END IF;
END
$$;

-- Store rollback information (environment-aware)
DO $$
DECLARE
    env_prefix TEXT;
    log_table_name TEXT;
    sql_cmd TEXT;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    log_table_name := env_prefix || '_migration_log';
    
    sql_cmd := format('UPDATE %I SET rollback_info = $1 WHERE migration_name = $2 AND phase = $3', log_table_name);
    EXECUTE sql_cmd USING 
        jsonb_build_object(
            'backup_tables', jsonb_build_array(
                env_prefix || '_daily_prices_backup_20250905',
                env_prefix || '_daily_prices_tiingo_backup_20250905', 
                env_prefix || '_reconciled_records_backup_20250905'
            ),
            'column_changes', jsonb_build_array(
                jsonb_build_object('table', env_prefix || '_daily_prices', 'column', 'adjusted_price', 'renamed_to', 'adjusted_close'),
                jsonb_build_object('table', env_prefix || '_daily_prices_tiingo', 'column', 'adjclose', 'renamed_to', 'adjusted_close'),
                jsonb_build_object('table', env_prefix || '_reconciled_records', 'column', 'instrument_id', 'type_change', 'text->integer')
            )
        ),
        '056_standardize_naming',
        'phase1';
    
    -- Mark migration as completed
    sql_cmd := format('UPDATE %I SET status = $1, completed_at = NOW() WHERE migration_name = $2 AND phase = $3', log_table_name);
    EXECUTE sql_cmd USING 'completed', '056_standardize_naming', 'phase1';
END
$$;

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

-- Environment-aware post-migration verification
DO $$
DECLARE
    env_prefix TEXT;
    result_row RECORD;
BEGIN
    SELECT prefix INTO env_prefix FROM migration_env;
    
    -- Check that all expected columns exist with correct names
    FOR result_row IN 
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name LIKE env_prefix || '_%'
        AND table_name IN (env_prefix || '_daily_prices', env_prefix || '_daily_prices_tiingo', env_prefix || '_reconciled_records')
        AND column_name IN ('adjusted_close', 'instrument_id', 'created_at', 'updated_at')
        ORDER BY table_name, column_name
    LOOP
        RAISE NOTICE 'Post-migration verification: % - % (%) %', 
                    result_row.table_name, result_row.column_name, result_row.data_type, 
                    CASE WHEN result_row.is_nullable = 'YES' THEN 'NULLABLE' ELSE 'NOT NULL' END;
    END LOOP;
END
$$;

\echo 'Migration 056 verification completed ✅'
\echo ''
\echo '⚠️  IMPORTANT: Test your applications to ensure they work with renamed columns'
\echo '⚠️  Next: Review Phase 2 migration script (057) before proceeding'