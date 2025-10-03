-- Migration 007: Add extended columns to runs table for enhanced run metadata tracking
-- This migration adds columns that RunMetadataTracker expects but were missing from the original schema.
-- These columns enable comprehensive tracking of run context, environment, and reproducibility metadata.

-- Environment-aware table name detection (dev_runs vs intg_runs)
DO $$
DECLARE
    table_name TEXT;
BEGIN
    -- Determine correct table name based on current database
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE information_schema.tables.table_name = 'intg_runs') THEN
        table_name := 'intg_runs';
    ELSIF EXISTS (SELECT 1 FROM information_schema.tables WHERE information_schema.tables.table_name = 'dev_runs') THEN
        table_name := 'dev_runs';
    ELSIF EXISTS (SELECT 1 FROM information_schema.tables WHERE information_schema.tables.table_name = 'runs') THEN
        table_name := 'runs';
    ELSE
        -- No runs table exists, skip this migration
        RAISE NOTICE 'Migration 007 skipped: No runs table found (intg_runs, dev_runs, or runs)';
        RETURN;
    END IF;
    
    -- Add missing columns to the identified table
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS command_line TEXT DEFAULT ''''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS git_commit_hash VARCHAR(64) DEFAULT ''''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS git_branch VARCHAR(100) DEFAULT ''''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS environment VARCHAR(50) DEFAULT ''dev''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS results JSONB DEFAULT ''{}''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS host_info JSONB DEFAULT ''{}''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS working_directory VARCHAR(500) DEFAULT ''''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS python_version VARCHAR(50) DEFAULT ''''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS dependencies_hash VARCHAR(64) DEFAULT ''''', table_name);
    
    -- Update existing columns to match RunMetadataTracker expectations
    EXECUTE format('ALTER TABLE %I ALTER COLUMN created_by SET DEFAULT ''system''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS created_by VARCHAR(100) DEFAULT ''system''', table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS parameters JSONB DEFAULT ''{}''', table_name);
    
    -- Add indexes for performance on new columns
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_environment ON %I(environment)', replace(table_name, '_', ''), table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_git_branch ON %I(git_branch)', replace(table_name, '_', ''), table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_git_commit ON %I(git_commit_hash)', replace(table_name, '_', ''), table_name);
    
    RAISE NOTICE 'Migration 007 applied to table: %', table_name;
END $$;