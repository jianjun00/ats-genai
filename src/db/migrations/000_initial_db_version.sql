-- Migration 000: Initial database version tracking
-- The db_version table is already created by get_current_version method
-- This migration just records that version 0 has been applied

-- Insert initial version record (table already exists from get_current_version)
INSERT INTO db_version (version, description, migration_file)
VALUES (0, 'Initial database version tracking', '000_initial_db_version.sql')
ON CONFLICT (version) DO UPDATE SET
    description = EXCLUDED.description,
    migration_file = EXCLUDED.migration_file,
    applied_at = now();
