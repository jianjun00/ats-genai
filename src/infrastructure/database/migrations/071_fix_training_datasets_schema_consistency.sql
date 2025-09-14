-- Migration: Fix training datasets schema consistency across environments
-- Date: 2025-09-11
-- Purpose: Establish single ground truth for training_datasets schema
-- 
-- ISSUE: Schema drift detected between DEV (44 cols) and INTG (33 cols)
-- SOLUTION: Add minimal required columns to INTG for analytics service compatibility
-- PRINCIPLE: Migration manager is single source of truth - no manual schema changes

-- Add missing columns to INTG to support analytics service
-- These columns are required by analytics_service.py queries

ALTER TABLE intg_training_datasets 
ADD COLUMN IF NOT EXISTS dataset_path VARCHAR(500) DEFAULT '';

ALTER TABLE intg_training_datasets 
ADD COLUMN IF NOT EXISTS symbol_files JSONB DEFAULT '{}';

ALTER TABLE intg_training_datasets 
ADD COLUMN IF NOT EXISTS file_metadata JSONB DEFAULT '{}';

-- Add comments to document the purpose
COMMENT ON COLUMN intg_training_datasets.dataset_path IS 'Primary dataset file path for analytics service compatibility';
COMMENT ON COLUMN intg_training_datasets.symbol_files IS 'Symbol-specific file paths (features, labels) in JSON format';
COMMENT ON COLUMN intg_training_datasets.file_metadata IS 'File metadata including sizes, checksums, and structure info';

-- Create indexes for performance on new columns
CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_dataset_path ON intg_training_datasets(dataset_path);
CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_file_metadata_gin ON intg_training_datasets USING gin(file_metadata);

-- Update constraint name to match new schema version
ALTER TABLE intg_training_datasets 
DROP CONSTRAINT IF EXISTS unique_dataset_name_intg;

ALTER TABLE intg_training_datasets 
ADD CONSTRAINT unique_dataset_name_intg_new UNIQUE(dataset_name);

-- Apply same minimal changes to DEV for consistency (DEV already has these columns)
-- This ensures both environments have the same minimal required schema

ALTER TABLE dev_training_datasets 
ADD COLUMN IF NOT EXISTS dataset_path VARCHAR(500) DEFAULT '';

ALTER TABLE dev_training_datasets 
ADD COLUMN IF NOT EXISTS symbol_files JSONB DEFAULT '{}';

ALTER TABLE dev_training_datasets 
ADD COLUMN IF NOT EXISTS file_metadata JSONB DEFAULT '{}';

-- Add comments for consistency
COMMENT ON COLUMN dev_training_datasets.dataset_path IS 'Primary dataset file path for analytics service compatibility';
COMMENT ON COLUMN dev_training_datasets.symbol_files IS 'Symbol-specific file paths (features, labels) in JSON format';  
COMMENT ON COLUMN dev_training_datasets.file_metadata IS 'File metadata including sizes, checksums, and structure info';

-- Create indexes for performance (if not exist)
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_dataset_path ON dev_training_datasets(dataset_path);

-- NOTE: DEV environment has additional columns that were manually added:
-- schema_protobuf, schema_version, schema_hash, schema_json, feature_schema, 
-- label_schema, validation_results, compatibility_info
-- 
-- These are NOT part of the migration manager ground truth and should be 
-- considered technical debt. They are left in place for DEV to avoid breaking
-- existing functionality, but new environments should NOT include them.
--
-- FUTURE: Consider migration to remove these extra columns from DEV to achieve
-- complete schema consistency across all environments.

-- Verification queries to ensure consistency
-- Run these after migration to verify schema alignment:
--
-- SELECT 'dev' as env, count(*) as columns FROM information_schema.columns WHERE table_name = 'dev_training_datasets'
-- UNION ALL
-- SELECT 'intg' as env, count(*) as columns FROM information_schema.columns WHERE table_name = 'intg_training_datasets';
--
-- Expected result: DEV will have more columns due to manual additions, but both will have 
-- the required columns (dataset_path, symbol_files, file_metadata) for analytics service.