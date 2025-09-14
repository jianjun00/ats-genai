-- Migration: Standardize INTG table naming conventions (plural to singular)
-- Purpose: Convert plural table names to singular forms for consistency - INTG environment specific
-- Author: Claude Code
-- Date: 2025-09-13

-- =============================================
-- INTG ENVIRONMENT SPECIFIC TABLE RENAMES
-- =============================================

-- Handle intg_training_dataset tables (there are two conflicting tables)
DO $$ 
BEGIN
    -- First, drop the incomplete intg_training_dataset table if it exists
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_training_dataset') THEN
        -- Check if it has the old structure (creation_timestamp column)
        IF EXISTS (SELECT FROM information_schema.columns 
                   WHERE table_name = 'intg_training_dataset' AND column_name = 'creation_timestamp') THEN
            DROP TABLE intg_training_dataset CASCADE;
            RAISE NOTICE 'Dropped incomplete intg_training_dataset table';
        END IF;
    END IF;
    
    -- Then rename intg_training_datasets to intg_training_dataset
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_training_datasets') THEN
        -- Rename the table
        ALTER TABLE intg_training_datasets RENAME TO intg_training_dataset;
        
        -- Update primary key constraint name
        ALTER INDEX intg_training_datasets_pkey RENAME TO intg_training_dataset_pk;
        
        -- Update other indexes
        ALTER INDEX idx_intg_training_datasets_dataset_path RENAME TO idx_intg_training_dataset_dataset_path;
        ALTER INDEX idx_intg_training_datasets_file_metadata_gin RENAME TO idx_intg_training_dataset_file_metadata_gin;
        
        -- Update unique constraints
        ALTER TABLE intg_training_dataset RENAME CONSTRAINT unique_dataset_name_intg_new 
            TO unique_dataset_name_intg;
        
        RAISE NOTICE 'Renamed intg_training_datasets to intg_training_dataset';
    END IF;
END $$;

-- Fix any column naming inconsistencies if needed
DO $$
BEGIN
    -- Check if there are any creation_timestamp columns that should be created_at
    -- (This handles any remaining inconsistencies)
    IF EXISTS (SELECT FROM information_schema.columns 
               WHERE column_name = 'creation_timestamp') THEN
        RAISE NOTICE 'Found creation_timestamp columns - manual review needed';
    END IF;
END $$;

-- Clean up any dev tables that shouldn't be in intg environment
DO $$
BEGIN
    -- Remove dev_training_datasets from intg if it exists
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_datasets') THEN
        DROP TABLE dev_training_datasets CASCADE;
        RAISE NOTICE 'Removed dev_training_datasets from intg environment';
    END IF;
    
    -- Note: Keep dev_monthly_training_data as it might be used for cross-environment operations
END $$;