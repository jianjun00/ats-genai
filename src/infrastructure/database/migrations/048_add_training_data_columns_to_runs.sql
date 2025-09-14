-- Migration: Add training data tracking columns to runs table
-- Date: 2025-08-30
-- Purpose: Add columns needed by training_data_job_runner.py for tracking training data generation

-- Add training data specific columns to all environment runs tables
ALTER TABLE dev_runs 
ADD COLUMN IF NOT EXISTS total_symbols INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS training_config JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS successful_unifications INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS total_dates INTEGER DEFAULT 0,  
ADD COLUMN IF NOT EXISTS processing_rate_per_second FLOAT DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS quality_summary TEXT DEFAULT '',
ADD COLUMN IF NOT EXISTS performance_summary TEXT DEFAULT '';

ALTER TABLE intg_runs 
ADD COLUMN IF NOT EXISTS total_symbols INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS training_config JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS successful_unifications INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS total_dates INTEGER DEFAULT 0,  
ADD COLUMN IF NOT EXISTS processing_rate_per_second FLOAT DEFAULT 0.0,
ADD COLUMN IF NOT EXISTS quality_summary TEXT DEFAULT '',
ADD COLUMN IF NOT EXISTS performance_summary TEXT DEFAULT '';

-- Add similar columns to test_runs if it exists (for unit tests)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_runs') THEN
        ALTER TABLE test_runs 
        ADD COLUMN IF NOT EXISTS total_symbols INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS training_config JSONB DEFAULT '{}',
        ADD COLUMN IF NOT EXISTS successful_unifications INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS total_dates INTEGER DEFAULT 0,  
        ADD COLUMN IF NOT EXISTS processing_rate_per_second FLOAT DEFAULT 0.0,
        ADD COLUMN IF NOT EXISTS quality_summary TEXT DEFAULT '',
        ADD COLUMN IF NOT EXISTS performance_summary TEXT DEFAULT '';
    END IF;
END $$;

-- Add comments explaining the new columns
COMMENT ON COLUMN dev_runs.total_symbols IS 'Number of symbols processed in training data generation';
COMMENT ON COLUMN dev_runs.training_config IS 'JSON configuration for training data generation parameters';
COMMENT ON COLUMN dev_runs.successful_unifications IS 'Count of successful data unifications during training data generation';
COMMENT ON COLUMN dev_runs.total_dates IS 'Total number of dates processed in training data generation';
COMMENT ON COLUMN dev_runs.processing_rate_per_second IS 'Processing rate in records per second';
COMMENT ON COLUMN dev_runs.quality_summary IS 'Human-readable summary of data quality metrics';
COMMENT ON COLUMN dev_runs.performance_summary IS 'Human-readable summary of processing performance';

COMMENT ON COLUMN intg_runs.total_symbols IS 'Number of symbols processed in training data generation';
COMMENT ON COLUMN intg_runs.training_config IS 'JSON configuration for training data generation parameters';
COMMENT ON COLUMN intg_runs.successful_unifications IS 'Count of successful data unifications during training data generation';
COMMENT ON COLUMN intg_runs.total_dates IS 'Total number of dates processed in training data generation';
COMMENT ON COLUMN intg_runs.processing_rate_per_second IS 'Processing rate in records per second';
COMMENT ON COLUMN intg_runs.quality_summary IS 'Human-readable summary of data quality metrics';
COMMENT ON COLUMN intg_runs.performance_summary IS 'Human-readable summary of processing performance';