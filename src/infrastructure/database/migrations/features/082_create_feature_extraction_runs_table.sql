-- Migration: Create feature_extraction_runs table for tracking feature extraction jobs
-- This table tracks feature extraction runs separately from generic runs table
-- Stores feature-specific metadata like feature groups, instruments, and file paths

-- Create feature_extraction_runs table for dev environment
CREATE TABLE IF NOT EXISTS dev_feature_extraction_runs (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES dev_runs(id) ON DELETE CASCADE,
    status VARCHAR(50) DEFAULT 'running',
    feature_groups TEXT[] DEFAULT '{}',
    date_range_start DATE,
    date_range_end DATE,
    total_instruments INTEGER DEFAULT 0,
    total_features_generated INTEGER DEFAULT 0,
    execution_duration_seconds INTEGER DEFAULT 0,
    command_line TEXT DEFAULT '',
    environment VARCHAR(50) DEFAULT 'dev',
    parameters JSONB DEFAULT '{}',
    results JSONB DEFAULT '{}',
    dataset_path VARCHAR(500) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for dev environment
CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_run_id ON dev_feature_extraction_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_status ON dev_feature_extraction_runs(status);
CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_date_range ON dev_feature_extraction_runs(date_range_start, date_range_end);
CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_dataset_path ON dev_feature_extraction_runs(dataset_path);

-- Create feature_extraction_runs table for intg environment
CREATE TABLE IF NOT EXISTS intg_feature_extraction_runs (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES intg_runs(id) ON DELETE CASCADE,
    status VARCHAR(50) DEFAULT 'running',
    feature_groups TEXT[] DEFAULT '{}',
    date_range_start DATE,
    date_range_end DATE,
    total_instruments INTEGER DEFAULT 0,
    total_features_generated INTEGER DEFAULT 0,
    execution_duration_seconds INTEGER DEFAULT 0,
    command_line TEXT DEFAULT '',
    environment VARCHAR(50) DEFAULT 'intg',
    parameters JSONB DEFAULT '{}',
    results JSONB DEFAULT '{}',
    dataset_path VARCHAR(500) DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for intg environment
CREATE INDEX IF NOT EXISTS idx_intg_feature_extraction_runs_run_id ON intg_feature_extraction_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_intg_feature_extraction_runs_status ON intg_feature_extraction_runs(status);
CREATE INDEX IF NOT EXISTS idx_intg_feature_extraction_runs_date_range ON intg_feature_extraction_runs(date_range_start, date_range_end);
CREATE INDEX IF NOT EXISTS idx_intg_feature_extraction_runs_dataset_path ON intg_feature_extraction_runs(dataset_path);

-- Add comments
COMMENT ON TABLE dev_feature_extraction_runs IS 'Tracks feature extraction runs with feature-specific metadata';
COMMENT ON TABLE intg_feature_extraction_runs IS 'Tracks feature extraction runs with feature-specific metadata';
COMMENT ON COLUMN dev_feature_extraction_runs.dataset_path IS 'Actual dataset directory name (e.g., dataset_20250928_022056)';
COMMENT ON COLUMN intg_feature_extraction_runs.dataset_path IS 'Actual dataset directory name (e.g., dataset_20250928_022056)';