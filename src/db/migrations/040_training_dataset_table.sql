-- Migration 036: Create training_dataset table for tracking training data generation

-- Create training_dataset table
CREATE TABLE IF NOT EXISTS dev_training_dataset (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) UNIQUE NOT NULL,
    run_id INTEGER NOT NULL,  -- Link to dev_runs table
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Data structure information
    total_sequences INTEGER NOT NULL DEFAULT 0,
    sequence_length INTEGER NOT NULL DEFAULT 0,
    prediction_horizon INTEGER NOT NULL DEFAULT 0,
    feature_count INTEGER NOT NULL DEFAULT 0,
    label_count INTEGER NOT NULL DEFAULT 0,
    
    -- Symbol and date range information
    symbols TEXT[] NOT NULL DEFAULT '{}',  -- Array of symbols
    date_range_start DATE,
    date_range_end DATE,
    
    -- File storage information
    features_file_path TEXT,
    labels_file_path TEXT,
    metadata_file_path TEXT,
    
    -- Generation configuration
    gin_config_path TEXT,
    generation_parameters JSONB,  -- Store generation parameters as JSON
    
    -- Data quality metrics
    data_quality_score NUMERIC(5,4) DEFAULT 0.0,
    feature_completeness NUMERIC(5,4) DEFAULT 0.0,
    label_completeness NUMERIC(5,4) DEFAULT 0.0,
    outlier_ratio NUMERIC(5,4) DEFAULT 0.0,
    missing_data_ratio NUMERIC(5,4) DEFAULT 0.0,
    
    -- Processing metrics
    generation_duration_seconds INTEGER DEFAULT 0,
    file_size_mb NUMERIC(10,2) DEFAULT 0.0,
    
    -- Data sources used
    data_sources TEXT[] DEFAULT '{}',  -- Array of data source names (polygon, tiingo, etc.)
    
    -- Status and validation
    status VARCHAR(50) DEFAULT 'created',  -- created, validated, failed, archived
    validation_results JSONB,  -- Store validation test results
    error_message TEXT,
    
    -- Versioning and lineage
    parent_dataset_id INTEGER REFERENCES dev_training_dataset(id),
    version_tag VARCHAR(100),
    
    -- Audit fields
    created_by VARCHAR(255) DEFAULT 'system',
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Add constraints
    CONSTRAINT valid_data_quality_score CHECK (data_quality_score >= 0.0 AND data_quality_score <= 1.0),
    CONSTRAINT valid_completeness CHECK (feature_completeness >= 0.0 AND feature_completeness <= 1.0 AND label_completeness >= 0.0 AND label_completeness <= 1.0),
    CONSTRAINT valid_status CHECK (status IN ('created', 'validated', 'failed', 'archived')),
    CONSTRAINT positive_sequences CHECK (total_sequences >= 0),
    CONSTRAINT positive_features CHECK (feature_count >= 0),
    CONSTRAINT positive_labels CHECK (label_count >= 0)
);

-- Add foreign key reference to runs table
ALTER TABLE dev_training_dataset ADD CONSTRAINT fk_training_dataset_run 
    FOREIGN KEY (run_id) REFERENCES dev_runs(id) ON DELETE CASCADE;

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_training_dataset_run_id ON dev_training_dataset(run_id);
CREATE INDEX IF NOT EXISTS idx_training_dataset_creation_timestamp ON dev_training_dataset(creation_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_training_dataset_symbols ON dev_training_dataset USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_training_dataset_date_range ON dev_training_dataset(date_range_start, date_range_end);
CREATE INDEX IF NOT EXISTS idx_training_dataset_status ON dev_training_dataset(status);
CREATE INDEX IF NOT EXISTS idx_training_dataset_quality_score ON dev_training_dataset(data_quality_score DESC);

-- Create a view for easy training dataset reporting
CREATE OR REPLACE VIEW dev_training_dataset_summary AS
SELECT 
    td.id,
    td.dataset_name,
    td.run_id,
    r.run_type,
    r.start_time as run_start_time,
    r.status as run_status,
    td.creation_timestamp,
    td.total_sequences,
    td.sequence_length,
    td.prediction_horizon,
    td.feature_count,
    td.label_count,
    array_length(td.symbols, 1) as symbol_count,
    td.date_range_start,
    td.date_range_end,
    td.data_quality_score,
    td.feature_completeness,
    td.label_completeness,
    td.generation_duration_seconds,
    td.file_size_mb,
    td.status,
    td.version_tag,
    td.parent_dataset_id,
    EXTRACT(EPOCH FROM (td.creation_timestamp - r.start_time))::INTEGER as run_to_dataset_delay_seconds
FROM dev_training_dataset td
JOIN dev_runs r ON td.run_id = r.id
ORDER BY td.creation_timestamp DESC;

-- Add a training data generation run type if it doesn't exist
-- This ensures the runs table supports training data generation jobs
DO $$
BEGIN
    -- Add any additional columns needed for training data runs
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dev_runs' AND column_name = 'training_config') THEN
        ALTER TABLE dev_runs ADD COLUMN training_config JSONB;
    END IF;
END $$;

-- Update the enhanced runs table summary view to include training data info
CREATE OR REPLACE VIEW dev_training_runs_summary AS
SELECT 
    r.id as run_id,
    r.run_type,
    r.start_time,
    r.end_time,
    EXTRACT(EPOCH FROM (r.end_time - r.start_time))::INTEGER as duration_seconds,
    r.status,
    r.training_config,
    COUNT(td.id) as datasets_generated,
    SUM(td.total_sequences) as total_sequences_generated,
    AVG(td.data_quality_score) as avg_data_quality_score,
    SUM(td.file_size_mb) as total_file_size_mb,
    STRING_AGG(DISTINCT td.dataset_name, ', ') as dataset_names
FROM dev_runs r
LEFT JOIN dev_training_dataset td ON r.id = td.run_id
WHERE r.run_type = 'training_data_generation'
GROUP BY r.id, r.run_type, r.start_time, r.end_time, r.status, r.training_config
ORDER BY r.start_time DESC;

-- Add comments for documentation
COMMENT ON TABLE dev_training_dataset IS 'Training dataset tracking with run linkage and comprehensive metadata';
COMMENT ON COLUMN dev_training_dataset.run_id IS 'Foreign key linking to dev_runs table for job tracking';
COMMENT ON COLUMN dev_training_dataset.total_sequences IS 'Number of time series sequences generated';
COMMENT ON COLUMN dev_training_dataset.generation_parameters IS 'JSON configuration used for dataset generation';
COMMENT ON COLUMN dev_training_dataset.data_quality_score IS 'Overall data quality score between 0 and 1';
COMMENT ON COLUMN dev_training_dataset.symbols IS 'Array of stock symbols included in the dataset';
COMMENT ON COLUMN dev_training_dataset.data_sources IS 'Array of data sources used (polygon, tiingo, fmp, etc.)';
COMMENT ON VIEW dev_training_dataset_summary IS 'Summary view joining training datasets with their originating runs';
COMMENT ON VIEW dev_training_runs_summary IS 'Summary of training data generation runs with aggregated dataset metrics';