-- Migration: Create training dataset tracking table with TFDV stats
-- Date: 2025-08-31
-- Purpose: Add comprehensive training dataset metadata for EDA tool integration

-- Create training datasets table for all environments
CREATE TABLE IF NOT EXISTS dev_training_datasets (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    run_id INTEGER,
    total_sequences INTEGER DEFAULT 0,
    sequence_length INTEGER DEFAULT 0,
    feature_count INTEGER DEFAULT 0,
    label_count INTEGER DEFAULT 0,
    symbols TEXT[], -- Array of symbols
    date_range_start DATE,
    date_range_end DATE,
    data_quality_score FLOAT DEFAULT 0.0,
    feature_completeness FLOAT DEFAULT 0.0,
    label_completeness FLOAT DEFAULT 0.0,
    generation_duration_seconds INTEGER DEFAULT 0,
    file_size_mb FLOAT DEFAULT 0.0,
    data_sources TEXT[], -- Array of data sources
    status VARCHAR(50) DEFAULT 'created',
    features_file_path TEXT DEFAULT '',
    labels_file_path TEXT DEFAULT '',
    metadata_file_path TEXT DEFAULT '',
    feature_metadata TEXT DEFAULT '',
    technical_indicators TEXT DEFAULT '',
    prediction_horizon INTEGER DEFAULT 0,
    created_by VARCHAR(100) DEFAULT 'system',
    generation_parameters JSONB DEFAULT '{}',
    
    -- TFDV (TensorFlow Data Validation) stats
    tfdv_statistics JSONB DEFAULT '{}', -- Full TFDV statistics
    tfdv_histogram_path TEXT DEFAULT '', -- Path to histogram files
    tfdv_anomalies JSONB DEFAULT '{}', -- Data anomalies detected
    tfdv_schema_path TEXT DEFAULT '', -- Path to inferred schema
    feature_distributions JSONB DEFAULT '{}', -- Feature distribution summaries
    label_distributions JSONB DEFAULT '{}', -- Label distribution summaries
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes
    CONSTRAINT unique_dataset_name_dev UNIQUE(dataset_name)
);

CREATE TABLE IF NOT EXISTS intg_training_datasets (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    run_id INTEGER,
    total_sequences INTEGER DEFAULT 0,
    sequence_length INTEGER DEFAULT 0,
    feature_count INTEGER DEFAULT 0,
    label_count INTEGER DEFAULT 0,
    symbols TEXT[], -- Array of symbols
    date_range_start DATE,
    date_range_end DATE,
    data_quality_score FLOAT DEFAULT 0.0,
    feature_completeness FLOAT DEFAULT 0.0,
    label_completeness FLOAT DEFAULT 0.0,
    generation_duration_seconds INTEGER DEFAULT 0,
    file_size_mb FLOAT DEFAULT 0.0,
    data_sources TEXT[], -- Array of data sources
    status VARCHAR(50) DEFAULT 'created',
    features_file_path TEXT DEFAULT '',
    labels_file_path TEXT DEFAULT '',
    metadata_file_path TEXT DEFAULT '',
    feature_metadata TEXT DEFAULT '',
    technical_indicators TEXT DEFAULT '',
    prediction_horizon INTEGER DEFAULT 0,
    created_by VARCHAR(100) DEFAULT 'system',
    generation_parameters JSONB DEFAULT '{}',
    
    -- TFDV (TensorFlow Data Validation) stats
    tfdv_statistics JSONB DEFAULT '{}', -- Full TFDV statistics
    tfdv_histogram_path TEXT DEFAULT '', -- Path to histogram files
    tfdv_anomalies JSONB DEFAULT '{}', -- Data anomalies detected
    tfdv_schema_path TEXT DEFAULT '', -- Path to inferred schema
    feature_distributions JSONB DEFAULT '{}', -- Feature distribution summaries
    label_distributions JSONB DEFAULT '{}', -- Label distribution summaries
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes
    CONSTRAINT unique_dataset_name_intg UNIQUE(dataset_name)
);

-- Add test table conditionally
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_runs') THEN
        CREATE TABLE IF NOT EXISTS test_training_datasets (
            id SERIAL PRIMARY KEY,
            dataset_name VARCHAR(255) NOT NULL,
            run_id INTEGER,
            total_sequences INTEGER DEFAULT 0,
            sequence_length INTEGER DEFAULT 0,
            feature_count INTEGER DEFAULT 0,
            label_count INTEGER DEFAULT 0,
            symbols TEXT[], -- Array of symbols
            date_range_start DATE,
            date_range_end DATE,
            data_quality_score FLOAT DEFAULT 0.0,
            feature_completeness FLOAT DEFAULT 0.0,
            label_completeness FLOAT DEFAULT 0.0,
            generation_duration_seconds INTEGER DEFAULT 0,
            file_size_mb FLOAT DEFAULT 0.0,
            data_sources TEXT[], -- Array of data sources
            status VARCHAR(50) DEFAULT 'created',
            features_file_path TEXT DEFAULT '',
            labels_file_path TEXT DEFAULT '',
            metadata_file_path TEXT DEFAULT '',
            feature_metadata TEXT DEFAULT '',
            technical_indicators TEXT DEFAULT '',
            prediction_horizon INTEGER DEFAULT 0,
            created_by VARCHAR(100) DEFAULT 'system',
            generation_parameters JSONB DEFAULT '{}',
            
            -- TFDV stats
            tfdv_statistics JSONB DEFAULT '{}',
            tfdv_histogram_path TEXT DEFAULT '',
            tfdv_anomalies JSONB DEFAULT '{}',
            tfdv_schema_path TEXT DEFAULT '',
            feature_distributions JSONB DEFAULT '{}',
            label_distributions JSONB DEFAULT '{}',
            
            -- Metadata
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            
            CONSTRAINT unique_dataset_name_test UNIQUE(dataset_name)
        );
    END IF;
END $$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_status ON dev_training_datasets(status);
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_created_at ON dev_training_datasets(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_symbols ON dev_training_datasets USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_run_id ON dev_training_datasets(run_id);

CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_status ON intg_training_datasets(status);
CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_created_at ON intg_training_datasets(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_symbols ON intg_training_datasets USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_run_id ON intg_training_datasets(run_id);

-- Add foreign key constraints if runs tables exist
DO $$ 
BEGIN
    -- Link to runs table if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_runs') THEN
        ALTER TABLE dev_training_datasets 
        ADD CONSTRAINT fk_dev_training_datasets_run_id 
        FOREIGN KEY (run_id) REFERENCES dev_runs(id) ON DELETE SET NULL;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'intg_runs') THEN
        ALTER TABLE intg_training_datasets 
        ADD CONSTRAINT fk_intg_training_datasets_run_id 
        FOREIGN KEY (run_id) REFERENCES intg_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Add column comments for documentation
COMMENT ON TABLE dev_training_datasets IS 'Training dataset metadata with TFDV statistics for EDA integration';
COMMENT ON COLUMN dev_training_datasets.tfdv_statistics IS 'Full TensorFlow Data Validation statistics in JSON format';
COMMENT ON COLUMN dev_training_datasets.tfdv_histogram_path IS 'File system path to TFDV histogram visualizations';
COMMENT ON COLUMN dev_training_datasets.tfdv_anomalies IS 'Data anomalies detected by TFDV';
COMMENT ON COLUMN dev_training_datasets.feature_distributions IS 'Summary statistics for each feature';
COMMENT ON COLUMN dev_training_datasets.label_distributions IS 'Summary statistics for each label';

COMMENT ON TABLE intg_training_datasets IS 'Training dataset metadata with TFDV statistics for EDA integration';
COMMENT ON COLUMN intg_training_datasets.tfdv_statistics IS 'Full TensorFlow Data Validation statistics in JSON format';
COMMENT ON COLUMN intg_training_datasets.tfdv_histogram_path IS 'File system path to TFDV histogram visualizations';
COMMENT ON COLUMN intg_training_datasets.tfdv_anomalies IS 'Data anomalies detected by TFDV';
COMMENT ON COLUMN intg_training_datasets.feature_distributions IS 'Summary statistics for each feature';
COMMENT ON COLUMN intg_training_datasets.label_distributions IS 'Summary statistics for each label';