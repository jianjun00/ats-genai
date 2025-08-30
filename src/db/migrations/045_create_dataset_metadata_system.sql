-- ==================================================
-- Migration 045: ATS Dataset Metadata System
-- Unified metadata tracking for all dataset types
-- ==================================================

-- Main datasets table - unified metadata for all dataset types
CREATE TABLE IF NOT EXISTS dev_datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    dataset_type VARCHAR(50) NOT NULL CHECK (dataset_type IN ('database_table', 'single_file', 'sharded_files', 'training_dataset')),
    
    -- Location information (mutually exclusive based on type)
    table_name VARCHAR(255),                    -- For database_table type
    file_path TEXT,                            -- For single_file type  
    directory_path TEXT,                       -- For sharded_files type
    training_config JSONB,                     -- For training_dataset type
    
    -- Basic metadata
    total_rows BIGINT,
    total_columns INTEGER,
    size_bytes BIGINT,
    file_format VARCHAR(50),                   -- csv, parquet, json, avro, etc.
    compression VARCHAR(20),                   -- gzip, snappy, etc.
    
    -- Schema and structure
    schema_version VARCHAR(50),
    column_count INTEGER,
    primary_keys TEXT[],
    partition_columns TEXT[],
    
    -- Statistics computation status
    stats_computed BOOLEAN DEFAULT FALSE,
    stats_computation_started_at TIMESTAMP WITH TIME ZONE,
    stats_computation_completed_at TIMESTAMP WITH TIME ZONE,
    stats_computation_duration_seconds DECIMAL(10,3),
    stats_version VARCHAR(50) DEFAULT '1.0',
    
    -- Quality metrics
    data_quality_score DECIMAL(5,2),           -- 0-100 score
    completeness_ratio DECIMAL(5,4),           -- 0-1 ratio of non-null values
    uniqueness_issues INTEGER DEFAULT 0,
    outlier_count INTEGER DEFAULT 0,
    
    -- Temporal information
    first_accessed_at TIMESTAMP WITH TIME ZONE,
    last_accessed_at TIMESTAMP WITH TIME ZONE,
    data_freshness_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Dataset columns metadata - detailed column information
CREATE TABLE IF NOT EXISTS dev_dataset_columns (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES dev_datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    ordinal_position INTEGER,
    
    -- Data type information
    data_type VARCHAR(100) NOT NULL,
    semantic_type VARCHAR(50),                 -- Links to column_semantic_types
    is_nullable BOOLEAN DEFAULT TRUE,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_foreign_key BOOLEAN DEFAULT FALSE,
    
    -- Basic statistics
    total_count BIGINT,
    null_count BIGINT DEFAULT 0,
    unique_count BIGINT,
    min_value TEXT,
    max_value TEXT,
    mean_value DECIMAL(20,6),
    std_value DECIMAL(20,6),
    
    -- Advanced statistics
    median_value DECIMAL(20,6),
    mode_value TEXT,
    skewness DECIMAL(10,6),
    kurtosis DECIMAL(10,6),
    
    -- String/categorical specific
    min_length INTEGER,
    max_length INTEGER,
    avg_length DECIMAL(10,2),
    top_values JSONB,                          -- {value: count} pairs
    
    -- Temporal specific (for date/timestamp columns)
    earliest_date TIMESTAMP WITH TIME ZONE,
    latest_date TIMESTAMP WITH TIME ZONE,
    date_range_days INTEGER,
    
    -- Quality metrics
    completeness_ratio DECIMAL(5,4),           -- (total_count - null_count) / total_count
    cardinality_ratio DECIMAL(10,8),           -- unique_count / total_count
    anomaly_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(dataset_id, column_name)
);

-- Pre-computed histogram statistics - TFDV-inspired storage
CREATE TABLE IF NOT EXISTS dev_dataset_column_stats (
    id SERIAL PRIMARY KEY,
    dataset_column_id INTEGER REFERENCES dev_dataset_columns(id) ON DELETE CASCADE,
    
    -- Histogram type and parameters
    histogram_type VARCHAR(50) NOT NULL CHECK (histogram_type IN ('standard', 'quantile', 'categorical')),
    bin_count INTEGER NOT NULL,
    
    -- Histogram data (stored as JSONB for flexibility)
    histogram_bins JSONB NOT NULL,             -- [{bin_start, bin_end, count, frequency}]
    
    -- Computation metadata
    sample_size BIGINT,
    computation_method VARCHAR(50),            -- 'full_scan', 'reservoir_sampling', 'ray_distributed'
    computation_time_seconds DECIMAL(10,3),
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Cache control
    cache_version VARCHAR(50) DEFAULT '1.0',
    expires_at TIMESTAMP WITH TIME ZONE,
    
    UNIQUE(dataset_column_id, histogram_type)
);

-- Create indexes for performance (only if they don't exist)
DO $$
BEGIN
    -- Indexes for dev_datasets
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_datasets' AND indexname = 'idx_datasets_name') THEN
        CREATE INDEX idx_datasets_name ON dev_datasets(name);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_datasets' AND indexname = 'idx_datasets_type') THEN
        CREATE INDEX idx_datasets_type ON dev_datasets(dataset_type);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_datasets' AND indexname = 'idx_datasets_table_name') THEN
        CREATE INDEX idx_datasets_table_name ON dev_datasets(table_name) WHERE table_name IS NOT NULL;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_datasets' AND indexname = 'idx_datasets_stats_computed') THEN
        CREATE INDEX idx_datasets_stats_computed ON dev_datasets(stats_computed);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_datasets' AND indexname = 'idx_datasets_last_accessed') THEN
        CREATE INDEX idx_datasets_last_accessed ON dev_datasets(last_accessed_at);
    END IF;
    
    -- Indexes for dev_dataset_columns  
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_dataset_columns' AND indexname = 'idx_dataset_columns_dataset_id') THEN
        CREATE INDEX idx_dataset_columns_dataset_id ON dev_dataset_columns(dataset_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_dataset_columns' AND indexname = 'idx_dataset_columns_semantic_type') THEN
        CREATE INDEX idx_dataset_columns_semantic_type ON dev_dataset_columns(semantic_type);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_dataset_columns' AND indexname = 'idx_dataset_columns_data_type') THEN
        CREATE INDEX idx_dataset_columns_data_type ON dev_dataset_columns(data_type);
    END IF;
    
    -- Indexes for dev_dataset_column_stats
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_dataset_column_stats' AND indexname = 'idx_column_stats_column_id') THEN
        CREATE INDEX idx_column_stats_column_id ON dev_dataset_column_stats(dataset_column_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_dataset_column_stats' AND indexname = 'idx_column_stats_type') THEN
        CREATE INDEX idx_column_stats_type ON dev_dataset_column_stats(histogram_type);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'dev_dataset_column_stats' AND indexname = 'idx_column_stats_computed_at') THEN
        CREATE INDEX idx_column_stats_computed_at ON dev_dataset_column_stats(computed_at);
    END IF;
END $$;

-- Create update trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic timestamp updates
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.triggers WHERE trigger_name = 'update_dev_datasets_updated_at') THEN
        CREATE TRIGGER update_dev_datasets_updated_at
            BEFORE UPDATE ON dev_datasets
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.triggers WHERE trigger_name = 'update_dev_dataset_columns_updated_at') THEN
        CREATE TRIGGER update_dev_dataset_columns_updated_at
            BEFORE UPDATE ON dev_dataset_columns
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Add comments for documentation
COMMENT ON TABLE dev_datasets IS 'Unified metadata for all dataset types (tables, files, training data) - Migration 045';
COMMENT ON TABLE dev_dataset_columns IS 'Detailed column metadata and statistics for all datasets - Migration 045';  
COMMENT ON TABLE dev_dataset_column_stats IS 'Pre-computed histogram statistics for fast EDA visualization - Migration 045';

COMMENT ON COLUMN dev_datasets.dataset_type IS 'Type: database_table, single_file, sharded_files, training_dataset';
COMMENT ON COLUMN dev_datasets.stats_computed IS 'Whether comprehensive statistics have been computed automatically';
COMMENT ON COLUMN dev_dataset_columns.semantic_type IS 'Business meaning: identifier, categorical, numeric, date, etc.';
COMMENT ON COLUMN dev_dataset_column_stats.histogram_bins IS 'JSON array of histogram bins with start, end, count, frequency';