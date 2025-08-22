-- Migration 037: Add enhanced technical indicators support to training_dataset table

-- Add columns for enhanced feature metadata
ALTER TABLE dev_training_dataset
ADD COLUMN IF NOT EXISTS feature_metadata JSONB DEFAULT '{}',  -- Store feature descriptions and metadata
ADD COLUMN IF NOT EXISTS technical_indicators JSONB DEFAULT '{}',  -- Store technical indicator configurations
ADD COLUMN IF NOT EXISTS feature_distributions JSONB DEFAULT '{}',  -- Store feature distribution data for visualization
ADD COLUMN IF NOT EXISTS ohlc_sequences JSONB DEFAULT '{}';  -- Store OHLC sequence information

-- Create index for faster JSON queries
CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_feature_metadata 
ON dev_training_dataset USING gin (feature_metadata);

CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_technical_indicators 
ON dev_training_dataset USING gin (technical_indicators);

-- Add comments for documentation
COMMENT ON COLUMN dev_training_dataset.feature_metadata IS 'JSON metadata describing each feature including name, description, type, and statistics';
COMMENT ON COLUMN dev_training_dataset.technical_indicators IS 'JSON configuration for technical indicators used (etop, ebot, pldot, oneonedot, etc.)';
COMMENT ON COLUMN dev_training_dataset.feature_distributions IS 'JSON data containing feature value distributions for visualization';
COMMENT ON COLUMN dev_training_dataset.ohlc_sequences IS 'JSON data containing OHLC sequence information and sample data';

-- Update the summary view to include enhanced features
DROP VIEW IF EXISTS dev_training_dataset_summary;

CREATE VIEW dev_training_dataset_summary AS
SELECT 
    td.id,
    td.dataset_name,
    td.run_id,
    td.creation_timestamp,
    td.total_sequences,
    td.sequence_length,
    td.feature_count,
    td.label_count,
    td.symbols,
    td.date_range_start,
    td.date_range_end,
    td.data_quality_score,
    td.file_size_mb,
    td.status,
    td.feature_metadata,
    td.technical_indicators,
    
    -- Run information
    r.run_type,
    r.start_time as run_start_time,
    r.end_time as run_end_time,
    r.status as run_status,
    r.quality_summary as run_quality,
    r.performance_summary as run_performance,
    
    -- Enhanced metadata
    COALESCE(jsonb_array_length(td.symbols), 0) as symbol_count,
    CASE 
        WHEN td.technical_indicators IS NOT NULL AND td.technical_indicators != '{}' 
        THEN true 
        ELSE false 
    END as has_technical_indicators,
    CASE 
        WHEN td.feature_metadata IS NOT NULL AND td.feature_metadata != '{}' 
        THEN jsonb_object_keys(td.feature_metadata)
        ELSE NULL 
    END as feature_names
    
FROM dev_training_dataset td
LEFT JOIN dev_runs r ON td.run_id = r.id
ORDER BY td.creation_timestamp DESC;

-- Grant permissions
GRANT SELECT ON dev_training_dataset_summary TO PUBLIC;

COMMENT ON VIEW dev_training_dataset_summary IS 'Enhanced summary view of training datasets with technical indicators and feature metadata';