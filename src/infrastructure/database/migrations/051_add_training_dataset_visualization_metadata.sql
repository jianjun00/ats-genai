-- Migration 051: Add Training Dataset Visualization Metadata
-- Adds essential metadata fields for proper frontend visualization handling

-- Add visualization and data format metadata to training datasets table
ALTER TABLE dev_training_dataset 
ADD COLUMN IF NOT EXISTS data_format VARCHAR(50) DEFAULT 'numpy_sequences',
ADD COLUMN IF NOT EXISTS sequence_length INTEGER DEFAULT 60,
ADD COLUMN IF NOT EXISTS time_resolution VARCHAR(50) DEFAULT 'daily',
ADD COLUMN IF NOT EXISTS visualization_type VARCHAR(50) DEFAULT 'sequence_window',
ADD COLUMN IF NOT EXISTS time_step_unit VARCHAR(20) DEFAULT 'time_step',
ADD COLUMN IF NOT EXISTS is_time_series BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS window_size INTEGER DEFAULT 21,
ADD COLUMN IF NOT EXISTS feature_metadata JSONB DEFAULT '{}';

-- Add helpful comments
COMMENT ON COLUMN dev_training_dataset.data_format IS 'Data format type: csv_time_series, numpy_sequences, parquet, etc.';
COMMENT ON COLUMN dev_training_dataset.sequence_length IS 'Number of time steps per sequence (1 for time series, 60+ for sequences)';
COMMENT ON COLUMN dev_training_dataset.time_resolution IS 'Time resolution: minute, hourly, daily, weekly, monthly';
COMMENT ON COLUMN dev_training_dataset.visualization_type IS 'Visualization type: time_series, sequence_window, candlestick';
COMMENT ON COLUMN dev_training_dataset.time_step_unit IS 'What each step represents: hour, day, minute, time_step';
COMMENT ON COLUMN dev_training_dataset.is_time_series IS 'True if data is continuous time series, false if sequences';
COMMENT ON COLUMN dev_training_dataset.window_size IS 'Default window size for visualization (21 for 21-row window)';
COMMENT ON COLUMN dev_training_dataset.feature_metadata IS 'JSON metadata about features, OHLC columns, technical indicators';

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_data_format 
ON dev_training_dataset(data_format);

CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_visualization_type 
ON dev_training_dataset(visualization_type);

CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_time_resolution 
ON dev_training_dataset(time_resolution);

-- Create INTG environment equivalent (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_training_dataset') THEN
        ALTER TABLE intg_training_dataset 
        ADD COLUMN IF NOT EXISTS data_format VARCHAR(50) DEFAULT 'numpy_sequences',
        ADD COLUMN IF NOT EXISTS sequence_length INTEGER DEFAULT 60,
        ADD COLUMN IF NOT EXISTS time_resolution VARCHAR(50) DEFAULT 'daily',
        ADD COLUMN IF NOT EXISTS visualization_type VARCHAR(50) DEFAULT 'sequence_window',
        ADD COLUMN IF NOT EXISTS time_step_unit VARCHAR(20) DEFAULT 'time_step',
        ADD COLUMN IF NOT EXISTS is_time_series BOOLEAN DEFAULT false,
        ADD COLUMN IF NOT EXISTS window_size INTEGER DEFAULT 21,
        ADD COLUMN IF NOT EXISTS feature_metadata JSONB DEFAULT '{}';
        
        -- Create indexes for INTG
        CREATE INDEX IF NOT EXISTS idx_intg_training_dataset_data_format 
        ON intg_training_dataset(data_format);
        
        CREATE INDEX IF NOT EXISTS idx_intg_training_dataset_visualization_type 
        ON intg_training_dataset(visualization_type);
    END IF;
END $$;

-- Update existing datasets with proper metadata based on detection heuristics
UPDATE dev_training_dataset SET 
    data_format = CASE 
        WHEN dataset_name LIKE '%hourly%' THEN 'csv_time_series'
        WHEN dataset_name LIKE '%daily%' THEN 'csv_time_series' 
        WHEN dataset_name LIKE '%minute%' THEN 'csv_time_series'
        ELSE 'numpy_sequences'
    END,
    sequence_length = CASE 
        WHEN dataset_name LIKE '%hourly%' THEN 1
        WHEN dataset_name LIKE '%daily%' THEN 1
        WHEN dataset_name LIKE '%minute%' THEN 1
        ELSE 60
    END,
    time_resolution = CASE
        WHEN dataset_name LIKE '%hourly%' THEN 'hourly'
        WHEN dataset_name LIKE '%daily%' THEN 'daily'
        WHEN dataset_name LIKE '%minute%' THEN 'minute'
        ELSE 'daily'
    END,
    visualization_type = CASE
        WHEN dataset_name LIKE '%hourly%' THEN 'time_series'
        WHEN dataset_name LIKE '%daily%' THEN 'time_series'
        WHEN dataset_name LIKE '%minute%' THEN 'time_series'
        ELSE 'sequence_window'
    END,
    time_step_unit = CASE
        WHEN dataset_name LIKE '%hourly%' THEN 'hour'
        WHEN dataset_name LIKE '%daily%' THEN 'day'
        WHEN dataset_name LIKE '%minute%' THEN 'minute'
        ELSE 'time_step'
    END,
    is_time_series = CASE
        WHEN dataset_name LIKE '%hourly%' THEN true
        WHEN dataset_name LIKE '%daily%' THEN true
        WHEN dataset_name LIKE '%minute%' THEN true
        ELSE false
    END,
    feature_metadata = CASE
        WHEN dataset_name LIKE '%hourly%' OR dataset_name LIKE '%daily%' THEN 
            '{"ohlc_columns": {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"}, "technical_indicators": ["sma_20", "ema_12", "ema_26", "etop", "ebot", "pldot"], "datetime_column": "datetime"}'::jsonb
        ELSE '{}'::jsonb
    END
WHERE TRUE; -- Update all existing records

-- Create view for easy visualization metadata access
CREATE OR REPLACE VIEW dev_training_dataset_viz_metadata AS
SELECT 
    id,
    dataset_name,
    data_format,
    sequence_length,
    time_resolution,
    visualization_type,
    time_step_unit,
    is_time_series,
    window_size,
    total_sequences,
    feature_count,
    technical_indicators,
    feature_metadata,
    date_range_start,
    date_range_end,
    creation_timestamp,
    CASE 
        WHEN is_time_series THEN total_sequences 
        ELSE CEIL(total_sequences::float / sequence_length)
    END as max_sequence_index,
    CASE
        WHEN is_time_series THEN time_step_unit || ' ' || (window_size::text || '-' || time_step_unit || ' window')
        ELSE 'Sequence ' || (window_size::text || '-row window')
    END as display_format
FROM dev_training_dataset
ORDER BY creation_timestamp DESC;

-- Create similar view for INTG if table exists
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_training_dataset') THEN
        CREATE OR REPLACE VIEW intg_training_dataset_viz_metadata AS
        SELECT 
            id,
            dataset_name,
            data_format,
            sequence_length,
            time_resolution,
            visualization_type,
            time_step_unit,
            is_time_series,
            window_size,
            total_sequences,
            feature_count,
            technical_indicators,
            feature_metadata,
            date_range_start,
            date_range_end,
            creation_timestamp,
            CASE 
                WHEN is_time_series THEN total_sequences 
                ELSE CEIL(total_sequences::float / sequence_length)
            END as max_sequence_index,
            CASE
                WHEN is_time_series THEN time_step_unit || ' ' || (window_size::text || '-' || time_step_unit || ' window')
                ELSE 'Sequence ' || (window_size::text || '-row window')
            END as display_format
        FROM intg_training_dataset
        ORDER BY creation_timestamp DESC;
    END IF;
END $$;