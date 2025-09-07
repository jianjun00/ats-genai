-- Migration to add unified training dataset structure support
-- Adds run_id, dataset_path, and symbol_files columns to existing training datasets tables
--
-- UNIFIED TRAINING DATASET STRUCTURE:
-- - File pattern: /mnt/d/ats-data/training/<run_id>/<symbol>/<startdatetime>_<enddatetime>.riegeli
-- - Example: Dataset ID 23 with run_20250901_193706 containing:
--   * AAPL: 79 sequences in /AAPL/20250131_000000_20250827_000000.riegeli
--   * TSLA: 90 sequences in /TSLA/20250128_000000_20250901_000000.riegeli
-- - symbol_files JSONB tracks file paths for each symbol in the dataset
-- - Per-symbol file table enables detailed tracking and management

-- Add new columns to dev_training_datasets
ALTER TABLE dev_training_datasets 
ADD COLUMN IF NOT EXISTS run_id VARCHAR(255) DEFAULT '',
ADD COLUMN IF NOT EXISTS dataset_path VARCHAR(500) DEFAULT '',
ADD COLUMN IF NOT EXISTS symbol_files JSONB DEFAULT '{}';

-- Create per-symbol file tracking table for dev environment
CREATE TABLE IF NOT EXISTS dev_training_dataset_files (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES dev_training_datasets(id) ON DELETE CASCADE,
    symbol VARCHAR(10) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP NOT NULL,
    sequence_count INTEGER DEFAULT 0,
    file_size_bytes BIGINT DEFAULT 0,
    data_quality_score DECIMAL(5,4) DEFAULT 0.0000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(dataset_id, symbol)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_run_id ON dev_training_datasets(run_id);
CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_files_symbol ON dev_training_dataset_files(symbol);
CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_files_dataset_id ON dev_training_dataset_files(dataset_id);

-- Update existing records to have empty defaults for new fields
UPDATE dev_training_datasets 
SET dataset_path = COALESCE(dataset_path, ''),
    symbol_files = COALESCE(symbol_files, '{}')
WHERE dataset_path IS NULL OR symbol_files IS NULL OR symbol_files::text = 'null';

-- Add comments for documentation
COMMENT ON COLUMN dev_training_datasets.run_id IS 'Training run identifier for organizing datasets';
COMMENT ON COLUMN dev_training_datasets.dataset_path IS 'Base path: /mnt/d/ats-data/training/<run_id>/';
COMMENT ON COLUMN dev_training_datasets.symbol_files IS 'JSON mapping of symbols to their Riegeli file paths';
COMMENT ON TABLE dev_training_dataset_files IS 'Per-symbol file tracking for unified training datasets';