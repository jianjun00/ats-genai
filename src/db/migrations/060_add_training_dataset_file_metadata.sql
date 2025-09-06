-- Migration: Add file_metadata JSONB column to training datasets table
-- Purpose: Store detailed file information for each training dataset to enable
--          precise multi-symbol, multi-timeframe sequence management
-- Date: September 6, 2025

BEGIN;

-- Add file_metadata column to dev environment
ALTER TABLE dev_training_datasets 
ADD COLUMN IF NOT EXISTS file_metadata JSONB;

-- Add comment explaining the structure
COMMENT ON COLUMN dev_training_datasets.file_metadata IS 
'JSONB structure: {
  "files": [
    {
      "symbol": "AAPL",
      "timeframe": "5m", 
      "file_path": "AAPL_20250701_000000_20250906_000000.arrayrecord",
      "sequences": 643,
      "file_size_bytes": 131072,
      "created_at": "2025-09-05T18:39:00"
    }
  ],
  "total_sequences": 3216,
  "total_files": 10,
  "timeframes": ["5m", "15m", "1h", "1d", "1w"],
  "symbols": ["AAPL", "TSLA"]
}';

-- Create index for efficient querying of file metadata
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_file_metadata_gin 
ON dev_training_datasets USING GIN (file_metadata);

-- Create index for specific metadata queries
CREATE INDEX IF NOT EXISTS idx_dev_training_datasets_file_metadata_symbols
ON dev_training_datasets USING GIN ((file_metadata->'symbols'));

COMMIT;