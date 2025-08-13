-- Migration 027: Add start_date_time and end_date_time columns to instrument_interval table

-- Add start_date_time column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'instrument_interval' AND column_name = 'start_date_time'
    ) THEN
        ALTER TABLE instrument_interval ADD COLUMN start_date_time TIMESTAMPTZ;
    END IF;
END $$;

-- Add end_date_time column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'instrument_interval' AND column_name = 'end_date_time'
    ) THEN
        ALTER TABLE instrument_interval ADD COLUMN end_date_time TIMESTAMPTZ;
    END IF;
END $$;
