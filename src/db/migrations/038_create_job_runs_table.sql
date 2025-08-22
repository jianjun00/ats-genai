-- Migration 038: Create job_runs table for job management functionality
-- This addresses the "relation dev_job_runs does not exist" error

-- Create enums for job types and statuses
CREATE TYPE job_type_enum AS ENUM (
    'training_data_gen',
    'training', 
    'backtest',
    'model_evaluation',
    'data_ingestion',
    'universe_update'
);

CREATE TYPE job_status_enum AS ENUM (
    'pending',
    'running', 
    'succeeded',
    'failed',
    'cancelled',
    'timeout'
);

-- Create the job_runs table (will be prefixed as dev_job_runs in dev environment)
CREATE TABLE job_runs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name VARCHAR(255) NOT NULL,
    job_type job_type_enum NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    flyte_execution_id VARCHAR(255) UNIQUE,
    status job_status_enum NOT NULL DEFAULT 'pending',
    parameters JSONB NOT NULL DEFAULT '{}',
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_seconds INTEGER,
    error_message TEXT,
    resource_usage JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_job_runs_status ON job_runs(status);
CREATE INDEX idx_job_runs_type ON job_runs(job_type);
CREATE INDEX idx_job_runs_user_id ON job_runs(user_id);
CREATE INDEX idx_job_runs_created_at ON job_runs(created_at DESC);
CREATE INDEX idx_job_runs_flyte_execution_id ON job_runs(flyte_execution_id) WHERE flyte_execution_id IS NOT NULL;

-- Create trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_job_runs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_job_runs_updated_at
    BEFORE UPDATE ON job_runs
    FOR EACH ROW
    EXECUTE FUNCTION update_job_runs_updated_at();

-- Insert some initial sample data for testing (optional)
INSERT INTO job_runs (job_name, job_type, user_id, status, parameters) VALUES
('Sample Training Data Generation', 'training_data_gen', 'system', 'succeeded', '{"symbols": ["AAPL", "MSFT"], "date_range": "2024-01-01 to 2024-08-21"}'),
('Model Training Job', 'training', 'system', 'succeeded', '{"model_type": "TFT", "dataset": "enhanced_dataset_001"}'),
('Portfolio Backtest', 'backtest', 'system', 'running', '{"strategy": "mean_reversion", "universe": "sp500_large_cap"}');

-- Update the job statistics for the sample data
UPDATE job_runs SET 
    start_time = created_at,
    end_time = created_at + INTERVAL '2 hours',
    duration_seconds = 7200
WHERE status = 'succeeded';

UPDATE job_runs SET 
    start_time = created_at
WHERE status = 'running';