-- Migration 042: Create dev_runs table for analytics service job tracking
-- This fixes the "relation dev_runs does not exist" error in analytics service

-- Create the dev_runs table for job tracking
CREATE TABLE IF NOT EXISTS dev_runs (
    id SERIAL PRIMARY KEY,
    run_type VARCHAR(100) NOT NULL DEFAULT 'analytics_job',
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    created_by VARCHAR(100) DEFAULT 'system',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    error_message TEXT,
    parameters JSONB DEFAULT '{}'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_runs_status ON dev_runs(status);
CREATE INDEX IF NOT EXISTS idx_dev_runs_type ON dev_runs(run_type);
CREATE INDEX IF NOT EXISTS idx_dev_runs_created_at ON dev_runs(created_at DESC);

-- Add some sample data for testing (optional)
INSERT INTO dev_runs (run_type, status, start_time, end_time, created_by, parameters) VALUES 
('analytics_job', 'completed', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour', 'system', '{"test": "data"}'),
('data_processing', 'running', NOW() - INTERVAL '30 minutes', NULL, 'user1', '{"symbols": ["AAPL", "MSFT"]}'),
('training_job', 'failed', NOW() - INTERVAL '1 day', NOW() - INTERVAL '23 hours', 'user2', '{"model": "transformer"}')
ON CONFLICT DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE dev_runs IS 'Job tracking table for analytics service to display job history and status';
COMMENT ON COLUMN dev_runs.run_type IS 'Type of job/run being tracked (analytics_job, data_processing, training_job, etc.)';
COMMENT ON COLUMN dev_runs.status IS 'Current status of the job (pending, running, completed, failed)';
COMMENT ON COLUMN dev_runs.parameters IS 'JSON parameters used for the job execution';