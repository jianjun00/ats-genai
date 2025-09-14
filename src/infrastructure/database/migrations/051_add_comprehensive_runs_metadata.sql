-- Migration: Add comprehensive metadata tracking to runs table
-- Adds critical metadata columns for full traceability of training runs
-- Including git commit info, command line arguments, environment details

-- Add missing metadata columns to dev_runs table
ALTER TABLE dev_runs 
ADD COLUMN IF NOT EXISTS command_line TEXT DEFAULT '',
ADD COLUMN IF NOT EXISTS git_commit_hash VARCHAR(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS git_branch VARCHAR(100) DEFAULT '',
ADD COLUMN IF NOT EXISTS environment VARCHAR(50) DEFAULT 'dev',
ADD COLUMN IF NOT EXISTS results JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS host_info JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS working_directory VARCHAR(500) DEFAULT '',
ADD COLUMN IF NOT EXISTS python_version VARCHAR(50) DEFAULT '',
ADD COLUMN IF NOT EXISTS dependencies_hash VARCHAR(64) DEFAULT '';

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_runs_git_commit ON dev_runs(git_commit_hash);
CREATE INDEX IF NOT EXISTS idx_dev_runs_environment ON dev_runs(environment);
CREATE INDEX IF NOT EXISTS idx_dev_runs_git_branch ON dev_runs(git_branch);

-- Add comments for documentation
COMMENT ON COLUMN dev_runs.command_line IS 'Full command line used to execute the run including all arguments';
COMMENT ON COLUMN dev_runs.git_commit_hash IS 'Git commit hash of the code used for this run';
COMMENT ON COLUMN dev_runs.git_branch IS 'Git branch name at time of execution';
COMMENT ON COLUMN dev_runs.environment IS 'Environment name (dev, intg, prod)';
COMMENT ON COLUMN dev_runs.results IS 'JSON structure containing run results and metrics';
COMMENT ON COLUMN dev_runs.host_info IS 'Host system information (hostname, OS, Docker container, etc.)';
COMMENT ON COLUMN dev_runs.working_directory IS 'Working directory path where the run was executed';
COMMENT ON COLUMN dev_runs.python_version IS 'Python version used for execution';
COMMENT ON COLUMN dev_runs.dependencies_hash IS 'Hash of requirements.txt or key dependencies for reproducibility';

-- Update existing rows to have default values
UPDATE dev_runs 
SET 
    command_line = COALESCE(command_line, ''),
    git_commit_hash = COALESCE(git_commit_hash, ''),
    git_branch = COALESCE(git_branch, ''),
    environment = COALESCE(environment, 'dev'),
    results = COALESCE(results, '{}'),
    host_info = COALESCE(host_info, '{}'),
    working_directory = COALESCE(working_directory, ''),
    python_version = COALESCE(python_version, ''),
    dependencies_hash = COALESCE(dependencies_hash, '')
WHERE 
    command_line IS NULL OR 
    git_commit_hash IS NULL OR 
    git_branch IS NULL OR 
    environment IS NULL OR 
    results IS NULL OR
    host_info IS NULL OR
    working_directory IS NULL OR
    python_version IS NULL OR
    dependencies_hash IS NULL;