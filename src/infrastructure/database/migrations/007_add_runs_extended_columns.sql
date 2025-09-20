-- Migration 007: Add extended columns to runs table for enhanced run metadata tracking
-- This migration adds columns that RunMetadataTracker expects but were missing from the original schema.
-- These columns enable comprehensive tracking of run context, environment, and reproducibility metadata.

-- Add missing columns to runs table
ALTER TABLE runs ADD COLUMN IF NOT EXISTS command_line TEXT DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS git_commit_hash VARCHAR(64) DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS git_branch VARCHAR(100) DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS environment VARCHAR(50) DEFAULT 'dev';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS host_info JSONB DEFAULT '{}';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS working_directory VARCHAR(500) DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS python_version VARCHAR(50) DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS dependencies_hash VARCHAR(64) DEFAULT '';

-- Update existing columns to match RunMetadataTracker expectations
ALTER TABLE runs ALTER COLUMN created_by SET DEFAULT 'system';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS created_by VARCHAR(100) DEFAULT 'system';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS parameters JSONB DEFAULT '{}';

-- Add indexes for performance on new columns
CREATE INDEX IF NOT EXISTS idx_runs_environment ON runs(environment);
CREATE INDEX IF NOT EXISTS idx_runs_git_branch ON runs(git_branch);
CREATE INDEX IF NOT EXISTS idx_runs_git_commit ON runs(git_commit_hash);