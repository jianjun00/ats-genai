-- Migration 003: Add website and api_key_env_var columns to vendors table
ALTER TABLE intg_vendors ADD COLUMN IF NOT EXISTS website TEXT;
ALTER TABLE intg_vendors ADD COLUMN IF NOT EXISTS api_key_env_var TEXT;
