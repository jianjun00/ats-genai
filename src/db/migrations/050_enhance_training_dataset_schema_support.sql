-- Migration 050: Enhanced Training Dataset Schema Support
-- Adds comprehensive schema management columns to training datasets table
-- and creates schema registry for reusable schema definitions

-- Add schema management columns to existing training_datasets table
ALTER TABLE dev_training_datasets 
ADD COLUMN IF NOT EXISTS schema_protobuf BYTEA,
ADD COLUMN IF NOT EXISTS schema_version VARCHAR(50) DEFAULT '1.0.0',
ADD COLUMN IF NOT EXISTS schema_hash VARCHAR(64),
ADD COLUMN IF NOT EXISTS schema_json JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS feature_schema JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS label_schema JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS validation_results JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS compatibility_info JSONB DEFAULT '{}';

-- Create schema registry table for reusable schemas
CREATE TABLE IF NOT EXISTS dev_training_schema_registry (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL,
    schema_version VARCHAR(50) NOT NULL,
    schema_hash VARCHAR(64) NOT NULL,
    schema_json JSONB NOT NULL,
    schema_protobuf BYTEA,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255) DEFAULT 'ATS Training System',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Categorization
    tags TEXT[] DEFAULT '{}',
    status VARCHAR(50) DEFAULT 'active', -- draft, active, deprecated
    
    -- Schema description and documentation
    description TEXT DEFAULT '',
    documentation_url TEXT DEFAULT '',
    
    -- Usage tracking
    usage_count INTEGER DEFAULT 0,
    last_used_at TIMESTAMP WITH TIME ZONE,
    
    -- Constraints
    UNIQUE(schema_name, schema_version),
    UNIQUE(schema_hash)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_training_datasets_schema_hash 
ON dev_training_datasets(schema_hash);

CREATE INDEX IF NOT EXISTS idx_training_schema_registry_name_version 
ON dev_training_schema_registry(schema_name, schema_version);

CREATE INDEX IF NOT EXISTS idx_training_schema_registry_hash 
ON dev_training_schema_registry(schema_hash);

CREATE INDEX IF NOT EXISTS idx_training_schema_registry_status 
ON dev_training_schema_registry(status);

CREATE INDEX IF NOT EXISTS idx_training_schema_registry_created_at 
ON dev_training_schema_registry(created_at);

-- Create INTG environment tables (same structure)
ALTER TABLE intg_training_datasets 
ADD COLUMN IF NOT EXISTS schema_protobuf BYTEA,
ADD COLUMN IF NOT EXISTS schema_version VARCHAR(50) DEFAULT '1.0.0',
ADD COLUMN IF NOT EXISTS schema_hash VARCHAR(64),
ADD COLUMN IF NOT EXISTS schema_json JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS feature_schema JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS label_schema JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS validation_results JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS compatibility_info JSONB DEFAULT '{}';

CREATE TABLE IF NOT EXISTS intg_training_schema_registry (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL,
    schema_version VARCHAR(50) NOT NULL,
    schema_hash VARCHAR(64) NOT NULL,
    schema_json JSONB NOT NULL,
    schema_protobuf BYTEA,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255) DEFAULT 'ATS Training System',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    tags TEXT[] DEFAULT '{}',
    status VARCHAR(50) DEFAULT 'active',
    
    description TEXT DEFAULT '',
    documentation_url TEXT DEFAULT '',
    
    usage_count INTEGER DEFAULT 0,
    last_used_at TIMESTAMP WITH TIME ZONE,
    
    UNIQUE(schema_name, schema_version),
    UNIQUE(schema_hash)
);

-- Indexes for INTG
CREATE INDEX IF NOT EXISTS idx_intg_training_datasets_schema_hash 
ON intg_training_datasets(schema_hash);

CREATE INDEX IF NOT EXISTS idx_intg_training_schema_registry_name_version 
ON intg_training_schema_registry(schema_name, schema_version);

CREATE INDEX IF NOT EXISTS idx_intg_training_schema_registry_hash 
ON intg_training_schema_registry(schema_hash);

CREATE INDEX IF NOT EXISTS idx_intg_training_schema_registry_status 
ON intg_training_schema_registry(status);

-- Add helpful comments
COMMENT ON COLUMN dev_training_datasets.schema_protobuf IS 'Binary Protocol Buffer schema definition';
COMMENT ON COLUMN dev_training_datasets.schema_version IS 'Schema version (semantic versioning)';
COMMENT ON COLUMN dev_training_datasets.schema_hash IS 'SHA256 hash of schema for integrity checking';
COMMENT ON COLUMN dev_training_datasets.schema_json IS 'Human-readable JSON schema for debugging';
COMMENT ON COLUMN dev_training_datasets.feature_schema IS 'Detailed feature schema definitions';
COMMENT ON COLUMN dev_training_datasets.label_schema IS 'Label/target schema definitions';
COMMENT ON COLUMN dev_training_datasets.validation_results IS 'Schema validation results and errors';
COMMENT ON COLUMN dev_training_datasets.compatibility_info IS 'Schema compatibility and migration info';

COMMENT ON TABLE dev_training_schema_registry IS 'Registry of reusable training dataset schemas';
COMMENT ON COLUMN dev_training_schema_registry.schema_hash IS 'Unique identifier for schema content';
COMMENT ON COLUMN dev_training_schema_registry.usage_count IS 'Number of times schema has been used';
COMMENT ON COLUMN dev_training_schema_registry.last_used_at IS 'Last time schema was used';

-- Create function to automatically update schema usage statistics
CREATE OR REPLACE FUNCTION update_schema_usage_stats()
RETURNS TRIGGER AS $$
BEGIN
    -- Update usage stats when a dataset references a schema
    IF NEW.schema_hash IS NOT NULL AND NEW.schema_hash != '' THEN
        UPDATE dev_training_schema_registry 
        SET usage_count = usage_count + 1,
            last_used_at = NOW()
        WHERE schema_hash = NEW.schema_hash;
        
        -- Also update INTG registry if exists
        UPDATE intg_training_schema_registry 
        SET usage_count = usage_count + 1,
            last_used_at = NOW()
        WHERE schema_hash = NEW.schema_hash;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to automatically update usage statistics
CREATE TRIGGER trigger_update_dev_schema_usage
    AFTER INSERT OR UPDATE OF schema_hash ON dev_training_datasets
    FOR EACH ROW
    EXECUTE FUNCTION update_schema_usage_stats();

CREATE TRIGGER trigger_update_intg_schema_usage
    AFTER INSERT OR UPDATE OF schema_hash ON intg_training_datasets
    FOR EACH ROW
    EXECUTE FUNCTION update_schema_usage_stats();

-- Create view for schema analytics
CREATE OR REPLACE VIEW dev_schema_usage_analytics AS
SELECT 
    sr.schema_name,
    sr.schema_version,
    sr.description,
    sr.status,
    sr.usage_count,
    sr.last_used_at,
    sr.created_at,
    COUNT(td.id) as active_datasets,
    ARRAY_AGG(DISTINCT td.dataset_name) FILTER (WHERE td.dataset_name IS NOT NULL) as dataset_names,
    ARRAY_AGG(DISTINCT sr.tags) as all_tags
FROM dev_training_schema_registry sr
LEFT JOIN dev_training_datasets td ON sr.schema_hash = td.schema_hash
GROUP BY sr.id, sr.schema_name, sr.schema_version, sr.description, sr.status, 
         sr.usage_count, sr.last_used_at, sr.created_at
ORDER BY sr.usage_count DESC, sr.last_used_at DESC;

-- Create similar view for INTG
CREATE OR REPLACE VIEW intg_schema_usage_analytics AS
SELECT 
    sr.schema_name,
    sr.schema_version,
    sr.description,
    sr.status,
    sr.usage_count,
    sr.last_used_at,
    sr.created_at,
    COUNT(td.id) as active_datasets,
    ARRAY_AGG(DISTINCT td.dataset_name) FILTER (WHERE td.dataset_name IS NOT NULL) as dataset_names,
    ARRAY_AGG(DISTINCT sr.tags) as all_tags
FROM intg_training_schema_registry sr
LEFT JOIN intg_training_datasets td ON sr.schema_hash = td.schema_hash
GROUP BY sr.id, sr.schema_name, sr.schema_version, sr.description, sr.status, 
         sr.usage_count, sr.last_used_at, sr.created_at
ORDER BY sr.usage_count DESC, sr.last_used_at DESC;