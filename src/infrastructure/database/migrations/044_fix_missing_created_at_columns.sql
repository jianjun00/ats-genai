-- Migration 044: Fix remaining missing created_at columns
-- Handle tables that already have updated_at but missing created_at

-- Add created_at to dev_realtime_collection_status (already has updated_at)
ALTER TABLE dev_realtime_collection_status 
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Add created_at to dev_realtime_gaps (already has updated_at)  
ALTER TABLE dev_realtime_gaps 
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

-- Special case: db_version table should be managed by migration system
-- Only add if this is the prefixed version used in dev environment
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_db_version') THEN
        ALTER TABLE dev_db_version 
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
    END IF;
END
$$;

-- Add indexes for the new created_at columns  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_realtime_collection_status_created_at ON dev_realtime_collection_status(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_realtime_gaps_created_at ON dev_realtime_gaps(created_at);

-- Add indexes for db_version if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_db_version') THEN
        EXECUTE 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dev_db_version_created_at ON dev_db_version(created_at)';
    END IF;
END
$$;

-- Add triggers for automatic updated_at maintenance (only if table doesn't already have trigger)
DO $$
BEGIN
    -- Check and add trigger for dev_realtime_collection_status
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_dev_realtime_collection_status_updated_at') THEN
        CREATE TRIGGER update_dev_realtime_collection_status_updated_at 
            BEFORE UPDATE ON dev_realtime_collection_status 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    -- Check and add trigger for dev_realtime_gaps  
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_dev_realtime_gaps_updated_at') THEN
        CREATE TRIGGER update_dev_realtime_gaps_updated_at 
            BEFORE UPDATE ON dev_realtime_gaps 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    -- Check and add trigger for dev_db_version if table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_db_version') AND 
       NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_dev_db_version_updated_at') THEN
        CREATE TRIGGER update_dev_db_version_updated_at 
            BEFORE UPDATE ON dev_db_version 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END
$$;