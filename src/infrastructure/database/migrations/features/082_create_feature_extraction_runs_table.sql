-- Migration: Create feature_extraction_runs table for tracking feature extraction jobs
-- Version: 082
-- Description: This table tracks feature extraction runs separately from generic runs table
-- Stores feature-specific metadata like feature groups, instruments, and file paths

-- Create feature_extraction_runs table dynamically for all environments
DO $$
DECLARE
    env_prefix TEXT;
    table_exists BOOLEAN;
    feature_extraction_runs_table TEXT;
    runs_table TEXT;
    create_table_sql TEXT;
    create_index_sql TEXT;
    comment_sql TEXT;
BEGIN
    -- Check all possible environment prefixes
    FOR env_prefix IN SELECT unnest(ARRAY['dev_', 'intg_', 'prod_', 'test_', ''])
    LOOP
        feature_extraction_runs_table := env_prefix || 'feature_extraction_runs';
        runs_table := env_prefix || 'runs';
        
        -- Check if runs table exists for this environment
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = runs_table
        ) INTO table_exists;
        
        -- Only create feature_extraction_runs table if corresponding runs table exists
        IF table_exists THEN
            RAISE NOTICE 'Creating table: %', feature_extraction_runs_table;
            
            -- Create the table
            create_table_sql := format('
                CREATE TABLE IF NOT EXISTS %I (
                    id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES %I(id) ON DELETE CASCADE,
                    status VARCHAR(50) DEFAULT ''running'',
                    feature_groups TEXT[] DEFAULT ''{}'',
                    date_range_start DATE,
                    date_range_end DATE,
                    total_instruments INTEGER DEFAULT 0,
                    total_features_generated INTEGER DEFAULT 0,
                    execution_duration_seconds INTEGER DEFAULT 0,
                    command_line TEXT DEFAULT '''',
                    environment VARCHAR(50) DEFAULT %L,
                    parameters JSONB DEFAULT ''{}'',
                    results JSONB DEFAULT ''{}'',
                    dataset_path VARCHAR(500) DEFAULT '''',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )', 
                feature_extraction_runs_table, 
                runs_table,
                CASE WHEN env_prefix = '' THEN 'default' ELSE rtrim(env_prefix, '_') END
            );
            EXECUTE create_table_sql;
            
            -- Create indexes
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_run_id ON %I(run_id)', 
                          replace(feature_extraction_runs_table, '_', '_'), feature_extraction_runs_table);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_status ON %I(status)', 
                          replace(feature_extraction_runs_table, '_', '_'), feature_extraction_runs_table);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_date_range ON %I(date_range_start, date_range_end)', 
                          replace(feature_extraction_runs_table, '_', '_'), feature_extraction_runs_table);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_dataset_path ON %I(dataset_path)', 
                          replace(feature_extraction_runs_table, '_', '_'), feature_extraction_runs_table);
            
            -- Add table comment
            EXECUTE format('COMMENT ON TABLE %I IS ''Tracks feature extraction runs with feature-specific metadata''', 
                          feature_extraction_runs_table);
            EXECUTE format('COMMENT ON COLUMN %I.dataset_path IS ''Actual dataset directory name (e.g., dataset_20250928_022056)''', 
                          feature_extraction_runs_table);
                          
            RAISE NOTICE 'Successfully created table and indexes: %', feature_extraction_runs_table;
        ELSE
            RAISE NOTICE 'Skipping % - runs table % does not exist', feature_extraction_runs_table, runs_table;
        END IF;
    END LOOP;
END
$$;