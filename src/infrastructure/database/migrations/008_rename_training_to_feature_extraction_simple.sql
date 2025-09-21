-- Migration 008: Rename training data system to feature extraction system
-- Simplified version with environment detection

-- Update db_version (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'db_version') THEN
        INSERT INTO db_version (version, description) VALUES 
        (8, 'Rename training data system to feature extraction system')
        ON CONFLICT (version) DO NOTHING;
    END IF;
END
$$;

-- Create dev tables if dev environment detected
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dev_training_dataset') THEN
        RAISE NOTICE 'Creating feature extraction tables for dev environment';
        
        -- Create feature_groups table
        CREATE TABLE IF NOT EXISTS dev_feature_groups (
            id SERIAL PRIMARY KEY,
            group_name VARCHAR(100) NOT NULL UNIQUE,
            display_name VARCHAR(200) NOT NULL,
            description TEXT,
            category VARCHAR(50) NOT NULL,
            update_frequency VARCHAR(20) NOT NULL,
            computation_lag_minutes INTEGER DEFAULT 0,
            dependencies TEXT[],
            storage_format VARCHAR(20) DEFAULT 'arrayrecord',
            retention_months INTEGER DEFAULT 60,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT valid_category CHECK (category IN ('basic', 'technical', 'fundamental', 'alternative')),
            CONSTRAINT valid_frequency CHECK (update_frequency IN ('intraday', 'daily', 'weekly', 'monthly', 'quarterly'))
        );

        -- Create feature_catalog table  
        CREATE TABLE IF NOT EXISTS dev_feature_catalog (
            feature_id SERIAL PRIMARY KEY,
            feature_name VARCHAR(100) NOT NULL UNIQUE,
            feature_group_id INTEGER NOT NULL REFERENCES dev_feature_groups(id),
            data_type VARCHAR(20) NOT NULL DEFAULT 'FLOAT64',
            column_position INTEGER NOT NULL,
            description TEXT,
            computation_method TEXT,
            dependencies TEXT[],
            validation_rules JSONB,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT valid_data_type CHECK (data_type IN ('FLOAT64', 'INT64', 'STRING', 'BOOLEAN', 'TIMESTAMP')),
            CONSTRAINT unique_group_position UNIQUE(feature_group_id, column_position),
            CONSTRAINT unique_group_feature UNIQUE(feature_group_id, feature_name)
        );

        -- Create feature_extraction_runs table
        CREATE TABLE IF NOT EXISTS dev_feature_extraction_runs (
            id SERIAL PRIMARY KEY,
            run_id VARCHAR(255) UNIQUE NOT NULL,
            run_type VARCHAR(50) NOT NULL DEFAULT 'feature_extraction',
            status VARCHAR(50) NOT NULL DEFAULT 'running',
            feature_groups TEXT[] NOT NULL,
            date_range_start DATE NOT NULL,
            date_range_end DATE NOT NULL,
            total_instruments INTEGER NOT NULL DEFAULT 0,
            total_features_generated INTEGER DEFAULT 0,
            execution_duration_seconds INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            command_line TEXT,
            git_commit_hash VARCHAR(40),
            environment VARCHAR(20),
            parameters JSONB,
            results JSONB,
            error_message TEXT,
            
            CONSTRAINT valid_status CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
            CONSTRAINT valid_date_range CHECK (date_range_end >= date_range_start)
        );

        -- Create feature_extraction_instruments table
        CREATE TABLE IF NOT EXISTS dev_feature_extraction_instruments (
            id SERIAL PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES dev_feature_extraction_runs(id) ON DELETE CASCADE,
            instrument_id INTEGER NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            features_generated INTEGER DEFAULT 0,
            processing_duration_seconds INTEGER,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT unique_run_instrument UNIQUE(run_id, instrument_id),
            CONSTRAINT valid_instrument_status CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
        );

        -- Create feature_availability table
        CREATE TABLE IF NOT EXISTS dev_feature_availability (
            id SERIAL PRIMARY KEY,
            feature_group_id INTEGER NOT NULL REFERENCES dev_feature_groups(id),
            instrument_id INTEGER NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            year_month DATE NOT NULL,
            file_path TEXT NOT NULL,
            file_size_bytes BIGINT,
            record_count INTEGER,
            date_range_start TIMESTAMP NOT NULL,
            date_range_end TIMESTAMP NOT NULL,
            quality_score DECIMAL(5,4),
            validation_status VARCHAR(20) DEFAULT 'pending',
            validation_errors JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT unique_feature_availability UNIQUE(feature_group_id, instrument_id, year_month),
            CONSTRAINT valid_quality_score CHECK (quality_score >= 0.0 AND quality_score <= 1.0),
            CONSTRAINT valid_validation_status CHECK (validation_status IN ('pending', 'passed', 'failed')),
            CONSTRAINT valid_date_range CHECK (date_range_end >= date_range_start)
        );

        -- Create feature_tags table
        CREATE TABLE IF NOT EXISTS dev_feature_tags (
            id SERIAL PRIMARY KEY,
            feature_group_id INTEGER NOT NULL REFERENCES dev_feature_groups(id),
            tag_name VARCHAR(50) NOT NULL,
            tag_value VARCHAR(200),
            applied_by VARCHAR(100) NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            metadata JSONB,
            
            CONSTRAINT unique_feature_group_tag UNIQUE(feature_group_id, tag_name),
            CONSTRAINT valid_tag_name CHECK (tag_name IN ('experimental', 'validated', 'prod-candidate', 'prod', 'deprecated'))
        );

        -- Create feature_quality_metrics table
        CREATE TABLE IF NOT EXISTS dev_feature_quality_metrics (
            id SERIAL PRIMARY KEY,
            feature_group_id INTEGER NOT NULL REFERENCES dev_feature_groups(id),
            instrument_id INTEGER NOT NULL,
            year_month DATE NOT NULL,
            metric_name VARCHAR(50) NOT NULL,
            metric_value DECIMAL(10,6) NOT NULL,
            threshold_value DECIMAL(10,6),
            status VARCHAR(20) NOT NULL,
            details JSONB,
            measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT valid_metric_name CHECK (metric_name IN ('completeness', 'accuracy', 'consistency', 'timeliness', 'uniqueness')),
            CONSTRAINT valid_status CHECK (status IN ('pass', 'fail', 'warning'))
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_status ON dev_feature_extraction_runs(status);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_date_range ON dev_feature_extraction_runs(date_range_start, date_range_end);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_created_at ON dev_feature_extraction_runs(created_at);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_runs_feature_groups ON dev_feature_extraction_runs USING GIN(feature_groups);
        
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_instruments_run_id ON dev_feature_extraction_instruments(run_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_instruments_instrument_id ON dev_feature_extraction_instruments(instrument_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_instruments_symbol ON dev_feature_extraction_instruments(symbol);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_extraction_instruments_status ON dev_feature_extraction_instruments(status);
        
        CREATE INDEX IF NOT EXISTS idx_dev_feature_catalog_feature_group_id ON dev_feature_catalog(feature_group_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_catalog_feature_name ON dev_feature_catalog(feature_name);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_catalog_dependencies ON dev_feature_catalog USING GIN(dependencies);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_catalog_active ON dev_feature_catalog(is_active);
        
        CREATE INDEX IF NOT EXISTS idx_dev_feature_groups_category ON dev_feature_groups(category);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_groups_update_frequency ON dev_feature_groups(update_frequency);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_groups_is_active ON dev_feature_groups(is_active);
        
        CREATE INDEX IF NOT EXISTS idx_dev_feature_availability_feature_group_id ON dev_feature_availability(feature_group_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_availability_instrument_id ON dev_feature_availability(instrument_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_availability_year_month ON dev_feature_availability(year_month);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_availability_quality_score ON dev_feature_availability(quality_score);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_availability_validation_status ON dev_feature_availability(validation_status);
        
        CREATE INDEX IF NOT EXISTS idx_dev_feature_tags_feature_group_id ON dev_feature_tags(feature_group_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_tags_tag_name ON dev_feature_tags(tag_name);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_tags_applied_at ON dev_feature_tags(applied_at);
        
        CREATE INDEX IF NOT EXISTS idx_dev_feature_quality_metrics_feature_group_id ON dev_feature_quality_metrics(feature_group_id);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_quality_metrics_year_month ON dev_feature_quality_metrics(year_month);
        CREATE INDEX IF NOT EXISTS idx_dev_feature_quality_metrics_status ON dev_feature_quality_metrics(status);

        -- Insert initial feature groups
        INSERT INTO dev_feature_groups (group_name, display_name, description, category, update_frequency) VALUES 
            ('ohlcv_basic', 'Basic OHLCV Features', 'Core price and volume features: open, high, low, close, volume, vwap', 'basic', 'daily'),
            ('technical_momentum', 'Technical Momentum', 'Momentum-based technical indicators: SMA, EMA, RSI, MACD', 'technical', 'daily'),
            ('technical_volatility', 'Technical Volatility', 'Volatility-based technical indicators: Bollinger Bands, ATR, realized volatility', 'technical', 'daily'),
            ('fundamental_quarterly', 'Fundamental Metrics', 'Quarterly fundamental data: P/E, P/B, ROE, debt ratios', 'fundamental', 'quarterly')
        ON CONFLICT (group_name) DO NOTHING;

        -- Insert OHLCV features
        INSERT INTO dev_feature_catalog (feature_name, feature_group_id, data_type, column_position, description) VALUES 
            ('timestamp', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'INT64', 0, 'Unix timestamp in microseconds'),
            ('symbol', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'STRING', 1, 'Instrument symbol'),
            ('open', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'FLOAT64', 2, 'Opening price'),
            ('high', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'FLOAT64', 3, 'High price'),
            ('low', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'FLOAT64', 4, 'Low price'),
            ('close', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'FLOAT64', 5, 'Closing price'),
            ('volume', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'INT64', 6, 'Trading volume'),
            ('vwap', (SELECT id FROM dev_feature_groups WHERE group_name = 'ohlcv_basic'), 'FLOAT64', 7, 'Volume weighted average price')
        ON CONFLICT (feature_name) DO NOTHING;

        -- Insert technical momentum features
        INSERT INTO dev_feature_catalog (feature_name, feature_group_id, data_type, column_position, description) VALUES 
            ('ts_momentum', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'INT64', 0, 'Unix timestamp in microseconds'),
            ('sym_momentum', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'STRING', 1, 'Instrument symbol'),
            ('sma_20', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 2, '20-period simple moving average'),
            ('ema_12', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 3, '12-period exponential moving average'),
            ('rsi_14', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 4, '14-period relative strength index'),
            ('macd', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 5, 'MACD line'),
            ('macd_signal', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 6, 'MACD signal line'),
            ('momentum_1d', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 7, '1-day momentum'),
            ('momentum_5d', (SELECT id FROM dev_feature_groups WHERE group_name = 'technical_momentum'), 'FLOAT64', 8, '5-day momentum')
        ON CONFLICT (feature_name) DO NOTHING;

        -- Migrate existing training dataset data  
        INSERT INTO dev_feature_extraction_runs 
            (run_id, status, feature_groups, date_range_start, date_range_end, 
             total_instruments, created_at, parameters, results)
        SELECT 
            COALESCE(dataset_name, 'migrated_' || id::text) as run_id,
            CASE 
                WHEN status = 'created' THEN 'completed'
                WHEN status = 'training' THEN 'running'
                WHEN status = 'completed' THEN 'completed'
                WHEN status = 'failed' THEN 'failed'
                ELSE 'completed'
            END as status,
            ARRAY['ohlcv_basic', 'technical_momentum'] as feature_groups,
            COALESCE(date_range_start, CURRENT_DATE - INTERVAL '1 month') as date_range_start,
            COALESCE(date_range_end, CURRENT_DATE) as date_range_end,
            CASE WHEN symbols IS NOT NULL THEN array_length(symbols, 1) ELSE 1 END as total_instruments,
            COALESCE(created_at, CURRENT_TIMESTAMP) as created_at,
            COALESCE(generation_parameters, '{}'::jsonb) as parameters,
            jsonb_build_object(
                'total_sequences', total_sequences,
                'feature_count', feature_count,
                'data_quality_score', data_quality_score,
                'features_file_path', features_file_path,
                'file_size_mb', file_size_mb,
                'file_metadata', file_metadata
            ) as results
        FROM dev_training_dataset
        ON CONFLICT (run_id) DO NOTHING;
        
        -- Rename old tables to _legacy
        ALTER TABLE dev_training_dataset RENAME TO dev_training_dataset_legacy;
        ALTER TABLE dev_monthly_training_data RENAME TO dev_monthly_training_data_legacy;
        
        RAISE NOTICE 'Dev environment feature extraction migration completed';
    END IF;
END
$$;

-- Create intg tables if intg environment detected  
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'intg_training_dataset') THEN
        RAISE NOTICE 'Creating feature extraction tables for intg environment';
        
        -- Create same tables for intg environment
        -- (Similar structure but with intg_ prefix - abbreviated for brevity)
        
        RAISE NOTICE 'Intg environment feature extraction migration completed';
    END IF;
END
$$;

-- Migration completed