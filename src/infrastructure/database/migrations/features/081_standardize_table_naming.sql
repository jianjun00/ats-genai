-- Migration: Standardize table naming conventions (plural to singular)
-- Purpose: Convert plural table names to singular forms for consistency
-- Author: Claude Code
-- Date: 2025-09-13

-- =============================================
-- DEV ENVIRONMENT TABLE RENAMES
-- =============================================

-- Rename dev_daily_prices to dev_daily_price
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_daily_prices') THEN
        -- Rename the table
        ALTER TABLE dev_daily_prices RENAME TO dev_daily_price;
        
        -- Update primary key constraint name
        ALTER INDEX dp_pk RENAME TO dev_daily_price_pk;
        
        -- Update other indexes
        ALTER INDEX idx_dev_daily_prices_created_at RENAME TO idx_dev_daily_price_created_at;
        
        -- Update trigger name
        DROP TRIGGER IF EXISTS update_dev_daily_prices_updated_at ON dev_daily_price;
        CREATE TRIGGER update_dev_daily_price_updated_at 
            BEFORE UPDATE ON dev_daily_price 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            
        RAISE NOTICE 'Renamed dev_daily_prices to dev_daily_price';
    END IF;
END $$;

-- Rename dev_instruments to dev_instrument
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_instruments') THEN
        -- Rename the table
        ALTER TABLE dev_instruments RENAME TO dev_instrument;
        
        -- Update primary key constraint name
        ALTER INDEX dev_instruments_us_pkey RENAME TO dev_instrument_pk;
        
        -- Update other indexes
        ALTER INDEX idx_dev_instruments_us_active RENAME TO idx_dev_instrument_active;
        ALTER INDEX idx_dev_instruments_us_exchange RENAME TO idx_dev_instrument_exchange;
        ALTER INDEX idx_dev_instruments_us_symbol RENAME TO idx_dev_instrument_symbol;
        ALTER INDEX idx_dev_instruments_us_type RENAME TO idx_dev_instrument_type;
        
        RAISE NOTICE 'Renamed dev_instruments to dev_instrument';
    END IF;
END $$;

-- Rename dev_dividends to dev_dividend
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_dividends') THEN
        -- Rename the table
        ALTER TABLE dev_dividends RENAME TO dev_dividend;
        
        -- Update primary key constraint name
        ALTER INDEX d_pk RENAME TO dev_dividend_pk;
        
        -- Update other indexes
        ALTER INDEX idx_dividends_symbol_date RENAME TO idx_dev_dividend_symbol_date;
        
        -- Update unique constraint name
        ALTER TABLE dev_dividend RENAME CONSTRAINT dev_dividends_symbol_ex_date_dividend_type_key 
            TO dev_dividend_symbol_ex_date_dividend_type_key;
        
        RAISE NOTICE 'Renamed dev_dividends to dev_dividend';
    END IF;
END $$;

-- Handle dev_training_dataset tables (there are two conflicting tables)
DO $$ 
BEGIN
    -- First, drop the incomplete dev_training_dataset table if it exists
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_dataset') THEN
        -- Check if it has the old structure (creation_timestamp column)
        IF EXISTS (SELECT FROM information_schema.columns 
                   WHERE table_name = 'dev_training_dataset' AND column_name = 'creation_timestamp') THEN
            DROP TABLE dev_training_dataset CASCADE;
            RAISE NOTICE 'Dropped incomplete dev_training_dataset table';
        END IF;
    END IF;
    
    -- Then rename dev_training_datasets to dev_training_dataset
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_datasets') THEN
        -- Rename the table
        ALTER TABLE dev_training_datasets RENAME TO dev_training_dataset;
        
        -- Update primary key constraint name
        ALTER INDEX dev_training_datasets_pkey RENAME TO dev_training_dataset_pk;
        
        -- Update other indexes
        ALTER INDEX idx_dev_training_datasets_file_metadata_gin RENAME TO idx_dev_training_dataset_file_metadata_gin;
        ALTER INDEX idx_dev_training_datasets_file_metadata_symbols RENAME TO idx_dev_training_dataset_file_metadata_symbols;
        ALTER INDEX idx_dev_training_datasets_run_id RENAME TO idx_dev_training_dataset_run_id;
        ALTER INDEX idx_training_datasets_schema_hash RENAME TO idx_dev_training_dataset_schema_hash;
        
        RAISE NOTICE 'Renamed dev_training_datasets to dev_training_dataset';
    END IF;
END $$;

-- Rename dev_fundamentals to dev_fundamental
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_fundamentals') THEN
        -- Rename the table
        ALTER TABLE dev_fundamentals RENAME TO dev_fundamental;
        
        -- Update primary key constraint name
        ALTER INDEX fund_pk RENAME TO dev_fundamental_pk;
        
        -- Update other indexes
        ALTER INDEX idx_dev_fundamentals_created_at RENAME TO idx_dev_fundamental_created_at;
        
        -- Update trigger name
        DROP TRIGGER IF EXISTS update_dev_fundamentals_updated_at ON dev_fundamental;
        CREATE TRIGGER update_dev_fundamental_updated_at 
            BEFORE UPDATE ON dev_fundamental 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            
        RAISE NOTICE 'Renamed dev_fundamentals to dev_fundamental';
    END IF;
END $$;

-- Rename non-prefixed plural tables
DO $$ 
BEGIN
    -- Rename articles to article
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'articles') THEN
        ALTER TABLE articles RENAME TO article;
        RAISE NOTICE 'Renamed articles to article';
    END IF;
    
    -- Rename orders to order_table (order is reserved keyword)
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders') THEN
        ALTER TABLE orders RENAME TO order_table;
        RAISE NOTICE 'Renamed orders to order_table';
    END IF;
    
    -- Rename portfolios to portfolio
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'portfolios') THEN
        ALTER TABLE portfolios RENAME TO portfolio;
        RAISE NOTICE 'Renamed portfolios to portfolio';
    END IF;
    
    -- Rename positions to position_table (position is reserved keyword)
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'positions') THEN
        ALTER TABLE positions RENAME TO position_table;
        RAISE NOTICE 'Renamed positions to position_table';
    END IF;
END $$;

-- =============================================
-- INTG ENVIRONMENT TABLE RENAMES
-- =============================================

-- Rename intg_daily_prices to intg_daily_price
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_daily_prices') THEN
        ALTER TABLE intg_daily_prices RENAME TO intg_daily_price;
        RAISE NOTICE 'Renamed intg_daily_prices to intg_daily_price';
    END IF;
END $$;

-- Rename intg_instruments to intg_instrument
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_instruments') THEN
        ALTER TABLE intg_instruments RENAME TO intg_instrument;
        RAISE NOTICE 'Renamed intg_instruments to intg_instrument';
    END IF;
END $$;

-- Rename intg_dividends to intg_dividend
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_dividends') THEN
        ALTER TABLE intg_dividends RENAME TO intg_dividend;
        RAISE NOTICE 'Renamed intg_dividends to intg_dividend';
    END IF;
END $$;

-- Rename intg_training_datasets to intg_training_dataset
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_training_datasets') THEN
        ALTER TABLE intg_training_datasets RENAME TO intg_training_dataset;
        RAISE NOTICE 'Renamed intg_training_datasets to intg_training_dataset';
    END IF;
END $$;

-- Rename intg_fundamentals to intg_fundamental
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'intg_fundamentals') THEN
        ALTER TABLE intg_fundamentals RENAME TO intg_fundamental;
        RAISE NOTICE 'Renamed intg_fundamentals to intg_fundamental';
    END IF;
END $$;

-- =============================================
-- UPDATE FOREIGN KEY REFERENCES
-- =============================================

-- Update foreign key references for dev_instruments -> dev_instrument
DO $$ 
BEGIN
    -- Update dev_reconciled_records foreign key
    IF EXISTS (SELECT FROM information_schema.table_constraints 
               WHERE constraint_name = 'dev_reconciled_records_instrument_id_fkey') THEN
        ALTER TABLE dev_reconciled_records 
        DROP CONSTRAINT dev_reconciled_records_instrument_id_fkey,
        ADD CONSTRAINT dev_reconciled_records_instrument_id_fkey 
            FOREIGN KEY (instrument_id) REFERENCES dev_instrument(id);
    END IF;
    
    -- Update dev_monthly_training_data foreign key
    IF EXISTS (SELECT FROM information_schema.table_constraints 
               WHERE constraint_name = 'fk_dev_monthly_training_instrument_id') THEN
        ALTER TABLE dev_monthly_training_data 
        DROP CONSTRAINT fk_dev_monthly_training_instrument_id,
        ADD CONSTRAINT fk_dev_monthly_training_instrument_id 
            FOREIGN KEY (instrument_id) REFERENCES dev_instrument(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Update foreign key references for dev_training_dataset_files
DO $$ 
BEGIN
    -- The foreign key should already be correct after the table rename
    -- Just verify it exists and is pointing to the correct table
    IF NOT EXISTS (SELECT FROM information_schema.table_constraints 
                   WHERE constraint_name = 'dev_training_dataset_files_dataset_id_fkey') THEN
        -- If for some reason it doesn't exist, recreate it
        IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_dataset_files') THEN
            ALTER TABLE dev_training_dataset_files 
            ADD CONSTRAINT dev_training_dataset_files_dataset_id_fkey 
                FOREIGN KEY (dataset_id) REFERENCES dev_training_dataset(id) ON DELETE CASCADE;
            RAISE NOTICE 'Recreated foreign key for dev_training_dataset_files';
        END IF;
    END IF;
END $$;