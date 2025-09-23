-- Migration 009: Comprehensive Feature Catalog Population for Database-Driven Feature Mapping
-- This migration populates the feature catalog with comprehensive feature definitions
-- to enable database-driven feature → feature group mapping

-- Update db_version (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'db_version') THEN
        INSERT INTO db_version (version, description) VALUES 
        (9, 'Comprehensive feature catalog population for database-driven mapping')
        ON CONFLICT (version) DO NOTHING;
    END IF;
END
$$;

-- Populate feature catalog for all environments dynamically
DO $$
DECLARE
    env_prefix TEXT;
    table_exists BOOLEAN;
    feature_catalog_table TEXT;
    feature_groups_table TEXT;
    feature_patterns_table TEXT;
BEGIN
    -- Check all possible environment prefixes
    FOR env_prefix IN SELECT unnest(ARRAY['dev_', 'intg_', 'prod_', 'test_', ''])
    LOOP
        feature_catalog_table := env_prefix || 'feature_catalog';
        feature_groups_table := env_prefix || 'feature_groups';
        feature_patterns_table := env_prefix || 'feature_patterns';
        
        -- Check if this environment's feature catalog table exists
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = feature_catalog_table
        ) INTO table_exists;
        
        IF table_exists THEN
            RAISE NOTICE 'Populating comprehensive feature catalog for % environment', 
                CASE WHEN env_prefix = '' THEN 'base' ELSE rtrim(env_prefix, '_') END;
            
            -- Add comprehensive technical momentum features
            EXECUTE format('
                INSERT INTO %I (feature_name, feature_group_id, data_type, column_position, description, computation_method) VALUES 
                    -- SMA variations
                    (''sma_5'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 10, ''5-period simple moving average'', ''Simple moving average with 5-period window''),
                    (''sma_10'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 11, ''10-period simple moving average'', ''Simple moving average with 10-period window''),
                    (''sma_20'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 12, ''20-period simple moving average'', ''Simple moving average with 20-period window''),
                    (''sma_50'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 13, ''50-period simple moving average'', ''Simple moving average with 50-period window''),
                    (''sma_200'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 14, ''200-period simple moving average'', ''Simple moving average with 200-period window''),
                    
                    -- EMA variations
                    (''ema_5'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 15, ''5-period exponential moving average'', ''Exponential moving average with 5-period window''),
                    (''ema_10'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 16, ''10-period exponential moving average'', ''Exponential moving average with 10-period window''),
                    (''ema_12'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 17, ''12-period exponential moving average'', ''Exponential moving average with 12-period window''),
                    (''ema_26'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 18, ''26-period exponential moving average'', ''Exponential moving average with 26-period window''),
                    (''ema_50'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 19, ''50-period exponential moving average'', ''Exponential moving average with 50-period window''),
                    
                    -- RSI variations
                    (''rsi_14'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 20, ''14-period relative strength index'', ''RSI with 14-period calculation''),
                    (''rsi_21'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 21, ''21-period relative strength index'', ''RSI with 21-period calculation''),
                    
                    -- MACD family
                    (''macd'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 22, ''MACD line (12,26,9)'', ''MACD line calculated from EMA(12) - EMA(26)''),
                    (''macd_signal'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 23, ''MACD signal line'', ''9-period EMA of MACD line''),
                    (''macd_histogram'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 24, ''MACD histogram'', ''MACD line minus signal line''),
                    
                    -- Momentum indicators
                    (''momentum_1d'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 25, ''1-day price momentum'', ''Price change over 1 day''),
                    (''momentum_5d'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 26, ''5-day price momentum'', ''Price change over 5 days''),
                    (''momentum_10d'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 27, ''10-day price momentum'', ''Price change over 10 days''),
                    
                    -- Custom technical indicators (existing in codebase)
                    (''etop'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 28, ''Envelope top indicator'', ''Custom envelope top technical indicator''),
                    (''ebot'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 29, ''Envelope bottom indicator'', ''Custom envelope bottom technical indicator''),
                    (''pldot'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 30, ''Price level dot indicator'', ''Custom price level dot technical indicator''),
                    
                    -- Stochastic indicators
                    (''stoch_k'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 31, ''Stochastic %%K'', ''Stochastic oscillator %%K line''),
                    (''stoch_d'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 32, ''Stochastic %%D'', ''Stochastic oscillator %%D line''),
                    
                    -- Williams %%R
                    (''williams_r'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''FLOAT64'', 33, ''Williams %%R'', ''Williams %%R momentum oscillator'')
                ON CONFLICT (feature_name) DO NOTHING',
                feature_catalog_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table
            );

            -- Add comprehensive technical volatility features
            EXECUTE format('
                INSERT INTO %I (feature_name, feature_group_id, data_type, column_position, description, computation_method) VALUES 
                    -- Bollinger Bands
                    (''bb_upper'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 10, ''Bollinger Band upper line'', ''Upper Bollinger Band (SMA + 2*StdDev)''),
                    (''bb_middle'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 11, ''Bollinger Band middle line'', ''Middle Bollinger Band (20-period SMA)''),
                    (''bb_lower'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 12, ''Bollinger Band lower line'', ''Lower Bollinger Band (SMA - 2*StdDev)''),
                    (''bb_width'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 13, ''Bollinger Band width'', ''Width between upper and lower Bollinger Bands''),
                    (''bb_percent'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 14, ''Bollinger Band %%B'', ''Position within Bollinger Bands (0-1)''),
                    
                    -- ATR variations  
                    (''atr_7'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 15, ''7-period Average True Range'', ''Average True Range with 7-period calculation''),
                    (''atr_14'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 16, ''14-period Average True Range'', ''Average True Range with 14-period calculation''),
                    (''atr_21'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 17, ''21-period Average True Range'', ''Average True Range with 21-period calculation''),
                    
                    -- Realized volatility
                    (''realized_vol_5d'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 18, ''5-day realized volatility'', ''Realized volatility calculated over 5 days''),
                    (''realized_vol_20d'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 19, ''20-day realized volatility'', ''Realized volatility calculated over 20 days''),
                    (''realized_vol_60d'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 20, ''60-day realized volatility'', ''Realized volatility calculated over 60 days''),
                    
                    -- GARCH volatility
                    (''garch_vol'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 21, ''GARCH volatility forecast'', ''GARCH(1,1) volatility forecast''),
                    
                    -- Volatility ratios
                    (''vol_ratio_5_20'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 22, ''5d/20d volatility ratio'', ''Ratio of 5-day to 20-day realized volatility''),
                    (''vol_ratio_20_60'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 23, ''20d/60d volatility ratio'', ''Ratio of 20-day to 60-day realized volatility''),
                    
                    -- Standard deviation
                    (''std_5'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 24, ''5-period standard deviation'', ''Standard deviation of returns over 5 periods''),
                    (''std_20'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 25, ''20-period standard deviation'', ''Standard deviation of returns over 20 periods''),
                    
                    -- Keltner Channels
                    (''kc_upper'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 26, ''Keltner Channel upper'', ''Upper Keltner Channel line''),
                    (''kc_lower'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''FLOAT64'', 27, ''Keltner Channel lower'', ''Lower Keltner Channel line'')
                ON CONFLICT (feature_name) DO NOTHING',
                feature_catalog_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table
            );

            -- Add comprehensive fundamental features
            EXECUTE format('
                INSERT INTO %I (feature_name, feature_group_id, data_type, column_position, description, computation_method) VALUES 
                    -- Valuation ratios
                    (''pe_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 10, ''Price-to-Earnings ratio'', ''Current price divided by earnings per share''),
                    (''pe_ratio_ttm'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 11, ''Trailing twelve months P/E ratio'', ''P/E ratio based on trailing 12 months earnings''),
                    (''pb_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 12, ''Price-to-Book ratio'', ''Current price divided by book value per share''),
                    (''ps_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 13, ''Price-to-Sales ratio'', ''Market cap divided by total revenue''),
                    (''pcf_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 14, ''Price-to-Cash-Flow ratio'', ''Current price divided by cash flow per share''),
                    
                    -- Profitability ratios
                    (''roe'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 15, ''Return on Equity'', ''Net income divided by shareholders equity''),
                    (''roa'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 16, ''Return on Assets'', ''Net income divided by total assets''),
                    (''roic'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 17, ''Return on Invested Capital'', ''Operating income divided by invested capital''),
                    (''gross_margin'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 18, ''Gross profit margin'', ''Gross profit divided by revenue''),
                    (''operating_margin'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 19, ''Operating profit margin'', ''Operating income divided by revenue''),
                    (''net_margin'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 20, ''Net profit margin'', ''Net income divided by revenue''),
                    
                    -- Leverage ratios
                    (''debt_equity'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 21, ''Debt-to-Equity ratio'', ''Total debt divided by shareholders equity''),
                    (''debt_assets'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 22, ''Debt-to-Assets ratio'', ''Total debt divided by total assets''),
                    (''interest_coverage'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 23, ''Interest coverage ratio'', ''EBIT divided by interest expense''),
                    
                    -- Growth ratios
                    (''revenue_growth'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 24, ''Revenue growth rate'', ''Quarterly revenue growth rate year-over-year''),
                    (''eps_growth'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 25, ''EPS growth rate'', ''Quarterly earnings per share growth rate''),
                    (''asset_growth'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 26, ''Asset growth rate'', ''Quarterly total assets growth rate''),
                    
                    -- Efficiency ratios
                    (''asset_turnover'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 27, ''Asset turnover ratio'', ''Revenue divided by average total assets''),
                    (''inventory_turnover'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 28, ''Inventory turnover ratio'', ''Cost of goods sold divided by average inventory''),
                    
                    -- Liquidity ratios
                    (''current_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 29, ''Current ratio'', ''Current assets divided by current liabilities''),
                    (''quick_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''FLOAT64'', 30, ''Quick ratio'', ''Quick assets divided by current liabilities'')
                ON CONFLICT (feature_name) DO NOTHING',
                feature_catalog_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table
            );

            -- Add comprehensive OHLCV-derived features
            EXECUTE format('
                INSERT INTO %I (feature_name, feature_group_id, data_type, column_position, description, computation_method) VALUES 
                    -- Additional OHLCV derived features
                    (''vwap'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 8, ''Volume Weighted Average Price'', ''Volume-weighted average price for the period''),
                    (''twap'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 9, ''Time Weighted Average Price'', ''Time-weighted average price for the period''),
                    (''typical_price'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 10, ''Typical price'', ''Average of high, low, and close prices''),
                    (''median_price'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 11, ''Median price'', ''Average of high and low prices''),
                    (''range_pct'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 12, ''Range percentage'', ''Daily range as percentage of close price''),
                    (''gap_pct'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 13, ''Gap percentage'', ''Gap between previous close and current open''),
                    (''body_pct'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 14, ''Candlestick body percentage'', ''Body size as percentage of total range''),
                    (''upper_shadow_pct'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 15, ''Upper shadow percentage'', ''Upper shadow as percentage of total range''),
                    (''lower_shadow_pct'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''FLOAT64'', 16, ''Lower shadow percentage'', ''Lower shadow as percentage of total range'')
                ON CONFLICT (feature_name) DO NOTHING',
                feature_catalog_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table
            );

            -- Create feature name patterns for flexible matching
            EXECUTE format('
                CREATE TABLE IF NOT EXISTS %I (
                    id SERIAL PRIMARY KEY,
                    pattern VARCHAR(100) NOT NULL UNIQUE,
                    feature_group_id INTEGER NOT NULL REFERENCES %I(id),
                    pattern_type VARCHAR(20) NOT NULL DEFAULT ''contains'',
                    priority INTEGER NOT NULL DEFAULT 100,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT valid_pattern_type CHECK (pattern_type IN (''exact'', ''starts_with'', ''ends_with'', ''contains'', ''regex''))
                )',
                feature_patterns_table, feature_groups_table
            );

            -- Insert pattern matching rules for flexible feature mapping
            EXECUTE format('
                INSERT INTO %I (pattern, feature_group_id, pattern_type, priority, description) VALUES 
                    -- OHLCV patterns (highest priority)
                    (''open'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''exact'', 10, ''Opening price''),
                    (''high'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''exact'', 10, ''High price''),
                    (''low'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''exact'', 10, ''Low price''),
                    (''close'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''exact'', 10, ''Closing price''),
                    (''volume'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''exact'', 10, ''Trading volume''),
                    (''vwap'', (SELECT id FROM %I WHERE group_name = ''ohlcv_basic''), ''contains'', 10, ''Volume weighted average price patterns''),
                    
                    -- Technical momentum patterns
                    (''sma'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Simple moving average patterns''),
                    (''ema'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Exponential moving average patterns''),
                    (''rsi'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''RSI indicator patterns''),
                    (''macd'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''MACD indicator patterns''),
                    (''momentum'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Momentum indicator patterns''),
                    (''stoch'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Stochastic indicator patterns''),
                    (''williams'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Williams %%R patterns''),
                    (''etop'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Envelope top patterns''),
                    (''ebot'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Envelope bottom patterns''),
                    (''pldot'', (SELECT id FROM %I WHERE group_name = ''technical_momentum''), ''contains'', 20, ''Price level dot patterns''),
                    
                    -- Technical volatility patterns
                    (''bb_'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''starts_with'', 30, ''Bollinger Band patterns''),
                    (''bollinger'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''contains'', 30, ''Bollinger Band patterns''),
                    (''atr'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''contains'', 30, ''Average True Range patterns''),
                    (''realized_vol'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''contains'', 30, ''Realized volatility patterns''),
                    (''garch'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''contains'', 30, ''GARCH volatility patterns''),
                    (''vol_ratio'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''contains'', 30, ''Volatility ratio patterns''),
                    (''std_'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''starts_with'', 30, ''Standard deviation patterns''),
                    (''kc_'', (SELECT id FROM %I WHERE group_name = ''technical_volatility''), ''starts_with'', 30, ''Keltner Channel patterns''),
                    
                    -- Fundamental patterns
                    (''pe_'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''starts_with'', 40, ''Price-to-earnings patterns''),
                    (''pb_'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''starts_with'', 40, ''Price-to-book patterns''),
                    (''ps_'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''starts_with'', 40, ''Price-to-sales patterns''),
                    (''pcf_'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''starts_with'', 40, ''Price-to-cash-flow patterns''),
                    (''roe'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''contains'', 40, ''Return on equity patterns''),
                    (''roa'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''contains'', 40, ''Return on assets patterns''),
                    (''roic'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''contains'', 40, ''Return on invested capital patterns''),
                    (''debt_'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''starts_with'', 40, ''Debt ratio patterns''),
                    (''_growth'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''ends_with'', 40, ''Growth rate patterns''),
                    (''_margin'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''ends_with'', 40, ''Profit margin patterns''),
                    (''_ratio'', (SELECT id FROM %I WHERE group_name = ''fundamental_quarterly''), ''ends_with'', 40, ''Financial ratio patterns'')
                ON CONFLICT (pattern) DO NOTHING',
                feature_patterns_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table,
                feature_groups_table, feature_groups_table, feature_groups_table, feature_groups_table
            );

            -- Create indexes for efficient pattern matching
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_pattern ON %I(pattern)', 
                replace(feature_patterns_table, '_', ''), feature_patterns_table);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_group_priority ON %I(feature_group_id, priority)', 
                replace(feature_patterns_table, '_', ''), feature_patterns_table);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_type ON %I(pattern_type)', 
                replace(feature_patterns_table, '_', ''), feature_patterns_table);

            RAISE NOTICE '% environment comprehensive feature catalog populated successfully', 
                CASE WHEN env_prefix = '' THEN 'Base' ELSE rtrim(env_prefix, '_') END;
        END IF;
    END LOOP;
END
$$;


-- Migration completed