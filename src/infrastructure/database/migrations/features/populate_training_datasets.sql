-- ==================================================
-- Populate sample training datasets for EDA testing
-- ==================================================

-- Insert sample training datasets
INSERT INTO dev_datasets (
    name, display_name, dataset_type, 
    file_path, total_rows, total_columns, file_format,
    stats_computed, created_at
) VALUES 
(
    'ml_feature_matrix_v1',
    'ML Feature Matrix V1',
    'training_dataset',
    '/data/training/ml_features_v1.parquet',
    125000,
    47,
    'parquet',
    FALSE,
    NOW()
),
(
    'backtest_results_2024',
    'Backtest Results 2024',
    'training_dataset',
    '/data/training/backtest_2024.csv',
    52000,
    23,
    'csv',
    FALSE,
    NOW()
),
(
    'portfolio_optimization_features',
    'Portfolio Optimization Features',
    'training_dataset',
    '/data/training/portfolio_features.json',
    89000,
    35,
    'json',
    FALSE,
    NOW()
),
(
    'sentiment_analysis_training',
    'Sentiment Analysis Training Data',
    'training_dataset',
    '/data/training/sentiment_data.parquet',
    340000,
    12,
    'parquet',
    FALSE,
    NOW()
)
ON CONFLICT (name) DO NOTHING;

-- Insert sample column metadata for training datasets
DO $$
DECLARE
    dataset_record RECORD;
    column_names TEXT[];
    col_name TEXT;
    i INTEGER;
BEGIN
    -- Get the training datasets we just inserted
    FOR dataset_record IN 
        SELECT id, name FROM dev_datasets 
        WHERE dataset_type = 'training_dataset'
    LOOP
        -- Define sample columns based on dataset type
        IF dataset_record.name = 'ml_feature_matrix_v1' THEN
            column_names := ARRAY['symbol', 'date', 'return_1d', 'return_5d', 'return_30d', 
                                'volume_sma_10', 'price_rsi', 'bollinger_position', 'macd_signal',
                                'sector', 'market_cap_category', 'target_return_5d'];
        ELSIF dataset_record.name = 'backtest_results_2024' THEN
            column_names := ARRAY['strategy_id', 'symbol', 'entry_date', 'exit_date', 'entry_price',
                                'exit_price', 'return_pct', 'holding_period', 'max_drawdown', 
                                'sharpe_ratio', 'trade_type'];
        ELSIF dataset_record.name = 'portfolio_optimization_features' THEN
            column_names := ARRAY['date', 'portfolio_id', 'symbol', 'weight', 'expected_return',
                                'volatility', 'correlation_spy', 'beta', 'alpha', 'information_ratio'];
        ELSIF dataset_record.name = 'sentiment_analysis_training' THEN
            column_names := ARRAY['news_id', 'symbol', 'publish_date', 'headline', 'content_snippet',
                                'sentiment_score', 'sentiment_label', 'source', 'price_impact_1h'];
        END IF;
        
        -- Insert column metadata
        FOR i IN 1..array_length(column_names, 1) LOOP
            col_name := column_names[i];
            
            INSERT INTO dev_dataset_columns (
                dataset_id, column_name, ordinal_position, data_type, semantic_type,
                is_nullable, created_at
            ) VALUES (
                dataset_record.id,
                col_name,
                i,
                CASE 
                    WHEN col_name LIKE '%date%' THEN 'timestamp'
                    WHEN col_name LIKE '%_id' OR col_name = 'symbol' THEN 'varchar'
                    WHEN col_name LIKE '%_pct' OR col_name LIKE '%return%' OR col_name LIKE '%ratio' THEN 'numeric'
                    WHEN col_name IN ('sector', 'market_cap_category', 'trade_type', 'sentiment_label', 'source') THEN 'varchar'
                    WHEN col_name LIKE '%price%' OR col_name LIKE '%volume%' OR col_name LIKE '%score%' THEN 'numeric'
                    ELSE 'varchar'
                END,
                CASE 
                    WHEN col_name LIKE '%date%' THEN 'date'
                    WHEN col_name LIKE '%_id' OR col_name = 'symbol' THEN 'identifier'  
                    WHEN col_name LIKE '%_pct' OR col_name LIKE '%return%' OR col_name LIKE '%ratio' THEN 'numeric'
                    WHEN col_name IN ('sector', 'market_cap_category', 'trade_type', 'sentiment_label', 'source') THEN 'categorical'
                    WHEN col_name LIKE '%price%' OR col_name LIKE '%volume%' OR col_name LIKE '%score%' THEN 'numeric'
                    ELSE 'text'
                END,
                TRUE
            ) ON CONFLICT (dataset_id, column_name) DO NOTHING;
        END LOOP;
    END LOOP;
END $$;

-- Update row counts to match column insertions
UPDATE dev_datasets 
SET column_count = (
    SELECT COUNT(*) 
    FROM dev_dataset_columns 
    WHERE dataset_id = dev_datasets.id
)
WHERE dataset_type = 'training_dataset';

-- Add some comments
COMMENT ON TABLE dev_datasets IS 'Updated with sample training datasets for EDA testing';

SELECT 
    'Training datasets populated:' as status,
    COUNT(*) as training_dataset_count
FROM dev_datasets 
WHERE dataset_type = 'training_dataset';