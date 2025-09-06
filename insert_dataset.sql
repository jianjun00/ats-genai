INSERT INTO dev_training_datasets (
    dataset_name, run_id, symbols, date_range_start, date_range_end,
    data_quality_score, feature_completeness, label_completeness,
    total_sequences, file_size_mb, status, dataset_path,
    symbol_files, file_metadata
) VALUES (
    'AAPL_TSLA_20250701_20250906_Run89', 89,
    ARRAY['AAPL', 'TSLA'], '2025-07-01', '2025-09-06',
    0.95, 0.98, 0.97, 2, 50.0, 'completed',
    '/mnt/d/ats-data/training_data/89',
    '{"AAPL": "AAPL_20250701_000000_20250906_000000", "TSLA": "TSLA_20250701_000000_20250906_000000"}',
    '{"symbols": ["AAPL", "TSLA"], "timeframes": ["5m", "15m", "1h", "1d", "1w"], "total_files": 10}'
) RETURNING id;