-- Populate semantic metadata table
INSERT INTO dev_column_semantic_types (table_name, column_name, semantic_type, enum_values, business_meaning) VALUES
('dev_instruments', 'instrument_type', 'categorical', ARRAY['Stock', 'ETF', 'PFD', 'WARRANT', 'CS', 'SP', 'UNIT', 'ADRC', 'RIGHT'], 'Type of financial instrument'),
('dev_instruments', 'exchange_code', 'categorical', ARRAY['NASDAQ', 'NYSE', 'NYSE_ARCA', 'BATS', 'XNYS', 'NYSE_MKT'], 'Exchange where instrument trades'),
('dev_instruments', 'currency_code', 'categorical', ARRAY['USD', 'CAD', 'EUR', 'GBP'], 'Base currency for instrument pricing'),
('dev_instruments', 'active', 'boolean', NULL, 'Whether instrument is currently active for trading'),
('dev_instruments', 'list_date', 'date', NULL, 'Date when instrument was first listed on exchange'),
('dev_instruments', 'delist_date', 'date', NULL, 'Date when instrument was delisted (null if still listed)'),
('dev_instruments', 'symbol', 'identifier', NULL, 'Unique ticker symbol for instrument'),
('dev_daily_price_tiingo', 'symbol', 'identifier', NULL, 'Stock ticker symbol'),
('dev_daily_price_tiingo', 'date', 'date', NULL, 'Trading date'),
('dev_daily_price_tiingo', 'open', 'numeric', NULL, 'Opening price'),
('dev_daily_price_tiingo', 'high', 'numeric', NULL, 'Highest price during trading day'),
('dev_daily_price_tiingo', 'low', 'numeric', NULL, 'Lowest price during trading day'),
('dev_daily_price_tiingo', 'close', 'numeric', NULL, 'Closing price'),
('dev_daily_price_tiingo', 'volume', 'numeric', NULL, 'Number of shares traded'),
('dev_financial_events', 'event_type', 'categorical', ARRAY['earnings', 'analyst_rating', 'corporate_action', 'announcement'], 'Type of financial event'),
('dev_financial_events', 'sentiment', 'categorical', ARRAY['positive', 'negative', 'neutral'], 'Market sentiment impact'),
('dev_financial_events', 'importance_level', 'categorical', ARRAY['high', 'medium', 'low'], 'Market importance level'),
('dev_financial_events', 'event_datetime', 'datetime', NULL, 'When the financial event occurred')
ON CONFLICT (table_name, column_name) DO UPDATE SET
    semantic_type = EXCLUDED.semantic_type,
    enum_values = EXCLUDED.enum_values,
    business_meaning = EXCLUDED.business_meaning;