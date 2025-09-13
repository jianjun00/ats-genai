-- Migration 014: Remove NOT NULL constraint from symbol column in all tables where it exists

ALTER TABLE daily_prices AlTER COLUMN symbol DROP NOT NULL;
ALTER TABLE daily_price_tiingo AlTER COLUMN symbol DROP NOT NULL;
ALTER TABLE daily_price_polygon AlTER COLUMN symbol DROP NOT NULL;

ALTER TABLE stock_splits AlTER COLUMN symbol DROP NOT NULL;
ALTER TABLE dividends AlTER COLUMN symbol DROP NOT NULL;

ALTER TABLE fundamentals AlTER COLUMN symbol DROP NOT NULL;
ALTER TABLE daily_market_cap AlTER COLUMN symbol DROP NOT NULL;
ALTER TABLE universe_membership AlTER COLUMN symbol DROP NOT NULL;
ALTER TABLE universe_membership_changes AlTER COLUMN symbol DROP NOT NULL;
