-- 017_comprehensive_backtest_schema.sql
-- Comprehensive database schema for backtest results and analytics

-- Enhanced backtest runs table with comprehensive metadata
CREATE TABLE IF NOT EXISTS comprehensive_backtest_runs (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) UNIQUE NOT NULL,
    strategy_name VARCHAR(500) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital DECIMAL(15,2) NOT NULL,
    final_value DECIMAL(15,2),
    total_return DECIMAL(10,6),
    annualized_return DECIMAL(10,6),
    sharpe_ratio DECIMAL(8,4),
    max_drawdown DECIMAL(8,4),
    volatility DECIMAL(8,4),
    calmar_ratio DECIMAL(8,4),
    sortino_ratio DECIMAL(8,4),
    win_rate DECIMAL(6,4),
    profit_factor DECIMAL(8,4),
    universe_size INTEGER,
    universe_symbols TEXT[], -- Array of symbols
    num_trades INTEGER,
    status VARCHAR(50) DEFAULT 'pending',
    metadata JSONB, -- Store flexible metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Market regime analysis table
CREATE TABLE IF NOT EXISTS market_regimes (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    period_name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    market_context TEXT NOT NULL,
    performance_characteristics TEXT,
    best_performer VARCHAR(10),
    key_events TEXT[],
    regime_metrics JSONB, -- Store regime-specific metrics
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Symbol performance tracking
CREATE TABLE IF NOT EXISTS symbol_performance (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    symbol VARCHAR(10) NOT NULL,
    start_price DECIMAL(12,4),
    end_price DECIMAL(12,4),
    total_return DECIMAL(10,6),
    trading_days INTEGER,
    rank_position INTEGER,
    daily_metrics JSONB, -- Store daily performance data if needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily portfolio performance time series
CREATE TABLE IF NOT EXISTS portfolio_performance (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    date DATE NOT NULL,
    portfolio_value DECIMAL(15,2) NOT NULL,
    daily_return DECIMAL(10,6),
    cumulative_return DECIMAL(10,6),
    drawdown DECIMAL(8,4),
    cash_position DECIMAL(15,2),
    positions_count INTEGER,
    gross_exposure DECIMAL(15,2),
    net_exposure DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(backtest_run_id, date)
);

-- Model configuration comparisons
CREATE TABLE IF NOT EXISTS model_comparisons (
    id SERIAL PRIMARY KEY,
    comparison_id VARCHAR(255) UNIQUE NOT NULL,
    baseline_backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    test_backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    comparison_type VARCHAR(50), -- e.g., 'baseline_vs_enhanced', 'adaptive_vs_static'
    statistical_significance DECIMAL(6,4), -- p-value
    effect_size DECIMAL(8,4), -- Cohen's d
    confidence_interval JSONB, -- Store confidence interval data
    recommendation VARCHAR(50), -- 'adopt_test', 'keep_baseline', 'requires_further_testing'
    recommendation_confidence VARCHAR(20), -- 'high', 'medium', 'low'
    reasons TEXT[],
    concerns TEXT[],
    next_steps TEXT[],
    analysis_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trade-level data for detailed analysis
CREATE TABLE IF NOT EXISTS backtest_trades (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    symbol VARCHAR(10) NOT NULL,
    entry_date DATE NOT NULL,
    exit_date DATE,
    entry_price DECIMAL(12,4),
    exit_price DECIMAL(12,4),
    shares INTEGER,
    pnl DECIMAL(12,2),
    pnl_percentage DECIMAL(8,4),
    exit_reason VARCHAR(50), -- 'stop_loss', 'take_profit', 'max_hold', 'signal_exit'
    days_held INTEGER,
    trade_type VARCHAR(20), -- 'long', 'short'
    commission_paid DECIMAL(8,2),
    slippage DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Risk metrics tracking
CREATE TABLE IF NOT EXISTS risk_metrics (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) REFERENCES comprehensive_backtest_runs(backtest_run_id),
    metric_date DATE NOT NULL,
    value_at_risk_95 DECIMAL(12,2),
    value_at_risk_99 DECIMAL(12,2),
    expected_shortfall DECIMAL(12,2),
    beta DECIMAL(6,4),
    alpha DECIMAL(6,4),
    information_ratio DECIMAL(6,4),
    tracking_error DECIMAL(6,4),
    max_leverage DECIMAL(6,4),
    portfolio_concentration DECIMAL(6,4), -- Herfindahl index
    sector_concentration JSONB, -- Sector exposure breakdown
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(backtest_run_id, metric_date)
);

-- Analytics dashboard configuration
CREATE TABLE IF NOT EXISTS dashboard_configs (
    id SERIAL PRIMARY KEY,
    config_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    layout_config JSONB NOT NULL, -- Store dashboard layout configuration
    chart_configs JSONB NOT NULL, -- Store chart configurations
    filters JSONB, -- Default filters
    permissions JSONB, -- Access permissions
    is_default BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User preferences and saved views
CREATE TABLE IF NOT EXISTS user_preferences (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    preference_type VARCHAR(50) NOT NULL, -- 'dashboard_layout', 'chart_settings', 'filters'
    preference_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, preference_type)
);

-- Portfolio holdings table for daily breakdown
CREATE TABLE IF NOT EXISTS portfolio_holdings (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(255) NOT NULL REFERENCES comprehensive_backtest_runs(backtest_run_id),
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    shares DECIMAL(15, 4) NOT NULL,
    price DECIMAL(15, 4) NOT NULL,
    market_value DECIMAL(15, 2) NOT NULL,
    weight DECIMAL(8, 6) NOT NULL,
    daily_pnl DECIMAL(15, 2),
    daily_return DECIMAL(10, 6),
    sector VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(backtest_run_id, date, symbol)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_backtest_runs_date ON comprehensive_backtest_runs(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy ON comprehensive_backtest_runs(strategy_name);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_status ON comprehensive_backtest_runs(status);
CREATE INDEX IF NOT EXISTS idx_portfolio_performance_date ON portfolio_performance(backtest_run_id, date);
CREATE INDEX IF NOT EXISTS idx_symbol_performance_rank ON symbol_performance(backtest_run_id, rank_position);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_date ON backtest_trades(backtest_run_id, symbol, entry_date);
CREATE INDEX IF NOT EXISTS idx_risk_metrics_date ON risk_metrics(backtest_run_id, metric_date);
CREATE INDEX IF NOT EXISTS idx_market_regimes_period ON market_regimes(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_portfolio_holdings_date ON portfolio_holdings(backtest_run_id, date);
CREATE INDEX IF NOT EXISTS idx_portfolio_holdings_symbol ON portfolio_holdings(backtest_run_id, symbol, date);

-- Insert sample comprehensive backtest data
INSERT INTO comprehensive_backtest_runs (
    backtest_run_id,
    strategy_name,
    start_date,
    end_date,
    initial_capital,
    final_value,
    total_return,
    annualized_return,
    sharpe_ratio,
    max_drawdown,
    volatility,
    universe_size,
    universe_symbols,
    num_trades,
    status,
    metadata
) VALUES (
    'comprehensive_2022_2025',
    '2022-2025 Comprehensive Analysis',
    '2022-01-01',
    '2025-08-19',
    10000000.00,
    152530000.00,
    14.253,
    1.088,
    2.87,
    0.145,
    0.25,
    9,
    ARRAY['AMZN', 'TSLA', 'GOOGL', 'META', 'MSFT', 'JNJ', 'AAPL', 'JPM', 'V'],
    0, -- No individual trades in this analysis
    'completed',
    '{"data_records": 2477396, "market_regimes": 4, "analysis_type": "comprehensive_market_analysis", "key_insight": "Equal-weight portfolio achieved 1425.3% return"}'::jsonb
) ON CONFLICT (backtest_run_id) DO NOTHING;

-- Insert market regime data
INSERT INTO market_regimes (backtest_run_id, period_name, start_date, end_date, market_context, performance_characteristics, key_events) VALUES
('comprehensive_2022_2025', '2022 Bear Market', '2022-01-01', '2022-12-31', 'Bear market with inflation/rate hikes', 'High volatility, value rotation', ARRAY['Russia-Ukraine conflict', 'Peak inflation', 'Aggressive rate hikes']),
('comprehensive_2022_2025', '2023 AI Recovery', '2023-01-01', '2023-12-31', 'Strong recovery driven by AI enthusiasm', 'Tech-led growth, momentum strategies', ARRAY['ChatGPT launch impact', 'AI investment boom', 'Nvidia surge']),
('comprehensive_2022_2025', '2024 Mixed Conditions', '2024-01-01', '2024-12-31', 'Mixed conditions with election uncertainty', 'Sector rotation, defensive positioning', ARRAY['Presidential election', 'Fed pivot expectations', 'Mega-cap rotation']),
('comprehensive_2022_2025', '2025 Current Dynamics', '2025-01-01', '2025-08-19', 'Current market dynamics through August', 'Continued tech leadership', ARRAY['New administration policies', 'AI regulation debates', 'Infrastructure investments'])
ON CONFLICT DO NOTHING;

-- Insert symbol performance data
INSERT INTO symbol_performance (backtest_run_id, symbol, start_price, end_price, total_return, trading_days, rank_position) VALUES
('comprehensive_2022_2025', 'AMZN', 37.89, 1789.25, 46.221, 937, 1),
('comprehensive_2022_2025', 'TSLA', 21.35, 799.85, 36.460, 939, 2),
('comprehensive_2022_2025', 'GOOGL', 78.16, 1554.00, 18.883, 937, 3),
('comprehensive_2022_2025', 'META', 78.03, 790.00, 9.124, 937, 4),
('comprehensive_2022_2025', 'MSFT', 214.25, 1716.30, 7.011, 937, 5),
('comprehensive_2022_2025', 'JNJ', 84.24, 360.78, 3.283, 937, 6),
('comprehensive_2022_2025', 'AAPL', 62.31, 259.02, 3.157, 937, 7),
('comprehensive_2022_2025', 'JPM', 101.96, 335.03, 2.286, 937, 8),
('comprehensive_2022_2025', 'V', 121.17, 346.06, 1.856, 937, 9)
ON CONFLICT DO NOTHING;

-- Insert default dashboard configuration
INSERT INTO dashboard_configs (
    config_name,
    description,
    layout_config,
    chart_configs,
    filters,
    is_default
) VALUES (
    'default_analytics_dashboard',
    'Default comprehensive analytics dashboard layout',
    '{
        "layout": "grid",
        "columns": 12,
        "sections": [
            {"id": "summary", "span": 12, "height": 200},
            {"id": "performance_chart", "span": 8, "height": 400},
            {"id": "metrics_panel", "span": 4, "height": 400},
            {"id": "symbol_performance", "span": 6, "height": 350},
            {"id": "market_regimes", "span": 6, "height": 350},
            {"id": "risk_analysis", "span": 12, "height": 300}
        ]
    }'::jsonb,
    '{
        "performance_chart": {"type": "line", "yAxis": "portfolio_value", "showDrawdown": true},
        "symbol_performance": {"type": "bar", "sortBy": "total_return"},
        "market_regimes": {"type": "timeline", "showEvents": true},
        "risk_analysis": {"type": "heatmap", "metrics": ["sharpe_ratio", "max_drawdown", "volatility"]}
    }'::jsonb,
    '{
        "default_date_range": "2022-01-01_2025-08-19",
        "default_backtest": "comprehensive_2022_2025",
        "show_comparisons": true
    }'::jsonb,
    true
) ON CONFLICT (config_name) DO NOTHING;

-- Update sequences
SELECT setval('comprehensive_backtest_runs_id_seq', (SELECT COALESCE(MAX(id), 1) FROM comprehensive_backtest_runs));
SELECT setval('market_regimes_id_seq', (SELECT COALESCE(MAX(id), 1) FROM market_regimes));
SELECT setval('symbol_performance_id_seq', (SELECT COALESCE(MAX(id), 1) FROM symbol_performance));