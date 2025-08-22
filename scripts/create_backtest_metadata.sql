-- Backtest metadata table that points to disk-based portfolio files
CREATE TABLE IF NOT EXISTS dev_backtest_runs (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(100) UNIQUE NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    portfolio_data_path VARCHAR(500) NOT NULL,  -- Path to portfolio holdings file
    initial_capital DECIMAL(15,2) NOT NULL,
    universe_size INTEGER,
    status VARCHAR(20) DEFAULT 'completed',
    performance_summary JSONB,  -- High-level metrics
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Current portfolio metadata pointing to disk file
CREATE TABLE IF NOT EXISTS dev_current_portfolio_config (
    id SERIAL PRIMARY KEY,
    portfolio_name VARCHAR(100) NOT NULL DEFAULT 'Main Portfolio',
    portfolio_data_path VARCHAR(500) NOT NULL,
    cash_position DECIMAL(15,2) NOT NULL DEFAULT 0,
    target_allocation_strategy VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert backtest metadata pointing to disk files
INSERT INTO dev_backtest_runs (
    backtest_run_id, strategy_name, start_date, end_date, 
    portfolio_data_path, initial_capital, universe_size, 
    performance_summary
) VALUES 
(
    'comprehensive_2022_2025',
    'Comprehensive Multi-Factor Strategy',
    '2022-01-01',
    '2025-08-19', 
    'data/portfolios/backtests/comprehensive_2022_2025.json',
    10000000.0,
    10,
    '{"total_return": 14.25, "sharpe_ratio": 1.58, "max_drawdown": -0.08, "win_rate": 0.68}'::jsonb
),
(
    'adaptive_sr_2024', 
    'Adaptive Support/Resistance Strategy',
    '2024-01-01',
    '2024-06-30',
    'data/portfolios/backtests/adaptive_sr_2024.json', 
    1000000.0,
    20,
    '{"total_return": 0.32, "sharpe_ratio": 1.45, "max_drawdown": -0.05, "win_rate": 0.62}'::jsonb
),
(
    'momentum_2024',
    'Momentum Strategy 2024', 
    '2024-01-01',
    '2024-06-30',
    'data/portfolios/backtests/momentum_2024.json',
    1000000.0, 
    15,
    '{"total_return": 0.28, "sharpe_ratio": 1.35, "max_drawdown": -0.06, "win_rate": 0.65}'::jsonb
)
ON CONFLICT (backtest_run_id) DO UPDATE SET
    updated_at = CURRENT_TIMESTAMP;

-- Insert current portfolio config pointing to disk file
INSERT INTO dev_current_portfolio_config (
    portfolio_name, portfolio_data_path, cash_position, target_allocation_strategy
) VALUES (
    'Main Portfolio',
    'data/portfolios/current/main_portfolio.json',
    125000.0,
    'diversified_growth'
) ON CONFLICT DO NOTHING;