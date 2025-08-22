-- Create portfolio-related tables for real data tracking

-- Portfolio holdings table for current portfolio positions
CREATE TABLE IF NOT EXISTS dev_current_portfolio_holdings (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    shares DECIMAL(15,4) NOT NULL,
    cost_basis DECIMAL(10,2) NOT NULL,
    purchase_date DATE NOT NULL,
    sector VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Current portfolio metadata
CREATE TABLE IF NOT EXISTS dev_current_portfolio_metadata (
    id SERIAL PRIMARY KEY,
    cash_position DECIMAL(15,2) NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    portfolio_name VARCHAR(100) DEFAULT 'Main Portfolio',
    target_allocation_strategy VARCHAR(50)
);

-- Historical portfolio snapshots for backtest analysis
CREATE TABLE IF NOT EXISTS dev_portfolio_snapshots (
    id SERIAL PRIMARY KEY,
    backtest_run_id VARCHAR(100) NOT NULL,
    snapshot_date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    shares DECIMAL(15,4) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    market_value DECIMAL(15,2) NOT NULL,
    weight DECIMAL(8,6) NOT NULL,
    daily_pnl DECIMAL(15,2),
    daily_return DECIMAL(10,8),
    sector VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(backtest_run_id, snapshot_date, symbol)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_current_holdings_symbol ON dev_current_portfolio_holdings(symbol);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_backtest_date ON dev_portfolio_snapshots(backtest_run_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_symbol ON dev_portfolio_snapshots(symbol);

-- Insert sample current portfolio holdings
INSERT INTO dev_current_portfolio_holdings (symbol, shares, cost_basis, purchase_date, sector) VALUES
('AAPL', 1500.0, 150.00, '2024-01-15', 'Technology'),
('MSFT', 800.0, 280.00, '2024-01-20', 'Technology'),
('GOOGL', 600.0, 120.00, '2024-02-01', 'Technology'),
('NVDA', 400.0, 450.00, '2024-02-15', 'Technology'),
('JPM', 900.0, 140.00, '2024-03-01', 'Financial'),
('JNJ', 1200.0, 150.00, '2024-03-15', 'Healthcare')
ON CONFLICT DO NOTHING;

-- Insert portfolio metadata
INSERT INTO dev_current_portfolio_metadata (cash_position, portfolio_name, target_allocation_strategy) VALUES
(125000.00, 'Main Portfolio', 'diversified_growth')
ON CONFLICT DO NOTHING;

-- Sample portfolio snapshots for backtests (recent dates)
INSERT INTO dev_portfolio_snapshots (backtest_run_id, snapshot_date, symbol, shares, price, market_value, weight, daily_pnl, daily_return, sector) VALUES
-- Comprehensive 2022-2025 strategy
('comprehensive_2022_2025', '2025-08-15', 'AAPL', 20000.0, 175.50, 3510000.0, 0.351, 35100.0, 0.01, 'Technology'),
('comprehensive_2022_2025', '2025-08-15', 'MSFT', 6500.0, 310.25, 2016625.0, 0.202, -20166.0, -0.01, 'Technology'),
('comprehensive_2022_2025', '2025-08-15', 'GOOGL', 14000.0, 140.80, 1971200.0, 0.197, 39424.0, 0.02, 'Technology'),
('comprehensive_2022_2025', '2025-08-15', 'AMZN', 11000.0, 135.75, 1493250.0, 0.149, -14932.0, -0.01, 'Consumer Discretionary'),
('comprehensive_2022_2025', '2025-08-15', 'TSLA', 3800.0, 260.00, 988000.0, 0.099, 19760.0, 0.02, 'Consumer Discretionary'),

-- Adaptive SR 2024 strategy  
('adaptive_sr_2024', '2024-06-28', 'AAPL', 800.0, 175.50, 140400.0, 0.140, 1404.0, 0.01, 'Technology'),
('adaptive_sr_2024', '2024-06-28', 'MSFT', 500.0, 310.25, 155125.0, 0.155, -1551.0, -0.01, 'Technology'),
('adaptive_sr_2024', '2024-06-28', 'NVDA', 300.0, 520.75, 156225.0, 0.156, 3124.0, 0.02, 'Technology'),
('adaptive_sr_2024', '2024-06-28', 'GOOGL', 800.0, 140.80, 112640.0, 0.113, 2252.0, 0.02, 'Technology'),
('adaptive_sr_2024', '2024-06-28', 'AMZN', 600.0, 135.75, 81450.0, 0.081, -814.0, -0.01, 'Consumer Discretionary'),
('adaptive_sr_2024', '2024-06-28', 'TSLA', 400.0, 260.00, 104000.0, 0.104, 2080.0, 0.02, 'Consumer Discretionary'),
('adaptive_sr_2024', '2024-06-28', 'META', 300.0, 480.00, 144000.0, 0.144, -1440.0, -0.01, 'Technology'),
('adaptive_sr_2024', '2024-06-28', 'JPM', 500.0, 155.60, 77800.0, 0.078, -778.0, -0.01, 'Financial'),

-- Momentum 2024 strategy
('momentum_2024', '2024-06-28', 'AAPL', 1000.0, 175.50, 175500.0, 0.176, 1755.0, 0.01, 'Technology'),
('momentum_2024', '2024-06-28', 'MSFT', 600.0, 310.25, 186150.0, 0.186, -1861.0, -0.01, 'Technology'),
('momentum_2024', '2024-06-28', 'NVDA', 300.0, 520.75, 156225.0, 0.156, 3124.0, 0.02, 'Technology'),
('momentum_2024', '2024-06-28', 'GOOGL', 600.0, 140.80, 84480.0, 0.084, 1689.0, 0.02, 'Technology'),
('momentum_2024', '2024-06-28', 'AMZN', 400.0, 135.75, 54300.0, 0.054, -543.0, -0.01, 'Consumer Discretionary'),
('momentum_2024', '2024-06-28', 'META', 200.0, 480.00, 96000.0, 0.096, -960.0, -0.01, 'Technology'),
('momentum_2024', '2024-06-28', 'TSLA', 200.0, 260.00, 52000.0, 0.052, 1040.0, 0.02, 'Consumer Discretionary'),
('momentum_2024', '2024-06-28', 'NFLX', 150.0, 620.00, 93000.0, 0.093, 930.0, 0.01, 'Technology'),
('momentum_2024', '2024-06-28', 'CRM', 300.0, 250.00, 75000.0, 0.075, -750.0, -0.01, 'Technology'),
('momentum_2024', '2024-06-28', 'ADBE', 100.0, 450.00, 45000.0, 0.045, 450.0, 0.01, 'Technology')
ON CONFLICT (backtest_run_id, snapshot_date, symbol) DO NOTHING;