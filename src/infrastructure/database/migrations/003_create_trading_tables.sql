-- Migration 003: Create trading and analytics tables from intg schema
-- Generated from current intg database schema on 2025-09-15

-- Update db_version
INSERT INTO db_version (version, description) VALUES 
(3, 'Trading tables - intervals, indicators, universe state')
ON CONFLICT (version) DO NOTHING;

-- Instrument interval table for time-series data
CREATE TABLE IF NOT EXISTS instrument_interval (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    universe_state_interval_id INTEGER, -- References universe_state_interval(id)
    instrument_id INTEGER NOT NULL REFERENCES instrument(id) ON DELETE CASCADE,
    datetime TIMESTAMP WITHOUT TIME ZONE, -- Legacy field
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    traded_volume DOUBLE PRECISION,
    traded_dollar DOUBLE PRECISION,
    status TEXT,
    market_cap DOUBLE PRECISION,
    interval_start TIMESTAMP WITHOUT TIME ZONE,
    interval_end TIMESTAMP WITHOUT TIME ZONE,
    interval_duration TEXT, -- '5m', '15m', '60m' etc.
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    -- Note: Removed UNIQUE constraint to allow idempotent operations
);

-- Instrument indicator interval table for technical indicators
CREATE TABLE IF NOT EXISTS instrument_indicator_interval (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id) ON DELETE CASCADE,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    indicator_name TEXT NOT NULL,
    indicator_value DOUBLE PRECISION,
    timeframe TEXT, -- '5m', '15m', '1h', '1d'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(run_id, instrument_id, datetime, indicator_name, timeframe)
);

-- Factor interval table for factor analysis
CREATE TABLE IF NOT EXISTS factor_interval (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    factor_name TEXT NOT NULL,
    factor_value DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(run_id, datetime, factor_name)
);

-- Universe state interval table for universe analytics
CREATE TABLE IF NOT EXISTS universe_state_interval (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    universe_id INTEGER NOT NULL REFERENCES universe(id),
    duration TEXT NOT NULL, -- '5m', '15m', '60m', '1h', '1d' etc.
    start_date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE, -- Optional legacy field
    state_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(run_id, universe_id, duration, start_date_time)
);

-- Runs table for tracking analysis runs
CREATE TABLE IF NOT EXISTS runs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    run_type TEXT NOT NULL, -- 'backtest', 'training', 'analysis', etc.
    status TEXT DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed'
    start_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    end_time TIMESTAMP WITH TIME ZONE,
    config JSONB,
    results JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Portfolio tables
CREATE TABLE IF NOT EXISTS portfolio_holdings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    position_size DOUBLE PRECISION NOT NULL,
    market_value DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(run_id, instrument_id, datetime)
);

CREATE TABLE IF NOT EXISTS portfolio_performance (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    total_value DOUBLE PRECISION NOT NULL,
    pnl DOUBLE PRECISION,
    daily_return DOUBLE PRECISION,
    cumulative_return DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    sharpe_ratio DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(run_id, datetime)
);

CREATE TABLE IF NOT EXISTS risk_metrics (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(run_id, datetime, metric_name)
);

-- Backtest tables
CREATE TABLE IF NOT EXISTS comprehensive_backtest_runs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    strategy_name TEXT,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital DOUBLE PRECISION DEFAULT 100000.0,
    final_value DOUBLE PRECISION,
    total_return DOUBLE PRECISION,
    annualized_return DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    sharpe_ratio DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    num_trades INTEGER DEFAULT 0,
    win_rate DOUBLE PRECISION,
    config JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    instrument_id INTEGER NOT NULL REFERENCES instrument(id),
    trade_date DATE NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('BUY', 'SELL')),
    quantity DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    commission DOUBLE PRECISION DEFAULT 0.0,
    total_value DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Trading indexes for performance
CREATE INDEX IF NOT EXISTS idx_instrument_interval_run_datetime ON instrument_interval(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_instrument_interval_instrument ON instrument_interval(instrument_id);
CREATE INDEX IF NOT EXISTS idx_instrument_indicator_interval_run_datetime ON instrument_indicator_interval(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_instrument_indicator_interval_instrument ON instrument_indicator_interval(instrument_id);
CREATE INDEX IF NOT EXISTS idx_instrument_indicator_interval_indicator ON instrument_indicator_interval(indicator_name);
CREATE INDEX IF NOT EXISTS idx_factor_interval_run_datetime ON factor_interval(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_universe_state_interval_run_datetime ON universe_state_interval(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_runs_run_id ON runs(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_run_type ON runs(run_type);
CREATE INDEX IF NOT EXISTS idx_portfolio_holdings_run_datetime ON portfolio_holdings(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_portfolio_performance_run_datetime ON portfolio_performance(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_risk_metrics_run_datetime ON risk_metrics(run_id, datetime);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_run_id ON backtest_trades(run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_trade_date ON backtest_trades(trade_date);