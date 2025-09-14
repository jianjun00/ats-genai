-- Migration 072: Service-based schema enhancement
-- Integrates service-oriented database structure into unified migration system
-- Replaces deployment/scripts/init-databases.sql with proper migration approach

-- Create service schemas within the current database
-- This follows the migration manager's unified database approach with environment-specific prefixes

-- Instruments Service Schema
CREATE SCHEMA IF NOT EXISTS instruments;

CREATE TABLE IF NOT EXISTS vendor_instruments (
    id SERIAL PRIMARY KEY,
    vendor_name VARCHAR(50) NOT NULL,
    vendor_symbol VARCHAR(100) NOT NULL,
    instrument_name VARCHAR(200),
    exchange VARCHAR(50),
    currency VARCHAR(10),
    instrument_type VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vendor_name, vendor_symbol)
);

CREATE TABLE IF NOT EXISTS instrument_xrefs (
    id SERIAL PRIMARY KEY,
    vendor_symbol VARCHAR(100) NOT NULL,
    vendor_name VARCHAR(50) NOT NULL,
    unified_symbol VARCHAR(100) NOT NULL,
    mapping_type VARCHAR(50) DEFAULT 'primary',
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(vendor_name, vendor_symbol, unified_symbol)
);

CREATE TABLE IF NOT EXISTS unified_instruments (
    id SERIAL PRIMARY KEY,
    unified_symbol VARCHAR(100) UNIQUE NOT NULL,
    primary_name VARCHAR(200) NOT NULL,
    instrument_type VARCHAR(50),
    primary_exchange VARCHAR(50),
    primary_currency VARCHAR(10),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap_category VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for instruments performance
CREATE INDEX IF NOT EXISTS idx_vendor_instruments_vendor ON vendor_instruments(vendor_name, vendor_symbol);
CREATE INDEX IF NOT EXISTS idx_vendor_instruments_symbol ON vendor_instruments(vendor_symbol);
CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_vendor ON instrument_xrefs(vendor_name, vendor_symbol);
CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_unified ON instrument_xrefs(unified_symbol);
CREATE INDEX IF NOT EXISTS idx_unified_instruments_symbol ON unified_instruments(unified_symbol);

-- Analytics Service Schema
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(100) NOT NULL,
    indicator_type VARCHAR(50) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    calculation_date DATE NOT NULL,
    indicator_values JSONB NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, indicator_type, timeframe, calculation_date)
);

CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(100) NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    metric_value DECIMAL(15,6),
    metric_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, metric_type, period_start, period_end)
);

-- Create indexes for analytics performance
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol ON technical_indicators(symbol, indicator_type, timeframe);
CREATE INDEX IF NOT EXISTS idx_performance_metrics_symbol ON performance_metrics(symbol, metric_type);

-- Trading Service Schema
CREATE SCHEMA IF NOT EXISTS trading;

CREATE TABLE IF NOT EXISTS portfolios (
    id SERIAL PRIMARY KEY,
    portfolio_name VARCHAR(200) NOT NULL,
    strategy_type VARCHAR(100),
    base_currency VARCHAR(10) DEFAULT 'USD',
    initial_capital DECIMAL(15,2),
    current_value DECIMAL(15,2),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    portfolio_id INTEGER REFERENCES portfolios(id),
    symbol VARCHAR(100) NOT NULL,
    position_type VARCHAR(10) NOT NULL, -- 'long' or 'short'
    quantity DECIMAL(15,6) NOT NULL,
    avg_entry_price DECIMAL(15,6) NOT NULL,
    current_price DECIMAL(15,6),
    unrealized_pnl DECIMAL(15,2),
    realized_pnl DECIMAL(15,2) DEFAULT 0,
    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    portfolio_id INTEGER REFERENCES portfolios(id),
    symbol VARCHAR(100) NOT NULL,
    order_type VARCHAR(20) NOT NULL, -- 'market', 'limit', 'stop'
    side VARCHAR(10) NOT NULL, -- 'buy' or 'sell'
    quantity DECIMAL(15,6) NOT NULL,
    price DECIMAL(15,6),
    stop_price DECIMAL(15,6),
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'filled', 'cancelled'
    filled_quantity DECIMAL(15,6) DEFAULT 0,
    avg_fill_price DECIMAL(15,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for trading performance
CREATE INDEX IF NOT EXISTS idx_positions_portfolio ON positions(portfolio_id, symbol);
CREATE INDEX IF NOT EXISTS idx_orders_portfolio ON orders(portfolio_id, status);

-- News Service Schema  
CREATE SCHEMA IF NOT EXISTS news;

CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    content TEXT,
    source VARCHAR(100) NOT NULL,
    author VARCHAR(200),
    published_at TIMESTAMP NOT NULL,
    url VARCHAR(1000),
    sentiment_score DECIMAL(3,2), -- -1 to 1
    relevance_score DECIMAL(3,2), -- 0 to 1
    symbols TEXT[], -- Array of related symbols
    keywords TEXT[],
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(url)
);

CREATE TABLE IF NOT EXISTS sentiment_analysis (
    id SERIAL PRIMARY KEY,
    article_id INTEGER REFERENCES articles(id),
    symbol VARCHAR(100) NOT NULL,
    sentiment_score DECIMAL(3,2) NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL,
    analysis_method VARCHAR(50) NOT NULL,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(article_id, symbol)
);

-- Create indexes for news performance
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_symbols ON articles USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_sentiment_symbol ON sentiment_analysis(symbol, sentiment_score);

-- Service Registry Schema
CREATE SCHEMA IF NOT EXISTS service_registry;

CREATE TABLE IF NOT EXISTS services (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    instance_id VARCHAR(200) NOT NULL,
    version VARCHAR(50) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    protocol VARCHAR(10) DEFAULT 'http',
    path VARCHAR(255) DEFAULT '/',
    metadata JSONB,
    status VARCHAR(20) DEFAULT 'starting',
    last_heartbeat TIMESTAMP,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(service_name, instance_id)
);

CREATE TABLE IF NOT EXISTS health_checks (
    id SERIAL PRIMARY KEY,
    service_id INTEGER REFERENCES services(id),
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    details JSONB,
    duration_ms DECIMAL(10,3),
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for service registry performance
CREATE INDEX IF NOT EXISTS idx_services_name ON services(service_name);
CREATE INDEX IF NOT EXISTS idx_services_status ON services(status, last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_health_checks_service ON health_checks(service_id, checked_at);

-- Insert service registry data for dev and intg environment services
INSERT INTO services (service_name, instance_id, version, host, port, protocol, path, status) VALUES
('market-data-service', 'dev-instance', '1.0.0', 'localhost', 3012, 'http', '/health', 'healthy'),
('trading-service', 'dev-instance', '1.0.0', 'localhost', 3013, 'http', '/health', 'healthy'),
('monitoring-dashboard', 'dev-instance', '1.0.0', 'localhost', 3014, 'http', '/health', 'healthy'),
('market-data-service', 'intg-instance', '1.0.0', 'localhost', 4012, 'http', '/health', 'healthy'),
('trading-service', 'intg-instance', '1.0.0', 'localhost', 4013, 'http', '/health', 'healthy'),
('monitoring-dashboard', 'intg-instance', '1.0.0', 'localhost', 4014, 'http', '/health', 'healthy')
ON CONFLICT (service_name, instance_id) DO UPDATE SET
    version = EXCLUDED.version,
    host = EXCLUDED.host,
    port = EXCLUDED.port,
    status = EXCLUDED.status,
    registered_at = CURRENT_TIMESTAMP;

-- Create a view for service health overview
CREATE OR REPLACE VIEW service_health_overview AS
SELECT 
    s.service_name,
    s.instance_id,
    s.version,
    s.host,
    s.port,
    s.status,
    s.last_heartbeat,
    COUNT(hc.id) as total_health_checks,
    COUNT(CASE WHEN hc.status = 'healthy' THEN 1 END) as healthy_checks,
    MAX(hc.checked_at) as last_health_check
FROM services s
LEFT JOIN health_checks hc ON s.id = hc.service_id
GROUP BY s.id, s.service_name, s.instance_id, s.version, s.host, s.port, s.status, s.last_heartbeat
ORDER BY s.service_name, s.instance_id;