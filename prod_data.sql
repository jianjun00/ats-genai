--
-- Production database setup - converted from dev_data.sql
-- Replace dev_ prefixes with prod_
--

-- Create prod tables from dev structure
CREATE TABLE IF NOT EXISTS prod_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prod_products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category_id INTEGER,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prod_daily_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(10,4) NOT NULL,
    high_price DECIMAL(10,4) NOT NULL,
    low_price DECIMAL(10,4) NOT NULL,
    close_price DECIMAL(10,4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prod_training_dataset (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    symbol VARCHAR(10),
    volatility DECIMAL(8,2),
    trading_days INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS prod_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES prod_users(id),
    product_id INTEGER REFERENCES prod_products(id),
    quantity INTEGER NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'pending'
);

-- Insert data converted from dev to prod
INSERT INTO prod_users (id, username, email, created_at, updated_at) VALUES 
(1, 'john_doe', 'john@example.com', '2025-08-23 14:47:33.552166', '2025-08-23 14:47:33.552166'),
(2, 'jane_smith', 'jane@example.com', '2025-08-23 14:47:33.552166', '2025-08-23 14:47:33.552166'),
(3, 'bob_wilson', 'bob@example.com', '2025-08-23 14:47:33.552166', '2025-08-23 14:47:33.552166');

INSERT INTO prod_products (id, name, price, category_id, description, created_at) VALUES 
(1, 'Laptop Pro', 1299.99, 1, 'High-performance laptop', '2025-08-23 14:47:33.552166'),
(2, 'Wireless Mouse', 29.99, 2, 'Ergonomic wireless mouse', '2025-08-23 14:47:33.552166'),
(3, 'Mechanical Keyboard', 149.99, 2, 'RGB mechanical keyboard', '2025-08-23 14:47:33.552166');

INSERT INTO prod_daily_prices (id, symbol, date, open_price, high_price, low_price, close_price, volume, created_at) VALUES 
(1, 'AAPL', '2025-01-01', 185.5000, 187.2500, 184.1000, 186.7500, 45000000, '2025-08-23 14:47:33.552166'),
(2, 'AAPL', '2025-01-02', 186.8000, 188.9000, 185.3000, 187.4500, 52000000, '2025-08-23 14:47:33.552166'),
(3, 'MSFT', '2025-01-01', 425.2000, 428.5000, 423.8000, 427.1000, 28000000, '2025-08-23 14:47:33.552166'),
(4, 'MSFT', '2025-01-02', 427.3000, 429.7500, 425.9000, 428.6500, 31000000, '2025-08-23 14:47:33.552166'),
(5, 'TSLA', '2025-01-01', 245.8000, 248.2000, 243.5000, 246.9000, 75000000, '2025-08-23 14:47:33.552166');

INSERT INTO prod_orders (id, user_id, product_id, quantity, total_amount, created_at, status) VALUES 
(1, 1, 1, 1, 1299.99, '2025-08-23 14:47:33.552166', 'completed'),
(2, 2, 2, 2, 59.98, '2025-08-23 14:47:33.552166', 'pending'),
(3, 3, 3, 1, 149.99, '2025-08-23 14:47:33.552166', 'shipped'),
(4, 1, 2, 1, 29.99, '2025-08-23 14:47:33.552166', 'completed');

INSERT INTO prod_training_dataset (id, name, symbol, volatility, trading_days, created_at, status) VALUES 
(1, 'aapl_2024_daily', 'AAPL', 15.60, 252, '2025-08-23 14:47:33.552166', 'active'),
(2, 'msft_2024_daily', 'MSFT', 14.80, 252, '2025-08-23 14:47:33.552166', 'active'),
(3, 'tsla_2024_daily', 'TSLA', 16.20, 252, '2025-08-23 14:47:33.552166', 'active'),
(4, 'combined_tech_stocks', NULL, 125.40, 2520, '2025-08-23 14:47:33.552166', 'active');

-- Update sequences to match the inserted data
SELECT setval('prod_users_id_seq', 3, true);
SELECT setval('prod_products_id_seq', 3, true);  
SELECT setval('prod_daily_prices_id_seq', 5, true);
SELECT setval('prod_orders_id_seq', 4, true);
SELECT setval('prod_training_dataset_id_seq', 4, true);