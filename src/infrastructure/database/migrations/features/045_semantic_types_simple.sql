-- Semantic Types Schema Migration (Simple Version)
-- Creates PostgreSQL ENUMs and semantic types for ATS dev tables

-- ================================================
-- 1. CREATE ENUM TYPES
-- ================================================

-- Create instrument type ENUM (normalize case differences)
CREATE TYPE instrument_type_enum AS ENUM (
    'Stock',      -- Common stocks (normalize 'stock' → 'Stock')
    'ETF',        -- Exchange Traded Funds
    'PFD',        -- Preferred Stock
    'WARRANT',    -- Warrants
    'CS',         -- Common Stock (alternative)
    'SP',         -- Special Purpose
    'UNIT',       -- Unit Investment Trust
    'ADRC',       -- American Depositary Receipt
    'RIGHT',      -- Rights
    'REIT',       -- Real Estate Investment Trust
    'FUND',       -- Mutual Funds
    'INDEX'       -- Index Securities
);

-- Create exchange code ENUM
CREATE TYPE exchange_code_enum AS ENUM (
    'NASDAQ',     -- NASDAQ Global Market
    'NYSE',       -- New York Stock Exchange  
    'NYSE_ARCA',  -- NYSE Arca (normalize 'NYSE ARCA')
    'BATS',       -- BATS Exchange
    'XNYS',       -- NYSE (alternative code)
    'NYSE_MKT',   -- NYSE MKT (normalize 'NYSE MKT')
    'XNAS',       -- NASDAQ (alternative code)
    'AMEX',       -- American Stock Exchange
    'XASE',       -- AMEX (alternative code)
    'NYSE_NAT',   -- NYSE National (normalize 'NYSE NAT')
    'CBOE',       -- CBOE Exchange
    'IEX',        -- Investors Exchange
    'EDGX',       -- EDGX Exchange
    'EDGA'        -- EDGA Exchange
);

-- Create currency code ENUM (ISO 4217 standard)
CREATE TYPE currency_code_enum AS ENUM (
    'USD',        -- US Dollar (normalize 'usd' → 'USD')
    'CAD',        -- Canadian Dollar
    'EUR',        -- Euro
    'GBP',        -- British Pound
    'JPY',        -- Japanese Yen
    'CHF',        -- Swiss Franc
    'AUD',        -- Australian Dollar
    'NZD'         -- New Zealand Dollar
);

-- Create vendor ENUM
CREATE TYPE vendor_enum AS ENUM (
    'POLYGON',
    'TIINGO', 
    'EODHD',
    'QUANDL',
    'FMP',
    'ALPHA_VANTAGE',
    'IEX',
    'YAHOO',
    'BLOOMBERG',
    'REFINITIV'
);