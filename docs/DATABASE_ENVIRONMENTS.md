# 🗄️ Database Environments Guide

**Complete database setup, connection, and management for ATS development and integration environments.**

---

## 🎯 Environment Overview

The ATS platform uses Docker-based PostgreSQL databases for development and integration testing, managed through `run_dev.py` and `run_intg.py` scripts.

### **Environment Configuration**

| Environment | Database | Host | Port | User | Password | Tables Prefix |
|-------------|----------|------|------|------|----------|---------------|
| **Development (ats-dev)** | `dev_db` | `localhost` | `5432` | `postgres` | `dev_password` | `dev_*` |
| **Integration (ats-intg)** | `intg_db` | `localhost` | `5433` | `postgres` | `intg_password` | `intg_*` |

---

## 🚀 Quick Start

### **Development Environment Setup**

```bash
# Start development database
python scripts/run_dev.py start --service postgres

# Verify connection
python scripts/run_dev.py query --query "SELECT version()"

# Connect with psql
PGPASSWORD=dev_password psql -h localhost -p 5432 -U postgres -d dev_db
```

### **Integration Environment Setup**

```bash
# Start integration database  
python scripts/run_intg.py start --service postgres

# Verify connection
python scripts/run_intg.py query --query "SELECT version()"

# Connect with psql
PGPASSWORD=intg_password psql -h localhost -p 5433 -U postgres -d intg_db
```

---

## 💾 Database Schema and Tables

### **Core Data Tables**

#### **Daily Price Data**
```sql
-- Development: dev_daily_prices
-- Integration: intg_daily_prices
CREATE TABLE dev_daily_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,4),
    high DECIMAL(10,4), 
    low DECIMAL(10,4),
    close DECIMAL(10,4),
    volume BIGINT,
    adjusted_close DECIMAL(10,4),
    instrument_id INTEGER,
    vendor VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **Instruments Data**
```sql
-- Development: dev_instruments  
-- Integration: intg_instruments
CREATE TABLE dev_instruments (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    name TEXT,
    exchange VARCHAR(20),
    sector VARCHAR(50),
    industry VARCHAR(100),
    active BOOLEAN DEFAULT true,
    instrument_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **News Data Tables**
```sql
-- Polygon News: dev_news_polygon / intg_news_polygon
CREATE TABLE dev_news_polygon (
    id SERIAL PRIMARY KEY,
    polygon_id TEXT NOT NULL UNIQUE,
    title TEXT,
    description TEXT,
    published_utc TIMESTAMP WITH TIME ZONE,
    article_url TEXT,
    image_url TEXT,
    author TEXT,
    tickers TEXT[],
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tiingo News: dev_news_tiingo / intg_news_tiingo  
CREATE TABLE dev_news_tiingo (
    id SERIAL PRIMARY KEY,
    tiingo_id CHARACTER VARYING NOT NULL UNIQUE,
    title TEXT NOT NULL,
    description TEXT,
    published_date TIMESTAMP WITH TIME ZONE,
    article_url TEXT,
    source TEXT,
    author TEXT,
    tickers TEXT[],
    tags TEXT[],
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **Fundamentals Data**
```sql
-- Tiingo Fundamentals: dev_tiingo_fundamentals_daily
CREATE TABLE dev_tiingo_fundamentals_daily (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    instrument_id INTEGER,
    market_cap DOUBLE PRECISION,
    enterprise_val DOUBLE PRECISION,
    pe_ratio DOUBLE PRECISION,
    pb_ratio DOUBLE PRECISION,
    trail_pe_ratio DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol)
);
```

---

## 🔧 Database Operations

### **Common Database Commands**

```bash
# Development Environment
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py query --query "SELECT DISTINCT vendor FROM dev_daily_prices"
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments WHERE active = true"

# Integration Environment  
python scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_daily_prices"
python scripts/run_intg.py query --query "SELECT DISTINCT vendor FROM intg_daily_prices"

# Data Collection Scripts
python scripts/run_dev.py run --script scripts/tiingo_30_year_fundamentals_backfill.py
python scripts/run_dev.py run --script scripts/tiingo_30_year_news_backfill.py
python scripts/run_dev.py run --script scripts/eodhd_daily_price_backfill.py
```

### **Data Validation Queries**

```sql
-- Check data quality and coverage
SELECT 
    vendor,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM dev_daily_prices 
GROUP BY vendor 
ORDER BY total_records DESC;

-- Validate instruments data
SELECT 
    active,
    COUNT(*) as count,
    COUNT(DISTINCT exchange) as exchanges
FROM dev_instruments 
GROUP BY active;

-- Check news data coverage
SELECT 
    COUNT(*) as total_articles,
    COUNT(DISTINCT UNNEST(tickers)) as unique_symbols,
    MIN(published_utc) as earliest_news,
    MAX(published_utc) as latest_news
FROM dev_news_polygon 
WHERE published_utc IS NOT NULL;
```

---

## 📊 Data Collection Status

### **Current Data Coverage (ats-dev)**

| Dataset | Records | Symbols | Date Range | Vendors |
|---------|---------|---------|------------|---------|
| **Daily Prices** | 7,544+ | 1+ | 1995-2025 | EODHD, Polygon, Tiingo |
| **News Articles** | 130,000+ | 9,000+ | 2016-2025 | Polygon, Tiingo |
| **Fundamentals** | 30+ | 30 (DOW) | 1995-2025 | Tiingo |
| **Instruments** | 20,657+ | All | Active | Multiple |

### **Vendor-Specific Collections**

```bash
# EODHD Daily Price Backfill
EODHD_API_KEY=your_key python scripts/run_dev.py run --script scripts/eodhd_daily_price_backfill.py

# Tiingo 30-Year Fundamentals (DOW 30 only)
TIINGO_API_KEY=your_key python scripts/run_dev.py run --script scripts/tiingo_30_year_fundamentals_backfill.py

# Tiingo News Collection  
TIINGO_API_KEY=your_key python scripts/run_dev.py run --script scripts/tiingo_30_year_news_backfill.py

# Polygon News Collection
POLYGON_API_KEY=your_key python scripts/run_dev.py run --script scripts/polygon_30_year_news_backfill.py
```

---

## 🔍 Database Monitoring and Maintenance

### **Health Checks**

```sql
-- Database size and table statistics
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    most_common_vals
FROM pg_stats 
WHERE tablename LIKE 'dev_%' 
ORDER BY tablename, attname;

-- Index usage analysis
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE schemaname = 'public' 
AND tablename LIKE 'dev_%'
ORDER BY idx_scan DESC;
```

### **Performance Optimization**

```sql
-- Create essential indexes for performance
CREATE INDEX IF NOT EXISTS idx_dev_daily_prices_symbol_date 
ON dev_daily_prices (symbol, date DESC);

CREATE INDEX IF NOT EXISTS idx_dev_daily_prices_vendor_date 
ON dev_daily_prices (vendor, date DESC);

CREATE INDEX IF NOT EXISTS idx_dev_instruments_symbol 
ON dev_instruments (symbol) WHERE active = true;

-- Analyze tables for query optimization
ANALYZE dev_daily_prices;
ANALYZE dev_instruments;
ANALYZE dev_news_polygon;
ANALYZE dev_news_tiingo;
```

---

## 🚨 Critical Database Information

### **DOW 30 Restriction (Tiingo Fundamentals)**
- **Important**: Tiingo fundamentals API is limited to DOW 30 companies only for Free/Power plans
- **DOW 30 Symbols**: AAPL, MSFT, UNH, GS, HD, CAT, AMGN, MCD, CRM, V, BA, JPM, JNJ, HON, AXP, PG, CVX, IBM, MRK, DIS, WMT, MMM, TRV, NKE, KO, DOW, CSCO, INTC, WBA, VZ

### **Critical Bug Fix Applied (Tiingo News)**
- **Issue**: Integer ID conversion to string for database compatibility
- **Fix**: Added `str()` conversion in `standardize_tiingo_article` method
- **Impact**: Fixed 100% insertion failure rate for Tiingo news articles

### **Idempotent Operations**
All data collection scripts use `ON CONFLICT DO UPDATE` for safe re-execution:

```sql
INSERT INTO dev_daily_prices 
(symbol, date, open, high, low, close, volume, vendor)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (symbol, date, vendor) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    updated_at = CURRENT_TIMESTAMP;
```

---

## 🔧 Troubleshooting

### **Common Connection Issues**

```bash
# Database not started
python scripts/run_dev.py start --service postgres

# Check if ports are available  
netstat -tuln | grep 5432
netstat -tuln | grep 5433

# Reset database if corrupted
python scripts/run_dev.py stop --service postgres
python scripts/run_dev.py start --service postgres --reset
```

### **Data Validation**

```bash
# Run comprehensive validation tests
PYTHONPATH=src pytest tests/integration/test_tiingo_fundamentals_collection.py -v
PYTHONPATH=src pytest tests/integration/test_tiingo_news_collection.py -v  
PYTHONPATH=src pytest tests/integration/test_news_data_analysis.py -v
```

---

**🎯 This database guide provides complete information for working with ATS development and integration environments using Docker-based PostgreSQL databases managed through run_dev.py and run_intg.py.**