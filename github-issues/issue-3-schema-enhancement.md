# Phase 1: Enhanced Daily Prices Schema

**Labels**: `database`, `phase-1`, `schema`
**Status**: 🚨 CRITICAL - Schema mismatch blocking data quality

## Current Status (2025-08-25)
- ❌ **Schema Gap**: Current vendor tables missing PRD-required columns:
  - Missing: `adjusted_close`, `split_factor`, `dividend`, `data_vendor`, `quality_score`
  - Current: Only basic OHLCV + `vwap`, `transactions`
- ❌ **No Unified Table**: Data in separate vendor tables (`dev_polygon_prices`, `dev_tiingo_prices`, `dev_eodhd_prices`)
- ❌ **Blocks Data Quality**: Cannot implement cross-vendor reconciliation without unified schema
- ✅ Vendor-specific tables operational and storing data

## Description
Enhance the daily prices database schema to support advanced features including data quality scoring, vendor tracking, and corporate actions handling.

## Business Context
Current schema needs enhancement to support the 30-year historical system requirements including multi-vendor data sources, quality metrics, and corporate actions tracking.

## Acceptance Criteria
- [ ] Add `data_vendor` column to track data source (polygon, eodhd, alphavantage, tiingo)
- [ ] Add `quality_score` column (INTEGER 0-100) for data quality metrics
- [ ] Add `split_factor` column (DECIMAL 8,4) for stock split adjustments
- [ ] Add `dividend` column (DECIMAL 8,4) for dividend payments
- [ ] Ensure `adjusted_close` column properly handles corporate actions
- [ ] Add proper indexing for query performance optimization
- [ ] Implement TimescaleDB partitioning by year for storage efficiency

## Technical Requirements
```sql
CREATE TABLE dev_daily_prices (
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12,4),
    high DECIMAL(12,4), 
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    adjusted_close DECIMAL(12,4),
    split_factor DECIMAL(8,4) DEFAULT 1.0,
    dividend DECIMAL(8,4) DEFAULT 0.0,
    data_vendor VARCHAR(20),
    quality_score INTEGER DEFAULT 100,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (symbol, date)
);
```

## Migration Strategy
- Create database migration scripts
- Ensure zero-downtime deployment
- Backward compatibility with existing queries
- Data validation during migration

## Definition of Done
- [ ] Tests written and passing (TDD)
- [ ] Migration scripts created and tested
- [ ] Schema validation tests updated
- [ ] Performance benchmarks meet <100ms query requirements
- [ ] Integration tests pass in K8s environment
- [ ] End-to-end validation successful
- [ ] Documentation updated

## Estimated Timeline
1 week

## Related Documentation
- [PRD](docs/projects/30year-price-history/PRD_30_Year_Daily_Price_History.md) - See "Technical Requirements"
- [DRD](docs/projects/30year-price-history/DRD_30_Year_Daily_Price_History.md)