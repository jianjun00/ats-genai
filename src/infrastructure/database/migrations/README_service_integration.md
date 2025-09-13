# Service-Based Schema Integration

## Overview

This document describes the integration of service-based database initialization into the unified migration manager system.

## Migration Changes

### Replaced: `deployment/scripts/init-databases.sql`

The standalone `init-databases.sql` script has been replaced with proper migration:

**New Migration**: `072_create_service_based_schema.sql`

### Key Improvements

1. **Unified Database Approach**: Instead of creating separate databases for each service, we now use schemas within the unified database
2. **Environment-Specific Prefixes**: Tables automatically get environment prefixes (test_, dev_, intg_) via migration manager
3. **Proper Migration Tracking**: All changes are tracked in the `db_version` table
4. **Rollback Support**: Schema changes can be properly managed and rolled back if needed

### Service Schemas Created

#### 1. Instruments Service
- `vendor_instruments` - Vendor-specific instrument data
- `instrument_xrefs` - Cross-reference mappings between vendors
- `unified_instruments` - Unified instrument master data

#### 2. Analytics Service  
- `technical_indicators` - Technical analysis data
- `performance_metrics` - Performance analytics

#### 3. Trading Service
- `portfolios` - Portfolio management
- `positions` - Position tracking
- `orders` - Order management

#### 4. News Service
- `articles` - News article storage
- `sentiment_analysis` - Sentiment analysis results

#### 5. Service Registry
- `services` - Service registration and discovery
- `health_checks` - Service health monitoring

## Running the Migration

### Development Environment
```bash
PYTHONPATH=src DB_URL="postgresql://postgres:dev_password@localhost:3432/dev_db" python3 src/infrastructure/database/migration_manager.py migrate --environment dev
```

### Integration Environment  
```bash
PYTHONPATH=src DB_URL="postgresql://postgres:intg_password@localhost:4432/intg_db" python3 src/infrastructure/database/migration_manager.py migrate --environment intg
```

## Verification

After running the migration, verify the service schema:

```sql
-- Check created schemas
SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('instruments', 'analytics', 'trading', 'news', 'service_registry');

-- Check service registry
SELECT service_name, instance_id, host, port, status FROM dev_services;  -- or intg_services

-- Check service health overview
SELECT * FROM dev_service_health_overview;  -- or intg_service_health_overview
```

## Service Integration

### Service Registration

Services automatically register in the `services` table with:
- Service name and instance ID
- Host and port information  
- Version and protocol details
- Health status and heartbeat

### Health Monitoring

Health checks are tracked in the `health_checks` table:
- Service-specific health check results
- Check duration and status
- Detailed health information

### Development Services

The migration automatically registers the development and integration services:

**Development Environment (3000-4000 ports)**:
- market-data-service: localhost:3012
- trading-service: localhost:3013  
- monitoring-dashboard: localhost:3014

**Integration Environment (4000-5000 ports)**:
- market-data-service: localhost:4012
- trading-service: localhost:4013
- monitoring-dashboard: localhost:4014

## Migration Benefits

1. **Consistency**: All database changes go through migration manager
2. **Environment Isolation**: Proper table prefixing prevents conflicts
3. **Tracking**: All schema changes are versioned and tracked
4. **Rollback**: Schema changes can be rolled back if needed
5. **Development Ready**: Proper database initialization for service architecture

## Deprecation Notice

⚠️ **The `deployment/scripts/init-databases.sql` file should no longer be used.**

Use the migration manager instead:
- All service schema initialization is now handled by migration 072
- Environment-specific table prefixes are applied automatically
- Proper version tracking and rollback support is available

This ensures consistency with the rest of the ATS platform's database management approach.