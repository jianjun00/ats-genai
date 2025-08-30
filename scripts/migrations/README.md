# Database Migrations

This directory contains database migration scripts for the ATS platform.

## Migration Naming Convention

Migrations follow the pattern: `{number}_{description}.sql`

- **001_**: Semantic types and metadata enhancements
- **002_**: Financial events schema creation

## Available Migrations

### 001 - Semantic Types Migration
- `001_semantic_types_migration.sql` - Main semantic types migration
- `001_semantic_types_migration_fixed.sql` - Fixed version for existing schemas
- `001_semantic_types_simple.sql` - Simplified version without complex constraints
- `001_populate_semantic_metadata.sql` - Populate semantic metadata after schema changes

### 002 - Financial Events Schema
- `002_create_financial_events_schema.sql` - Main financial events schema
- `002_create_financial_events_schema_fixed.sql` - Version compatible with existing dev_instruments
- `002_create_financial_events_schema_simple.sql` - Simplified version without foreign key constraints

## Running Migrations

Use the migration runner scripts:

```bash
# Run specific migration
python scripts/run_migrations.py --migration 001

# Run all migrations
python scripts/run_migrations.py --all

# Check migration status
python scripts/run_migrations.py --status
```

## Migration Development Guidelines

1. **Always use numbered prefixes** for proper ordering
2. **Include both creation and rollback** capabilities when possible
3. **Test migrations on dev environment** before applying to production
4. **Document breaking changes** in migration comments
5. **Use idempotent operations** (CREATE IF NOT EXISTS, DROP IF EXISTS)

## Database Snapshots

Database snapshots are stored in `/db/snapshot/` for backup and recovery purposes.