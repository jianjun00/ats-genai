# Database Migrations

This directory contains database migration scripts for the ATS platform.

## Migration Naming Convention

Migrations follow the pattern: `{number}_{description}.sql`

Current sequence: **000-047**

## Recent Additions (045-047)

### 045 - Semantic Types Migration
- `045_semantic_types_migration.sql` - Main semantic types migration
- `045_semantic_types_migration_fixed.sql` - Fixed version for existing schemas
- `045_semantic_types_simple.sql` - Simplified version without complex constraints

### 046 - Semantic Metadata Population
- `046_populate_semantic_metadata.sql` - Populate semantic metadata after schema changes

### 047 - Financial Events Schema
- `047_create_financial_events_schema.sql` - Main financial events schema
- `047_create_financial_events_schema_fixed.sql` - Version compatible with existing dev_instruments
- `047_create_financial_events_schema_simple.sql` - Simplified version without foreign key constraints

## Running Migrations

Use the migration manager:

```bash
# Run specific migration
python src/db/migration_manager.py --migration 045

# Run all pending migrations
python src/db/migration_manager.py --all

# Check migration status
python src/db/migration_manager.py --status
```

## Migration Development Guidelines

1. **Use sequential numbering** starting from the next available number
2. **Include both creation and rollback** capabilities when possible
3. **Test migrations on dev environment** before applying to production
4. **Document breaking changes** in migration comments
5. **Use idempotent operations** (CREATE IF NOT EXISTS, DROP IF EXISTS)
6. **Place all migration files** in `src/db/migrations/`

## Database Snapshots

Database snapshots are stored in `db/snapshot/` for backup and recovery purposes.