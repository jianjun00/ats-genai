# Database Scripts

This directory contains scripts for database management, migration, and verification for the ATS-GenAI project.

## Key Scripts

- **backup_db.py** - Backs up database data
- **db_seed_tsla_membership.py** - Seeds TSLA membership data
- **delete_test_symbols.py** - Deletes test symbols from the database
- **drop_empty_tables.py** - Drops empty tables from the database
- **inspect_database.py** - Inspects database structure and contents
- **inspect_existing_db.py** - Inspects an existing database
- **lookup_instrument_id.py** - Looks up instrument IDs
- **migrate_intg_to_prod.py** - Migrates data from integration to production
- **migrate_test_intg_only.py** - Migrates test data to integration
- **migrate_to_environment_structure.py** - Migrates data to environment structure
- **verify_db_data.py** - Verifies database data
- **verify_db_setup.py** - Verifies database setup

## Connection Testing

- **test_ats_dev_db_connection.py** - Tests connection to ATS dev database
- **test_db_connection_params.py** - Tests database connection parameters

## DB Checks

The `db_checks/` directory contains various scripts for checking database connectivity, schema, and data integrity.
