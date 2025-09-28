#!/usr/bin/env python3
"""Apply training dataset schema migration."""

import asyncio
import asyncpg
import sys

async def apply_migration():
    """Apply the database migration for enhanced schema support."""

    # Connect to dev database
    conn = await asyncpg.connect(
        host='ats-dev-postgres',
        port=5432,
        user='postgres',
        password='dev_password',
        database='dev_db'
    )

    print('📊 Applying training dataset schema migration...')

    # Read and execute migration
    with open('/workspace/src/db/migrations/050_enhance_training_dataset_schema_support.sql', 'r') as f:
        migration_sql = f.read()

    # Split migration into individual statements
    statements = [stmt.strip() for stmt in migration_sql.split(';') if stmt.strip()]

    for i, statement in enumerate(statements):
        if statement:
            await conn.execute(statement)
            print(f'✅ Statement {i+1}/{len(statements)} executed')
    print('✅ Migration completed successfully')

    # Verify schema registry table exists
    tables = await conn.fetch("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE '%training_schema_registry%'
    """)

    if tables:
        print(f'📋 Schema registry tables created: {[t["table_name"] for t in tables]}')
    else:
        print('❌ No schema registry tables found')

    # Check new columns in training_datasets table
    columns = await conn.fetch("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'dev_training_datasets'
        AND column_name LIKE '%schema%'
    """)

    print(f'📋 Schema columns added: {[(c["column_name"], c["data_type"]) for c in columns]}')

    return 0

if __name__ == '__main__':
    sys.exit(asyncio.run(apply_migration()))