#!/usr/bin/env python3
"""Script to check the schema of the instrument_xrefs table."""

import asyncio
import asyncpg
import os
from config.environment import Environment, EnvironmentType

async def check_table(conn, table_name):
    """Check schema for a specific table."""
    print(f"\n\n=== Checking table: {table_name} ===")
    
    # Check if table exists
    table_exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
        )
        """,
        table_name
    )
    
    if not table_exists:
        print(f"Table {table_name} does not exist.")
        return
            
    # Get table constraints
    print("\n=== Table Constraints ===")
    constraints = await conn.fetch(
        """
        SELECT 
            tc.constraint_name, 
            tc.constraint_type,
            tc.table_name,
            array_agg(kcu.column_name) as columns
        FROM 
            information_schema.table_constraints tc
        JOIN 
            information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_name = kcu.table_name
        WHERE 
            tc.table_name = $1
        GROUP BY 
            tc.constraint_name, tc.constraint_type, tc.table_name
        """,
        table_name
    )
    
    if not constraints:
        print("No constraints found for table")
    else:
        for row in constraints:
            print(f"\nConstraint: {row['constraint_name']}")
            print(f"Type: {row['constraint_type']}")
            print(f"Columns: {row['columns']}")
    
    # Get table columns
    print("\n=== Table Columns ===")
    columns = await conn.fetch(
        """
        SELECT 
            column_name, 
            data_type,
            is_nullable,
            column_default
        FROM 
            information_schema.columns
        WHERE 
            table_name = $1
        ORDER BY 
            ordinal_position
        """,
        table_name
    )
    
    print("\nColumns in table:")
    for col in columns:
        print(f"- {col['column_name']}: {col['data_type']} "
              f"(Nullable: {col['is_nullable']}, Default: {col['column_default']})")
    
    # Get indexes
    print("\n=== Indexes ===")
    indexes = await conn.fetch(
        """
        SELECT 
            i.relname as index_name,
            a.attname as column_name,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary
        FROM 
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        WHERE 
            t.oid = ix.indrelid
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relname = $1
        ORDER BY 
            i.relname, a.attnum
        """,
        table_name
    )
    
    if not indexes:
        print("No indexes found for table")
    else:
        print("\nIndexes on table:")
        current_index = None
        for idx in indexes:
            if idx['index_name'] != current_index:
                current_index = idx['index_name']
                index_type = []
                if idx['is_primary']:
                    index_type.append("PRIMARY KEY")
                if idx['is_unique']:
                    index_type.append("UNIQUE")
                print(f"\n- {current_index} ({', '.join(index_type) if index_type else 'INDEX'})")
            print(f"  - {idx['column_name']}")

async def get_schema():
    # Connect to the database
    db_url = os.environ.get('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/postgres')
    env = Environment(EnvironmentType.TEST, db_url=db_url)
    
    # Get the actual database name from the URL
    db_name = db_url.split('/')[-1].split('?')[0]
    print(f"Connecting to database: {db_name}")
    
    # Get the table prefix from environment
    table_prefix = 'test_'  # Default test prefix
    
    # Check both prefixed and non-prefixed table names
    table_names = [f'{table_prefix}instrument_xrefs', 'instrument_xrefs']
    
    conn = await asyncpg.connect(env.get_database_url())
    
    try:
        # First, list all tables in the database
        print("\n=== All Tables in Database ===")
        tables = await conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
        )
        print("\n".join([f"- {t['table_name']}" for t in tables]))
        
        # Check each potential table name
        for table_name in table_names:
            await check_table(conn, table_name)
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(get_schema())
