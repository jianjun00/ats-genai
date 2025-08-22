#!/usr/bin/env python3
"""Simple script to check database state."""

import asyncio
import asyncpg
import os

async def list_databases():
    """List all databases."""
    conn = await asyncpg.connect('postgresql://postgres:password@localhost:5432/postgres')
    try:
        dbs = await conn.fetch('SELECT datname FROM pg_database WHERE datistemplate = false')
        print("\n=== Available Databases ===")
        for db in dbs:
            print(f"- {db['datname']}")
        
        # Check for test databases
        test_dbs = [db['datname'] for db in dbs if db['datname'].startswith('test_')]
        print("\n=== Test Databases ===")
        for db in test_dbs:
            print(f"- {db}")
            await check_database(db)
            
    finally:
        await conn.close()

async def check_database(db_name):
    """Check the state of a specific database."""
    print(f"\n=== Checking database: {db_name} ===")
    
    try:
        conn = await asyncpg.connect(f'postgresql://postgres:password@localhost:5432/{db_name}')
        try:
            # Check if migrations table exists
            has_migrations = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'db_version')"
            )
            
            if has_migrations:
                try:
                    version = await conn.fetchval('SELECT version FROM db_version')
                    print(f"Migration version: {version}")
                except Exception as e:
                    print(f"Error getting migration version: {e}")
            else:
                print("No migrations table found")
            
            # List all tables
            tables = await conn.fetch(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
            print("\nTables:")
            for table in tables:
                print(f"- {table['table_name']}")
            
            # Check for instrument_xrefs table
            if any(t['table_name'] == 'instrument_xrefs' for t in tables):
                print("\n=== instrument_xrefs table structure ===")
                columns = await conn.fetch(
                    "SELECT column_name, data_type, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_name = 'instrument_xrefs' "
                    "ORDER BY ordinal_position"
                )
                for col in columns:
                    print(f"- {col['column_name']}: {col['data_type']} "
                          f"(Nullable: {col['is_nullable']}, Default: {col['column_default']})")
                
                # Check constraints
                constraints = await conn.fetch(
                    "SELECT conname, contype, conkey, pg_get_constraintdef(oid) as constr_def "
                    "FROM pg_constraint "
                    "WHERE conrelid = 'instrument_xrefs'::regclass"
                )
                if constraints:
                    print("\nConstraints:")
                    for c in constraints:
                        print(f"- {c['conname']} ({c['contype']}): {c['constr_def']}")
            
        finally:
            await conn.close()
    except Exception as e:
        print(f"Error checking database {db_name}: {e}")

if __name__ == "__main__":
    asyncio.run(list_databases())
