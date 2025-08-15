#!/usr/bin/env python3
"""Script to check the instrument_xrefs table in a specific test database."""

import asyncio
import asyncpg

async def check_instrument_xrefs(db_name):
    """Check the instrument_xrefs table in the specified database."""
    print(f"\n=== Checking instrument_xrefs in {db_name} ===")
    
    conn = await asyncpg.connect(f'postgresql://postgres:password@localhost:5432/{db_name}')
    try:
        # Check if the table exists
        table_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
            'test_instrument_xrefs'
        )
        
        if not table_exists:
            print("test_instrument_xrefs table does not exist")
            return
        
        # Get table columns
        print("\n=== Columns ===")
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
                table_name = 'test_instrument_xrefs'
            ORDER BY 
                ordinal_position
            """
        )
        
        for col in columns:
            print(f"- {col['column_name']}: {col['data_type']} "
                  f"(Nullable: {col['is_nullable']}, Default: {col['column_default']})")
        
        # Get constraints
        print("\n=== Constraints ===")
        constraints = await conn.fetch(
            """
            SELECT 
                tc.constraint_name, 
                tc.constraint_type,
                array_agg(kcu.column_name) as columns,
                pg_get_constraintdef(con.oid) as constraint_definition
            FROM 
                information_schema.table_constraints tc
            JOIN 
                information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_name = kcu.table_name
            LEFT JOIN 
                pg_constraint con ON con.conname = tc.constraint_name
            WHERE 
                tc.table_name = 'test_instrument_xrefs'
            GROUP BY 
                tc.constraint_name, tc.constraint_type, con.oid
            """
        )
        
        if not constraints:
            print("No constraints found")
        else:
            for row in constraints:
                print(f"\n- {row['constraint_name']} ({row['constraint_type']}): {row['constraint_definition']}")
                print(f"  Columns: {row['columns']}")
        
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
                AND t.relname = 'test_instrument_xrefs'
            ORDER BY 
                i.relname, a.attnum
            """
        )
        
        if not indexes:
            print("No indexes found")
        else:
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
        
        # Check for foreign keys
        print("\n=== Foreign Keys ===")
        fks = await conn.fetch(
            """
            SELECT
                tc.constraint_name,
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
              AND tc.table_name = 'test_instrument_xrefs';
            """
        )
        
        if not fks:
            print("No foreign keys found")
        else:
            for fk in fks:
                print(f"- {fk['constraint_name']}: {fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    test_db = "test_db_testuniversemanager_testupda_9c2e7bee"
    asyncio.run(check_instrument_xrefs(test_db))
