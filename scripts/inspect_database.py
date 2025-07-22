#!/usr/bin/env python3
"""
Database inspection script to check tables and data in different environments.
"""
import asyncio
import asyncpg
import sys
import os
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.environment import get_environment, set_environment, EnvironmentType


async def get_database_info(db_url: str) -> Dict[str, Any]:
    """Get basic database information."""
    try:
        conn = await asyncpg.connect(db_url)
        
        # Get database name
        db_name = await conn.fetchval("SELECT current_database()")
        
        # Get all tables
        tables = await conn.fetch("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        # Get table row counts
        table_counts = {}
        for table in tables:
            table_name = table['table_name']
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                table_counts[table_name] = count
            except Exception as e:
                table_counts[table_name] = f"Error: {e}"
        
        await conn.close()
        
        return {
            'database_name': db_name,
            'tables': [dict(row) for row in tables],
            'table_counts': table_counts
        }
    except Exception as e:
        return {
            'error': str(e),
            'database_name': None,
            'tables': [],
            'table_counts': {}
        }


async def inspect_table_structure(db_url: str, table_name: str) -> List[Dict]:
    """Get detailed structure of a specific table."""
    try:
        conn = await asyncpg.connect(db_url)
        
        columns = await conn.fetch("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = 'public'
            ORDER BY ordinal_position
        """, table_name)
        
        await conn.close()
        return [dict(row) for row in columns]
    except Exception as e:
        return [{'error': str(e)}]


async def sample_table_data(db_url: str, table_name: str, limit: int = 5) -> List[Dict]:
    """Get sample data from a table."""
    try:
        conn = await asyncpg.connect(db_url)
        
        # Get sample data
        rows = await conn.fetch(f"SELECT * FROM {table_name} LIMIT $1", limit)
        
        await conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        return [{'error': str(e)}]


async def main():
    """Main inspection function."""
    if len(sys.argv) > 1:
        env_type = sys.argv[1].lower()
        if env_type in ['test', 'intg', 'prod']:
            set_environment(EnvironmentType(env_type))
        else:
            print(f"Invalid environment: {env_type}. Using default.")
    
    env = get_environment()
    print(f"Inspecting {env.env_type.value} environment")
    print(f"Database URL: {env.get_database_url()}")
    print("=" * 60)
    
    # Get basic database info
    db_info = await get_database_info(env.get_database_url())
    
    if 'error' in db_info:
        print(f"❌ Connection Error: {db_info['error']}")
        return
    
    print(f"📊 Database: {db_info['database_name']}")
    print(f"📋 Found {len(db_info['tables'])} tables")
    print()
    
    # Show tables and row counts
    print("Tables and Row Counts:")
    print("-" * 40)
    for table in db_info['tables']:
        table_name = table['table_name']
        table_type = table['table_type']
        count = db_info['table_counts'].get(table_name, 'Unknown')
        print(f"  {table_name:<30} ({table_type}) - {count} rows")
    
    print()
    
    # Show sample data for tables with data
    tables_with_data = [
        table['table_name'] for table in db_info['tables'] 
        if isinstance(db_info['table_counts'].get(table['table_name']), int) 
        and db_info['table_counts'][table['table_name']] > 0
    ]
    
    if tables_with_data:
        print("Sample Data from Non-Empty Tables:")
        print("-" * 40)
        
        for table_name in tables_with_data[:3]:  # Limit to first 3 tables
            print(f"\n🔍 {table_name}:")
            
            # Show structure
            structure = await inspect_table_structure(env.get_database_url(), table_name)
            if structure and 'error' not in structure[0]:
                print("  Columns:")
                for col in structure[:5]:  # Show first 5 columns
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    print(f"    - {col['column_name']}: {col['data_type']} {nullable}")
                if len(structure) > 5:
                    print(f"    ... and {len(structure) - 5} more columns")
            
            # Show sample data
            sample_data = await sample_table_data(env.get_database_url(), table_name, 3)
            if sample_data and 'error' not in sample_data[0]:
                print("  Sample rows:")
                for i, row in enumerate(sample_data, 1):
                    # Show first few columns of each row
                    row_preview = {k: v for k, v in list(row.items())[:4]}
                    print(f"    {i}: {row_preview}")
                    if len(row) > 4:
                        print(f"       ... and {len(row) - 4} more columns")
    else:
        print("No tables contain data.")


if __name__ == "__main__":
    asyncio.run(main())
