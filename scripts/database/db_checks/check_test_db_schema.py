import asyncio
import asyncpg
import sys

async def check_schema(db_url):
    print(f"Connecting to {db_url}")
    conn = await asyncpg.connect(db_url)
    try:
        # List all tables
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        print("Tables:", [t['table_name'] for t in tables])
        
        # Check universe_membership table
        for table_name in [t['table_name'] for t in tables]:
            if 'universe_membership' in table_name:
                result = await conn.fetch(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
                print(f"Table {table_name} columns:", [(col['column_name'], col['data_type']) for col in result])
    finally:
        await conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        db_url = sys.argv[1]
    else:
        db_url = "postgresql://test_user:test_password@localhost:5432/test_db"
    asyncio.run(check_schema(db_url))
