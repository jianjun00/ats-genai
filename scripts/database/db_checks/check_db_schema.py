import asyncio
import asyncpg

async def check_schema():
    conn = await asyncpg.connect('postgresql://test_user:test_password@localhost:5432/test_db')
    try:
        result = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'test_universe_membership'
        """)
        print('Universe membership columns:', [(col['column_name'], col['data_type']) for col in result])
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_schema())
