from config.environment import Environment
import asyncpg

class VendorsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('vendors')
        self.db_url = self.env.get_database_url()

    async def create_vendor(self, name: str, description: str = None, api_key_env_var: str = None) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (name, description, api_key_env_var)
                    VALUES ($1, $2, $3)
                    RETURNING vendor_id
                """, name, description, api_key_env_var)
                return result['vendor_id'] if result else None
        finally:
            await pool.close()

    async def get_vendor(self, vendor_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE vendor_id = $1", vendor_id)
        finally:
            await pool.close()

    async def list_vendors(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name}")
        finally:
            await pool.close()
