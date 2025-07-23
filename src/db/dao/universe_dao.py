from config.environment import Environment
import asyncpg

class UniverseDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('universe')
        self.db_url = self.env.get_database_url()

    async def create_universe(self, name: str, description: str = None) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (name, description)
                    VALUES ($1, $2)
                    RETURNING id
                """, name, description)
                return result['id'] if result else None
        finally:
            await pool.close()

    async def update_universe(self, universe_id: int, name: str = None, description: str = None) -> bool:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                fields = []
                params = []
                idx = 1
                if name is not None:
                    fields.append(f"name = ${idx}")
                    params.append(name)
                    idx += 1
                if description is not None:
                    fields.append(f"description = ${idx}")
                    params.append(description)
                    idx += 1
                if not fields:
                    return False
                params.append(universe_id)
                set_clause = ', '.join(fields)
                query = f"UPDATE {self.table_name} SET {set_clause} WHERE id = ${idx}"
                result = await conn.execute(query, *params)
                return 'UPDATE' in result
        finally:
            await pool.close()

    async def get_universe(self, universe_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE id = $1", universe_id)
        finally:
            await pool.close()

    async def list_universes(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name}")
        finally:
            await pool.close()
