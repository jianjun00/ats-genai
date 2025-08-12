import gin
import os

@gin.configurable
class Database:
    def __init__(self, host=None, port=None, user=None, password=None, database=None, base_database=None, pool_min_size=1, pool_max_size=10, command_timeout=60):
        # Use provided values or fall back to Gin config values
        self.host = host or 'localhost'
        self.port = port or 5432
        self.user = user or 'postgres'
        self.password = password or 'password'
        self.database = database or 'test_db'
        self.base_database = base_database if base_database is not None else self.database
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.command_timeout = command_timeout

    def get_database_url(self):
        """Return a DSN string for asyncpg or psycopg2."""
        # Ensure all components are properly set before constructing the URL
        if not all([self.host, self.port, self.user, self.password, self.database]):
            print(f"[WARNING] Incomplete database configuration: host={self.host}, port={self.port}, user={self.user}, password={'*****' if self.password else None}, database={self.database}")
            
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
