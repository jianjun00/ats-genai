import gin

@gin.configurable
class Database:
    def __init__(self, host, port, user, password, database, base_database=None, pool_min_size=1, pool_max_size=10, command_timeout=60):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.base_database = base_database if base_database is not None else database
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.command_timeout = command_timeout

    def dsn(self):
        """Return a DSN string for asyncpg or psycopg2."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
