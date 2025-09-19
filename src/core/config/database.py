"""
Stub database module for gin configuration compatibility.
"""

import gin


@gin.configurable
class Database:
    """Stub Database class for gin compatibility."""

    def __init__(self, host=None, port=None, user=None, password=None, database=None,
                 base_database=None, pool_min_size=None, pool_max_size=None, command_timeout=None):
        self.host = host or 'localhost'
        self.port = port or 5432
        self.user = user or 'postgres'
        self.password = password or 'postgres'
        self.database = database or 'trading_db'
        self.base_database = base_database
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.command_timeout = command_timeout

    def get_database_url(self):
        """Return a DSN string for database connections."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"