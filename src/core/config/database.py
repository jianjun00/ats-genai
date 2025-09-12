"""
Stub database module for gin configuration compatibility.
"""

import gin


@gin.configurable
class Database:
    """Stub Database class for gin compatibility."""

    def __init__(self, host=None, port=None, user=None, password=None, database=None,
                 base_database=None, pool_min_size=None, pool_max_size=None, command_timeout=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.base_database = base_database
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.command_timeout = command_timeout