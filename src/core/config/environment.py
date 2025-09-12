"""
Stub environment module for gin configuration compatibility.
"""

import gin


@gin.configurable
def environment_config():
    """Configurable environment function."""
    pass


@gin.configurable
class Environment:
    """Stub Environment class for gin compatibility."""

    def __init__(self, env_type=None, logging_config=None):
        self.env_type = env_type
        self.logging_config = logging_config

# Default environment instance for compatibility with imports
env = Environment()