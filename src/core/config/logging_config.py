"""
Stub logging_config module for gin configuration compatibility.
"""

import gin


@gin.configurable
class LoggingConfig:
    """Stub LoggingConfig class for gin compatibility."""

    def __init__(self, log_level=None, log_format=None):
        self.log_level = log_level
        self.log_format = log_format