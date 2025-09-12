"""
Stub indicator_runner module for gin configuration compatibility.
This is a minimal implementation to resolve gin config import issues.
"""

import gin


@gin.configurable
def indicator_config():
    """Configurable indicator function."""
    pass


@gin.configurable
class IndicatorRunner:
    """Stub IndicatorRunner class for gin compatibility."""

    def __init__(self):
        pass

    def run(self):
        pass