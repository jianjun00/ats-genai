"""
Environment utilities for shared modules.

This module provides environment detection and configuration utilities
for the shared utilities framework.
"""

# Import the main Environment and EnvironmentType from the main location
from shared.data_handling.utils.environment import Environment, EnvironmentType

# Re-export for backwards compatibility
__all__ = ['Environment', 'EnvironmentType']