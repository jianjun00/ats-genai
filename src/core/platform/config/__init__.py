"""
Configuration management module for ATS platform.

This module provides environment-specific configuration management with support for
test, integration, and production environments. Now consolidated under core.config.
"""

from .environment import Environment, EnvironmentType
from .gin_loader import GinConfigLoader
from .settings import Settings

__all__ = [
    "Environment",
    "EnvironmentType",
    "GinConfigLoader",
    "Settings",
]