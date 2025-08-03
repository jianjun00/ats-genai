"""
Configuration management module for market-forecast-app.

This module provides environment-specific configuration management with support for
test, integration, and production environments.
"""

from .environment import Environment, EnvironmentType

__all__ = [
    "Environment",
    "EnvironmentType", 


]
