"""
Core Platform Components.

Organized into logical domains following the 7-item directory structure:
- business: Analytics and domain-specific business logic
- dao: Data Access Objects organized by domain
- platform: Infrastructure (config, database, logging)
- security: Auth, validation, defensive programming
- shared: Common utilities and cross-cutting concerns
"""

__version__ = "1.0.0"

# Import organized core modules
from . import business
from . import dao
from . import platform
from . import security
from . import shared

__all__ = [
    'business',
    'dao',
    'platform', 
    'security',
    'shared'
]