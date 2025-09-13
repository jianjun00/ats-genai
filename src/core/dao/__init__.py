# DAO Package for ATS Platform
"""
Data Access Object (DAO) layer for the ATS platform.

This package provides clean abstraction between business logic and data persistence,
following the Repository pattern with comprehensive validation and error handling.

Key Principles:
- Business logic never touches SQL directly
- DAOs are easily mockable for testing
- Database implementation is swappable
- Proper connection management and error handling
- Follows existing BaseDAO infrastructure patterns
"""

# Import base classes
from .base.base_dao import BaseDAO

# Import organized DAO modules
from . import analytics
from . import corporate_actions
from . import infrastructure
from . import instruments
from . import market_data
from . import trading
from . import vendors

__all__ = [
    'BaseDAO',
    'VendorDAO',
    'analytics',
    'corporate_actions',
    'infrastructure',
    'instruments',
    'market_data',
    'trading',
    'vendors'
]