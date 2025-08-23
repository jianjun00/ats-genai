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

from .base.base_dao import BaseDAO
from .exchange_dao import ExchangeDAO
from .instrument_xref_dao import InstrumentXrefDAO
from .vendor_dao import VendorDAO

__all__ = [
    'BaseDAO',
    'ExchangeDAO',
    'InstrumentXrefDAO', 
    'VendorDAO',
]