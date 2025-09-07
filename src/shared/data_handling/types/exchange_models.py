"""
Exchange domain models for the ATS platform.

Contains data classes representing exchange entities and business objects.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime


@dataclass
class Exchange:
    """Exchange entity model."""
    id: Optional[int] = None
    exchange_code: str = ""
    exchange_name: str = ""
    country: Optional[str] = None
    timezone: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass 
class ExchangeEntry:
    """Exchange entry model for instrument cross-references."""
    id: Optional[int] = None
    instrument_id: int = 0
    vendor_id: int = 0
    external_symbol: str = ""
    start_date: date = date.today()
    end_date: Optional[date] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class ExchangeMigration:
    """Exchange migration event model."""
    instrument_id: int = 0
    symbol: str = ""
    from_exchange: str = ""
    to_exchange: str = ""
    migration_date: date = date.today()
    from_exchange_name: Optional[str] = None
    to_exchange_name: Optional[str] = None
    from_major: bool = False
    to_otc: bool = False