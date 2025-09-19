"""
Instrument domain models for the ATS platform.

Contains data classes representing instrument entities and business objects.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime


@dataclass
class Instrument:
    """Instrument entity model."""
    id: Optional[int] = None
    symbol: str = ""
    instrument_name: str = ""
    instrument_type: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class InstrumentXref:
    """Instrument cross-reference model for vendor mappings."""
    id: Optional[int] = None
    instrument_id: int = 0
    vendor_id: int = 0
    external_symbol: str = ""
    start_date: date = date.today()
    end_date: Optional[date] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None