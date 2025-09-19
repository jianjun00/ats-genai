"""
Vendor domain models for the ATS platform.

Contains data classes representing vendor entities and business objects.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Vendor:
    """Vendor entity model."""
    vendor_id: Optional[int] = None
    vendor_name: str = ""
    vendor_description: Optional[str] = None
    api_endpoint: Optional[str] = None
    api_key: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None