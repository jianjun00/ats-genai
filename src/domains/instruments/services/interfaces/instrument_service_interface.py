"""
Instrument Service Interface

Defines the public contract for instrument operations.
Service clients should only use this interface, never access DAOs directly.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import date
from dataclasses import dataclass


@dataclass
class InstrumentDTO:
    """Data Transfer Object for Instrument information"""
    id: Optional[int] = None
    symbol: str = None
    name: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    currency: Optional[str] = None
    list_date: Optional[date] = None
    delist_date: Optional[date] = None


@dataclass
class InstrumentXrefDTO:
    """Data Transfer Object for Instrument Cross-Reference information"""
    id: Optional[int] = None
    instrument_id: int = None
    vendor_name: str = None
    vendor_symbol: str = None
    xref_type: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


@dataclass
class VendorInstrumentDTO:
    """Data Transfer Object for Vendor-specific instrument information"""
    vendor_symbol: str = None
    vendor_name: str = None
    name: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    currency: Optional[str] = None
    list_date: Optional[date] = None
    delist_date: Optional[date] = None
    additional_metadata: Optional[Dict[str, Any]] = None


@dataclass
class UnifiedInstrumentDTO:
    """Unified view of instrument with all cross-references"""
    instrument: InstrumentDTO
    cross_references: List[InstrumentXrefDTO]
    vendor_data: Dict[str, VendorInstrumentDTO]


@dataclass
class InstrumentSearchCriteria:
    """Search criteria for instrument queries"""
    symbols: Optional[List[str]] = None
    exchanges: Optional[List[str]] = None
    instrument_types: Optional[List[str]] = None
    currencies: Optional[List[str]] = None
    vendor_name: Optional[str] = None
    active_on_date: Optional[date] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


@dataclass
class InstrumentOperationResult:
    """Result of instrument operation with success/error information"""
    success: bool
    instrument_id: Optional[int] = None
    error_message: Optional[str] = None
    created_count: Optional[int] = None
    updated_count: Optional[int] = None
    skipped_count: Optional[int] = None


class InstrumentServiceInterface(ABC):
    """
    Public interface for instrument operations.
    
    This interface defines all operations that clients can perform
    on instruments. Service clients should only use this interface,
    never access DAOs or implementation details directly.
    """
    
    # Core CRUD Operations
    
    @abstractmethod
    async def create_instrument(self, instrument: InstrumentDTO) -> InstrumentOperationResult:
        """Create a new instrument"""
        pass
    
    @abstractmethod
    async def get_instrument_by_id(self, instrument_id: int) -> Optional[InstrumentDTO]:
        """Retrieve instrument by ID"""
        pass
    
    @abstractmethod
    async def get_instrument_by_symbol(self, symbol: str, vendor_name: str = "ticker") -> Optional[InstrumentDTO]:
        """Retrieve instrument by symbol and vendor"""
        pass
    
    @abstractmethod
    async def update_instrument(self, instrument: InstrumentDTO) -> InstrumentOperationResult:
        """Update existing instrument"""
        pass
    
    @abstractmethod
    async def list_instruments(self, criteria: InstrumentSearchCriteria) -> List[InstrumentDTO]:
        """List instruments based on search criteria"""
        pass
    
    # Cross-Reference Operations
    
    @abstractmethod
    async def create_cross_reference(self, xref: InstrumentXrefDTO) -> InstrumentOperationResult:
        """Create instrument cross-reference"""
        pass
    
    @abstractmethod
    async def get_cross_references(self, instrument_id: int) -> List[InstrumentXrefDTO]:
        """Get all cross-references for an instrument"""
        pass
    
    @abstractmethod
    async def resolve_instrument_by_vendor_symbol(
        self, 
        vendor_symbol: str, 
        vendor_name: str, 
        as_of_date: Optional[date] = None
    ) -> Optional[InstrumentDTO]:
        """Resolve instrument by vendor symbol and date"""
        pass
    
    # Unified Operations
    
    @abstractmethod
    async def get_unified_instrument(
        self, 
        identifier: str, 
        identifier_type: str = "symbol"
    ) -> Optional[UnifiedInstrumentDTO]:
        """Get unified instrument view with all cross-references and vendor data"""
        pass
    
    @abstractmethod
    async def populate_from_vendor(
        self, 
        vendor_name: str, 
        symbols: Optional[List[str]] = None
    ) -> InstrumentOperationResult:
        """Populate instruments from vendor data source"""
        pass
    
    # Batch Operations
    
    @abstractmethod
    async def create_instruments_batch(self, instruments: List[InstrumentDTO]) -> InstrumentOperationResult:
        """Create multiple instruments in batch"""
        pass
    
    @abstractmethod
    async def create_cross_references_batch(self, xrefs: List[InstrumentXrefDTO]) -> InstrumentOperationResult:
        """Create multiple cross-references in batch"""
        pass
    
    # Utility Operations
    
    @abstractmethod
    async def get_all_symbols(self, vendor_name: str = "ticker") -> List[str]:
        """Get all symbols for a vendor"""
        pass
    
    @abstractmethod
    async def get_instrument_count(self) -> int:
        """Get total number of instruments"""
        pass
    
    @abstractmethod
    async def validate_symbol(self, symbol: str, vendor_name: str = "ticker") -> bool:
        """Validate if symbol exists for vendor"""
        pass