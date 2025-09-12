"""
Instrument Service Implementation

Business logic layer for instrument operations.
Implements the InstrumentServiceInterface and contains all business rules.
Only this layer should access DAOs directly.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import date

from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    InstrumentXrefDTO,
    VendorInstrumentDTO,
    UnifiedInstrumentDTO,
    InstrumentSearchCriteria,
    InstrumentOperationResult
)

# DAO imports - only the service implementation should import these
from core.dao.instruments.instruments_dao import InstrumentsDAO
from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
from core.dao.infrastructure.vendors_dao import VendorsDAO
from infrastructure.vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO


logger = logging.getLogger(__name__)


class InstrumentServiceImpl(InstrumentServiceInterface):
    """
    Business logic implementation for instrument operations.
    
    This class:
    1. Implements all business rules and validation logic
    2. Coordinates between multiple DAOs
    3. Provides transaction boundaries
    4. Handles error scenarios and logging
    5. Converts between DAOs data and DTOs
    """
    
    def __init__(
        self,
        instruments_dao: InstrumentsDAO,
        xrefs_dao: InstrumentXrefsDAO,
        vendors_dao: VendorsDAO,
        vendor_daos: Optional[Dict[str, Any]] = None
    ):
        self.instruments_dao = instruments_dao
        self.xrefs_dao = xrefs_dao
        self.vendors_dao = vendors_dao
        self.vendor_daos = vendor_daos or {}
        
        # Cache vendor IDs to avoid repeated lookups
        self._vendor_id_cache: Dict[str, int] = {}
    
    async def _get_vendor_id(self, vendor_name: str) -> Optional[int]:
        """Get vendor ID with caching"""
        if vendor_name not in self._vendor_id_cache:
            vendor = await self.vendors_dao.get_vendor_by_name(vendor_name)
            if vendor:
                self._vendor_id_cache[vendor_name] = vendor['id']
            else:
                return None
        return self._vendor_id_cache.get(vendor_name)
    
    def _dao_to_instrument_dto(self, dao_record) -> InstrumentDTO:
        """Convert DAO record to InstrumentDTO"""
        if not dao_record:
            return None
            
        return InstrumentDTO(
            id=dao_record.get('id'),
            symbol=dao_record.get('symbol'),
            name=dao_record.get('name'),
            exchange=dao_record.get('exchange'),
            instrument_type=dao_record.get('type'),
            currency=dao_record.get('currency'),
            list_date=dao_record.get('list_date'),
            delist_date=dao_record.get('delist_date')
        )
    
    def _dao_to_xref_dto(self, dao_record, vendor_name: str = None) -> InstrumentXrefDTO:
        """Convert DAO record to InstrumentXrefDTO"""
        if not dao_record:
            return None
            
        return InstrumentXrefDTO(
            id=dao_record.get('id'),
            instrument_id=dao_record.get('instrument_id'),
            vendor_name=vendor_name,  # Will be populated by caller
            vendor_symbol=dao_record.get('vendor_symbol'),
            xref_type=dao_record.get('type'),
            start_date=dao_record.get('start_at'),
            end_date=dao_record.get('end_at')
        )
    
    # Core CRUD Operations
    
    async def create_instrument(self, instrument: InstrumentDTO) -> InstrumentOperationResult:
        """Create a new instrument with business validation"""
        try:
            # Business validation
            if not instrument.symbol:
                return InstrumentOperationResult(
                    success=False,
                    error_message="Symbol is required"
                )
            
            # Check if instrument already exists
            existing = await self.instruments_dao.get_instrument_by_symbol(instrument.symbol)
            if existing:
                return InstrumentOperationResult(
                    success=False,
                    error_message=f"Instrument with symbol {instrument.symbol} already exists",
                    instrument_id=existing['id']
                )
            
            # Create instrument
            instrument_id = await self.instruments_dao.create_instrument(
                symbol=instrument.symbol,
                name=instrument.name,
                exchange=instrument.exchange,
                type_=instrument.instrument_type,
                currency=instrument.currency,
                list_date=instrument.list_date,
                delist_date=instrument.delist_date
            )
            
            logger.info(f"Created instrument {instrument.symbol} with ID {instrument_id}")
            
            return InstrumentOperationResult(
                success=True,
                instrument_id=instrument_id,
                created_count=1
            )
            
        except Exception as e:
            logger.error(f"Error creating instrument {instrument.symbol}: {e}")
            return InstrumentOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def get_instrument_by_id(self, instrument_id: int) -> Optional[InstrumentDTO]:
        """Retrieve instrument by ID"""
        try:
            dao_record = await self.instruments_dao.get_instrument(instrument_id)
            return self._dao_to_instrument_dto(dao_record)
        except Exception as e:
            logger.error(f"Error retrieving instrument {instrument_id}: {e}")
            return None
    
    async def get_instrument_by_symbol(self, symbol: str, vendor_name: str = "ticker") -> Optional[InstrumentDTO]:
        """Retrieve instrument by symbol and vendor with business logic"""
        try:
            # Resolve via cross-reference first (preferred method)
            instrument_id = await self.xrefs_dao.resolve_instrument_id_by_symbol(symbol)
            if instrument_id:
                return await self.get_instrument_by_id(instrument_id)
            
            # Fallback to direct symbol lookup
            dao_record = await self.instruments_dao.get_instrument_by_symbol(symbol)
            return self._dao_to_instrument_dto(dao_record)
            
        except Exception as e:
            logger.error(f"Error retrieving instrument by symbol {symbol}: {e}")
            return None
    
    async def update_instrument(self, instrument: InstrumentDTO) -> InstrumentOperationResult:
        """Update existing instrument - implement as needed"""
        return InstrumentOperationResult(
            success=False,
            error_message="Update operation not yet implemented"
        )
    
    async def list_instruments(self, criteria: InstrumentSearchCriteria) -> List[InstrumentDTO]:
        """List instruments based on search criteria with business logic"""
        try:
            # For now, return all instruments (implement filtering as needed)
            dao_records = await self.instruments_dao.list_instruments()
            
            # Apply client-side filtering (move to DAO for efficiency)
            results = []
            for record in dao_records:
                dto = self._dao_to_instrument_dto(record)
                
                # Apply basic filtering
                if criteria.symbols and dto.symbol not in criteria.symbols:
                    continue
                if criteria.exchanges and dto.exchange not in criteria.exchanges:
                    continue
                if criteria.instrument_types and dto.instrument_type not in criteria.instrument_types:
                    continue
                if criteria.currencies and dto.currency not in criteria.currencies:
                    continue
                    
                results.append(dto)
            
            # Apply limit and offset
            if criteria.offset:
                results = results[criteria.offset:]
            if criteria.limit:
                results = results[:criteria.limit]
                
            return results
            
        except Exception as e:
            logger.error(f"Error listing instruments: {e}")
            return []
    
    # Cross-Reference Operations
    
    async def create_cross_reference(self, xref: InstrumentXrefDTO) -> InstrumentOperationResult:
        """Create instrument cross-reference with business validation"""
        try:
            # Get vendor ID
            vendor_id = await self._get_vendor_id(xref.vendor_name)
            if not vendor_id:
                return InstrumentOperationResult(
                    success=False,
                    error_message=f"Vendor '{xref.vendor_name}' not found"
                )
            
            # Business validation
            if not xref.instrument_id or not xref.vendor_symbol:
                return InstrumentOperationResult(
                    success=False,
                    error_message="instrument_id and vendor_symbol are required"
                )
            
            # Check if xref already exists
            existing = await self.xrefs_dao.find_xref(vendor_id, xref.vendor_symbol)
            if existing:
                return InstrumentOperationResult(
                    success=False,
                    error_message=f"Cross-reference already exists for {xref.vendor_name}:{xref.vendor_symbol}",
                    skipped_count=1
                )
            
            # Create cross-reference
            xref_id = await self.xrefs_dao.create_xref(
                instrument_id=xref.instrument_id,
                vendor_id=vendor_id,
                symbol=xref.vendor_symbol,
                type=xref.xref_type,
                start_at=xref.start_date,
                end_at=xref.end_date
            )
            
            logger.info(f"Created cross-reference {xref.vendor_name}:{xref.vendor_symbol} -> {xref.instrument_id}")
            
            return InstrumentOperationResult(
                success=True,
                instrument_id=xref_id,
                created_count=1
            )
            
        except Exception as e:
            logger.error(f"Error creating cross-reference {xref.vendor_name}:{xref.vendor_symbol}: {e}")
            return InstrumentOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def get_cross_references(self, instrument_id: int) -> List[InstrumentXrefDTO]:
        """Get all cross-references for an instrument"""
        try:
            dao_records = await self.xrefs_dao.list_xrefs_for_instrument(instrument_id)
            
            # Convert to DTOs with vendor name resolution
            results = []
            for record in dao_records:
                # Look up vendor name by ID (could be optimized with batch lookup)
                vendor_name = None
                for name, cached_id in self._vendor_id_cache.items():
                    if cached_id == record.get('vendor_id'):
                        vendor_name = name
                        break
                
                xref_dto = self._dao_to_xref_dto(record, vendor_name)
                if xref_dto:
                    results.append(xref_dto)
                    
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving cross-references for instrument {instrument_id}: {e}")
            return []
    
    async def resolve_instrument_by_vendor_symbol(
        self, 
        vendor_symbol: str, 
        vendor_name: str, 
        as_of_date: Optional[date] = None
    ) -> Optional[InstrumentDTO]:
        """Resolve instrument by vendor symbol and date with business logic"""
        try:
            # Get vendor ID
            vendor_id = await self._get_vendor_id(vendor_name)
            if not vendor_id:
                logger.warning(f"Vendor '{vendor_name}' not found")
                return None
            
            # Resolve instrument ID
            instrument_id = await self.xrefs_dao.resolve_instrument_id(
                vendor_symbol, vendor_id, as_of_date
            )
            
            if instrument_id:
                return await self.get_instrument_by_id(instrument_id)
            
            return None
            
        except Exception as e:
            logger.error(f"Error resolving instrument by vendor symbol {vendor_name}:{vendor_symbol}: {e}")
            return None
    
    # Unified Operations
    
    async def get_unified_instrument(
        self, 
        identifier: str, 
        identifier_type: str = "symbol"
    ) -> Optional[UnifiedInstrumentDTO]:
        """Get unified instrument view with all cross-references and vendor data"""
        try:
            # Get base instrument
            if identifier_type == "symbol":
                instrument = await self.get_instrument_by_symbol(identifier)
            elif identifier_type == "id":
                instrument = await self.get_instrument_by_id(int(identifier))
            else:
                raise ValueError(f"Unsupported identifier_type: {identifier_type}")
            
            if not instrument:
                return None
            
            # Get all cross-references
            cross_references = await self.get_cross_references(instrument.id)
            
            # Get vendor-specific data (placeholder for now)
            vendor_data = {}
            
            return UnifiedInstrumentDTO(
                instrument=instrument,
                cross_references=cross_references,
                vendor_data=vendor_data
            )
            
        except Exception as e:
            logger.error(f"Error getting unified instrument {identifier_type}:{identifier}: {e}")
            return None
    
    async def populate_from_vendor(
        self, 
        vendor_name: str, 
        symbols: Optional[List[str]] = None
    ) -> InstrumentOperationResult:
        """Populate instruments from vendor data source - simplified implementation"""
        try:
            # This would integrate with the existing populate_unified_instruments logic
            # For now, return placeholder
            return InstrumentOperationResult(
                success=False,
                error_message="populate_from_vendor not yet implemented - use existing populate_unified_instruments script"
            )
            
        except Exception as e:
            logger.error(f"Error populating from vendor {vendor_name}: {e}")
            return InstrumentOperationResult(
                success=False,
                error_message=str(e)
            )
    
    # Batch Operations
    
    async def create_instruments_batch(self, instruments: List[InstrumentDTO]) -> InstrumentOperationResult:
        """Create multiple instruments in batch with business validation"""
        try:
            if not instruments:
                return InstrumentOperationResult(success=True, created_count=0)
            
            # Convert DTOs to DAO format
            dao_instruments = []
            for instrument in instruments:
                if not instrument.symbol:
                    continue  # Skip invalid instruments
                
                dao_instruments.append({
                    'symbol': instrument.symbol,
                    'name': instrument.name,
                    'exchange': instrument.exchange,
                    'type_': instrument.instrument_type,
                    'currency': instrument.currency,
                    'list_date': instrument.list_date,
                    'delist_date': instrument.delist_date
                })
            
            # Batch create
            created_ids = await self.instruments_dao.create_instruments_batch(dao_instruments)
            
            logger.info(f"Batch created {len(created_ids)} instruments")
            
            return InstrumentOperationResult(
                success=True,
                created_count=len(created_ids)
            )
            
        except Exception as e:
            logger.error(f"Error in batch instrument creation: {e}")
            return InstrumentOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def create_cross_references_batch(self, xrefs: List[InstrumentXrefDTO]) -> InstrumentOperationResult:
        """Create multiple cross-references in batch"""
        try:
            if not xrefs:
                return InstrumentOperationResult(success=True, created_count=0)
            
            # Group by vendor for batch processing
            vendor_groups = {}
            for xref in xrefs:
                if xref.vendor_name not in vendor_groups:
                    vendor_groups[xref.vendor_name] = []
                vendor_groups[xref.vendor_name].append(xref)
            
            total_created = 0
            
            for vendor_name, vendor_xrefs in vendor_groups.items():
                vendor_id = await self._get_vendor_id(vendor_name)
                if not vendor_id:
                    logger.warning(f"Vendor '{vendor_name}' not found, skipping batch")
                    continue
                
                # Convert to DAO format
                dao_xrefs = []
                for xref in vendor_xrefs:
                    dao_xrefs.append({
                        'instrument_id': xref.instrument_id,
                        'vendor_id': vendor_id,
                        'symbol': xref.vendor_symbol,
                        'type': xref.xref_type,
                        'start_at': xref.start_date,
                        'end_at': xref.end_date
                    })
                
                created_ids = await self.xrefs_dao.create_xrefs_batch(dao_xrefs)
                total_created += len(created_ids)
            
            logger.info(f"Batch created {total_created} cross-references")
            
            return InstrumentOperationResult(
                success=True,
                created_count=total_created
            )
            
        except Exception as e:
            logger.error(f"Error in batch cross-reference creation: {e}")
            return InstrumentOperationResult(
                success=False,
                error_message=str(e)
            )
    
    # Utility Operations
    
    async def get_all_symbols(self, vendor_name: str = "ticker") -> List[str]:
        """Get all symbols for a vendor"""
        try:
            return await self.xrefs_dao.get_all_symbols()
        except Exception as e:
            logger.error(f"Error getting all symbols: {e}")
            return []
    
    async def get_instrument_count(self) -> int:
        """Get total number of instruments"""
        try:
            return await self.instruments_dao.count_instruments()
        except Exception as e:
            logger.error(f"Error getting instrument count: {e}")
            return 0
    
    async def validate_symbol(self, symbol: str, vendor_name: str = "ticker") -> bool:
        """Validate if symbol exists for vendor"""
        try:
            instrument = await self.get_instrument_by_symbol(symbol, vendor_name)
            return instrument is not None
        except Exception as e:
            logger.error(f"Error validating symbol {symbol}: {e}")
            return False