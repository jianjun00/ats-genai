"""
Instruments REST API

HTTP endpoints for instrument operations using the service-based architecture.
This API layer only handles HTTP concerns and delegates all business logic to services.
"""

from fastapi import APIRouter, HTTPException, Query, Path, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date
import logging

from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    InstrumentXrefDTO,
    UnifiedInstrumentDTO,
    InstrumentSearchCriteria,
    InstrumentOperationResult
)

logger = logging.getLogger(__name__)

# Create router
instruments_router = APIRouter(prefix="/api/v1/instruments", tags=["instruments"])

# HTTP Request/Response Models (separate from service DTOs)

class InstrumentRequest(BaseModel):
    """HTTP request model for creating/updating instruments"""
    symbol: str = Field(..., description="Instrument symbol")
    name: Optional[str] = Field(None, description="Instrument name")
    exchange: Optional[str] = Field(None, description="Exchange code")
    instrument_type: Optional[str] = Field(None, description="Instrument type")
    currency: Optional[str] = Field(None, description="Currency code")
    list_date: Optional[date] = Field(None, description="Listing date")
    delist_date: Optional[date] = Field(None, description="Delisting date")

class InstrumentResponse(BaseModel):
    """HTTP response model for instrument data"""
    id: int
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    currency: Optional[str] = None
    list_date: Optional[date] = None
    delist_date: Optional[date] = None

class InstrumentXrefRequest(BaseModel):
    """HTTP request model for creating cross-references"""
    instrument_id: int = Field(..., description="Instrument ID")
    vendor_name: str = Field(..., description="Vendor name")
    vendor_symbol: str = Field(..., description="Vendor symbol")
    xref_type: Optional[str] = Field(None, description="Cross-reference type")
    start_date: Optional[date] = Field(None, description="Start date")
    end_date: Optional[date] = Field(None, description="End date")

class InstrumentXrefResponse(BaseModel):
    """HTTP response model for cross-reference data"""
    id: int
    instrument_id: int
    vendor_name: str
    vendor_symbol: str
    xref_type: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class UnifiedInstrumentResponse(BaseModel):
    """HTTP response model for unified instrument view"""
    instrument: InstrumentResponse
    cross_references: List[InstrumentXrefResponse]
    vendor_data: Dict[str, Any]

class InstrumentListResponse(BaseModel):
    """HTTP response model for instrument lists"""
    instruments: List[InstrumentResponse]
    total_count: int

class OperationResponse(BaseModel):
    """HTTP response model for operation results"""
    success: bool
    message: Optional[str] = None
    instrument_id: Optional[int] = None
    created_count: Optional[int] = None
    updated_count: Optional[int] = None
    skipped_count: Optional[int] = None

# Import dependency injection from service container
from domains.instruments.services.config.service_container import provide_instrument_service

# Use the proper dependency injection
async def get_instrument_service() -> InstrumentServiceInterface:
    """
    Dependency injection for InstrumentService using service container
    """
    return await provide_instrument_service()

# Utility functions for converting between HTTP models and service DTOs

def request_to_dto(request: InstrumentRequest) -> InstrumentDTO:
    """Convert HTTP request to service DTO"""
    return InstrumentDTO(
        symbol=request.symbol,
        name=request.name,
        exchange=request.exchange,
        instrument_type=request.instrument_type,
        currency=request.currency,
        list_date=request.list_date,
        delist_date=request.delist_date
    )

def dto_to_response(dto: InstrumentDTO) -> InstrumentResponse:
    """Convert service DTO to HTTP response"""
    return InstrumentResponse(
        id=dto.id,
        symbol=dto.symbol,
        name=dto.name,
        exchange=dto.exchange,
        instrument_type=dto.instrument_type,
        currency=dto.currency,
        list_date=dto.list_date,
        delist_date=dto.delist_date
    )

def xref_request_to_dto(request: InstrumentXrefRequest) -> InstrumentXrefDTO:
    """Convert HTTP cross-reference request to service DTO"""
    return InstrumentXrefDTO(
        instrument_id=request.instrument_id,
        vendor_name=request.vendor_name,
        vendor_symbol=request.vendor_symbol,
        xref_type=request.xref_type,
        start_date=request.start_date,
        end_date=request.end_date
    )

def xref_dto_to_response(dto: InstrumentXrefDTO) -> InstrumentXrefResponse:
    """Convert service cross-reference DTO to HTTP response"""
    return InstrumentXrefResponse(
        id=dto.id,
        instrument_id=dto.instrument_id,
        vendor_name=dto.vendor_name,
        vendor_symbol=dto.vendor_symbol,
        xref_type=dto.xref_type,
        start_date=dto.start_date,
        end_date=dto.end_date
    )

def operation_result_to_response(result: InstrumentOperationResult) -> OperationResponse:
    """Convert service operation result to HTTP response"""
    return OperationResponse(
        success=result.success,
        message=result.error_message if not result.success else "Operation completed successfully",
        instrument_id=result.instrument_id,
        created_count=result.created_count,
        updated_count=result.updated_count,
        skipped_count=result.skipped_count
    )

# API Endpoints

@instruments_router.post("/", response_model=OperationResponse, status_code=201)
async def create_instrument(
    request: InstrumentRequest,
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Create a new instrument"""
    try:
        dto = request_to_dto(request)
        result = await service.create_instrument(dto)
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_message)
        
        return operation_result_to_response(result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating instrument: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/{instrument_id}", response_model=InstrumentResponse)
async def get_instrument(
    instrument_id: int = Path(..., description="Instrument ID"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Get instrument by ID"""
    try:
        dto = await service.get_instrument_by_id(instrument_id)
        if not dto:
            raise HTTPException(status_code=404, detail="Instrument not found")
        
        return dto_to_response(dto)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving instrument {instrument_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/", response_model=InstrumentListResponse)
async def list_instruments(
    symbols: Optional[List[str]] = Query(None, description="Filter by symbols"),
    exchanges: Optional[List[str]] = Query(None, description="Filter by exchanges"),
    instrument_types: Optional[List[str]] = Query(None, description="Filter by instrument types"),
    currencies: Optional[List[str]] = Query(None, description="Filter by currencies"),
    vendor_name: Optional[str] = Query("ticker", description="Vendor name for symbol resolution"),
    limit: Optional[int] = Query(100, description="Maximum number of results"),
    offset: Optional[int] = Query(0, description="Number of results to skip"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """List instruments with optional filtering"""
    try:
        criteria = InstrumentSearchCriteria(
            symbols=symbols,
            exchanges=exchanges,
            instrument_types=instrument_types,
            currencies=currencies,
            vendor_name=vendor_name,
            limit=limit,
            offset=offset
        )
        
        dtos = await service.list_instruments(criteria)
        responses = [dto_to_response(dto) for dto in dtos]
        
        return InstrumentListResponse(
            instruments=responses,
            total_count=len(responses)  # TODO: Implement proper total count
        )
        
    except Exception as e:
        logger.error(f"Error listing instruments: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/by-symbol/{symbol}", response_model=InstrumentResponse)
async def get_instrument_by_symbol(
    symbol: str = Path(..., description="Instrument symbol"),
    vendor_name: str = Query("ticker", description="Vendor name for symbol resolution"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Get instrument by symbol"""
    try:
        dto = await service.get_instrument_by_symbol(symbol, vendor_name)
        if not dto:
            raise HTTPException(status_code=404, detail=f"Instrument not found for symbol {symbol}")
        
        return dto_to_response(dto)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving instrument by symbol {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.post("/cross-references", response_model=OperationResponse, status_code=201)
async def create_cross_reference(
    request: InstrumentXrefRequest,
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Create instrument cross-reference"""
    try:
        dto = xref_request_to_dto(request)
        result = await service.create_cross_reference(dto)
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_message)
        
        return operation_result_to_response(result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating cross-reference: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/{instrument_id}/cross-references", response_model=List[InstrumentXrefResponse])
async def get_cross_references(
    instrument_id: int = Path(..., description="Instrument ID"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Get all cross-references for an instrument"""
    try:
        dtos = await service.get_cross_references(instrument_id)
        return [xref_dto_to_response(dto) for dto in dtos]
        
    except Exception as e:
        logger.error(f"Error retrieving cross-references for instrument {instrument_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/unified/{identifier}", response_model=UnifiedInstrumentResponse)
async def get_unified_instrument(
    identifier: str = Path(..., description="Instrument identifier (symbol or ID)"),
    identifier_type: str = Query("symbol", description="Type of identifier: 'symbol' or 'id'"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Get unified instrument view with all cross-references and vendor data"""
    try:
        dto = await service.get_unified_instrument(identifier, identifier_type)
        if not dto:
            raise HTTPException(status_code=404, detail=f"Instrument not found for {identifier_type} {identifier}")
        
        return UnifiedInstrumentResponse(
            instrument=dto_to_response(dto.instrument),
            cross_references=[xref_dto_to_response(xref) for xref in dto.cross_references],
            vendor_data=dto.vendor_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving unified instrument {identifier_type}:{identifier}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.post("/resolve-vendor-symbol", response_model=InstrumentResponse)
async def resolve_vendor_symbol(
    vendor_symbol: str = Query(..., description="Vendor symbol"),
    vendor_name: str = Query(..., description="Vendor name"),
    as_of_date: Optional[date] = Query(None, description="As of date for temporal resolution"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Resolve instrument by vendor symbol"""
    try:
        dto = await service.resolve_instrument_by_vendor_symbol(vendor_symbol, vendor_name, as_of_date)
        if not dto:
            raise HTTPException(
                status_code=404, 
                detail=f"Instrument not found for vendor symbol {vendor_name}:{vendor_symbol}"
            )
        
        return dto_to_response(dto)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resolving vendor symbol {vendor_name}:{vendor_symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/symbols/all", response_model=List[str])
async def get_all_symbols(
    vendor_name: str = Query("ticker", description="Vendor name"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Get all symbols for a vendor"""
    try:
        symbols = await service.get_all_symbols(vendor_name)
        return symbols
        
    except Exception as e:
        logger.error(f"Error retrieving all symbols for vendor {vendor_name}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.get("/count", response_model=int)
async def get_instrument_count(
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Get total number of instruments"""
    try:
        count = await service.get_instrument_count()
        return count
        
    except Exception as e:
        logger.error(f"Error getting instrument count: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.post("/validate-symbol", response_model=bool)
async def validate_symbol(
    symbol: str = Query(..., description="Symbol to validate"),
    vendor_name: str = Query("ticker", description="Vendor name"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Validate if symbol exists for vendor"""
    try:
        is_valid = await service.validate_symbol(symbol, vendor_name)
        return is_valid
        
    except Exception as e:
        logger.error(f"Error validating symbol {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@instruments_router.post("/batch", response_model=OperationResponse)
async def create_instruments_batch(
    requests: List[InstrumentRequest],
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Create multiple instruments in batch"""
    try:
        dtos = [request_to_dto(req) for req in requests]
        result = await service.create_instruments_batch(dtos)
        
        return operation_result_to_response(result)
        
    except Exception as e:
        logger.error(f"Error in batch instrument creation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Health check endpoint with comprehensive monitoring
@instruments_router.get("/health", response_model=Dict[str, Any])
async def health_check(
    service: InstrumentServiceInterface = Depends(get_instrument_service)
):
    """Comprehensive health check endpoint"""
    try:
        from src.infrastructure.service_discovery import get_health_manager
        
        health_manager = get_health_manager()
        overall_health = await health_manager.perform_all_checks()
        
        # Add service-specific health info
        health_response = overall_health.to_dict()
        health_response["service"] = "instruments-api"
        health_response["version"] = "1.0.0"
        
        return health_response
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "service": "instruments-api",
            "message": f"Health check failed: {str(e)}",
            "timestamp": "2024-01-01T00:00:00Z"  # Fallback timestamp
        }

# Service discovery endpoint
@instruments_router.get("/service-info", response_model=Dict[str, Any])
async def get_service_info():
    """Get service discovery information"""
    return {
        "service_name": "instrument-service",
        "version": "1.0.0",
        "endpoints": {
            "health": "/api/v1/instruments/health",
            "create_instrument": "/api/v1/instruments/",
            "get_instrument": "/api/v1/instruments/{instrument_id}",
            "list_instruments": "/api/v1/instruments/",
            "unified_instrument": "/api/v1/instruments/unified/{identifier}",
            "resolve_vendor": "/api/v1/instruments/resolve-vendor-symbol"
        },
        "capabilities": [
            "vendor_instruments",
            "instrument_xrefs", 
            "unified_instruments",
            "symbol_resolution",
            "batch_operations"
        ],
        "supported_vendors": ["ticker", "bloomberg", "reuters", "refinitiv"],
        "metadata": {
            "environment": "dev",
            "weight": 1,
            "tags": ["instruments", "reference-data"]
        }
    }