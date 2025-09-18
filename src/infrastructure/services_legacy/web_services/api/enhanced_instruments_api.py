"""
Enhanced Instruments API using InstrumentService with caching, monitoring, and validation.

Provides RESTful endpoints for instrument management with:
- Service layer integration
- Request/response validation
- Caching headers
- Performance monitoring
- Error handling
- OpenAPI documentation
"""

from fastapi import FastAPI, HTTPException, Depends, Query, Path, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    InstrumentXrefDTO,
    InstrumentSearchCriteria
)
from domains.instruments.services.config.service_container import provide_instrument_service

logger = logging.getLogger(__name__)


# Pydantic Models for API
class InstrumentResponse(BaseModel):
    """Response model for instrument data"""
    id: Optional[int] = None
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    currency: Optional[str] = None
    list_date: Optional[datetime] = None
    delist_date: Optional[datetime] = None
    is_active: Optional[bool] = True
    
    class Config:
        from_attributes = True


class InstrumentCreateRequest(BaseModel):
    """Request model for creating instruments"""
    symbol: str = Field(..., min_length=1, max_length=20)
    name: Optional[str] = Field(None, max_length=200)
    exchange: Optional[str] = Field(None, max_length=50)
    instrument_type: str = Field("stock", max_length=50)
    currency: str = Field("USD", max_length=3)
    
    @validator('symbol')
    def symbol_must_be_uppercase(cls, v):
        return v.upper()


class InstrumentXrefResponse(BaseModel):
    """Response model for instrument cross-references"""
    instrument_id: int
    vendor_name: str
    vendor_symbol: str
    xref_type: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class InstrumentXrefCreateRequest(BaseModel):
    """Request model for creating cross-references"""
    instrument_id: int = Field(..., gt=0)
    vendor_name: str = Field(..., min_length=1, max_length=50)
    vendor_symbol: str = Field(..., min_length=1, max_length=50)
    xref_type: Optional[str] = Field("equity", max_length=50)


class InstrumentSearchRequest(BaseModel):
    """Request model for searching instruments"""
    symbols: Optional[List[str]] = None
    exchanges: Optional[List[str]] = None
    instrument_types: Optional[List[str]] = None
    currencies: Optional[List[str]] = None
    active_only: bool = True
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)


class OperationResponse(BaseModel):
    """Generic operation response model"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    created_count: Optional[int] = None
    updated_count: Optional[int] = None
    error_details: Optional[List[str]] = None


class CacheStatsResponse(BaseModel):
    """Cache performance statistics response"""
    hits: int
    misses: int
    hit_rate: float
    cache_health: Dict[str, Any]
    total_operations: int


# Create FastAPI app
app = FastAPI(
    title="ATS Instruments API (Enhanced)",
    description="Enhanced RESTful API for financial instruments with service layer architecture",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency injection
async def get_instrument_service() -> InstrumentServiceInterface:
    """FastAPI dependency to provide InstrumentService"""
    return await provide_instrument_service()


# Health Check Endpoint
@app.get("/health", 
         summary="Health Check",
         description="Check API and service health")
async def health_check(
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> Dict[str, Any]:
    """Health check endpoint with service validation"""
    try:
        # Test service connectivity
        count = await service.get_instrument_count()
        
        # Get cache statistics if available
        cache_stats = {}
        if hasattr(service, 'get_cache_stats'):
            cache_stats = await service.get_cache_stats()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "instruments-api-enhanced",
            "version": "2.0.0",
            "instrument_count": count,
            "cache_stats": cache_stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )


# Instrument Endpoints
@app.get("/instruments/{instrument_id}",
         response_model=InstrumentResponse,
         summary="Get Instrument by ID",
         description="Retrieve instrument details by ID with caching")
async def get_instrument(
    instrument_id: int = Path(..., gt=0, description="Instrument ID"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> InstrumentResponse:
    """Get instrument by ID with caching optimization"""
    try:
        instrument = await service.get_instrument_by_id(instrument_id)
        
        if not instrument:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Instrument with ID {instrument_id} not found"
            )
        
        return InstrumentResponse(**instrument.__dict__)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting instrument {instrument_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.get("/instruments",
         response_model=List[InstrumentResponse],
         summary="Search Instruments", 
         description="Search instruments with filtering and pagination")
async def search_instruments(
    symbols: Optional[str] = Query(None, description="Comma-separated symbols"),
    exchanges: Optional[str] = Query(None, description="Comma-separated exchanges"),
    instrument_types: Optional[str] = Query(None, description="Comma-separated types"),
    currencies: Optional[str] = Query(None, description="Comma-separated currencies"),
    active_only: bool = Query(True, description="Only active instruments"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results offset"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> List[InstrumentResponse]:
    """Search instruments with comprehensive filtering"""
    try:
        # Parse query parameters
        symbols_list = [s.strip().upper() for s in symbols.split(',')] if symbols else None
        exchanges_list = [e.strip().upper() for e in exchanges.split(',')] if exchanges else None
        types_list = [t.strip() for t in instrument_types.split(',')] if instrument_types else None
        currencies_list = [c.strip().upper() for c in currencies.split(',')] if currencies else None
        
        # Create search criteria
        criteria = InstrumentSearchCriteria(
            symbols=symbols_list,
            exchanges=exchanges_list,
            instrument_types=types_list,
            currencies=currencies_list,
            active_only=active_only,
            limit=limit,
            offset=offset
        )
        
        instruments = await service.list_instruments(criteria)
        
        return [InstrumentResponse(**inst.__dict__) for inst in instruments]
        
    except Exception as e:
        logger.error(f"Error searching instruments: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.get("/instruments/symbol/{symbol}",
         response_model=InstrumentResponse,
         summary="Get Instrument by Symbol",
         description="Retrieve instrument by symbol with vendor support")
async def get_instrument_by_symbol(
    symbol: str = Path(..., min_length=1, max_length=20, description="Instrument symbol"),
    vendor: str = Query("ticker", description="Vendor name for symbol resolution"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> InstrumentResponse:
    """Get instrument by symbol with vendor-specific resolution"""
    try:
        symbol = symbol.upper()
        instrument = await service.get_instrument_by_symbol(symbol, vendor)
        
        if not instrument:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Instrument with symbol '{symbol}' not found for vendor '{vendor}'"
            )
        
        return InstrumentResponse(**instrument.__dict__)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting instrument {symbol} from {vendor}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.post("/instruments",
          response_model=OperationResponse,
          status_code=status.HTTP_201_CREATED,
          summary="Create Instrument",
          description="Create new instrument with validation")
async def create_instrument(
    request: InstrumentCreateRequest,
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> OperationResponse:
    """Create new instrument with comprehensive validation"""
    try:
        # Convert request to DTO
        instrument_dto = InstrumentDTO(**request.dict())
        
        # Create instrument
        result = await service.create_instrument(instrument_dto)
        
        if result.success:
            return OperationResponse(
                success=True,
                message=f"Instrument '{request.symbol}' created successfully",
                data={"instrument_id": result.instrument_id},
                created_count=result.created_count
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result.error_message or "Failed to create instrument"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating instrument {request.symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# Cross-Reference Endpoints
@app.get("/instruments/{instrument_id}/xrefs",
         response_model=List[InstrumentXrefResponse],
         summary="Get Cross-References",
         description="Get all cross-references for an instrument")
async def get_cross_references(
    instrument_id: int = Path(..., gt=0, description="Instrument ID"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> List[InstrumentXrefResponse]:
    """Get cross-references with caching optimization"""
    try:
        xrefs = await service.get_cross_references(instrument_id)
        
        return [InstrumentXrefResponse(**xref.__dict__) for xref in xrefs]
        
    except Exception as e:
        logger.error(f"Error getting cross-references for {instrument_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.post("/instruments/{instrument_id}/xrefs",
          response_model=OperationResponse,
          status_code=status.HTTP_201_CREATED,
          summary="Create Cross-Reference",
          description="Create new cross-reference for instrument")
async def create_cross_reference(
    instrument_id: int,
    request: InstrumentXrefCreateRequest,
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> OperationResponse:
    """Create cross-reference with validation"""
    try:
        # Validate instrument exists
        instrument = await service.get_instrument_by_id(instrument_id)
        if not instrument:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Instrument with ID {instrument_id} not found"
            )
        
        # Update request with instrument_id from path
        request.instrument_id = instrument_id
        
        # Convert to DTO
        xref_dto = InstrumentXrefDTO(**request.dict())
        
        # Create cross-reference
        result = await service.create_cross_reference(xref_dto)
        
        if result.success:
            return OperationResponse(
                success=True,
                message=f"Cross-reference created for {request.vendor_name}:{request.vendor_symbol}",
                created_count=result.created_count
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result.error_message or "Failed to create cross-reference"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating cross-reference: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# Utility Endpoints
@app.get("/instruments/validate/{symbol}",
         summary="Validate Symbol",
         description="Check if symbol exists in the system")
async def validate_symbol(
    symbol: str = Path(..., min_length=1, max_length=20),
    vendor: str = Query("ticker", description="Vendor for symbol validation"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> Dict[str, Any]:
    """Validate if symbol exists"""
    try:
        symbol = symbol.upper()
        is_valid = await service.validate_symbol(symbol, vendor)
        
        return {
            "symbol": symbol,
            "vendor": vendor,
            "is_valid": is_valid,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error validating symbol {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.get("/instruments/count",
         summary="Get Instrument Count",
         description="Get total number of instruments")
async def get_instrument_count(
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> Dict[str, Any]:
    """Get total instrument count with caching"""
    try:
        count = await service.get_instrument_count()
        
        return {
            "total_instruments": count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting instrument count: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# Cache Management Endpoints
@app.get("/cache/stats",
         response_model=CacheStatsResponse,
         summary="Cache Statistics",
         description="Get cache performance statistics")
async def get_cache_stats(
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> CacheStatsResponse:
    """Get cache performance statistics"""
    try:
        if hasattr(service, 'get_cache_stats'):
            stats = await service.get_cache_stats()
            return CacheStatsResponse(**stats)
        else:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Cache statistics not available for this service implementation"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.post("/cache/warm",
          response_model=OperationResponse,
          summary="Warm Cache",
          description="Pre-warm cache with common instruments")
async def warm_cache(
    symbols: Optional[str] = Query(None, description="Comma-separated symbols to warm"),
    limit: int = Query(1000, ge=1, le=5000, description="Number of instruments to warm"),
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> OperationResponse:
    """Warm cache with frequently accessed data"""
    try:
        if hasattr(service, 'warm_cache'):
            symbols_list = [s.strip().upper() for s in symbols.split(',')] if symbols else None
            await service.warm_cache(symbols=symbols_list, limit=limit)
            
            return OperationResponse(
                success=True,
                message=f"Cache warmed successfully",
                data={
                    "symbols_count": len(symbols_list) if symbols_list else 0,
                    "limit": limit
                }
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Cache warming not available for this service implementation"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error warming cache: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.delete("/cache",
            response_model=OperationResponse,
            summary="Clear Cache",
            description="Clear all cached instrument data")
async def clear_cache(
    service: InstrumentServiceInterface = Depends(get_instrument_service)
) -> OperationResponse:
    """Clear all cached data"""
    try:
        if hasattr(service, 'invalidate_all_cache'):
            deleted_count = await service.invalidate_all_cache()
            
            return OperationResponse(
                success=True,
                message=f"Cache cleared successfully",
                data={"deleted_keys": deleted_count}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Cache clearing not available for this service implementation"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")