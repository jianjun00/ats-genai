"""
Generic API Router Template

Use this template to create consistent HTTP API routers across all domains.
Replace {DOMAIN} with your domain name and customize for your specific endpoints.

Example Usage:
    # For Market Data API
    sed 's/{DOMAIN}/MarketData/g' api_router_template.py > market_data_api.py
"""

from fastapi import APIRouter, HTTPException, Query, Path, Depends, status
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime
import logging

# Import your domain service interface
from domains.{DOMAIN.lower()}.services.interfaces.{DOMAIN.lower()}_service_interface import (
    {DOMAIN}ServiceInterface,
    {DOMAIN}DTO,
    {DOMAIN}SearchCriteria,
    {DOMAIN}OperationResult,
    {DOMAIN}BulkOperationResult
)

# Import service container for dependency injection
from domains.{DOMAIN.lower()}.services.config.service_container import get_{DOMAIN.lower()}_service

logger = logging.getLogger(__name__)

# Create router with proper prefix and tags
{DOMAIN.lower()}_router = APIRouter(
    prefix="/api/v1/{DOMAIN.lower()}",
    tags=["{DOMAIN.lower()}"],
    responses={
        400: {"description": "Bad Request - Validation Error"},
        404: {"description": "Resource Not Found"},
        500: {"description": "Internal Server Error"}
    }
)

# ========================================================================================
# HTTP REQUEST/RESPONSE MODELS - SEPARATE FROM SERVICE DTOS
# ========================================================================================

class {DOMAIN}Request(BaseModel):
    """
    HTTP request model for creating/updating {DOMAIN.lower()} entities.

    Note: This is separate from service DTOs to maintain proper layer separation.
    HTTP models handle serialization, validation, and API documentation.
    Service DTOs handle business logic and data consistency.
    """
    # TODO: Add your domain-specific fields with proper validation
    # symbol: str = Field(..., min_length=1, max_length=10, description="Entity symbol")
    # name: Optional[str] = Field(None, max_length=255, description="Entity name")
    # description: Optional[str] = Field(None, max_length=1000, description="Entity description")

    # Common fields
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

    class Config:
        schema_extra = {
            "example": {
                # TODO: Add example request data
                # "symbol": "AAPL",
                # "name": "Apple Inc.",
                # "description": "Technology company",
                "metadata": {"source": "api", "version": "1.0"}
            }
        }


class {DOMAIN}Response(BaseModel):
    """HTTP response model for {DOMAIN.lower()} entity data"""
    id: int = Field(..., description="Unique entity identifier")
    # TODO: Add your domain-specific response fields
    # symbol: str = Field(..., description="Entity symbol")
    # name: Optional[str] = Field(None, description="Entity name")
    # description: Optional[str] = Field(None, description="Entity description")

    # Common response fields
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    class Config:
        schema_extra = {
            "example": {
                "id": 123,
                # TODO: Add example response data
                # "symbol": "AAPL",
                # "name": "Apple Inc.",
                # "description": "Technology company",
                "created_at": "2024-01-01T10:00:00Z",
                "updated_at": "2024-01-01T10:00:00Z"
            }
        }


class {DOMAIN}SearchRequest(BaseModel):
    """HTTP model for search criteria"""
    # Common search parameters
    date_from: Optional[date] = Field(None, description="Start date filter")
    date_to: Optional[date] = Field(None, description="End date filter")
    limit: Optional[int] = Field(100, ge=1, le=1000, description="Maximum results")
    offset: Optional[int] = Field(0, ge=0, description="Results offset")

    # TODO: Add domain-specific search fields
    # symbols: Optional[List[str]] = Field(None, description="Filter by symbols")
    # categories: Optional[List[str]] = Field(None, description="Filter by categories")
    # status: Optional[str] = Field(None, description="Filter by status")


class OperationResponse(BaseModel):
    """Standardized response for operations"""
    success: bool = Field(..., description="Operation success status")
    message: Optional[str] = Field(None, description="Result message")
    entity_id: Optional[int] = Field(None, description="Created/updated entity ID")
    entity_ids: Optional[List[int]] = Field(None, description="Batch operation entity IDs")
    created_count: Optional[int] = Field(None, description="Number of entities created")
    updated_count: Optional[int] = Field(None, description="Number of entities updated")
    deleted_count: Optional[int] = Field(None, description="Number of entities deleted")
    skipped_count: Optional[int] = Field(None, description="Number of entities skipped")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional result data")


class {DOMAIN}ListResponse(BaseModel):
    """Response for list operations"""
    items: List[{DOMAIN}Response] = Field(..., description="List of entities")
    total_count: int = Field(..., description="Total available items")
    has_more: bool = Field(..., description="Whether more items are available")
    next_offset: Optional[int] = Field(None, description="Offset for next page")


class BulkOperationResponse(BaseModel):
    """Response for bulk operations"""
    overall_success: bool = Field(..., description="Overall operation success")
    total_items: int = Field(..., description="Total items processed")
    successful_count: int = Field(..., description="Successfully processed items")
    failed_count: int = Field(..., description="Failed items")
    summary: OperationResponse = Field(..., description="Operation summary")
    errors: Optional[List[str]] = Field(None, description="Error details for failed items")


# ========================================================================================
# DEPENDENCY INJECTION
# ========================================================================================

async def get_service() -> {DOMAIN}ServiceInterface:
    """
    Dependency injection for {DOMAIN} service.

    This function provides the service instance to API endpoints.
    It uses the service container for proper dependency management.
    """
    return await get_{DOMAIN.lower()}_service()


# ========================================================================================
# UTILITY FUNCTIONS FOR MODEL CONVERSION
# ========================================================================================

def request_to_dto(request: {DOMAIN}Request) -> {DOMAIN}DTO:
    """Convert HTTP request model to service DTO"""
    return {DOMAIN}DTO(
        # TODO: Map request fields to DTO
        # symbol=request.symbol,
        # name=request.name,
        # description=request.description,
    )


def dto_to_response(dto: {DOMAIN}DTO) -> {DOMAIN}Response:
    """Convert service DTO to HTTP response model"""
    return {DOMAIN}Response(
        id=dto.id,
        # TODO: Map DTO fields to response
        # symbol=dto.symbol,
        # name=dto.name,
        # description=dto.description,
        created_at=dto.created_at,
        updated_at=dto.updated_at
    )


def search_request_to_criteria(request: {DOMAIN}SearchRequest) -> {DOMAIN}SearchCriteria:
    """Convert HTTP search request to service search criteria"""
    return {DOMAIN}SearchCriteria(
        date_from=request.date_from,
        date_to=request.date_to,
        limit=request.limit,
        offset=request.offset,
        # TODO: Map search-specific fields
        # symbols=request.symbols,
        # categories=request.categories,
        # status=request.status,
    )


def operation_result_to_response(result: {DOMAIN}OperationResult) -> OperationResponse:
    """Convert service operation result to HTTP response"""
    return OperationResponse(
        success=result.success,
        message=result.error_message if not result.success else "Operation completed successfully",
        entity_id=result.entity_id,
        entity_ids=result.entity_ids,
        created_count=result.created_count,
        updated_count=result.updated_count,
        deleted_count=result.deleted_count,
        skipped_count=result.skipped_count,
        metadata=result.metadata
    )


def bulk_result_to_response(result: {DOMAIN}BulkOperationResult) -> BulkOperationResponse:
    """Convert service bulk result to HTTP response"""
    errors = []
    for failed_item in result.failed_items:
        if failed_item.error_message:
            errors.append(failed_item.error_message)

    return BulkOperationResponse(
        overall_success=result.overall_success,
        total_items=result.total_items,
        successful_count=len(result.successful_items),
        failed_count=len(result.failed_items),
        summary=operation_result_to_response(result.summary),
        errors=errors if errors else None
    )


# ========================================================================================
# CORE CRUD ENDPOINTS
# ========================================================================================

@{DOMAIN.lower()}_router.post("/",
                             response_model=OperationResponse,
                             status_code=status.HTTP_201_CREATED,
                             summary="Create {DOMAIN} Entity",
                             description="Create a new {DOMAIN.lower()} entity with validation")
async def create_{DOMAIN.lower()}(
    request: {DOMAIN}Request,
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Create a new {DOMAIN.lower()} entity"""
    try:
        dto = request_to_dto(request)
        result = await service.create_{DOMAIN.lower()}(dto)

        if not result.success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result.error_message
            )

        return operation_result_to_response(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating {DOMAIN.lower()}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.get("/{entity_id}",
                            response_model={DOMAIN}Response,
                            summary="Get {DOMAIN} by ID",
                            description="Retrieve a {DOMAIN.lower()} entity by its ID")
async def get_{DOMAIN.lower()}_by_id(
    entity_id: int = Path(..., description="Entity ID", ge=1),
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Get {DOMAIN.lower()} entity by ID"""
    try:
        dto = await service.get_{DOMAIN.lower()}_by_id(entity_id)

        if not dto:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{DOMAIN} entity {entity_id} not found"
            )

        return dto_to_response(dto)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving {DOMAIN.lower()} {entity_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.put("/{entity_id}",
                            response_model=OperationResponse,
                            summary="Update {DOMAIN} Entity",
                            description="Update an existing {DOMAIN.lower()} entity")
async def update_{DOMAIN.lower()}(
    entity_id: int = Path(..., description="Entity ID", ge=1),
    request: {DOMAIN}Request = None,
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Update {DOMAIN.lower()} entity"""
    try:
        dto = request_to_dto(request)
        dto.id = entity_id  # Ensure ID matches path parameter

        result = await service.update_{DOMAIN.lower()}(dto)

        if not result.success:
            if "not found" in result.error_message.lower():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=result.error_message
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=result.error_message
                )

        return operation_result_to_response(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating {DOMAIN.lower()} {entity_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.delete("/{entity_id}",
                               response_model=OperationResponse,
                               summary="Delete {DOMAIN} Entity",
                               description="Delete a {DOMAIN.lower()} entity")
async def delete_{DOMAIN.lower()}(
    entity_id: int = Path(..., description="Entity ID", ge=1),
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Delete {DOMAIN.lower()} entity"""
    try:
        result = await service.delete_{DOMAIN.lower()}(entity_id)

        if not result.success:
            if "not found" in result.error_message.lower():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=result.error_message
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=result.error_message
                )

        return operation_result_to_response(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting {DOMAIN.lower()} {entity_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# ========================================================================================
# SEARCH AND LISTING ENDPOINTS
# ========================================================================================

@{DOMAIN.lower()}_router.get("/",
                            response_model={DOMAIN}ListResponse,
                            summary="List {DOMAIN} Entities",
                            description="List {DOMAIN.lower()} entities with optional filtering")
async def list_{DOMAIN.lower()}s(
    date_from: Optional[date] = Query(None, description="Filter by start date"),
    date_to: Optional[date] = Query(None, description="Filter by end date"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results offset"),
    # TODO: Add domain-specific query parameters
    # symbols: Optional[List[str]] = Query(None, description="Filter by symbols"),
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """List {DOMAIN.lower()} entities with filtering"""
    try:
        # Build search criteria from query parameters
        search_request = {DOMAIN}SearchRequest(
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            # TODO: Add domain-specific parameters
            # symbols=symbols,
        )

        criteria = search_request_to_criteria(search_request)

        # Get entities and total count
        entities = await service.list_{DOMAIN.lower()}s(criteria)
        total_count = await service.count_{DOMAIN.lower()}s(criteria)

        # Convert to response format
        items = [dto_to_response(dto) for dto in entities]
        has_more = offset + len(items) < total_count
        next_offset = offset + len(items) if has_more else None

        return {DOMAIN}ListResponse(
            items=items,
            total_count=total_count,
            has_more=has_more,
            next_offset=next_offset
        )

    except Exception as e:
        logger.error(f"Error listing {DOMAIN.lower()}s: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.get("/search",
                            response_model={DOMAIN}ListResponse,
                            summary="Search {DOMAIN} Entities",
                            description="Full-text search across {DOMAIN.lower()} entities")
async def search_{DOMAIN.lower()}s(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results offset"),
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Search {DOMAIN.lower()} entities"""
    try:
        criteria = {DOMAIN}SearchCriteria(limit=limit, offset=offset)
        entities = await service.search_{DOMAIN.lower()}s(q, criteria)

        # For search, we don't have exact total count easily available
        # So we estimate based on returned results
        items = [dto_to_response(dto) for dto in entities]
        has_more = len(items) >= limit  # Simple heuristic

        return {DOMAIN}ListResponse(
            items=items,
            total_count=len(items),  # Approximate for search
            has_more=has_more,
            next_offset=offset + len(items) if has_more else None
        )

    except Exception as e:
        logger.error(f"Error searching {DOMAIN.lower()}s with query '{q}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.get("/count",
                            response_model=int,
                            summary="Count {DOMAIN} Entities",
                            description="Get total count of {DOMAIN.lower()} entities")
async def count_{DOMAIN.lower()}s(
    date_from: Optional[date] = Query(None, description="Filter by start date"),
    date_to: Optional[date] = Query(None, description="Filter by end date"),
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Count {DOMAIN.lower()} entities"""
    try:
        criteria = {DOMAIN}SearchCriteria(
            date_from=date_from,
            date_to=date_to
        ) if date_from or date_to else None

        count = await service.count_{DOMAIN.lower()}s(criteria)
        return count

    except Exception as e:
        logger.error(f"Error counting {DOMAIN.lower()}s: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# ========================================================================================
# BATCH OPERATIONS ENDPOINTS
# ========================================================================================

@{DOMAIN.lower()}_router.post("/batch",
                             response_model=BulkOperationResponse,
                             status_code=status.HTTP_201_CREATED,
                             summary="Batch Create {DOMAIN} Entities",
                             description="Create multiple {DOMAIN.lower()} entities in batch")
async def create_{DOMAIN.lower()}s_batch(
    requests: List[{DOMAIN}Request],
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Create multiple {DOMAIN.lower()} entities in batch"""
    try:
        if not requests:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Request body cannot be empty"
            )

        if len(requests) > 1000:  # Reasonable batch size limit
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch size cannot exceed 1000 items"
            )

        dtos = [request_to_dto(req) for req in requests]
        result = await service.create_{DOMAIN.lower()}s_batch(dtos)

        return bulk_result_to_response(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in batch {DOMAIN.lower()} creation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.put("/batch",
                            response_model=BulkOperationResponse,
                            summary="Batch Update {DOMAIN} Entities",
                            description="Update multiple {DOMAIN.lower()} entities in batch")
async def update_{DOMAIN.lower()}s_batch(
    requests: List[{DOMAIN}Request],
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Update multiple {DOMAIN.lower()} entities in batch"""
    try:
        if not requests:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Request body cannot be empty"
            )

        dtos = [request_to_dto(req) for req in requests]

        # Validate that all DTOs have IDs for updates
        missing_ids = [i for i, dto in enumerate(dtos) if not dto.id]
        if missing_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing IDs for update at indices: {missing_ids}"
            )

        result = await service.update_{DOMAIN.lower()}s_batch(dtos)

        return bulk_result_to_response(result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in batch {DOMAIN.lower()} update: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


# ========================================================================================
# UTILITY ENDPOINTS
# ========================================================================================

@{DOMAIN.lower()}_router.post("/validate",
                             response_model=OperationResponse,
                             summary="Validate {DOMAIN} Data",
                             description="Validate {DOMAIN.lower()} data without persisting")
async def validate_{DOMAIN.lower()}_data(
    request: {DOMAIN}Request,
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Validate {DOMAIN.lower()} data without creating"""
    try:
        dto = request_to_dto(request)
        result = await service.validate_{DOMAIN.lower()}_data(dto)

        return operation_result_to_response(result)

    except Exception as e:
        logger.error(f"Error validating {DOMAIN.lower()} data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.get("/metadata",
                            response_model=Dict[str, Any],
                            summary="Get Service Metadata",
                            description="Get metadata about the {DOMAIN.lower()} service")
async def get_metadata(
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Get service metadata"""
    try:
        metadata = await service.get_{DOMAIN.lower()}_metadata()
        return metadata

    except Exception as e:
        logger.error(f"Error getting {DOMAIN.lower()} metadata: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@{DOMAIN.lower()}_router.get("/health",
                            response_model=Dict[str, Any],
                            summary="Health Check",
                            description="Perform health check on {DOMAIN.lower()} service")
async def health_check(
    service: {DOMAIN}ServiceInterface = Depends(get_service)
):
    """Service health check"""
    try:
        health = await service.health_check()

        # Return appropriate HTTP status based on health
        if health.get('status') == 'unhealthy':
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=health
            )
        elif health.get('status') == 'error':
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=health
            )

        return health

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Health check error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "error": str(e)}
        )


# ========================================================================================
# TODO: ADD DOMAIN-SPECIFIC ENDPOINTS
# ========================================================================================

# TODO: Add your domain-specific API endpoints here
# Examples for different domains:

# For Market Data Service:
# @{DOMAIN.lower()}_router.get("/{symbol}/ohlcv")
# async def get_ohlcv_data(symbol: str, timeframe: str, start_date: date, end_date: date):
#     """Get OHLCV data for symbol"""
#     pass

# For Analytics Service:
# @{DOMAIN.lower()}_router.post("/{symbol}/indicators")
# async def calculate_indicators(symbol: str, indicators: List[str]):
#     """Calculate technical indicators"""
#     pass

# For Trading Service:
# @{DOMAIN.lower()}_router.post("/orders")
# async def place_order(order: OrderRequest):
#     """Place trading order"""
#     pass

# For News Service:
# @{DOMAIN.lower()}_router.get("/articles/{symbol}")
# async def get_news_by_symbol(symbol: str, date_range: tuple):
#     """Get news articles for symbol"""
#     pass


# ========================================================================================
# ERROR HANDLERS (OPTIONAL - CAN BE GLOBAL)
# ========================================================================================

@{DOMAIN.lower()}_router.exception_handler(ValueError)
async def value_error_handler(request, exc):
    """Handle validation errors"""
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=str(exc)
    )


# ========================================================================================
# USAGE EXAMPLES AND TESTING
# ========================================================================================

"""
API USAGE EXAMPLES:

1. Create Entity:
   POST /api/v1/{DOMAIN.lower()}/
   {
     "symbol": "AAPL",
     "name": "Apple Inc.",
     "description": "Technology company"
   }

2. Get Entity:
   GET /api/v1/{DOMAIN.lower()}/123

3. List Entities:
   GET /api/v1/{DOMAIN.lower()}/?limit=50&offset=0&date_from=2024-01-01

4. Search Entities:
   GET /api/v1/{DOMAIN.lower()}/search?q=apple&limit=10

5. Batch Create:
   POST /api/v1/{DOMAIN.lower()}/batch
   [
     {"symbol": "AAPL", "name": "Apple Inc."},
     {"symbol": "GOOGL", "name": "Alphabet Inc."}
   ]

6. Health Check:
   GET /api/v1/{DOMAIN.lower()}/health

7. Validate Data:
   POST /api/v1/{DOMAIN.lower()}/validate
   {"symbol": "TEST", "name": "Test Entity"}
"""