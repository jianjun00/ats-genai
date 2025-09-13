"""
Generic Service Interface Template

Use this template to create consistent service interfaces across all domains.
Replace {DOMAIN} with your domain name (e.g., MarketData, Analytics, Trading, News).

Example Usage:
    # For Market Data Service
    sed 's/{DOMAIN}/MarketData/g' service_interface_template.py > market_data_service_interface.py

    # For Analytics Service
    sed 's/{DOMAIN}/Analytics/g' service_interface_template.py > analytics_service_interface.py
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass


# ========================================================================================
# DOMAIN-SPECIFIC DTOS - CUSTOMIZE FOR YOUR DOMAIN
# ========================================================================================

@dataclass
class {DOMAIN}DTO:
    """
    Primary data transfer object for {DOMAIN} entities.

    Customize fields based on your domain requirements:
    - Keep fields immutable (use Optional for nullable fields)
    - Use proper types (date, datetime, Enum, etc.)
    - Add validation rules in service implementation
    """
    id: Optional[int] = None
    # TODO: Add domain-specific fields
    # symbol: str = None                    # For market data
    # calculation_type: str = None          # For analytics
    # order_type: str = None                # For trading
    # article_title: str = None             # For news
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class {DOMAIN}SearchCriteria:
    """
    Search and filtering criteria for {DOMAIN} operations.

    Customize based on common search patterns in your domain:
    """
    # Common search fields
    ids: Optional[List[int]] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    limit: Optional[int] = None
    offset: Optional[int] = None

    # TODO: Add domain-specific search criteria
    # symbols: Optional[List[str]] = None           # For market data
    # calculation_types: Optional[List[str]] = None # For analytics
    # order_statuses: Optional[List[str]] = None    # For trading
    # sentiment_range: Optional[tuple] = None       # For news


@dataclass
class {DOMAIN}OperationResult:
    """
    Standardized result for {DOMAIN} operations.

    Provides consistent success/error handling across all operations.
    """
    success: bool
    entity_id: Optional[int] = None
    entity_ids: Optional[List[int]] = None
    error_message: Optional[str] = None
    warning_message: Optional[str] = None
    created_count: Optional[int] = None
    updated_count: Optional[int] = None
    deleted_count: Optional[int] = None
    skipped_count: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class {DOMAIN}BulkOperationResult:
    """
    Result for bulk operations with detailed status per item.
    """
    overall_success: bool
    total_items: int
    successful_items: List[{DOMAIN}OperationResult]
    failed_items: List[{DOMAIN}OperationResult]
    summary: {DOMAIN}OperationResult


# ========================================================================================
# SERVICE INTERFACE - STANDARD OPERATIONS PATTERN
# ========================================================================================

class {DOMAIN}ServiceInterface(ABC):
    """
    Public interface for {DOMAIN} operations.

    This interface defines all operations that clients can perform.
    Service clients should only use this interface, never access DAOs directly.

    Standard Operations Pattern:
    - CRUD operations with business validation
    - Search and filtering capabilities
    - Batch processing for performance
    - Utility operations for common needs

    Usage:
        service = await get_{DOMAIN.lower()}_service()
        result = await service.create_{DOMAIN.lower()}(dto)
        if result.success:
            print(f"Created {DOMAIN.lower()} with ID: {{result.entity_id}}")
    """

    # ================================================================================
    # CORE CRUD OPERATIONS
    # ================================================================================

    @abstractmethod
    async def create_{DOMAIN.lower()}(self, dto: {DOMAIN}DTO) -> {DOMAIN}OperationResult:
        """
        Create a new {DOMAIN.lower()} entity with business validation.

        Args:
            dto: {DOMAIN} data with all required fields

        Returns:
            OperationResult with success status and entity ID or error details

        Business Rules:
            - TODO: Add domain-specific validation rules
            - TODO: Add uniqueness constraints
            - TODO: Add dependency validations
        """
        pass

    @abstractmethod
    async def get_{DOMAIN.lower()}_by_id(self, entity_id: int) -> Optional[{DOMAIN}DTO]:
        """
        Retrieve {DOMAIN.lower()} by primary key.

        Args:
            entity_id: Primary key of the entity

        Returns:
            {DOMAIN}DTO if found, None otherwise
        """
        pass

    @abstractmethod
    async def update_{DOMAIN.lower()}(self, dto: {DOMAIN}DTO) -> {DOMAIN}OperationResult:
        """
        Update existing {DOMAIN.lower()} entity.

        Args:
            dto: {DOMAIN} data with ID and updated fields

        Returns:
            OperationResult with success status or error details

        Business Rules:
            - Entity must exist (checked by ID)
            - TODO: Add domain-specific update validation
        """
        pass

    @abstractmethod
    async def delete_{DOMAIN.lower()}(self, entity_id: int) -> {DOMAIN}OperationResult:
        """
        Delete {DOMAIN.lower()} entity (soft delete recommended).

        Args:
            entity_id: Primary key of entity to delete

        Returns:
            OperationResult with success status or error details

        Business Rules:
            - TODO: Add cascade delete rules
            - TODO: Add soft delete vs hard delete logic
        """
        pass

    # ================================================================================
    # SEARCH AND FILTERING OPERATIONS
    # ================================================================================

    @abstractmethod
    async def list_{DOMAIN.lower()}s(self, criteria: {DOMAIN}SearchCriteria) -> List[{DOMAIN}DTO]:
        """
        List {DOMAIN.lower()} entities based on search criteria.

        Args:
            criteria: Search and filtering parameters

        Returns:
            List of {DOMAIN}DTOs matching criteria

        Performance Notes:
            - Results are paginated using limit/offset
            - Consider implementing cursor-based pagination for large datasets
            - Add indexes for common search fields
        """
        pass

    @abstractmethod
    async def search_{DOMAIN.lower()}s(
        self,
        query: str,
        criteria: Optional[{DOMAIN}SearchCriteria] = None
    ) -> List[{DOMAIN}DTO]:
        """
        Full-text search across {DOMAIN.lower()} entities.

        Args:
            query: Search query string
            criteria: Additional filtering criteria

        Returns:
            List of {DOMAIN}DTOs matching search query

        Implementation Notes:
            - TODO: Define searchable fields for your domain
            - Consider using full-text search engines for complex queries
        """
        pass

    @abstractmethod
    async def count_{DOMAIN.lower()}s(self, criteria: Optional[{DOMAIN}SearchCriteria] = None) -> int:
        """
        Count {DOMAIN.lower()} entities matching criteria.

        Args:
            criteria: Optional filtering criteria

        Returns:
            Total count of entities
        """
        pass

    # ================================================================================
    # BATCH OPERATIONS FOR PERFORMANCE
    # ================================================================================

    @abstractmethod
    async def create_{DOMAIN.lower()}s_batch(
        self,
        dtos: List[{DOMAIN}DTO]
    ) -> {DOMAIN}BulkOperationResult:
        """
        Create multiple {DOMAIN.lower()} entities in batch for performance.

        Args:
            dtos: List of {DOMAIN} entities to create

        Returns:
            BulkOperationResult with detailed success/failure information

        Performance Notes:
            - Use database batch operations for efficiency
            - Consider transaction boundaries for consistency
            - Implement partial success handling
        """
        pass

    @abstractmethod
    async def update_{DOMAIN.lower()}s_batch(
        self,
        dtos: List[{DOMAIN}DTO]
    ) -> {DOMAIN}BulkOperationResult:
        """
        Update multiple {DOMAIN.lower()} entities in batch.

        Args:
            dtos: List of {DOMAIN} entities to update (must include IDs)

        Returns:
            BulkOperationResult with detailed success/failure information
        """
        pass

    # ================================================================================
    # DOMAIN-SPECIFIC OPERATIONS - CUSTOMIZE FOR YOUR DOMAIN
    # ================================================================================

    # TODO: Add domain-specific operations here
    # Examples for different domains:

    # For Market Data Service:
    # @abstractmethod
    # async def get_ohlcv_data(self, symbol: str, timeframe: str, date_range: tuple) -> List[OHLCVData]:
    #     """Get OHLCV data for symbol and timeframe"""
    #     pass

    # For Analytics Service:
    # @abstractmethod
    # async def calculate_technical_indicators(self, symbol: str, indicators: List[str]) -> Dict[str, Any]:
    #     """Calculate technical indicators for symbol"""
    #     pass

    # For Trading Service:
    # @abstractmethod
    # async def place_order(self, order: OrderDTO) -> OrderResult:
    #     """Place trading order with risk checks"""
    #     pass

    # For News Service:
    # @abstractmethod
    # async def get_news_by_symbol(self, symbol: str, date_range: tuple) -> List[NewsArticleDTO]:
    #     """Get news articles related to symbol"""
    #     pass

    # ================================================================================
    # UTILITY AND METADATA OPERATIONS
    # ================================================================================

    @abstractmethod
    async def validate_{DOMAIN.lower()}_data(self, dto: {DOMAIN}DTO) -> {DOMAIN}OperationResult:
        """
        Validate {DOMAIN.lower()} data without persisting.

        Args:
            dto: {DOMAIN} data to validate

        Returns:
            OperationResult indicating validation success or specific errors

        Use Cases:
            - Form validation in UI
            - Data quality checks in ETL pipelines
            - Pre-flight checks before batch operations
        """
        pass

    @abstractmethod
    async def get_{DOMAIN.lower()}_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the {DOMAIN.lower()} service and its data.

        Returns:
            Dictionary with service metadata

        Example metadata:
            - Total entity count
            - Data freshness indicators
            - Available operations and their status
            - Service health indicators
        """
        pass

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on {DOMAIN.lower()} service.

        Returns:
            Dictionary with health status information

        Health Check Components:
            - Database connectivity
            - External service dependencies
            - Resource utilization
            - Recent error rates
        """
        pass


# ========================================================================================
# HELPER FUNCTIONS FOR SERVICE IMPLEMENTATION
# ========================================================================================

def create_success_result(
    entity_id: Optional[int] = None,
    entity_ids: Optional[List[int]] = None,
    created_count: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> {DOMAIN}OperationResult:
    """
    Helper to create successful operation result.

    Args:
        entity_id: Single entity ID for create operations
        entity_ids: Multiple entity IDs for batch operations
        created_count: Count of created entities
        metadata: Additional result metadata

    Returns:
        {DOMAIN}OperationResult indicating success
    """
    return {DOMAIN}OperationResult(
        success=True,
        entity_id=entity_id,
        entity_ids=entity_ids,
        created_count=created_count,
        metadata=metadata
    )


def create_error_result(
    error_message: str,
    entity_id: Optional[int] = None,
    warning_message: Optional[str] = None
) -> {DOMAIN}OperationResult:
    """
    Helper to create error operation result.

    Args:
        error_message: Descriptive error message
        entity_id: Entity ID related to the error (if applicable)
        warning_message: Additional warning information

    Returns:
        {DOMAIN}OperationResult indicating failure with error details
    """
    return {DOMAIN}OperationResult(
        success=False,
        error_message=error_message,
        entity_id=entity_id,
        warning_message=warning_message
    )


def create_validation_error_result(validation_errors: List[str]) -> {DOMAIN}OperationResult:
    """
    Helper to create validation error result.

    Args:
        validation_errors: List of validation error messages

    Returns:
        {DOMAIN}OperationResult with consolidated validation errors
    """
    error_message = "Validation failed: " + "; ".join(validation_errors)
    return create_error_result(error_message)


# ========================================================================================
# USAGE EXAMPLES AND PATTERNS
# ========================================================================================

"""
EXAMPLE USAGE PATTERNS:

1. Basic CRUD Operations:
    service = await get_{DOMAIN.lower()}_service()

    # Create
    dto = {DOMAIN}DTO(name="Example", description="Test entity")
    result = await service.create_{DOMAIN.lower()}(dto)
    if result.success:
        entity_id = result.entity_id

    # Read
    entity = await service.get_{DOMAIN.lower()}_by_id(entity_id)

    # Update
    entity.description = "Updated description"
    result = await service.update_{DOMAIN.lower()}(entity)

    # Delete
    result = await service.delete_{DOMAIN.lower()}(entity_id)

2. Search Operations:
    criteria = {DOMAIN}SearchCriteria(
        date_from=date(2024, 1, 1),
        date_to=date(2024, 12, 31),
        limit=100
    )
    entities = await service.list_{DOMAIN.lower()}s(criteria)

3. Batch Operations:
    dtos = [create_test_dto() for _ in range(1000)]
    result = await service.create_{DOMAIN.lower()}s_batch(dtos)
    print(f"Created {{result.successful_count}} entities")

4. Error Handling:
    result = await service.create_{DOMAIN.lower()}(invalid_dto)
    if not result.success:
        print(f"Error: {{result.error_message}}")
        if result.warning_message:
            print(f"Warning: {{result.warning_message}}")

5. Health Monitoring:
    health = await service.health_check()
    if health['status'] == 'healthy':
        print("Service is operational")
"""