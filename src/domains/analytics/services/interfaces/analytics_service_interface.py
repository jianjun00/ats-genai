"""
Analytics Service Interface

Defines the business logic interface for analytics operations including
events management, economic events, and analytics data processing.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class EventDTO:
    """Event data transfer object"""
    id: Optional[int] = None
    event_type: Optional[str] = None
    instrument_id: Optional[int] = None
    event_time: Optional[datetime] = None
    reported_time: Optional[datetime] = None
    source: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


@dataclass
class EconomicEventTypeDTO:
    """Economic event type data transfer object"""
    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    country: Optional[str] = None
    importance_level: Optional[int] = None
    frequency: Optional[str] = None
    typical_release_time: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class EconomicEventDTO:
    """Economic event data transfer object"""
    id: Optional[int] = None
    event_type_id: Optional[int] = None
    date: Optional[date] = None
    release_time: Optional[datetime] = None
    estimate: Optional[Decimal] = None
    actual: Optional[Decimal] = None
    previous: Optional[Decimal] = None
    revised: Optional[Decimal] = None
    unit: Optional[str] = None
    currency: Optional[str] = None
    source_vendor: Optional[str] = None
    source_event_id: Optional[str] = None
    is_preliminary: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class EventSearchCriteria:
    """Search criteria for events"""
    instrument_id: Optional[int] = None
    event_type: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: Optional[int] = 1000
    offset: Optional[int] = None
    sources: Optional[List[str]] = None


@dataclass
class EconomicEventSearchCriteria:
    """Search criteria for economic events"""
    event_type_ids: Optional[List[int]] = None
    countries: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    importance_levels: Optional[List[int]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit: Optional[int] = 1000
    offset: Optional[int] = None


@dataclass
class AnalyticsOperationResult:
    """Result of analytics operations"""
    success: bool
    event_id: Optional[int] = None
    created_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class AnalyticsServiceInterface(ABC):
    """
    Interface for analytics business operations.
    
    This service handles:
    1. Event management and querying
    2. Economic events and types management
    3. Analytics data processing
    4. Cross-domain analytics operations
    """

    # Event Operations
    
    @abstractmethod
    async def create_event(self, event: EventDTO) -> AnalyticsOperationResult:
        """Create a new event with business validation"""
    
    @abstractmethod
    async def get_event_by_id(self, event_id: int) -> Optional[EventDTO]:
        """Retrieve event by ID"""
    
    @abstractmethod
    async def list_events(self, criteria: EventSearchCriteria) -> List[EventDTO]:
        """List events based on search criteria"""
    
    @abstractmethod
    async def get_events_count(self, criteria: Optional[EventSearchCriteria] = None) -> int:
        """Get total number of events matching criteria"""
    
    @abstractmethod
    async def create_events_batch(self, events: List[EventDTO]) -> AnalyticsOperationResult:
        """Create multiple events in batch"""
    
    # Economic Event Type Operations
    
    @abstractmethod
    async def create_economic_event_type(self, event_type: EconomicEventTypeDTO) -> AnalyticsOperationResult:
        """Create a new economic event type"""
    
    @abstractmethod
    async def get_economic_event_type_by_id(self, type_id: int) -> Optional[EconomicEventTypeDTO]:
        """Retrieve economic event type by ID"""
    
    @abstractmethod
    async def list_economic_event_types(self, 
                                      category: Optional[str] = None,
                                      country: Optional[str] = None) -> List[EconomicEventTypeDTO]:
        """List economic event types with optional filtering"""
    
    @abstractmethod
    async def update_economic_event_type(self, event_type: EconomicEventTypeDTO) -> AnalyticsOperationResult:
        """Update economic event type"""
    
    # Economic Event Operations
    
    @abstractmethod
    async def create_economic_event(self, event: EconomicEventDTO) -> AnalyticsOperationResult:
        """Create a new economic event"""
    
    @abstractmethod
    async def get_economic_event_by_id(self, event_id: int) -> Optional[EconomicEventDTO]:
        """Retrieve economic event by ID"""
    
    @abstractmethod
    async def list_economic_events(self, criteria: EconomicEventSearchCriteria) -> List[EconomicEventDTO]:
        """List economic events based on search criteria"""
    
    @abstractmethod
    async def update_economic_event(self, event: EconomicEventDTO) -> AnalyticsOperationResult:
        """Update economic event (for revisions, actuals, etc.)"""
    
    @abstractmethod
    async def create_economic_events_batch(self, events: List[EconomicEventDTO]) -> AnalyticsOperationResult:
        """Create multiple economic events in batch"""
    
    # Analytics Operations
    
    @abstractmethod
    async def get_economic_calendar(self, 
                                  start_date: date, 
                                  end_date: date,
                                  importance_level: Optional[int] = None,
                                  countries: Optional[List[str]] = None) -> List[EconomicEventDTO]:
        """Get economic calendar for date range"""
    
    @abstractmethod
    async def get_events_by_instrument(self, 
                                     instrument_id: int,
                                     start_date: Optional[datetime] = None,
                                     end_date: Optional[datetime] = None) -> List[EventDTO]:
        """Get all events for a specific instrument"""
    
    @abstractmethod
    async def get_analytics_summary(self) -> Dict[str, Any]:
        """Get analytics data summary and statistics"""
    
    # Data Quality Operations
    
    @abstractmethod
    async def validate_event_data(self, event: EventDTO) -> Dict[str, Any]:
        """Validate event data quality and consistency"""
    
    @abstractmethod
    async def get_data_quality_report(self) -> Dict[str, Any]:
        """Get data quality report for analytics data"""
