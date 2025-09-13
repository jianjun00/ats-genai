"""
Analytics Service Implementation

Business logic layer for analytics operations including events management,
economic events, and analytics data processing.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, date

from domains.analytics.services.interfaces.analytics_service_interface import (
    AnalyticsServiceInterface,
    EventDTO,
    EconomicEventTypeDTO, 
    EconomicEventDTO,
    EventSearchCriteria,
    EconomicEventSearchCriteria,
    AnalyticsOperationResult
)

# DAO imports - only the service implementation should import these
from core.dao.analytics.events_dao import EventsDAO
from core.dao.analytics.economic_events_dao import EconomicEventsDAO

# Monitoring imports
from infrastructure.monitoring.instrument_service_monitor import (
    record_instrument_business_metric
)

logger = logging.getLogger(__name__)


class AnalyticsServiceImpl(AnalyticsServiceInterface):
    """
    Business logic implementation for analytics operations.
    
    This class:
    1. Implements all business rules and validation logic for analytics
    2. Coordinates between multiple analytics DAOs
    3. Provides transaction boundaries for complex operations
    4. Handles error scenarios and logging
    5. Converts between DAO data and DTOs
    """
    
    def __init__(self, events_dao: EventsDAO, economic_events_dao: EconomicEventsDAO):
        self.events_dao = events_dao
        self.economic_events_dao = economic_events_dao
        
        # Cache for performance optimization
        self._event_type_cache: Dict[int, EconomicEventTypeDTO] = {}
    
    def _dao_to_event_dto(self, dao_record) -> EventDTO:
        """Convert DAO record to EventDTO"""
        if not dao_record:
            return None
        
        return EventDTO(
            id=dao_record.get('id'),
            event_type=dao_record.get('event_type'),
            instrument_id=dao_record.get('instrument_id'),
            event_time=dao_record.get('event_time'),
            reported_time=dao_record.get('reported_time'),
            source=dao_record.get('source'),
            data=dao_record.get('data')
        )
    
    def _dao_to_economic_event_type_dto(self, dao_record) -> EconomicEventTypeDTO:
        """Convert DAO record to EconomicEventTypeDTO"""
        if not dao_record:
            return None
        
        return EconomicEventTypeDTO(
            id=dao_record.get('id'),
            name=dao_record.get('name'),
            description=dao_record.get('description'),
            category=dao_record.get('category'),
            country=dao_record.get('country'),
            importance_level=dao_record.get('importance_level'),
            frequency=dao_record.get('frequency'),
            typical_release_time=dao_record.get('typical_release_time'),
            created_at=dao_record.get('created_at'),
            updated_at=dao_record.get('updated_at')
        )
    
    def _dao_to_economic_event_dto(self, dao_record) -> EconomicEventDTO:
        """Convert DAO record to EconomicEventDTO"""
        if not dao_record:
            return None
        
        return EconomicEventDTO(
            id=dao_record.get('id'),
            event_type_id=dao_record.get('event_type_id'),
            date=dao_record.get('date'),
            release_time=dao_record.get('release_time'),
            estimate=dao_record.get('estimate'),
            actual=dao_record.get('actual'),
            previous=dao_record.get('previous'),
            revised=dao_record.get('revised'),
            unit=dao_record.get('unit'),
            currency=dao_record.get('currency'),
            source_vendor=dao_record.get('source_vendor'),
            source_event_id=dao_record.get('source_event_id'),
            is_preliminary=dao_record.get('is_preliminary', False),
            created_at=dao_record.get('created_at'),
            updated_at=dao_record.get('updated_at')
        )
    
    # Event Operations
    
    async def create_event(self, event: EventDTO) -> AnalyticsOperationResult:
        """Create a new event with business validation"""
        try:
            # Business validation
            if not event.event_type:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event type is required"
                )
            
            if not event.event_time:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event time is required"
                )
            
            # Create event
            dao_record = await self.events_dao.insert_event(
                event_type=event.event_type,
                instrument_id=event.instrument_id,
                event_time=event.event_time,
                reported_time=event.reported_time or datetime.utcnow(),
                source=event.source,
                data=event.data
            )
            
            logger.info(f"Created event {event.event_type} for instrument {event.instrument_id}")
            
            # Record business metric
            record_instrument_business_metric('analytics_events_created', 1, {
                'event_type': event.event_type,
                'source': event.source or 'unknown'
            })
            
            return AnalyticsOperationResult(
                success=True,
                event_id=dao_record.get('id'),
                created_count=1
            )
            
        except Exception as e:
            logger.error(f"Error creating event {event.event_type}: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def get_event_by_id(self, event_id: int) -> Optional[EventDTO]:
        """Retrieve event by ID"""
        try:
            # Note: EventsDAO doesn't have get_by_id method, need to implement
            # For now, use get_events with no filters and find by ID
            events = await self.events_dao.get_events()
            for event_record in events:
                if event_record.get('id') == event_id:
                    return self._dao_to_event_dto(event_record)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving event {event_id}: {e}")
            return None
    
    async def list_events(self, criteria: EventSearchCriteria) -> List[EventDTO]:
        """List events based on search criteria"""
        try:
            dao_records = await self.events_dao.get_events(
                instrument_id=criteria.instrument_id,
                event_type=criteria.event_type,
                start=criteria.start_date,
                end=criteria.end_date
            )
            
            results = []
            for record in dao_records:
                dto = self._dao_to_event_dto(record)
                if dto:
                    # Apply additional filtering if needed
                    if criteria.sources and dto.source not in criteria.sources:
                        continue
                    results.append(dto)
            
            # Apply limit and offset
            if criteria.offset:
                results = results[criteria.offset:]
            if criteria.limit:
                results = results[:criteria.limit]
            
            return results
            
        except Exception as e:
            logger.error(f"Error listing events: {e}")
            return []
    
    async def get_events_count(self, criteria: Optional[EventSearchCriteria] = None) -> int:
        """Get total number of events matching criteria"""
        try:
            if not criteria:
                criteria = EventSearchCriteria()
            
            events = await self.list_events(criteria)
            return len(events)
            
        except Exception as e:
            logger.error(f"Error getting events count: {e}")
            return 0
    
    async def create_events_batch(self, events: List[EventDTO]) -> AnalyticsOperationResult:
        """Create multiple events in batch"""
        try:
            if not events:
                return AnalyticsOperationResult(success=True, created_count=0)
            
            created_count = 0
            failed_count = 0
            
            for event in events:
                try:
                    result = await self.create_event(event)
                    if result.success:
                        created_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to create event in batch: {e}")
                    failed_count += 1
            
            logger.info(f"Batch created {created_count} events, {failed_count} failed")
            
            return AnalyticsOperationResult(
                success=True,
                created_count=created_count,
                skipped_count=failed_count
            )
            
        except Exception as e:
            logger.error(f"Error in batch events creation: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    # Economic Event Type Operations
    
    async def create_economic_event_type(self, event_type: EconomicEventTypeDTO) -> AnalyticsOperationResult:
        """Create a new economic event type"""
        try:
            # Business validation
            if not event_type.name:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event type name is required"
                )
            
            # Check if event type already exists
            existing_types = await self.economic_events_dao.get_event_types(name=event_type.name)
            if existing_types:
                return AnalyticsOperationResult(
                    success=False,
                    error_message=f"Economic event type '{event_type.name}' already exists"
                )
            
            # Create event type
            type_id = await self.economic_events_dao.create_event_type(
                name=event_type.name,
                description=event_type.description,
                category=event_type.category,
                country=event_type.country,
                importance_level=event_type.importance_level,
                frequency=event_type.frequency,
                typical_release_time=event_type.typical_release_time
            )
            
            logger.info(f"Created economic event type {event_type.name} with ID {type_id}")
            
            # Clear cache
            self._event_type_cache.clear()
            
            return AnalyticsOperationResult(
                success=True,
                event_id=type_id,
                created_count=1
            )
            
        except Exception as e:
            logger.error(f"Error creating economic event type {event_type.name}: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def get_economic_event_type_by_id(self, type_id: int) -> Optional[EconomicEventTypeDTO]:
        """Retrieve economic event type by ID"""
        try:
            # Check cache first
            if type_id in self._event_type_cache:
                return self._event_type_cache[type_id]
            
            event_type_record = await self.economic_events_dao.get_event_type_by_id(type_id)
            dto = self._dao_to_economic_event_type_dto(event_type_record)
            
            # Cache result
            if dto:
                self._event_type_cache[type_id] = dto
            
            return dto
            
        except Exception as e:
            logger.error(f"Error retrieving economic event type {type_id}: {e}")
            return None
    
    async def list_economic_event_types(self, 
                                      category: Optional[str] = None,
                                      country: Optional[str] = None) -> List[EconomicEventTypeDTO]:
        """List economic event types with optional filtering"""
        try:
            dao_records = await self.economic_events_dao.get_event_types(
                category=category,
                country=country
            )
            
            results = []
            for record in dao_records:
                dto = self._dao_to_economic_event_type_dto(record)
                if dto:
                    results.append(dto)
            
            return results
            
        except Exception as e:
            logger.error(f"Error listing economic event types: {e}")
            return []
    
    async def update_economic_event_type(self, event_type: EconomicEventTypeDTO) -> AnalyticsOperationResult:
        """Update economic event type"""
        try:
            if not event_type.id:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event type ID is required for update"
                )
            
            # Update event type
            updated = await self.economic_events_dao.update_event_type(
                type_id=event_type.id,
                name=event_type.name,
                description=event_type.description,
                category=event_type.category,
                country=event_type.country,
                importance_level=event_type.importance_level,
                frequency=event_type.frequency,
                typical_release_time=event_type.typical_release_time
            )
            
            if updated:
                logger.info(f"Updated economic event type {event_type.id}")
                
                # Clear cache
                if event_type.id in self._event_type_cache:
                    del self._event_type_cache[event_type.id]
                
                return AnalyticsOperationResult(
                    success=True,
                    updated_count=1
                )
            else:
                return AnalyticsOperationResult(
                    success=False,
                    error_message=f"Economic event type {event_type.id} not found"
                )
            
        except Exception as e:
            logger.error(f"Error updating economic event type {event_type.id}: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    # Economic Event Operations
    
    async def create_economic_event(self, event: EconomicEventDTO) -> AnalyticsOperationResult:
        """Create a new economic event"""
        try:
            # Business validation
            if not event.event_type_id:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event type ID is required"
                )
            
            if not event.date:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event date is required"
                )
            
            # Validate event type exists
            event_type = await self.get_economic_event_type_by_id(event.event_type_id)
            if not event_type:
                return AnalyticsOperationResult(
                    success=False,
                    error_message=f"Economic event type {event.event_type_id} not found"
                )
            
            # Create economic event
            event_id = await self.economic_events_dao.create_economic_event(
                event_type_id=event.event_type_id,
                date=event.date,
                release_time=event.release_time,
                estimate=event.estimate,
                actual=event.actual,
                previous=event.previous,
                revised=event.revised,
                unit=event.unit,
                currency=event.currency,
                source_vendor=event.source_vendor,
                source_event_id=event.source_event_id,
                is_preliminary=event.is_preliminary
            )
            
            logger.info(f"Created economic event for type {event.event_type_id} on {event.date}")
            
            # Record business metric
            record_instrument_business_metric('economic_events_created', 1, {
                'event_type': event_type.name,
                'country': event_type.country or 'unknown',
                'source_vendor': event.source_vendor or 'unknown'
            })
            
            return AnalyticsOperationResult(
                success=True,
                event_id=event_id,
                created_count=1
            )
            
        except Exception as e:
            logger.error(f"Error creating economic event: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def get_economic_event_by_id(self, event_id: int) -> Optional[EconomicEventDTO]:
        """Retrieve economic event by ID"""
        try:
            event_record = await self.economic_events_dao.get_economic_event_by_id(event_id)
            return self._dao_to_economic_event_dto(event_record)
            
        except Exception as e:
            logger.error(f"Error retrieving economic event {event_id}: {e}")
            return None
    
    async def list_economic_events(self, criteria: EconomicEventSearchCriteria) -> List[EconomicEventDTO]:
        """List economic events based on search criteria"""
        try:
            dao_records = await self.economic_events_dao.get_economic_events(
                event_type_ids=criteria.event_type_ids,
                countries=criteria.countries,
                categories=criteria.categories,
                importance_levels=criteria.importance_levels,
                start_date=criteria.start_date,
                end_date=criteria.end_date,
                limit=criteria.limit,
                offset=criteria.offset
            )
            
            results = []
            for record in dao_records:
                dto = self._dao_to_economic_event_dto(record)
                if dto:
                    results.append(dto)
            
            return results
            
        except Exception as e:
            logger.error(f"Error listing economic events: {e}")
            return []
    
    async def update_economic_event(self, event: EconomicEventDTO) -> AnalyticsOperationResult:
        """Update economic event (for revisions, actuals, etc.)"""
        try:
            if not event.id:
                return AnalyticsOperationResult(
                    success=False,
                    error_message="Event ID is required for update"
                )
            
            # Update economic event
            updated = await self.economic_events_dao.update_economic_event(
                event_id=event.id,
                release_time=event.release_time,
                estimate=event.estimate,
                actual=event.actual,
                previous=event.previous,
                revised=event.revised,
                unit=event.unit,
                currency=event.currency,
                is_preliminary=event.is_preliminary
            )
            
            if updated:
                logger.info(f"Updated economic event {event.id}")
                
                return AnalyticsOperationResult(
                    success=True,
                    updated_count=1
                )
            else:
                return AnalyticsOperationResult(
                    success=False,
                    error_message=f"Economic event {event.id} not found"
                )
            
        except Exception as e:
            logger.error(f"Error updating economic event {event.id}: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    async def create_economic_events_batch(self, events: List[EconomicEventDTO]) -> AnalyticsOperationResult:
        """Create multiple economic events in batch"""
        try:
            if not events:
                return AnalyticsOperationResult(success=True, created_count=0)
            
            created_count = 0
            failed_count = 0
            
            for event in events:
                try:
                    result = await self.create_economic_event(event)
                    if result.success:
                        created_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to create economic event in batch: {e}")
                    failed_count += 1
            
            logger.info(f"Batch created {created_count} economic events, {failed_count} failed")
            
            return AnalyticsOperationResult(
                success=True,
                created_count=created_count,
                skipped_count=failed_count
            )
            
        except Exception as e:
            logger.error(f"Error in batch economic events creation: {e}")
            return AnalyticsOperationResult(
                success=False,
                error_message=str(e)
            )
    
    # Analytics Operations
    
    async def get_economic_calendar(self, 
                                  start_date: date, 
                                  end_date: date,
                                  importance_level: Optional[int] = None,
                                  countries: Optional[List[str]] = None) -> List[EconomicEventDTO]:
        """Get economic calendar for date range"""
        try:
            criteria = EconomicEventSearchCriteria(
                start_date=start_date,
                end_date=end_date,
                importance_levels=[importance_level] if importance_level else None,
                countries=countries,
                limit=10000  # Large limit for calendar view
            )
            
            return await self.list_economic_events(criteria)
            
        except Exception as e:
            logger.error(f"Error getting economic calendar: {e}")
            return []
    
    async def get_events_by_instrument(self, 
                                     instrument_id: int,
                                     start_date: Optional[datetime] = None,
                                     end_date: Optional[datetime] = None) -> List[EventDTO]:
        """Get all events for a specific instrument"""
        try:
            criteria = EventSearchCriteria(
                instrument_id=instrument_id,
                start_date=start_date.isoformat() if start_date else None,
                end_date=end_date.isoformat() if end_date else None
            )
            
            return await self.list_events(criteria)
            
        except Exception as e:
            logger.error(f"Error getting events by instrument {instrument_id}: {e}")
            return []
    
    async def get_analytics_summary(self) -> Dict[str, Any]:
        """Get analytics data summary and statistics"""
        try:
            # Get counts of different data types
            total_events = await self.get_events_count()
            
            # Get economic event types count
            event_types = await self.list_economic_event_types()
            
            # Get recent economic events count
            recent_criteria = EconomicEventSearchCriteria(
                start_date=date.today(),
                limit=1000
            )
            recent_economic_events = await self.list_economic_events(recent_criteria)
            
            return {
                "total_events": total_events,
                "total_economic_event_types": len(event_types),
                "recent_economic_events": len(recent_economic_events),
                "event_types_by_category": self._group_by_category(event_types),
                "event_types_by_country": self._group_by_country(event_types),
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting analytics summary: {e}")
            return {"error": str(e)}
    
    def _group_by_category(self, event_types: List[EconomicEventTypeDTO]) -> Dict[str, int]:
        """Group event types by category"""
        categories = {}
        for event_type in event_types:
            category = event_type.category or 'unknown'
            categories[category] = categories.get(category, 0) + 1
        return categories
    
    def _group_by_country(self, event_types: List[EconomicEventTypeDTO]) -> Dict[str, int]:
        """Group event types by country"""
        countries = {}
        for event_type in event_types:
            country = event_type.country or 'unknown'
            countries[country] = countries.get(country, 0) + 1
        return countries
    
    # Data Quality Operations
    
    async def validate_event_data(self, event: EventDTO) -> Dict[str, Any]:
        """Validate event data quality and consistency"""
        validation_results = {
            "valid": True,
            "issues": [],
            "warnings": []
        }
        
        # Required field validation
        if not event.event_type:
            validation_results["valid"] = False
            validation_results["issues"].append("Missing event_type")
        
        if not event.event_time:
            validation_results["valid"] = False
            validation_results["issues"].append("Missing event_time")
        
        # Data quality checks
        if event.event_time and event.reported_time:
            if event.event_time > event.reported_time:
                validation_results["warnings"].append("Event time is after reported time")
        
        if event.data and not isinstance(event.data, dict):
            validation_results["issues"].append("Event data must be a dictionary")
            validation_results["valid"] = False
        
        return validation_results
    
    async def get_data_quality_report(self) -> Dict[str, Any]:
        """Get data quality report for analytics data"""
        try:
            # Sample recent events for quality analysis
            recent_criteria = EventSearchCriteria(limit=1000)
            recent_events = await self.list_events(recent_criteria)
            
            quality_stats = {
                "total_events_analyzed": len(recent_events),
                "events_with_data": 0,
                "events_with_instrument_id": 0,
                "events_with_source": 0,
                "unique_event_types": set(),
                "unique_sources": set(),
                "data_quality_score": 0.0
            }
            
            for event in recent_events:
                if event.data:
                    quality_stats["events_with_data"] += 1
                if event.instrument_id:
                    quality_stats["events_with_instrument_id"] += 1
                if event.source:
                    quality_stats["events_with_source"] += 1
                    quality_stats["unique_sources"].add(event.source)
                if event.event_type:
                    quality_stats["unique_event_types"].add(event.event_type)
            
            # Calculate data quality score
            if quality_stats["total_events_analyzed"] > 0:
                completeness_score = (
                    (quality_stats["events_with_data"] * 0.3) +
                    (quality_stats["events_with_instrument_id"] * 0.4) +
                    (quality_stats["events_with_source"] * 0.3)
                ) / quality_stats["total_events_analyzed"]
                quality_stats["data_quality_score"] = completeness_score
            
            # Convert sets to lists for JSON serialization
            quality_stats["unique_event_types"] = list(quality_stats["unique_event_types"])
            quality_stats["unique_sources"] = list(quality_stats["unique_sources"])
            
            return quality_stats
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {e}")
            return {"error": str(e)}