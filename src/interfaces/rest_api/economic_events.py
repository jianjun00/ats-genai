#!/usr/bin/env python3
"""
Economic Events API endpoints.
REST API for querying economic events data.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
from pydantic import BaseModel

from shared.utils.environment import Environment
from shared.utils.database import get_connection_pool
from domains.analytics.repositories.economic_events_dao import EconomicEventsDAO

router = APIRouter(prefix="/economic-events", tags=["Economic Events"])


class EconomicEventResponse(BaseModel):
    """Economic event response model."""
    id: int
    event_name: str
    event_date: date
    release_time: Optional[datetime] = None
    actual: Optional[float] = None
    estimate: Optional[float] = None
    previous: Optional[float] = None
    revised: Optional[float] = None
    unit: Optional[str] = None
    currency: Optional[str] = None
    country: Optional[str] = None
    importance_level: Optional[int] = None
    category: Optional[str] = None
    source_vendor: str
    is_preliminary: bool = False


class EconomicEventTypeResponse(BaseModel):
    """Economic event type response model."""
    id: int
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    country: Optional[str] = None
    importance_level: Optional[int] = None
    frequency: Optional[str] = None


class EconomicEventsStatsResponse(BaseModel):
    """Economic events statistics response."""
    total_events: int
    unique_event_types: int
    unique_vendors: int
    earliest_date: Optional[date] = None
    latest_date: Optional[date] = None
    events_with_actual: int
    events_with_estimate: int
    by_vendor: List[Dict[str, Any]]
    by_importance: List[Dict[str, Any]]


async def get_economic_events_dao() -> EconomicEventsDAO:
    """Dependency to get Economic Events DAO."""
    # In a real application, this would be injected via dependency injection
    # For now, we'll create it here
    from shared.utils.environment import EnvironmentType
    env = Environment(EnvironmentType.DEV)  # Adjust as needed
    pool = await get_connection_pool(env)
    return EconomicEventsDAO(pool, env)


@router.get("/", response_model=List[EconomicEventResponse])
async def get_economic_events(
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    vendor: Optional[str] = Query(None, description="Source vendor (polygon, tiingo, alpha_vantage, fred)"),
    min_importance: int = Query(1, ge=1, le=5, description="Minimum importance level (1-5)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of events to return"),
    dao: EconomicEventsDAO = Depends(get_economic_events_dao)
):
    """
    Get economic events within a date range.
    
    Returns economic events filtered by date range, vendor, and importance level.
    """
    # Set default date range if not provided
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    try:
        # Get events with type information
        events = await dao.get_economic_events_with_types(
            start_date=start_date,
            end_date=end_date,
            min_importance=min_importance
        )
        
        # Filter by vendor if specified
        if vendor:
            events = [e for e in events if e.get("source_vendor") == vendor]
        
        # Limit results
        events = events[:limit]
        
        # Convert to response model
        response_events = []
        for event in events:
            response_events.append(EconomicEventResponse(
                id=event["id"],
                event_name=event["event_name"],
                event_date=event["date"],
                release_time=event.get("release_time"),
                actual=float(event["actual"]) if event.get("actual") is not None else None,
                estimate=float(event["estimate"]) if event.get("estimate") is not None else None,
                previous=float(event["previous"]) if event.get("previous") is not None else None,
                revised=float(event["revised"]) if event.get("revised") is not None else None,
                unit=event.get("unit"),
                currency=event.get("currency"),
                country=event.get("country"),
                importance_level=event.get("importance_level"),
                category=event.get("category"),
                source_vendor=event["source_vendor"],
                is_preliminary=event.get("is_preliminary", False)
            ))
        
        return response_events
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching economic events: {str(e)}")


@router.get("/upcoming", response_model=List[EconomicEventResponse])
async def get_upcoming_events(
    days_ahead: int = Query(7, ge=1, le=30, description="Number of days ahead to look"),
    min_importance: int = Query(3, ge=1, le=5, description="Minimum importance level"),
    dao: EconomicEventsDAO = Depends(get_economic_events_dao)
):
    """
    Get upcoming high-impact economic events.
    
    Returns economic events scheduled for the next N days with high importance.
    """
    try:
        events = await dao.get_upcoming_events(days_ahead, min_importance)
        
        # Convert to response model
        response_events = []
        for event in events:
            response_events.append(EconomicEventResponse(
                id=event["id"],
                event_name=event["event_name"],
                event_date=event["date"],
                release_time=event.get("release_time"),
                actual=float(event["actual"]) if event.get("actual") is not None else None,
                estimate=float(event["estimate"]) if event.get("estimate") is not None else None,
                previous=float(event["previous"]) if event.get("previous") is not None else None,
                revised=float(event["revised"]) if event.get("revised") is not None else None,
                unit=event.get("unit"),
                currency=event.get("currency"),
                country=event.get("country"),
                importance_level=event.get("importance_level"),
                category=event.get("category"),
                source_vendor=event["source_vendor"],
                is_preliminary=event.get("is_preliminary", False)
            ))
        
        return response_events
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching upcoming events: {str(e)}")


@router.get("/types", response_model=List[EconomicEventTypeResponse])
async def get_event_types(
    country: Optional[str] = Query(None, description="Filter by country (3-letter code)"),
    min_importance: int = Query(1, ge=1, le=5, description="Minimum importance level"),
    dao: EconomicEventsDAO = Depends(get_economic_events_dao)
):
    """
    Get economic event types.
    
    Returns available economic event types, optionally filtered by country and importance.
    """
    try:
        if country:
            event_types = await dao.get_event_types_by_country(country)
        else:
            event_types = await dao.get_event_types_by_importance(min_importance)
        
        # Convert to response model
        response_types = []
        for event_type in event_types:
            if event_type.importance_level and event_type.importance_level >= min_importance:
                response_types.append(EconomicEventTypeResponse(
                    id=event_type.id,
                    name=event_type.name,
                    description=event_type.description,
                    category=event_type.category,
                    country=event_type.country,
                    importance_level=event_type.importance_level,
                    frequency=event_type.frequency
                ))
        
        return response_types
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching event types: {str(e)}")


@router.get("/stats", response_model=EconomicEventsStatsResponse)
async def get_economic_events_stats(
    dao: EconomicEventsDAO = Depends(get_economic_events_dao)
):
    """
    Get statistics about economic events data.
    
    Returns overall statistics including counts by vendor and importance level.
    """
    try:
        stats = await dao.get_event_statistics()
        
        return EconomicEventsStatsResponse(
            total_events=stats["overall"]["total_events"],
            unique_event_types=stats["overall"]["unique_event_types"],
            unique_vendors=stats["overall"]["unique_vendors"],
            earliest_date=stats["overall"]["earliest_date"],
            latest_date=stats["overall"]["latest_date"],
            events_with_actual=stats["overall"]["events_with_actual"],
            events_with_estimate=stats["overall"]["events_with_estimate"],
            by_vendor=stats["by_vendor"],
            by_importance=stats["by_importance"]
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching statistics: {str(e)}")


@router.get("/calendar")
async def get_economic_calendar(
    year: int = Query(..., ge=2020, le=2030, description="Year"),
    month: int = Query(..., ge=1, le=12, description="Month"),
    min_importance: int = Query(3, ge=1, le=5, description="Minimum importance level"),
    dao: EconomicEventsDAO = Depends(get_economic_events_dao)
):
    """
    Get economic calendar for a specific month.
    
    Returns events organized by date for calendar display.
    """
    try:
        # Calculate date range for the month
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
        
        # Get events for the month
        events = await dao.get_economic_events_with_types(
            start_date=start_date,
            end_date=end_date,
            min_importance=min_importance
        )
        
        # Group events by date
        calendar = {}
        for event in events:
            event_date_str = event["date"].strftime("%Y-%m-%d")
            if event_date_str not in calendar:
                calendar[event_date_str] = []
            
            calendar[event_date_str].append({
                "id": event["id"],
                "name": event["event_name"],
                "time": event.get("release_time").strftime("%H:%M") if event.get("release_time") else None,
                "importance": event.get("importance_level"),
                "category": event.get("category"),
                "country": event.get("country"),
                "actual": float(event["actual"]) if event.get("actual") is not None else None,
                "estimate": float(event["estimate"]) if event.get("estimate") is not None else None,
                "unit": event.get("unit"),
                "vendor": event["source_vendor"]
            })
        
        return {
            "year": year,
            "month": month,
            "total_events": len(events),
            "calendar": calendar
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching calendar: {str(e)}")


@router.get("/today", response_model=List[EconomicEventResponse])
async def get_todays_events(
    min_importance: int = Query(3, ge=1, le=5, description="Minimum importance level"),
    dao: EconomicEventsDAO = Depends(get_economic_events_dao)
):
    """
    Get today's economic events.
    
    Returns economic events scheduled for today.
    """
    today = date.today()
    
    try:
        events = await dao.get_economic_events_with_types(
            start_date=today,
            end_date=today,
            min_importance=min_importance
        )
        
        # Convert to response model
        response_events = []
        for event in events:
            response_events.append(EconomicEventResponse(
                id=event["id"],
                event_name=event["event_name"],
                event_date=event["date"],
                release_time=event.get("release_time"),
                actual=float(event["actual"]) if event.get("actual") is not None else None,
                estimate=float(event["estimate"]) if event.get("estimate") is not None else None,
                previous=float(event["previous"]) if event.get("previous") is not None else None,
                revised=float(event["revised"]) if event.get("revised") is not None else None,
                unit=event.get("unit"),
                currency=event.get("currency"),
                country=event.get("country"),
                importance_level=event.get("importance_level"),
                category=event.get("category"),
                source_vendor=event["source_vendor"],
                is_preliminary=event.get("is_preliminary", False)
            ))
        
        return response_events
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching today's events: {str(e)}")