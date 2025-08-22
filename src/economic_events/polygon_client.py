#!/usr/bin/env python3
"""
Polygon Economic Events API Client.
Fetches economic events data from Polygon.io API.
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class PolygonEconomicEventsClient:
    """Client for Polygon Economic Events API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
    
    async def fetch_economic_events(self, start_date: date, end_date: date,
                                  importance: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch economic events from Polygon API.
        
        Args:
            start_date: Start date for events
            end_date: End date for events  
            importance: Filter by importance level (1-5)
            
        Returns:
            List of economic events data
        """
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.base_url}/v1/economic/events"
            
            params = {
                "apikey": self.api_key,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "limit": 1000  # Maximum allowed by Polygon
            }
            
            if importance:
                params["importance"] = importance
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        events = data.get("results", [])
                        
                        logger.info(f"Fetched {len(events)} economic events from Polygon")
                        return events
                    elif response.status == 429:
                        logger.warning("Polygon API rate limit hit")
                        await asyncio.sleep(12)  # Polygon rate limit handling
                        return []
                    else:
                        logger.error(f"Polygon API error: {response.status}")
                        return []
                        
            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching Polygon events: {e}")
                return []
    
    async def fetch_specific_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a specific economic event by ID.
        
        Args:
            event_id: Polygon event ID
            
        Returns:
            Event data or None if not found
        """
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.base_url}/v1/economic/events/{event_id}"
            
            params = {"apikey": self.api_key}
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("results")
                    elif response.status == 404:
                        logger.warning(f"Polygon event {event_id} not found")
                        return None
                    else:
                        logger.error(f"Polygon API error for event {event_id}: {response.status}")
                        return None
                        
            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching Polygon event {event_id}: {e}")
                return None
    
    def parse_polygon_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Polygon event data into standardized format.
        
        Args:
            event_data: Raw Polygon event data
            
        Returns:
            Parsed event data
        """
        try:
            # Parse Polygon-specific fields
            event_date = None
            if event_data.get("date"):
                event_date = datetime.fromisoformat(event_data["date"]).date()
            
            release_time = None
            if event_data.get("release_time"):
                release_time = datetime.fromisoformat(event_data["release_time"])
            
            # Extract numeric values
            actual = None
            estimate = None
            previous = None
            
            if "actual" in event_data:
                try:
                    actual = Decimal(str(event_data["actual"]))
                except (ValueError, TypeError):
                    actual = None
            
            if "estimate" in event_data:
                try:
                    estimate = Decimal(str(event_data["estimate"]))
                except (ValueError, TypeError):
                    estimate = None
            
            if "previous" in event_data:
                try:
                    previous = Decimal(str(event_data["previous"]))
                except (ValueError, TypeError):
                    previous = None
            
            # Parse change percentages
            actual_change_percent = None
            estimated_change_percent = None
            previous_change_percent = None
            
            if "actual_change_percent" in event_data:
                try:
                    actual_change_percent = Decimal(str(event_data["actual_change_percent"]))
                except (ValueError, TypeError):
                    pass
            
            if "estimated_change_percent" in event_data:
                try:
                    estimated_change_percent = Decimal(str(event_data["estimated_change_percent"]))
                except (ValueError, TypeError):
                    pass
            
            if "previous_change_percent" in event_data:
                try:
                    previous_change_percent = Decimal(str(event_data["previous_change_percent"]))
                except (ValueError, TypeError):
                    pass
            
            return {
                "event_name": event_data.get("name", "").strip(),
                "event_date": event_date,
                "release_time": release_time,
                "actual": actual,
                "estimate": estimate,
                "previous": previous,
                "unit": event_data.get("unit"),
                "currency": event_data.get("currency"),
                "country": event_data.get("country"),
                "importance": event_data.get("importance"),
                "source_vendor": "polygon",
                "source_event_id": event_data.get("id"),
                "vendor_specific_data": {
                    "name": event_data.get("name"),
                    "country": event_data.get("country"),
                    "importance": event_data.get("importance"),
                    "actual_change_percent": actual_change_percent,
                    "estimated_change_percent": estimated_change_percent,
                    "previous_change_percent": previous_change_percent
                },
                "raw_data": event_data
            }
            
        except Exception as e:
            logger.error(f"Error parsing Polygon event data: {e}")
            return {
                "event_name": "Unknown Event",
                "source_vendor": "polygon",
                "raw_data": event_data,
                "parse_error": str(e)
            }
    
    async def get_event_types(self) -> List[Dict[str, Any]]:
        """
        Get available event types from Polygon.
        
        Returns:
            List of event types
        """
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.base_url}/v1/economic/event-types"
            
            params = {"apikey": self.api_key}
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("results", [])
                    else:
                        logger.error(f"Polygon event types API error: {response.status}")
                        return []
                        
            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching Polygon event types: {e}")
                return []