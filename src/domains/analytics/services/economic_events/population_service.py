#!/usr/bin/env python3
"""
Economic Events Population Service.
Coordinates fetching and storing economic events from multiple vendors.
"""

import asyncpg
import logging
from datetime import date, datetime
from typing import List, Dict, Any, Optional

from src.core.platform.config.environment import Environment
from src.core.dao.analytics.economic_events_dao import EconomicEventsDAO, EconomicEvent, EconomicEventType, EconomicEventVendorData
from src.infrastructure.vendor.polygon.economic_events_client import PolygonEconomicEventsClient
from src.infrastructure.vendor.tiingo.economic_events_client import TiingoEconomicEventsClient
from src.infrastructure.vendor.alpha_vantage.economic_events_client import AlphaVantageEconomicClient
from src.infrastructure.vendor.eodhd.economic_events_client import EODHDEconomicEventsClient
from src.domains.analytics.services.economic_events.fred_client import FREDEconomicClient

logger = logging.getLogger(__name__)


class EconomicEventsPopulationService:
    """Service for populating economic events from multiple vendors."""

    def __init__(self, env: Environment, connection_pool: asyncpg.Pool):
        self.env = env
        self.pool = connection_pool
        self.dao = EconomicEventsDAO(connection_pool, env)

        # Initialize clients (will be set with API keys)
        self.polygon_client: Optional[PolygonEconomicEventsClient] = None
        self.tiingo_client: Optional[TiingoEconomicEventsClient] = None
        self.alpha_vantage_client: Optional[AlphaVantageEconomicClient] = None
        self.eodhd_client: Optional[EODHDEconomicEventsClient] = None
        self.fred_client: Optional[FREDEconomicClient] = None

    def initialize_clients(self, polygon_api_key: Optional[str] = None,
                          tiingo_api_key: Optional[str] = None,
                          alpha_vantage_api_key: Optional[str] = None,
                          eodhd_api_key: Optional[str] = None,
                          fred_api_key: Optional[str] = None):
        """
        Initialize API clients with provided keys.

        Args:
            polygon_api_key: Polygon API key
            tiingo_api_key: Tiingo API key
            alpha_vantage_api_key: Alpha Vantage API key
            eodhd_api_key: EODHD API key
            fred_api_key: FRED API key
        """
        if polygon_api_key:
            self.polygon_client = PolygonEconomicEventsClient(polygon_api_key)
            logger.info("Initialized Polygon Economic Events client")

        if tiingo_api_key:
            self.tiingo_client = TiingoEconomicEventsClient(tiingo_api_key)
            logger.info("Initialized Tiingo Economic Events client")

        if alpha_vantage_api_key:
            self.alpha_vantage_client = AlphaVantageEconomicClient(alpha_vantage_api_key)
            logger.info("Initialized Alpha Vantage Economic client")

        if eodhd_api_key:
            self.eodhd_client = EODHDEconomicEventsClient(eodhd_api_key)
            logger.info("Initialized EODHD Economic Events client")

        if fred_api_key:
            self.fred_client = FREDEconomicClient(fred_api_key)
            logger.info("Initialized FRED Economic client")

    async def populate_economic_events(self, start_date: date, end_date: date,
                                     vendors: Optional[List[str]] = None,
                                     min_importance: int = 1) -> Dict[str, Any]:
        """
        Populate economic events from specified vendors.

        Args:
            start_date: Start date for events
            end_date: End date for events
            vendors: List of vendors to fetch from (None = all available)
            min_importance: Minimum importance level to store

        Returns:
            Population results summary
        """
        logger.info(f"Starting economic events population for {start_date} to {end_date}")

        if vendors is None:
            vendors = ["polygon", "tiingo", "alpha_vantage", "eodhd", "fred"]

        results = {
            "start_date": start_date,
            "end_date": end_date,
            "vendors_requested": vendors,
            "vendors_processed": [],
            "total_events_processed": 0,
            "total_events_stored": 0,
            "vendor_results": {}
        }

        # Process each vendor
        for vendor in vendors:
            try:
                vendor_result = await self._populate_from_vendor(
                    vendor, start_date, end_date, min_importance
                )

                results["vendor_results"][vendor] = vendor_result
                results["vendors_processed"].append(vendor)
                results["total_events_processed"] += vendor_result.get("events_processed", 0)
                results["total_events_stored"] += vendor_result.get("events_stored", 0)

            except Exception as e:
                logger.error(f"Error populating from {vendor}: {e}")
                results["vendor_results"][vendor] = {
                    "error": str(e),
                    "events_processed": 0,
                    "events_stored": 0
                }

        logger.info(f"Economic events population completed. "
                   f"Processed {results['total_events_processed']} events, "
                   f"stored {results['total_events_stored']} events")

        return results

    async def _populate_from_vendor(self, vendor: str, start_date: date,
                                  end_date: date, min_importance: int) -> Dict[str, Any]:
        """
        Populate economic events from a specific vendor.

        Args:
            vendor: Vendor name
            start_date: Start date
            end_date: End date
            min_importance: Minimum importance level

        Returns:
            Vendor-specific results
        """
        logger.info(f"Populating economic events from {vendor}")

        if vendor == "polygon" and self.polygon_client:
            return await self._populate_from_polygon(start_date, end_date, min_importance)
        elif vendor == "tiingo" and self.tiingo_client:
            return await self._populate_from_tiingo(start_date, end_date, min_importance)
        elif vendor == "alpha_vantage" and self.alpha_vantage_client:
            return await self._populate_from_alpha_vantage(start_date, end_date, min_importance)
        elif vendor == "eodhd" and self.eodhd_client:
            return await self._populate_from_eodhd(start_date, end_date, min_importance)
        elif vendor == "fred" and self.fred_client:
            return await self._populate_from_fred(start_date, end_date, min_importance)
        else:
            logger.warning(f"Vendor {vendor} client not initialized or unsupported")
            return {"error": f"Vendor {vendor} not available", "events_processed": 0, "events_stored": 0}

    async def _populate_from_polygon(self, start_date: date, end_date: date,
                                   min_importance: int) -> Dict[str, Any]:
        """Populate events from Polygon."""
        try:
            # Fetch events from Polygon
            raw_events = await self.polygon_client.fetch_economic_events(
                start_date, end_date, min_importance
            )

            events_stored = 0

            for raw_event in raw_events:
                try:
                    # Parse event
                    parsed_event = self.polygon_client.parse_polygon_event(raw_event)

                    # Skip if importance too low
                    if parsed_event.get("importance", 0) < min_importance:
                        continue

                    # Store event
                    await self._store_economic_event(parsed_event, "polygon")
                    events_stored += 1

                except Exception as e:
                    logger.error(f"Error storing Polygon event: {e}")

            return {
                "events_processed": len(raw_events),
                "events_stored": events_stored,
                "source": "polygon"
            }

        except Exception as e:
            logger.error(f"Error populating from Polygon: {e}")
            return {"error": str(e), "events_processed": 0, "events_stored": 0}

    async def _populate_from_tiingo(self, start_date: date, end_date: date,
                                  min_importance: int) -> Dict[str, Any]:
        """Populate events from Tiingo."""
        try:
            # Fetch events from Tiingo
            raw_events = await self.tiingo_client.fetch_economic_events(start_date, end_date)

            events_stored = 0

            for raw_event in raw_events:
                try:
                    # Parse event
                    parsed_event = self.tiingo_client.parse_tiingo_event(raw_event)

                    # Skip if importance too low
                    if parsed_event.get("importance", 0) < min_importance:
                        continue

                    # Store event
                    await self._store_economic_event(parsed_event, "tiingo")
                    events_stored += 1

                except Exception as e:
                    logger.error(f"Error storing Tiingo event: {e}")

            return {
                "events_processed": len(raw_events),
                "events_stored": events_stored,
                "source": "tiingo"
            }

        except Exception as e:
            logger.error(f"Error populating from Tiingo: {e}")
            return {"error": str(e), "events_processed": 0, "events_stored": 0}

    async def _populate_from_alpha_vantage(self, start_date: date, end_date: date,
                                         min_importance: int) -> Dict[str, Any]:
        """Populate events from Alpha Vantage."""
        try:
            # Fetch events from Alpha Vantage
            raw_events = await self.alpha_vantage_client.fetch_all_indicators(start_date, end_date)

            events_stored = 0

            for parsed_event in raw_events:
                try:
                    # Skip if importance too low
                    if parsed_event.get("importance", 0) < min_importance:
                        continue

                    # Store event
                    await self._store_economic_event(parsed_event, "alpha_vantage")
                    events_stored += 1

                except Exception as e:
                    logger.error(f"Error storing Alpha Vantage event: {e}")

            return {
                "events_processed": len(raw_events),
                "events_stored": events_stored,
                "source": "alpha_vantage"
            }

        except Exception as e:
            logger.error(f"Error populating from Alpha Vantage: {e}")
            return {"error": str(e), "events_processed": 0, "events_stored": 0}

    async def _populate_from_eodhd(self, start_date: date, end_date: date,
                                 min_importance: int) -> Dict[str, Any]:
        """Populate events from EODHD."""
        try:
            # Fetch economic calendar events
            calendar_events = await self.eodhd_client.fetch_economic_events(start_date, end_date)

            # Fetch macro indicators (recent data)
            macro_events = await self.eodhd_client.fetch_macro_indicators()

            # Combine all events
            all_raw_events = []
            events_stored = 0

            # Process calendar events
            for raw_event in calendar_events:
                try:
                    parsed_event = self.eodhd_client.parse_eodhd_event(raw_event)

                    # Skip if importance too low
                    if parsed_event.get("importance", 0) < min_importance:
                        continue

                    all_raw_events.append(raw_event)

                    # Store event
                    await self._store_economic_event(parsed_event, "eodhd")
                    events_stored += 1

                except Exception as e:
                    logger.error(f"Error storing EODHD calendar event: {e}")

            # Process macro indicators
            for raw_indicator in macro_events:
                try:
                    parsed_event = self.eodhd_client.parse_macro_indicator(raw_indicator)

                    # Skip if importance too low
                    if parsed_event.get("importance", 0) < min_importance:
                        continue

                    # Only include recent macro data (within date range)
                    if parsed_event.get("event_date") and start_date <= parsed_event["event_date"] <= end_date:
                        all_raw_events.append(raw_indicator)

                        # Store event
                        await self._store_economic_event(parsed_event, "eodhd")
                        events_stored += 1

                except Exception as e:
                    logger.error(f"Error storing EODHD macro indicator: {e}")

            return {
                "events_processed": len(all_raw_events),
                "events_stored": events_stored,
                "source": "eodhd",
                "calendar_events": len(calendar_events),
                "macro_events": len([e for e in macro_events if start_date <= datetime.strptime(e.get("Date", "1900-01-01"), "%Y-%m-%d").date() <= end_date])
            }

        except Exception as e:
            logger.error(f"Error populating from EODHD: {e}")
            return {"error": str(e), "events_processed": 0, "events_stored": 0}

    async def _populate_from_fred(self, start_date: date, end_date: date,
                                min_importance: int) -> Dict[str, Any]:
        """Populate events from FRED."""
        try:
            # Fetch events from FRED
            raw_events = await self.fred_client.fetch_popular_indicators(start_date, end_date)

            events_stored = 0

            for parsed_event in raw_events:
                try:
                    # Skip if importance too low
                    if parsed_event.get("importance", 0) < min_importance:
                        continue

                    # Store event
                    await self._store_economic_event(parsed_event, "fred")
                    events_stored += 1

                except Exception as e:
                    logger.error(f"Error storing FRED event: {e}")

            return {
                "events_processed": len(raw_events),
                "events_stored": events_stored,
                "source": "fred"
            }

        except Exception as e:
            logger.error(f"Error populating from FRED: {e}")
            return {"error": str(e), "events_processed": 0, "events_stored": 0}

    async def _store_economic_event(self, event_data: Dict[str, Any], vendor: str) -> Optional[int]:
        """
        Store economic event in database.

        Args:
            event_data: Parsed event data
            vendor: Source vendor

        Returns:
            Event ID if stored successfully
        """
        try:
            # Get or create event type
            event_type = await self._get_or_create_event_type(event_data)
            if not event_type:
                logger.warning(f"Could not create event type for {event_data.get('event_name')}")
                return None

            # Create economic event
            economic_event = EconomicEvent(
                id=None,
                event_type_id=event_type.id,
                date=event_data.get("event_date"),
                release_time=event_data.get("release_time"),
                estimate=event_data.get("estimate"),
                actual=event_data.get("actual"),
                previous=event_data.get("previous"),
                revised=event_data.get("revised"),
                unit=event_data.get("unit"),
                currency=event_data.get("currency"),
                source_vendor=vendor,
                source_event_id=event_data.get("source_event_id"),
                is_preliminary=event_data.get("is_preliminary", False)
            )

            # Store main event
            event_id = await self.dao.create_economic_event(economic_event)

            # Store vendor-specific data
            if event_data.get("vendor_specific_data"):
                vendor_data = EconomicEventVendorData(
                    id=None,
                    economic_event_id=event_id,
                    vendor_event_id=event_data.get("source_event_id"),
                    vendor_specific_data=event_data.get("vendor_specific_data"),
                    raw_data=event_data.get("raw_data")
                )

                if vendor == "polygon":
                    await self.dao.create_polygon_event_data(vendor_data)
                elif vendor == "tiingo":
                    await self.dao.create_tiingo_event_data(vendor_data)
                elif vendor == "alpha_vantage":
                    await self.dao.create_alpha_vantage_event_data(vendor_data)
                elif vendor == "eodhd":
                    await self.dao.create_eodhd_event_data(vendor_data)
                elif vendor == "fred":
                    await self.dao.create_fred_event_data(vendor_data)

            return event_id

        except Exception as e:
            logger.error(f"Error storing economic event from {vendor}: {e}")
            return None

    async def _get_or_create_event_type(self, event_data: Dict[str, Any]) -> Optional[EconomicEventType]:
        """
        Get existing or create new event type.

        Args:
            event_data: Event data containing type information

        Returns:
            Event type record
        """
        event_name = event_data.get("event_name")
        if not event_name:
            return None

        # Try to get existing event type
        existing_type = await self.dao.get_event_type_by_name(event_name)
        if existing_type:
            return existing_type

        # Create new event type
        new_event_type = EconomicEventType(
            id=None,
            name=event_name,
            description=f"Economic indicator: {event_name}",
            category=self._categorize_event(event_data),
            country=event_data.get("country", "USA"),
            importance_level=event_data.get("importance", 3),
            frequency=self._determine_frequency(event_data),
            typical_release_time=None
        )

        try:
            event_type_id = await self.dao.create_event_type(new_event_type)
            new_event_type.id = event_type_id
            return new_event_type
        except Exception as e:
            logger.error(f"Error creating event type for {event_name}: {e}")
            return None

    def _categorize_event(self, event_data: Dict[str, Any]) -> str:
        """Categorize economic event based on name and data."""
        event_name = event_data.get("event_name", "").lower()

        if any(word in event_name for word in ["employment", "unemployment", "payroll", "jobless"]):
            return "Employment"
        elif any(word in event_name for word in ["inflation", "cpi", "consumer price", "ppi"]):
            return "Inflation"
        elif any(word in event_name for word in ["fed", "federal", "interest rate", "funds rate"]):
            return "Interest Rates"
        elif any(word in event_name for word in ["gdp", "growth", "production"]):
            return "Growth"
        elif any(word in event_name for word in ["retail", "sales", "consumer", "spending"]):
            return "Consumption"
        elif any(word in event_name for word in ["housing", "home", "mortgage"]):
            return "Housing"
        else:
            return "General Economic"

    def _determine_frequency(self, event_data: Dict[str, Any]) -> str:
        """Determine frequency of economic event."""
        vendor_data = event_data.get("vendor_specific_data", {})

        # Check vendor-specific frequency data
        if "frequency" in vendor_data:
            return vendor_data["frequency"]

        # Infer from event name
        event_name = event_data.get("event_name", "").lower()

        if any(word in event_name for word in ["weekly", "jobless claims"]):
            return "weekly"
        elif any(word in event_name for word in ["quarterly", "gdp"]):
            return "quarterly"
        elif any(word in event_name for word in ["annual", "yearly"]):
            return "annual"
        else:
            return "monthly"  # Default assumption

    async def get_population_statistics(self) -> Dict[str, Any]:
        """Get statistics about populated economic events."""
        return await self.dao.get_event_statistics()

    async def get_upcoming_high_impact_events(self, days_ahead: int = 7) -> List[Dict[str, Any]]:
        """Get upcoming high-impact economic events."""
        return await self.dao.get_upcoming_events(days_ahead, min_importance=4)