#!/usr/bin/env python3
"""
EODHD Economic Events API Client.
Fetches economic events and macro indicators from EODHD API.
"""

import asyncio
import aiohttp
import gin
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)

@gin.configurable
class EODHDEconomicConfig:
    def __init__(self,
                 base_url: str = "https://eodhd.com/api",
                 timeout_seconds: int = 30,
                 event_limit: int = 1000,
                 rate_limit_delay_seconds: int = 1):
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.event_limit = event_limit
        self.rate_limit_delay_seconds = rate_limit_delay_seconds

class EODHDEconomicEventsClient:
    """Client for EODHD Economic Events API."""

    def __init__(self, api_key: str, config: EODHDEconomicConfig = None):
        self.api_key = api_key
        self.config = config or EODHDEconomicConfig()

    async def fetch_economic_events(self, start_date: date, end_date: date,
                                  country: str = "US", importance: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch economic calendar events from EODHD API.

        Args:
            start_date: Start date for events
            end_date: End date for events
            country: Country code (US, GB, etc.)
            importance: Importance filter (low, medium, high)

        Returns:
            List of economic events data
        """
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.config.base_url}/economic-events"

            params = {
                "api_token": self.api_key,
                "from": start_date.strftime("%Y-%m-%d"),
                "to": end_date.strftime("%Y-%m-%d"),
                "country": country,
                "fmt": "json"
            }

            if importance:
                params["importance"] = importance

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        # EODHD returns the events directly as a list
                        events = data if isinstance(data, list) else []

                        logger.info(f"Fetched {len(events)} economic events from EODHD")
                        return events
                    elif response.status == 429:
                        logger.warning("EODHD API rate limit hit")
                        await asyncio.sleep(self.config.rate_limit_delay_seconds)
                        return []
                    else:
                        logger.error(f"EODHD API error: {response.status}")
                        return []

            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching EODHD events: {e}")
                return []

    async def fetch_macro_indicators(self, country: str = "US",
                                   indicators: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fetch macro economic indicators from EODHD.

        Args:
            country: Country code
            indicators: List of specific indicators to fetch

        Returns:
            List of macro indicator data
        """
        if indicators is None:
            indicators = [
                "gdp_growth_rate", "inflation_rate", "unemployment_rate",
                "interest_rate", "consumer_price_index", "producer_price_index"
            ]

        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        all_data = []

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for indicator in indicators:
                try:
                    url = f"{self.config.base_url}/macro-indicator"

                    params = {
                        "api_token": self.api_key,
                        "country": country,
                        "indicator": indicator,
                        "fmt": "json"
                    }

                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()

                            if isinstance(data, list) and data:
                                # Add indicator name to each data point
                                for item in data:
                                    item['indicator_name'] = indicator
                                    item['country'] = country
                                all_data.extend(data)

                            logger.info(f"Fetched {len(data) if isinstance(data, list) else 1} data points for {indicator}")
                        else:
                            logger.warning(f"EODHD macro indicator {indicator} error: {response.status}")

                        # Rate limiting between requests
                        await asyncio.sleep(self.config.rate_limit_delay_seconds)

                except aiohttp.ClientError as e:
                    logger.error(f"Connection error fetching {indicator}: {e}")

        logger.info(f"Fetched {len(all_data)} total macro indicator data points from EODHD")
        return all_data

    def parse_eodhd_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse EODHD event data into standardized format.

        Args:
            event_data: Raw EODHD event data

        Returns:
            Parsed event data
        """
        try:
            # Parse event date
            event_date = None
            if event_data.get("date"):
                try:
                    event_date = datetime.strptime(event_data["date"], "%Y-%m-%d").date()
                except ValueError:
                    # Try alternative format
                    try:
                        event_date = datetime.fromisoformat(event_data["date"]).date()
                    except ValueError:
                        logger.warning(f"Could not parse event date: {event_data.get('date')}")

            # Parse time if available
            release_time = None
            if event_data.get("time") and event_date:
                try:
                    time_str = event_data["time"]
                    # Handle various time formats
                    if ":" in time_str:
                        hour, minute = map(int, time_str.split(":"))
                        release_time = datetime.combine(event_date, datetime.min.time().replace(hour=hour, minute=minute))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse event time: {event_data.get('time')}")

            # Extract numeric values
            actual = None
            estimate = None
            previous = None

            for field, value in [("actual", event_data.get("actual")),
                               ("forecast", event_data.get("forecast")),
                               ("previous", event_data.get("previous"))]:
                if value is not None and value != "" and value != "N/A":
                    try:
                        # Clean the value
                        if isinstance(value, str):
                            # Remove common symbols and convert to float
                            cleaned = value.replace("%", "").replace(",", "").replace("$", "").replace("K", "000").replace("M", "000000").strip()
                            if cleaned and cleaned != "-":
                                numeric_value = Decimal(str(float(cleaned)))
                                if field == "actual":
                                    actual = numeric_value
                                elif field == "forecast":
                                    estimate = numeric_value
                                elif field == "previous":
                                    previous = numeric_value
                        else:
                            numeric_value = Decimal(str(value))
                            if field == "actual":
                                actual = numeric_value
                            elif field == "forecast":
                                estimate = numeric_value
                            elif field == "previous":
                                previous = numeric_value
                    except (ValueError, TypeError, Exception):
                        logger.debug(f"Could not parse {field} value: {value}")

            # Determine importance level
            importance = self._map_importance(event_data.get("importance", ""))

            # Determine units
            unit = self._extract_unit(event_data)

            return {
                "event_name": event_data.get("event", event_data.get("type", "")).strip(),
                "event_date": event_date,
                "release_time": release_time,
                "actual": actual,
                "estimate": estimate,
                "previous": previous,
                "unit": unit,
                "currency": event_data.get("currency", "USD"),
                "country": event_data.get("country", "US"),
                "importance": importance,
                "source_vendor": "eodhd",
                "source_event_id": event_data.get("id") or f"eodhd_{event_date}_{event_data.get('event', '').replace(' ', '_')}",
                "vendor_specific_data": {
                    "importance_text": event_data.get("importance"),
                    "period": event_data.get("period"),
                    "reference": event_data.get("reference"),
                    "source": event_data.get("source")
                },
                "raw_data": event_data
            }

        except Exception as e:
            logger.error(f"Error parsing EODHD event data: {e}")
            return {
                "event_name": "Unknown Economic Event",
                "source_vendor": "eodhd",
                "raw_data": event_data,
                "parse_error": str(e)
            }

    def parse_macro_indicator(self, indicator_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse EODHD macro indicator data into economic event format.

        Args:
            indicator_data: Raw EODHD macro indicator data

        Returns:
            Parsed event data
        """
        try:
            # Parse date
            event_date = None
            if indicator_data.get("Date"):
                try:
                    event_date = datetime.strptime(indicator_data["Date"], "%Y-%m-%d").date()
                except ValueError:
                    try:
                        event_date = datetime.fromisoformat(indicator_data["Date"]).date()
                    except ValueError:
                        logger.warning(f"Could not parse indicator date: {indicator_data.get('Date')}")

            # Extract value
            actual = None
            if indicator_data.get("Value") is not None:
                try:
                    actual = Decimal(str(indicator_data["Value"]))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse indicator value: {indicator_data.get('Value')}")

            # Format event name
            indicator_name = indicator_data.get("indicator_name", "Unknown Indicator")
            formatted_name = indicator_name.replace("_", " ").title()

            return {
                "event_name": formatted_name,
                "event_date": event_date,
                "release_time": None,
                "actual": actual,
                "estimate": None,
                "previous": None,
                "unit": self._get_indicator_unit(indicator_name),
                "currency": "USD" if indicator_data.get("country") == "US" else None,
                "country": indicator_data.get("country", "US"),
                "importance": self._get_indicator_importance(indicator_name),
                "source_vendor": "eodhd",
                "source_event_id": f"eodhd_macro_{indicator_name}_{event_date}",
                "vendor_specific_data": {
                    "indicator_type": "macro_indicator",
                    "data_source": "eodhd_macro"
                },
                "raw_data": indicator_data
            }

        except Exception as e:
            logger.error(f"Error parsing EODHD macro indicator: {e}")
            return {
                "event_name": "Unknown Macro Indicator",
                "source_vendor": "eodhd",
                "raw_data": indicator_data,
                "parse_error": str(e)
            }

    def _map_importance(self, importance_text: str) -> int:
        """Map EODHD importance text to numeric level."""
        if not importance_text:
            return 2

        importance_lower = importance_text.lower()
        if "high" in importance_lower:
            return 5
        elif "medium" in importance_lower:
            return 3
        elif "low" in importance_lower:
            return 1
        else:
            return 2

    def _extract_unit(self, event_data: Dict[str, Any]) -> Optional[str]:
        """Extract unit from event data."""
        # Check if unit is explicitly provided
        if event_data.get("unit"):
            return event_data["unit"]

        # Infer from event name or values
        event_name = event_data.get("event", "").lower()

        if any(word in event_name for word in ["rate", "percentage", "%"]):
            return "percentage"
        elif any(word in event_name for word in ["index", "pmi", "confidence"]):
            return "index"
        elif any(word in event_name for word in ["jobs", "employment", "payrolls"]):
            return "thousands"
        elif any(word in event_name for word in ["sales", "spending", "gdp"]):
            return "billions"

        return None

    def _get_indicator_unit(self, indicator_name: str) -> Optional[str]:
        """Get unit for macro indicator."""
        if "rate" in indicator_name.lower():
            return "percentage"
        elif "index" in indicator_name.lower():
            return "index"
        elif "gdp" in indicator_name.lower():
            return "percentage"
        else:
            return None

    def _get_indicator_importance(self, indicator_name: str) -> int:
        """Get importance level for macro indicator."""
        high_importance = ["gdp_growth_rate", "unemployment_rate", "inflation_rate", "interest_rate"]
        medium_importance = ["consumer_price_index", "producer_price_index"]

        if indicator_name in high_importance:
            return 5
        elif indicator_name in medium_importance:
            return 4
        else:
            return 3

    async def get_available_countries(self) -> List[str]:
        """
        Get list of available countries for economic events.

        Returns:
            List of country codes
        """
        # EODHD supports many countries, here are the major ones
        return [
            "US", "GB", "EU", "DE", "FR", "IT", "ES", "JP", "CN", "IN",
            "CA", "AU", "NZ", "CH", "SE", "NO", "DK", "BR", "MX", "RU"
        ]

    async def get_available_indicators(self) -> List[str]:
        """
        Get list of available macro indicators.

        Returns:
            List of indicator names
        """
        return [
            "gdp_growth_rate", "inflation_rate", "unemployment_rate", "interest_rate",
            "consumer_price_index", "producer_price_index", "retail_sales",
            "industrial_production", "consumer_confidence", "business_confidence",
            "balance_of_trade", "government_debt", "current_account"
        ]