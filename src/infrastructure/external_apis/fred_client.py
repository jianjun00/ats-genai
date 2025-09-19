#!/usr/bin/env python3
"""
FRED (Federal Reserve Economic Data) API Client.
Fetches economic data from St. Louis Federal Reserve FRED API.
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
class FREDEconomicConfig:
    def __init__(self,
                 base_url: str = "https://api.stlouisfed.org/fred",
                 timeout_seconds: int = 30,
                 search_limit_default: int = 20,
                 observations_limit: int = 100000):
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.search_limit_default = search_limit_default
        self.observations_limit = observations_limit

class FREDEconomicClient:
    """Client for FRED Economic Data API."""

    def __init__(self, api_key: str, config: FREDEconomicConfig = None):
        self.api_key = api_key
        self.config = config or FREDEconomicConfig()

    # Popular FRED series IDs for economic indicators
    SERIES_MAP = {
        "GDP": "GDP",  # Gross Domestic Product
        "GDPC1": "Real GDP",  # Real Gross Domestic Product
        "UNRATE": "Unemployment Rate",
        "PAYEMS": "Nonfarm Payrolls",
        "CPIAUCSL": "Consumer Price Index",
        "CPILFESL": "Core CPI",
        "FEDFUNDS": "Federal Funds Rate",
        "DGS10": "10-Year Treasury Rate",
        "DGS2": "2-Year Treasury Rate",
        "RSAFS": "Retail Sales",
        "INDPRO": "Industrial Production",
        "HOUST": "Housing Starts",
        "UMCSENT": "Consumer Sentiment",
        "DEXUSEU": "USD/EUR Exchange Rate",
        "DEXJPUS": "JPY/USD Exchange Rate",
        "VIXCLS": "VIX Volatility Index",
        "TB3MS": "3-Month Treasury Rate",
        "MORTGAGE30US": "30-Year Fixed Mortgage Rate",
        "ICSA": "Initial Jobless Claims",
        "CCSA": "Continued Jobless Claims"
    }

    async def fetch_series_data(self, series_id: str, start_date: date,
                              end_date: date) -> List[Dict[str, Any]]:
        """
        Fetch data for a specific FRED series.

        Args:
            series_id: FRED series ID
            start_date: Start date
            end_date: End date

        Returns:
            List of observations
        """
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            # First get series info
            series_info = await self._fetch_series_info(session, series_id)

            # Then get observations
            url = f"{self.config.base_url}/series/observations"
            params = {
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": start_date.strftime("%Y-%m-%d"),
                "observation_end": end_date.strftime("%Y-%m-%d"),
                "limit": self.config.observations_limit
            }

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        observations = data.get("observations", [])

                        # Parse observations into economic events format
                        events = []
                        for obs in observations:
                            event = self.parse_fred_observation(obs, series_info)
                            if event:
                                events.append(event)

                        logger.info(f"Fetched {len(events)} observations for FRED series {series_id}")
                        return events
                    else:
                        logger.error(f"FRED API error for series {series_id}: {response.status}")
                        return []

            # Let all connection errors propagate - fail fast on FRED API issues

    async def _fetch_series_info(self, session: aiohttp.ClientSession,
                               series_id: str) -> Dict[str, Any]:
        """
        Fetch metadata for a FRED series.

        Args:
            session: HTTP session
            series_id: FRED series ID

        Returns:
            Series metadata
        """
        url = f"{self.config.base_url}/series"
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json"
        }

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    series_list = data.get("seriess", [])
                    return series_list[0] if series_list else {}
                else:
                    logger.warning(f"Could not fetch FRED series info for {series_id}")
                    return {}

        # Let all connection errors propagate - fail fast on FRED API issues

    def parse_fred_observation(self, observation: Dict[str, Any],
                             series_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse FRED observation into economic event format.

        Args:
            observation: FRED observation data
            series_info: Series metadata

        Returns:
            Parsed economic event or None
        """
        try:
            # Parse observation date
            obs_date = datetime.strptime(observation["date"], "%Y-%m-%d").date()

            # Parse value
            value_str = observation.get("value", "")
            if value_str == "." or value_str == "":
                return None  # Missing data

            try:
                actual_value = Decimal(value_str)
            except (ValueError, TypeError):
                return None

            # Get series information
            series_id = observation.get("series_id") or series_info.get("id")
            series_title = series_info.get("title", series_id)
            units = series_info.get("units", "")
            frequency = series_info.get("frequency", "")
            seasonal_adjustment = series_info.get("seasonal_adjustment", "")

            # Determine event name
            event_name = self.SERIES_MAP.get(series_id, series_title)

            return {
                "event_name": event_name,
                "event_date": obs_date,
                "release_time": None,
                "actual": actual_value,
                "estimate": None,
                "previous": None,
                "unit": self._standardize_units(units),
                "currency": "USD" if "dollar" in units.lower() or "$" in units else None,
                "country": "USA",
                "importance": self._get_importance_for_series(series_id),
                "source_vendor": "fred",
                "source_event_id": f"{series_id}_{obs_date}",
                "vendor_specific_data": {
                    "observation_date": obs_date,
                    "series_title": series_title,
                    "series_units": units,
                    "seasonal_adjustment": seasonal_adjustment,
                    "frequency": frequency
                },
                "raw_data": {
                    "observation": observation,
                    "series_info": series_info
                }
            }

        # Let all parsing exceptions propagate - fail fast on data format issues

    def _standardize_units(self, fred_units: str) -> str:
        """
        Standardize FRED units to common format.

        Args:
            fred_units: FRED units string

        Returns:
            Standardized unit
        """
        units_lower = fred_units.lower()

        if "percent" in units_lower or "%" in units_lower:
            return "percentage"
        elif "billion" in units_lower:
            return "billions_usd"
        elif "million" in units_lower:
            return "millions_usd"
        elif "thousand" in units_lower:
            return "thousands"
        elif "index" in units_lower:
            return "index"
        elif "rate" in units_lower:
            return "percentage"
        elif "dollar" in units_lower or "$" in units_lower:
            return "usd"
        else:
            return fred_units

    def _get_importance_for_series(self, series_id: str) -> int:
        """
        Get importance level for FRED series.

        Args:
            series_id: FRED series ID

        Returns:
            Importance level (1-5)
        """
        high_importance = [
            "GDP", "GDPC1", "UNRATE", "PAYEMS", "CPIAUCSL",
            "FEDFUNDS", "DGS10"
        ]

        medium_importance = [
            "RSAFS", "INDPRO", "UMCSENT", "DGS2", "TB3MS",
            "MORTGAGE30US", "CPILFESL"
        ]

        if series_id in high_importance:
            return 5
        elif series_id in medium_importance:
            return 4
        else:
            return 3

    async def fetch_popular_indicators(self, start_date: date,
                                     end_date: date) -> List[Dict[str, Any]]:
        """
        Fetch popular economic indicators from FRED.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of economic events
        """
        logger.info("Fetching popular economic indicators from FRED...")

        all_events = []

        # Fetch data for each popular series
        for series_id, series_name in self.SERIES_MAP.items():
            try:
                series_data = await self.fetch_series_data(series_id, start_date, end_date)
                all_events.extend(series_data)

                # Rate limiting - FRED allows 120 requests per 60 seconds
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Error fetching FRED series {series_id}: {e}")

        logger.info(f"Fetched total of {len(all_events)} economic events from FRED")
        return all_events

    async def search_series(self, search_text: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search for FRED series by text.

        Args:
            search_text: Search query
            limit: Maximum results

        Returns:
            List of matching series
        """
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.config.base_url}/series/search"
            params = {
                "search_text": search_text,
                "api_key": self.api_key,
                "file_type": "json",
                "limit": limit
            }

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("seriess", [])
                    else:
                        logger.error(f"FRED search API error: {response.status}")
                        return []

            except aiohttp.ClientError as e:
                logger.error(f"Connection error searching FRED: {e}")
                return []

    async def get_series_categories(self) -> List[Dict[str, Any]]:
        """
        Get FRED categories for organizing series.

        Returns:
            List of categories
        """
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.config.base_url}/categories"
            params = {
                "api_key": self.api_key,
                "file_type": "json"
            }

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("categories", [])
                    else:
                        logger.error(f"FRED categories API error: {response.status}")
                        return []

            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching FRED categories: {e}")
                return []