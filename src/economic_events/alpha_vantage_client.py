#!/usr/bin/env python3
"""
Alpha Vantage Economic Indicators API Client.
Fetches economic indicator data from Alpha Vantage API.
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class AlphaVantageEconomicClient:
    """Client for Alpha Vantage Economic Indicators API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
    
    async def fetch_real_gdp(self, interval: str = "quarterly") -> List[Dict[str, Any]]:
        """
        Fetch Real GDP data.
        
        Args:
            interval: quarterly or annual
            
        Returns:
            List of GDP data points
        """
        params = {
            "function": "REAL_GDP",
            "interval": interval,
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Real GDP")
    
    async def fetch_real_gdp_per_capita(self) -> List[Dict[str, Any]]:
        """Fetch Real GDP per capita data."""
        params = {
            "function": "REAL_GDP_PER_CAPITA",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Real GDP per capita")
    
    async def fetch_federal_funds_rate(self, interval: str = "monthly") -> List[Dict[str, Any]]:
        """
        Fetch Federal Funds Rate data.
        
        Args:
            interval: daily, weekly, or monthly
            
        Returns:
            List of federal funds rate data
        """
        params = {
            "function": "FEDERAL_FUNDS_RATE",
            "interval": interval,
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Federal Funds Rate")
    
    async def fetch_cpi(self, interval: str = "monthly") -> List[Dict[str, Any]]:
        """
        Fetch Consumer Price Index data.
        
        Args:
            interval: monthly or semiannual
            
        Returns:
            List of CPI data points
        """
        params = {
            "function": "CPI",
            "interval": interval,
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Consumer Price Index")
    
    async def fetch_inflation(self) -> List[Dict[str, Any]]:
        """Fetch inflation rate data."""
        params = {
            "function": "INFLATION",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Inflation Rate")
    
    async def fetch_retail_sales(self) -> List[Dict[str, Any]]:
        """Fetch retail sales data."""
        params = {
            "function": "RETAIL_SALES",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Retail Sales")
    
    async def fetch_unemployment_rate(self) -> List[Dict[str, Any]]:
        """Fetch unemployment rate data."""
        params = {
            "function": "UNEMPLOYMENT",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Unemployment Rate")
    
    async def fetch_nonfarm_payroll(self) -> List[Dict[str, Any]]:
        """Fetch nonfarm payroll data."""
        params = {
            "function": "NONFARM_PAYROLL",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Nonfarm Payroll")
    
    async def fetch_consumer_sentiment(self) -> List[Dict[str, Any]]:
        """Fetch consumer sentiment data."""
        params = {
            "function": "CONSUMER_SENTIMENT",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Consumer Sentiment")
    
    async def fetch_durable_goods_orders(self) -> List[Dict[str, Any]]:
        """Fetch durable goods orders data."""
        params = {
            "function": "DURABLE",
            "apikey": self.api_key
        }
        
        return await self._fetch_economic_data(params, "Durable Goods Orders")
    
    async def _fetch_economic_data(self, params: Dict[str, str], 
                                 indicator_name: str) -> List[Dict[str, Any]]:
        """
        Generic method to fetch economic data from Alpha Vantage.
        
        Args:
            params: API parameters
            indicator_name: Name of the economic indicator
            
        Returns:
            List of economic data points
        """
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Check for API error messages
                        if "Error Message" in data:
                            logger.error(f"Alpha Vantage API error for {indicator_name}: {data['Error Message']}")
                            return []
                        
                        if "Note" in data:
                            logger.warning(f"Alpha Vantage API note for {indicator_name}: {data['Note']}")
                            return []
                        
                        # Parse the data
                        parsed_data = self._parse_alpha_vantage_response(data, indicator_name)
                        logger.info(f"Fetched {len(parsed_data)} {indicator_name} data points from Alpha Vantage")
                        return parsed_data
                    else:
                        logger.error(f"Alpha Vantage API error for {indicator_name}: {response.status}")
                        return []
                        
            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching {indicator_name} from Alpha Vantage: {e}")
                return []
    
    def _parse_alpha_vantage_response(self, data: Dict[str, Any], 
                                    indicator_name: str) -> List[Dict[str, Any]]:
        """
        Parse Alpha Vantage API response into standardized format.
        
        Args:
            data: Raw API response
            indicator_name: Name of the indicator
            
        Returns:
            List of parsed data points
        """
        try:
            parsed_events = []
            
            # Alpha Vantage response structure varies by endpoint
            # Common patterns: "data", "Time Series", or direct data key
            time_series_data = None
            
            # Find the time series data in the response
            for key, value in data.items():
                if isinstance(value, dict) and any(date_key for date_key in value.keys() 
                                                 if self._is_date_string(date_key)):
                    time_series_data = value
                    break
            
            if not time_series_data:
                logger.warning(f"No time series data found in Alpha Vantage response for {indicator_name}")
                return []
            
            # Parse each data point
            for date_str, values in time_series_data.items():
                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    # Extract the main value (usually 'value' or similar key)
                    actual_value = None
                    for value_key in ["value", "price", "rate", "index"]:
                        if value_key in values:
                            try:
                                actual_value = Decimal(str(values[value_key]))
                                break
                            except (ValueError, TypeError):
                                continue
                    
                    # If no standard key found, try the first numeric value
                    if actual_value is None:
                        for key, val in values.items():
                            try:
                                actual_value = Decimal(str(val))
                                break
                            except (ValueError, TypeError):
                                continue
                    
                    parsed_event = {
                        "event_name": indicator_name,
                        "event_date": event_date,
                        "release_time": None,
                        "actual": actual_value,
                        "estimate": None,
                        "previous": None,
                        "unit": self._get_unit_for_indicator(indicator_name),
                        "currency": "USD",
                        "country": "USA",
                        "importance": self._get_importance_for_indicator(indicator_name),
                        "source_vendor": "alpha_vantage",
                        "source_event_id": f"{indicator_name}_{date_str}",
                        "vendor_specific_data": {
                            "function_name": indicator_name,
                            "interval_period": "monthly"  # Default assumption
                        },
                        "raw_data": {
                            "date": date_str,
                            "values": values,
                            "metadata": {k: v for k, v in data.items() if not isinstance(v, dict)}
                        }
                    }
                    
                    parsed_events.append(parsed_event)
                    
                except ValueError as e:
                    logger.warning(f"Error parsing date {date_str} for {indicator_name}: {e}")
                    continue
            
            return parsed_events
            
        except Exception as e:
            logger.error(f"Error parsing Alpha Vantage response for {indicator_name}: {e}")
            return []
    
    def _is_date_string(self, s: str) -> bool:
        """Check if a string looks like a date."""
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    def _get_unit_for_indicator(self, indicator_name: str) -> str:
        """Get the unit for a specific economic indicator."""
        unit_mapping = {
            "Real GDP": "billions_usd",
            "Real GDP per capita": "usd",
            "Federal Funds Rate": "percentage",
            "Consumer Price Index": "index",
            "Inflation Rate": "percentage",
            "Retail Sales": "millions_usd",
            "Unemployment Rate": "percentage",
            "Nonfarm Payroll": "thousands",
            "Consumer Sentiment": "index",
            "Durable Goods Orders": "percentage_change"
        }
        
        return unit_mapping.get(indicator_name, "unknown")
    
    def _get_importance_for_indicator(self, indicator_name: str) -> int:
        """Get the importance level for a specific economic indicator."""
        importance_mapping = {
            "Real GDP": 5,
            "Federal Funds Rate": 5,
            "Unemployment Rate": 5,
            "Nonfarm Payroll": 5,
            "Consumer Price Index": 5,
            "Inflation Rate": 5,
            "Real GDP per capita": 4,
            "Retail Sales": 4,
            "Consumer Sentiment": 3,
            "Durable Goods Orders": 3
        }
        
        return importance_mapping.get(indicator_name, 3)
    
    async def fetch_all_indicators(self, start_date: Optional[date] = None,
                                 end_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Fetch all available economic indicators.
        
        Args:
            start_date: Filter start date (not all indicators support date filtering)
            end_date: Filter end date
            
        Returns:
            List of all economic events
        """
        logger.info("Fetching all economic indicators from Alpha Vantage...")
        
        # Fetch all indicator types
        all_events = []
        
        indicators = [
            self.fetch_real_gdp(),
            self.fetch_federal_funds_rate(),
            self.fetch_cpi(),
            self.fetch_inflation(),
            self.fetch_unemployment_rate(),
            self.fetch_nonfarm_payroll(),
            self.fetch_retail_sales(),
            self.fetch_consumer_sentiment(),
            self.fetch_durable_goods_orders()
        ]
        
        # Execute requests with rate limiting
        for indicator_coro in indicators:
            try:
                indicator_data = await indicator_coro
                all_events.extend(indicator_data)
                
                # Rate limiting - Alpha Vantage free tier allows 5 requests per minute
                await asyncio.sleep(15)  # Wait 15 seconds between requests
                
            except Exception as e:
                logger.error(f"Error fetching indicator data: {e}")
        
        # Filter by date range if provided
        if start_date or end_date:
            filtered_events = []
            for event in all_events:
                event_date = event.get("event_date")
                if event_date:
                    if start_date and event_date < start_date:
                        continue
                    if end_date and event_date > end_date:
                        continue
                    filtered_events.append(event)
            all_events = filtered_events
        
        logger.info(f"Fetched total of {len(all_events)} economic events from Alpha Vantage")
        return all_events