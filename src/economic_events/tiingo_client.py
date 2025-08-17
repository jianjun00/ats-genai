#!/usr/bin/env python3
"""
Tiingo Economic Events API Client.
Fetches economic events data from Tiingo API.
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class TiingoEconomicEventsClient:
    """Client for Tiingo Economic Events API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tiingo.com"
    
    async def fetch_economic_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Fetch economic events from Tiingo API.
        Note: Tiingo's economic calendar endpoint might be limited, 
        but they do have news feeds that include economic event mentions.
        
        Args:
            start_date: Start date for events
            end_date: End date for events
            
        Returns:
            List of economic events data
        """
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Tiingo doesn't have a dedicated economic events endpoint like Polygon
            # We'll use their news API to find economic event mentions
            url = f"{self.base_url}/tiingo/news"
            
            params = {
                "token": self.api_key,
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "limit": 1000,
                "search": "economic|GDP|inflation|employment|fed|unemployment|CPI|PPI|retail|jobless|payrolls"
            }
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Filter news articles for economic events
                        economic_events = []
                        for article in data:
                            if self._is_economic_event(article):
                                economic_events.append(article)
                        
                        logger.info(f"Fetched {len(economic_events)} economic events from Tiingo news")
                        return economic_events
                    elif response.status == 429:
                        logger.warning("Tiingo API rate limit hit")
                        await asyncio.sleep(5)  # Tiingo rate limit handling
                        return []
                    else:
                        logger.error(f"Tiingo API error: {response.status}")
                        return []
                        
            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching Tiingo events: {e}")
                return []
    
    async def fetch_crypto_economic_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Fetch crypto-related economic events from Tiingo.
        
        Args:
            start_date: Start date for events
            end_date: End date for events
            
        Returns:
            List of crypto economic events
        """
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{self.base_url}/tiingo/crypto/news"
            
            params = {
                "token": self.api_key,
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "limit": 500,
                "search": "economic|regulation|fed|policy|SEC|CFTC"
            }
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Filter for regulatory/economic crypto events
                        economic_events = []
                        for article in data:
                            if self._is_crypto_economic_event(article):
                                economic_events.append(article)
                        
                        logger.info(f"Fetched {len(economic_events)} crypto economic events from Tiingo")
                        return economic_events
                    else:
                        logger.error(f"Tiingo crypto API error: {response.status}")
                        return []
                        
            except aiohttp.ClientError as e:
                logger.error(f"Connection error fetching Tiingo crypto events: {e}")
                return []
    
    def _is_economic_event(self, article: Dict[str, Any]) -> bool:
        """
        Determine if a news article represents an economic event.
        
        Args:
            article: Tiingo news article data
            
        Returns:
            True if article is about economic events
        """
        title = article.get("title", "").lower()
        description = article.get("description", "").lower()
        text = f"{title} {description}"
        
        # Keywords that indicate economic events
        economic_keywords = [
            "nonfarm payrolls", "unemployment rate", "cpi", "consumer price index",
            "producer price index", "ppi", "gdp", "gross domestic product",
            "federal reserve", "fed rate", "interest rate", "fomc",
            "retail sales", "industrial production", "consumer confidence",
            "housing starts", "jobless claims", "inflation", "deflation",
            "economic data", "economic indicator", "economic report"
        ]
        
        return any(keyword in text for keyword in economic_keywords)
    
    def _is_crypto_economic_event(self, article: Dict[str, Any]) -> bool:
        """
        Determine if a crypto news article represents an economic/regulatory event.
        
        Args:
            article: Tiingo crypto news article
            
        Returns:
            True if article is about crypto economic events
        """
        title = article.get("title", "").lower()
        description = article.get("description", "").lower()
        text = f"{title} {description}"
        
        # Keywords for crypto economic/regulatory events
        crypto_economic_keywords = [
            "sec", "securities and exchange commission", "cftc", "regulation",
            "regulatory", "compliance", "policy", "government", "federal",
            "treasury", "yellen", "powell", "fed", "federal reserve",
            "legislation", "bill", "congress", "senate", "house"
        ]
        
        return any(keyword in text for keyword in crypto_economic_keywords)
    
    def parse_tiingo_event(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Tiingo news article into economic event format.
        
        Args:
            article_data: Raw Tiingo article data
            
        Returns:
            Parsed event data
        """
        try:
            # Parse publication date
            event_date = None
            if article_data.get("publishedDate"):
                event_date = datetime.fromisoformat(
                    article_data["publishedDate"].replace("Z", "+00:00")
                ).date()
            
            # Extract event name from title
            event_name = article_data.get("title", "").strip()
            
            # Categorize the event based on title/description
            category = self._categorize_event(article_data)
            
            return {
                "event_name": event_name,
                "event_date": event_date,
                "release_time": None,  # Tiingo news doesn't have specific release times
                "actual": None,        # News articles don't have actual values
                "estimate": None,      # News articles don't have estimates
                "previous": None,      # News articles don't have previous values
                "unit": None,
                "currency": None,
                "country": "USA",      # Most Tiingo economic news is US-focused
                "importance": self._estimate_importance(article_data),
                "source_vendor": "tiingo",
                "source_event_id": article_data.get("id"),
                "vendor_specific_data": {
                    "description": article_data.get("description"),
                    "source_url": article_data.get("url"),
                    "tags": article_data.get("tags", [])
                },
                "raw_data": article_data
            }
            
        except Exception as e:
            logger.error(f"Error parsing Tiingo event data: {e}")
            return {
                "event_name": "Unknown Economic Event",
                "source_vendor": "tiingo",
                "raw_data": article_data,
                "parse_error": str(e)
            }
    
    def _categorize_event(self, article_data: Dict[str, Any]) -> str:
        """
        Categorize the economic event based on article content.
        
        Args:
            article_data: Article data
            
        Returns:
            Event category
        """
        title = article_data.get("title", "") or ""
        description = article_data.get("description", "") or ""
        text = f"{title.lower()} {description.lower()}"
        
        if any(word in text for word in ["employment", "jobless", "payroll", "unemployment"]):
            return "Employment"
        elif any(word in text for word in ["inflation", "cpi", "consumer price", "ppi", "producer price"]):
            return "Inflation"
        elif any(word in text for word in ["fed", "federal reserve", "interest rate", "fomc"]):
            return "Interest Rates"
        elif any(word in text for word in ["gdp", "growth", "economic growth"]):
            return "Growth"
        elif any(word in text for word in ["retail sales", "consumer spending", "consumption"]):
            return "Consumption"
        elif any(word in text for word in ["housing", "real estate", "home sales"]):
            return "Housing"
        else:
            return "General Economic"
    
    def _estimate_importance(self, article_data: Dict[str, Any]) -> int:
        """
        Estimate importance level of economic event based on article.
        
        Args:
            article_data: Article data
            
        Returns:
            Importance level (1-5)
        """
        title = article_data.get("title", "").lower()
        description = article_data.get("description", "").lower()
        text = f"{title} {description}"
        
        # High importance indicators
        high_importance_keywords = [
            "federal reserve", "fed decision", "interest rate", "fomc",
            "nonfarm payrolls", "unemployment rate", "cpi", "gdp"
        ]
        
        # Medium importance indicators
        medium_importance_keywords = [
            "retail sales", "consumer confidence", "industrial production",
            "housing starts", "jobless claims"
        ]
        
        if any(keyword in text for keyword in high_importance_keywords):
            return 5
        elif any(keyword in text for keyword in medium_importance_keywords):
            return 3
        else:
            return 2  # Default to low-medium importance for other economic news