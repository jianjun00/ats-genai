"""
Real Grok Financial Events Extractor
Uses xAI API to access Grok-4 with Live Search for real-time financial events
"""

import asyncio
import json
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from .xai_event_extractor import FinancialEvent
from .cache_manager import SmartCacheManager

logger = logging.getLogger(__name__)

class GrokFinancialEventExtractor:
    """
    Real Grok integration for financial events extraction
    Uses xAI API with Grok-4 and Live Search capabilities
    """
    
    def __init__(
        self, 
        api_key: str,
        enable_cache: bool = True,
        cache_ttl_hours: int = 6
    ):
        self.api_key = api_key
        self.base_url = "https://api.x.ai/v1"
        self.model = "grok-4"  # Latest Grok model with best capabilities
        
        # Initialize caching system
        self.enable_cache = enable_cache
        if enable_cache:
            self.cache_manager = SmartCacheManager(
                cache_dir="/tmp/grok_event_cache",
                ttl_hours=cache_ttl_hours
            )
            logger.info("Grok cache manager initialized")
        
        # Cached system prompt (saves 75% on input tokens)
        self.cached_system_prompt = """
        You are Grok, a real-time financial events analyst with access to live market data.
        
        Extract financial events from recent market data using Live Search.
        Focus on HIGH-IMPACT events that could significantly affect stock prices.
        
        Return events in this exact JSON structure:
        {
            "events": [
                {
                    "event_type": "earnings|fed_announcement|stock_event|economic_indicator|merger|ipo|dividend|split|guidance",
                    "company_symbol": "SYMBOL or null for market-wide events",
                    "details": "Detailed description with specific numbers/dates",
                    "event_date": "YYYY-MM-DD",
                    "event_time": "HH:MM:SS or null",
                    "impact_level": "high|medium|low", 
                    "sentiment": "positive|negative|neutral",
                    "confidence_score": 0.0-1.0,
                    "source_url": "URL of primary source"
                }
            ]
        }
        
        Requirements:
        - Use Live Search to get REAL, current financial data
        - Include specific numbers, percentages, dates
        - Focus on events from major exchanges (NYSE, NASDAQ)
        - Prioritize S&P 500 and major tech stocks
        - Include confidence score based on source reliability
        """
        
        logger.info("Grok financial events extractor initialized")

    async def extract_events_batch(
        self,
        start_date: str,
        end_date: str, 
        symbols: List[str] = None,
        max_events_per_call: int = 25,
        force_refresh: bool = False
    ) -> List[FinancialEvent]:
        """
        Extract financial events using real Grok API with Live Search
        """
        
        logger.info(f"Grok extraction: {start_date} to {end_date}, symbols: {symbols}")
        
        # Check cache first
        cache_key = f"grok_events_{start_date}_{end_date}_{hash(str(symbols))}"
        
        if self.enable_cache and not force_refresh:
            cached_events = await self.cache_manager.get_cached_events(cache_key)
            if cached_events:
                logger.info(f"Cache hit: {len(cached_events)} events")
                return cached_events
        
        # Execute real API extraction
        events = await self._execute_live_extraction(
            start_date, end_date, symbols, max_events_per_call
        )
        
        # Cache results
        if self.enable_cache and events:
            await self.cache_manager.cache_events(cache_key, events)
        
        return events

    async def _execute_live_extraction(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None,
        max_events_per_call: int = 25
    ) -> List[FinancialEvent]:
        """Execute real Grok API extraction with Live Search"""
        
        # Build optimized search query for Live Search
        search_query = self._build_live_search_query(start_date, end_date, symbols)
        
        try:
            # Real API call to Grok-4 with Live Search
            response = await self._make_real_grok_api_call(search_query, max_events_per_call)
            events = self._parse_grok_response(response)
            
            logger.info(f"Grok API call completed: {len(events)} events extracted")
            return events
            
        except Exception as e:
            logger.error(f"Grok event extraction failed: {e}")
            return []

    def _build_live_search_query(
        self, 
        start_date: str, 
        end_date: str, 
        symbols: List[str] = None
    ) -> str:
        """
        Build Live Search query optimized for real-time financial data
        """
        
        # Build symbol filter
        symbol_filter = ""
        if symbols:
            symbol_list = " OR ".join([f'"{symbol}"' for symbol in symbols])
            symbol_filter = f" AND ({symbol_list})"
        
        # Optimized query for Grok's Live Search
        query = f"""
        Use Live Search to find HIGH-IMPACT financial events from {start_date} to {end_date}.
        
        Search for:
        - Earnings reports and earnings beats/misses
        - Federal Reserve announcements and interest rate decisions  
        - Major stock events (splits, dividends, buybacks)
        - Economic indicators (GDP, inflation, employment)
        - Merger and acquisition announcements
        - IPO launches and major offerings
        - Analyst upgrades/downgrades
        - Executive changes at major companies
        {symbol_filter}
        
        Focus on:
        - S&P 500 companies
        - Major technology stocks (AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA)
        - Financial sector leaders (JPM, BAC, WFC, GS)
        - Events that moved stock prices >2%
        
        Use current, verified financial news sources.
        Extract specific details: revenue numbers, percentage changes, exact dates/times.
        Include confidence scores based on source credibility.
        """
        
        return query

    async def _make_real_grok_api_call(self, query: str, max_events: int) -> Dict[str, Any]:
        """
        Make REAL API call to Grok-4 via xAI API with Live Search
        """
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # API payload for Grok-4 with Live Search
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": self.cached_system_prompt
                },
                {
                    "role": "user", 
                    "content": query
                }
            ],
            "temperature": 0.1,  # Low temperature for factual accuracy
            "max_tokens": 4000,
            "stream": False,
            # Enable Live Search for real-time data
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "search_web",
                        "description": "Search for current financial information",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Search query for financial events"
                                }
                            },
                            "required": ["query"]
                        }
                    }
                }
            ]
        }
        
        logger.info(f"Making REAL Grok API call to {self.base_url}/chat/completions")
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=payload
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        logger.info("✅ Grok API call successful")
                        return result
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Grok API error {response.status}: {error_text}")
                        raise Exception(f"Grok API error {response.status}: {error_text}")
                        
        except aiohttp.ClientError as e:
            logger.error(f"❌ Grok API connection error: {e}")
            raise Exception(f"Grok API connection failed: {e}")

    def _parse_grok_response(self, response: Dict[str, Any]) -> List[FinancialEvent]:
        """
        Parse real Grok API response into FinancialEvent objects
        """
        
        try:
            events = []
            
            # Extract message content from Grok response
            if 'choices' in response and response['choices']:
                content = response['choices'][0]['message']['content']
                
                # Try to parse as JSON
                try:
                    events_data = json.loads(content)
                except json.JSONDecodeError:
                    # If not JSON, try to extract JSON from markdown
                    import re
                    json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
                    if json_match:
                        events_data = json.loads(json_match.group(1))
                    else:
                        logger.error("Could not extract JSON from Grok response")
                        return []
                
                # Parse events from response
                for event_data in events_data.get('events', []):
                    event = FinancialEvent(
                        event_type=event_data['event_type'],
                        company_symbol=event_data.get('company_symbol'),
                        details=event_data['details'],
                        event_date=event_data['event_date'],
                        event_time=event_data.get('event_time'),
                        impact_level=event_data['impact_level'],
                        sentiment=event_data.get('sentiment'),
                        confidence_score=event_data.get('confidence_score'),
                        source_url=event_data.get('source_url')
                    )
                    events.append(event)
                    
                logger.info(f"✅ Parsed {len(events)} events from Grok response")
                return events
                
            else:
                logger.error("No choices in Grok API response")
                return []
                
        except Exception as e:
            logger.error(f"❌ Failed to parse Grok response: {e}")
            return []

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        if self.enable_cache:
            return await self.cache_manager.get_cache_stats()
        return {"cache_enabled": False}

    async def clear_cache(self):
        """Clear all cached events"""
        if self.enable_cache:
            await self.cache_manager.clear_cache()

    async def extract_trending_events(self, hours_back: int = 24) -> List[FinancialEvent]:
        """
        Extract trending financial events from last N hours
        Uses Live Search to get most current data
        """
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        start_date = start_time.strftime('%Y-%m-%d')
        end_date = end_time.strftime('%Y-%m-%d')
        
        logger.info(f"Extracting trending events from last {hours_back} hours")
        
        # Use trending-focused query
        trending_query = f"""
        Use Live Search to find TRENDING financial events from the last {hours_back} hours.
        
        Focus on:
        - Breaking news that's actively moving markets
        - Events trending on financial Twitter/X
        - After-hours earnings releases
        - Federal Reserve emergency announcements
        - Sudden analyst downgrades/upgrades
        - Unexpected executive departures
        - Major regulatory news
        
        Prioritize events that:
        - Have high social media engagement
        - Are causing significant price movements
        - Are being widely discussed by financial analysts
        
        Search recent financial news, X/Twitter financial discussions, and market data feeds.
        """
        
        try:
            response = await self._make_real_grok_api_call(trending_query, 20)
            events = self._parse_grok_response(response)
            
            logger.info(f"✅ Extracted {len(events)} trending events")
            return events
            
        except Exception as e:
            logger.error(f"❌ Trending events extraction failed: {e}")
            return []


# Usage example and testing
async def demo_real_grok_integration():
    """Demonstrate real Grok integration"""
    
    print("🚀 Real Grok Financial Events Integration Demo")
    print("=" * 60)
    
    # Replace with real API key
    api_key = "your_real_xai_api_key_here"  # Get from x.ai
    
    if api_key == "your_real_xai_api_key_here":
        print("❌ Please provide a real xAI API key to test Grok integration")
        return
    
    # Initialize real Grok extractor
    grok = GrokFinancialEventExtractor(
        api_key=api_key,
        enable_cache=True,
        cache_ttl_hours=6
    )
    
    print("📊 Extracting trending financial events...")
    trending_events = await grok.extract_trending_events(hours_back=24)
    
    print(f"✅ Found {len(trending_events)} trending events:")
    for i, event in enumerate(trending_events[:5], 1):
        symbol = event.company_symbol or 'MARKET'
        confidence = int((event.confidence_score or 0) * 100)
        print(f"  {i}. {event.event_date} | {symbol} ({event.impact_level}) - {event.details[:80]}... [{confidence}%]")
    
    print("\n📈 Extracting recent events for major tech stocks...")
    recent_events = await grok.extract_events_batch(
        start_date="2025-09-10",
        end_date="2025-09-13",
        symbols=["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]
    )
    
    print(f"✅ Found {len(recent_events)} recent events:")
    for i, event in enumerate(recent_events[:3], 1):
        symbol = event.company_symbol or 'MARKET'
        confidence = int((event.confidence_score or 0) * 100)
        print(f"  {i}. {symbol}: {event.details[:100]}... [{confidence}%]")
    
    # Show cache performance
    cache_stats = await grok.get_cache_stats()
    print(f"\n🚀 Cache Performance: {cache_stats.get('hit_rate', 'N/A')} hit rate")

if __name__ == "__main__":
    asyncio.run(demo_real_grok_integration())