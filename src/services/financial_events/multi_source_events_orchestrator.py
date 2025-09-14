"""
Unified Financial Events Integration
Combines xAI, Grok, and other sources for comprehensive financial events coverage
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from enum import Enum

from .xai_event_extractor import OptimizedXAIEventExtractor, FinancialEvent
from .grok_event_extractor import GrokFinancialEventExtractor
from .analytics_integration import AnalyticsEventIntegration, convert_dates_to_strings

logger = logging.getLogger(__name__)

class EventSource(Enum):
    XAI = "xai"
    GROK = "grok"
    COMBINED = "combined"

class UnifiedFinancialEventsIntegration:
    """
    Unified integration for multiple AI financial event sources
    Provides intelligent source selection and result aggregation
    """
    
    def __init__(
        self,
        xai_api_key: str = None,
        grok_api_key: str = None,
        analytics_base_url: str = "http://localhost:4000",
        enable_cache: bool = True
    ):
        self.analytics_url = analytics_base_url
        self.enable_cache = enable_cache
        
        # Initialize available extractors
        self.extractors = {}
        
        if xai_api_key and xai_api_key != "demo_key_for_testing":
            self.extractors[EventSource.XAI] = OptimizedXAIEventExtractor(
                api_key=xai_api_key,
                enable_cache=enable_cache,
                cache_ttl_hours=6
            )
            logger.info("✅ xAI extractor initialized")
        
        if grok_api_key and grok_api_key != "demo_key_for_testing":
            self.extractors[EventSource.GROK] = GrokFinancialEventExtractor(
                api_key=grok_api_key,
                enable_cache=enable_cache,
                cache_ttl_hours=6
            )
            logger.info("✅ Grok extractor initialized")
        
        if not self.extractors:
            logger.warning("⚠️ No real API keys provided - running in demo mode")
        
        # Initialize analytics integration
        self.analytics = AnalyticsEventIntegration(
            xai_api_key=xai_api_key or "demo_key",
            analytics_base_url=analytics_base_url
        )
        
        logger.info(f"Unified integration initialized with {len(self.extractors)} source(s)")

    async def extract_events_multi_source(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None,
        preferred_source: EventSource = EventSource.COMBINED,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Extract events from multiple AI sources with intelligent aggregation
        """
        
        logger.info(f"Multi-source extraction: {start_date} to {end_date}, source: {preferred_source.value}")
        
        all_events = []
        source_results = {}
        
        try:
            # Extract from available sources
            if preferred_source == EventSource.COMBINED:
                # Use all available sources
                tasks = []
                for source, extractor in self.extractors.items():
                    task = self._extract_from_source(
                        extractor, source, start_date, end_date, symbols, force_refresh
                    )
                    tasks.append(task)
                
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for i, (source, result) in enumerate(zip(self.extractors.keys(), results)):
                        if isinstance(result, Exception):
                            logger.error(f"❌ {source.value} extraction failed: {result}")
                            source_results[source.value] = {"events": [], "error": str(result)}
                        else:
                            source_results[source.value] = {"events": result, "error": None}
                            all_events.extend(result)
                            logger.info(f"✅ {source.value}: {len(result)} events")
            
            else:
                # Use specific source
                if preferred_source in self.extractors:
                    events = await self._extract_from_source(
                        self.extractors[preferred_source], 
                        preferred_source,
                        start_date, end_date, symbols, force_refresh
                    )
                    all_events = events
                    source_results[preferred_source.value] = {"events": events, "error": None}
                else:
                    logger.error(f"❌ Requested source {preferred_source.value} not available")
                    return {
                        "success": False,
                        "error": f"Source {preferred_source.value} not configured",
                        "events_extracted": 0,
                        "events_stored": 0
                    }
            
            # Deduplicate and enhance events
            unique_events = self._deduplicate_events(all_events)
            enhanced_events = self._enhance_events_with_source_info(unique_events, source_results)
            
            # Store in analytics database
            stored_count = await self.analytics.store_events(enhanced_events)
            
            return {
                "success": True,
                "events_extracted": len(all_events),
                "events_unique": len(unique_events), 
                "events_stored": stored_count,
                "date_range": f"{start_date} to {end_date}",
                "symbols": symbols or "All",
                "sources_used": list(source_results.keys()),
                "source_breakdown": {
                    source: len(data["events"]) 
                    for source, data in source_results.items()
                },
                "events_preview": [
                    {
                        "source": getattr(event, 'ai_source', 'unknown'),
                        "type": event.event_type,
                        "symbol": event.company_symbol,
                        "date": event.event_date,
                        "impact": event.impact_level,
                        "details": event.details[:100] + "..." if len(event.details) > 100 else event.details,
                        "confidence": event.confidence_score
                    }
                    for event in enhanced_events[:5]
                ]
            }
            
        except Exception as e:
            logger.error(f"❌ Multi-source extraction failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "events_extracted": 0,
                "events_stored": 0
            }

    async def _extract_from_source(
        self,
        extractor: Union[OptimizedXAIEventExtractor, GrokFinancialEventExtractor],
        source: EventSource,
        start_date: str,
        end_date: str, 
        symbols: List[str],
        force_refresh: bool
    ) -> List[FinancialEvent]:
        """Extract events from a specific source"""
        
        try:
            logger.info(f"🔄 Extracting from {source.value}...")
            
            events = await extractor.extract_events_batch(
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
                force_refresh=force_refresh
            )
            
            # Tag events with source
            for event in events:
                event.ai_source = source.value
                
            return events
            
        except Exception as e:
            logger.error(f"❌ {source.value} extraction error: {e}")
            raise

    def _deduplicate_events(self, events: List[FinancialEvent]) -> List[FinancialEvent]:
        """
        Remove duplicate events using intelligent similarity matching
        """
        
        if len(events) <= 1:
            return events
        
        unique_events = []
        seen_signatures = set()
        
        for event in events:
            # Create signature for duplicate detection
            signature = self._create_event_signature(event)
            
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                unique_events.append(event)
            else:
                logger.debug(f"Duplicate event filtered: {event.details[:50]}...")
        
        logger.info(f"✅ Deduplication: {len(events)} → {len(unique_events)} unique events")
        return unique_events

    def _create_event_signature(self, event: FinancialEvent) -> str:
        """Create a signature for duplicate detection"""
        
        # Normalize key attributes for comparison
        symbol = (event.company_symbol or "").upper().strip()
        event_type = event.event_type.lower().strip()
        date = event.event_date
        
        # Create normalized details (first 50 chars, lowercase, no special chars)
        import re
        details_normalized = re.sub(r'[^a-z0-9\s]', '', event.details.lower())[:50].strip()
        
        signature = f"{symbol}|{event_type}|{date}|{details_normalized}"
        return signature

    def _enhance_events_with_source_info(
        self, 
        events: List[FinancialEvent], 
        source_results: Dict[str, Any]
    ) -> List[FinancialEvent]:
        """
        Enhance events with source reliability and consensus information
        """
        
        enhanced_events = []
        
        for event in events:
            # Add source information
            if not hasattr(event, 'ai_source'):
                event.ai_source = 'unknown'
            
            # Adjust confidence based on source reliability
            if hasattr(event, 'ai_source'):
                if event.ai_source == 'grok':
                    # Grok has real-time data, boost confidence for recent events
                    if event.confidence_score and self._is_recent_event(event):
                        event.confidence_score = min(1.0, event.confidence_score * 1.1)
                elif event.ai_source == 'xai':
                    # xAI has structured analysis, boost for complex events
                    if event.event_type in ['earnings', 'fed_announcement']:
                        event.confidence_score = min(1.0, (event.confidence_score or 0.5) * 1.05)
            
            enhanced_events.append(event)
        
        return enhanced_events

    def _is_recent_event(self, event: FinancialEvent) -> bool:
        """Check if event is recent (within 7 days)"""
        try:
            event_date = datetime.strptime(event.event_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            return (today - event_date).days <= 7
        except:
            return False

    async def get_trending_events_all_sources(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        Get trending events from all available sources
        """
        
        logger.info(f"Getting trending events from all sources (last {hours_back}h)")
        
        trending_events = []
        source_results = {}
        
        # Extract trending from available sources
        for source, extractor in self.extractors.items():
            try:
                if hasattr(extractor, 'extract_trending_events'):
                    # Grok has trending events method
                    events = await extractor.extract_trending_events(hours_back)
                else:
                    # For xAI, use recent date range
                    end_date = datetime.now()
                    start_date = end_date - timedelta(hours=hours_back)
                    events = await extractor.extract_events_batch(
                        start_date=start_date.strftime('%Y-%m-%d'),
                        end_date=end_date.strftime('%Y-%m-%d'),
                        force_refresh=True
                    )
                
                # Tag with source
                for event in events:
                    event.ai_source = source.value
                
                trending_events.extend(events)
                source_results[source.value] = len(events)
                logger.info(f"✅ {source.value} trending: {len(events)} events")
                
            except Exception as e:
                logger.error(f"❌ {source.value} trending extraction failed: {e}")
                source_results[source.value] = 0
        
        # Deduplicate and store
        unique_events = self._deduplicate_events(trending_events)
        stored_count = await self.analytics.store_events(unique_events)
        
        return {
            "success": True,
            "trending_events": len(unique_events),
            "events_stored": stored_count,
            "hours_back": hours_back,
            "sources": source_results,
            "events_preview": [
                {
                    "source": getattr(event, 'ai_source', 'unknown'),
                    "symbol": event.company_symbol or 'MARKET',
                    "impact": event.impact_level,
                    "details": event.details[:80] + "...",
                    "confidence": int((event.confidence_score or 0) * 100)
                }
                for event in unique_events[:8]
            ]
        }

    async def get_unified_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics from all sources"""
        
        cache_stats = {"sources": {}}
        
        for source, extractor in self.extractors.items():
            try:
                stats = await extractor.get_cache_stats()
                cache_stats["sources"][source.value] = stats
            except Exception as e:
                cache_stats["sources"][source.value] = {"error": str(e)}
        
        return cache_stats

    async def clear_all_caches(self):
        """Clear caches from all sources"""
        
        for source, extractor in self.extractors.items():
            try:
                await extractor.clear_cache()
                logger.info(f"✅ Cleared {source.value} cache")
            except Exception as e:
                logger.error(f"❌ Failed to clear {source.value} cache: {e}")

    def get_available_sources(self) -> List[str]:
        """Get list of available/configured sources"""
        return [source.value for source in self.extractors.keys()]

    def get_integration_status(self) -> Dict[str, Any]:
        """Get status of all integrations"""
        return {
            "available_sources": self.get_available_sources(),
            "total_sources": len(self.extractors),
            "analytics_url": self.analytics_url,
            "cache_enabled": self.enable_cache,
            "status": "ready" if self.extractors else "demo_mode"
        }


# Demo and testing
async def demo_unified_integration():
    """Demonstrate unified multi-source integration"""
    
    print("🚀 Unified Financial Events Integration Demo")
    print("=" * 60)
    
    # Initialize with real API keys (replace with actual keys)
    unified = UnifiedFinancialEventsIntegration(
        xai_api_key="your_xai_api_key_here",    # Replace with real key
        grok_api_key="your_grok_api_key_here",  # Replace with real key
        enable_cache=True
    )
    
    status = unified.get_integration_status()
    print(f"📊 Integration Status: {status['status']}")
    print(f"📡 Available Sources: {', '.join(status['available_sources'])}")
    
    if not status['available_sources']:
        print("⚠️ No real API keys provided - demo mode only")
        return
    
    # Test multi-source extraction
    print("\n🔄 Testing multi-source event extraction...")
    result = await unified.extract_events_multi_source(
        start_date="2025-09-10",
        end_date="2025-09-13",
        symbols=["AAPL", "TSLA", "NVDA"],
        preferred_source=EventSource.COMBINED
    )
    
    if result['success']:
        print(f"✅ Multi-source extraction successful:")
        print(f"   📈 Events extracted: {result['events_extracted']}")
        print(f"   🔍 Unique events: {result['events_unique']}")
        print(f"   💾 Events stored: {result['events_stored']}")
        print(f"   📡 Sources used: {', '.join(result['sources_used'])}")
        
        print("\n📋 Source breakdown:")
        for source, count in result['source_breakdown'].items():
            print(f"   {source}: {count} events")
    
    # Test trending events
    print("\n🔥 Testing trending events extraction...")
    trending = await unified.get_trending_events_all_sources(hours_back=24)
    
    if trending['success']:
        print(f"✅ Trending events: {trending['trending_events']} found")
        print("📈 Sample trending events:")
        for event in trending['events_preview'][:3]:
            print(f"   {event['source']}: {event['symbol']} - {event['details']} [{event['confidence']}%]")

if __name__ == "__main__":
    asyncio.run(demo_unified_integration())