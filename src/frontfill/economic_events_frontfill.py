#!/usr/bin/env python3
"""
Economic Events Frontfill Job.
Continuously updates economic events data every 5 minutes.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Tuple, Optional

from frontfill.base_frontfill_job import BaseFrontfillJob, FrontfillConfig, CheckpointType
from config.environment import Environment
from economic_events.polygon_client import PolygonEconomicEventsClient
from economic_events.tiingo_client import TiingoEconomicEventsClient
from economic_events.alpha_vantage_client import AlphaVantageEconomicClient
from economic_events.fred_client import FREDEconomicClient
from economic_events.population_service import EconomicEventsPopulationService
import asyncpg

logger = logging.getLogger(__name__)


class EconomicEventsFrontfillJob(BaseFrontfillJob):
    """Frontfill job for economic events data."""
    
    def __init__(self, config: FrontfillConfig, connection_pool: asyncpg.Pool, 
                 env: Environment, api_key: str):
        super().__init__(config, connection_pool, env)
        self.api_key = api_key
        self.population_service = EconomicEventsPopulationService(env, connection_pool)
        
        # Initialize the appropriate client
        if config.vendor.lower() == "polygon":
            self.client = PolygonEconomicEventsClient(api_key)
        elif config.vendor.lower() == "tiingo":
            self.client = TiingoEconomicEventsClient(api_key)
        elif config.vendor.lower() == "alpha_vantage":
            self.client = AlphaVantageEconomicClient(api_key)
        elif config.vendor.lower() == "fred":
            self.client = FREDEconomicClient(api_key)
        else:
            raise ValueError(f"Unsupported vendor: {config.vendor}")
    
    async def get_default_starting_checkpoint(self) -> str:
        """Get default starting checkpoint - 1 day ago for economic events."""
        yesterday = date.today() - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    
    async def fetch_data_batch(self, checkpoint: str, batch_size: int) -> Tuple[List[Dict[str, Any]], str]:
        """Fetch economic events since the checkpoint."""
        try:
            # Parse checkpoint as date
            checkpoint_date = datetime.strptime(checkpoint, "%Y-%m-%d").date()
            
            # For economic events, we look ahead for upcoming events and back for updates
            start_date = checkpoint_date
            end_date = date.today() + timedelta(days=7)  # Look ahead 1 week
            
            events_data = await self._fetch_economic_events(start_date, end_date)
            
            # Next checkpoint is today (we'll check again tomorrow)
            next_checkpoint = date.today().strftime("%Y-%m-%d")
            
            return events_data, next_checkpoint
            
        except Exception as e:
            logger.error(f"Error fetching economic events batch: {e}")
            raise
    
    async def _fetch_economic_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch economic events from the specific vendor."""
        try:
            if self.config.vendor.lower() == "polygon":
                return await self._fetch_polygon_events(start_date, end_date)
            elif self.config.vendor.lower() == "tiingo":
                return await self._fetch_tiingo_events(start_date, end_date)
            elif self.config.vendor.lower() == "alpha_vantage":
                return await self._fetch_alpha_vantage_events(start_date, end_date)
            elif self.config.vendor.lower() == "fred":
                return await self._fetch_fred_events(start_date, end_date)
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error fetching {self.config.vendor} events: {e}")
            return []
    
    async def _fetch_polygon_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch events from Polygon."""
        try:
            raw_events = await self.client.fetch_economic_events(
                start_date, end_date, importance=3  # High importance events only
            )
            
            parsed_events = []
            for raw_event in raw_events:
                parsed_event = self.client.parse_polygon_event(raw_event)
                if parsed_event and parsed_event.get("importance", 0) >= 3:
                    parsed_events.append(parsed_event)
            
            logger.info(f"Fetched {len(parsed_events)} Polygon economic events")
            return parsed_events
            
        except Exception as e:
            logger.warning(f"Error fetching Polygon events: {e}")
            return []
    
    async def _fetch_tiingo_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch events from Tiingo (via news)."""
        try:
            raw_events = await self.client.fetch_economic_events(start_date, end_date)
            
            parsed_events = []
            for raw_event in raw_events:
                parsed_event = self.client.parse_tiingo_event(raw_event)
                if parsed_event and parsed_event.get("importance", 0) >= 3:
                    parsed_events.append(parsed_event)
            
            logger.info(f"Fetched {len(parsed_events)} Tiingo economic events")
            return parsed_events
            
        except Exception as e:
            logger.warning(f"Error fetching Tiingo events: {e}")
            return []
    
    async def _fetch_alpha_vantage_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch events from Alpha Vantage."""
        try:
            # Alpha Vantage provides economic indicators, not events per se
            # We fetch latest data and treat new observations as "events"
            raw_events = await self.client.fetch_all_indicators(start_date, end_date)
            
            # Filter for recent/updated indicators only
            recent_events = []
            for event in raw_events:
                event_date = event.get("event_date")
                if event_date and event_date >= start_date:
                    recent_events.append(event)
            
            logger.info(f"Fetched {len(recent_events)} Alpha Vantage economic indicators")
            return recent_events
            
        except Exception as e:
            logger.warning(f"Error fetching Alpha Vantage events: {e}")
            return []
    
    async def _fetch_fred_events(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch events from FRED."""
        try:
            # FRED provides economic data observations
            # We fetch recent data for popular series
            raw_events = await self.client.fetch_popular_indicators(start_date, end_date)
            
            # Filter for recent observations only
            recent_events = []
            for event in raw_events:
                event_date = event.get("event_date")
                if event_date and event_date >= start_date:
                    recent_events.append(event)
            
            logger.info(f"Fetched {len(recent_events)} FRED economic data points")
            return recent_events
            
        except Exception as e:
            logger.warning(f"Error fetching FRED events: {e}")
            return []
    
    async def process_data_batch(self, batch_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Process and store economic events data."""
        if not batch_data:
            return 0, 0
        
        inserted_count = 0
        updated_count = 0
        
        try:
            # Use the population service to store events with proper type handling
            for event_data in batch_data:
                try:
                    event_id = await self.population_service._store_economic_event(
                        event_data, self.config.vendor.lower()
                    )
                    if event_id:
                        inserted_count += 1
                except Exception as e:
                    logger.warning(f"Error storing economic event: {e}")
                    self.stats["error_count"] += 1
            
            logger.info(f"Processed {inserted_count} economic events for {self.config.vendor}")
            
        except Exception as e:
            logger.error(f"Error processing economic events batch: {e}")
            raise
        
        return inserted_count, updated_count


class InstrumentsFrontfillJob(BaseFrontfillJob):
    """Frontfill job for instruments data."""
    
    def __init__(self, config: FrontfillConfig, connection_pool: asyncpg.Pool, 
                 env: Environment, polygon_api_key: str):
        super().__init__(config, connection_pool, env)
        self.polygon_api_key = polygon_api_key
        self.table_name = env.get_table_name("instruments")
    
    async def get_default_starting_checkpoint(self) -> str:
        """Get default starting checkpoint - yesterday."""
        yesterday = date.today() - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    
    async def fetch_data_batch(self, checkpoint: str, batch_size: int) -> Tuple[List[Dict[str, Any]], str]:
        """Fetch new or updated instruments."""
        # For instruments, we typically get the full list and check for updates
        # This is a simplified version - in practice you'd use Polygon's tickers endpoint
        
        # For now, return empty as instruments don't change frequently
        # In production, you'd implement:
        # 1. Fetch tickers from Polygon
        # 2. Compare with existing instruments  
        # 3. Return new/updated instruments
        
        next_checkpoint = date.today().strftime("%Y-%m-%d")
        return [], next_checkpoint
    
    async def process_data_batch(self, batch_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Process instruments data."""
        # Implementation would depend on instruments data structure
        return 0, 0


# Factory function to create economic events frontfill jobs
async def create_economic_events_frontfill_jobs(connection_pool: asyncpg.Pool, 
                                              env: Environment,
                                              polygon_api_key: Optional[str] = None,
                                              tiingo_api_key: Optional[str] = None,
                                              alpha_vantage_api_key: Optional[str] = None,
                                              fred_api_key: Optional[str] = None) -> List[EconomicEventsFrontfillJob]:
    """Create economic events frontfill jobs for available vendors."""
    jobs = []
    
    # Polygon economic events job
    if polygon_api_key:
        polygon_config = FrontfillConfig(
            job_name="economic_events_polygon_frontfill",
            job_type="economic_events",
            vendor="polygon",
            checkpoint_type=CheckpointType.TIMESTAMP,
            batch_size=100,
            rate_limit_delay=0.1,
            duplicate_check_hours=24
        )
        
        polygon_job = EconomicEventsFrontfillJob(polygon_config, connection_pool, env, polygon_api_key)
        jobs.append(polygon_job)
    
    # Tiingo economic events job
    if tiingo_api_key:
        tiingo_config = FrontfillConfig(
            job_name="economic_events_tiingo_frontfill",
            job_type="economic_events",
            vendor="tiingo",
            checkpoint_type=CheckpointType.TIMESTAMP,
            batch_size=50,
            rate_limit_delay=0.5,
            duplicate_check_hours=24
        )
        
        tiingo_job = EconomicEventsFrontfillJob(tiingo_config, connection_pool, env, tiingo_api_key)
        jobs.append(tiingo_job)
    
    # Alpha Vantage economic events job
    if alpha_vantage_api_key:
        alpha_vantage_config = FrontfillConfig(
            job_name="economic_events_alpha_vantage_frontfill",
            job_type="economic_events", 
            vendor="alpha_vantage",
            checkpoint_type=CheckpointType.TIMESTAMP,
            batch_size=20,
            rate_limit_delay=15.0,  # Free tier limitation
            duplicate_check_hours=24
        )
        
        alpha_vantage_job = EconomicEventsFrontfillJob(alpha_vantage_config, connection_pool, env, alpha_vantage_api_key)
        jobs.append(alpha_vantage_job)
    
    # FRED economic events job
    if fred_api_key:
        fred_config = FrontfillConfig(
            job_name="economic_events_fred_frontfill",
            job_type="economic_events",
            vendor="fred",
            checkpoint_type=CheckpointType.TIMESTAMP,
            batch_size=50,
            rate_limit_delay=0.5,
            duplicate_check_hours=24
        )
        
        fred_job = EconomicEventsFrontfillJob(fred_config, connection_pool, env, fred_api_key)
        jobs.append(fred_job)
    
    return jobs


# Factory function for instruments job
async def create_instruments_frontfill_job(connection_pool: asyncpg.Pool, 
                                         env: Environment,
                                         polygon_api_key: str) -> InstrumentsFrontfillJob:
    """Create instruments frontfill job."""
    config = FrontfillConfig(
        job_name="instruments_frontfill",
        job_type="instruments",
        vendor="polygon",
        checkpoint_type=CheckpointType.TIMESTAMP,
        batch_size=1000,
        rate_limit_delay=1.0,
        duplicate_check_hours=168  # 1 week
    )
    
    return InstrumentsFrontfillJob(config, connection_pool, env, polygon_api_key)