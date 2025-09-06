#!/usr/bin/env python3
"""
Populate Economic Events Script.
Command-line script to populate economic events from multiple vendors.
"""

import asyncio
import logging
import os
import argparse
from datetime import datetime, date, timedelta
from typing import Optional

from core.config.environment import Environment, EnvironmentType
from core.config.database import get_connection_pool
from economic_events.population_service import EconomicEventsPopulationService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def populate_economic_events(
    environment: str = "dev",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    vendors: Optional[list] = None,
    min_importance: int = 1,
    polygon_api_key: Optional[str] = None,
    tiingo_api_key: Optional[str] = None,
    alpha_vantage_api_key: Optional[str] = None,
    fred_api_key: Optional[str] = None
):
    """
    Main function to populate economic events.
    
    Args:
        environment: Environment type (dev, intg, prod)
        start_date: Start date for events
        end_date: End date for events
        vendors: List of vendors to use
        min_importance: Minimum importance level
        polygon_api_key: Polygon API key
        tiingo_api_key: Tiingo API key
        alpha_vantage_api_key: Alpha Vantage API key
        fred_api_key: FRED API key
    """
    logger.info(f"🚀 Starting economic events population for {environment} environment")
    
    # Set default date range (last 30 days)
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    
    # Set default vendors
    if not vendors:
        vendors = ["polygon", "tiingo", "alpha_vantage", "fred"]
    
    logger.info(f"🌐 Vendors: {', '.join(vendors)}")
    
    try:
        # Initialize environment
        env_type = EnvironmentType(environment.upper())
        env = Environment(env_type)
        
        # Get database connection
        connection_pool = await get_connection_pool(env)
        logger.info("✅ Connected to database")
        
        # Initialize population service
        service = EconomicEventsPopulationService(env, connection_pool)
        
        # Initialize API clients
        service.initialize_clients(
            polygon_api_key=polygon_api_key,
            tiingo_api_key=tiingo_api_key,
            alpha_vantage_api_key=alpha_vantage_api_key,
            fred_api_key=fred_api_key
        )
        
        # Populate events
        results = await service.populate_economic_events(
            start_date=start_date,
            end_date=end_date,
            vendors=vendors,
            min_importance=min_importance
        )
        
        # Display results
        logger.info("📊 POPULATION RESULTS:")
        logger.info(f"   • Total events processed: {results['total_events_processed']}")
        logger.info(f"   • Total events stored: {results['total_events_stored']}")
        logger.info(f"   • Vendors processed: {', '.join(results['vendors_processed'])}")
        
        for vendor, vendor_result in results["vendor_results"].items():
            if "error" in vendor_result:
                logger.error(f"   • {vendor}: ERROR - {vendor_result['error']}")
            else:
                logger.info(f"   • {vendor}: {vendor_result['events_stored']}/{vendor_result['events_processed']} events stored")
        
        # Get final statistics
        stats = await service.get_population_statistics()
        logger.info("📈 DATABASE STATISTICS:")
        logger.info(f"   • Total events in database: {stats['overall']['total_events']}")
        logger.info(f"   • Unique event types: {stats['overall']['unique_event_types']}")
        logger.info(f"   • Vendors represented: {stats['overall']['unique_vendors']}")
        
        # Show upcoming high-impact events
        upcoming = await service.get_upcoming_high_impact_events(days_ahead=7)
        if upcoming:
            logger.info(f"📢 UPCOMING HIGH-IMPACT EVENTS (next 7 days): {len(upcoming)} events")
            for event in upcoming[:5]:  # Show first 5
                logger.info(f"   • {event['date']}: {event['event_name']} (importance: {event['importance_level']})")
        
        await connection_pool.close()
        logger.info("✅ Economic events population completed successfully!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during population: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Populate economic events from multiple vendors"
    )
    
    parser.add_argument(
        "--environment",
        choices=["dev", "intg", "prod"],
        default="dev",
        help="Environment to populate (default: dev)"
    )
    
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Start date (YYYY-MM-DD, default: 30 days ago)"
    )
    
    parser.add_argument(
        "--end-date", 
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="End date (YYYY-MM-DD, default: today)"
    )
    
    parser.add_argument(
        "--vendors",
        nargs="+",
        choices=["polygon", "tiingo", "alpha_vantage", "fred"],
        help="Vendors to use (default: all)"
    )
    
    parser.add_argument(
        "--min-importance",
        type=int,
        default=1,
        choices=[1, 2, 3, 4, 5],
        help="Minimum importance level (1-5, default: 1)"
    )
    
    parser.add_argument(
        "--polygon-api-key",
        help="Polygon API key (or set POLYGON_API_KEY env var)"
    )
    
    parser.add_argument(
        "--tiingo-api-key",
        help="Tiingo API key (or set TIINGO_API_KEY env var)"
    )
    
    parser.add_argument(
        "--alpha-vantage-api-key",
        help="Alpha Vantage API key (or set ALPHA_VANTAGE_API_KEY env var)"
    )
    
    parser.add_argument(
        "--fred-api-key",
        help="FRED API key (or set FRED_API_KEY env var)"
    )
    
    args = parser.parse_args()
    
    # Get API keys using centralized management system with fallback
    def get_api_key_with_fallback(vendor, arg_value, env_var):
        """Get API key with centralized system fallback."""
        # First try argument
        if arg_value:
            return arg_value
            
        try:
            # Try centralized system
            from core.config.environment import env
            if env:
                key = env.get_api_key(vendor)
                if key:
                    return key
        except Exception:
            pass
            
        # Fallback to environment variable
        return os.getenv(env_var)
    
    polygon_api_key = get_api_key_with_fallback('polygon', args.polygon_api_key, "POLYGON_API_KEY")
    tiingo_api_key = get_api_key_with_fallback('tiingo', args.tiingo_api_key, "TIINGO_API_KEY")  
    alpha_vantage_api_key = get_api_key_with_fallback('alpha_vantage', args.alpha_vantage_api_key, "ALPHA_VANTAGE_API_KEY")
    fred_api_key = args.fred_api_key or os.getenv("FRED_API_KEY")  # FRED not in centralized system yet
    
    # Run population
    success = asyncio.run(populate_economic_events(
        environment=args.environment,
        start_date=args.start_date,
        end_date=args.end_date,
        vendors=args.vendors,
        min_importance=args.min_importance,
        polygon_api_key=polygon_api_key,
        tiingo_api_key=tiingo_api_key,
        alpha_vantage_api_key=alpha_vantage_api_key,
        fred_api_key=fred_api_key
    ))
    
    exit(0 if success else 1)


if __name__ == "__main__":
    main()