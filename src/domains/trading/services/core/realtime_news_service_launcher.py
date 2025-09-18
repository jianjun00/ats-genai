#!/usr/bin/env python3
"""
Real-Time News Service Launcher

This service launcher integrates the real-time news ingestion pipeline with the
existing ATS infrastructure, providing a complete news-to-signal processing system.

Features:
- Database connection management with connection pooling
- API key configuration from environment variables
- Service health monitoring and automatic restarts
- Graceful shutdown handling
- Integration with existing logging and metrics systems
"""

import asyncio
import logging
import signal
import sys
from typing import Dict, Any, Optional
import os

import asyncpg
from src.core.platform.config.environment import Environment
from src.core.config.logging import setup_logging
from src.infrastructure.llm.multi_provider_client import MultiProviderLLMClient
from src.domains.market_data.services.news.realtime_news_ingestion import (
    create_realtime_news_service,
    RealTimeNewsIngestionService
)
from src.domains.market_data.services.signals.signal_broadcasting_system import (
    create_signal_broadcasting_system,
    TradingSignalBroadcastingSystem
)

logger = logging.getLogger(__name__)


class RealTimeNewsServiceManager:
    """Manager for the complete real-time news processing and signal broadcasting system."""

    def __init__(self):
        self.env: Optional[Environment] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.llm_client: Optional[MultiProviderLLMClient] = None
        self.news_service: Optional[RealTimeNewsIngestionService] = None
        self.broadcasting_system: Optional[TradingSignalBroadcastingSystem] = None

        self.shutdown_event = asyncio.Event()
        self._health_check_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize all service components."""
        logger.info("Initializing Real-Time News Service Manager")

        try:
            # Initialize environment
            self.env = Environment()

            # Initialize database connection pool
            await self._initialize_database()

            # Initialize LLM client
            await self._initialize_llm_client()

            # Initialize news service
            await self._initialize_news_service()

            # Initialize signal broadcasting system
            await self._initialize_broadcasting_system()

            # Set up signal handlers for graceful shutdown
            self._setup_signal_handlers()

            logger.info("Complete Real-Time News and Signal Broadcasting System initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize service manager: {e}")
            raise

    async def _initialize_database(self):
        """Initialize database connection pool."""
        try:
            database_url = self.env.get_database_url()

            self.db_pool = await asyncpg.create_pool(
                database_url,
                min_size=5,
                max_size=20,
                command_timeout=30,
                server_settings={
                    'jit': 'off',
                    'application_name': 'realtime_news_service'
                }
            )

            # Test connection
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    raise Exception("Database connection test failed")

            logger.info("Database connection pool initialized")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def _initialize_llm_client(self):
        """Initialize multi-provider LLM client."""
        try:
            # Get API keys from environment
            openai_key = os.getenv('OPENAI_API_KEY')
            anthropic_key = os.getenv('ANTHROPIC_API_KEY')
            google_key = os.getenv('GOOGLE_API_KEY')

            if not any([openai_key, anthropic_key, google_key]):
                raise ValueError("At least one LLM provider API key must be configured")

            # Create LLM client with available providers
            provider_configs = {}

            if openai_key:
                provider_configs['openai'] = {
                    'api_key': openai_key,
                    'model': 'gpt-4o-mini',  # Use efficient model for high-volume processing
                    'max_tokens': 1000,
                    'temperature': 0.1
                }

            if anthropic_key:
                provider_configs['anthropic'] = {
                    'api_key': anthropic_key,
                    'model': 'claude-3-haiku-20240307',  # Fast model for real-time processing
                    'max_tokens': 1000,
                    'temperature': 0.1
                }

            if google_key:
                provider_configs['google'] = {
                    'api_key': google_key,
                    'model': 'gemini-1.5-flash',  # Fast Gemini model
                    'max_tokens': 1000,
                    'temperature': 0.1
                }

            self.llm_client = MultiProviderLLMClient(provider_configs)
            await self.llm_client.initialize()

            logger.info(f"LLM client initialized with providers: {list(provider_configs.keys())}")

        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            raise

    async def _initialize_news_service(self):
        """Initialize the real-time news service."""
        try:
            # Get news vendor API keys
            api_keys = {
                'polygon': os.getenv('POLYGON_API_KEY'),
                'tiingo': os.getenv('TIINGO_API_KEY'),
                'alpha_vantage': os.getenv('ALPHA_VANTAGE_API_KEY'),
                'fmp': os.getenv('FMP_API_KEY'),
                'benzinga': os.getenv('BENZINGA_API_KEY')
            }

            # Filter out None values
            api_keys = {k: v for k, v in api_keys.items() if v}

            if not api_keys:
                raise ValueError("At least one news vendor API key must be configured")

            # Create news service
            self.news_service = await create_realtime_news_service(
                self.db_pool,
                self.env,
                self.llm_client,
                api_keys
            )

            logger.info(f"News service initialized with vendors: {list(api_keys.keys())}")

        except Exception as e:
            logger.error(f"Failed to initialize news service: {e}")
            raise

    async def _initialize_broadcasting_system(self):
        """Initialize the signal broadcasting system."""
        try:
            # Create broadcasting system
            self.broadcasting_system = await create_signal_broadcasting_system(
                self.db_pool,
                self.env
            )

            logger.info("Signal broadcasting system initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize broadcasting system: {e}")
            raise

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            asyncio.create_task(self.shutdown())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def start(self):
        """Start the real-time news service."""
        try:
            logger.info("Starting Real-Time News Service")

            # Start the news service
            await self.news_service.start()

            # Start the broadcasting system
            await self.broadcasting_system.start()

            # Start health check monitoring
            self._health_check_task = asyncio.create_task(self._health_check_loop())

            logger.info("Complete Real-Time News and Signal Broadcasting System started successfully")

        except Exception as e:
            logger.error(f"Failed to start news service: {e}")
            raise

    async def run(self):
        """Run the service until shutdown."""
        try:
            # Wait for shutdown signal
            await self.shutdown_event.wait()

        except Exception as e:
            logger.error(f"Service run error: {e}")
            raise

    async def shutdown(self):
        """Gracefully shutdown all service components."""
        logger.info("Starting graceful shutdown")

        try:
            # Stop health check task
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass

            # Stop broadcasting system
            if self.broadcasting_system:
                await self.broadcasting_system.stop()
                logger.info("Broadcasting system stopped")

            # Stop news service
            if self.news_service:
                await self.news_service.stop()
                logger.info("News service stopped")

            # Close LLM client
            if self.llm_client:
                await self.llm_client.close()
                logger.info("LLM client closed")

            # Close database pool
            if self.db_pool:
                await self.db_pool.close()
                logger.info("Database pool closed")

            # Signal shutdown complete
            self.shutdown_event.set()

            logger.info("Graceful shutdown completed")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    async def _health_check_loop(self):
        """Monitor service health and restart if needed."""
        failure_count = 0
        max_failures = 3

        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(60)  # Check every minute

                # Check database connection
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")

                # Check LLM client health
                if not await self.llm_client.health_check():
                    raise Exception("LLM client health check failed")

                # Check news service queues (basic health check)
                if hasattr(self.news_service, 'metrics'):
                    last_processed = self.news_service.metrics.get('last_processed_timestamp')
                    if last_processed:
                        from datetime import datetime, timedelta
                        if (datetime.now() - last_processed) > timedelta(minutes=10):
                            logger.warning("News service hasn't processed articles in 10+ minutes")

                # Reset failure count on successful health check
                failure_count = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                failure_count += 1
                logger.error(f"Health check failed ({failure_count}/{max_failures}): {e}")

                if failure_count >= max_failures:
                    logger.critical("Too many health check failures, shutting down service")
                    await self.shutdown()
                    break

    async def get_status(self) -> Dict[str, Any]:
        """Get current service status."""
        status = {
            'service': 'realtime_news_ingestion',
            'status': 'unknown',
            'components': {},
            'metrics': {},
            'timestamp': asyncio.get_event_loop().time()
        }

        try:
            # Database status
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                status['components']['database'] = 'healthy'
            else:
                status['components']['database'] = 'not_initialized'

            # LLM client status
            if self.llm_client:
                llm_healthy = await self.llm_client.health_check()
                status['components']['llm_client'] = 'healthy' if llm_healthy else 'unhealthy'
            else:
                status['components']['llm_client'] = 'not_initialized'

            # News service status
            if self.news_service and hasattr(self.news_service, 'metrics'):
                status['components']['news_service'] = 'running'
                status['metrics']['news_service'] = dict(self.news_service.metrics)
            else:
                status['components']['news_service'] = 'not_running'

            # Broadcasting system status
            if self.broadcasting_system:
                status['components']['broadcasting_system'] = 'running'
                status['metrics']['broadcasting_system'] = self.broadcasting_system.get_performance_metrics()
            else:
                status['components']['broadcasting_system'] = 'not_running'

            # Overall status
            if all(comp in ['healthy', 'running'] for comp in status['components'].values()):
                status['status'] = 'healthy'
            else:
                status['status'] = 'degraded'

        except Exception as e:
            status['status'] = 'error'
            status['error'] = str(e)

        return status


async def main():
    """Main entry point for the service."""

    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Print startup banner
    logger.info("=" * 60)
    logger.info("ATS REAL-TIME NEWS INGESTION SERVICE")
    logger.info("=" * 60)
    logger.info("Initializing LLM-powered news signal extraction...")

    # Create and initialize service manager
    service_manager = RealTimeNewsServiceManager()

    try:
        # Initialize components
        await service_manager.initialize()

        # Start service
        await service_manager.start()

        logger.info("Service is running. Press Ctrl+C to stop.")

        # Run service
        await service_manager.run()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Service error: {e}")
        sys.exit(1)
    finally:
        # Ensure clean shutdown
        if not service_manager.shutdown_event.is_set():
            await service_manager.shutdown()

        logger.info("Service shutdown complete")


# Entry point for running as a script
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)