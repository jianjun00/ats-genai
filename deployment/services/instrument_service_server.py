"""
Instrument Service Server

Standalone server for the instrument service with service discovery integration.
Provides vendor instrument, instrument xrefs, and unified instrument APIs.
"""

import asyncio
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Add src to path for imports
sys.path.insert(0, '/app/src')

from infrastructure.service_discovery import (
    ServiceInstance,
    ServiceEndpoint,
    HealthCheck,
    service_registration_context,
    get_global_registry,
    initialize_service_registry
)
from domains.instruments.services.config.service_container import InstrumentServiceContainer
from domains.instruments.services.impl.instrument_service_health import InstrumentServiceHealthIntegration
from services.web_services.api.instruments_api import instruments_router

# Setup logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
service_container = None
health_integration = None
registry = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage service lifecycle with proper startup and shutdown."""
    global service_container, health_integration, registry

    try:
        # Initialize service registry connection
        registry = get_global_registry()

        # Initialize service container
        service_container = InstrumentServiceContainer(
            environment=os.getenv('ENVIRONMENT', 'production')
        )
        await service_container.initialize()

        # Get service implementation
        service_impl = service_container.get_instrument_service()

        # Initialize health integration
        health_integration = InstrumentServiceHealthIntegration(
            service_impl=service_impl,
            service_name=os.getenv('SERVICE_NAME', 'instrument-service'),
            service_version=os.getenv('SERVICE_VERSION', '1.0.0'),
            host=os.getenv('SERVICE_HOST', '0.0.0.0'),
            port=int(os.getenv('SERVICE_PORT', '8001'))
        )

        # Register service with service registry
        await health_integration.register_service()

        # Start health monitoring
        await health_integration.start_health_monitoring()

        logger.info("Instrument service started successfully")

        yield

    except Exception as e:
        logger.error(f"Failed to start instrument service: {e}")
        raise
    finally:
        # Cleanup
        try:
            if health_integration:
                await health_integration.deregister_service()
            if service_container:
                await service_container.shutdown()
            logger.info("Instrument service shut down successfully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

# Create FastAPI app with lifecycle management
app = FastAPI(
    title="ATS Instrument Service",
    description="Vendor instrument, instrument xrefs, and unified instrument APIs",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the instruments router
app.include_router(instruments_router)

# Service-specific health endpoint
@app.get("/health")
async def service_health():
    """Service-specific health check endpoint."""
    try:
        if health_integration:
            return await health_integration.get_health_status()
        else:
            return {
                "status": "unhealthy",
                "message": "Health integration not initialized",
                "service": "instrument-service"
            }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "message": f"Health check failed: {str(e)}",
            "service": "instrument-service"
        }

# Service info endpoint
@app.get("/info")
async def service_info():
    """Get service information."""
    return {
        "service_name": os.getenv('SERVICE_NAME', 'instrument-service'),
        "version": os.getenv('SERVICE_VERSION', '1.0.0'),
        "environment": os.getenv('ENVIRONMENT', 'production'),
        "capabilities": [
            "vendor_instruments",
            "instrument_xrefs",
            "unified_instruments",
            "symbol_resolution",
            "batch_operations"
        ],
        "endpoints": {
            "health": "/health",
            "api_docs": "/docs",
            "instruments": "/api/v1/instruments/"
        }
    }

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "ATS Instrument Service",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    # Configuration
    host = os.getenv("SERVICE_HOST", "0.0.0.0")
    port = int(os.getenv("SERVICE_PORT", "8001"))

    # Wait for dependencies
    import time
    logger.info("Waiting for dependencies...")
    time.sleep(10)  # Give dependencies time to start

    logger.info(f"Starting Instrument Service on {host}:{port}")

    # Run the server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        access_log=True
    )