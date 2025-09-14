#!/usr/bin/env python3
"""
Market Data Service Startup Script

Starts the Market Data Service with FastAPI health endpoint.
"""
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from typing import Dict, Any
import logging
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title='ATS Market Data Service', version='1.0.0')

@app.get('/health')
async def health() -> Dict[str, Any]:
    """Health check endpoint for Market Data Service"""
    try:
        # Import here to avoid import errors during startup
        from domains.market_data.services.config.market_data_service_container import get_market_data_service

        # Try to initialize the service
        service = await get_market_data_service()

        return {
            'status': 'healthy',
            'service': 'market-data',
            'version': '1.0.0',
            'environment': os.getenv('ENVIRONMENT', 'unknown')
        }
    except ImportError as e:
        logger.warning(f"Service container not available: {e}")
        return {
            'status': 'degraded',
            'service': 'market-data',
            'version': '1.0.0',
            'warning': 'Service container not available',
            'environment': os.getenv('ENVIRONMENT', 'unknown')
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail={
            'status': 'unhealthy',
            'service': 'market-data',
            'error': str(e),
            'environment': os.getenv('ENVIRONMENT', 'unknown')
        })

@app.get('/')
async def root():
    """Root endpoint"""
    return {
        'service': 'ATS Market Data Service',
        'version': '1.0.0',
        'status': 'running'
    }

if __name__ == "__main__":
    logger.info("Starting ATS Market Data Service...")
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=8000,
        log_level="info"
    )