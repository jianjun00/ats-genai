#!/usr/bin/env python3
"""
Monitoring Dashboard Startup Script

Starts the Service Monitoring Dashboard with FastAPI.
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

app = FastAPI(title='ATS Service Monitoring Dashboard', version='1.0.0')

@app.get('/health')
async def health() -> Dict[str, Any]:
    """Health check endpoint for Monitoring Dashboard"""
    try:
        # Import here to avoid import errors during startup
        from infrastructure.monitoring.service_metrics import get_global_metrics_collector
        
        # Try to get metrics collector
        metrics_collector = get_global_metrics_collector()
        
        return {
            'status': 'healthy',
            'service': 'monitoring-dashboard',
            'version': '1.0.0',
            'environment': os.getenv('ENVIRONMENT', 'unknown'),
            'metrics_available': metrics_collector is not None
        }
    except ImportError as e:
        logger.warning(f"Monitoring infrastructure not available: {e}")
        return {
            'status': 'degraded',
            'service': 'monitoring-dashboard',
            'version': '1.0.0',
            'warning': 'Monitoring infrastructure not available',
            'environment': os.getenv('ENVIRONMENT', 'unknown')
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail={
            'status': 'unhealthy',
            'service': 'monitoring-dashboard',
            'error': str(e),
            'environment': os.getenv('ENVIRONMENT', 'unknown')
        })

@app.get('/')
async def root():
    """Root endpoint"""
    return {
        'service': 'ATS Service Monitoring Dashboard',
        'version': '1.0.0',
        'status': 'running'
    }

@app.get('/metrics')
async def get_metrics():
    """Get system metrics"""
    try:
        from infrastructure.monitoring.service_metrics import get_global_metrics_collector
        metrics_collector = get_global_metrics_collector()
        
        # Return basic metrics
        return {
            'status': 'success',
            'timestamp': asyncio.get_event_loop().time(),
            'services': {
                'redis': 'unknown',
                'postgres': 'unknown',
                'monitoring': 'healthy'
            }
        }
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }

if __name__ == "__main__":
    logger.info("Starting ATS Service Monitoring Dashboard...")
    uvicorn.run(
        app, 
        host='0.0.0.0', 
        port=8000,
        log_level="info"
    )