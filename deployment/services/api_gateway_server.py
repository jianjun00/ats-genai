"""
API Gateway Server

Unified entry point for all ATS services with service discovery integration,
load balancing, and request routing.
"""

import asyncio
import logging
import os
import signal
import sys
from typing import Dict, Any

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Add src to path for imports
sys.path.insert(0, '/app/src')

from infrastructure.service_discovery import (
    ServiceDiscoveryClient,
    get_global_registry,
    RoundRobinBalancer,
    ServiceClient,
    service_client
)

# Setup logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="ATS API Gateway",
    description="Unified entry point for all ATS services",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv('CORS_ORIGINS', 'http://localhost:3000,http://localhost:3001').split(','),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global service discovery client
discovery_client = None

@app.on_event("startup")
async def startup_event():
    """Initialize service discovery on startup."""
    global discovery_client
    registry = get_global_registry()
    discovery_client = ServiceDiscoveryClient(registry)
    logger.info("API Gateway initialized successfully")
SERVICE_ROUTES = {
    "/api/v1/instruments": "instrument-service",
    "/api/v1/analytics": "analytics-service",
    "/api/v1/trading": "trading-service",
    "/api/v1/news": "news-service"
}

# Helper functions

async def discover_service_endpoint(service_name: str) -> str:
    """Discover service endpoint using service registry."""
    instances = await discovery_client.discover_service(service_name)
    if not instances:
        raise HTTPException(
            status_code=503,
            detail=f"Service {service_name} not available"
        )

    # Use round-robin load balancing
    balancer = RoundRobinBalancer()
    selected_instance = balancer.select_instance(instances)

    return selected_instance.endpoint.url
def determine_target_service(path: str) -> str:
    """Determine target service based on request path."""
    for route_prefix, service_name in SERVICE_ROUTES.items():
        if path.startswith(route_prefix):
            return service_name

    raise HTTPException(
        status_code=404,
        detail=f"No service found for path: {path}"
    )

async def proxy_request(
    request: Request,
    service_name: str,
    target_path: str
) -> Response:
    """Proxy request to target service."""
    # Get service endpoint
    async with service_client(service_name) as client:
        # Prepare request data
        method = request.method
        headers = dict(request.headers)

        # Remove hop-by-hop headers
        hop_by_hop_headers = [
            'connection', 'keep-alive', 'proxy-authenticate',
            'proxy-authorization', 'te', 'trailers', 'transfer-encoding',
            'upgrade', 'host'
        ]
        for header in hop_by_hop_headers:
            headers.pop(header, None)

        # Get request body
        body = None
        if method in ['POST', 'PUT', 'PATCH']:
            body = await request.body()

        # Make request to service
        if method == 'GET':
            response = await client.get(
                target_path,
                params=dict(request.query_params),
                headers=headers
            )
        elif method == 'POST':
            response = await client.post(
                target_path,
                data=body,
                params=dict(request.query_params),
                headers=headers
            )
        elif method == 'PUT':
            response = await client.put(
                target_path,
                data=body,
                params=dict(request.query_params),
                headers=headers
            )
        elif method == 'DELETE':
            response = await client.delete(
                target_path,
                params=dict(request.query_params),
                headers=headers
            )
        else:
            raise HTTPException(
                status_code=405,
                detail=f"Method {method} not supported"
            )

        # Get response content
        content = await response.read()

        # Prepare response headers
        response_headers = {}
        for key, value in response.headers.items():
            if key.lower() not in hop_by_hop_headers:
                response_headers[key] = value

        # Add gateway headers
        response_headers['X-Gateway'] = 'ATS-API-Gateway'
        response_headers['X-Service'] = service_name

        return Response(
            content=content,
            status_code=response.status,
            headers=response_headers,
            media_type=response.headers.get('content-type')
        )

@app.get("/health")
async def gateway_health():
    """Gateway health check."""
    # Check service discovery
    all_services = await discovery_client.registry.get_all_services()

    service_health = {}
    for service_name in SERVICE_ROUTES.values():
        instances = await discovery_client.discover_service(service_name)
        healthy_instances = [i for i in instances if i.status.value == 'healthy']
        service_health[service_name] = {
            'total_instances': len(instances),
            'healthy_instances': len(healthy_instances),
            'status': 'healthy' if healthy_instances else 'unhealthy'
        }

    overall_healthy = all(
        svc['healthy_instances'] > 0
        for svc in service_health.values()
    )

    return {
        'status': 'healthy' if overall_healthy else 'degraded',
        'gateway': 'operational',
        'services': service_health,
        'total_services': len(service_health)
    }

@app.get("/services")
async def list_services():
    """List all available services."""
    all_services = await discovery_client.registry.get_all_services()

    services_info = {}
    for service_name, instances in all_services.items():
        healthy_instances = [i for i in instances if i.status.value == 'healthy']
        services_info[service_name] = {
            'total_instances': len(instances),
            'healthy_instances': len(healthy_instances),
            'endpoints': [i.endpoint.url for i in healthy_instances],
            'versions': list(set(i.version for i in instances))
        }

    return {
        'services': services_info,
        'total_services': len(services_info),
        'routing': SERVICE_ROUTES
    }

@app.get("/")
async def root():
    """Gateway root endpoint."""
    return {
        'service': 'ATS API Gateway',
        'version': '1.0.0',
        'status': 'running',
        'endpoints': {
            'health': '/health',
            'services': '/services',
            'docs': '/docs'
        },
        'available_routes': list(SERVICE_ROUTES.keys())
    }

# Catch-all route for service proxying
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_to_service(request: Request, path: str):
    """Proxy requests to appropriate services."""
    full_path = f"/{path}"

    # Determine target service
    service_name = determine_target_service(full_path)

    # Log request
    logger.info(f"Proxying {request.method} {full_path} to {service_name}")

    # Proxy request
    return await proxy_request(request, service_name, full_path)

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    # Configuration
    host = os.getenv("GATEWAY_HOST", "0.0.0.0")
    port = int(os.getenv("GATEWAY_PORT", "8000"))

    # Wait for service registry
    import time
    logger.info("Waiting for service registry...")
    time.sleep(15)  # Give service registry time to start

    logger.info(f"Starting API Gateway on {host}:{port}")

    # Run the server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        access_log=True
    )