"""
Service Registry Server

Standalone server providing centralized service discovery and health monitoring.
This server runs the service registry and provides REST APIs for service management.
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Dict, List, Any

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add src to path for imports
sys.path.insert(0, '/app/src')

from infrastructure.service_discovery import (
    initialize_service_registry,
    shutdown_service_registry,
    get_global_registry,
    ServiceInstance,
    ServiceEndpoint,
    ServiceStatus,
    HealthCheck as ServiceHealthCheck
)

# Setup logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="ATS Service Registry",
    description="Centralized service discovery and health monitoring",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global registry instance
registry = None

# Request/Response models
class ServiceRegistrationRequest(BaseModel):
    service_name: str
    instance_id: str
    version: str
    host: str
    port: int
    protocol: str = "http"
    path: str = "/"
    metadata: Dict[str, Any] = {}
    health_check_endpoint: str = "/health"
    health_check_interval: int = 30
    health_check_timeout: int = 5

class ServiceInstanceResponse(BaseModel):
    service_name: str
    instance_id: str
    version: str
    endpoint: Dict[str, Any]
    metadata: Dict[str, Any]
    status: str
    last_heartbeat: str = None
    registration_time: str

class ServiceListResponse(BaseModel):
    services: Dict[str, List[ServiceInstanceResponse]]
    total_services: int
    total_instances: int

class HealthResponse(BaseModel):
    status: str
    message: str
    timestamp: str
    uptime_seconds: float
    registered_services: int
    total_instances: int

# Global state
start_time = datetime.utcnow()

@app.on_event("startup")
async def startup_event():
    """Initialize service registry on startup."""
    global registry
    try:
        registry = await initialize_service_registry()
        logger.info("Service registry initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize service registry: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    try:
        await shutdown_service_registry()
        logger.info("Service registry shut down successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# API Endpoints

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        all_services = await registry.get_all_services()
        total_instances = sum(len(instances) for instances in all_services.values())
        
        uptime = (datetime.utcnow() - start_time).total_seconds()
        
        return HealthResponse(
            status="healthy",
            message="Service registry is operational",
            timestamp=datetime.utcnow().isoformat(),
            uptime_seconds=uptime,
            registered_services=len(all_services),
            total_instances=total_instances
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

@app.post("/services/register")
async def register_service(request: ServiceRegistrationRequest):
    """Register a new service instance."""
    try:
        service_instance = ServiceInstance(
            service_name=request.service_name,
            instance_id=request.instance_id,
            version=request.version,
            endpoint=ServiceEndpoint(
                host=request.host,
                port=request.port,
                protocol=request.protocol,
                path=request.path
            ),
            metadata=request.metadata,
            health_check=ServiceHealthCheck(
                endpoint=request.health_check_endpoint,
                interval_seconds=request.health_check_interval,
                timeout_seconds=request.health_check_timeout
            )
        )
        
        success = await registry.register_service(service_instance)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to register service")
        
        logger.info(f"Registered service: {request.service_name}:{request.instance_id}")
        return {"message": "Service registered successfully", "instance_id": request.instance_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering service: {e}")
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@app.delete("/services/{service_name}/instances/{instance_id}")
async def deregister_service(service_name: str, instance_id: str):
    """Deregister a service instance."""
    try:
        success = await registry.deregister_service(service_name, instance_id)
        if not success:
            raise HTTPException(status_code=404, detail="Service instance not found")
        
        logger.info(f"Deregistered service: {service_name}:{instance_id}")
        return {"message": "Service deregistered successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deregistering service: {e}")
        raise HTTPException(status_code=500, detail=f"Deregistration failed: {str(e)}")

@app.get("/services", response_model=ServiceListResponse)
async def list_all_services():
    """List all registered services."""
    try:
        all_services = await registry.get_all_services()
        
        services_response = {}
        total_instances = 0
        
        for service_name, instances in all_services.items():
            instance_responses = []
            for instance in instances:
                instance_responses.append(ServiceInstanceResponse(
                    service_name=instance.service_name,
                    instance_id=instance.instance_id,
                    version=instance.version,
                    endpoint={
                        "host": instance.endpoint.host,
                        "port": instance.endpoint.port,
                        "protocol": instance.endpoint.protocol,
                        "path": instance.endpoint.path,
                        "url": instance.endpoint.url
                    },
                    metadata=instance.metadata,
                    status=instance.status.value,
                    last_heartbeat=instance.last_heartbeat.isoformat() if instance.last_heartbeat else None,
                    registration_time=instance.registration_time.isoformat()
                ))
                total_instances += 1
            
            services_response[service_name] = instance_responses
        
        return ServiceListResponse(
            services=services_response,
            total_services=len(all_services),
            total_instances=total_instances
        )
        
    except Exception as e:
        logger.error(f"Error listing services: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list services: {str(e)}")

@app.get("/services/{service_name}")
async def get_service_instances(service_name: str):
    """Get all instances of a specific service."""
    try:
        instances = await registry.get_service_instances(service_name)
        
        instance_responses = []
        for instance in instances:
            instance_responses.append(ServiceInstanceResponse(
                service_name=instance.service_name,
                instance_id=instance.instance_id,
                version=instance.version,
                endpoint={
                    "host": instance.endpoint.host,
                    "port": instance.endpoint.port,
                    "protocol": instance.endpoint.protocol,
                    "path": instance.endpoint.path,
                    "url": instance.endpoint.url
                },
                metadata=instance.metadata,
                status=instance.status.value,
                last_heartbeat=instance.last_heartbeat.isoformat() if instance.last_heartbeat else None,
                registration_time=instance.registration_time.isoformat()
            ))
        
        return {
            "service_name": service_name,
            "instances": instance_responses,
            "instance_count": len(instance_responses)
        }
        
    except Exception as e:
        logger.error(f"Error getting service instances: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get instances: {str(e)}")

@app.post("/services/{service_name}/instances/{instance_id}/heartbeat")
async def update_heartbeat(service_name: str, instance_id: str):
    """Update service instance heartbeat."""
    try:
        success = await registry.heartbeat(service_name, instance_id)
        if not success:
            raise HTTPException(status_code=404, detail="Service instance not found")
        
        return {"message": "Heartbeat updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating heartbeat: {e}")
        raise HTTPException(status_code=500, detail=f"Heartbeat update failed: {str(e)}")

@app.put("/services/{service_name}/instances/{instance_id}/status")
async def update_service_status(service_name: str, instance_id: str, status: str):
    """Update service instance status."""
    try:
        # Validate status
        try:
            service_status = ServiceStatus(status.lower())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
        
        success = await registry.update_health_status(service_name, instance_id, service_status)
        if not success:
            raise HTTPException(status_code=404, detail="Service instance not found")
        
        logger.info(f"Updated status for {service_name}:{instance_id} to {status}")
        return {"message": f"Status updated to {status}"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating status: {e}")
        raise HTTPException(status_code=500, detail=f"Status update failed: {str(e)}")

@app.get("/metrics")
async def get_metrics():
    """Get service registry metrics."""
    try:
        all_services = await registry.get_all_services()
        
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": (datetime.utcnow() - start_time).total_seconds(),
            "total_services": len(all_services),
            "total_instances": sum(len(instances) for instances in all_services.values()),
            "services": {}
        }
        
        for service_name, instances in all_services.items():
            status_counts = {}
            for instance in instances:
                status = instance.status.value
                status_counts[status] = status_counts.get(status, 0) + 1
            
            metrics["services"][service_name] = {
                "instance_count": len(instances),
                "status_distribution": status_counts
            }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    # Configuration
    host = os.getenv("REGISTRY_HOST", "0.0.0.0")
    port = int(os.getenv("REGISTRY_PORT", "8500"))
    
    logger.info(f"Starting Service Registry on {host}:{port}")
    
    # Run the server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        access_log=True
    )