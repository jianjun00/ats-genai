#!/usr/bin/env python3
"""
ATS Unified Analytics App Service
Consolidates portfolio analytics, data collection monitoring, and system health
"""

import gin
import asyncio
import logging
import os
import aiohttp
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union
from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from dataclasses import dataclass, asdict
import json
import redis.asyncio as redis
from pathlib import Path

# Reuse existing ATS framework
from config.environment import Environment
from core.logging.logger_config import get_logger

# Import existing analytics engine
from analytics.portfolio_analytics import (
    PortfolioAnalyticsEngine,
    PortfolioMetrics,
    AttributionMetrics,
    ModelPerformanceMetrics,
    DrillDownAnalysis
)

# Import components
from services.universe_analytics_service import UniverseAnalyticsService

# Optional type-aware analytics - disable if causing issues
try:
    from services.analytics_service_class import AnalyticsService
    from api.type_aware_analytics_api import create_type_aware_analytics_router
    from schema.registry import schema_registry
    TYPE_AWARE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Type-aware analytics not available: {e}")
    TYPE_AWARE_AVAILABLE = False

# Configure Gin
try:
    gin.parse_config_file('config/app_dev.gin')
except Exception as e:
    print(f'[WARN] Could not parse gin config: {e}')

logger = get_logger(__name__)

@dataclass
class ServiceHealth:
    """Health status of individual services"""
    service_name: str
    status: str  # "healthy", "degraded", "unhealthy"
    response_time_ms: float
    last_check: datetime
    details: Dict[str, Any]

@dataclass
class SystemStatus:
    """Overall system status"""
    overall_status: str
    services: List[ServiceHealth]
    data_quality_score: float
    active_collections: int
    cache_hit_rate: float
    timestamp: datetime

@dataclass
class DataQualityReport:
    """Data quality assessment across vendors"""
    vendor_scores: Dict[str, float]
    gap_analysis: Dict[str, List[str]]
    freshness_report: Dict[str, datetime]
    completeness_metrics: Dict[str, float]
    anomaly_detection: Dict[str, List[Dict]]

class ConnectionManager:
    """WebSocket connection manager for real-time updates"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connection established. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket connection closed. Total: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Failed to send personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: str):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Failed to broadcast to connection: {e}")
                disconnected.append(connection)
        
        # Remove disconnected connections
        for conn in disconnected:
            if conn in self.active_connections:
                self.active_connections.remove(conn)

# FastAPI Application - Unified Analytics Service
app = FastAPI(
    title="ATS Unified Analytics App",
    description="Consolidated analytics platform integrating portfolio analysis and data collection monitoring",
    version="3.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include type-aware analytics router (if available)
if TYPE_AWARE_AVAILABLE:
    try:
        type_aware_router = create_type_aware_analytics_router()
        app.include_router(type_aware_router)
        logger.info("✅ Type-aware analytics router added")
    except Exception as e:
        logger.warning(f"Failed to add type-aware router: {e}")
else:
    logger.info("⏭️ Skipping type-aware analytics router")

# Include datasets EDA router for table analysis
try:
    from api.datasets_api import router as datasets_router
    app.include_router(datasets_router, prefix="/api/v1/datasets", tags=["datasets"])
    logger.info("✅ Datasets EDA API routes added")
except ImportError as e:
    logger.warning(f"⚠️  Datasets API not available: {e}")

# Include training dataset EDA router
try:
    from ml.training_data.apis.training_dataset_api import create_training_dataset_router
    training_dataset_router = create_training_dataset_router()
    app.include_router(training_dataset_router, tags=["training-datasets"])
    logger.info("✅ Training dataset EDA API routes added")
except ImportError as e:
    logger.warning(f"⚠️  Training dataset API not available: {e}")

class UnifiedAnalyticsService:
    """Main unified analytics service orchestrator"""
    
    def __init__(self):
        self.env = Environment(gin_config_path='config/app_dev.gin')
        self.analytics_engine = PortfolioAnalyticsEngine(self.env)
        self.redis_client = None
        self.connection_manager = ConnectionManager()
        self.type_aware_analytics = None  # Will be initialized on startup
        self.universe_service = None      # Will be initialized on startup
        
        # Service endpoints for health monitoring
        self.service_endpoints = {
            "minute-service": "http://localhost:8081/health",
            "eod-service": "http://localhost:8082/health", 
            "database": None,  # Special handling for DB
            "redis": None,     # Special handling for Redis
        }
        
        self.system_metrics = {
            "total_requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "active_backtests": 0,
            "data_collections": 0,
            "errors": 0,
            "startup_time": datetime.now()
        }
        
        self.logger = get_logger(__name__)
    
    async def initialize(self):
        """Initialize all components"""
        # Initialize analytics engine
        await self.analytics_engine.initialize()
        
        # Initialize Redis
        try:
            self.redis_client = redis.Redis.from_url(
                "redis://localhost:6379",
                decode_responses=True
            )
            await self.redis_client.ping()
            self.logger.info("Redis connection established")
        except Exception as e:
            self.logger.warning(f"Redis connection failed: {e}")
        
        # Initialize type-aware analytics service (if available)
        if TYPE_AWARE_AVAILABLE:
            try:
                db_manager = self.env.get_database()
                self.type_aware_analytics = AnalyticsService(db_manager)
                self.logger.info("Type-aware analytics service initialized")
                self.logger.info(f"Schema registry loaded with {len(schema_registry.get_schema_summary()['entities'])} entities")
            except Exception as e:
                self.logger.warning(f"Failed to initialize type-aware analytics: {e}")
                self.type_aware_analytics = None
        else:
            self.logger.info("⏭️ Skipping type-aware analytics initialization")
            self.type_aware_analytics = None
        
        # Initialize universe analytics service
        try:
            self.universe_service = UniverseAnalyticsService(self.env)
            await self.universe_service.initialize()
            self.logger.info("Universe analytics service initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize universe analytics: {e}")
            raise
        
        self.logger.info("Unified analytics service initialized with type system")
    
    async def close(self):
        """Clean up connections"""
        await self.analytics_engine.close()
        if self.redis_client:
            await self.redis_client.close()
        if self.universe_service:
            await self.universe_service.close()
    
    async def check_system_health(self) -> SystemStatus:
        """Comprehensive system health check"""
        service_healths = []
        
        # Check data services
        for service_name, endpoint in self.service_endpoints.items():
            if endpoint:
                health = await self._check_service_health(service_name, endpoint)
            else:
                # Special handling for infrastructure
                if service_name == "database":
                    health = await self._check_database_health()
                elif service_name == "redis":
                    health = await self._check_redis_health()
                else:
                    continue
            
            service_healths.append(health)
        
        # Determine overall status
        healthy_services = sum(1 for s in service_healths if s.status == "healthy")
        total_services = len(service_healths)
        
        if healthy_services == total_services:
            overall_status = "healthy"
        elif healthy_services >= total_services * 0.5:
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"
        
        # Calculate data quality score (simplified)
        data_quality_score = 0.85  # Placeholder - would implement real calculation
        
        # Cache metrics
        cache_hits = self.system_metrics.get("cache_hits", 0)
        total_requests = cache_hits + self.system_metrics.get("cache_misses", 0)
        cache_hit_rate = cache_hits / total_requests if total_requests > 0 else 0.0
        
        return SystemStatus(
            overall_status=overall_status,
            services=service_healths,
            data_quality_score=data_quality_score,
            active_collections=self.system_metrics.get("data_collections", 0),
            cache_hit_rate=cache_hit_rate,
            timestamp=datetime.now()
        )
    
    async def _check_service_health(self, service_name: str, endpoint: str) -> ServiceHealth:
        """Check health of external service"""
        start_time = datetime.now()
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    response_time = (datetime.now() - start_time).total_seconds() * 1000
                    
                    if response.status == 200:
                        data = await response.json()
                        return ServiceHealth(
                            service_name=service_name,
                            status="healthy",
                            response_time_ms=response_time,
                            last_check=datetime.now(),
                            details=data
                        )
                    else:
                        return ServiceHealth(
                            service_name=service_name,
                            status="unhealthy",
                            response_time_ms=response_time,
                            last_check=datetime.now(),
                            details={"error": f"HTTP {response.status}"}
                        )
        
        except asyncio.TimeoutError:
            return ServiceHealth(
                service_name=service_name,
                status="unhealthy",
                response_time_ms=5000,
                last_check=datetime.now(),
                details={"error": "Timeout"}
            )
        except Exception as e:
            return ServiceHealth(
                service_name=service_name,
                status="unhealthy",
                response_time_ms=-1,
                last_check=datetime.now(),
                details={"error": str(e)}
            )
    
    async def _check_database_health(self) -> ServiceHealth:
        """Check database connectivity"""
        start_time = datetime.now()
        
        try:
            # Use the analytics engine's database pool
            if self.analytics_engine.db_pool:
                async with self.analytics_engine.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                
                response_time = (datetime.now() - start_time).total_seconds() * 1000
                return ServiceHealth(
                    service_name="database",
                    status="healthy",
                    response_time_ms=response_time,
                    last_check=datetime.now(),
                    details={"connection_pool": "active"}
                )
            else:
                return ServiceHealth(
                    service_name="database",
                    status="unhealthy",
                    response_time_ms=-1,
                    last_check=datetime.now(),
                    details={"error": "No connection pool"}
                )
        
        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            return ServiceHealth(
                service_name="database",
                status="unhealthy",
                response_time_ms=response_time,
                last_check=datetime.now(),
                details={"error": str(e)}
            )
    
    async def _check_redis_health(self) -> ServiceHealth:
        """Check Redis connectivity"""
        start_time = datetime.now()
        
        try:
            if self.redis_client:
                await self.redis_client.ping()
                response_time = (datetime.now() - start_time).total_seconds() * 1000
                
                return ServiceHealth(
                    service_name="redis",
                    status="healthy",
                    response_time_ms=response_time,
                    last_check=datetime.now(),
                    details={"connection": "active"}
                )
            else:
                return ServiceHealth(
                    service_name="redis",
                    status="unhealthy",
                    response_time_ms=-1,
                    last_check=datetime.now(),
                    details={"error": "No connection"}
                )
        
        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            return ServiceHealth(
                service_name="redis",
                status="unhealthy",
                response_time_ms=response_time,
                last_check=datetime.now(),
                details={"error": str(e)}
            )
    
    async def generate_data_quality_report(self) -> DataQualityReport:
        """Generate comprehensive data quality report"""
        # This would query the database for actual data quality metrics
        # For now, providing a realistic example
        
        vendor_scores = {
            "polygon": 0.92,
            "tiingo": 0.88,
            "fmp": 0.85
        }
        
        gap_analysis = {
            "polygon": ["2024-08-15", "2024-08-16"],  # Missing dates
            "tiingo": [],
            "fmp": ["2024-08-18"]
        }
        
        freshness_report = {
            "polygon": datetime.now() - timedelta(minutes=5),
            "tiingo": datetime.now() - timedelta(minutes=15),
            "fmp": datetime.now() - timedelta(hours=1)
        }
        
        completeness_metrics = {
            "minute_data": 0.95,
            "eod_data": 0.98,
            "market_cap": 0.85,
            "volume": 0.99
        }
        
        anomaly_detection = {
            "price_spikes": [
                {"symbol": "NVDA", "date": "2024-08-20", "severity": "medium"},
                {"symbol": "TSLA", "date": "2024-08-19", "severity": "low"}
            ],
            "volume_anomalies": []
        }
        
        return DataQualityReport(
            vendor_scores=vendor_scores,
            gap_analysis=gap_analysis,
            freshness_report=freshness_report,
            completeness_metrics=completeness_metrics,
            anomaly_detection=anomaly_detection
        )
    
    async def trigger_data_collection(self, service: str, params: Dict) -> Dict:
        """Trigger data collection on remote services"""
        service_map = {
            "minute": "http://localhost:8081/collect",
            "eod": "http://localhost:8082/collect"
        }
        
        if service not in service_map:
            raise ValueError(f"Unknown service: {service}")
        
        endpoint = service_map[service]
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=params) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.system_metrics["data_collections"] += 1
                        
                        # Broadcast update to WebSocket clients
                        await self.connection_manager.broadcast(json.dumps({
                            "type": "collection_triggered",
                            "service": service,
                            "params": params,
                            "timestamp": datetime.now().isoformat()
                        }))
                        
                        return {"success": True, "result": result}
                    else:
                        error_text = await response.text()
                        return {"success": False, "error": f"HTTP {response.status}: {error_text}"}
        
        except Exception as e:
            self.system_metrics["errors"] += 1
            return {"success": False, "error": str(e)}

# Global service instance
service = UnifiedAnalyticsService()

@app.on_event("startup")
async def startup():
    """Service startup"""
    logger.info("Starting ATS Unified Analytics App")
    await service.initialize()
    logger.info("Unified Analytics App started successfully")

@app.on_event("shutdown")
async def shutdown():
    """Service shutdown"""
    logger.info("Shutting down Unified Analytics App")
    await service.close()

# Core API Endpoints
@app.get("/health")
async def health():
    """Comprehensive system health check"""
    system_status = await service.check_system_health()
    service.system_metrics["total_requests"] += 1
    
    return {
        "status": system_status.overall_status,
        "services": [asdict(s) for s in system_status.services],
        "data_quality_score": system_status.data_quality_score,
        "active_collections": system_status.active_collections,
        "cache_hit_rate": system_status.cache_hit_rate,
        "system_metrics": service.system_metrics,
        "timestamp": system_status.timestamp.isoformat()
    }

@app.get("/metrics")
async def get_metrics():
    """System metrics endpoint"""
    service.system_metrics["total_requests"] += 1
    return service.system_metrics

# Portfolio Analytics Endpoints (delegate to existing engine)
@app.get("/analytics/portfolio/{backtest_run_id}/metrics")
async def get_portfolio_metrics(
    backtest_run_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    benchmark_run_id: Optional[str] = None
):
    """Get comprehensive portfolio metrics"""
    service.system_metrics["total_requests"] += 1
    
    try:
        start_dt = date.fromisoformat(start_date) if start_date else None
        end_dt = date.fromisoformat(end_date) if end_date else None
        
        metrics = await service.analytics_engine.compute_portfolio_metrics(
            backtest_run_id, start_dt, end_dt, benchmark_run_id
        )
        
        return metrics.to_dict()
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/portfolio/{backtest_run_id}/attribution")
async def get_attribution_analysis(
    backtest_run_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get performance attribution analysis"""
    service.system_metrics["total_requests"] += 1
    
    try:
        start_dt = date.fromisoformat(start_date) if start_date else None
        end_dt = date.fromisoformat(end_date) if end_date else None
        
        attribution = await service.analytics_engine.compute_attribution_analysis(
            backtest_run_id, start_dt, end_dt
        )
        
        return attribution.to_dict()
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/portfolio/{backtest_run_id}/model-performance")
async def get_model_performance(
    backtest_run_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get model performance metrics"""
    service.system_metrics["total_requests"] += 1
    
    try:
        start_dt = date.fromisoformat(start_date) if start_date else None
        end_dt = date.fromisoformat(end_date) if end_date else None
        
        model_perf = await service.analytics_engine.compute_model_performance(
            backtest_run_id, start_dt, end_dt
        )
        
        return model_perf.to_dict()
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/portfolio/{backtest_run_id}/drill-down")
async def get_drill_down_analysis(
    backtest_run_id: str,
    analysis_type: str,
    analysis_target: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get detailed drill-down analysis"""
    service.system_metrics["total_requests"] += 1
    
    try:
        start_dt = date.fromisoformat(start_date) if start_date else None
        end_dt = date.fromisoformat(end_date) if end_date else None
        
        drill_down = await service.analytics_engine.drill_down_analysis(
            backtest_run_id, analysis_type, analysis_target, start_dt, end_dt
        )
        
        return drill_down.to_dict()
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

# Universe Analytics Endpoints
@app.get("/analytics/universes")
async def get_all_universes():
    """Get list of all available universes with summary statistics"""
    service.system_metrics["total_requests"] += 1
    
    try:
        universes = await service.universe_service.get_all_universes()
        return {
            "status": "success",
            "universes": universes,
            "count": len(universes)
        }
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/universe/{universe_id}/metrics")
async def get_universe_metrics(universe_id: int, as_of_date: Optional[str] = None):
    """Get comprehensive metrics for a universe as of a specific date"""
    service.system_metrics["total_requests"] += 1
    
    try:
        # Default to today if no date specified
        target_date = date.fromisoformat(as_of_date) if as_of_date else date.today()
        
        metrics = await service.universe_service.get_universe_metrics(universe_id, target_date)
        
        return {
            "status": "success",
            "metrics": asdict(metrics)
        }
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/universe/{universe_id}/membership")
async def get_universe_membership(
    universe_id: int, 
    as_of_date: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """Get paginated membership table for a universe as of a specific date"""
    service.system_metrics["total_requests"] += 1
    
    try:
        target_date = date.fromisoformat(as_of_date) if as_of_date else date.today()
        
        membership = await service.universe_service.get_membership_table(
            universe_id, target_date, limit, offset
        )
        
        return {
            "status": "success",
            "membership": membership,
            "as_of_date": target_date.isoformat(),
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/universe/{universe_id}/scatter")
async def get_qualification_scatter(universe_id: int, as_of_date: Optional[str] = None):
    """Get market cap vs volume scatter plot data for universe members"""
    service.system_metrics["total_requests"] += 1
    
    try:
        target_date = date.fromisoformat(as_of_date) if as_of_date else date.today()
        
        scatter_data = await service.universe_service.get_qualification_scatter_data(
            universe_id, target_date
        )
        
        return {
            "status": "success",
            "scatter_data": scatter_data,
            "as_of_date": target_date.isoformat()
        }
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/universe/{universe_id}/timeseries")
async def get_universe_timeseries(
    universe_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get time series data for universe size and composition"""
    service.system_metrics["total_requests"] += 1
    
    try:
        # Default to last 30 days if no dates specified
        end_dt = date.fromisoformat(end_date) if end_date else date.today()
        start_dt = date.fromisoformat(start_date) if start_date else end_dt - timedelta(days=30)
        
        timeseries = await service.universe_service.get_universe_time_series(
            universe_id, start_dt, end_dt
        )
        
        return {
            "status": "success",
            "timeseries": asdict(timeseries),
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat()
        }
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/universe/{universe_id}/warnings")
async def get_universe_warnings(universe_id: int):
    """Get current warning indicators for universe members"""
    service.system_metrics["total_requests"] += 1
    
    try:
        warnings = await service.universe_service.get_universe_warnings(universe_id)
        
        return {
            "status": "success", 
            "warnings": warnings,
            "count": len(warnings)
        }
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

# Data Quality and Monitoring Endpoints
@app.get("/data-quality/report")
async def get_data_quality_report():
    """Get comprehensive data quality report"""
    service.system_metrics["total_requests"] += 1
    
    try:
        report = await service.generate_data_quality_report()
        
        # Convert datetime objects for JSON serialization
        freshness = {k: v.isoformat() for k, v in report.freshness_report.items()}
        
        return {
            "vendor_scores": report.vendor_scores,
            "gap_analysis": report.gap_analysis,
            "freshness_report": freshness,
            "completeness_metrics": report.completeness_metrics,
            "anomaly_detection": report.anomaly_detection,
            "generated_at": datetime.now().isoformat()
        }
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

# Data Collection Control Endpoints
@app.post("/collection/trigger")
async def trigger_collection(
    background_tasks: BackgroundTasks,
    service_name: str,
    symbols: Optional[str] = None,
    vendors: Optional[str] = None,
    days_back: int = 5
):
    """Trigger data collection on specific service"""
    service.system_metrics["total_requests"] += 1
    
    params = {
        "symbols": symbols,
        "vendors": vendors,
        "days_back": days_back
    }
    
    if service_name not in ["minute", "eod"]:
        raise HTTPException(status_code=400, detail="Invalid service name")
    
    try:
        result = await service.trigger_data_collection(service_name, params)
        return result
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time system updates"""
    await service.connection_manager.connect(websocket)
    
    try:
        # Send initial system status
        system_status = await service.check_system_health()
        await service.connection_manager.send_personal_message(
            json.dumps({
                "type": "system_status",
                "data": asdict(system_status),
                "timestamp": datetime.now().isoformat()
            }), websocket
        )
        
        # Keep connection alive and handle messages
        while True:
            try:
                # Wait for client messages (ping/pong, requests)
                message = await websocket.receive_text()
                data = json.loads(message)
                
                if data.get("type") == "ping":
                    await service.connection_manager.send_personal_message(
                        json.dumps({"type": "pong", "timestamp": datetime.now().isoformat()}),
                        websocket
                    )
                elif data.get("type") == "request_status":
                    system_status = await service.check_system_health()
                    await service.connection_manager.send_personal_message(
                        json.dumps({
                            "type": "system_status",
                            "data": asdict(system_status),
                            "timestamp": datetime.now().isoformat()
                        }), websocket
                    )
            
            except asyncio.TimeoutError:
                # Send periodic heartbeat
                await service.connection_manager.send_personal_message(
                    json.dumps({"type": "heartbeat", "timestamp": datetime.now().isoformat()}),
                    websocket
                )
                continue
    
    except WebSocketDisconnect:
        service.connection_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        service.connection_manager.disconnect(websocket)

# Cache management endpoints
@app.post("/cache/invalidate")
async def invalidate_cache(pattern: Optional[str] = None):
    """Invalidate analytics cache"""
    service.system_metrics["total_requests"] += 1
    
    try:
        await service.analytics_engine.invalidate_cache(pattern)
        return {"success": True, "message": "Cache invalidated"}
    
    except Exception as e:
        service.system_metrics["errors"] += 1
        raise HTTPException(status_code=500, detail=str(e))

# Root endpoint with comprehensive service information
@app.get("/")
async def root():
    """Root endpoint with unified analytics app information"""
    service.system_metrics["total_requests"] += 1
    
    return {
        "service": "ATS Unified Analytics App",
        "version": "3.0.0",
        "description": "Consolidated analytics platform integrating portfolio analysis and data collection monitoring",
        "features": [
            "Portfolio performance analytics",
            "Attribution analysis",
            "Model performance tracking",
            "Real-time system monitoring",
            "Data quality assessment",
            "Multi-service orchestration",
            "WebSocket real-time updates",
            "Comprehensive health checks"
        ],
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics", 
            "portfolio_analytics": "/analytics/portfolio/{backtest_run_id}/metrics",
            "data_quality": "/data-quality/report",
            "websocket": "/ws",
            "collection_trigger": "/collection/trigger"
        },
        "integrated_services": list(service.service_endpoints.keys()),
        "uptime_seconds": (datetime.now() - service.system_metrics["startup_time"]).total_seconds()
    }

# Dataset detail page for enhanced EDA
@app.get("/dataset-detail", response_class=HTMLResponse)
async def dataset_detail_page():
    """Serve the enhanced dataset detail page"""
    try:
        # Read the HTML file
        html_file_path = Path(__file__).parent.parent.parent.parent / "dataset_detail_page_frontend.html"
        if html_file_path.exists():
            with open(html_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            return HTMLResponse(content="<h1>Dataset Detail Page Not Found</h1><p>HTML file not found at expected location.</p>", status_code=404)
    except Exception as e:
        logger.error(f"Error serving dataset detail page: {e}")
        return HTMLResponse(content=f"<h1>Error</h1><p>Error loading dataset detail page: {e}</p>", status_code=500)

@app.get("/dual_axis_ohlc_chart.js")
async def serve_chart_js():
    """Serve the dual axis OHLC chart JavaScript file"""
    try:
        js_file_path = Path(__file__).parent.parent.parent.parent / "dual_axis_ohlc_chart.js"
        if js_file_path.exists():
            return FileResponse(js_file_path, media_type='application/javascript')
        else:
            raise HTTPException(status_code=404, detail="Chart JavaScript file not found")
    except Exception as e:
        logger.error(f"Error serving chart JS file: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading chart JS file: {e}")

# Simple HTML dashboard for development
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Simple HTML dashboard for development"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ATS Unified Analytics Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .status-healthy { color: green; }
            .status-unhealthy { color: red; }
            .status-degraded { color: orange; }
            .metric-card { 
                border: 1px solid #ddd; 
                border-radius: 5px; 
                padding: 15px; 
                margin: 10px 0; 
                background: #f9f9f9; 
            }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        </style>
    </head>
    <body>
        <h1>🚀 ATS Unified Analytics Dashboard</h1>
        <div id="content">
            <div class="metric-card">
                <h3>📊 System Status</h3>
                <p>Loading system status...</p>
            </div>
            <div class="metric-card">
                <h3>📈 Real-time Updates</h3>
                <p id="websocket-status">Connecting to WebSocket...</p>
                <div id="real-time-updates"></div>
            </div>
            <div class="metric-card">
                <h3>🌌 Universe Analytics</h3>
                <p>Interactive universe membership analysis with qualification metrics, time series, and scatter plots.</p>
                <a href="/universe" style="color: #667eea; text-decoration: none; font-weight: 600;">→ Open Universe Dashboard</a>
            </div>
        </div>
        
        <script>
            // Fetch initial system status
            fetch('/health')
                .then(response => response.json())
                .then(data => {
                    const statusElement = document.querySelector('.metric-card p');
                    statusElement.innerHTML = `
                        <strong>Overall Status:</strong> <span class="status-${data.status}">${data.status.toUpperCase()}</span><br>
                        <strong>Data Quality:</strong> ${(data.data_quality_score * 100).toFixed(1)}%<br>
                        <strong>Active Collections:</strong> ${data.active_collections}<br>
                        <strong>Cache Hit Rate:</strong> ${(data.cache_hit_rate * 100).toFixed(1)}%
                    `;
                });
            
            // WebSocket connection for real-time updates
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = function() {
                document.getElementById('websocket-status').textContent = '🔗 Connected';
            };
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                const updatesDiv = document.getElementById('real-time-updates');
                updatesDiv.innerHTML += `<p><strong>${data.type}:</strong> ${new Date().toLocaleTimeString()}</p>`;
                
                // Keep only last 10 updates
                const updates = updatesDiv.querySelectorAll('p');
                if (updates.length > 10) {
                    updates[0].remove();
                }
            };
            
            ws.onerror = function() {
                document.getElementById('websocket-status').textContent = '❌ Connection Error';
            };
            
            ws.onclose = function() {
                document.getElementById('websocket-status').textContent = '🔌 Disconnected';
            };
            
            // Send ping every 30 seconds
            setInterval(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({type: 'ping'}));
                }
            }, 30000);
        </script>
    </body>
    </html>
    """
    return html_content

@app.get("/universe", response_class=HTMLResponse)
async def universe_dashboard():
    """Interactive universe analytics dashboard"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ATS Universe Analytics Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/date-fns@2.29.3/index.min.js"></script>
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                margin: 0; 
                background: #f5f5f5;
            }
            .header { 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                color: white; 
                padding: 20px; 
                text-align: center;
            }
            .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
            .controls { 
                background: white; 
                padding: 20px; 
                border-radius: 8px; 
                margin-bottom: 20px; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
            .chart-container { 
                background: white; 
                padding: 20px; 
                border-radius: 8px; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .full-width { grid-column: 1 / -1; }
            .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
            .metric-card { 
                background: white; 
                padding: 20px; 
                border-radius: 8px; 
                text-align: center; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                border-left: 4px solid #667eea;
            }
            .metric-value { font-size: 2em; font-weight: bold; color: #333; margin-bottom: 5px; }
            .metric-label { color: #666; font-size: 0.9em; }
            select, input { 
                padding: 8px 12px; 
                border: 1px solid #ddd; 
                border-radius: 4px; 
                margin: 0 10px 0 5px;
                font-size: 14px;
            }
            button { 
                padding: 8px 16px; 
                background: #667eea; 
                color: white; 
                border: none; 
                border-radius: 4px; 
                cursor: pointer;
                font-size: 14px;
            }
            button:hover { background: #5a67d8; }
            .table-container { 
                background: white; 
                border-radius: 8px; 
                overflow: hidden; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
            th { background: #f8f9fa; font-weight: 600; }
            .status-active { color: #28a745; font-weight: 600; }
            .status-inactive { color: #dc3545; }
            .loading { text-align: center; padding: 40px; color: #666; }
            .warning-badge { 
                background: #ffeaa7; 
                color: #d63031; 
                padding: 4px 8px; 
                border-radius: 12px; 
                font-size: 0.8em; 
                font-weight: 600;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🌌 Universe Analytics Dashboard</h1>
            <p>Comprehensive universe membership analysis and qualification metrics</p>
        </div>
        
        <div class="container">
            <!-- Universe Selection and Controls -->
            <div class="controls">
                <label>Universe:</label>
                <select id="universeSelect">
                    <option value="">Loading universes...</option>
                </select>
                
                <label>As of Date:</label>
                <input type="date" id="asOfDate" />
                
                <button onclick="loadUniverseData()">🔄 Refresh</button>
                <button onclick="exportData()">📊 Export</button>
            </div>
            
            <!-- Key Metrics -->
            <div class="metrics-grid" id="metricsGrid">
                <div class="metric-card">
                    <div class="metric-value" id="totalMembers">-</div>
                    <div class="metric-label">Total Members</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="activeMembers">-</div>
                    <div class="metric-label">Active Members</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="totalMarketCap">-</div>
                    <div class="metric-label">Total Market Cap</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="avgVolume">-</div>
                    <div class="metric-label">Avg Daily Volume</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="warningCount">-</div>
                    <div class="metric-label">⚠️ Warnings</div>
                </div>
            </div>
            
            <!-- Charts Grid -->
            <div class="grid">
                <!-- Universe Size Time Series -->
                <div class="chart-container">
                    <h3>📈 Universe Size Over Time</h3>
                    <canvas id="timeSeriesChart" height="300"></canvas>
                </div>
                
                <!-- Market Cap vs Volume Scatter -->
                <div class="chart-container">
                    <h3>💰 Market Cap vs Trading Volume</h3>
                    <canvas id="scatterChart" height="300"></canvas>
                </div>
            </div>
            
            <!-- Membership Table -->
            <div class="table-container">
                <div style="padding: 20px; border-bottom: 1px solid #eee; background: #f8f9fa;">
                    <h3>📋 Current Membership</h3>
                    <span id="membershipInfo">As of: <span id="asOfDateDisplay">-</span></span>
                </div>
                <div style="max-height: 600px; overflow-y: auto;">
                    <table id="membershipTable">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Status</th>
                                <th>Entry Date</th>
                                <th>Market Cap</th>
                                <th>Volume</th>
                                <th>Warnings</th>
                            </tr>
                        </thead>
                        <tbody id="membershipBody">
                            <tr><td colspan="6" class="loading">Select a universe to view membership data</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <script>
            let currentUniverseId = null;
            let timeSeriesChart = null;
            let scatterChart = null;
            
            // Initialize dashboard
            document.addEventListener('DOMContentLoaded', async () => {
                // Set default date to today
                document.getElementById('asOfDate').value = new Date().toISOString().split('T')[0];
                
                // Load universes
                await loadUniverses();
            });
            
            async function loadUniverses() {
                try {
                    const response = await fetch('/analytics/universes');
                    const data = await response.json();
                    
                    const select = document.getElementById('universeSelect');
                    select.innerHTML = '<option value="">Select a universe...</option>';
                    
                    data.universes.forEach(universe => {
                        const option = document.createElement('option');
                        option.value = universe.id;
                        option.textContent = `${universe.name} (${universe.active_members} active)`;
                        select.appendChild(option);
                    });
                    
                    // Auto-select first universe if available
                    if (data.universes.length > 0) {
                        select.value = data.universes[0].id;
                        currentUniverseId = data.universes[0].id;
                        await loadUniverseData();
                    }
                } catch (error) {
                    console.error('Error loading universes:', error);
                }
            }
            
            async function loadUniverseData() {
                const universeId = document.getElementById('universeSelect').value;
                const asOfDate = document.getElementById('asOfDate').value;
                
                if (!universeId) return;
                
                currentUniverseId = universeId;
                document.getElementById('asOfDateDisplay').textContent = asOfDate;
                
                // Load metrics
                await loadMetrics(universeId, asOfDate);
                
                // Load charts
                await loadTimeSeriesChart(universeId);
                await loadScatterChart(universeId, asOfDate);
                
                // Load membership table
                await loadMembershipTable(universeId, asOfDate);
            }
            
            async function loadMetrics(universeId, asOfDate) {
                try {
                    const url = `/analytics/universe/${universeId}/metrics${asOfDate ? `?as_of_date=${asOfDate}` : ''}`;
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    const metrics = data.metrics;
                    
                    document.getElementById('totalMembers').textContent = metrics.total_members || '-';
                    document.getElementById('activeMembers').textContent = metrics.active_members || '-';
                    
                    const marketCap = metrics.total_market_cap;
                    document.getElementById('totalMarketCap').textContent = marketCap ? 
                        `$${(marketCap / 1e9).toFixed(1)}B` : '-';
                    
                    const volume = metrics.avg_dollar_volume;
                    document.getElementById('avgVolume').textContent = volume ? 
                        `$${(volume / 1e6).toFixed(0)}M` : '-';
                    
                    document.getElementById('warningCount').textContent = metrics.warning_count || '0';
                } catch (error) {
                    console.error('Error loading metrics:', error);
                }
            }
            
            async function loadTimeSeriesChart(universeId) {
                try {
                    const endDate = new Date().toISOString().split('T')[0];
                    const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
                    
                    const response = await fetch(`/analytics/universe/${universeId}/timeseries?start_date=${startDate}&end_date=${endDate}`);
                    const data = await response.json();
                    
                    const ctx = document.getElementById('timeSeriesChart').getContext('2d');
                    
                    if (timeSeriesChart) {
                        timeSeriesChart.destroy();
                    }
                    
                    timeSeriesChart = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: data.timeseries.dates,
                            datasets: [{
                                label: 'Universe Size',
                                data: data.timeseries.member_counts,
                                borderColor: '#667eea',
                                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                                fill: true,
                                tension: 0.4
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                y: {
                                    beginAtZero: true,
                                    title: { display: true, text: 'Number of Members' }
                                }
                            }
                        }
                    });
                } catch (error) {
                    console.error('Error loading time series chart:', error);
                }
            }
            
            async function loadScatterChart(universeId, asOfDate) {
                try {
                    const url = `/analytics/universe/${universeId}/scatter${asOfDate ? `?as_of_date=${asOfDate}` : ''}`;
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    const ctx = document.getElementById('scatterChart').getContext('2d');
                    
                    if (scatterChart) {
                        scatterChart.destroy();
                    }
                    
                    scatterChart = new Chart(ctx, {
                        type: 'scatter',
                        data: {
                            datasets: [{
                                label: 'Active Members',
                                data: data.scatter_data.filter(d => d.status === 'Active').map(d => ({
                                    x: d.volume_millions,
                                    y: d.market_cap_millions,
                                    symbol: d.symbol
                                })),
                                backgroundColor: 'rgba(40, 167, 69, 0.6)',
                                borderColor: '#28a745'
                            }, {
                                label: 'Inactive Members', 
                                data: data.scatter_data.filter(d => d.status === 'Inactive').map(d => ({
                                    x: d.volume_millions,
                                    y: d.market_cap_millions,
                                    symbol: d.symbol
                                })),
                                backgroundColor: 'rgba(220, 53, 69, 0.6)',
                                borderColor: '#dc3545'
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                x: { 
                                    title: { display: true, text: 'Daily Volume ($M)' },
                                    type: 'logarithmic'
                                },
                                y: { 
                                    title: { display: true, text: 'Market Cap ($M)' },
                                    type: 'logarithmic'
                                }
                            },
                            plugins: {
                                tooltip: {
                                    callbacks: {
                                        label: (context) => {
                                            return `${context.raw.symbol}: $${context.parsed.y}M cap, $${context.parsed.x}M vol`;
                                        }
                                    }
                                }
                            }
                        }
                    });
                } catch (error) {
                    console.error('Error loading scatter chart:', error);
                }
            }
            
            async function loadMembershipTable(universeId, asOfDate) {
                try {
                    const url = `/analytics/universe/${universeId}/membership${asOfDate ? `?as_of_date=${asOfDate}` : ''}`;
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    const tbody = document.getElementById('membershipBody');
                    
                    if (data.membership.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="6" class="loading">No membership data available</td></tr>';
                        return;
                    }
                    
                    tbody.innerHTML = data.membership.map(member => `
                        <tr>
                            <td><strong>${member.symbol}</strong></td>
                            <td>
                                <span class="${member.active ? 'status-active' : 'status-inactive'}">
                                    ${member.active ? '✅ Active' : '❌ Inactive'}
                                </span>
                            </td>
                            <td>${new Date(member.start_at).toLocaleDateString()}</td>
                            <td>${member.market_cap ? `$${(member.market_cap / 1e6).toFixed(0)}M` : '-'}</td>
                            <td>${member.dollar_volume ? `$${(member.dollar_volume / 1e6).toFixed(0)}M` : '-'}</td>
                            <td>${member.removal_reason ? `<span class="warning-badge">${member.removal_reason}</span>` : '-'}</td>
                        </tr>
                    `).join('');
                } catch (error) {
                    console.error('Error loading membership table:', error);
                }
            }
            
            function exportData() {
                if (!currentUniverseId) return;
                
                const asOfDate = document.getElementById('asOfDate').value;
                const url = `/analytics/universe/${currentUniverseId}/membership?as_of_date=${asOfDate}&limit=1000&format=csv`;
                window.open(url);
            }
            
            // Event listeners
            document.getElementById('universeSelect').addEventListener('change', loadUniverseData);
            document.getElementById('asOfDate').addEventListener('change', loadUniverseData);
        </script>
    </body>
    </html>
    """
    return html_content

if __name__ == "__main__":
    import uvicorn
    import os
    
    port = int(os.environ.get("PORT", 3000))
    uvicorn.run(app, host="0.0.0.0", port=port)