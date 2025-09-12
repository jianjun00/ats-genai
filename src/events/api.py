"""
Event API - FastAPI endpoints for querying events and correlations
"""

import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from events.database import EventStorage
from events.correlation import CorrelationEngine
from events.producer import EventProducer
from events.proto.events_pb2 import Event, EventType, Priority, Classification

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class EventQuery(BaseModel):
    """Query parameters for event search"""
    symbol: Optional[str] = None
    event_type: Optional[str] = None
    source: Optional[str] = None
    priority: Optional[str] = None
    after_timestamp: Optional[datetime] = None
    before_timestamp: Optional[datetime] = None
    search_text: Optional[str] = None
    limit: int = Field(default=100, le=1000)
    offset: int = Field(default=0, ge=0)

class EventResponse(BaseModel):
    """Event response model"""
    event_id: str
    event_type: str
    symbol: Optional[str]
    timestamp: datetime
    source: str
    priority: str
    confidence: float
    event_data: Dict[str, Any]
    correlations_count: int = 0

class CorrelationResponse(BaseModel):
    """Correlation response model"""
    primary_event_id: str
    related_event_id: str
    correlation_type: str
    correlation_score: float
    time_lag_seconds: int
    description: str

class NewsEventCreate(BaseModel):
    """Model for creating news events"""
    headline: str
    symbol: str
    sentiment: Optional[float] = 0.0
    publisher: str = "unknown"
    url: str = ""
    source: str = "manual"

class EarningsEventCreate(BaseModel):
    """Model for creating earnings events"""
    symbol: str
    eps_actual: float
    eps_consensus: float
    year: int
    quarter: int
    source: str = "manual"

class EventStats(BaseModel):
    """Event statistics model"""
    total_events: int
    events_by_type: Dict[str, int]
    events_by_source: Dict[str, int]
    recent_events_24h: int
    total_correlations: int
    timestamp: str

# Create FastAPI app
app = FastAPI(
    title="ATS Event API",
    description="API for querying and managing financial events",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances (will be initialized on startup)
event_storage: Optional[EventStorage] = None
correlation_engine: Optional[CorrelationEngine] = None
event_producer: Optional[EventProducer] = None

@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    global event_storage, correlation_engine, event_producer

    try:
        event_storage = EventStorage()
        correlation_engine = CorrelationEngine(event_storage)
        event_producer = EventProducer()
        logger.info("✅ Event API initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Event API: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections on shutdown"""
    global event_storage, event_producer

    if event_storage:
        event_storage.close()
    if event_producer:
        event_producer.close()

    logger.info("🔒 Event API connections closed")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        stats = event_storage.get_event_stats()

        if 'error' in stats:
            raise HTTPException(status_code=503, detail="Database connection failed")

        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected",
            "total_events": stats.get('total_events', 0)
        }
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        raise HTTPException(status_code=503, detail=str(e))

# Event query endpoints
@app.get("/events", response_model=List[EventResponse])
async def query_events(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    source: Optional[str] = Query(None, description="Filter by source"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    hours_back: Optional[int] = Query(24, description="Hours back from now"),
    limit: int = Query(100, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Results offset")
):
    """Query events with filters"""
    try:
        # Calculate time range
        after_timestamp = datetime.utcnow() - timedelta(hours=hours_back) if hours_back else None

        events = event_storage.query_events(
            symbol=symbol,
            event_type=event_type,
            source=source,
            priority=priority,
            after_timestamp=after_timestamp,
            limit=limit,
            offset=offset
        )

        # Convert to response format
        response_events = []
        for event in events:
            # Get correlation count
            correlations = correlation_engine.get_correlations(event['event_id'])

            response_events.append(EventResponse(
                event_id=event['event_id'],
                event_type=event['event_type'],
                symbol=event.get('symbol'),
                timestamp=event['timestamp'],
                source=event['source'],
                priority=event['priority'],
                confidence=float(event.get('confidence', 0.0)),
                event_data=event['event_data'],
                correlations_count=len(correlations)
            ))

        return response_events

    except Exception as e:
        logger.error(f"❌ Error querying events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/{event_id}")
async def get_event(event_id: str):
    """Get single event by ID"""
    try:
        event = event_storage.get_event(event_id)
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Get correlations for this event
        correlations = correlation_engine.get_correlations(event_id)

        return {
            "event": event,
            "correlations": correlations
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting event {event_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/search")
async def search_events(
    query: str = Query(..., description="Search text"),
    limit: int = Query(100, le=1000, description="Maximum results")
):
    """Full-text search events"""
    try:
        events = event_storage.search_events(query, limit)

        return {
            "query": query,
            "results": events,
            "count": len(events)
        }

    except Exception as e:
        logger.error(f"❌ Error searching events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Correlation endpoints
@app.get("/events/{event_id}/correlations", response_model=List[CorrelationResponse])
async def get_event_correlations(
    event_id: str,
    min_score: float = Query(0.5, ge=0.0, le=1.0, description="Minimum correlation score")
):
    """Get correlations for a specific event"""
    try:
        correlations = correlation_engine.get_correlations(event_id, min_score)

        response_correlations = []
        for corr in correlations:
            response_correlations.append(CorrelationResponse(
                primary_event_id=corr['primary_event_id'],
                related_event_id=corr['related_event_id'],
                correlation_type=corr['correlation_type'],
                correlation_score=float(corr['correlation_score']),
                time_lag_seconds=int(corr.get('time_lag_seconds', 0)),
                description=corr.get('rule_description', corr['correlation_type'])
            ))

        return response_correlations

    except Exception as e:
        logger.error(f"❌ Error getting correlations for event {event_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/correlations/stats")
async def get_correlation_stats(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    hours: int = Query(24, description="Hours back from now")
):
    """Get correlation statistics"""
    try:
        stats = correlation_engine.get_correlation_stats(symbol, hours)
        return stats

    except Exception as e:
        logger.error(f"❌ Error getting correlation stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Event creation endpoints
@app.post("/events/news")
async def create_news_event(event: NewsEventCreate, background_tasks: BackgroundTasks):
    """Create a news event"""
    try:
        event_id = event_producer.publish_news_event(
            headline=event.headline,
            symbol=event.symbol,
            sentiment=event.sentiment,
            publisher=event.publisher,
            url=event.url,
            source=event.source
        )

        return {
            "status": "created",
            "event_id": event_id,
            "message": f"News event created and queued for processing"
        }

    except Exception as e:
        logger.error(f"❌ Error creating news event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/earnings")
async def create_earnings_event(event: EarningsEventCreate, background_tasks: BackgroundTasks):
    """Create an earnings event"""
    try:
        event_id = event_producer.publish_earnings_event(
            symbol=event.symbol,
            eps_actual=event.eps_actual,
            eps_consensus=event.eps_consensus,
            year=event.year,
            quarter=event.quarter,
            source=event.source
        )

        return {
            "status": "created",
            "event_id": event_id,
            "message": f"Earnings event created and queued for processing"
        }

    except Exception as e:
        logger.error(f"❌ Error creating earnings event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Statistics endpoints
@app.get("/stats", response_model=EventStats)
async def get_event_stats():
    """Get event statistics"""
    try:
        stats = event_storage.get_event_stats()

        if 'error' in stats:
            raise HTTPException(status_code=500, detail=stats['error'])

        return EventStats(
            total_events=stats['total_events'],
            events_by_type=stats['events_by_type'],
            events_by_source=stats['events_by_source'],
            recent_events_24h=stats['recent_events_24h'],
            total_correlations=stats['total_correlations'],
            timestamp=stats['timestamp']
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting event stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/queue/stats")
async def get_queue_stats():
    """Get queue statistics"""
    try:
        stats = event_producer.get_queue_stats()
        return {
            "queue_stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error getting queue stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Administrative endpoints
@app.post("/admin/queue/clear/{queue_name}")
async def clear_queue(queue_name: str):
    """Clear specific queue (admin only)"""
    try:
        success = event_producer.clear_queue(queue_name)
        return {
            "status": "success" if success else "failed",
            "queue": queue_name,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error clearing queue {queue_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/queue/clear-all")
async def clear_all_queues():
    """Clear all event queues (admin only)"""
    try:
        success = event_producer.clear_all_queues()
        return {
            "status": "success" if success else "failed",
            "message": "All event queues cleared",
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Error clearing all queues: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket support for real-time events
@app.websocket("/ws/events")
async def websocket_events(websocket):
    """WebSocket endpoint for real-time event streaming"""
    await websocket.accept()

    try:
        # This is a placeholder for real-time event streaming
        # In a full implementation, this would subscribe to Redis pub/sub
        # and stream events to connected clients

        await websocket.send_text(json.dumps({
            "type": "connection",
            "message": "Connected to ATS Event Stream",
            "timestamp": datetime.utcnow().isoformat()
        }))

        # Keep connection alive
        while True:
            # Wait for client messages or implement real-time streaming
            data = await websocket.receive_text()

            # Echo back for now
            await websocket.send_text(json.dumps({
                "type": "echo",
                "data": data,
                "timestamp": datetime.utcnow().isoformat()
            }))

    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
        await websocket.close()

# Custom exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"❌ Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# Main application runner
if __name__ == "__main__":
    import uvicorn

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Run the API server
    uvicorn.run(
        "events.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )