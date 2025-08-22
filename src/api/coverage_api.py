"""
Data Coverage API

FastAPI application providing REST endpoints for data coverage analytics,
gap detection, vendor comparison, and real-time monitoring.
"""

import asyncio
import asyncpg
import logging
import json
import requests
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal

from fastapi import FastAPI, Depends, Query, Path, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Slack webhook URL for alerts
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

# Pydantic models for API requests/responses
class CoverageInterval(BaseModel):
    """Coverage interval data point"""
    interval_id: int
    symbol: str
    vendor: str
    data_type: str
    start_time: datetime
    end_time: datetime
    record_count: int
    expected_count: int
    completeness_ratio: float
    avg_quality_score: Optional[float] = None
    has_gaps: bool = False
    gap_count: int = 0
    total_gap_duration_minutes: int = 0

class CoverageSummary(BaseModel):
    """Coverage summary for dashboard display"""
    symbol: str
    vendor: str
    data_type: str
    current_status: str
    coverage_24h: float
    quality_24h: Optional[float] = None
    records_24h: int
    coverage_7d: Optional[float] = None
    coverage_30d: Optional[float] = None
    latest_data_time: Optional[datetime] = None
    hours_since_update: float
    coverage_trend: Optional[str] = None
    quality_trend: Optional[str] = None

class CoverageGap(BaseModel):
    """Detected coverage gap"""
    gap_id: int
    symbol: str
    vendor: str
    data_type: str
    gap_start: datetime
    gap_end: datetime
    gap_duration_minutes: int
    expected_records: int
    gap_type: str
    gap_severity: str
    trading_day: date
    is_market_hours: bool
    detection_method: str
    detection_confidence: float
    is_resolved: bool = False

class VendorComparison(BaseModel):
    """Vendor coverage comparison"""
    symbol: str
    data_type: str
    time_period: str
    vendors: List[Dict[str, Any]]
    best_vendor: Optional[Dict[str, Any]] = None
    worst_vendor: Optional[Dict[str, Any]] = None
    average_coverage: float
    coverage_variance: float
    vendor_count: int

class CoverageStats(BaseModel):
    """Coverage statistics for time periods"""
    symbol: str
    vendor: str
    data_type: str
    aggregation_level: str
    period_start: datetime
    period_end: datetime
    total_expected: int
    total_actual: int
    coverage_percentage: float
    completeness_score: float
    avg_quality_score: Optional[float] = None
    gap_count: int = 0
    total_gap_duration_minutes: int = 0

class CoverageOverview(BaseModel):
    """High-level coverage overview"""
    vendor: str
    data_type: str
    total_symbols: int
    avg_coverage: float
    active_symbols: int
    stale_symbols: int
    missing_symbols: int

class AlertRequest(BaseModel):
    """Coverage alert configuration"""
    vendor: str
    data_type: str
    symbol: Optional[str] = None
    min_coverage_threshold: float = 95.0
    max_gap_duration_minutes: int = 5
    alert_channels: List[str] = ["slack"]

# Database connection management
class DatabaseManager:
    def __init__(self):
        self.db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"
        self.pool = None
    
    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("✅ Database connection pool initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            raise
    
    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

# Slack alerting service
class SlackAlerter:
    @staticmethod
    async def send_alert(message: str, severity: str = "warning"):
        """Send alert to Slack channel"""
        try:
            color_map = {
                "info": "#36a64f",      # Green
                "warning": "#ff9500",   # Orange  
                "error": "#ff0000",     # Red
                "critical": "#8B0000"   # Dark Red
            }
            
            payload = {
                "attachments": [{
                    "color": color_map.get(severity, "#ff9500"),
                    "title": f"🚨 Data Coverage Alert ({severity.upper()})",
                    "text": message,
                    "footer": "ATS Coverage Monitoring",
                    "ts": int(datetime.now().timestamp())
                }]
            }
            
            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Slack alert sent: {message[:100]}...")
            else:
                logger.error(f"❌ Failed to send Slack alert: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Error sending Slack alert: {e}")

# Coverage analytics engine
class CoverageAnalyticsEngine:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.alerter = SlackAlerter()
    
    async def get_coverage_overview(self) -> List[CoverageOverview]:
        """Get high-level coverage overview"""
        async with self.db_manager.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    vendor,
                    data_type,
                    COUNT(*) as total_symbols,
                    ROUND(AVG(coverage_24h), 2) as avg_coverage,
                    COUNT(*) FILTER (WHERE current_status = 'active') as active_symbols,
                    COUNT(*) FILTER (WHERE current_status = 'stale') as stale_symbols,
                    COUNT(*) FILTER (WHERE current_status = 'missing') as missing_symbols
                FROM coverage_summary
                GROUP BY vendor, data_type
                ORDER BY vendor, data_type
            """)
            
            return [CoverageOverview(**dict(row)) for row in rows]
    
    async def get_coverage_summary(
        self, 
        symbols: Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
        data_types: Optional[List[str]] = None,
        min_coverage: Optional[float] = None
    ) -> List[CoverageSummary]:
        """Get coverage summary with filtering"""
        
        where_conditions = []
        params = []
        param_count = 0
        
        if symbols:
            param_count += 1
            where_conditions.append(f"symbol = ANY(${param_count})")
            params.append(symbols)
        
        if vendors:
            param_count += 1
            where_conditions.append(f"vendor = ANY(${param_count})")
            params.append(vendors)
        
        if data_types:
            param_count += 1
            where_conditions.append(f"data_type = ANY(${param_count})")
            params.append(data_types)
        
        if min_coverage:
            param_count += 1
            where_conditions.append(f"coverage_24h >= ${param_count}")
            params.append(min_coverage)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.db_manager.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT 
                    symbol, vendor, data_type, current_status,
                    coverage_24h, quality_24h, records_24h,
                    coverage_7d, coverage_30d,
                    latest_data_time, 
                    EXTRACT(EPOCH FROM (NOW() - latest_data_time)) / 3600.0 as hours_since_update,
                    coverage_trend, quality_trend
                FROM coverage_summary
                {where_clause}
                ORDER BY symbol, vendor, data_type
            """, *params)
            
            return [CoverageSummary(**dict(row)) for row in rows]
    
    async def get_coverage_intervals(
        self,
        symbol: str,
        vendor: Optional[str] = None,
        data_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[CoverageInterval]:
        """Get coverage intervals for detailed analysis"""
        
        where_conditions = ["symbol = $1"]
        params = [symbol]
        param_count = 1
        
        if vendor:
            param_count += 1
            where_conditions.append(f"vendor = ${param_count}")
            params.append(vendor)
        
        if data_type:
            param_count += 1
            where_conditions.append(f"data_type = ${param_count}")
            params.append(data_type)
        
        if start_time:
            param_count += 1
            where_conditions.append(f"start_time >= ${param_count}")
            params.append(start_time)
        
        if end_time:
            param_count += 1
            where_conditions.append(f"end_time <= ${param_count}")
            params.append(end_time)
        
        where_clause = " AND ".join(where_conditions)
        
        async with self.db_manager.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT 
                    interval_id, symbol, vendor, data_type,
                    start_time, end_time, record_count, expected_count,
                    completeness_ratio, avg_quality_score,
                    has_gaps, gap_count, total_gap_duration_minutes
                FROM coverage_intervals
                WHERE {where_clause}
                ORDER BY start_time DESC
                LIMIT ${param_count + 1}
            """, *params, limit)
            
            return [CoverageInterval(**dict(row)) for row in rows]
    
    async def get_coverage_gaps(
        self,
        symbol: Optional[str] = None,
        vendor: Optional[str] = None,
        data_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        severity: Optional[str] = None,
        unresolved_only: bool = True,
        limit: int = 100
    ) -> List[CoverageGap]:
        """Get detected coverage gaps"""
        
        where_conditions = []
        params = []
        param_count = 0
        
        if symbol:
            param_count += 1
            where_conditions.append(f"symbol = ${param_count}")
            params.append(symbol)
        
        if vendor:
            param_count += 1
            where_conditions.append(f"vendor = ${param_count}")
            params.append(vendor)
        
        if data_type:
            param_count += 1
            where_conditions.append(f"data_type = ${param_count}")
            params.append(data_type)
        
        if start_date:
            param_count += 1
            where_conditions.append(f"trading_day >= ${param_count}")
            params.append(start_date)
        
        if end_date:
            param_count += 1
            where_conditions.append(f"trading_day <= ${param_count}")
            params.append(end_date)
        
        if severity:
            param_count += 1
            where_conditions.append(f"gap_severity = ${param_count}")
            params.append(severity)
        
        if unresolved_only:
            where_conditions.append("is_resolved = FALSE")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.db_manager.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT 
                    gap_id, symbol, vendor, data_type,
                    gap_start, gap_end, gap_duration_minutes, expected_records,
                    gap_type, gap_severity, trading_day, is_market_hours,
                    detection_method, detection_confidence, is_resolved
                FROM coverage_gaps
                {where_clause}
                ORDER BY gap_start DESC
                LIMIT ${param_count + 1}
            """, *params, limit)
            
            # Note: We need to check if the gaps table exists first
            return [CoverageGap(**dict(row)) for row in rows]
    
    async def get_vendor_comparison(
        self,
        symbol: str,
        data_type: str = 'minute',
        time_period: str = '24h'
    ) -> VendorComparison:
        """Compare coverage across vendors for a symbol"""
        
        time_field_map = {
            '24h': 'coverage_24h',
            '7d': 'coverage_7d', 
            '30d': 'coverage_30d'
        }
        
        coverage_field = time_field_map.get(time_period, 'coverage_24h')
        quality_field = coverage_field.replace('coverage_', 'quality_')
        
        async with self.db_manager.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT 
                    vendor,
                    {coverage_field} as coverage_percentage,
                    {quality_field} as quality_score,
                    current_status,
                    latest_data_time,
                    hours_since_update
                FROM coverage_summary
                WHERE symbol = $1 AND data_type = $2
                AND {coverage_field} IS NOT NULL
                ORDER BY {coverage_field} DESC
            """, symbol, data_type)
            
            vendors = []
            for row in rows:
                vendors.append({
                    'vendor': row['vendor'],
                    'coverage_percentage': float(row['coverage_percentage'] or 0),
                    'quality_score': float(row['quality_score'] or 0),
                    'status': row['current_status'],
                    'latest_data_time': row['latest_data_time'],
                    'hours_since_update': float(row['hours_since_update'] or 0)
                })
            
            # Calculate comparison metrics
            if vendors:
                best_vendor = vendors[0]
                worst_vendor = vendors[-1]
                avg_coverage = sum(v['coverage_percentage'] for v in vendors) / len(vendors)
                coverage_values = [v['coverage_percentage'] for v in vendors]
                coverage_variance = sum((x - avg_coverage) ** 2 for x in coverage_values) / len(coverage_values)
            else:
                best_vendor = worst_vendor = None
                avg_coverage = coverage_variance = 0
            
            return VendorComparison(
                symbol=symbol,
                data_type=data_type,
                time_period=time_period,
                vendors=vendors,
                best_vendor=best_vendor,
                worst_vendor=worst_vendor,
                average_coverage=avg_coverage,
                coverage_variance=coverage_variance,
                vendor_count=len(vendors)
            )
    
    async def get_coverage_trends(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        days_back: int = 30,
        aggregation_level: str = 'day'
    ) -> List[CoverageStats]:
        """Get coverage trends over time"""
        
        start_date = datetime.now() - timedelta(days=days_back)
        
        async with self.db_manager.pool.acquire() as conn:
            # Check if coverage_stats table exists, if not use coverage_intervals
            stats_exist = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'coverage_stats'
                )
            """)
            
            if stats_exist:
                rows = await conn.fetch("""
                    SELECT 
                        symbol, vendor, data_type, aggregation_level,
                        period_start, period_end, total_expected, total_actual,
                        coverage_percentage, total_actual::float / GREATEST(total_expected, 1) as completeness_score,
                        avg_quality_score, gap_count, total_gap_duration_minutes
                    FROM coverage_stats
                    WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                        AND aggregation_level = $4
                        AND period_start >= $5
                    ORDER BY period_start
                """, symbol, vendor, data_type, aggregation_level, start_date)
            else:
                # Fallback to coverage_intervals with aggregation
                rows = await conn.fetch("""
                    SELECT 
                        symbol, vendor, data_type, 'day' as aggregation_level,
                        DATE_TRUNC('day', start_time) as period_start,
                        DATE_TRUNC('day', start_time) + INTERVAL '1 day' as period_end,
                        SUM(expected_count) as total_expected,
                        SUM(record_count) as total_actual,
                        AVG(completeness_ratio * 100) as coverage_percentage,
                        AVG(completeness_ratio) as completeness_score,
                        AVG(avg_quality_score) as avg_quality_score,
                        SUM(gap_count) as gap_count,
                        SUM(total_gap_duration_minutes) as total_gap_duration_minutes
                    FROM coverage_intervals
                    WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                        AND start_time >= $4
                    GROUP BY symbol, vendor, data_type, DATE_TRUNC('day', start_time)
                    ORDER BY period_start
                """, symbol, vendor, data_type, start_date)
            
            return [CoverageStats(**dict(row)) for row in rows]
    
    async def check_coverage_alerts(self) -> List[Dict[str, Any]]:
        """Check for coverage issues and send alerts"""
        alerts_sent = []
        
        async with self.db_manager.pool.acquire() as conn:
            # Check for low coverage
            low_coverage = await conn.fetch("""
                SELECT symbol, vendor, data_type, coverage_24h, current_status
                FROM coverage_summary
                WHERE coverage_24h < 90.0 AND current_status IN ('active', 'stale')
                ORDER BY coverage_24h ASC
            """)
            
            for row in low_coverage:
                message = (f"🔴 LOW COVERAGE ALERT\n"
                          f"Symbol: {row['symbol']}\n"
                          f"Vendor: {row['vendor']}\n"
                          f"Data Type: {row['data_type']}\n"
                          f"Coverage: {row['coverage_24h']:.1f}%\n"
                          f"Status: {row['current_status']}")
                
                await self.alerter.send_alert(message, "warning")
                alerts_sent.append({
                    "type": "low_coverage",
                    "symbol": row['symbol'],
                    "vendor": row['vendor'],
                    "coverage": float(row['coverage_24h'])
                })
            
            # Check for stale data
            stale_data = await conn.fetch("""
                SELECT symbol, vendor, data_type, hours_since_update
                FROM coverage_summary
                WHERE hours_since_update > 4 AND current_status = 'stale'
                ORDER BY hours_since_update DESC
            """)
            
            for row in stale_data:
                message = (f"⏰ STALE DATA ALERT\n"
                          f"Symbol: {row['symbol']}\n"
                          f"Vendor: {row['vendor']}\n"
                          f"Data Type: {row['data_type']}\n"
                          f"Last Update: {row['hours_since_update']:.1f} hours ago")
                
                await self.alerter.send_alert(message, "warning")
                alerts_sent.append({
                    "type": "stale_data",
                    "symbol": row['symbol'],
                    "vendor": row['vendor'],
                    "hours_since_update": float(row['hours_since_update'])
                })
        
        return alerts_sent

# Initialize FastAPI app
app = FastAPI(
    title="Data Coverage API",
    description="Real-time data coverage monitoring and analytics",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global dependencies
db_manager = DatabaseManager()
coverage_engine = None

async def get_coverage_engine() -> CoverageAnalyticsEngine:
    """Dependency to get coverage analytics engine"""
    global coverage_engine
    if coverage_engine is None:
        coverage_engine = CoverageAnalyticsEngine(db_manager)
    return coverage_engine

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    await db_manager.initialize()
    global coverage_engine
    coverage_engine = CoverageAnalyticsEngine(db_manager)
    logger.info("✅ Data Coverage API started")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    await db_manager.close()
    logger.info("Data Coverage API shutdown")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy", 
        "timestamp": datetime.now(),
        "service": "data_coverage_api"
    }

# Coverage overview endpoints
@app.get("/api/v1/coverage/overview", response_model=List[CoverageOverview])
async def get_coverage_overview(
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Get high-level coverage overview across all vendors"""
    try:
        return await engine.get_coverage_overview()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coverage overview: {str(e)}")

@app.get("/api/v1/coverage/summary", response_model=List[CoverageSummary])
async def get_coverage_summary(
    symbols: Optional[List[str]] = Query(None),
    vendors: Optional[List[str]] = Query(None),
    data_types: Optional[List[str]] = Query(None),
    min_coverage: Optional[float] = Query(None, ge=0, le=100),
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Get coverage summary with filtering options"""
    try:
        return await engine.get_coverage_summary(symbols, vendors, data_types, min_coverage)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coverage summary: {str(e)}")

# Detailed coverage endpoints
@app.get("/api/v1/coverage/intervals/{symbol}", response_model=List[CoverageInterval])
async def get_coverage_intervals(
    symbol: str = Path(..., description="Symbol to get coverage for"),
    vendor: Optional[str] = Query(None),
    data_type: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(100, le=1000, ge=1),
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Get detailed coverage intervals for a symbol"""
    try:
        return await engine.get_coverage_intervals(symbol, vendor, data_type, start_time, end_time, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coverage intervals: {str(e)}")

@app.get("/api/v1/coverage/gaps", response_model=List[CoverageGap])
async def get_coverage_gaps(
    symbol: Optional[str] = Query(None),
    vendor: Optional[str] = Query(None),
    data_type: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    severity: Optional[str] = Query(None),
    unresolved_only: bool = Query(True),
    limit: int = Query(100, le=1000, ge=1),
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Get detected coverage gaps with filtering"""
    try:
        return await engine.get_coverage_gaps(symbol, vendor, data_type, start_date, end_date, severity, unresolved_only, limit)
    except Exception as e:
        # If gaps table doesn't exist yet, return empty list
        if "relation" in str(e) and "does not exist" in str(e):
            return []
        raise HTTPException(status_code=500, detail=f"Failed to get coverage gaps: {str(e)}")

# Vendor comparison endpoints
@app.get("/api/v1/coverage/comparison/{symbol}", response_model=VendorComparison)
async def get_vendor_comparison(
    symbol: str = Path(..., description="Symbol to compare vendors for"),
    data_type: str = Query("minute", regex="^(minute|daily)$"),
    time_period: str = Query("24h", regex="^(24h|7d|30d)$"),
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Compare coverage across vendors for a specific symbol"""
    try:
        return await engine.get_vendor_comparison(symbol, data_type, time_period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get vendor comparison: {str(e)}")

# Trends and analytics endpoints
@app.get("/api/v1/coverage/trends/{symbol}/{vendor}", response_model=List[CoverageStats])
async def get_coverage_trends(
    symbol: str = Path(...),
    vendor: str = Path(...),
    data_type: str = Query("minute", regex="^(minute|daily)$"),
    days_back: int = Query(30, le=365, ge=1),
    aggregation_level: str = Query("day", regex="^(hour|day|week|month)$"),
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Get coverage trends over time for analysis"""
    try:
        return await engine.get_coverage_trends(symbol, vendor, data_type, days_back, aggregation_level)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get coverage trends: {str(e)}")

# Alerting endpoints
@app.post("/api/v1/coverage/alerts/check")
async def check_coverage_alerts(
    engine: CoverageAnalyticsEngine = Depends(get_coverage_engine)
):
    """Check for coverage issues and send alerts"""
    try:
        alerts = await engine.check_coverage_alerts()
        return {
            "status": "success",
            "alerts_sent": len(alerts),
            "alerts": alerts,
            "timestamp": datetime.now()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to check alerts: {str(e)}")

@app.post("/api/v1/coverage/alerts/test")
async def test_slack_alert():
    """Test Slack alert functionality"""
    try:
        message = "🧪 Test alert from ATS Coverage Monitoring System"
        alerter = SlackAlerter()
        await alerter.send_alert(message, "info")
        return {
            "status": "success",
            "message": "Test alert sent to Slack",
            "timestamp": datetime.now()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send test alert: {str(e)}")

# Real-time WebSocket endpoints
@app.websocket("/ws/coverage/realtime")
async def coverage_realtime_websocket(websocket: WebSocket):
    """Real-time coverage updates via WebSocket"""
    await websocket.accept()
    try:
        engine = await get_coverage_engine()
        
        # Send initial coverage overview
        overview = await engine.get_coverage_overview()
        await websocket.send_json({
            "type": "coverage_overview",
            "data": [item.dict() for item in overview],
            "timestamp": datetime.now().isoformat()
        })
        
        # Keep connection alive and send periodic updates
        while True:
            try:
                # Wait for client message or timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                
                # Handle client requests
                import json
                message = json.loads(data)
                
                if message.get("type") == "request_summary":
                    summary = await engine.get_coverage_summary()
                    await websocket.send_json({
                        "type": "coverage_summary",
                        "data": [item.dict() for item in summary],
                        "timestamp": datetime.now().isoformat()
                    })
                
            except asyncio.TimeoutError:
                # Send periodic heartbeat
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": datetime.now().isoformat()
                })
            except WebSocketDisconnect:
                break
            except Exception as e:
                await websocket.send_json({
                    "type": "error",
                    "message": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")

# Error handlers
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    uvicorn.run(
        "coverage_api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )