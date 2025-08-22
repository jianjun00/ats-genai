"""
Coverage Catalog API Integration

FastAPI endpoints for the data coverage catalog, integrated with
the existing analytics platform for seamless user experience.
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any
from dataclasses import asdict
from fastapi import FastAPI, Depends, Query, HTTPException, WebSocket
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import asyncpg

from .coverage_engine import (
    CoverageAnalyticsEngine, CoverageQuery, CoverageSummary,
    CoverageStats, CoverageGap, AggregationLevel
)

logger = logging.getLogger(__name__)

# =====================================================
# Pydantic Models for API
# =====================================================

class CoverageQueryRequest(BaseModel):
    """Request model for coverage queries"""
    symbols: Optional[List[str]] = None
    vendors: Optional[List[str]] = None
    data_types: Optional[List[str]] = Field(default=['minute'])
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    aggregation_level: Optional[str] = None
    min_coverage_percentage: Optional[float] = None
    include_gaps: bool = True

class CoverageStatsResponse(BaseModel):
    """Response model for coverage statistics"""
    symbol: str
    vendor: str
    data_type: str
    aggregation_level: str
    period_start: datetime
    period_end: datetime
    coverage_percentage: float
    completeness_score: float
    avg_quality_score: Optional[float]
    gap_count: int
    total_gap_duration_minutes: int
    records_per_minute: Optional[float] = None

class VendorComparisonResponse(BaseModel):
    """Response model for vendor comparison"""
    symbol: str
    data_type: str
    time_period: str
    vendors: List[Dict[str, Any]]
    best_vendor: Optional[Dict[str, Any]]
    worst_vendor: Optional[Dict[str, Any]]
    average_coverage: float
    coverage_variance: float
    vendor_count: int

class CoverageHeatmapResponse(BaseModel):
    """Response model for coverage heatmap data"""
    symbols: List[str]
    vendors: List[str]
    time_periods: List[str]
    coverage_matrix: List[List[List[float]]]  # [symbol][vendor][time_period]
    quality_matrix: List[List[List[Optional[float]]]]
    metadata: Dict[str, Any]

class SLAComplianceResponse(BaseModel):
    """Response model for SLA compliance"""
    symbol: str
    vendor: str
    data_type: str
    current_coverage: float
    required_coverage: float
    compliance_status: str
    coverage_gap: float
    quality_score: float

# =====================================================
# Coverage API Class
# =====================================================

class CoverageAPI:
    """
    Coverage catalog API integrated with analytics platform
    """
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self.coverage_engine = CoverageAnalyticsEngine(db_pool)
        
    async def initialize(self):
        """Initialize the coverage API"""
        await self.coverage_engine.initialize()
        logger.info("✅ Coverage API initialized")
    
    # =====================================================
    # Core Coverage Endpoints
    # =====================================================
    
    async def get_coverage_summary(
        self,
        symbols: Optional[List[str]] = None,
        vendors: Optional[List[str]] = None,
        data_types: Optional[List[str]] = None,
        min_coverage: Optional[float] = None
    ) -> List[CoverageSummary]:
        """Get coverage summary with optional filtering"""
        
        query = CoverageQuery(
            symbols=symbols,
            vendors=vendors,
            data_types=data_types,
            min_coverage_percentage=min_coverage
        )
        
        return await self.coverage_engine.query_coverage_summary(query)
    
    async def get_coverage_stats(
        self,
        symbol: str,
        vendor: str,
        data_type: str = 'minute',
        aggregation_level: str = 'hour',
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[CoverageStatsResponse]:
        """Get detailed coverage statistics"""
        
        # Default to last 24 hours if no time range specified
        if not start_time:
            start_time = datetime.now() - timedelta(hours=24)
        if not end_time:
            end_time = datetime.now()
        
        # Query coverage stats from database
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("""
                SELECT 
                    symbol, vendor, data_type, aggregation_level,
                    period_start, period_end, coverage_percentage,
                    completeness_score, avg_quality_score, gap_count,
                    total_gap_duration_minutes, records_per_minute
                FROM coverage_stats
                WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                    AND aggregation_level = $4
                    AND period_start >= $5 AND period_start < $6
                ORDER BY period_start
            """, symbol, vendor, data_type, aggregation_level, start_time, end_time)
            
            return [CoverageStatsResponse(**dict(record)) for record in records]
    
    async def get_vendor_comparison(
        self,
        symbol: str,
        data_type: str = 'minute',
        time_period: str = '24h'
    ) -> VendorComparisonResponse:
        """Compare coverage across vendors for a symbol"""
        
        comparison_data = await self.coverage_engine.get_vendor_comparison(
            symbol, data_type, time_period
        )
        
        return VendorComparisonResponse(**comparison_data)
    
    async def get_coverage_gaps(
        self,
        symbol: Optional[str] = None,
        vendor: Optional[str] = None,
        data_type: str = 'minute',
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        severity: Optional[str] = None,
        resolved: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """Get coverage gaps with filtering"""
        
        where_conditions = []
        params = []
        param_count = 0
        
        # Build WHERE clause
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
        
        if start_time:
            param_count += 1
            where_conditions.append(f"gap_start >= ${param_count}")
            params.append(start_time)
        
        if end_time:
            param_count += 1
            where_conditions.append(f"gap_end <= ${param_count}")
            params.append(end_time)
        
        if severity:
            param_count += 1
            where_conditions.append(f"gap_severity = ${param_count}")
            params.append(severity)
        
        if resolved is not None:
            param_count += 1
            where_conditions.append(f"is_resolved = ${param_count}")
            params.append(resolved)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(f"""
                SELECT 
                    gap_id, symbol, vendor, data_type,
                    gap_start, gap_end, gap_duration_minutes,
                    expected_records, gap_type, gap_severity,
                    trading_day, is_market_hours, detection_method,
                    detection_confidence, is_resolved, resolution_method,
                    resolved_at, resolution_notes, detected_at
                FROM coverage_gaps
                {where_clause}
                ORDER BY gap_start DESC
                LIMIT 1000
            """, *params)
            
            return [dict(record) for record in records]
    
    # =====================================================
    # Advanced Analytics Endpoints
    # =====================================================
    
    async def get_coverage_heatmap(
        self,
        symbols: List[str],
        vendors: List[str],
        start_date: date,
        end_date: date,
        data_type: str = 'minute'
    ) -> CoverageHeatmapResponse:
        """Generate coverage heatmap data for visualization"""
        
        # Validate date range
        if end_date < start_date:
            raise ValueError("End date cannot be before start date")
        
        # Generate time periods (daily)
        time_periods = []
        current_date = start_date
        while current_date <= end_date:
            time_periods.append(current_date.isoformat())
            current_date += timedelta(days=1)
        
        # Initialize matrices
        coverage_matrix = []
        quality_matrix = []
        
        # Query coverage data for each symbol/vendor/date combination
        async with self.db_pool.acquire() as conn:
            for symbol in symbols:
                symbol_coverage = []
                symbol_quality = []
                
                for vendor in vendors:
                    vendor_coverage = []
                    vendor_quality = []
                    
                    for period in time_periods:
                        period_date = datetime.fromisoformat(period).date()
                        
                        # Query coverage for this specific combination
                        record = await conn.fetchrow("""
                            SELECT 
                                coverage_percentage, 
                                avg_quality_score
                            FROM coverage_stats
                            WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                                AND aggregation_level = 'day'
                                AND period_start::DATE = $4
                        """, symbol, vendor, data_type, period_date)
                        
                        if record:
                            vendor_coverage.append(float(record['coverage_percentage']))
                            vendor_quality.append(
                                float(record['avg_quality_score']) if record['avg_quality_score'] else None
                            )
                        else:
                            vendor_coverage.append(0.0)
                            vendor_quality.append(None)
                    
                    symbol_coverage.append(vendor_coverage)
                    symbol_quality.append(vendor_quality)
                
                coverage_matrix.append(symbol_coverage)
                quality_matrix.append(symbol_quality)
        
        return CoverageHeatmapResponse(
            symbols=symbols,
            vendors=vendors,
            time_periods=time_periods,
            coverage_matrix=coverage_matrix,
            quality_matrix=quality_matrix,
            metadata={
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'data_type': data_type,
                'total_cells': len(symbols) * len(vendors) * len(time_periods)
            }
        )
    
    async def get_coverage_trends(
        self,
        symbol: str,
        vendor: str,
        data_type: str = 'minute',
        days_back: int = 30
    ) -> Dict[str, Any]:
        """Get coverage trends for time-series analysis"""
        
        return await self.coverage_engine.get_coverage_trends(
            symbol, vendor, data_type, days_back
        )
    
    async def get_sla_compliance(
        self,
        symbol: Optional[str] = None,
        vendor: Optional[str] = None
    ) -> List[SLAComplianceResponse]:
        """Get SLA compliance status"""
        
        compliance_data = await self.coverage_engine.check_sla_compliance(symbol, vendor)
        
        return [SLAComplianceResponse(**item) for item in compliance_data]
    
    async def get_top_coverage_issues(
        self,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get the most critical coverage issues"""
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("""
                SELECT * FROM get_top_coverage_issues($1)
            """, limit)
            
            return [dict(record) for record in records]
    
    # =====================================================
    # Real-Time WebSocket Endpoints
    # =====================================================
    
    async def stream_coverage_updates(self, websocket: WebSocket):
        """Stream real-time coverage updates via WebSocket"""
        
        await websocket.accept()
        
        try:
            # Send initial coverage summary
            summary = await self.get_coverage_summary()
            await websocket.send_json({
                'type': 'initial_summary',
                'data': [asdict(item) for item in summary]
            })
            
            # Stream updates every 30 seconds
            while True:
                await asyncio.sleep(30)
                
                # Get recent updates (coverage changes in last minute)
                async with self.db_pool.acquire() as conn:
                    recent_updates = await conn.fetch("""
                        SELECT 
                            symbol, vendor, data_type, coverage_24h, 
                            quality_24h, current_status, last_updated
                        FROM coverage_summary
                        WHERE last_updated >= NOW() - INTERVAL '1 minute'
                        ORDER BY last_updated DESC
                    """)
                    
                    if recent_updates:
                        await websocket.send_json({
                            'type': 'coverage_updates',
                            'timestamp': datetime.now().isoformat(),
                            'data': [dict(record) for record in recent_updates]
                        })
                
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await websocket.close()
    
    # =====================================================
    # Utility Methods
    # =====================================================
    
    async def trigger_coverage_refresh(
        self,
        symbol: Optional[str] = None,
        vendor: Optional[str] = None
    ) -> Dict[str, Any]:
        """Manually trigger coverage statistics refresh"""
        
        start_time = datetime.now()
        
        # Refresh coverage intervals and stats
        async with self.db_pool.acquire() as conn:
            if symbol and vendor:
                # Refresh specific symbol/vendor
                await conn.execute("""
                    SELECT update_minute_bars_coverage()
                    WHERE EXISTS (
                        SELECT 1 FROM minute_bars 
                        WHERE symbol = $1 AND vendor = $2
                        LIMIT 1
                    )
                """, symbol, vendor)
                
                refresh_scope = f"{symbol}/{vendor}"
            else:
                # Refresh all active symbols/vendors
                await conn.execute("""
                    UPDATE coverage_summary 
                    SET last_updated = NOW()
                    WHERE current_status IN ('active', 'stale')
                """)
                
                refresh_scope = "all active symbols/vendors"
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return {
            'success': True,
            'scope': refresh_scope,
            'duration_ms': duration_ms,
            'timestamp': datetime.now().isoformat()
        }

# =====================================================
# FastAPI App Integration
# =====================================================

def create_coverage_api_routes(app: FastAPI, coverage_api: CoverageAPI):
    """
    Add coverage catalog routes to the existing analytics FastAPI app
    """
    
    # ===== Coverage Summary Endpoints =====
    @app.get("/api/v1/coverage/summary")
    async def get_coverage_summary(
        symbols: Optional[str] = Query(None, description="Comma-separated list of symbols"),
        vendors: Optional[str] = Query(None, description="Comma-separated list of vendors"),
        data_types: Optional[str] = Query('minute', description="Comma-separated list of data types"),
        min_coverage: Optional[float] = Query(None, description="Minimum coverage percentage")
    ):
        """Get coverage summary with optional filtering"""
        
        # Parse comma-separated parameters
        symbols_list = symbols.split(',') if symbols else None
        vendors_list = vendors.split(',') if vendors else None
        data_types_list = data_types.split(',') if data_types else ['minute']
        
        summary = await coverage_api.get_coverage_summary(
            symbols=symbols_list,
            vendors=vendors_list,
            data_types=data_types_list,
            min_coverage=min_coverage
        )
        
        return {
            'summary': [item.dict() for item in summary],
            'total_items': len(summary),
            'filters_applied': {
                'symbols': symbols_list,
                'vendors': vendors_list,
                'data_types': data_types_list,
                'min_coverage': min_coverage
            }
        }
    
    @app.get("/api/v1/coverage/stats/{symbol}/{vendor}")
    async def get_coverage_stats(
        symbol: str,
        vendor: str,
        data_type: str = Query('minute'),
        aggregation_level: str = Query('hour'),
        start_time: Optional[datetime] = Query(None),
        end_time: Optional[datetime] = Query(None)
    ):
        """Get detailed coverage statistics for a symbol/vendor"""
        
        stats = await coverage_api.get_coverage_stats(
            symbol, vendor, data_type, aggregation_level, start_time, end_time
        )
        
        return {
            'stats': [item.dict() for item in stats],
            'symbol': symbol,
            'vendor': vendor,
            'data_type': data_type,
            'aggregation_level': aggregation_level
        }
    
    # ===== Vendor Comparison Endpoints =====
    @app.get("/api/v1/coverage/comparison/{symbol}")
    async def get_vendor_comparison(
        symbol: str,
        data_type: str = Query('minute'),
        time_period: str = Query('24h')
    ):
        """Compare coverage across vendors for a symbol"""
        
        comparison = await coverage_api.get_vendor_comparison(symbol, data_type, time_period)
        return comparison.dict()
    
    # ===== Gap Analysis Endpoints =====
    @app.get("/api/v1/coverage/gaps")
    async def get_coverage_gaps(
        symbol: Optional[str] = Query(None),
        vendor: Optional[str] = Query(None),
        data_type: str = Query('minute'),
        start_time: Optional[datetime] = Query(None),
        end_time: Optional[datetime] = Query(None),
        severity: Optional[str] = Query(None),
        resolved: Optional[bool] = Query(None)
    ):
        """Get coverage gaps with filtering"""
        
        gaps = await coverage_api.get_coverage_gaps(
            symbol, vendor, data_type, start_time, end_time, severity, resolved
        )
        
        return {
            'gaps': gaps,
            'total_gaps': len(gaps),
            'filters': {
                'symbol': symbol,
                'vendor': vendor,
                'data_type': data_type,
                'severity': severity,
                'resolved': resolved
            }
        }
    
    # ===== Advanced Analytics Endpoints =====
    @app.get("/api/v1/coverage/heatmap")
    async def get_coverage_heatmap(
        symbols: str = Query(..., description="Comma-separated list of symbols"),
        vendors: str = Query(..., description="Comma-separated list of vendors"),
        start_date: date = Query(...),
        end_date: date = Query(...),
        data_type: str = Query('minute')
    ):
        """Generate coverage heatmap data"""
        
        symbols_list = symbols.split(',')
        vendors_list = vendors.split(',')
        
        heatmap = await coverage_api.get_coverage_heatmap(
            symbols_list, vendors_list, start_date, end_date, data_type
        )
        
        return heatmap.dict()
    
    @app.get("/api/v1/coverage/trends/{symbol}/{vendor}")
    async def get_coverage_trends(
        symbol: str,
        vendor: str,
        data_type: str = Query('minute'),
        days_back: int = Query(30)
    ):
        """Get coverage trends for time-series analysis"""
        
        trends = await coverage_api.get_coverage_trends(symbol, vendor, data_type, days_back)
        return trends
    
    # ===== SLA and Monitoring Endpoints =====
    @app.get("/api/v1/coverage/sla-compliance")
    async def get_sla_compliance(
        symbol: Optional[str] = Query(None),
        vendor: Optional[str] = Query(None)
    ):
        """Get SLA compliance status"""
        
        compliance = await coverage_api.get_sla_compliance(symbol, vendor)
        return {
            'compliance': [item.dict() for item in compliance],
            'total_items': len(compliance)
        }
    
    @app.get("/api/v1/coverage/issues")
    async def get_top_coverage_issues(
        limit: int = Query(20, le=100)
    ):
        """Get the most critical coverage issues"""
        
        issues = await coverage_api.get_top_coverage_issues(limit)
        return {
            'issues': issues,
            'total_issues': len(issues)
        }
    
    # ===== Utility Endpoints =====
    @app.post("/api/v1/coverage/refresh")
    async def trigger_coverage_refresh(
        symbol: Optional[str] = Query(None),
        vendor: Optional[str] = Query(None)
    ):
        """Manually trigger coverage statistics refresh"""
        
        result = await coverage_api.trigger_coverage_refresh(symbol, vendor)
        return result
    
    # ===== WebSocket Endpoints =====
    @app.websocket("/ws/coverage/live")
    async def websocket_coverage_updates(websocket: WebSocket):
        """Stream real-time coverage updates"""
        await coverage_api.stream_coverage_updates(websocket)