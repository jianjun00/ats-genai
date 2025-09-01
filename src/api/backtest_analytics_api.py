"""
Backtest Analytics API

FastAPI application providing REST endpoints for portfolio analytics,
model performance analysis, and drill-down capabilities.
"""

import logging
from datetime import date, datetime
from typing import Dict, List, Optional, Any

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query, Path, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import pandas as pd
import uvicorn

from analytics.portfolio_analytics import (
    PortfolioAnalyticsEngine,
    PortfolioMetrics,
    AttributionMetrics,
    ModelPerformanceMetrics
)

# Pydantic models for API requests/responses
class BacktestSummary(BaseModel):
    """Summary information for a backtest run"""
    backtest_run_id: str
    strategy_name: str
    strategy_type: str  # "adaptive", "static"
    universe_size: int
    start_date: date
    end_date: date
    status: str  # "running", "completed", "failed"
    total_return: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    created_at: datetime
    updated_at: datetime

class PerformanceDataPoint(BaseModel):
    """Single data point in portfolio performance time series"""
    date: date
    portfolio_value: float
    daily_return: Optional[float] = None
    cumulative_return: Optional[float] = None
    drawdown: Optional[float] = None
    positions_count: Optional[int] = None

class ForecastDataPoint(BaseModel):
    """Support/resistance forecast data point"""
    date: date
    symbol: str
    support_levels: List[float]
    resistance_levels: List[float]
    support_confidence: List[float]
    resistance_confidence: List[float]
    actual_low: Optional[float] = None
    actual_high: Optional[float] = None

class PortfolioComparisonRequest(BaseModel):
    """Request for comparing multiple portfolio strategies"""
    backtest_run_ids: List[str] = Field(..., min_length=2, max_length=5)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    metrics_to_compare: List[str] = ["total_return", "sharpe_ratio", "max_drawdown"]

class ModelComparisonRequest(BaseModel):
    """Request for comparing model performance"""
    backtest_run_ids: List[str] = Field(..., min_length=2, max_length=5)
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class PortfolioComparisonResult(BaseModel):
    """Results of portfolio comparison"""
    comparison_summary: Dict[str, Any]
    individual_metrics: Dict[str, PortfolioMetrics]
    relative_performance: Dict[str, float]
    statistical_significance: Dict[str, bool]

class ModelComparisonResult(BaseModel):
    """Results of model comparison"""
    comparison_summary: Dict[str, Any]
    individual_performance: Dict[str, ModelPerformanceMetrics]
    accuracy_comparison: Dict[str, float]
    confidence_analysis: Dict[str, Any]

# WebSocket connection manager
class ConnectionManager:
    """Manage WebSocket connections for real-time updates"""
    
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, backtest_run_id: str):
        """Connect a new WebSocket client"""
        await websocket.accept()
        if backtest_run_id not in self.active_connections:
            self.active_connections[backtest_run_id] = []
        self.active_connections[backtest_run_id].append(websocket)
    
    def disconnect(self, websocket: WebSocket, backtest_run_id: str):
        """Disconnect a WebSocket client"""
        if backtest_run_id in self.active_connections:
            if websocket in self.active_connections[backtest_run_id]:
                self.active_connections[backtest_run_id].remove(websocket)
    
    async def broadcast(self, message: dict, backtest_run_id: str):
        """Broadcast message to all connected clients for a backtest"""
        if backtest_run_id in self.active_connections:
            disconnected = []
            for connection in self.active_connections[backtest_run_id]:
                try:
                    await connection.send_json(message)
                except WebSocketDisconnect:
                    disconnected.append(connection)
            
            # Remove disconnected clients
            for connection in disconnected:
                self.active_connections[backtest_run_id].remove(connection)

# Global dependencies
analytics_engine = None
connection_manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    # Startup
    global analytics_engine
    analytics_engine = PortfolioAnalyticsEngine()
    await analytics_engine.initialize()
    logging.info("Backtest Analytics API started")
    
    yield
    
    # Shutdown
    if analytics_engine:
        await analytics_engine.close()
    logging.info("Backtest Analytics API shutdown")

# Initialize FastAPI app
app = FastAPI(
    title="Backtest Analytics API",
    description="Advanced portfolio analytics and model performance analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_analytics_engine() -> PortfolioAnalyticsEngine:
    """Dependency to get analytics engine instance"""
    global analytics_engine
    if analytics_engine is None:
        analytics_engine = PortfolioAnalyticsEngine()
        await analytics_engine.initialize()
    return analytics_engine

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}

# Backtest listing and management
@app.get("/api/v1/backtests", response_model=List[BacktestSummary])
async def list_backtests(
    limit: int = Query(50, le=100, ge=1),
    offset: int = Query(0, ge=0),
    strategy_type: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
):
    """List available backtest runs with filtering"""
    try:
        # This would query the database for backtest runs
        # For now, return mock data
        mock_backtests = [
            BacktestSummary(
                backtest_run_id="adaptive_2023_100",
                strategy_name="Adaptive S/R Strategy",
                strategy_type="adaptive",
                universe_size=100,
                start_date=date(2023, 1, 1),
                end_date=date(2024, 6, 30),
                status="completed",
                total_return=0.15,
                sharpe_ratio=1.2,
                max_drawdown=0.08,
                created_at=datetime(2024, 7, 1, 10, 0, 0),
                updated_at=datetime(2024, 7, 1, 18, 30, 0)
            ),
            BacktestSummary(
                backtest_run_id="static_2023_100",
                strategy_name="Static S/R Strategy",
                strategy_type="static",
                universe_size=100,
                start_date=date(2023, 1, 1),
                end_date=date(2024, 6, 30),
                status="completed",
                total_return=0.12,
                sharpe_ratio=1.0,
                max_drawdown=0.12,
                created_at=datetime(2024, 7, 1, 10, 0, 0),
                updated_at=datetime(2024, 7, 1, 16, 45, 0)
            )
        ]
        
        # Apply filters
        filtered_backtests = mock_backtests
        if strategy_type:
            filtered_backtests = [bt for bt in filtered_backtests if bt.strategy_type == strategy_type]
        
        # Apply pagination
        return filtered_backtests[offset:offset + limit]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list backtests: {str(e)}")

# Portfolio analytics endpoints
@app.get("/api/v1/backtests/{backtest_run_id}/portfolio/metrics")
async def get_portfolio_metrics(
    backtest_run_id: str = Path(..., description="Backtest run identifier"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    benchmark_id: Optional[str] = Query(None),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> PortfolioMetrics:
    """Get comprehensive portfolio performance metrics"""
    try:
        metrics = await engine.compute_portfolio_metrics(
            backtest_run_id=backtest_run_id,
            start_date=start_date,
            end_date=end_date,
            benchmark_run_id=benchmark_id
        )
        return metrics
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute metrics: {str(e)}")

@app.get("/api/v1/backtests/{backtest_run_id}/portfolio/performance")
async def get_portfolio_performance(
    backtest_run_id: str = Path(...),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    granularity: str = Query("daily", pattern="^(daily|hourly|minute)$"),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> List[PerformanceDataPoint]:
    """Get time-series portfolio performance data"""
    try:
        # Fetch portfolio performance data
        performance_data = await engine._fetch_portfolio_performance_data(
            backtest_run_id, start_date, end_date
        )
        
        if performance_data.empty:
            return []
        
        # Convert to response format
        result = []
        for idx, row in performance_data.iterrows():
            result.append(PerformanceDataPoint(
                date=idx.date(),
                portfolio_value=float(row['portfolio_value']),
                daily_return=float(row['daily_return']) if pd.notna(row['daily_return']) else None,
                cumulative_return=float(row['cumulative_return']) if pd.notna(row['cumulative_return']) else None,
                drawdown=float(row['drawdown']) if pd.notna(row['drawdown']) else None,
                positions_count=int(row['positions_count']) if pd.notna(row['positions_count']) else None
            ))
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get performance data: {str(e)}")

@app.get("/api/v1/backtests/{backtest_run_id}/attribution")
async def get_attribution_analysis(
    backtest_run_id: str = Path(...),
    attribution_type: str = Query("stock", pattern="^(stock|sector|signal)$"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> AttributionMetrics:
    """Get performance attribution breakdown"""
    try:
        attribution = await engine.compute_attribution_analysis(
            backtest_run_id=backtest_run_id,
            start_date=start_date,
            end_date=end_date
        )
        return attribution
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute attribution: {str(e)}")

# Model performance endpoints
@app.get("/api/v1/backtests/{backtest_run_id}/model/performance")
async def get_model_performance(
    backtest_run_id: str = Path(...),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> ModelPerformanceMetrics:
    """Get model prediction accuracy and performance metrics"""
    try:
        performance = await engine.compute_model_performance(
            backtest_run_id=backtest_run_id,
            start_date=start_date,
            end_date=end_date
        )
        return performance
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get model performance: {str(e)}")

@app.get("/api/v1/backtests/{backtest_run_id}/forecasts")
async def get_forecasts(
    backtest_run_id: str = Path(...),
    symbol: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> List[ForecastDataPoint]:
    """Get support/resistance forecasts with confidence levels"""
    try:
        # This would fetch forecast data from the database
        # For now, return mock data
        mock_forecasts = [
            ForecastDataPoint(
                date=date(2024, 1, 15),
                symbol=symbol or "AAPL",
                support_levels=[180.0, 175.0],
                resistance_levels=[190.0, 195.0],
                support_confidence=[0.75, 0.60],
                resistance_confidence=[0.70, 0.55],
                actual_low=178.5,
                actual_high=188.2
            )
        ]
        return mock_forecasts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get forecasts: {str(e)}")

# Comparison endpoints
@app.post("/api/v1/comparison/portfolio")
async def compare_portfolios(
    request: PortfolioComparisonRequest,
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> PortfolioComparisonResult:
    """Compare performance between multiple backtest runs"""
    try:
        individual_metrics = {}
        
        # Get metrics for each backtest
        for run_id in request.backtest_run_ids:
            metrics = await engine.compute_portfolio_metrics(
                backtest_run_id=run_id,
                start_date=request.start_date,
                end_date=request.end_date
            )
            individual_metrics[run_id] = metrics
        
        # Compute comparison summary
        comparison_summary = {
            "best_return": max(individual_metrics.items(), key=lambda x: x[1].total_return)[0],
            "best_sharpe": max(individual_metrics.items(), key=lambda x: x[1].sharpe_ratio)[0],
            "lowest_drawdown": min(individual_metrics.items(), key=lambda x: x[1].max_drawdown)[0]
        }
        
        # Compute relative performance (vs first strategy)
        baseline_run_id = request.backtest_run_ids[0]
        baseline_return = individual_metrics[baseline_run_id].total_return
        
        relative_performance = {}
        for run_id, metrics in individual_metrics.items():
            relative_performance[run_id] = metrics.total_return - baseline_return
        
        # Statistical significance (simplified)
        statistical_significance = {
            run_id: abs(relative_performance[run_id]) > 0.02 
            for run_id in request.backtest_run_ids
        }
        
        return PortfolioComparisonResult(
            comparison_summary=comparison_summary,
            individual_metrics=individual_metrics,
            relative_performance=relative_performance,
            statistical_significance=statistical_significance
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compare portfolios: {str(e)}")

@app.post("/api/v1/comparison/models")
async def compare_models(
    request: ModelComparisonRequest,
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> ModelComparisonResult:
    """Compare model performance between different strategies"""
    try:
        individual_performance = {}
        
        # Get model performance for each backtest
        for run_id in request.backtest_run_ids:
            performance = await engine.compute_model_performance(
                backtest_run_id=run_id,
                start_date=request.start_date,
                end_date=request.end_date
            )
            individual_performance[run_id] = performance
        
        # Compute comparison summary
        comparison_summary = {
            "best_accuracy": max(individual_performance.items(), key=lambda x: x[1].overall_accuracy)[0],
            "best_confidence": max(individual_performance.items(), key=lambda x: x[1].confidence_correlation)[0],
            "lowest_mae": min(individual_performance.items(), key=lambda x: x[1].overall_mae)[0]
        }
        
        # Accuracy comparison
        accuracy_comparison = {
            run_id: perf.overall_accuracy 
            for run_id, perf in individual_performance.items()
        }
        
        # Confidence analysis
        confidence_analysis = {
            "average_correlation": sum(perf.confidence_correlation for perf in individual_performance.values()) / len(individual_performance),
            "best_calibrated": max(individual_performance.items(), key=lambda x: x[1].confidence_correlation)[0]
        }
        
        return ModelComparisonResult(
            comparison_summary=comparison_summary,
            individual_performance=individual_performance,
            accuracy_comparison=accuracy_comparison,
            confidence_analysis=confidence_analysis
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compare models: {str(e)}")

# Drill-down endpoints
@app.get("/api/v1/backtests/{backtest_run_id}/drill-down/period")
async def drill_down_period(
    backtest_run_id: str = Path(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    analysis_type: str = Query("detailed", pattern="^(detailed|trades|positions|model)$"),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> Dict[str, Any]:
    """Get detailed analysis for specific time period"""
    try:
        period_spec = f"{start_date.isoformat()}:{end_date.isoformat()}"
        analysis = await engine.drill_down_analysis(
            backtest_run_id=backtest_run_id,
            analysis_type="period",
            analysis_target=period_spec
        )
        return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to perform drill-down: {str(e)}")

@app.get("/api/v1/backtests/{backtest_run_id}/drill-down/stock/{symbol}")
async def drill_down_stock(
    backtest_run_id: str = Path(...),
    symbol: str = Path(...),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> Dict[str, Any]:
    """Get detailed analysis for specific stock"""
    try:
        analysis = await engine.drill_down_analysis(
            backtest_run_id=backtest_run_id,
            analysis_type="stock",
            analysis_target=symbol,
            start_date=start_date,
            end_date=end_date
        )
        return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to perform stock drill-down: {str(e)}")

@app.get("/api/v1/backtests/{backtest_run_id}/drill-down/trade/{trade_id}")
async def drill_down_trade(
    backtest_run_id: str = Path(...),
    trade_id: str = Path(...),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
) -> Dict[str, Any]:
    """Get detailed analysis for specific trade"""
    try:
        analysis = await engine.drill_down_analysis(
            backtest_run_id=backtest_run_id,
            analysis_type="trade",
            analysis_target=trade_id
        )
        return analysis
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to perform trade drill-down: {str(e)}")

# Real-time WebSocket endpoints
@app.websocket("/ws/backtests/{backtest_run_id}/portfolio")
async def portfolio_websocket(websocket: WebSocket, backtest_run_id: str):
    """Real-time portfolio performance updates"""
    await connection_manager.connect(websocket, backtest_run_id)
    try:
        # Send initial data
        engine = await get_analytics_engine()
        initial_metrics = await engine.compute_portfolio_metrics(backtest_run_id)
        
        await websocket.send_json({
            "type": "portfolio_metrics",
            "data": initial_metrics.to_dict(),
            "timestamp": datetime.now().isoformat()
        })
        
        # Keep connection alive and handle client messages
        while True:
            try:
                data = await websocket.receive_text()
                # Handle client requests (e.g., filter changes)
                import json
                message = json.loads(data)
                
                if message.get("type") == "request_update":
                    # Send updated metrics
                    metrics = await engine.compute_portfolio_metrics(backtest_run_id)
                    await websocket.send_json({
                        "type": "portfolio_update",
                        "data": metrics.to_dict(),
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
    finally:
        connection_manager.disconnect(websocket, backtest_run_id)

@app.websocket("/ws/backtests/{backtest_run_id}/model")
async def model_websocket(websocket: WebSocket, backtest_run_id: str):
    """Real-time model performance updates"""
    await connection_manager.connect(websocket, backtest_run_id)
    try:
        # Send initial model performance data
        engine = await get_analytics_engine()
        initial_performance = await engine.compute_model_performance(backtest_run_id)
        
        await websocket.send_json({
            "type": "model_performance",
            "data": initial_performance.to_dict(),
            "timestamp": datetime.now().isoformat()
        })
        
        # Keep connection alive
        while True:
            await websocket.receive_text()
            # Handle model-specific requests - data processing not yet implemented
            
    except WebSocketDisconnect:
        pass
    finally:
        connection_manager.disconnect(websocket, backtest_run_id)

# Cache management endpoints
@app.post("/api/v1/cache/invalidate")
async def invalidate_cache(
    pattern: Optional[str] = Query(None, description="Cache key pattern to invalidate"),
    engine: PortfolioAnalyticsEngine = Depends(get_analytics_engine)
):
    """Invalidate cache entries (admin endpoint)"""
    try:
        await engine.invalidate_cache(pattern)
        return {"status": "success", "message": f"Cache invalidated for pattern: {pattern or 'all'}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to invalidate cache: {str(e)}")

# Error handlers
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    uvicorn.run(
        "backtest_analytics_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )