#!/usr/bin/env python3
"""
Current Portfolio API - Real-time portfolio management and tracking
Runs on port 8001 to complement the backtest analytics API on port 8000
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import random
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Current Portfolio API",
    description="Real-time portfolio management and tracking system",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data Models
class Holding(BaseModel):
    symbol: str
    shares: float
    price: float
    market_value: float
    weight: float
    daily_pnl: float
    daily_return: float
    cost_basis: float
    unrealized_pnl: float
    sector: str

class PerformanceMetrics(BaseModel):
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    beta: float
    var_95: float
    win_rate: float

class PortfolioData(BaseModel):
    total_portfolio_value: float
    daily_return: float
    total_return: float
    cash_position: float
    invested_amount: float
    holdings: List[Holding]
    sector_allocation: Dict[str, float]
    performance_metrics: PerformanceMetrics
    last_updated: str

class ComparisonDeviation(BaseModel):
    symbol: str
    current: float
    target: float
    deviation: float

class DeviationAnalysis(BaseModel):
    over_allocated: List[ComparisonDeviation]
    under_allocated: List[ComparisonDeviation]
    alignment_score: float

class PerformanceComparison(BaseModel):
    current_ytd: float
    strategy_ytd: float
    current_sharpe: float
    strategy_sharpe: float
    current_volatility: float
    strategy_volatility: float
    tracking_error: float

class RebalancingRecommendation(BaseModel):
    action: str
    symbol: str
    current_weight: float
    target_weight: float
    amount: float

class PortfolioComparison(BaseModel):
    current_allocation: Dict[str, float]
    strategy_allocation: Dict[str, float]
    deviation_analysis: DeviationAnalysis
    performance_comparison: PerformanceComparison
    rebalancing_recommendations: List[RebalancingRecommendation]
    risk_attribution: Dict[str, Dict[str, float]]

# Database connection
import asyncpg

async def get_database_pool():
    """Get database connection pool"""
    try:
        return await asyncpg.create_pool(
            host="localhost",
            port=5433,
            user="postgres", 
            password="postgres",
            database="dev_db",
            min_size=1,
            max_size=5
        )
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        return None

# Global database pool
db_pool = None

# Strategy allocations for comparison
STRATEGY_ALLOCATIONS = {
    "comprehensive_2022_2025": {
        "AAPL": 0.15, "MSFT": 0.20, "GOOGL": 0.12, "NVDA": 0.10,
        "AMZN": 0.18, "TSLA": 0.08, "META": 0.10, "JPM": 0.05, "JNJ": 0.02
    },
    "adaptive_sr_2024": {
        "AAPL": 0.12, "MSFT": 0.15, "GOOGL": 0.10, "NVDA": 0.08,
        "TSLA": 0.15, "AMZN": 0.12, "META": 0.08, "JPM": 0.08,
        "V": 0.06, "UNH": 0.06
    },
    "momentum_2024": {
        "AAPL": 0.20, "MSFT": 0.18, "NVDA": 0.15, "GOOGL": 0.12,
        "AMZN": 0.10, "META": 0.08, "TSLA": 0.10, "NFLX": 0.07
    }
}

def get_real_time_prices(symbols: List[str]) -> Dict[str, float]:
    """Get simulated real-time prices (mock data for demo)"""
    base_prices = {
        "AAPL": 175.50, "MSFT": 310.25, "GOOGL": 140.80,
        "NVDA": 520.75, "JPM": 155.60, "JNJ": 158.45
    }
    
    prices = {}
    for symbol in symbols:
        base_price = base_prices.get(symbol, 100.0)
        # Add some random variation to simulate real-time changes
        variation = random.uniform(-0.02, 0.02)  # ±2% variation
        prices[symbol] = base_price * (1 + variation)
    
    return prices

def calculate_portfolio_metrics(holdings: List[Dict[str, Any]], total_value: float) -> PerformanceMetrics:
    """Calculate portfolio risk and performance metrics"""
    # Mock calculations for demonstration
    return PerformanceMetrics(
        volatility=0.18,
        sharpe_ratio=1.35,
        max_drawdown=-0.08,
        beta=1.05,
        var_95=-15000.0,
        win_rate=0.62
    )

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint"""
    return {"message": "Current Portfolio API is running", "version": "1.0.0"}

@app.get("/api/v1/portfolio/status", tags=["Health"])
async def portfolio_status():
    """Check portfolio service status"""
    return {
        "status": "healthy",
        "service": "current_portfolio_api",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get("/api/v1/portfolio/current", response_model=PortfolioData, tags=["Portfolio"])
async def get_current_portfolio():
    """Get current portfolio holdings and metrics"""
    global db_pool
    
    if not db_pool:
        db_pool = await get_database_pool()
    
    try:
        # Get portfolio holdings from database
        if db_pool:
            async with db_pool.acquire() as conn:
                # Get current holdings
                holdings_rows = await conn.fetch("""
                    SELECT symbol, shares, cost_basis, sector, purchase_date
                    FROM dev_current_portfolio_holdings
                    ORDER BY symbol
                """)
                
                # Get cash position
                cash_rows = await conn.fetch("""
                    SELECT cash_position FROM dev_current_portfolio_metadata
                    ORDER BY last_updated DESC LIMIT 1
                """)
                
                cash_position = float(cash_rows[0]['cash_position']) if cash_rows else 125000.0
        else:
            # Fallback to mock data if database not available
            holdings_rows = [
                {'symbol': 'AAPL', 'shares': 1500, 'cost_basis': 150.00, 'sector': 'Technology'},
                {'symbol': 'MSFT', 'shares': 800, 'cost_basis': 280.00, 'sector': 'Technology'},
                {'symbol': 'GOOGL', 'shares': 600, 'cost_basis': 120.00, 'sector': 'Technology'},
                {'symbol': 'NVDA', 'shares': 400, 'cost_basis': 450.00, 'sector': 'Technology'},
                {'symbol': 'JPM', 'shares': 900, 'cost_basis': 140.00, 'sector': 'Financial'},
                {'symbol': 'JNJ', 'shares': 1200, 'cost_basis': 150.00, 'sector': 'Healthcare'}
            ]
            cash_position = 125000.0
        
        # Get current prices for all symbols
        symbols = [row['symbol'] for row in holdings_rows]
        prices = get_real_time_prices(symbols)
        
        holdings = []
        total_market_value = 0
        total_cost_basis = 0
        
        for row in holdings_rows:
            symbol = row['symbol']
            shares = float(row['shares'])
            cost_basis = float(row['cost_basis'])
            current_price = prices.get(symbol, cost_basis * 1.1)  # Fallback
            
            market_value = shares * current_price
            total_market_value += market_value
            position_cost = shares * cost_basis
            total_cost_basis += position_cost
            
            # Simulate daily price change (would be calculated from actual price data)
            daily_change = random.uniform(-0.05, 0.05)
            daily_pnl = market_value * daily_change
            daily_return = daily_change
            
            unrealized_pnl = market_value - position_cost
            
            holdings.append(Holding(
                symbol=symbol,
                shares=shares,
                price=current_price,
                market_value=market_value,
                weight=0,  # Will be calculated after total
                daily_pnl=daily_pnl,
                daily_return=daily_return,
                cost_basis=cost_basis,
                unrealized_pnl=unrealized_pnl,
                sector=row['sector']
            ))
        
        # Calculate total portfolio value
        total_portfolio_value = total_market_value + cash_position
        
        # Update weights
        for holding in holdings:
            holding.weight = holding.market_value / total_portfolio_value
        
        # Calculate sector allocation
        sector_allocation = {}
        for holding in holdings:
            sector = holding.sector
            if sector not in sector_allocation:
                sector_allocation[sector] = 0
            sector_allocation[sector] += holding.weight
        
        # Add cash allocation
        sector_allocation["Cash"] = cash_position / total_portfolio_value
        
        # Calculate returns
        total_return = (total_portfolio_value - total_cost_basis - cash_position) / (total_cost_basis + cash_position)
        daily_return = sum(h.daily_pnl for h in holdings) / total_portfolio_value
        
        # Calculate performance metrics
        performance_metrics = calculate_portfolio_metrics(
            [h.dict() for h in holdings], 
            total_portfolio_value
        )
        
        return PortfolioData(
            total_portfolio_value=total_portfolio_value,
            daily_return=daily_return,
            total_return=total_return,
            cash_position=cash_position,
            invested_amount=total_market_value,
            holdings=holdings,
            sector_allocation=sector_allocation,
            performance_metrics=performance_metrics,
            last_updated=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error fetching current portfolio: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch portfolio data: {str(e)}")

@app.get("/api/v1/portfolio/compare/{strategy_id}", response_model=PortfolioComparison, tags=["Comparison"])
async def compare_portfolio_to_strategy(strategy_id: str):
    """Compare current portfolio allocation to a backtest strategy"""
    try:
        if strategy_id not in STRATEGY_ALLOCATIONS:
            raise HTTPException(status_code=404, detail=f"Strategy {strategy_id} not found")
        
        # Get current portfolio
        current_portfolio = await get_current_portfolio()
        
        # Get current allocation
        current_allocation = {}
        for holding in current_portfolio.holdings:
            current_allocation[holding.symbol] = holding.weight
        current_allocation["Cash"] = current_portfolio.cash_position / current_portfolio.total_portfolio_value
        
        # Get strategy allocation
        strategy_allocation = STRATEGY_ALLOCATIONS[strategy_id]
        
        # Calculate deviations
        all_symbols = set(list(current_allocation.keys()) + list(strategy_allocation.keys()))
        over_allocated = []
        under_allocated = []
        
        total_deviation = 0
        for symbol in all_symbols:
            current_weight = current_allocation.get(symbol, 0)
            target_weight = strategy_allocation.get(symbol, 0)
            deviation = current_weight - target_weight
            
            total_deviation += abs(deviation)
            
            if deviation > 0.01:  # Over-allocated by more than 1%
                over_allocated.append(ComparisonDeviation(
                    symbol=symbol,
                    current=current_weight,
                    target=target_weight,
                    deviation=deviation
                ))
            elif deviation < -0.01:  # Under-allocated by more than 1%
                under_allocated.append(ComparisonDeviation(
                    symbol=symbol,
                    current=current_weight,
                    target=target_weight,
                    deviation=deviation
                ))
        
        # Calculate alignment score (higher is better)
        alignment_score = max(0, 1 - (total_deviation / 2))
        
        # Generate rebalancing recommendations
        recommendations = []
        portfolio_value = current_portfolio.total_portfolio_value
        
        for deviation in over_allocated + under_allocated:
            if abs(deviation.deviation) > 0.01:  # Only recommend if deviation > 1%
                action = "SELL" if deviation.deviation > 0 else "BUY"
                amount = -deviation.deviation * portfolio_value
                
                recommendations.append(RebalancingRecommendation(
                    action=action,
                    symbol=deviation.symbol,
                    current_weight=deviation.current,
                    target_weight=deviation.target,
                    amount=amount
                ))
        
        # Sort by absolute amount
        recommendations.sort(key=lambda x: abs(x.amount), reverse=True)
        
        return PortfolioComparison(
            current_allocation=current_allocation,
            strategy_allocation=strategy_allocation,
            deviation_analysis=DeviationAnalysis(
                over_allocated=over_allocated,
                under_allocated=under_allocated,
                alignment_score=alignment_score
            ),
            performance_comparison=PerformanceComparison(
                current_ytd=current_portfolio.total_return,
                strategy_ytd=0.32,  # Mock strategy performance
                current_sharpe=current_portfolio.performance_metrics.sharpe_ratio,
                strategy_sharpe=1.58,  # Mock strategy Sharpe
                current_volatility=current_portfolio.performance_metrics.volatility,
                strategy_volatility=0.16,  # Mock strategy volatility
                tracking_error=0.08
            ),
            rebalancing_recommendations=recommendations,
            risk_attribution={
                "sector_risk_current": dict(current_portfolio.sector_allocation),
                "sector_risk_strategy": {
                    "Technology": 0.82,
                    "Financials": 0.08,
                    "Healthcare": 0.05,
                    "Consumer": 0.05
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing portfolio to strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compare portfolio: {str(e)}")

@app.get("/api/v1/portfolio/strategies", tags=["Comparison"])
async def get_available_strategies():
    """Get list of available strategies for comparison"""
    return {
        "strategies": [
            {"id": "comprehensive_2022_2025", "name": "Comprehensive Strategy 2022-2025"},
            {"id": "adaptive_sr_2024", "name": "Adaptive Support/Resistance 2024"},
            {"id": "momentum_2024", "name": "Momentum Strategy 2024"}
        ]
    }

if __name__ == "__main__":
    logger.info("Starting Current Portfolio API on port 8001...")
    uvicorn.run(
        "current_portfolio_api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )