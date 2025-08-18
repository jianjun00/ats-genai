#!/usr/bin/env python3
"""
Script to demonstrate accessing real analytics from the system
"""

import asyncio
import sys
from pathlib import Path
from datetime import date, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from analytics.portfolio_analytics import PortfolioAnalyticsEngine
from config.environment import Environment

async def check_real_analytics():
    """Check real analytics data"""
    
    print("🔍 Checking Real Analytics Access Points...")
    
    # Initialize with real database
    env = Environment()
    print(f"📊 Database URL: {env.get_database_url()}")
    
    # Initialize analytics engine with real database
    engine = PortfolioAnalyticsEngine(env=env)
    await engine.initialize()
    print("✅ Analytics engine connected to real database")
    
    # Example: Get portfolio metrics for a backtest run
    # Note: You'll need actual backtest_run_ids from your database
    try:
        # This would fetch real metrics if you have backtest data
        print("\n📈 Available Analytics:")
        print("1. Portfolio Performance Metrics")
        print("2. Attribution Analysis") 
        print("3. Model Performance Tracking")
        print("4. Risk Analytics")
        print("5. Factor Exposure Analysis")
        
        print("\n🎯 To access real analytics:")
        print("1. Run production backtest to generate data")
        print("2. Query via API endpoints")
        print("3. Use analytics engine directly")
        
    except Exception as e:
        print(f"⚠️ Note: {e}")
        print("💡 Run production backtests first to generate analytics data")
    
    await engine.close()

if __name__ == "__main__":
    asyncio.run(check_real_analytics())