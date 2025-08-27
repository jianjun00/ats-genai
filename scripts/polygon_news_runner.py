#!/usr/bin/env python3
"""
Polygon News Runner - Environment Variable Based

Runs Polygon news backfill using environment variables instead of command line arguments.
This works better with the run_dev.py Docker system.

Environment Variables:
- POLYGON_API_KEY: Required Polygon API key
- NEWS_START_DATE: Start date (YYYY-MM-DD), defaults to 7 days ago
- NEWS_END_DATE: End date (YYYY-MM-DD), defaults to today
- NEWS_LIMIT: Max articles per symbol, defaults to 5000
- NEWS_ENVIRONMENT: Environment (dev/intg/prod), defaults to dev
"""

import os
import sys
from datetime import datetime, timedelta

# Add src to Python path
sys.path.insert(0, '/workspace/src')

def main():
    try:
        # Get configuration from environment
        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            print("❌ POLYGON_API_KEY environment variable required")
            sys.exit(1)
        
        # Date defaults
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        
        start_date = os.getenv('NEWS_START_DATE', week_ago.strftime('%Y-%m-%d'))
        end_date = os.getenv('NEWS_END_DATE', today.strftime('%Y-%m-%d'))
        limit = int(os.getenv('NEWS_LIMIT', '5000'))
        environment = os.getenv('NEWS_ENVIRONMENT', 'dev')
        
        print(f"🚀 Starting Polygon News Backfill")
        print(f"📅 Date Range: {start_date} to {end_date}")
        print(f"📊 Max Articles per Symbol: {limit:,}")
        print(f"🌍 Environment: {environment}")
        
        # Set up sys.argv to match what turbo_news_backfill expects
        sys.argv = [
            "polygon_news_runner.py",
            "--start_date", start_date,
            "--end_date", end_date,
            "--limit", str(limit),
            "--environment", environment
        ]
        
        # Import and run the news backfill
        import asyncio
        from market_data.news.turbo_news_backfill import main as news_main
        
        # Run the async main function
        asyncio.run(news_main())
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're running in the Docker environment with all dependencies")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()