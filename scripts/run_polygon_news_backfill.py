#!/usr/bin/env python3
"""
Polygon News Backfill Runner

Simplified interface to populate Polygon news data with sensible defaults.
Supports both recent news and comprehensive historical backfills.

Usage:
    python3 scripts/run_polygon_news_backfill.py --days 30
    python3 scripts/run_polygon_news_backfill.py --years 2
    python3 scripts/run_polygon_news_backfill.py --start-date 2023-01-01 --end-date 2024-01-01
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime, timedelta

def main():
    parser = argparse.ArgumentParser(description="Run Polygon news backfill with simplified interface")
    
    # Time range options (mutually exclusive)
    time_group = parser.add_mutually_exclusive_group(required=True)
    time_group.add_argument('--days', type=int, help='Backfill last N days of news')
    time_group.add_argument('--years', type=int, help='Backfill last N years of news')
    time_group.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--limit', type=int, default=10000, help='Max articles per symbol (default: 10000)')
    parser.add_argument('--environment', default='dev', choices=['dev', 'intg', 'prod'], 
                       help='Environment (default: dev)')
    
    args = parser.parse_args()
    
    # Calculate date range
    today = datetime.now().date()
    
    if args.days:
        start_date = today - timedelta(days=args.days)
        end_date = today
    elif args.years:
        start_date = today - timedelta(days=args.years * 365)
        end_date = today
    else:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else today
    
    # Validate API key
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print("❌ POLYGON_API_KEY environment variable not set")
        print("   Please set it in your .env file or environment")
        sys.exit(1)
    
    # Build command arguments
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"🚀 Starting Polygon News Backfill")
    print(f"📅 Date Range: {start_str} to {end_str}")
    print(f"📊 Max Articles per Symbol: {args.limit:,}")
    print(f"🌍 Environment: {args.environment}")
    
    # Build the run_dev.py command
    script_path = "src/market_data/news/turbo_news_backfill.py"
    
    # Set environment variables for the Docker container
    env_vars = {
        "POLYGON_API_KEY": api_key,
        "NEWS_START_DATE": start_str,
        "NEWS_END_DATE": end_str,
        "NEWS_LIMIT": str(args.limit),
        "NEWS_ENVIRONMENT": args.environment
    }
    
    cmd = [
        "python3", "scripts/run_dev.py", "run",
        "--script", script_path,
        "--env", f'{{"POLYGON_API_KEY": "{api_key}"}}',
        "--", 
        "--start_date", start_str,
        "--end_date", end_str,
        "--limit", str(args.limit),
        "--environment", args.environment
    ]
    
    print(f"🔧 Running command: {' '.join(cmd[:6])} [args...]")
    
    try:
        # Create a temporary script that calls the news backfill with arguments
        temp_script_content = f'''#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, '/workspace/src')

# Import and run the turbo news backfill
from market_data.news.turbo_news_backfill import main

if __name__ == "__main__":
    # Set up sys.argv to match expected arguments
    sys.argv = [
        "turbo_news_backfill.py",
        "--start_date", "{start_str}",
        "--end_date", "{end_str}",
        "--limit", "{args.limit}",
        "--environment", "{args.environment}"
    ]
    main()
'''
        
        # Write temporary script
        temp_script_path = "/tmp/run_news_backfill.py"
        with open(temp_script_path, 'w') as f:
            f.write(temp_script_content)
        
        os.chmod(temp_script_path, 0o755)
        
        # Run via run_dev.py with the temporary script
        run_dev_cmd = [
            "python3", "scripts/run_dev.py", "run",
            "--script", temp_script_path,
            "--env", f'{{"POLYGON_API_KEY": "{api_key}"}}'
        ]
        
        print("🐳 Running news backfill via Docker...")
        result = subprocess.run(run_dev_cmd)
        
        if result.returncode == 0:
            print("✅ News backfill completed successfully!")
        else:
            print(f"❌ News backfill failed with exit code: {result.returncode}")
            sys.exit(result.returncode)
            
    except KeyboardInterrupt:
        print("🛑 News backfill interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()