#!/usr/bin/env python3
"""
Wrapper script to run Polygon news backfill with proper arguments
"""

import sys
import os
sys.path.insert(0, '/workspace/src')

def main():
    # Import the news backfill module
    try:
        from market_data.news.turbo_news_backfill import main as news_main
        
        # The arguments will be set by the calling script
        # Default arguments for testing
        if len(sys.argv) < 5:
            print("Usage: script --start_date YYYY-MM-DD --end_date YYYY-MM-DD --limit N --environment env")
            sys.exit(1)
        
        # Run the news backfill
        news_main()
        
    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()