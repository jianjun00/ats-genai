#!/usr/bin/env python3
"""
Wrapper script to run Tiingo instrument population with specific tickers
"""

import sys
sys.path.append('/workspace/src')

import subprocess
import os

def main():
    # Set Tiingo API key from environment
    tiingo_key = os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5')
    
    # Set environment variable for the subprocess
    env = os.environ.copy()
    env['TIINGO_API_KEY'] = tiingo_key
    
    # Run the populate script with popular tickers
    cmd = [
        'python', '/workspace/src/secmaster/populate_instrument_tiingo.py',
        '--ticker', 'AAPL,MSFT,GOOGL,TSLA,NVDA,AMZN,META,NFLX,AMD,CRM',
        '--environment', 'dev',
        '--debug'
    ]
    
    print("🚀 Running Tiingo instrument population...")
    result = subprocess.run(cmd, env=env)
    
    if result.returncode == 0:
        print("✅ Tiingo instrument population completed successfully")
    else:
        print(f"❌ Tiingo instrument population failed with code: {result.returncode}")
        sys.exit(1)

if __name__ == "__main__":
    main()