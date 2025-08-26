#!/usr/bin/env python3
"""
Wrapper script to run EODHD instrument population with specific tickers
"""

import sys
sys.path.append('/workspace/src')

import subprocess
import os

def main():
    # Set EODHD API key from environment
    eodhd_key = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')
    
    # Set environment variable for the subprocess
    env = os.environ.copy()
    env['EODHD_API_KEY'] = eodhd_key
    
    # Run the populate script with popular tickers
    cmd = [
        'python', '/workspace/src/secmaster/populate_instrument_eodhd.py',
        '--ticker', 'AAPL.US,MSFT.US,GOOGL.US,TSLA.US,NVDA.US,AMZN.US,META.US,NFLX.US,AMD.US,CRM.US',
        '--environment', 'dev',
        '--debug'
    ]
    
    print("🚀 Running EODHD instrument population...")
    result = subprocess.run(cmd, env=env)
    
    if result.returncode == 0:
        print("✅ EODHD instrument population completed successfully")
    else:
        print(f"❌ EODHD instrument population failed with code: {result.returncode}")
        sys.exit(1)

if __name__ == "__main__":
    main()