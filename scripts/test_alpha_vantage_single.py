#!/usr/bin/env python3
"""
Quick test script for Alpha Vantage collector with single symbol.
"""

import sys
sys.path.append('/workspace/src')
import os

# Set demo API key if not already set
if not os.environ.get("ALPHA_VANTAGE_API_KEY"):
    os.environ["ALPHA_VANTAGE_API_KEY"] = "demo"

# Import and run the collector
from alpha_vantage_events_collector import AlphaVantageEventsCollector
import asyncio
import logging

async def main():
    try:
        collector = AlphaVantageEventsCollector("demo")
        await collector.run_collection(
            years_back=2, 
            limit=1, 
            specific_symbols=['AAPL'],
            earnings_only=True
        )
        collector.log_final_summary()
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())