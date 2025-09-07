#!/usr/bin/env python3
"""
Test script for Historic News Signal Extraction Service

This script tests the news signal extraction pipeline with a small batch
of historic news data to verify everything works correctly.
"""

import asyncio
import sys
import os
sys.path.insert(0, 'src')

from services.historic_news_signal_extractor import HistoricNewsSignalExtractor
from datetime import datetime

async def test_extraction():
    """Test the historic news signal extraction with a small batch."""

    print("🧪 Testing Historic News Signal Extraction")
    print("=" * 60)

    extractor = HistoricNewsSignalExtractor()

    try:
        # Initialize the service
        print("🔧 Initializing extractor...")
        await extractor.initialize()

        print("✅ Extractor initialized successfully!")

        # Test with a very small batch (10 records from 2024)
        print("\n📊 Processing test batch...")
        results = await extractor.process_historic_news(
            start_date=datetime(2024, 8, 1),
            end_date=datetime(2024, 8, 31),
            limit=10  # Small test batch
        )

        print(f"\n🎉 Processing completed!")
        print(f"Results: {results}")

        # Check results in database
        print("\n🔍 Checking database results...")
        status = await extractor.get_processing_status()
        print(f"Service status: {status}")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await extractor.close()
        print("\n✅ Test completed")

if __name__ == "__main__":
    # Set environment variables for dev database
    os.environ['ENVIRONMENT'] = 'dev'
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '3432'
    os.environ['DB_USER'] = 'postgres'
    os.environ['DB_PASSWORD'] = 'dev_password'
    os.environ['DB_NAME'] = 'dev_db'

    asyncio.run(test_extraction())