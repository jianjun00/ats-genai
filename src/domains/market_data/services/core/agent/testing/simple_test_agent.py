#!/usr/bin/env python
"""
Simplified test script for the Instrument Data Agent DAO methods.
This script tests the DAO methods we added without relying on the full agent implementation.
"""

import asyncio
import json
from datetime import datetime

from src.core.shared.utils.environment import Environment
from src.core.dao.instruments_dao import InstrumentsDAO
from src.core.dao.instrument_xrefs_dao import InstrumentXrefsDAO
from src.core.dao.instrument_polygon_dao import InstrumentPolygonDAO


async def test_dao_methods():
    """Test the DAO methods we added for the instrument data agent."""
    env = Environment()

    # Initialize DAOs
    instruments_dao = InstrumentsDAO(env)
    instrument_xrefs_dao = InstrumentXrefsDAO(env)
    instrument_polygon_dao = InstrumentPolygonDAO(env)

    # Test count methods
    print("=== Testing DAO Count Methods ===")

    try:
        instruments_count = await instruments_dao.count_instruments()
        print(f"Instruments count: {instruments_count}")
    except Exception as e:
        print(f"Error counting instruments: {str(e)}")

    try:
        xrefs_count = await instrument_xrefs_dao.count_xrefs()
        print(f"Instrument xrefs count: {xrefs_count}")
    except Exception as e:
        print(f"Error counting xrefs: {str(e)}")

    try:
        polygon_count = await instrument_polygon_dao.count_instruments()
        print(f"Instrument polygon count: {polygon_count}")
    except Exception as e:
        print(f"Error counting polygon instruments: {str(e)}")

    # Test timestamp method
    print("\n=== Testing Latest Update Timestamp ===")

    try:
        latest_timestamp = await instrument_polygon_dao.get_latest_update_timestamp()
        print(f"Latest update timestamp: {latest_timestamp}")
    except Exception as e:
        print(f"Error getting latest timestamp: {str(e)}")

    # Return stats
    return {
        "instruments_count": instruments_count if 'instruments_count' in locals() else None,
        "xrefs_count": xrefs_count if 'xrefs_count' in locals() else None,
        "polygon_count": polygon_count if 'polygon_count' in locals() else None,
        "latest_timestamp": latest_timestamp if 'latest_timestamp' in locals() else None
    }


async def main():
    """Run all tests."""
    print("=== Testing Instrument Data Agent DAO Methods ===")
    print(f"Current time: {datetime.now()}")

    try:
        stats = await test_dao_methods()
        print("\n=== Summary ===")
        print(json.dumps(stats, indent=2, default=str))
        print("\n✅ All tests completed!")
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
