#!/usr/bin/env python
"""
Test script for the Instrument Data Agent.
This script tests the basic functionality of the InstrumentDataAgent class.
"""

import asyncio
import json
import os
from datetime import datetime

from core.shared.utils.environment import Environment
from src.market_data.agent.instrument_data_agent import InstrumentDataAgent


async def test_agent_stats():
    """Test the agent's ability to gather statistics about instruments."""
    env = Environment()
    agent = InstrumentDataAgent(env)

    # Get stats
    stats = await agent.get_instrument_stats()

    # Print stats
    print("Instrument Statistics:")
    print(json.dumps(stats, indent=2, default=str))

    return stats


async def test_agent_get_instrument():
    """Test the agent's ability to get instrument data by symbol."""
    env = Environment()
    agent = InstrumentDataAgent(env)

    # Test symbols
    symbols = ["AAPL", "MSFT", "GOOGL"]

    for symbol in symbols:
        print(f"\nFetching instrument data for {symbol}:")
        instrument = await agent.get_instrument_by_symbol(symbol)
        if instrument:
            print(json.dumps({
                "symbol": instrument["symbol"],
                "name": instrument["name"],
                "exchange": instrument["exchange"],
                "type": instrument["type"],
                "currency": instrument["currency"]
            }, indent=2))
        else:
            print(f"No data found for {symbol}")


async def test_agent_plan_execution():
    """Test the agent's ability to execute a simple plan."""
    env = Environment()
    agent = InstrumentDataAgent(env)

    # Create a test plan
    test_plan = {
        "name": "test_plan",
        "steps": [
            {
                "name": "get_stats",
                "status": "pending",
                "result": None
            }
        ]
    }

    # Execute the plan
    print("\nExecuting test plan:")
    await agent.execute_plan(test_plan)

    # Print plan results
    print("Plan execution results:")
    print(json.dumps(test_plan, indent=2, default=str))


async def test_agent_report_generation():
    """Test the agent's ability to generate a report."""
    env = Environment()
    agent = InstrumentDataAgent(env)

    # Generate a report
    print("\nGenerating test report:")
    report = await agent.generate_report("test_report")

    # Print report path
    print(f"Report saved to: {report}")

    # Print report contents
    if os.path.exists(report):
        with open(report, 'r') as f:
            report_data = json.load(f)
            print(json.dumps(report_data, indent=2, default=str))


async def main():
    """Run all tests."""
    print("=== Testing Instrument Data Agent ===")
    print(f"Current time: {datetime.now()}")

    try:
        await test_agent_stats()
        await test_agent_get_instrument()
        await test_agent_plan_execution()
        await test_agent_report_generation()
        print("\n✅ All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
