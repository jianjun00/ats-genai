import asyncio
import os
import asyncpg
from datetime import datetime
import json

# Set your TimescaleDB/Postgres connection string as an environment variable TSDB_URL
DB_URL = os.getenv('TSDB_URL', 'postgresql://user:password@localhost:5432/yourdb')

async def test_timescaledb_setup():
    print("Connecting to TimescaleDB...")
    pool = await asyncpg.create_pool(DB_URL)
    async with pool.acquire() as conn:
        # Check for table existence
        market_data_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'market_data'
            )
        """)
        signals_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'signals'
            )
        """)
        assert market_data_exists, "market_data table does not exist!"
        assert signals_exists, "signals table does not exist!"
        print("Tables exist.")

        # Insert and read test row for market_data
        now = datetime.utcnow()
        await conn.execute(
            """
            INSERT INTO market_data (time, symbol, bid, ask, last)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (time, symbol) DO NOTHING
            """,
            now, 'TEST', 100.0, 101.0, 100.5
        )
        row = await conn.fetchrow(
            "SELECT * FROM market_data WHERE symbol = $1 ORDER BY time DESC LIMIT 1", 'TEST'
        )
        assert row is not None, "Failed to insert/read from market_data!"
        print("market_data insert/read OK.")

        # Insert and read test row for signals
        signal = {'action': 'BUY', 'quantity': 1}
        await conn.execute(
            """
            INSERT INTO signals (time, symbol, signal)
            VALUES ($1, $2, $3)
            ON CONFLICT (time, symbol) DO NOTHING
            """,
            now, 'TEST', json.dumps(signal)
        )
        row = await conn.fetchrow(
            "SELECT * FROM signals WHERE symbol = $1 ORDER BY time DESC LIMIT 1", 'TEST'
        )
        assert row is not None, "Failed to insert/read from signals!"
        print("signals insert/read OK.")

    await pool.close()
    print("TimescaleDB setup verified successfully.")

import pandas as pd

def test_ema_20():
    # Create a sample DataFrame with 30 closing prices
    closes = [i for i in range(1, 31)]  # 1, 2, ..., 30
    df = pd.DataFrame({'close': closes})
    # Compute EMA 20 using pandas
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    pandas_ema = df['ema_20'].iloc[-1]
    assert not pd.isna(pandas_ema), "EMA20 is NaN!"

    # Manually compute EMA20 using the recursive formula
    alpha = 2 / (20 + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = alpha * price + (1 - alpha) * ema
    manual_ema = ema

    print(f"EMA20 computed by pandas: {pandas_ema}")
    print(f"EMA20 computed manually: {manual_ema}")

    # Assert that pandas and manual EMA values are very close
    assert abs(pandas_ema - manual_ema) < 1e-8, f"EMA mismatch: pandas={pandas_ema}, manual={manual_ema}"
    print("EMA20 calculation matches between pandas and manual formula. Test passed.")

import asyncio

class MockMarketDataStreamer:
    def __init__(self, events):
        self.events = events
    async def stream_ticks(self):
        for event in self.events:
            await asyncio.sleep(0)  # Simulate async
            yield event

async def test_streaming_no_updates():
    print("Test: No bid/trade update (empty stream)")
    streamer = MockMarketDataStreamer([])
    count = 0
    async for tick in streamer.stream_ticks():
        count += 1
    assert count == 0, "Should not receive any ticks for empty stream!"
    print("Passed: No updates processed.")

async def test_streaming_multiple_updates():
    print("Test: Multiple bid/trade updates")
    events = [
        {'symbol': 'AAPL', 'bid': 100.0, 'ask': 101.0, 'last': 100.5, 'time': '2023-01-01T10:00:00Z'},
        {'symbol': 'AAPL', 'bid': 100.1, 'ask': 101.2, 'last': 100.7, 'time': '2023-01-01T10:00:01Z'},
        {'symbol': 'AAPL', 'bid': 100.2, 'ask': 101.3, 'last': 100.9, 'time': '2023-01-01T10:00:02Z'}
    ]
    streamer = MockMarketDataStreamer(events)
    received = []
    async for tick in streamer.stream_ticks():
        received.append(tick)
    assert len(received) == 3, f"Expected 3 ticks, got {len(received)}"
    assert received[0]['bid'] == 100.0 and received[-1]['bid'] == 100.2
    print("Passed: Multiple updates processed.")

async def test_streaming_partial_and_mixed_updates():
    print("Test: Only bid, only trade, and mixed updates")
    events = [
        {'symbol': 'AAPL', 'bid': 100.0, 'time': '2023-01-01T10:00:00Z'}, # Only bid
        {'symbol': 'AAPL', 'last': 100.5, 'time': '2023-01-01T10:00:01Z'}, # Only trade
        {'symbol': 'AAPL', 'bid': 100.2, 'last': 100.7, 'time': '2023-01-01T10:00:02Z'}, # Both
    ]
    streamer = MockMarketDataStreamer(events)
    bids, trades, mixed = 0, 0, 0
    async for tick in streamer.stream_ticks():
        if 'bid' in tick and 'last' in tick:
            mixed += 1
        elif 'bid' in tick:
            bids += 1
        elif 'last' in tick:
            trades += 1
    assert bids == 1 and trades == 1 and mixed == 1
    print("Passed: Partial and mixed updates processed.")

async def test_streaming_malformed_updates():
    print("Test: Malformed or missing fields")
    events = [
        {},  # Empty dict
        {'symbol': 'AAPL'},  # Missing price fields
        {'bid': 100.0},  # Missing symbol
        {'symbol': 'AAPL', 'bid': None, 'last': None, 'time': None}  # All fields None
    ]
    streamer = MockMarketDataStreamer(events)
    count = 0
    async for tick in streamer.stream_ticks():
        # Handler should not crash; just count events
        count += 1
    assert count == 4, f"Expected 4 malformed events, got {count}"
    print("Passed: Malformed updates handled.")

async def test_streaming_high_frequency_burst():
    print("Test: High-frequency burst")
    events = [
        {'symbol': 'AAPL', 'bid': 100.0 + i, 'last': 100.5 + i, 'time': f'2023-01-01T10:00:{i:02d}Z'}
        for i in range(100)
    ]
    streamer = MockMarketDataStreamer(events)
    count = 0
    async for tick in streamer.stream_ticks():
        count += 1
    assert count == 100, f"Expected 100 ticks, got {count}"
    print("Passed: High-frequency burst processed.")

async def run_all_streaming_tests():
    await test_streaming_no_updates()
    await test_streaming_multiple_updates()
    await test_streaming_partial_and_mixed_updates()
    await test_streaming_malformed_updates()
    await test_streaming_high_frequency_burst()
    print("All streaming market data tests passed.")

if __name__ == "__main__":
    asyncio.run(test_timescaledb_setup())
    test_ema_20()
    asyncio.run(run_all_streaming_tests())
