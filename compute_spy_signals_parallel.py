import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from spy_universe import SPYUniverse
from market_data_simulator import simulate_market_data
from signals import extract_all_signals
import pyarrow.parquet as pq
import pyarrow as pa
import os

# Parameters
START_DATE = datetime.utcnow() - timedelta(days=365*10)
END_DATE = datetime.utcnow()
INTERVAL_MINUTES = 5
PARQUET_DIR = 'spy_signals_parquet'
BATCH_SIZE = 288  # One day (12 intervals/hour * 24 hours)
MAX_WORKERS = 8

os.makedirs(PARQUET_DIR, exist_ok=True)

universe = SPYUniverse('spy_membership.csv')

fieldnames = ['datetime', 'symbol', 'bid', 'ask', 'last', 'volume',
              'hour_of_day', 'day_of_week', 'week_of_month',
              'lse_last_open', 'lse_last_close',
              '5m_high', '5m_low', '5m_close', '5m_vwap', '5m_true_range']

def process_batch(batch_start_dt):
    batch = []
    dt = batch_start_dt
    for _ in range(BATCH_SIZE):
        if dt >= END_DATE:
            break
        symbols = universe.get_universe(dt.date())
        for symbol in symbols:
            # Replace with real data loader if available
            ticks = simulate_market_data(symbol, dt, 1, interval_seconds=INTERVAL_MINUTES*60)
            tick = ticks[0]
            tick['hour_of_day'] = tick['time'].hour
            tick['day_of_week'] = tick['time'].weekday()
            tick['week_of_month'] = 1 + (tick['time'].day - 1) // 7
            tick['lse_last_open'] = dt.replace(hour=8, minute=0)
            tick['lse_last_close'] = dt.replace(hour=16, minute=30)
            tick['interval_signals'] = {
                '5m': {
                    'high': tick['last'] + 1,
                    'low': tick['last'] - 1,
                    'close': tick['last'],
                    'vwap': tick['last'],
                    'true_range': 2
                }
            }
            signals = extract_all_signals(tick)
            signals['datetime'] = dt
            signals['symbol'] = symbol
            row = {k: signals.get(k, None) for k in fieldnames}
            batch.append(row)
        dt += timedelta(minutes=INTERVAL_MINUTES)
    # Write batch to Parquet
    if batch:
        table = pa.Table.from_pandas(pd.DataFrame(batch))
        pq.write_table(table, f"{PARQUET_DIR}/signals_{batch_start_dt.strftime('%Y%m%d_%H%M')}.parquet")
    return len(batch)

def main():
    dt = START_DATE
    batch_starts = []
    while dt < END_DATE:
        batch_starts.append(dt)
        dt += timedelta(minutes=BATCH_SIZE * INTERVAL_MINUTES)
    total_rows = 0
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_batch, batch_start): batch_start for batch_start in batch_starts}
        for f in as_completed(futures):
            processed = f.result()
            total_rows += processed
            print(f"Processed batch starting {futures[f]}: {processed} rows")
    print(f"Total rows written: {total_rows}")

if __name__ == '__main__':
    main()
