from datetime import datetime, timedelta
from spy_universe import SPYUniverse
from market_data_simulator import simulate_market_data
from signals import extract_all_signals
import csv

# Parameters
START_DATE = datetime.utcnow() - timedelta(days=365*10)
END_DATE = datetime.utcnow()
INTERVAL_MINUTES = 5

# Load SPY universe membership
universe = SPYUniverse('spy_membership.csv')

# Prepare output CSV
with open('spy_signals_5min_sample.csv', 'w', newline='') as f:
    fieldnames = ['datetime', 'symbol'] + [
        'bid', 'ask', 'last', 'volume', 'hour_of_day', 'day_of_week', 'week_of_month',
        'lse_last_open', 'lse_last_close',
        '5m_high', '5m_low', '5m_close', '5m_vwap', '5m_true_range'
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    dt = START_DATE
    # For demo: only process the first 7 days of the last 10 years
    for _ in range(7 * 24 * 12):  # 7 days, 12 intervals/hour, 24 hours/day
        symbols = universe.get_universe(dt.date())
        for symbol in symbols:
            # Simulate a 5-min bar for this symbol
            ticks = simulate_market_data(symbol, dt, 1, interval_seconds=INTERVAL_MINUTES*60)
            tick = ticks[0]
            # Add fake time/calendar/interval signals for demo
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
            # Only keep columns in fieldnames
            row = {k: signals.get(k, None) for k in fieldnames}
            writer.writerow(row)
        dt += timedelta(minutes=INTERVAL_MINUTES)
