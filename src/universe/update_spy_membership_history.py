# moved from project root
import pandas as pd
from datetime import datetime
import os

# Path to Alpaca changes CSV (download from https://github.com/alpacahq/alpaca-labs/tree/master/sp500-history)
ALPACA_CSV = 'sp500_changes.csv'  # Update this path to your downloaded file
OUT_CSV = 'spy_membership.csv'
YEARS_BACK = 10


def update_spy_membership_history(alpaca_csv=ALPACA_CSV, out_csv=OUT_CSV, years_back=YEARS_BACK):
    df = pd.read_csv(alpaca_csv, parse_dates=['date'])
    # Only keep changes within the past N years
    cutoff = pd.Timestamp(datetime.utcnow() - pd.DateOffset(years=years_back)).date()
    df = df[df['date'].dt.date >= cutoff]
    # Build membership set with effective/removal dates
    membership = {}
    for _, row in df.iterrows():
        dt = row['date'].date()
        added = str(row['added']).split(';') if pd.notna(row['added']) else []
        removed = str(row['removed']).split(';') if pd.notna(row['removed']) else []
        for symbol in added:
            symbol = symbol.strip().upper()
            if symbol:
                membership.setdefault(symbol, []).append({'effective_date': dt, 'removal_date': None})
        for symbol in removed:
            symbol = symbol.strip().upper()
            if symbol and symbol in membership:
                # Find the most recent entry without a removal_date
                for m in reversed(membership[symbol]):
                    if m['removal_date'] is None:
                        m['removal_date'] = dt
                        break
    # Flatten to rows
    rows = []
    for symbol, periods in membership.items():
        for period in periods:
            # Only include if effective_date is within the last N years
            if period['effective_date'] >= cutoff:
                rows.append({
                    'symbol': symbol,
                    'effective_date': period['effective_date'],
                    'removal_date': period['removal_date']
                })
    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(['symbol', 'effective_date']).reset_index(drop=True)
    out_df.to_csv(out_csv, index=False, date_format='%Y-%m-%d')
    print(f"Wrote {len(out_df)} membership periods to {out_csv}")

if __name__ == '__main__':
    update_spy_membership_history()
