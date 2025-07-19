import pandas as pd
from datetime import datetime
import os

# Path to Alpaca changes CSV (download from https://github.com/alpacahq/alpaca-labs/tree/master/sp500-history)
ALPACA_CSV = 'sp500_changes.csv'  # Update this path to your downloaded file
OUT_CSV = 'spy_membership.csv'

def convert_alpaca_to_membership(alpaca_csv=ALPACA_CSV, out_csv=OUT_CSV):
    df = pd.read_csv(alpaca_csv, parse_dates=['date'])
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
    convert_alpaca_to_membership()
