# moved from project root
import pandas as pd
from datetime import datetime
import requests
import os

# Paths
ALPACA_CSV = 'sp500_changes.csv'  # Downloaded from Alpaca
OUT_CSV = 'spy_membership.csv'
YEARS_BACK = 10
WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'


def build_historical_membership(alpaca_csv, years_back):
    df = pd.read_csv(alpaca_csv, parse_dates=['date'])
    cutoff = pd.Timestamp(datetime.utcnow() - pd.DateOffset(years=years_back)).date()
    df = df[df['date'].dt.date >= cutoff]
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
                for m in reversed(membership[symbol]):
                    if m['removal_date'] is None:
                        m['removal_date'] = dt
                        break
    # Flatten
    hist_rows = []
    for symbol, periods in membership.items():
        for period in periods:
            if period['effective_date'] >= cutoff:
                hist_rows.append({
                    'symbol': symbol,
                    'effective_date': period['effective_date'],
                    'removal_date': period['removal_date']
                })
    return pd.DataFrame(hist_rows)

def fetch_current_wiki_members():
    tables = pd.read_html(WIKI_URL)
    df = tables[0]
    return set(df['Symbol'].str.upper())

def update_with_current_membership(hist_df):
    today = pd.Timestamp(datetime.utcnow().date())
    # Get current S&P 500 from Wikipedia
    current_members = fetch_current_wiki_members()
    # Find all symbols ever in history
    all_symbols = set(hist_df['symbol'])
    # Find symbols that should be added (not in history or removed previously)
    to_add = current_members - set(hist_df.loc[hist_df['removal_date'].isna(), 'symbol'])
    # Add new members
    for symbol in to_add:
        hist_df = pd.concat([
            hist_df,
            pd.DataFrame([{'symbol': symbol, 'effective_date': today, 'removal_date': pd.NaT}])
        ], ignore_index=True)
    # Mark removed members
    still_active = set(hist_df.loc[hist_df['removal_date'].isna(), 'symbol'])
    to_remove = still_active - current_members
    for symbol in to_remove:
        idx = (hist_df['symbol'] == symbol) & (hist_df['removal_date'].isna())
        hist_df.loc[idx, 'removal_date'] = today
    return hist_df

def main():
    # Step 1: Build historical membership
    hist_df = build_historical_membership(ALPACA_CSV, YEARS_BACK)
    # Step 2: Update with current live membership
    full_df = update_with_current_membership(hist_df)
    # Step 3: Sort and deduplicate
    full_df = full_df.sort_values(['symbol', 'effective_date']).reset_index(drop=True)
    full_df = full_df.drop_duplicates(['symbol', 'effective_date', 'removal_date'])
    # Step 4: Write to CSV
    full_df.to_csv(OUT_CSV, index=False, date_format='%Y-%m-%d')
    print(f"Updated {OUT_CSV}: {len(full_df[full_df['removal_date'].isna()])} current members, {len(full_df)} periods total.")

if __name__ == '__main__':
    main()
