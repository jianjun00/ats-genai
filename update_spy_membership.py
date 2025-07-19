import pandas as pd
from datetime import datetime
import os

WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
CSV_PATH = 'spy_membership.csv'
TODAY = pd.Timestamp(datetime.utcnow().date())

def fetch_current_spy_members():
    tables = pd.read_html(WIKI_URL)
    # Wikipedia: first table is current S&P 500
    df = tables[0]
    return set(df['Symbol'].str.upper())

def update_membership_csv():
    # Load current membership
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH, parse_dates=['effective_date', 'removal_date'])
    else:
        df = pd.DataFrame(columns=['symbol', 'effective_date', 'removal_date'])

    df['symbol'] = df['symbol'].str.upper()
    current_members = fetch_current_spy_members()
    existing_members = set(df.loc[df['removal_date'].isna(), 'symbol'])

    # Add new members
    new_members = current_members - existing_members
    for symbol in new_members:
        df = pd.concat([
            df,
            pd.DataFrame([{'symbol': symbol, 'effective_date': TODAY, 'removal_date': pd.NaT}])
        ], ignore_index=True)

    # Mark removed members
    removed_members = existing_members - current_members
    for symbol in removed_members:
        idx = (df['symbol'] == symbol) & (df['removal_date'].isna())
        df.loc[idx, 'removal_date'] = TODAY

    # Save updated CSV
    df = df.sort_values(['symbol', 'effective_date']).reset_index(drop=True)
    df.to_csv(CSV_PATH, index=False, date_format='%Y-%m-%d')
    print(f"Updated {CSV_PATH}: now {len(df[df['removal_date'].isna()])} current members.")

if __name__ == '__main__':
    update_membership_csv()
