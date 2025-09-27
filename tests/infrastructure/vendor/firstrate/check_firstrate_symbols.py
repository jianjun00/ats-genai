#!/usr/bin/env python3
"""
Check available symbols in FirstRate data
"""

import sys
import os

# Set working directory and path
os.chdir('/workspace')
sys.path.insert(0, '/workspace/src')

from domains.market_data.services.data_collection.backfill.unified_backfill_orchestrator import FirstRateAdapter

def main():
    print("🔍 Checking FirstRate symbol inventory...")

    adapter = FirstRateAdapter("/data/firstrate-data")

    # Get all symbol inventories
    for asset_type in ['stock', 'etf', 'fx', 'index']:
        inventory = adapter.get_symbol_inventory(asset_type)
        print(f"\n📊 {asset_type.upper()} Assets: {len(inventory)} symbols")

        target_symbols = ['SPY', 'QQQ', 'IWM', 'DXY', 'TLT', 'USO', 'TSLA', 'GLD']
        found_symbols = []

        for symbol in target_symbols:
            if symbol in inventory:
                info = inventory[symbol]
                found_symbols.append(symbol)
                print(f"✅ {symbol}: {info['min_date']} to {info['max_date']} ({len(info['zip_files'])} files)")

        if not found_symbols:
            print(f"❌ None of our target symbols found in {asset_type}")

if __name__ == "__main__":
    main()