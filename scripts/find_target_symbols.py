#!/usr/bin/env python3
"""
Find target symbols in FirstRate zip files
"""

import zipfile
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Searching for target symbols in FirstRate zip files...")

    # Major symbols we're looking for
    target_symbols = [
        'BRKA', 'BRKB', 'BRK.A', 'BRK.B', 'BRKA', 'BERKH',  # Different Berkshire variations
        'CVX', 'CRM', 'KO', 'LLY', 'ORCL', 'SPY', 'SLV', 'WMT', 'WFC', 'XOM'
    ]

    zip_base = Path("/data/firstrate-data/stock")

    # Letters to search
    search_letters = ['B', 'C', 'K', 'L', 'O', 'S', 'W', 'X']

    for letter in search_letters:
        zip_pattern = f"stock_{letter}_full_*.zip"
        zip_files = list(zip_base.glob(zip_pattern))

        if zip_files:
            zip_file = zip_files[0]
            logger.info(f"\n📦 Searching {letter}: {zip_file.name}")

            try:
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                    logger.info(f"   📄 Found {len(txt_files)} TXT files")

                    # Extract all symbols
                    symbols = [Path(f).stem.split('_')[0] for f in txt_files]
                    symbols = [s for s in symbols if s and s not in ['metadata', 'readme']]

                    # Look for target symbols
                    found_targets = []
                    for target in target_symbols:
                        if target in symbols:
                            found_targets.append(target)

                    if found_targets:
                        logger.info(f"   🎯 Found target symbols: {found_targets}")
                    else:
                        # Show some sample symbols that start with common letters
                        if letter == 'B':
                            brk_symbols = [s for s in symbols if s.startswith('BRK')]
                            if brk_symbols:
                                logger.info(f"   🔍 BRK symbols found: {brk_symbols}")

                        logger.info(f"   📋 Sample symbols: {symbols[:10]}")

            except Exception as e:
                logger.error(f"   ❌ Error processing {zip_file}: {e}")

if __name__ == "__main__":
    main()