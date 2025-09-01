#!/usr/bin/env python3
"""
Comprehensive search for missing ETF symbols in FirstRate data
Target symbols: SPY (S&P 500), QQQ (Nasdaq 100), DXY (US Dollar Index), TLT (Treasury Bonds), USO (Oil ETF)
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def search_missing_etf_symbols():
    """Comprehensive search for missing ETF symbols across all categories"""
    adapter = FirstRateAdapter("/data/firstrate-data")
    
    # Target symbols we're looking for
    target_symbols = {
        'SPY': 'S&P 500 ETF',
        'QQQ': 'Nasdaq 100 ETF', 
        'DXY': 'US Dollar Index',
        'TLT': 'Treasury Bond ETF',
        'USO': 'Oil ETF'
    }
    
    # All available data categories to search
    categories = ['stock', 'etf', 'fx', 'index', 'futures', 'crypto']
    
    found_symbols = {}
    all_zip_files = {}
    
    logger.info(f"🔍 Searching for missing ETF symbols: {list(target_symbols.keys())}")
    logger.info(f"📂 Checking categories: {categories}")
    
    # Search each category systematically
    for category in categories:
        logger.info(f"\n🗂️ Searching {category.upper()} category...")
        
        try:
            zip_files = adapter.get_available_zip_files(category)
            all_zip_files[category] = zip_files
            
            if not zip_files:
                logger.info(f"   ❌ No {category} zip files found")
                continue
                
            logger.info(f"   📦 Found {len(zip_files)} {category} zip files")
            
            # Check each zip file in this category
            for zip_file in zip_files:
                logger.info(f"   🔍 Checking {zip_file.name}...")
                
                try:
                    symbols = adapter.extract_symbols_from_zip(zip_file)
                    logger.info(f"      📊 {len(symbols)} symbols in {zip_file.name}")
                    
                    # Show sample symbols for reference
                    if len(symbols) > 0:
                        sample = list(symbols)[:5]
                        logger.info(f"      📝 Sample: {sample}")
                    
                    # Check for our target symbols
                    found_in_this_zip = []
                    for target in target_symbols:
                        if target in symbols:
                            try:
                                min_date, max_date = adapter.get_date_range_for_symbol(zip_file, target)
                                found_symbols[target] = {
                                    'category': category,
                                    'zip_file': zip_file.name,
                                    'zip_path': str(zip_file),
                                    'min_date': min_date,
                                    'max_date': max_date,
                                    'description': target_symbols[target]
                                }
                                found_in_this_zip.append(target)
                                logger.info(f"      ✅ FOUND {target} ({target_symbols[target]})")
                                logger.info(f"         📅 Date range: {min_date} to {max_date}")
                            except Exception as e:
                                logger.error(f"         ❌ Error getting date range for {target}: {e}")
                    
                    if found_in_this_zip:
                        logger.info(f"      🎯 Found symbols in {zip_file.name}: {found_in_this_zip}")
                    
                except Exception as e:
                    logger.error(f"      ❌ Error processing {zip_file.name}: {e}")
        
        except Exception as e:
            logger.error(f"   ❌ Error checking {category} category: {e}")
    
    # Final comprehensive report
    logger.info(f"\n📊 COMPREHENSIVE SEARCH RESULTS")
    logger.info("=" * 60)
    
    logger.info(f"\n✅ FOUND SYMBOLS:")
    for symbol in target_symbols:
        if symbol in found_symbols:
            info = found_symbols[symbol]
            logger.info(f"✅ {symbol} ({info['description']})")
            logger.info(f"   📂 Category: {info['category']}")
            logger.info(f"   📦 File: {info['zip_file']}")
            logger.info(f"   📅 Data: {info['min_date']} to {info['max_date']}")
            logger.info(f"   📁 Path: {info['zip_path']}")
            
    logger.info(f"\n❌ MISSING SYMBOLS:")
    missing_symbols = []
    for symbol in target_symbols:
        if symbol not in found_symbols:
            missing_symbols.append(symbol)
            logger.info(f"❌ {symbol} ({target_symbols[symbol]}) - Not found in any category")
    
    # Category summary
    logger.info(f"\n📂 CATEGORY SUMMARY:")
    for category in categories:
        if category in all_zip_files:
            zip_count = len(all_zip_files[category])
            logger.info(f"   {category.upper()}: {zip_count} zip files")
        else:
            logger.info(f"   {category.upper()}: No files or category not accessible")
    
    # Return results for potential scripting
    return {
        'found_symbols': found_symbols,
        'missing_symbols': missing_symbols,
        'categories_checked': categories,
        'total_zip_files': sum(len(files) for files in all_zip_files.values())
    }

if __name__ == "__main__":
    results = search_missing_etf_symbols()
    
    # Summary stats
    found_count = len(results['found_symbols'])
    missing_count = len(results['missing_symbols'])
    total_targets = found_count + missing_count
    
    print(f"\n🎯 SEARCH COMPLETE:")
    print(f"   Found: {found_count}/{total_targets} symbols")
    print(f"   Missing: {missing_count}/{total_targets} symbols") 
    print(f"   Categories checked: {len(results['categories_checked'])}")
    print(f"   Total zip files examined: {results['total_zip_files']}")