#!/usr/bin/env python3
"""
Detailed search for specific symbols in FirstRate data
Check for SPY, QQQ, TLT, USO specifically and look for similar symbols
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def detailed_symbol_search():
    """Search for specific symbols and similar patterns"""
    adapter = FirstRateAdapter("/data/firstrate-data")
    
    # Target symbols
    targets = ['SPY', 'QQQ', 'DXY', 'TLT', 'USO']
    
    # Keywords to look for (partial matches)
    keywords = {
        'spy': ['SPY', 'SPDR', 'S&P'],
        'qqq': ['QQQ', 'NASDAQ', 'NDX'],
        'dxy': ['DXY', 'DOLLAR', 'DXX'],
        'tlt': ['TLT', 'TREASURY', 'BOND'],
        'uso': ['USO', 'OIL', 'CRUDE']
    }
    
    all_symbols_found = set()
    category_symbols = {}
    
    # Check stock and etf categories (most likely to contain these)
    categories_to_check = ['stock', 'etf']
    
    logger.info("🔍 Detailed symbol search for major ETFs...")
    
    for category in categories_to_check:
        logger.info(f"\n📂 Examining {category.upper()} category in detail...")
        
        try:
            zip_files = adapter.get_available_zip_files(category)
            category_symbols[category] = set()
            
            for zip_file in zip_files:
                try:
                    symbols = adapter.extract_symbols_from_zip(zip_file)
                    category_symbols[category].update(symbols)
                    all_symbols_found.update(symbols)
                    
                    # Check for exact matches
                    for target in targets:
                        if target in symbols:
                            logger.info(f"✅ FOUND {target} in {zip_file.name}")
                    
                    # Check for keyword matches
                    for keyword_group, keyword_list in keywords.items():
                        matching_symbols = []
                        for symbol in symbols:
                            for keyword in keyword_list:
                                if keyword.upper() in symbol.upper():
                                    matching_symbols.append(symbol)
                        
                        if matching_symbols:
                            logger.info(f"🔍 {keyword_group.upper()} related symbols in {zip_file.name}: {matching_symbols[:10]}")
                
                except Exception as e:
                    logger.error(f"Error processing {zip_file.name}: {e}")
        
        except Exception as e:
            logger.error(f"Error checking {category}: {e}")
    
    # Final analysis
    logger.info(f"\n📊 DETAILED ANALYSIS:")
    
    for category in categories_to_check:
        if category in category_symbols:
            symbols = category_symbols[category]
            logger.info(f"\n{category.upper()} symbols ({len(symbols)} total):")
            
            # Check for our targets
            found_targets = [t for t in targets if t in symbols]
            if found_targets:
                logger.info(f"✅ Target symbols found: {found_targets}")
            else:
                logger.info(f"❌ No target symbols found")
            
            # Look for similar symbols
            for target in targets:
                similar = [s for s in symbols if target[:2] in s or target[:3] in s]
                if similar and target not in similar:
                    logger.info(f"🔍 Symbols similar to {target}: {similar[:5]}")
    
    # Final check for specific patterns
    logger.info(f"\n🎯 SPECIFIC PATTERN ANALYSIS:")
    
    # Check for S&P 500 related
    sp500_symbols = [s for s in all_symbols_found if any(pattern in s.upper() for pattern in ['SPY', 'SPDR', 'SPX', 'SPD'])]
    logger.info(f"S&P 500 related: {sp500_symbols[:10] if sp500_symbols else 'None found'}")
    
    # Check for NASDAQ related  
    nasdaq_symbols = [s for s in all_symbols_found if any(pattern in s.upper() for pattern in ['QQQ', 'NDX', 'NASDAQ'])]
    logger.info(f"NASDAQ related: {nasdaq_symbols[:10] if nasdaq_symbols else 'None found'}")
    
    # Check for treasury/bond related
    bond_symbols = [s for s in all_symbols_found if any(pattern in s.upper() for pattern in ['TLT', 'TREASURY', 'BOND', 'TLH', 'TLO'])]
    logger.info(f"Treasury/Bond related: {bond_symbols[:10] if bond_symbols else 'None found'}")
    
    # Check for oil related
    oil_symbols = [s for s in all_symbols_found if any(pattern in s.upper() for pattern in ['USO', 'OIL', 'CRUDE', 'UCO', 'UNG'])]
    logger.info(f"Oil related: {oil_symbols[:10] if oil_symbols else 'None found'}")
    
    # Check for dollar index related
    dollar_symbols = [s for s in all_symbols_found if any(pattern in s.upper() for pattern in ['DXY', 'DOLLAR', 'DXX', 'UUP', 'USD'])]
    logger.info(f"Dollar index related: {dollar_symbols[:10] if dollar_symbols else 'None found'}")
    
    return {
        'targets_found': [t for t in targets if t in all_symbols_found],
        'targets_missing': [t for t in targets if t not in all_symbols_found],
        'total_symbols': len(all_symbols_found),
        'category_counts': {cat: len(syms) for cat, syms in category_symbols.items()}
    }

if __name__ == "__main__":
    results = detailed_symbol_search()
    
    logger.info(f"\n🎯 FINAL RESULTS:")
    logger.info(f"Found targets: {results['targets_found']}")
    logger.info(f"Missing targets: {results['targets_missing']}")
    logger.info(f"Total symbols examined: {results['total_symbols']}")
    logger.info(f"Category counts: {results['category_counts']}")