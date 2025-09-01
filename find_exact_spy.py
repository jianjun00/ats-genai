#!/usr/bin/env python3
"""
Look for exact SPY file in ETF S zip
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import zipfile
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_exact_spy():
    """Look for exact SPY file"""
    zip_path = Path("/data/firstrate-data/etf/etf_S_full_1min_adjsplitdiv_1py2dog.zip")
    
    logger.info(f"🔍 Looking for exact SPY file in: {zip_path.name}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            file_list = zip_file.namelist()
            
            # Look for exact SPY file
            spy_file = "SPY_full_1min_adjsplitdiv.txt"
            
            if spy_file in file_list:
                logger.info(f"🎉 ✅ FOUND EXACT SPY FILE: {spy_file}")
                
                # Get file info
                file_info = zip_file.getinfo(spy_file)
                logger.info(f"📊 File size: {file_info.file_size:,} bytes")
                logger.info(f"📅 File date: {file_info.date_time}")
                
                return True
            else:
                logger.info(f"❌ Exact SPY file not found: {spy_file}")
                
                # Show all files that contain SPY exactly (not just as substring)
                exact_spy_matches = []
                spy_variants = []
                
                for filename in file_list:
                    base_name = Path(filename).stem.split('_')[0]
                    if base_name == 'SPY':
                        exact_spy_matches.append(filename)
                    elif 'SPY' in base_name:
                        spy_variants.append(filename)
                
                if exact_spy_matches:
                    logger.info(f"✅ Exact SPY matches: {exact_spy_matches}")
                else:
                    logger.info(f"❌ No exact SPY matches")
                
                logger.info(f"🔍 SPY variants found: {len(spy_variants)}")
                for variant in spy_variants[:10]:
                    logger.info(f"   📄 {variant}")
                
                return len(exact_spy_matches) > 0
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    found = find_exact_spy()
    if found:
        print("\n🎯 RESULT: SPY found! Ready for backfill.")
    else:
        print("\n❌ RESULT: Exact SPY not found.")