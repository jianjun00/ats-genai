#!/usr/bin/env python3
"""
Examine the actual file structure in ETF S zip
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import zipfile
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def examine_etf_s_structure():
    """Examine actual file structure in ETF S zip"""
    zip_path = Path("/data/firstrate-data/etf/etf_S_full_1min_adjsplitdiv_1py2dog.zip")
    
    logger.info(f"🔍 Examining file structure in: {zip_path.name}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            file_list = zip_file.namelist()
            
            logger.info(f"📊 Total files: {len(file_list)}")
            
            # Show first 20 file names to understand structure
            logger.info("📁 First 20 files:")
            for i, filename in enumerate(file_list[:20]):
                logger.info(f"   {i+1:2d}. {filename}")
            
            # Look for any file containing SPY
            spy_files = [f for f in file_list if 'SPY' in f.upper()]
            if spy_files:
                logger.info(f"\n🎉 Files containing SPY: {len(spy_files)}")
                for spy_file in spy_files[:10]:
                    logger.info(f"   📄 {spy_file}")
            else:
                logger.info("\n❌ No files containing 'SPY'")
            
            # Check different file patterns
            logger.info(f"\n📝 File patterns analysis:")
            csv_files = [f for f in file_list if f.endswith('.csv')]
            txt_files = [f for f in file_list if f.endswith('.txt')]
            other_files = [f for f in file_list if not f.endswith('.csv') and not f.endswith('.txt')]
            
            logger.info(f"   CSV files: {len(csv_files)}")
            logger.info(f"   TXT files: {len(txt_files)}")
            logger.info(f"   Other files: {len(other_files)}")
            
            if csv_files:
                logger.info("   Sample CSV files:")
                for csv_file in csv_files[:5]:
                    logger.info(f"     {csv_file}")
            
            # Look for any S-prefixed symbols
            s_files = [f for f in file_list if any(part.startswith('S') for part in f.split('/') + f.split('_'))]
            if s_files:
                logger.info(f"\n📂 Files with S-prefixed parts: {len(s_files)}")
                for s_file in s_files[:10]:
                    logger.info(f"   📄 {s_file}")
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    examine_etf_s_structure()