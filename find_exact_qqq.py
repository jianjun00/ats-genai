#!/usr/bin/env python3
"""
Look for exact QQQ file in ETF Q zip
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import zipfile
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_exact_qqq():
    """Look for exact QQQ file"""
    zip_path = Path("/data/firstrate-data/etf/etf_Q_full_1min_adjsplitdiv_fd7pi7f.zip")
    
    logger.info(f"🔍 Looking for exact QQQ file in: {zip_path.name}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            file_list = zip_file.namelist()
            
            logger.info(f"📊 Total files in Q zip: {len(file_list)}")
            
            # Look for exact QQQ file
            qqq_file = "QQQ_full_1min_adjsplitdiv.txt"
            
            if qqq_file in file_list:
                logger.info(f"🎉 ✅ FOUND EXACT QQQ FILE: {qqq_file}")
                
                # Get file info
                file_info = zip_file.getinfo(qqq_file)
                logger.info(f"📊 File size: {file_info.file_size:,} bytes")
                logger.info(f"📅 File date: {file_info.date_time}")
                
                return True
            else:
                logger.info(f"❌ Exact QQQ file not found: {qqq_file}")
                
                # Show first few files to see structure
                logger.info("📁 Sample files:")
                for filename in file_list[:10]:
                    logger.info(f"   📄 {filename}")
                
                # Look for any QQQ-related files
                qqq_related = [f for f in file_list if 'QQQ' in f.upper()]
                if qqq_related:
                    logger.info(f"🔍 QQQ-related files: {len(qqq_related)}")
                    for qqq_file in qqq_related[:10]:
                        logger.info(f"   📄 {qqq_file}")
                else:
                    logger.info("❌ No QQQ-related files found")
                
                return False
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    found = find_exact_qqq()
    if found:
        print("\n🎯 RESULT: QQQ found! Ready for backfill.")
    else:
        print("\n❌ RESULT: Exact QQQ not found.")