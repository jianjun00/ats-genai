#!/usr/bin/env python3
"""
Process specific remaining target symbols from FirstRate zips
"""

import zipfile
import pandas as pd
from pathlib import Path
import logging
import io
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_target_symbols():
    """Process only the remaining target symbols."""
    
    target_symbols = {
        'B': ['BRK.A', 'BRK.B'],
        'C': ['CVX', 'CRM'], 
        'K': ['KO'],
        'L': ['LLY'],
        'O': ['ORCL'],
        'W': ['WMT', 'WFC'],
        'X': ['XOM']
    }
    
    stock_data_path = Path("/data/firstrate-data/stock")
    output_path = Path("/data/minute-bars/firstrate")
    
    total_processed = 0
    
    for letter, symbols in target_symbols.items():
        logger.info(f"🔄 Processing letter {letter} for symbols: {symbols}")
        
        zip_pattern = f"stock_{letter}_full_*.zip"
        zip_files = list(stock_data_path.glob(zip_pattern))
        
        if not zip_files:
            logger.warning(f"⚠️  No zip file found for letter {letter}")
            continue
            
        zip_file = zip_files[0]
        logger.info(f"📦 Processing {zip_file.name} ({zip_file.stat().st_size / 1024**3:.1f} GB)")
        
        # Create output directory for letter
        letter_output = output_path / letter
        letter_output.mkdir(parents=True, exist_ok=True)
        
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                logger.info(f"   📄 Found {len(txt_files)} TXT files")
                
                # Process only target symbols
                for txt_file in txt_files:
                    try:
                        symbol = Path(txt_file).stem.split('_')[0]
                        if symbol not in symbols:
                            continue
                            
                        logger.info(f"   🎯 Processing target symbol: {symbol}")
                        
                        # Read TXT data from zip
                        with zf.open(txt_file) as f:
                            content = f.read()
                            df = pd.read_csv(io.BytesIO(content), header=None, 
                                           names=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            
                            if len(df) > 0:
                                # Create symbol directory
                                symbol_dir = letter_output / symbol
                                symbol_dir.mkdir(parents=True, exist_ok=True)
                                
                                # Save full dataset (not just sample)
                                output_file = symbol_dir / f"{symbol}_full.parquet"
                                df.to_parquet(output_file, engine='auto')
                                
                                logger.info(f"   ✅ {symbol}: Saved {len(df):,} records to {output_file.name}")
                                total_processed += 1
                            else:
                                logger.warning(f"   ⚠️  {symbol}: Empty data")
                                
                    except Exception as e:
                        logger.error(f"   ❌ Error processing {txt_file}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"❌ Error processing zip {zip_file}: {e}")
            continue
            
        # Brief pause between letters
        time.sleep(1)
    
    logger.info(f"✅ Processing completed: {total_processed} target symbols processed")
    return total_processed

def main():
    logger.info("🚀 Processing remaining target symbols from FirstRate zips...")
    process_target_symbols()
    logger.info("✅ Target symbol processing completed!")

if __name__ == "__main__":
    main()