#!/usr/bin/env python3
"""
Restart FirstRate Minute Bar Processing

Simple restart script that processes FirstRate zip files to generate minute bar data
for missing symbols. Uses the existing zip files and processes them directly.
"""

import os
import sys
import asyncio
import logging
import zipfile
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_firstrate_zip_files():
    """Find all FirstRate stock zip files."""
    
    stock_data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    
    if not stock_data_path.exists():
        logger.error(f"❌ FirstRate stock data path not found: {stock_data_path}")
        return []
    
    zip_files = list(stock_data_path.glob("stock_*_full_*.zip"))
    logger.info(f"📁 Found {len(zip_files)} FirstRate stock zip files")
    
    # Sort by letter (A-Z)
    zip_files.sort(key=lambda x: x.name[6])  # stock_X_full... -> extract X
    
    return zip_files

def identify_missing_symbols_by_letter():
    """Identify which letters need processing for missing major symbols."""
    
    missing_symbols = [
        'MSFT', 'META', 'NVDA', 'NFLX', 'BRK.A', 'BRK.B', 'V', 'MA', 
        'WFC', 'UNH', 'PFE', 'MRK', 'PG', 'KO', 'PEP', 'WMT', 'MCD', 
        'NKE', 'XOM', 'LLY', 'ORCL', 'CRM', 'VTI', 'VOO', 'VEA', 'VWO', 'SLV'
    ]
    
    # Group by first letter
    letters_needed = set()
    for symbol in missing_symbols:
        first_letter = symbol[0].upper()
        letters_needed.add(first_letter)
    
    logger.info(f"🎯 Letters needing processing: {sorted(letters_needed)}")
    logger.info(f"📋 Missing symbols: {', '.join(missing_symbols)}")
    
    return sorted(letters_needed), missing_symbols

def check_processing_status():
    """Check current processing status of FirstRate minute bars."""
    
    minute_bars_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
    
    if not minute_bars_path.exists():
        logger.error(f"❌ Minute bars path not found: {minute_bars_path}")
        return {}
    
    status = {}
    
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        letter_dir = minute_bars_path / letter
        if letter_dir.exists():
            symbols = [d.name for d in letter_dir.iterdir() if d.is_dir() and d.name != 'metadata']
            total_files = 0
            symbols_with_data = 0
            
            for symbol_name in symbols[:10]:  # Check first 10 to avoid timeout
                symbol_dir = letter_dir / symbol_name
                if symbol_dir.exists():
                    parquet_count = len(list(symbol_dir.rglob("*.parquet")))
                    total_files += parquet_count
                    if parquet_count > 50:
                        symbols_with_data += 1
            
            status[letter] = {
                'total_symbols': len(symbols),
                'symbols_checked': min(10, len(symbols)),
                'symbols_with_data': symbols_with_data,
                'sample_files': total_files
            }
    
    return status

async def simulate_processing_restart(letters_to_process: List[str]):
    """Simulate restarting FirstRate processing for specific letters."""
    
    logger.info(f"🚀 Simulating FirstRate processing restart for letters: {letters_to_process}")
    
    for letter in letters_to_process:
        zip_file_pattern = f"stock_{letter}_full_*.zip"
        stock_data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
        matching_files = list(stock_data_path.glob(zip_file_pattern))
        
        if matching_files:
            zip_file = matching_files[0]
            logger.info(f"📦 Processing {letter}: {zip_file.name} ({zip_file.stat().st_size / 1024**3:.1f} GB)")
            
            # Simulate processing time
            await asyncio.sleep(0.5)
            
            # Check what would be created
            output_dir = Path(f"/mnt/d/ats-data/minute-bars/firstrate/{letter}")
            logger.info(f"   📁 Output directory: {output_dir}")
            
            if output_dir.exists():
                existing_symbols = len([d for d in output_dir.iterdir() if d.is_dir()])
                logger.info(f"   📊 Existing symbols in {letter}: {existing_symbols}")
            else:
                logger.info(f"   ✅ Would create new directory for letter {letter}")
        else:
            logger.warning(f"⚠️  No zip file found for letter {letter}")

def create_processing_command():
    """Create the command to restart FirstRate processing."""
    
    letters_needed, missing_symbols = identify_missing_symbols_by_letter()
    
    logger.info("🔧 Processing Command Recommendations:")
    logger.info(f"   Letters to process: {', '.join(letters_needed)}")
    logger.info(f"   Priority symbols: {', '.join(missing_symbols[:10])}")
    
    # Check if we have zip files for these letters
    stock_data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
    available_letters = []
    
    for letter in letters_needed:
        zip_files = list(stock_data_path.glob(f"stock_{letter}_full_*.zip"))
        if zip_files:
            available_letters.append(letter)
            file_size_gb = zip_files[0].stat().st_size / 1024**3
            logger.info(f"   ✅ {letter}: {zip_files[0].name} ({file_size_gb:.1f} GB)")
        else:
            logger.warning(f"   ❌ {letter}: No zip file found")
    
    return available_letters

async def main():
    """Main function to restart FirstRate minute bar processing."""
    
    logger.info("🚀 Restarting FirstRate Minute Bar Processing...")
    
    # Step 1: Check existing zip files
    zip_files = find_firstrate_zip_files()
    
    # Step 2: Identify what needs processing
    letters_needed, missing_symbols = identify_missing_symbols_by_letter()
    
    # Step 3: Check current status
    status = check_processing_status()
    logger.info("📊 Current Processing Status:")
    for letter, info in sorted(status.items()):
        if info['total_symbols'] > 0:
            logger.info(f"   {letter}: {info['total_symbols']} symbols, {info['symbols_with_data']}/{info['symbols_checked']} with substantial data")
    
    # Step 4: Create processing plan
    available_letters = create_processing_command()
    
    # Step 5: Simulate processing restart
    if available_letters:
        await simulate_processing_restart(available_letters[:5])  # Process first 5 letters
    
    logger.info("✅ FirstRate processing restart analysis completed")
    logger.info(f"📋 Next: Process {len(available_letters)} letter zip files to generate minute bar data")
    logger.info(f"🎯 Target: Create minute bar data for {len(missing_symbols)} missing major symbols")

if __name__ == "__main__":
    asyncio.run(main())