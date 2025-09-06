#!/usr/bin/env python3
"""
Populate file_metadata for existing training datasets.

This script scans the training data files and populates the file_metadata JSONB
column with detailed information about each file.
"""

import asyncio
import asyncpg
import json
from pathlib import Path
from datetime import datetime
import os
import sys

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

async def populate_file_metadata():
    """Populate file_metadata for existing training datasets."""
    
    print("🔍 POPULATING TRAINING DATASET FILE METADATA")
    print("=" * 60)
    
    # Connect to database
    conn = await asyncpg.connect(
        host="localhost",
        port=3432,
        user="postgres",
        password="dev_password",
        database="dev_db"
    )
    
    try:
        # Get datasets that need file_metadata populated
        datasets = await conn.fetch("""
            SELECT id, dataset_name, run_id, symbols, total_sequences
            FROM dev_training_datasets 
            WHERE file_metadata IS NULL
            ORDER BY id DESC
        """)
        
        print(f"📊 Found {len(datasets)} datasets needing file metadata")
        
        for dataset in datasets:
            dataset_id = dataset['id']
            run_id = dataset['run_id']
            dataset_name = dataset['dataset_name']
            symbols = dataset['symbols']
            expected_sequences = dataset['total_sequences']
            
            print(f"\n📁 Processing Dataset {dataset_id}: {dataset_name}")
            print(f"   Run ID: {run_id}")
            print(f"   Symbols: {symbols}")
            print(f"   Expected sequences: {expected_sequences}")
            
            # Parse symbols from PostgreSQL array format
            if isinstance(symbols, str):
                if symbols.startswith('{') and symbols.endswith('}'):
                    symbol_list = [s.strip() for s in symbols.strip('{}').split(',') if s.strip()]
                else:
                    symbol_list = [s.strip() for s in symbols.split(',') if s.strip()]
            else:
                symbol_list = symbols if symbols else []
            
            print(f"   Parsed symbols: {symbol_list}")
            
            # Scan training data directory
            training_base_dirs = [
                Path("/mnt/d/ats-data/training_data"),
                Path("/data/training_data")  # If running in container
            ]
            
            file_metadata = {
                "files": [],
                "total_sequences": 0,
                "total_files": 0,
                "timeframes": [],
                "symbols": symbol_list,
                "generation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            files_found = []
            
            for base_dir in training_base_dirs:
                if base_dir.exists():
                    run_dir = base_dir / str(run_id)
                    if run_dir.exists():
                        print(f"   🔍 Scanning: {run_dir}")
                        
                        # Scan all timeframe directories
                        timeframes = []
                        for item in run_dir.iterdir():
                            if item.is_dir() and item.name != "metadata":
                                timeframes.append(item.name)
                        
                        timeframes.sort()  # Sort timeframes: 5m, 15m, 1h, 1d, 1w
                        print(f"   ⏰ Timeframes found: {timeframes}")
                        
                        for timeframe in timeframes:
                            timeframe_dir = run_dir / timeframe
                            
                            # Find ArrayRecord files for each symbol
                            for symbol in symbol_list:
                                arrayrecord_files = list(timeframe_dir.glob(f"{symbol}_*.arrayrecord"))
                                
                                for file_path in arrayrecord_files:
                                    file_stats = file_path.stat()
                                    
                                    # Try to get sequence count from metadata file
                                    # Convert AAPL_20250701_000000_20250906_000000.arrayrecord 
                                    # to AAPL_20250701_000000_20250906_000000_metadata.json
                                    base_name = file_path.stem  # Remove .arrayrecord
                                    metadata_file = file_path.parent / f"{base_name}_metadata.json"
                                    sequences = 0
                                    
                                    if metadata_file.exists():
                                        try:
                                            with open(metadata_file) as f:
                                                metadata = json.load(f)
                                                sequences = metadata.get('example_count', 0)
                                        except Exception as e:
                                            print(f"     ⚠️  Could not read metadata for {file_path.name}: {e}")
                                    
                                    if sequences == 0:
                                        # Estimate sequences based on file size (rough approximation)
                                        # ArrayRecord files are typically ~131KB with ~160 sequences
                                        estimated_sequences = max(1, int(file_stats.st_size / 1024))
                                        sequences = estimated_sequences
                                        print(f"     📊 Estimated {sequences} sequences for {file_path.name}")
                                    
                                    file_info = {
                                        "symbol": symbol,
                                        "timeframe": timeframe,
                                        "file_path": file_path.name,
                                        "sequences": sequences,
                                        "file_size_bytes": file_stats.st_size,
                                        "created_at": datetime.fromtimestamp(file_stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                                    }
                                    
                                    file_metadata["files"].append(file_info)
                                    files_found.append(f"{symbol}_{timeframe}")
                                    
                                    print(f"     ✅ {symbol} {timeframe}: {sequences} sequences, {file_stats.st_size} bytes")
                        
                        file_metadata["timeframes"] = timeframes
                        break  # Found run directory, no need to check other base dirs
            
            # Calculate totals
            file_metadata["total_files"] = len(file_metadata["files"])
            file_metadata["total_sequences"] = sum(f["sequences"] for f in file_metadata["files"])
            
            print(f"   📊 Summary:")
            print(f"     Total files: {file_metadata['total_files']}")
            print(f"     Total sequences: {file_metadata['total_sequences']}")
            print(f"     Expected sequences: {expected_sequences}")
            
            if file_metadata["total_files"] > 0:
                # Update database with file metadata
                await conn.execute("""
                    UPDATE dev_training_datasets 
                    SET file_metadata = $1
                    WHERE id = $2
                """, json.dumps(file_metadata), dataset_id)
                
                print(f"   ✅ Updated dataset {dataset_id} with file metadata")
                
                # Also update total_sequences if our calculation differs significantly
                if abs(file_metadata["total_sequences"] - expected_sequences) > 100:
                    await conn.execute("""
                        UPDATE dev_training_datasets 
                        SET total_sequences = $1
                        WHERE id = $2
                    """, file_metadata["total_sequences"], dataset_id)
                    print(f"   🔄 Updated total_sequences: {expected_sequences} → {file_metadata['total_sequences']}")
            else:
                print(f"   ❌ No files found for dataset {dataset_id}")
    
    finally:
        await conn.close()
        
    print(f"\n✅ File metadata population complete!")

if __name__ == "__main__":
    asyncio.run(populate_file_metadata())