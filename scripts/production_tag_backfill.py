#!/usr/bin/env python3
"""
Production Tag Backfill Script for Integration Environment
Tags valid ArrayRecord files (>64KB) with production status in integration database
"""

import os
import json
import psycopg2
import glob
import argparse
from pathlib import Path

# Database configuration - Integration Environment
DB_CONFIG = {
    'host': 'localhost',
    'port': 4432,  # Integration postgres port
    'user': 'postgres', 
    'password': 'intg_password',
    'database': 'intg_db'
}

def verify_arrayrecord_file(file_path):
    """Verify ArrayRecord file is valid by checking actual record count"""
    try:
        if not os.path.exists(file_path):
            return False, "File does not exist"
            
        file_size = os.path.getsize(file_path)
        
        # Check if array_record module is available
        try:
            from array_record.python import array_record_module
        except ImportError:
            # ArrayRecord module required for proper validation - fail if not available
            return False, f"array_record module not available for validation"
        
        # Use ArrayRecord to get actual record count
        try:
            reader = array_record_module.ArrayRecordReader(file_path)
            num_records = reader.num_records()
            reader.close()
            
            if num_records == 0:
                return False, f"Empty ArrayRecord file: 0 records"
            
            return True, f"Valid ArrayRecord: {num_records} records ({file_size} bytes)"
            
        except Exception as ar_error:
            return False, f"ArrayRecord read error: {ar_error}"
        
    except Exception as e:
        return False, f"Error checking file: {e}"

def find_arrayrecord_files(dataset_pattern):
    """Find all ArrayRecord files matching the dataset pattern"""
    dataset_base = '/data/training_data'
    
    # Find all datasets matching the pattern
    if '*' in dataset_pattern:
        dataset_dirs = glob.glob(f"{dataset_base}/{dataset_pattern}")
    else:
        dataset_dirs = [f"{dataset_base}/{dataset_pattern}"]
    
    files_found = []
    
    for dataset_dir in dataset_dirs:
        if not os.path.exists(dataset_dir):
            print(f"⚠️ Dataset directory does not exist: {dataset_dir}")
            continue
            
        dataset_id = os.path.basename(dataset_dir)
        
        # Find all ArrayRecord files in this dataset
        arrayrecord_files = glob.glob(f"{dataset_dir}/**/*.arrayrecord", recursive=True)
        
        for file_path in arrayrecord_files:
            # Parse the structure: {dataset_id}/{feature_group}/{symbol_year_month}/{timeframe}/{symbol_year_month_feature_group}.arrayrecord
            path_parts = Path(file_path).parts
            
            # Find the indices of relevant parts
            dataset_idx = None
            for i, part in enumerate(path_parts):
                if part.startswith('dataset_'):
                    dataset_idx = i
                    break
                    
            if dataset_idx is None:
                continue
                
            try:
                feature_group = path_parts[dataset_idx + 1]
                symbol_year_month = path_parts[dataset_idx + 2]
                timeframe = path_parts[dataset_idx + 3]
                
                # Parse symbol_year_month (e.g., "AAPL_2025_01")
                parts = symbol_year_month.split('_')
                if len(parts) != 3:
                    continue
                    
                symbol = parts[0]
                year = int(parts[1])
                month = int(parts[2])
                
                files_found.append({
                    'dataset_id': dataset_id,
                    'feature_group': feature_group,
                    'symbol': symbol,
                    'year': year,
                    'month': month,
                    'timeframe': timeframe,
                    'file_path': file_path
                })
                
            except (IndexError, ValueError) as e:
                print(f"⚠️ Could not parse file path {file_path}: {e}")
                continue
    
    return files_found

def backfill_production_tags(dataset_pattern):
    """Backfill production tags for valid ArrayRecord files"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Find all ArrayRecord files
    all_files = find_arrayrecord_files(dataset_pattern)
    
    total_files_checked = 0
    valid_files_tagged = 0
    invalid_files_skipped = 0
    
    print("🔍 Starting production tag backfill for ArrayRecord files...")
    print(f"Dataset pattern: {dataset_pattern}")
    print(f"Found {len(all_files)} ArrayRecord files to process")
    print()
    
    for file_info in all_files:
        total_files_checked += 1
        
        file_path = file_info['file_path']
        dataset_id = file_info['dataset_id']
        symbol = file_info['symbol']
        year = file_info['year']
        month = file_info['month']
        feature_group = file_info['feature_group']
        timeframe = file_info['timeframe']
        
        # Check if file exists and is valid
        is_valid, message = verify_arrayrecord_file(file_path)
        
        if not is_valid:
            invalid_files_skipped += 1
            print(f"❌ {symbol}_{year:04d}_{month:02d} {feature_group} {timeframe}: {message}")
            continue
        
        # Extract record count from validation message
        try:
            # Message format: "Valid ArrayRecord: {num_records} records ({file_size} bytes)"
            num_records = int(message.split(': ')[1].split(' records')[0])
        except:
            num_records = 0
        
        # File is valid, add production tag
        # Convert dict to JSON string for PostgreSQL JSONB
        generation_params = {
            'feature_group': feature_group,
            'timeframe': timeframe,
            'dataset_id': dataset_id
        }
        
        try:
            # Insert production tag into intg_training_dataset table
            dataset_name = f"{dataset_id}_{symbol}_{year:04d}_{month:02d}_{feature_group}_{timeframe}"
            
            cursor.execute("""
                INSERT INTO intg_training_dataset (
                    dataset_name, dataset_path, symbols, 
                    generation_parameters, status,
                    file_size_mb, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, NOW(), NOW()
                ) ON CONFLICT (dataset_name) 
                DO UPDATE SET 
                    generation_parameters = EXCLUDED.generation_parameters,
                    file_size_mb = EXCLUDED.file_size_mb,
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (
                dataset_name,
                file_path,
                [symbol],  # symbols array
                json.dumps(generation_params),
                'production',  # status  
                os.path.getsize(file_path) / (1024 * 1024)  # file_size_mb
            ))
            
            # Insert or update record in intg_feature_group_files table
            year_month_str = f"{year}_{month:02d}"
            cursor.execute("""
                INSERT INTO intg_feature_group_files (
                    dataset_id, feature_group, symbol, year_month,
                    timeframes, file_paths, file_sizes, record_count,
                    status, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, 'prod', NOW(), NOW()
                ) ON CONFLICT (dataset_id, feature_group, symbol, year_month)
                DO UPDATE SET 
                    record_count = EXCLUDED.record_count,
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (
                dataset_id,
                feature_group,
                symbol,
                year_month_str,
                [timeframe],  # timeframes array
                json.dumps({timeframe: [file_path]}),  # file_paths jsonb
                json.dumps({timeframe: {os.path.basename(file_path): os.path.getsize(file_path)}}),  # file_sizes jsonb
                num_records  # record count from ArrayRecord validation
            ))
            
            valid_files_tagged += 1
            print(f"✅ {symbol}_{year:04d}_{month:02d} {feature_group} {timeframe}: Tagged for production ({message})")
            
        except Exception as e:
            print(f"❌ Database error for {symbol}_{year:04d}_{month:02d} {feature_group} {timeframe}: {e}")
            invalid_files_skipped += 1
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n📊 Production Tag Backfill Summary:")
    print(f"   Total files checked: {total_files_checked}")
    print(f"   ✅ Valid files tagged for production: {valid_files_tagged}")
    print(f"   ❌ Invalid files skipped: {invalid_files_skipped}")
    if total_files_checked > 0:
        print(f"   📈 Success rate: {valid_files_tagged / total_files_checked * 100:.1f}%")
    print()
    print("🎯 Production tag backfill completed for integration environment!")

def main():
    """Main entry point with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Backfill production tags for ArrayRecord files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Tag all datasets from a specific date
  python3 scripts/production_tag_backfill.py --dataset-pattern "dataset_20251116_*"
  
  # Tag a specific dataset
  python3 scripts/production_tag_backfill.py --dataset-pattern "dataset_20251116_141516"
  
  # Tag all datasets (use with caution)
  python3 scripts/production_tag_backfill.py --dataset-pattern "dataset_*"
        """
    )
    
    parser.add_argument(
        '--dataset-pattern',
        required=True,
        help='Dataset pattern to match (e.g., "dataset_20251116_*", "dataset_20251116_141516")'
    )
    
    args = parser.parse_args()
    
    # Validate dataset pattern
    if not args.dataset_pattern:
        print("❌ Error: --dataset-pattern is required")
        parser.print_help()
        return 1
    
    try:
        backfill_production_tags(args.dataset_pattern)
        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())