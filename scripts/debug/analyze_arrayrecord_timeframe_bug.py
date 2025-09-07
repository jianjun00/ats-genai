#!/usr/bin/env python3
"""
Debug utility to analyze ArrayRecord timeframe separation bug.

This script provides detailed analysis of training dataset ArrayRecord files
to help understand the extent of the timeframe mixing bug.

Usage:
    python scripts/debug/analyze_arrayrecord_timeframe_bug.py [dataset_path]
"""

import os
import sys
import hashlib
import json
import ast
from typing import Dict, List, Set, Tuple
import numpy as np
from pathlib import Path
from collections import defaultdict


def get_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def read_arrayrecord_metadata(file_path: str) -> Tuple[List[str], int]:
    """Read column names and record count from ArrayRecord file"""
    try:
        from array_record.python.array_record_module import ArrayRecordReader
    except ImportError:
        print("ERROR: array_record package not installed. Install with: pip install array_record")
        sys.exit(1)

    reader = ArrayRecordReader(str(file_path))
    total_records = reader.num_records()

    if total_records == 0:
        reader.close()
        return [], 0

    # First record contains column names
    reader.seek(0)
    first_record = reader.read()
    reader.close()

    # Parse column names - handle both JSON and Python list format
    column_names_str = first_record.decode('utf-8') if isinstance(first_record, bytes) else str(first_record)
    try:
        column_names = json.loads(column_names_str)
    except json.JSONDecodeError:
        try:
            column_names = ast.literal_eval(column_names_str)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Failed to parse column names: {e}")

    # Total records minus metadata record
    record_count = total_records - 1

    return column_names, record_count


def analyze_timeframe_distribution(column_names: List[str]) -> Dict[str, List[str]]:
    """Analyze column names and categorize by timeframe"""
    timeframes = ['5m', '15m', '1h', '1d', '1w']
    distribution = {tf: [] for tf in timeframes}
    distribution['unknown'] = []

    for col in column_names:
        col_lower = col.lower()
        categorized = False

        for tf in timeframes:
            if col_lower.startswith(f'{tf}_'):
                distribution[tf].append(col)
                categorized = True
                break

        if not categorized:
            distribution['unknown'].append(col)

    return distribution


def analyze_feature_types(column_names: List[str]) -> Dict[str, List[str]]:
    """Categorize columns by feature type"""
    categories = {
        'price': [],      # open, high, low, close
        'volume': [],     # volume, vwap
        'technical': [],  # indicators, ratios
        'meta': [],       # timestamp, symbol
        'unknown': []
    }

    for col in column_names:
        col_lower = col.lower()

        if any(word in col_lower for word in ['open', 'high', 'low', 'close']):
            categories['price'].append(col)
        elif any(word in col_lower for word in ['volume', 'vwap']):
            categories['volume'].append(col)
        elif any(word in col_lower for word in ['timestamp', 'symbol']):
            categories['meta'].append(col)
        elif any(word in col_lower for word in ['ema', 'sma', 'rsi', 'macd', 'bb', 'atr', 'ratio', 'indicator']):
            categories['technical'].append(col)
        else:
            categories['unknown'].append(col)

    return categories


def main():
    """Main analysis function"""
    # Default dataset path
    default_path = "/mnt/d/ats-data/training_data/89/AAPL_20250701_000000_20250906_000000"
    dataset_path = sys.argv[1] if len(sys.argv) > 1 else default_path

    if not os.path.exists(dataset_path):
        print(f"ERROR: Dataset path does not exist: {dataset_path}")
        sys.exit(1)

    print("🔍 ArrayRecord Timeframe Separation Bug Analysis")
    print("=" * 60)
    print(f"Dataset Path: {dataset_path}")
    print()

    timeframes = ['5m', '15m', '1h', '1d', '1w']
    file_analysis = {}

    # Analyze each timeframe file
    for timeframe in timeframes:
        file_path = os.path.join(dataset_path, timeframe, f"AAPL_20250701_000000_20250906_000000.arrayrecord")

        if not os.path.exists(file_path):
            print(f"⚠️  Missing file for {timeframe}: {file_path}")
            continue

        print(f"📊 Analyzing {timeframe} timeframe...")

        # File properties
        file_size = os.path.getsize(file_path)
        file_hash = get_file_hash(file_path)

        # ArrayRecord metadata
        try:
            column_names, record_count = read_arrayrecord_metadata(file_path)

            # Timeframe distribution
            tf_distribution = analyze_timeframe_distribution(column_names)

            # Feature type analysis
            feature_types = analyze_feature_types(column_names)

            file_analysis[timeframe] = {
                'path': file_path,
                'size': file_size,
                'hash': file_hash,
                'column_count': len(column_names),
                'record_count': record_count,
                'timeframe_dist': tf_distribution,
                'feature_types': feature_types,
                'columns': column_names
            }

            print(f"  ✓ File size: {file_size:,} bytes")
            print(f"  ✓ MD5 hash: {file_hash}")
            print(f"  ✓ Columns: {len(column_names)}")
            print(f"  ✓ Records: {record_count:,}")

        except Exception as e:
            print(f"  ❌ Error analyzing {timeframe}: {e}")
            file_analysis[timeframe] = {'error': str(e)}

    print()
    print("🚨 BUG DETECTION RESULTS")
    print("=" * 40)

    # Check for identical files (critical bug indicator)
    hashes = [info.get('hash') for info in file_analysis.values() if 'hash' in info]
    sizes = [info.get('size') for info in file_analysis.values() if 'size' in info]

    if len(set(hashes)) == 1 and len(hashes) > 1:
        print("🔴 CRITICAL BUG DETECTED: All ArrayRecord files are IDENTICAL!")
        print(f"   All files have hash: {hashes[0]}")
        print("   This means timeframe separation is completely broken.")
    elif len(set(hashes)) < len(hashes):
        print("🟡 WARNING: Some ArrayRecord files are identical")
        hash_counts = {}
        for i, h in enumerate(hashes):
            if h not in hash_counts:
                hash_counts[h] = []
            hash_counts[h].append(timeframes[i])

        for hash_val, tfs in hash_counts.items():
            if len(tfs) > 1:
                print(f"   Hash {hash_val}: {', '.join(tfs)}")
    else:
        print("✅ File uniqueness: All ArrayRecord files are unique")

    print()
    print("📈 COLUMN COUNT ANALYSIS")
    print("=" * 30)

    for timeframe in timeframes:
        if timeframe in file_analysis and 'column_count' in file_analysis[timeframe]:
            info = file_analysis[timeframe]
            print(f"{timeframe:>3}: {info['column_count']:>4} columns")

            # Show timeframe distribution
            tf_dist = info['timeframe_dist']
            non_empty_tfs = [(tf, len(cols)) for tf, cols in tf_dist.items() if cols]
            if len(non_empty_tfs) > 1:
                print(f"     🔴 Mixed timeframes: {dict(non_empty_tfs)}")
            elif len(non_empty_tfs) == 1:
                tf_name, count = non_empty_tfs[0]
                if tf_name == timeframe:
                    print(f"     ✅ Pure {timeframe} features: {count}")
                else:
                    print(f"     🔴 Wrong timeframe: {count} {tf_name} features")

    print()
    print("🎯 EXPECTED vs ACTUAL STRUCTURE")
    print("=" * 35)

    expected_structure = {
        '5m': "timestamp, symbol, open, high, low, close, volume, vwap + indicators",
        '15m': "timestamp, symbol, 15m_open, 15m_high, 15m_low, 15m_close, 15m_volume, 15m_vwap + 15m_indicators",
        '1h': "timestamp, symbol, 1h_open, 1h_high, 1h_low, 1h_close, 1h_volume, 1h_vwap + 1h_indicators",
        '1d': "timestamp, symbol, 1d_open, 1d_high, 1d_low, 1d_close, 1d_volume, 1d_vwap + 1d_indicators",
        '1w': "timestamp, symbol, 1w_open, 1w_high, 1w_low, 1w_close, 1w_volume, 1w_vwap + 1w_indicators"
    }

    for timeframe in timeframes:
        print(f"\n{timeframe} TIMEFRAME:")
        print(f"  Expected: {expected_structure[timeframe]}")

        if timeframe in file_analysis and 'timeframe_dist' in file_analysis[timeframe]:
            tf_dist = file_analysis[timeframe]['timeframe_dist']

            actual_summary = []
            for tf, cols in tf_dist.items():
                if cols:
                    actual_summary.append(f"{len(cols)} {tf} features")

            print(f"  Actual:   {', '.join(actual_summary) if actual_summary else 'No features detected'}")

    print()
    print("🔧 RECOMMENDED FIXES")
    print("=" * 20)
    print("1. Update training dataset generation logic to separate timeframes")
    print("2. Each timeframe should only contain its native features:")
    print("   - 5m: base OHLCV without prefixes")
    print("   - Other: prefixed OHLCV (e.g., 1h_open, 1h_close)")
    print("3. Run comprehensive tests to validate separation")
    print("4. Regenerate training datasets with corrected logic")

    # Save detailed analysis to file
    output_file = "arrayrecord_bug_analysis.json"
    try:
        # Prepare serializable data
        serializable_analysis = {}
        for tf, info in file_analysis.items():
            if 'columns' in info:
                # Don't save full column list, just summaries
                serializable_analysis[tf] = {
                    'size': info['size'],
                    'hash': info['hash'],
                    'column_count': info['column_count'],
                    'record_count': info['record_count'],
                    'timeframe_dist_counts': {k: len(v) for k, v in info['timeframe_dist'].items()},
                    'feature_type_counts': {k: len(v) for k, v in info['feature_types'].items()}
                }
            else:
                serializable_analysis[tf] = info

        with open(output_file, 'w') as f:
            json.dump({
                'dataset_path': dataset_path,
                'analysis_timestamp': str(np.datetime64('now')),
                'bug_detected': len(set(hashes)) < len(hashes) if hashes else False,
                'file_analysis': serializable_analysis
            }, f, indent=2)

        print(f"\n📄 Detailed analysis saved to: {output_file}")
    except Exception as e:
        print(f"\n⚠️  Could not save analysis file: {e}")


if __name__ == "__main__":
    main()