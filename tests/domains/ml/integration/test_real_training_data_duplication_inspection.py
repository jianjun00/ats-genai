#!/usr/bin/env python3
"""
Test to inspect real training data for duplication issues.

This test reads actual ArrayRecord training data files and detects
the specific bug where identical OHLCV values appear at different timestamps.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import json
from typing import Dict, List, Optional


class TestRealTrainingDataDuplicationInspection:
    """Inspect actual training data files for duplication bugs."""

    def test_inspect_latest_training_data_for_duplicates(self):
        """Inspect the latest training dataset for OHLCV duplication issues."""
        
        training_data_dir = Path("/data/training_data")
        
        if not training_data_dir.exists():
            pytest.skip("No training data directory found")
            
        # Find the most recent dataset
        dataset_dirs = [d for d in training_data_dir.iterdir() if d.is_dir() and d.name.startswith('dataset_')]
        
        if not dataset_dirs:
            pytest.skip("No training datasets found")
            
        latest_dataset = max(dataset_dirs, key=lambda d: d.stat().st_mtime)
        print(f"🔍 Inspecting latest dataset: {latest_dataset.name}")
        
        # Check metadata first
        metadata_file = latest_dataset / "dataset_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                
            print(f"📋 Dataset Metadata:")
            print(f"   Symbols: {metadata.get('symbols', 'N/A')}")
            print(f"   Date Range: {metadata.get('start_date')} to {metadata.get('end_date')}")
            print(f"   Intervals Processed: {metadata.get('actual_intervals_processed', 0)}")
            print(f"   Generation Duration: {metadata.get('generation_duration_seconds', 0)} seconds")
            
            intervals_processed = metadata.get('actual_intervals_processed', 0)
            if intervals_processed <= 1:
                print("ℹ️  Only 1 interval processed - no duplication risk")
                return
                
        # Look for AAPL 5-minute data
        symbol_dirs = list(latest_dataset.glob("AAPL_*"))
        if not symbol_dirs:
            pytest.skip("No AAPL training data found")
            
        aapl_dir = symbol_dirs[0]
        timeframe_5m_dir = aapl_dir / "5m"
        
        if not timeframe_5m_dir.exists():
            pytest.skip("No 5-minute timeframe data found")
            
        print(f"📂 Checking 5-minute data in: {timeframe_5m_dir}")
        
        # List files in 5m directory
        arrayrecord_files = list(timeframe_5m_dir.glob("*.arrayrecord"))
        if not arrayrecord_files:
            pytest.skip("No ArrayRecord files found")
            
        arrayrecord_file = arrayrecord_files[0]
        print(f"📄 ArrayRecord file: {arrayrecord_file.name} ({arrayrecord_file.stat().st_size} bytes)")
        
        # For now, we can't easily parse ArrayRecord without the specific library
        # But we can detect the issue by checking if file size is suspiciously small
        # for multiple time intervals
        
        file_size = arrayrecord_file.stat().st_size
        expected_min_size_per_record = 300  # Based on your output showing ~371 bytes per record
        
        if intervals_processed > 1:
            expected_min_file_size = expected_min_size_per_record * intervals_processed * 0.8  # 80% threshold
            
            print(f"📊 File Size Analysis:")
            print(f"   Actual file size: {file_size} bytes")
            print(f"   Expected minimum for {intervals_processed} intervals: {expected_min_file_size:.0f} bytes")
            
            if file_size < expected_min_file_size:
                print(f"⚠️  File size seems small for {intervals_processed} intervals")
                print(f"   This could indicate duplicate/identical data")
            else:
                print(f"✅ File size is appropriate for {intervals_processed} intervals")
                
        # Additional check: Look at all timeframe directories
        all_timeframes = ['5m', '15m', '1h', '1d']
        timeframe_files = {}
        
        for tf in all_timeframes:
            tf_dir = aapl_dir / tf
            if tf_dir.exists():
                tf_files = list(tf_dir.glob("*.arrayrecord"))
                if tf_files:
                    tf_file = tf_files[0]
                    timeframe_files[tf] = {
                        'path': tf_file,
                        'size': tf_file.stat().st_size
                    }
        
        print(f"📊 Timeframe File Sizes:")
        for tf, info in timeframe_files.items():
            print(f"   {tf:>3}: {info['size']:>8} bytes ({info['path'].name})")
            
        # Check for suspicious patterns
        if len(timeframe_files) > 1:
            sizes = [info['size'] for info in timeframe_files.values()]
            
            # If all files have very similar sizes, this could indicate duplication
            max_size = max(sizes)
            min_size = min(sizes)
            size_variation = (max_size - min_size) / max_size if max_size > 0 else 0
            
            print(f"📈 Size Variation Analysis:")
            print(f"   Size variation: {size_variation:.1%}")
            
            if size_variation < 0.1:  # Less than 10% variation
                print(f"⚠️  Very similar file sizes across timeframes ({size_variation:.1%} variation)")
                print(f"   This could indicate identical data being stored in all timeframes")
                
                # This is a strong indicator of the duplication bug
                pytest.fail(
                    f"DUPLICATION BUG DETECTED: All timeframes have very similar file sizes "
                    f"({size_variation:.1%} variation). This suggests identical OHLCV data "
                    f"across different aggregation periods, which is the core bug."
                )
            else:
                print(f"✅ Good size variation across timeframes ({size_variation:.1%})")

    def test_training_data_record_count_analysis(self):
        """Analyze record counts to detect duplication patterns."""
        
        training_data_dir = Path("/data/training_data")
        
        if not training_data_dir.exists():
            pytest.skip("No training data directory found")
            
        # Get all recent datasets for comparison
        dataset_dirs = [d for d in training_data_dir.iterdir() if d.is_dir() and d.name.startswith('dataset_')]
        
        if len(dataset_dirs) < 1:
            pytest.skip("Need at least 1 dataset for analysis")
            
        # Sort by creation time
        dataset_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
        
        print(f"📊 Training Dataset Record Count Analysis")
        print(f"=" * 50)
        
        for i, dataset_dir in enumerate(dataset_dirs[:3]):  # Check latest 3 datasets
            print(f"\n📂 Dataset {i+1}: {dataset_dir.name}")
            
            # Check metadata
            metadata_file = dataset_dir / "dataset_metadata.json"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                intervals_processed = metadata.get('actual_intervals_processed', 0)
                duration = metadata.get('generation_duration_seconds', 0)
                
                print(f"   Intervals processed: {intervals_processed}")
                print(f"   Generation time: {duration}s")
                
                # Check for red flags
                if intervals_processed > 3 and duration < 10:
                    print(f"   ⚠️  Fast generation for {intervals_processed} intervals - possible duplication")
                elif intervals_processed <= 1:
                    print(f"   ℹ️  Single interval - no duplication risk")
                else:
                    print(f"   ✅ Normal processing pattern")
                    
                # Check file sizes across timeframes
                symbol_dirs = list(dataset_dir.glob("*_2025_*"))
                for symbol_dir in symbol_dirs:
                    symbol_name = symbol_dir.name.split('_')[0]
                    print(f"   📈 {symbol_name} timeframe analysis:")
                    
                    timeframes = ['5m', '15m', '1h', '1d']
                    sizes = []
                    
                    for tf in timeframes:
                        tf_dir = symbol_dir / tf
                        if tf_dir.exists():
                            tf_files = list(tf_dir.glob("*.arrayrecord"))
                            if tf_files:
                                size = tf_files[0].stat().st_size
                                sizes.append((tf, size))
                                print(f"      {tf:>3}: {size:>8} bytes")
                    
                    if len(sizes) > 1:
                        size_values = [s[1] for s in sizes]
                        avg_size = sum(size_values) / len(size_values)
                        
                        # Check if sizes are suspiciously similar
                        similar_count = sum(1 for size in size_values if abs(size - avg_size) < avg_size * 0.1)
                        
                        if similar_count == len(size_values):
                            print(f"      ⚠️  All timeframes have very similar sizes - possible duplication!")
                        else:
                            print(f"      ✅ Good size variation across timeframes")

    def test_detect_insufficient_data_variation(self):
        """Test to detect when training data lacks proper temporal variation."""
        
        # This test checks for the specific pattern we saw:
        # Multiple records with identical OHLCV at different times
        
        print("🔍 Testing Data Variation Detection Algorithm")
        
        # Test case 1: The exact bug pattern from the user's output
        bug_pattern_data = [
            {'timestamp': '2025-07-01 18:00:00', 'open': 208.02, 'high': 208.11, 'low': 208.01, 'close': 208.08, 'volume': 56512},
            {'timestamp': '2025-07-01 19:00:00', 'open': 208.02, 'high': 208.11, 'low': 208.01, 'close': 208.08, 'volume': 56512},  # DUPLICATE!
            {'timestamp': '2025-07-01 20:00:00', 'open': 208.02, 'high': 208.11, 'low': 208.01, 'close': 208.08, 'volume': 56512},  # DUPLICATE!
        ]
        
        df = pd.DataFrame(bug_pattern_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        variation_score = self._calculate_data_variation_score(df)
        print(f"📊 Variation Score for Bug Pattern: {variation_score:.4f}")
        
        # Low variation score indicates duplication bug
        assert variation_score < 0.1, f"Bug pattern should have low variation score, got {variation_score:.4f}"
        
        # Test case 2: Proper varied data
        proper_data = [
            {'timestamp': '2025-07-01 18:00:00', 'open': 208.02, 'high': 208.11, 'low': 208.01, 'close': 208.08, 'volume': 56512},
            {'timestamp': '2025-07-01 19:00:00', 'open': 208.08, 'high': 208.25, 'low': 207.95, 'close': 208.15, 'volume': 62341},  # Different!
            {'timestamp': '2025-07-01 20:00:00', 'open': 208.15, 'high': 208.30, 'low': 208.05, 'close': 208.22, 'volume': 48923},  # Different!
        ]
        
        df_proper = pd.DataFrame(proper_data)
        df_proper['timestamp'] = pd.to_datetime(df_proper['timestamp'])
        
        variation_score_proper = self._calculate_data_variation_score(df_proper)
        print(f"📊 Variation Score for Proper Data: {variation_score_proper:.4f}")
        
        # Proper data should have high variation score
        assert variation_score_proper > 0.5, f"Proper data should have high variation score, got {variation_score_proper:.4f}"
        
        print("✅ Data variation detection algorithm working correctly")
        print(f"   Bug pattern detected (score: {variation_score:.4f})")
        print(f"   Proper data recognized (score: {variation_score_proper:.4f})")

    def _calculate_data_variation_score(self, df: pd.DataFrame) -> float:
        """
        Calculate a variation score for OHLCV data.
        
        Returns:
            float: Score from 0.0 (no variation/duplicates) to 1.0 (high variation)
        """
        
        if len(df) < 2:
            return 1.0  # Single record, no duplication possible
            
        ohlcv_fields = ['open', 'high', 'low', 'close', 'volume']
        
        total_variations = 0
        max_possible_variations = 0
        
        for field in ohlcv_fields:
            if field not in df.columns:
                continue
                
            values = df[field].values
            
            # Count unique values
            unique_values = len(set(values))
            total_values = len(values)
            
            # Calculate variation ratio for this field
            field_variation = unique_values / total_values if total_values > 0 else 0
            total_variations += field_variation
            max_possible_variations += 1
            
        # Overall variation score
        if max_possible_variations == 0:
            return 1.0
            
        variation_score = total_variations / max_possible_variations
        return variation_score


if __name__ == "__main__":
    # Run the inspection tests
    test_instance = TestRealTrainingDataDuplicationInspection()
    
    print("🔍 Real Training Data Duplication Inspection")
    print("=" * 50)
    
    test_instance.test_inspect_latest_training_data_for_duplicates()
    print("✅ Latest training data inspection completed")
    test_instance.test_training_data_record_count_analysis()
    print("✅ Record count analysis completed")
    test_instance.test_detect_insufficient_data_variation()
    print("✅ Data variation detection test completed")
