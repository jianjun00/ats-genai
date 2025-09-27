#!/usr/bin/env python3
"""
Test ArrayRecord writer initialization and validation for 1d timeframes.

The core issue may be that 1d ArrayRecord writers are not being initialized correctly,
resulting in files that exist but cannot be read properly.

This test validates the writer creation, data writing, and reading process specifically for 1d data.
"""

import pytest
import tempfile
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import array_record.python.array_record_module as array_record


class TestArrayRecordWriterValidation:
    """Test ArrayRecord writer initialization and validation for all timeframes."""

    @pytest.fixture
    def test_output_dir(self):
        """Create temporary directory for test output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    def create_test_schema_for_timeframe(self, timeframe: str) -> str:
        """Create a test schema for the given timeframe."""
        schema = {
            "type": "record",
            "name": f"TrainingData_{timeframe}",
            "fields": [
                {"name": "timestamp", "type": "double"},
                {"name": "symbol", "type": "string"},
                {"name": "open", "type": "double"},
                {"name": "high", "type": "double"},
                {"name": "low", "type": "double"},
                {"name": "close", "type": "double"},
                {"name": "volume", "type": "double"},
            ]
        }
        return json.dumps(schema)

    def create_test_record_for_timeframe(self, timeframe: str, index: int) -> Dict[str, Any]:
        """Create a test record for the given timeframe."""
        base_time = 1751402100.0  # 2025-07-01 00:00:00 UTC
        
        if timeframe == '1d':
            # Daily data - one record per day
            timestamp = base_time + (index * 86400)  # Add days
            volume = 500000 + (index * 10000)
        elif timeframe == '5m':
            # 5-minute data
            timestamp = base_time + (index * 300)  # Add 5 minutes
            volume = 1000 + (index * 10)
        elif timeframe == '15m':
            # 15-minute data
            timestamp = base_time + (index * 900)  # Add 15 minutes
            volume = 3000 + (index * 30)
        else:
            # Default to 5m behavior
            timestamp = base_time + (index * 300)
            volume = 1000 + (index * 10)
        
        return {
            'timestamp': timestamp,
            'symbol': 'AAPL',
            'open': 207.50 + (index * 0.1),
            'high': 208.00 + (index * 0.1),
            'low': 207.30 + (index * 0.1),
            'close': 207.75 + (index * 0.1),
            'volume': float(volume),
        }

    @pytest.mark.asyncio
    async def test_arrayrecord_writer_initialization_comparison(self, test_output_dir):
        """Test ArrayRecord writer initialization for multiple timeframes."""
        print("🔧 TESTING ARRAYRECORD WRITER INITIALIZATION ACROSS TIMEFRAMES")
        
        timeframes = ['5m', '15m', '1d']
        results = {}
        
        for timeframe in timeframes:
            print(f"\n📊 Testing {timeframe} ArrayRecord writer...")
            
            # Create schema and file path
            schema = self.create_test_schema_for_timeframe(timeframe)
            file_path = test_output_dir / f"test_{timeframe}.arrayrecord"
            
            try:
                # Initialize writer
                print(f"🔧 Initializing writer for {timeframe}...")
                writer = array_record.ArrayRecordWriter(str(file_path), schema)
                
                # Write test records
                print(f"📝 Writing test records for {timeframe}...")
                record_count = 3 if timeframe == '1d' else 10
                
                for i in range(record_count):
                    record = self.create_test_record_for_timeframe(timeframe, i)
                    writer.write(record)
                
                # Close writer
                writer.close()
                print(f"✅ Writer closed for {timeframe}")
                
                # Verify file exists and has size
                file_size = file_path.stat().st_size if file_path.exists() else 0
                print(f"📁 File size: {file_size} bytes")
                
                # Test reading the file
                print(f"📖 Testing read for {timeframe}...")
                reader = array_record.ArrayRecordReader(str(file_path))
                records = list(reader)
                
                results[timeframe] = {
                    'writer_success': True,
                    'file_exists': file_path.exists(),
                    'file_size': file_size,
                    'records_written': record_count,
                    'records_read': len(records),
                    'read_success': len(records) > 0,
                    'sample_record': records[0] if records else None
                }
                
                print(f"✅ {timeframe}: {len(records)} records read successfully")
                
            except Exception as e:
                print(f"❌ {timeframe}: Error - {e}")
                results[timeframe] = {
                    'writer_success': False,
                    'error': str(e),
                    'file_exists': file_path.exists() if file_path else False,
                    'file_size': file_path.stat().st_size if file_path and file_path.exists() else 0,
                    'records_written': 0,
                    'records_read': 0,
                    'read_success': False,
                    'sample_record': None
                }
        
        # Analysis
        print("\n📊 WRITER COMPARISON ANALYSIS:")
        for timeframe, result in results.items():
            status = "✅ SUCCESS" if result['read_success'] else "❌ FAILED"
            print(f"   {timeframe:>3s}: {status} - {result['records_read']}/{result['records_written']} records, {result['file_size']} bytes")
            
            if not result['read_success'] and result['file_exists'] and result['file_size'] > 0:
                print(f"       🚨 ISSUE: File exists ({result['file_size']} bytes) but reads as empty!")
        
        # Verify all timeframes work
        for timeframe, result in results.items():
            assert result['writer_success'], f"{timeframe} writer should initialize successfully"
            assert result['file_exists'], f"{timeframe} file should exist"
            assert result['file_size'] > 0, f"{timeframe} file should have content"
            assert result['read_success'], f"{timeframe} file should be readable"

    @pytest.mark.asyncio
    async def test_1d_arrayrecord_schema_validation(self, test_output_dir):
        """Test 1d ArrayRecord with different schema configurations."""
        print("🔍 TESTING 1D ARRAYRECORD SCHEMA VARIATIONS")
        
        # Test different schema configurations that might affect 1d reading
        schema_variations = [
            {
                'name': 'basic_ohlcv',
                'schema': {
                    "type": "record",
                    "name": "BasicOHLCV",
                    "fields": [
                        {"name": "timestamp", "type": "double"},
                        {"name": "symbol", "type": "string"},
                        {"name": "open", "type": "double"},
                        {"name": "high", "type": "double"},
                        {"name": "low", "type": "double"},
                        {"name": "close", "type": "double"},
                        {"name": "volume", "type": "double"},
                    ]
                }
            },
            {
                'name': 'with_nullable_fields',
                'schema': {
                    "type": "record",
                    "name": "OHLCVWithNulls",
                    "fields": [
                        {"name": "timestamp", "type": "double"},
                        {"name": "symbol", "type": "string"},
                        {"name": "open", "type": ["null", "double"], "default": None},
                        {"name": "high", "type": ["null", "double"], "default": None},
                        {"name": "low", "type": ["null", "double"], "default": None},
                        {"name": "close", "type": ["null", "double"], "default": None},
                        {"name": "volume", "type": ["null", "double"], "default": None},
                    ]
                }
            },
            {
                'name': 'minimal_fields',
                'schema': {
                    "type": "record", 
                    "name": "Minimal",
                    "fields": [
                        {"name": "timestamp", "type": "double"},
                        {"name": "symbol", "type": "string"},
                        {"name": "close", "type": "double"},
                    ]
                }
            }
        ]
        
        for variation in schema_variations:
            print(f"\n🧪 Testing schema variation: {variation['name']}")
            
            schema_json = json.dumps(variation['schema'])
            file_path = test_output_dir / f"1d_{variation['name']}.arrayrecord"
            
            try:
                # Write with this schema
                writer = array_record.ArrayRecordWriter(str(file_path), schema_json)
                
                # Create record compatible with this schema
                if variation['name'] == 'minimal_fields':
                    record = {
                        'timestamp': 1751402100.0,
                        'symbol': 'AAPL',
                        'close': 207.75,
                    }
                else:
                    record = self.create_test_record_for_timeframe('1d', 0)
                
                writer.write(record)
                writer.close()
                
                # Test reading
                reader = array_record.ArrayRecordReader(str(file_path))
                records = list(reader)
                
                print(f"✅ {variation['name']}: {len(records)} records, {file_path.stat().st_size} bytes")
                
                assert len(records) == 1, f"Should have 1 record for {variation['name']}"
                assert records[0]['symbol'] == 'AAPL', f"Symbol should be AAPL for {variation['name']}"
                
            except Exception as e:
                print(f"❌ {variation['name']}: Failed - {e}")
                raise

    @pytest.mark.asyncio
    async def test_1d_arrayrecord_large_volume_data(self, test_output_dir):
        """Test 1d ArrayRecord with larger data volume similar to real files."""
        print("📊 TESTING 1D ARRAYRECORD WITH REALISTIC DATA VOLUME")
        
        # The actual 1d file is 128K, let's create a similar volume
        file_path = test_output_dir / "1d_large_volume.arrayrecord"
        schema = self.create_test_schema_for_timeframe('1d')
        
        # Calculate how many records to create ~128K file
        # Estimate ~1K per record for JSON, maybe 200-500 bytes for binary
        target_records = 256  # Should create substantial file
        
        try:
            writer = array_record.ArrayRecordWriter(str(file_path), schema)
            
            print(f"📝 Writing {target_records} records to simulate real 1d data volume...")
            for i in range(target_records):
                record = self.create_test_record_for_timeframe('1d', i)
                writer.write(record)
            
            writer.close()
            
            file_size = file_path.stat().st_size
            print(f"📁 Created file: {file_size} bytes ({file_size/1024:.1f} KB)")
            
            # Test reading the large file
            print("📖 Reading large 1d file...")
            reader = array_record.ArrayRecordReader(str(file_path))
            records = list(reader)
            
            print(f"✅ Successfully read {len(records)} records from {file_size} byte file")
            
            # Validate sample records
            assert len(records) == target_records, f"Should read {target_records} records"
            assert all(r['symbol'] == 'AAPL' for r in records), "All records should have AAPL symbol"
            
            # Test that timestamps are increasing (daily progression)
            timestamps = [r['timestamp'] for r in records]
            assert timestamps == sorted(timestamps), "Timestamps should be in ascending order"
            
            # Verify daily spacing (86400 seconds = 1 day)
            if len(records) > 1:
                daily_diff = timestamps[1] - timestamps[0]
                assert abs(daily_diff - 86400) < 1, f"Daily records should be 86400 seconds apart, got {daily_diff}"
            
            print("✅ 1d large volume test PASSED")
            
        except Exception as e:
            print(f"❌ 1d large volume test FAILED: {e}")
            raise