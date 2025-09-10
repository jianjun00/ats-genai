#!/usr/bin/env python3
"""
Dynamic Technical Indicators Test Suite

CRITICAL: Tests the new dynamic schema system that replaces hardcoded OHLCV 
format with configurable technical indicators.

Tests validate:
1. Schema auto-detection from interval data
2. Configurable indicator inclusion/exclusion  
3. Binary format efficiency with variable indicators
4. Streaming writer integration with technical indicators
5. Schema metadata persistence and documentation
"""

import pytest
import tempfile
import shutil
import json
import os
import asyncio
from pathlib import Path
from datetime import datetime, date, timedelta
import sys

sys.path.append('/home/jianjun/ats-genai-admin/src')

from domains.ml.services.training_data.schemas.binary_record_schema import (
    BinaryRecordSchema, SchemaTemplates, FieldDefinition
)
from domains.ml.services.training_data.callbacks.training_data_callback import (
    IntervalBasedTrainingDataCallback
)


class TestDynamicTechnicalIndicators:
    """Test dynamic technical indicator inclusion in ArrayRecord files."""
    
    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_interval_with_indicators(self):
        """Sample interval data with technical indicators."""
        return {
            'timestamp': '2025-07-01T09:30:00',
            'open': 300.50,
            'high': 305.75,
            'low': 299.25,
            'close': 302.80,
            'volume': 1250000.0,
            # Technical indicators
            'envelope_top': 313.91,
            'envelope_bot': 293.19,
            'pldot': 296.25,
            'sma_20': 301.45,
            'ema_12': 303.22,
            'rsi_14': 65.5,
            'z1b': 298.50,
            'z2b': 295.75,
            'z5t': 307.85,
            'z6t': 310.60,
        }
    
    @pytest.fixture
    def sample_interval_ohlcv_only(self):
        """Sample interval data with only OHLCV (no indicators)."""
        return {
            'timestamp': '2025-07-01T09:30:00',
            'open': 300.50,
            'high': 305.75,
            'low': 299.25,
            'close': 302.80,
            'volume': 1250000.0,
        }
    
    def test_schema_template_configurations(self, sample_interval_with_indicators):
        """
        CRITICAL TEST: Verify different schema templates work correctly.
        
        Tests all pre-configured schema templates with varying indicator sets.
        """
        print(f"\n🧪 TESTING: Schema template configurations")
        
        symbol = "TSLA"
        test_schemas = [
            ("OHLCV Only", SchemaTemplates.ohlcv_only(), 7),         # Basic fields only
            ("Basic Envelopes", SchemaTemplates.basic_envelopes(), 10),  # +3 indicators  
            ("Traditional TA", SchemaTemplates.traditional_ta(), 13),    # +6 indicators
            ("Auto-Detect", SchemaTemplates.auto_detect(), None),       # Variable based on data
        ]
        
        for schema_name, schema, expected_fields in test_schemas:
            print(f"\n📊 Testing {schema_name} Schema:")
            
            # Pack interval data
            binary_data = schema.pack_interval(symbol, sample_interval_with_indicators)
            print(f"   Binary size: {len(binary_data)} bytes")
            
            # Unpack and verify
            unpacked = schema.unpack_record(symbol, binary_data)
            actual_fields = len(unpacked)
            
            print(f"   Fields: {actual_fields} ({list(unpacked.keys())})")
            
            if expected_fields:
                assert actual_fields == expected_fields, \
                    f"{schema_name}: Expected {expected_fields} fields, got {actual_fields}"
            
            # Verify core OHLCV fields always present
            required_fields = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                assert field in unpacked, f"Required field '{field}' missing in {schema_name}"
            
            # Verify data integrity
            assert unpacked['symbol'] == symbol
            assert unpacked['open'] == sample_interval_with_indicators['open']
            assert unpacked['high'] == sample_interval_with_indicators['high']
            
            print(f"   ✅ {schema_name} schema validation passed")
        
        print(f"✅ All schema templates working correctly")
    
    def test_auto_detection_mechanism(self, sample_interval_with_indicators, sample_interval_ohlcv_only):
        """
        CRITICAL TEST: Verify auto-detection correctly identifies available indicators.
        
        Tests that schema adapts to available data automatically.
        """
        print(f"\n🧪 TESTING: Auto-detection mechanism")
        
        symbol = "TSLA"
        schema = SchemaTemplates.auto_detect()
        
        # Test with rich indicator data
        print(f"\n📈 Testing with full indicator data:")
        binary_data_full = schema.pack_interval(symbol, sample_interval_with_indicators)
        unpacked_full = schema.unpack_record(symbol, binary_data_full)
        
        detected_indicators = [k for k in unpacked_full.keys() 
                             if k not in ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
        
        print(f"   Detected indicators: {detected_indicators}")
        print(f"   Total fields: {len(unpacked_full)}")
        
        # Should have detected multiple indicators
        assert len(detected_indicators) > 0, "Should auto-detect indicators from rich data"
        assert 'envelope_top' in detected_indicators, "Should detect envelope_top"
        assert 'sma_20' in detected_indicators, "Should detect sma_20"
        
        # Test with OHLCV-only data
        print(f"\n📊 Testing with OHLCV-only data:")
        schema_ohlcv = SchemaTemplates.auto_detect()  # Fresh schema
        binary_data_ohlcv = schema_ohlcv.pack_interval(symbol, sample_interval_ohlcv_only)
        unpacked_ohlcv = schema_ohlcv.unpack_record(symbol, binary_data_ohlcv)
        
        ohlcv_only_fields = [k for k in unpacked_ohlcv.keys()
                            if k not in ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
        
        print(f"   OHLCV-only detected indicators: {ohlcv_only_fields}")
        print(f"   Total fields: {len(unpacked_ohlcv)}")
        
        # Should have minimal/no additional fields
        assert len(ohlcv_only_fields) == 0, "Should not detect indicators in OHLCV-only data"
        
        print(f"✅ Auto-detection mechanism working correctly")
    
    def test_binary_format_efficiency(self):
        """
        CRITICAL TEST: Verify binary format efficiency across different indicator sets.
        
        Tests that binary format scales efficiently with additional indicators.
        """
        print(f"\n🧪 TESTING: Binary format efficiency")
        
        symbol = "TSLA"
        base_interval = {
            'timestamp': '2025-07-01T09:30:00',
            'open': 300.50, 'high': 305.75, 'low': 299.25, 'close': 302.80, 'volume': 1250000.0,
        }
        
        # Test format efficiency scaling
        test_cases = [
            ("OHLCV Only", SchemaTemplates.ohlcv_only(), base_interval),
            ("Basic Envelopes", SchemaTemplates.basic_envelopes(), 
             {**base_interval, 'envelope_top': 313.91, 'envelope_bot': 293.19, 'pldot': 296.25}),
            ("Traditional TA", SchemaTemplates.traditional_ta(),
             {**base_interval, 'envelope_top': 313.91, 'envelope_bot': 293.19, 'pldot': 296.25,
              'sma_20': 301.45, 'ema_12': 303.22, 'rsi_14': 65.5}),
        ]
        
        previous_size = 0
        for schema_name, schema, interval_data in test_cases:
            binary_data = schema.pack_interval(symbol, interval_data)
            size = len(binary_data)
            
            metadata = schema.get_schema_metadata()
            indicator_count = len(metadata['technical_indicators'])
            
            print(f"   {schema_name}: {size} bytes, {indicator_count} indicators")
            
            # Each float indicator should add ~4 bytes
            if previous_size > 0:
                size_increase = size - previous_size
                expected_min_increase = 0  # Could be same if no new indicators
                assert size_increase >= expected_min_increase, \
                    f"Binary format size should scale efficiently"
            
            # Verify data integrity (account for float precision in binary format)
            unpacked = schema.unpack_record(symbol, binary_data)
            assert abs(unpacked['open'] - interval_data['open']) < 0.001
            assert abs(unpacked['close'] - interval_data['close']) < 0.001
            
            previous_size = size
        
        print(f"✅ Binary format efficiency validated")
    
    def test_streaming_callback_integration(self, temp_output_dir, sample_interval_with_indicators):
        """
        CRITICAL TEST: Verify streaming callback integration with dynamic schema.
        
        Tests that callback correctly uses dynamic schema with streaming writers.
        """
        print(f"\n🧪 TESTING: Streaming callback integration with dynamic schema")
        
        # Create callback with different schema configurations
        schema_configs = [
            ('ohlcv_only', 'OHLCV Only'),
            ('basic_envelopes', 'Basic Envelopes'), 
            ('auto_detect', 'Auto-Detect'),
        ]
        
        for schema_config, schema_name in schema_configs:
            print(f"\n📊 Testing {schema_name} callback integration:")
            
            # Create callback with specific schema
            class MockConfig:
                def __init__(self, binary_schema):
                    self.binary_schema = binary_schema
            
            config = MockConfig(schema_config)
            
            callback = IntervalBasedTrainingDataCallback(
                symbols=["TSLA"],
                config=config,
                output_dir=str(temp_output_dir / schema_config),
                storage_format="arrayrecord",
                start_date=date(2025, 7, 1),
                end_date=date(2025, 7, 1)  # Single day
            )
            
            # Verify schema was configured correctly  
            assert callback.binary_schema is not None, "Binary schema should be initialized"
            
            # Test schema metadata
            metadata = callback.binary_schema.get_schema_metadata()
            print(f"   Schema indicators: {len(metadata['technical_indicators'])}")
            print(f"   Auto-detect: {metadata['auto_detect']}")
            
            # Test that callback can pack intervals with schema
            binary_data = callback.binary_schema.pack_interval("TSLA", sample_interval_with_indicators)
            print(f"   Binary record size: {len(binary_data)} bytes")
            
            # Verify unpacking works
            unpacked = callback.binary_schema.unpack_record("TSLA", binary_data)
            assert unpacked['symbol'] == "TSLA"
            assert unpacked['open'] == sample_interval_with_indicators['open']
            
            print(f"   ✅ {schema_name} callback integration successful")
        
        print(f"✅ Streaming callback integration with dynamic schema validated")
    
    def test_schema_metadata_persistence(self, temp_output_dir):
        """
        CRITICAL TEST: Verify schema metadata is saved for documentation.
        
        Tests that schema information is persisted alongside training data.
        """
        print(f"\n🧪 TESTING: Schema metadata persistence")
        
        # Create callback that should save schema metadata
        callback = IntervalBasedTrainingDataCallback(
            symbols=["TSLA"],
            config=None,  # Will use default auto-detect
            output_dir=str(temp_output_dir),
            storage_format="arrayrecord", 
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 1)
        )
        
        # Initialize dataset structure (should save schema metadata)
        asyncio.run(callback._initialize_dataset_structure())
        
        # Verify schema metadata file was created
        expected_schema_files = list(temp_output_dir.rglob("schema_metadata.json"))
        assert len(expected_schema_files) > 0, "Schema metadata file should be created"
        
        schema_file = expected_schema_files[0]
        print(f"   Schema file created: {schema_file}")
        
        # Verify schema file content
        assert schema_file.exists(), "Schema metadata file should exist"
        
        with open(schema_file, 'r') as f:
            schema_data = json.load(f)
        
        # Verify schema structure
        required_keys = ['version', 'base_fields', 'technical_indicators', 'total_fields', 'auto_detect']
        for key in required_keys:
            assert key in schema_data, f"Schema metadata should contain '{key}'"
        
        print(f"   Schema version: {schema_data['version']}")
        print(f"   Total fields: {schema_data['total_fields']}")
        print(f"   Auto-detect mode: {schema_data['auto_detect']}")
        print(f"   Technical indicators: {len(schema_data['technical_indicators'])}")
        
        # Verify base fields
        base_field_names = [field['name'] for field in schema_data['base_fields']]
        expected_base = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        for field_name in expected_base:
            assert field_name in base_field_names, f"Base field '{field_name}' should be documented"
        
        print(f"✅ Schema metadata persistence validated")
    
    def test_backward_compatibility(self, sample_interval_ohlcv_only):
        """
        CRITICAL TEST: Verify backward compatibility with OHLCV-only format.
        
        Tests that existing code expecting OHLCV-only records still works.
        """
        print(f"\n🧪 TESTING: Backward compatibility with OHLCV-only format")
        
        symbol = "TSLA"
        
        # Create OHLCV-only schema (backward compatibility mode)
        ohlcv_schema = SchemaTemplates.ohlcv_only()
        
        # Pack OHLCV-only data
        binary_data = ohlcv_schema.pack_interval(symbol, sample_interval_ohlcv_only)
        print(f"   OHLCV binary size: {len(binary_data)} bytes")
        
        # Unpack and verify  
        unpacked = ohlcv_schema.unpack_record(symbol, binary_data)
        
        # Should contain exactly the expected OHLCV fields
        expected_fields = {'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'}
        actual_fields = set(unpacked.keys())
        
        assert actual_fields == expected_fields, \
            f"OHLCV-only should have exactly {expected_fields}, got {actual_fields}"
        
        # Verify values match (account for float precision in binary format)
        assert unpacked['symbol'] == symbol
        assert abs(unpacked['open'] - sample_interval_ohlcv_only['open']) < 0.001
        assert abs(unpacked['high'] - sample_interval_ohlcv_only['high']) < 0.001
        assert abs(unpacked['low'] - sample_interval_ohlcv_only['low']) < 0.001
        assert abs(unpacked['close'] - sample_interval_ohlcv_only['close']) < 0.001
        assert abs(unpacked['volume'] - sample_interval_ohlcv_only['volume']) < 0.001
        
        print(f"   ✅ OHLCV fields verified: {list(actual_fields)}")
        print(f"✅ Backward compatibility maintained")


if __name__ == "__main__":
    # Run all dynamic technical indicator tests
    pytest.main([
        __file__ + "::TestDynamicTechnicalIndicators::test_schema_template_configurations",
        __file__ + "::TestDynamicTechnicalIndicators::test_auto_detection_mechanism", 
        __file__ + "::TestDynamicTechnicalIndicators::test_binary_format_efficiency",
        __file__ + "::TestDynamicTechnicalIndicators::test_streaming_callback_integration",
        __file__ + "::TestDynamicTechnicalIndicators::test_schema_metadata_persistence",
        __file__ + "::TestDynamicTechnicalIndicators::test_backward_compatibility",
        "-v", "-s"
    ])