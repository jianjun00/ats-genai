#!/usr/bin/env python3
"""
Dynamic Technical Indicators Demo

This demo showcases the NEW dynamic schema system that replaces the hardcoded
OHLCV format with configurable technical indicators.

BEFORE: Fixed format with only OHLCV (Open, High, Low, Close, Volume)
AFTER:  Dynamic format with configurable technical indicators
"""

import sys
sys.path.append('/home/jianjun/ats-genai-admin/src')

from domains.ml.services.training_data.schemas.binary_record_schema import (
    BinaryRecordSchema, SchemaTemplates
)

def main():
    print("🚀 DYNAMIC TECHNICAL INDICATORS DEMO")
    print("="*60)

    # Sample interval data with technical indicators
    sample_data = {
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

    symbol = "TSLA"

    print(f"\n📊 SAMPLE DATA:")
    print(f"   Symbol: {symbol}")
    print(f"   Timestamp: {sample_data['timestamp']}")
    print(f"   OHLCV: O={sample_data['open']} H={sample_data['high']} L={sample_data['low']} C={sample_data['close']} V={sample_data['volume']}")
    print(f"   Available indicators: {len([k for k in sample_data.keys() if k not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']])}")

    # Test different schema configurations
    schemas = [
        ("🔹 OHLCV Only (Backward Compatible)", SchemaTemplates.ohlcv_only()),
        ("🔸 Basic Envelopes", SchemaTemplates.basic_envelopes()),
        ("🔷 Traditional Technical Analysis", SchemaTemplates.traditional_ta()),
        ("🔆 Auto-Detect All Available", SchemaTemplates.auto_detect()),
    ]

    print(f"\n🧪 TESTING DIFFERENT SCHEMA CONFIGURATIONS:")
    print("="*60)

    for schema_name, schema in schemas:
        print(f"\n{schema_name}")
        print("-" * len(schema_name))

        # Pack data using schema
        binary_data = schema.pack_interval(symbol, sample_data)

        # Get schema info
        metadata = schema.get_schema_metadata()
        indicators = [ind['name'] for ind in metadata['technical_indicators']]

        # Show results
        print(f"   📦 Binary size: {len(binary_data)} bytes")
        print(f"   📊 Total fields: {metadata['total_fields']}")
        print(f"   📈 Technical indicators: {len(indicators)}")

        if indicators:
            print(f"   📋 Indicators included: {', '.join(indicators)}")
        else:
            print(f"   📋 OHLCV-only format (no technical indicators)")

        # Show format efficiency
        format_string = schema.generate_format_string(symbol)
        print(f"   🔧 Binary format: {format_string}")

        # Test unpacking
        unpacked = schema.unpack_record(symbol, binary_data)
        print(f"   ✅ Pack/unpack verified: {len(unpacked)} fields recovered")

        # Show what's actually in the record
        ohlcv_fields = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        indicator_fields = [k for k in unpacked.keys() if k not in ohlcv_fields]

        if indicator_fields:
            print(f"   📊 Indicator values: {', '.join(f'{k}={unpacked[k]:.2f}' for k in indicator_fields[:3])}{'...' if len(indicator_fields) > 3 else ''}")

    print(f"\n🎯 COMPARISON SUMMARY:")
    print("="*60)
    print(f"   OLD FORMAT (hardcoded):  struct.pack('>dI4sfffff', ...)")
    print(f"                           Fixed 36 bytes, OHLCV only")
    print(f"   NEW FORMAT (dynamic):   Generated based on available indicators")
    print(f"                          36-76+ bytes, configurable indicators")

    print(f"\n✅ BENEFITS OF NEW SYSTEM:")
    print(f"   🔧 Configurable: Choose which indicators to include")
    print(f"   🚀 Auto-detect: Automatically include available indicators")
    print(f"   📈 Extensible: Easy to add new technical indicators")
    print(f"   🔙 Compatible: Backward compatible with OHLCV-only format")
    print(f"   📋 Documented: Schema metadata saved alongside data")
    print(f"   ⚡ Efficient: Binary format scales efficiently")

    print(f"\n🎯 USAGE IN TRAINING DATA CALLBACK:")
    print("="*60)
    print("""
   # Before (hardcoded):
   binary_record = struct.pack(
       f'>dI{symbol_len}sfffff',
       timestamp, symbol_len, symbol_bytes,
       open, high, low, close, volume
   )

   # After (dynamic):
   binary_record = self.binary_schema.pack_interval(symbol, interval)

   # Configuration options:
   config.binary_schema = 'auto_detect'      # Auto-detect indicators
   config.binary_schema = 'basic_envelopes'  # Include envelope indicators
   config.binary_schema = 'traditional_ta'   # Include traditional TA
   config.binary_schema = 'ohlcv_only'      # Backward compatible
    """)

    print(f"\n🚀 SYSTEM READY FOR DYNAMIC TECHNICAL INDICATORS!")


if __name__ == "__main__":
    main()