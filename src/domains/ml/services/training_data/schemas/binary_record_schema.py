#!/usr/bin/env python3
"""
Dynamic Binary Record Schema System for Training Data

CRITICAL: This system replaces hardcoded OHLCV format with configurable
schema that can dynamically include technical indicators.

Design Goals:
1. Schema-driven: Define indicators via configuration
2. Dynamic format: Generate struct format based on available data
3. Extensible: Easy to add new indicators
4. Backward compatible: Defaults to OHLCV if no indicators specified
5. Performance: Efficient binary packing/unpacking
"""

import struct
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class FieldDefinition:
    """Define a field in the binary record."""
    name: str
    format_char: str  # struct format character ('f' for float, 'd' for double, etc.)
    description: str
    default_value: Any = 0.0


class BinaryRecordSchema:
    """
    Dynamic schema system for binary training data records.

    Supports configurable inclusion of technical indicators while maintaining
    efficient binary format and backward compatibility.
    """

    # Core OHLCV fields (always included)
    BASE_FIELDS = [
        FieldDefinition('timestamp', 'd', 'Unix timestamp (double)', 0.0),
        FieldDefinition('symbol_len', 'I', 'Symbol string length (uint32)', 0),
        FieldDefinition('symbol', 's', 'Symbol string (variable length)', ''),
        FieldDefinition('open', 'f', 'Open price (float)', 0.0),
        FieldDefinition('high', 'f', 'High price (float)', 0.0),
        FieldDefinition('low', 'f', 'Low price (float)', 0.0),
        FieldDefinition('close', 'f', 'Close price (float)', 0.0),
        FieldDefinition('volume', 'f', 'Volume (float)', 0.0),
    ]

    # Available technical indicators (configurable)
    AVAILABLE_TECHNICAL_INDICATORS = [
        # Basic envelope indicators
        FieldDefinition('envelope_top', 'f', 'Price envelope upper bound', 0.0),
        FieldDefinition('envelope_bot', 'f', 'Price envelope lower bound', 0.0),
        FieldDefinition('pldot', 'f', 'Pivot low dots', 0.0),

        # Traditional technical indicators
        FieldDefinition('sma_20', 'f', 'Simple Moving Average (20 periods)', 0.0),
        FieldDefinition('ema_12', 'f', 'Exponential Moving Average (12 periods)', 0.0),
        FieldDefinition('rsi_14', 'f', 'Relative Strength Index (14 periods)', 0.0),

        # Support/Resistance levels
        FieldDefinition('z1b', 'f', 'Support level 1', 0.0),
        FieldDefinition('z2b', 'f', 'Support level 2', 0.0),
        FieldDefinition('z5t', 'f', 'Resistance level 1', 0.0),
        FieldDefinition('z6t', 'f', 'Resistance level 2', 0.0),

        # Additional indicators (extensible)
        FieldDefinition('macd', 'f', 'MACD indicator', 0.0),
        FieldDefinition('macd_signal', 'f', 'MACD signal line', 0.0),
        FieldDefinition('bollinger_upper', 'f', 'Bollinger Bands upper', 0.0),
        FieldDefinition('bollinger_lower', 'f', 'Bollinger Bands lower', 0.0),
        FieldDefinition('stochastic_k', 'f', 'Stochastic %K', 0.0),
        FieldDefinition('williams_r', 'f', 'Williams %R', 0.0),
    ]

    def __init__(self, include_indicators: Optional[List[str]] = None, auto_detect: bool = True):
        """
        Initialize schema with specified technical indicators.

        Args:
            include_indicators: List of indicator names to include, or None for auto-detection
            auto_detect: If True, automatically include indicators found in data
        """
        self.include_indicators = include_indicators or []
        self.auto_detect = auto_detect

        # Build active schema
        self.active_fields = self.BASE_FIELDS.copy()
        self.indicator_fields = []

        if include_indicators:
            # Include specified indicators
            for indicator_name in include_indicators:
                indicator_field = self._find_indicator_field(indicator_name)
                if indicator_field:
                    self.indicator_fields.append(indicator_field)
                    self.active_fields.append(indicator_field)

        # Cache format strings for performance
        self._format_cache = {}

    def _find_indicator_field(self, name: str) -> Optional[FieldDefinition]:
        """Find indicator field definition by name."""
        for field in self.AVAILABLE_TECHNICAL_INDICATORS:
            if field.name == name:
                return field
        return None

    def auto_detect_indicators(self, interval_data: Dict) -> List[str]:
        """
        Auto-detect available technical indicators in interval data.

        Args:
            interval_data: Sample interval data to inspect

        Returns:
            List of detected indicator names
        """
        detected = []

        for field in self.AVAILABLE_TECHNICAL_INDICATORS:
            if field.name in interval_data and interval_data[field.name] is not None:
                # Check if the value is meaningful (not just default)
                value = interval_data[field.name]
                if isinstance(value, (int, float)) and value != 0.0:
                    detected.append(field.name)
                elif isinstance(value, str) and value.strip():
                    detected.append(field.name)

        return detected

    def update_schema_from_data(self, interval_data: Dict):
        """
        Update schema based on actual available data (auto-detection).

        Args:
            interval_data: Sample interval data to analyze
        """
        if not self.auto_detect:
            return

        detected = self.auto_detect_indicators(interval_data)

        # Add newly detected indicators
        for indicator_name in detected:
            if indicator_name not in [f.name for f in self.indicator_fields]:
                indicator_field = self._find_indicator_field(indicator_name)
                if indicator_field:
                    self.indicator_fields.append(indicator_field)
                    self.active_fields.append(indicator_field)

        # Clear format cache when schema changes
        self._format_cache.clear()

    def generate_format_string(self, symbol: str) -> str:
        """
        Generate struct format string for current schema.

        Args:
            symbol: Symbol for variable-length string calculation

        Returns:
            struct format string like '>dI4sfffffff'
        """
        cache_key = f"{len(self.active_fields)}_{len(symbol)}"
        if cache_key in self._format_cache:
            return self._format_cache[cache_key]

        format_parts = ['>']  # Big-endian byte order

        symbol_len = len(symbol.encode('utf-8'))

        for field in self.active_fields:
            if field.name == 'symbol':
                format_parts.append(f'{symbol_len}s')
            else:
                format_parts.append(field.format_char)

        format_string = ''.join(format_parts)
        self._format_cache[cache_key] = format_string

        return format_string

    def pack_interval(self, symbol: str, interval_data: Dict) -> bytes:
        """
        Pack interval data into binary format using current schema.

        Args:
            symbol: Symbol string
            interval_data: Interval data dictionary

        Returns:
            Binary record data
        """
        # Auto-detect indicators if enabled
        if self.auto_detect:
            self.update_schema_from_data(interval_data)

        # Prepare values in schema order
        values = []
        symbol_bytes = symbol.encode('utf-8')

        for field in self.active_fields:
            if field.name == 'timestamp':
                # Handle timestamp conversion
                ts = interval_data.get('timestamp', 0)
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts).timestamp()
                values.append(float(ts))

            elif field.name == 'symbol_len':
                values.append(len(symbol_bytes))

            elif field.name == 'symbol':
                values.append(symbol_bytes)

            else:
                # Get field value with fallback to default
                value = interval_data.get(field.name, field.default_value)

                # Type conversion based on format character
                if field.format_char == 'f':
                    values.append(float(value))
                elif field.format_char == 'd':
                    values.append(float(value))
                elif field.format_char == 'I':
                    values.append(int(value))
                else:
                    values.append(value)

        # Generate format and pack
        format_string = self.generate_format_string(symbol)

        try:
            return struct.pack(format_string, *values)
        except struct.error as e:
            raise ValueError(f"Failed to pack interval data: {e}. "
                           f"Format: {format_string}, Values: {len(values)} items")

    def unpack_record(self, symbol: str, binary_data: bytes) -> Dict:
        """
        Unpack binary record data back to dictionary format.

        Args:
            symbol: Symbol string (needed for format calculation)
            binary_data: Binary record data

        Returns:
            Dictionary with field names and values
        """
        format_string = self.generate_format_string(symbol)

        try:
            values = struct.unpack(format_string, binary_data)
        except struct.error as e:
            raise ValueError(f"Failed to unpack binary data: {e}. "
                           f"Format: {format_string}, Data length: {len(binary_data)} bytes")

        # Map values back to field names
        result = {}
        value_index = 0

        for field in self.active_fields:
            if field.name == 'symbol':
                # Decode symbol bytes
                result[field.name] = values[value_index].decode('utf-8')
            elif field.name == 'timestamp':
                # Convert back to datetime string for consistency
                result[field.name] = datetime.fromtimestamp(values[value_index]).isoformat()
            elif field.name != 'symbol_len':
                # Skip symbol_len as it's metadata
                result[field.name] = values[value_index]

            value_index += 1

        return result

    def get_schema_metadata(self) -> Dict:
        """
        Get schema metadata for storage/documentation.

        Returns:
            Dictionary with schema information
        """
        return {
            'version': '1.0',
            'base_fields': [
                {
                    'name': field.name,
                    'format': field.format_char,
                    'description': field.description
                }
                for field in self.BASE_FIELDS if field.name not in ['symbol_len']
            ],
            'technical_indicators': [
                {
                    'name': field.name,
                    'format': field.format_char,
                    'description': field.description
                }
                for field in self.indicator_fields
            ],
            'total_fields': len([f for f in self.active_fields if f.name != 'symbol_len']),
            'auto_detect': self.auto_detect,
            'include_indicators': self.include_indicators
        }

    def save_schema_to_file(self, file_path: str):
        """Save schema metadata to JSON file."""
        import json
        from pathlib import Path

        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w') as f:
            json.dump(self.get_schema_metadata(), f, indent=2)

    @classmethod
    def load_schema_from_file(cls, file_path: str) -> 'BinaryRecordSchema':
        """Load schema from JSON file."""
        with open(file_path, 'r') as f:
            metadata = json.load(f)

        include_indicators = [
            indicator['name'] for indicator in metadata.get('technical_indicators', [])
        ]

        return cls(
            include_indicators=include_indicators,
            auto_detect=metadata.get('auto_detect', True)
        )


# Pre-configured schema templates for common use cases
class SchemaTemplates:
    """Pre-configured schema templates for common training data scenarios."""

    @staticmethod
    def ohlcv_only() -> BinaryRecordSchema:
        """OHLCV only (backward compatibility)."""
        return BinaryRecordSchema(include_indicators=[], auto_detect=False)

    @staticmethod
    def basic_envelopes() -> BinaryRecordSchema:
        """OHLCV + basic envelope indicators."""
        return BinaryRecordSchema(
            include_indicators=['envelope_top', 'envelope_bot', 'pldot'],
            auto_detect=False
        )

    @staticmethod
    def traditional_ta() -> BinaryRecordSchema:
        """OHLCV + traditional technical analysis indicators."""
        return BinaryRecordSchema(
            include_indicators=[
                'envelope_top', 'envelope_bot', 'pldot',
                'sma_20', 'ema_12', 'rsi_14'
            ],
            auto_detect=False
        )

    @staticmethod
    def full_signals() -> BinaryRecordSchema:
        """OHLCV + all available technical indicators."""
        return BinaryRecordSchema(
            include_indicators=[
                'envelope_top', 'envelope_bot', 'pldot',
                'sma_20', 'ema_12', 'rsi_14',
                'z1b', 'z2b', 'z5t', 'z6t',
                'macd', 'macd_signal',
                'bollinger_upper', 'bollinger_lower',
                'stochastic_k', 'williams_r'
            ],
            auto_detect=False
        )

    @staticmethod
    def auto_detect() -> BinaryRecordSchema:
        """Auto-detect available indicators from data."""
        return BinaryRecordSchema(include_indicators=None, auto_detect=True)


if __name__ == "__main__":
    # Example usage and testing
    print("🧪 Testing Dynamic Binary Record Schema System")

    # Test different schema configurations
    schemas = [
        ("OHLCV Only", SchemaTemplates.ohlcv_only()),
        ("Basic Envelopes", SchemaTemplates.basic_envelopes()),
        ("Traditional TA", SchemaTemplates.traditional_ta()),
        ("Auto-Detect", SchemaTemplates.auto_detect()),
    ]

    # Sample interval data
    sample_interval = {
        'timestamp': '2025-07-01T09:30:00',
        'open': 300.50,
        'high': 305.75,
        'low': 299.25,
        'close': 302.80,
        'volume': 1250000.0,
        'envelope_top': 313.91,
        'envelope_bot': 293.19,
        'pldot': 296.25,
        'sma_20': 301.45,
        'rsi_14': 65.5
    }

    symbol = "TSLA"

    for schema_name, schema in schemas:
        print(f"\n📊 Testing {schema_name} Schema:")

        # Pack data
        binary_data = schema.pack_interval(symbol, sample_interval)
        print(f"   Binary size: {len(binary_data)} bytes")

        # Show format string
        format_string = schema.generate_format_string(symbol)
        print(f"   Format: {format_string}")

        # Unpack and verify
        unpacked = schema.unpack_record(symbol, binary_data)
        print(f"   Fields: {list(unpacked.keys())}")

        # Show metadata
        metadata = schema.get_schema_metadata()
        print(f"   Indicators: {len(metadata['technical_indicators'])}")

        print(f"   ✅ Pack/unpack successful")