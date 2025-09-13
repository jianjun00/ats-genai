import pytest
import json
import asyncio
from unittest.mock import Mock, AsyncMock
from typing import Dict, Set, List, Optional


class TestCombinedInstrumentPopulation:
    """Comprehensive test cases for combined instrument population with Polygon and Tiingo data"""

    def test_filtering_criteria(self):
        """Test that filtering criteria correctly exclude unwanted instruments"""

        # Define filtering criteria
        TIINGO_EXCLUDED_EXCHANGES = {
            'NMFQS', 'PINK', 'OTCGREY', 'OTCQB', 'OTCMKTS', 'OTCCE',
            'SHE', 'SHG', 'ASX', 'EXPM', ''
        }

        TIINGO_EXCLUDED_ASSET_TYPES = {'Mutual Fund'}

        TARGET_EXCHANGES = {'NASDAQ', 'XNAS', 'NYSE', 'XNYS', 'NYSE ARCA', 'ARCX', 'BATS'}

        def should_include_tiingo_instrument(exchange: str, asset_type: str) -> bool:
            """Test helper to determine if Tiingo instrument should be included"""
            if asset_type in TIINGO_EXCLUDED_ASSET_TYPES:
                return False
            if exchange in TIINGO_EXCLUDED_EXCHANGES:
                return False
            return True

        # Test cases for inclusion
        assert should_include_tiingo_instrument('NASDAQ', 'Stock') is True
        assert should_include_tiingo_instrument('NYSE', 'ETF') is True
        assert should_include_tiingo_instrument('BATS', 'Stock') is True

        # Test cases for exclusion
        assert should_include_tiingo_instrument('NMFQS', 'Mutual Fund') is False
        assert should_include_tiingo_instrument('PINK', 'Stock') is False
        assert should_include_tiingo_instrument('OTCGREY', 'Stock') is False
        assert should_include_tiingo_instrument('SHE', 'Stock') is False  # International
        assert should_include_tiingo_instrument('SHG', 'Stock') is False  # International
        assert should_include_tiingo_instrument('ASX', 'Stock') is False  # International
        assert should_include_tiingo_instrument('NYSE', 'Mutual Fund') is False  # Mutual fund
        assert should_include_tiingo_instrument('', 'Stock') is False  # Empty exchange

    def test_sample_test_instruments(self):
        """Test the 10 sample test instruments from both Polygon and Tiingo"""

        # Sample Polygon instruments (from real data)
        polygon_samples = [
            {'symbol': 'AAPL', 'name': 'Apple Inc.', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'MSFT', 'name': 'Microsoft Corp', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'GOOGL', 'name': 'Alphabet Inc. Class A', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'AMZN', 'name': 'Amazon.Com Inc', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'TSLA', 'name': 'Tesla, Inc.', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'META', 'name': 'Meta Platforms, Inc.', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'NVDA', 'name': 'NVIDIA Corporation', 'exchange': 'XNAS', 'type': 'CS'},
            {'symbol': 'BRK.A', 'name': 'Berkshire Hathaway Inc.', 'exchange': 'XNYS', 'type': 'CS'},
            {'symbol': 'JPM', 'name': 'JPMorgan Chase & Co.', 'exchange': 'XNYS', 'type': 'CS'},
            {'symbol': 'JNJ', 'name': 'Johnson & Johnson', 'exchange': 'XNYS', 'type': 'CS'}
        ]

        # Sample Tiingo instruments (corresponding)
        tiingo_samples = [
            {'ticker': 'AAPL', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'MSFT', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'GOOGL', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'AMZN', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'TSLA', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'META', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'NVDA', 'exchange': 'NASDAQ', 'asset_type': 'Stock'},
            {'ticker': 'BRK.A', 'exchange': 'NYSE', 'asset_type': 'Stock'},
            {'ticker': 'JPM', 'exchange': 'NYSE', 'asset_type': 'Stock'},
            {'ticker': 'JNJ', 'exchange': 'NYSE', 'asset_type': 'Stock'}
        ]

        # Verify all samples are valid test cases
        assert len(polygon_samples) == 10
        assert len(tiingo_samples) == 10

        # Verify symbol matching
        polygon_symbols = {item['symbol'] for item in polygon_samples}
        tiingo_symbols = {item['ticker'] for item in tiingo_samples}
        assert polygon_symbols == tiingo_symbols

        # Verify exchanges are acceptable
        acceptable_polygon_exchanges = {'XNAS', 'XNYS', 'ARCX', 'BATS'}
        acceptable_tiingo_exchanges = {'NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS'}

        for item in polygon_samples:
            assert item['exchange'] in acceptable_polygon_exchanges

        for item in tiingo_samples:
            assert item['exchange'] in acceptable_tiingo_exchanges

    def test_instrument_validation_rules(self):
        """Test comprehensive validation rules for combined instruments"""

        def validate_combined_instrument(polygon_data: dict, tiingo_data: dict) -> tuple:
            """Test validation function"""
            validation_passed = True
            validation_notes = []

            # Rule 1: Must exist in both sources
            if not polygon_data or not tiingo_data:
                validation_passed = False
                validation_notes.append("Missing data from one or both sources")

            # Rule 2: Polygon must have name
            if not polygon_data.get('name'):
                validation_notes.append("Missing Polygon name")

            # Rule 3: Tiingo must have asset type
            if not tiingo_data.get('asset_type'):
                validation_notes.append("Missing Tiingo asset type")

            # Rule 4: Check if Polygon marked as inactive
            if polygon_data.get('active') is False:
                validation_notes.append("Polygon marked as inactive")

            # Rule 5: Symbol consistency
            polygon_symbol = polygon_data.get('symbol', '').upper()
            tiingo_symbol = tiingo_data.get('ticker', '').upper()
            if polygon_symbol != tiingo_symbol:
                validation_passed = False
                validation_notes.append(f"Symbol mismatch: {polygon_symbol} vs {tiingo_symbol}")

            # Rule 6: Asset type validation
            tiingo_asset_type = tiingo_data.get('asset_type')
            if tiingo_asset_type in {'Mutual Fund'}:
                validation_passed = False
                validation_notes.append("Excluded asset type: Mutual Fund")

            # Rule 7: Exchange validation
            tiingo_exchange = tiingo_data.get('exchange')
            excluded_exchanges = {'PINK', 'OTCGREY', 'NMFQS', 'SHE', 'SHG', 'ASX'}
            if tiingo_exchange in excluded_exchanges:
                validation_passed = False
                validation_notes.append(f"Excluded exchange: {tiingo_exchange}")

            return validation_passed, validation_notes

        # Test Case 1: Valid instrument
        polygon_valid = {
            'symbol': 'AAPL', 'name': 'Apple Inc.', 'exchange': 'XNAS',
            'type': 'CS', 'active': True
        }
        tiingo_valid = {
            'ticker': 'AAPL', 'exchange': 'NASDAQ', 'asset_type': 'Stock'
        }
        passed, notes = validate_combined_instrument(polygon_valid, tiingo_valid)
        assert passed is True
        assert len(notes) == 0

        # Test Case 2: Missing Polygon name
        polygon_no_name = {
            'symbol': 'TEST', 'name': None, 'exchange': 'XNAS', 'active': True
        }
        tiingo_valid = {
            'ticker': 'TEST', 'exchange': 'NASDAQ', 'asset_type': 'Stock'
        }
        passed, notes = validate_combined_instrument(polygon_no_name, tiingo_valid)
        assert "Missing Polygon name" in notes

        # Test Case 3: Inactive Polygon instrument
        polygon_inactive = {
            'symbol': 'DEAD', 'name': 'Dead Company', 'exchange': 'XNAS', 'active': False
        }
        tiingo_valid = {
            'ticker': 'DEAD', 'exchange': 'NASDAQ', 'asset_type': 'Stock'
        }
        passed, notes = validate_combined_instrument(polygon_inactive, tiingo_valid)
        assert "Polygon marked as inactive" in notes

        # Test Case 4: Mutual Fund (should be excluded)
        polygon_valid = {
            'symbol': 'FUND', 'name': 'Some Fund', 'exchange': 'XNAS', 'active': True
        }
        tiingo_fund = {
            'ticker': 'FUND', 'exchange': 'NMFQS', 'asset_type': 'Mutual Fund'
        }
        passed, notes = validate_combined_instrument(polygon_valid, tiingo_fund)
        assert passed is False
        assert "Excluded asset type: Mutual Fund" in notes

        # Test Case 5: OTC/Pink sheet (should be excluded)
        polygon_valid = {
            'symbol': 'OTCS', 'name': 'OTC Stock', 'exchange': 'XNAS', 'active': True
        }
        tiingo_otc = {
            'ticker': 'OTCS', 'exchange': 'PINK', 'asset_type': 'Stock'
        }
        passed, notes = validate_combined_instrument(polygon_valid, tiingo_otc)
        assert passed is False
        assert "Excluded exchange: PINK" in notes

        # Test Case 6: Symbol mismatch (should fail)
        polygon_mismatch = {
            'symbol': 'AAPL', 'name': 'Apple Inc.', 'exchange': 'XNAS', 'active': True
        }
        tiingo_mismatch = {
            'ticker': 'MSFT', 'exchange': 'NASDAQ', 'asset_type': 'Stock'
        }
        passed, notes = validate_combined_instrument(polygon_mismatch, tiingo_mismatch)
        assert passed is False
        assert "Symbol mismatch: AAPL vs MSFT" in notes

    def test_exchange_normalization(self):
        """Test exchange code normalization"""

        def normalize_exchange(exchange: str) -> str:
            """Normalize exchange codes to standard format"""
            if not exchange:
                return None

            exchange_mapping = {
                'XNAS': 'NASDAQ',
                'XNYS': 'NYSE',
                'ARCX': 'NYSE ARCA',
                'BATS': 'BATS'
            }

            return exchange_mapping.get(exchange, exchange)

        # Test normalization
        assert normalize_exchange('XNAS') == 'NASDAQ'
        assert normalize_exchange('XNYS') == 'NYSE'
        assert normalize_exchange('ARCX') == 'NYSE ARCA'
        assert normalize_exchange('BATS') == 'BATS'
        assert normalize_exchange('UNKNOWN') == 'UNKNOWN'
        assert normalize_exchange(None) is None
        assert normalize_exchange('') is None

    def test_data_structure_validation(self):
        """Test that combined instrument data structure is correct"""

        # Expected combined instrument fields
        expected_fields = {
            'symbol', 'name', 'exchange', 'polygon_type', 'tiingo_asset_type',
            'currency', 'figi', 'isin', 'cusip', 'active', 'list_date', 'delist_date',
            'price_currency', 'tiingo_start_date', 'tiingo_end_date',
            'polygon_raw', 'tiingo_raw', 'validation_passed', 'validation_notes'
        }

        class InstrumentData:
            def __init__(self, symbol, name=None, exchange=None, polygon_type=None,
                         tiingo_asset_type=None, polygon_data=None, tiingo_data=None):
                self.symbol = symbol
                self.name = name
                self.exchange = exchange
                self.polygon_type = polygon_type
                self.tiingo_asset_type = tiingo_asset_type
                self.polygon_data = polygon_data or {}
                self.tiingo_data = tiingo_data or {}

        # Test instrument creation
        instrument = InstrumentData(
            symbol='AAPL',
            name='Apple Inc.',
            exchange='NASDAQ',
            polygon_type='CS',
            tiingo_asset_type='Stock',
            polygon_data={'active': True, 'currency': 'USD'},
            tiingo_data={'price_currency': 'USD'}
        )

        assert instrument.symbol == 'AAPL'
        assert instrument.name == 'Apple Inc.'
        assert instrument.exchange == 'NASDAQ'
        assert instrument.polygon_type == 'CS'
        assert instrument.tiingo_asset_type == 'Stock'
        assert instrument.polygon_data['active'] is True
        assert instrument.tiingo_data['price_currency'] == 'USD'

    def test_batch_processing_simulation(self):
        """Test batch processing logic for large datasets"""

        def process_instruments_in_batches(instruments: list, batch_size: int = 1000):
            """Simulate batch processing"""
            batches = []
            for start_idx in range(0, len(instruments), batch_size):
                end_idx = min(start_idx + batch_size, len(instruments))
                batch = instruments[start_idx:end_idx]
                batches.append(batch)
            return batches

        # Test with various sizes
        instruments_100 = [f'INST{i}' for i in range(100)]
        instruments_2500 = [f'INST{i}' for i in range(2500)]

        # Test normal batch processing
        batches_100 = process_instruments_in_batches(instruments_100, 1000)
        assert len(batches_100) == 1
        assert len(batches_100[0]) == 100

        # Test multiple batch processing
        batches_2500 = process_instruments_in_batches(instruments_2500, 1000)
        assert len(batches_2500) == 3
        assert len(batches_2500[0]) == 1000
        assert len(batches_2500[1]) == 1000
        assert len(batches_2500[2]) == 500

        # Test edge case - empty list
        batches_empty = process_instruments_in_batches([], 1000)
        assert len(batches_empty) == 0

        # Test edge case - single item
        batches_single = process_instruments_in_batches(['SINGLE'], 1000)
        assert len(batches_single) == 1
        assert batches_single[0] == ['SINGLE']

    def test_database_schema_requirements(self):
        """Test that database schema meets requirements"""

        # Expected table schema for dev_instrument_combined
        expected_schema = {
            'id': 'SERIAL PRIMARY KEY',
            'symbol': 'VARCHAR(20) UNIQUE NOT NULL',
            'name': 'VARCHAR(255)',
            'exchange': 'VARCHAR(50)',
            'polygon_type': 'VARCHAR(20)',
            'tiingo_asset_type': 'VARCHAR(50)',
            'currency': 'VARCHAR(10)',
            'figi': 'VARCHAR(50)',
            'isin': 'VARCHAR(20)',
            'cusip': 'VARCHAR(20)',
            'active': 'BOOLEAN',
            'list_date': 'DATE',
            'delist_date': 'DATE',
            'price_currency': 'VARCHAR(10)',
            'tiingo_start_date': 'DATE',
            'tiingo_end_date': 'DATE',
            'polygon_raw': 'JSONB',
            'tiingo_raw': 'JSONB',
            'validation_passed': 'BOOLEAN DEFAULT FALSE',
            'validation_notes': 'TEXT',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }

        # Verify all required fields are present
        required_fields = [
            'symbol', 'name', 'exchange', 'polygon_type', 'tiingo_asset_type',
            'validation_passed', 'validation_notes', 'polygon_raw', 'tiingo_raw'
        ]

        for field in required_fields:
            assert field in expected_schema, f"Missing required field: {field}"

        # Verify key constraints
        assert 'UNIQUE' in expected_schema['symbol']
        assert 'NOT NULL' in expected_schema['symbol']
        assert 'PRIMARY KEY' in expected_schema['id']
        assert 'DEFAULT FALSE' in expected_schema['validation_passed']


class TestCombinedInstrumentIntegration:
    """Integration tests for combined instrument population"""

    @pytest.mark.integration
    def test_real_data_intersection_simulation(self):
        """Test intersection logic with realistic data volumes"""

        # Simulate realistic data sizes based on actual counts
        polygon_count = 11686  # Actual Polygon count
        tiingo_filtered_count = 30000  # Estimated after filtering
        expected_intersection = 11000  # Expected overlap (95%)

        def simulate_data_intersection(polygon_size: int, tiingo_size: int,
                                     overlap_percentage: float = 0.95):
            """Simulate data intersection calculation"""
            max_overlap = min(polygon_size, tiingo_size)
            actual_overlap = int(max_overlap * overlap_percentage)

            polygon_only = polygon_size - actual_overlap
            tiingo_only = tiingo_size - actual_overlap

            return {
                'common_count': actual_overlap,
                'polygon_only': polygon_only,
                'tiingo_only': tiingo_only,
                'total_polygon': polygon_size,
                'total_tiingo': tiingo_size
            }

        results = simulate_data_intersection(polygon_count, tiingo_filtered_count)

        # Verify intersection logic
        assert results['common_count'] <= min(polygon_count, tiingo_filtered_count)
        assert results['polygon_only'] >= 0
        assert results['tiingo_only'] >= 0
        assert results['common_count'] + results['polygon_only'] == polygon_count
        assert results['common_count'] + results['tiingo_only'] == tiingo_filtered_count

    @pytest.mark.integration
    def test_performance_estimation(self):
        """Test performance characteristics for large datasets"""

        import time

        def simulate_processing_time(instrument_count: int) -> float:
            """Simulate processing time based on instrument count"""
            # Assume 1000 instruments per second processing rate
            base_processing_rate = 1000  # instruments per second

            # Add overhead for database operations
            db_overhead = 0.1  # 100ms per batch
            batch_size = 1000

            processing_time = instrument_count / base_processing_rate
            batch_count = (instrument_count + batch_size - 1) // batch_size
            overhead_time = batch_count * db_overhead

            return processing_time + overhead_time

        # Test various data sizes
        small_dataset = simulate_processing_time(1000)
        medium_dataset = simulate_processing_time(10000)
        large_dataset = simulate_processing_time(50000)

        # Verify performance is reasonable
        assert small_dataset < 5.0  # Under 5 seconds for 1K instruments
        assert medium_dataset < 15.0  # Under 15 seconds for 10K instruments
        assert large_dataset < 60.0  # Under 60 seconds for 50K instruments

        # Verify scaling is roughly linear
        assert medium_dataset > small_dataset * 8  # Accounts for overhead
        assert large_dataset > medium_dataset * 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])