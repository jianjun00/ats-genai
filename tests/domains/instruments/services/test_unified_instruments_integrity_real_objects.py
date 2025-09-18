#!/usr/bin/env python3
"""
Real Objects Test: Unified Instruments & Data Integrity

This replaces mock vendor data with real vendor API integration and database validation.
Tests use actual vendor data sources, real database constraints, and end-to-end data flow.

BEFORE: Mock vendor data hid integration and data quality issues
AFTER: Real vendor APIs reveal actual data inconsistencies, API failures, and constraint violations
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, AsyncGenerator

from shared.utils.environment import Environment, EnvironmentType
from domains.instruments.services.unified_instrument_populator import UnifiedInstrumentPopulator
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from infrastructure.vendor.polygon.polygon_instruments_dao import PolygonInstrumentsDAO
from infrastructure.vendor.eodhd.eodhd_instruments_dao import EodhdInstrumentsDAO
from infrastructure.vendor.tiingo.tiingo_instruments_dao import TiingoInstrumentsDAO


class TestUnifiedInstrumentCreationRealObjects:
    """Test unified instrument population with real vendor APIs and database integration"""

    @pytest.fixture(scope="session")
    async def test_environment(self) -> Environment:
        """Real test environment with actual database and vendor API keys"""
        return Environment(
            env_type=EnvironmentType.TEST,
            db_url="postgresql://test:test@localhost/test_unified_instruments_db"
        )

    @pytest.fixture
    async def clean_database(self, test_environment: Environment) -> AsyncGenerator[Environment, None]:
        """Clean database with real schema for unified instruments testing"""
        # Set up real database schema
        async with test_environment.get_connection() as conn:
            # Create real instruments table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_instruments (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL UNIQUE,
                    name VARCHAR(255) NOT NULL,
                    exchange VARCHAR(50) NOT NULL,
                    type VARCHAR(50) NOT NULL,
                    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
                    list_date DATE,
                    delist_date DATE,
                    market_cap BIGINT,
                    sector VARCHAR(100),
                    industry VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Create real cross-references table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_instrument_xrefs (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER NOT NULL REFERENCES test_instruments(id) ON DELETE CASCADE,
                    vendor_name VARCHAR(50) NOT NULL,
                    vendor_symbol VARCHAR(50) NOT NULL,
                    vendor_id VARCHAR(100),
                    xref_type VARCHAR(50) NOT NULL,
                    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    end_date DATE,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(vendor_name, vendor_symbol, xref_type)
                )
            """)

            # Create real vendor metadata table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_vendor_metadata (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER NOT NULL REFERENCES test_instruments(id) ON DELETE CASCADE,
                    vendor_name VARCHAR(50) NOT NULL,
                    raw_data JSONB NOT NULL,
                    last_updated TIMESTAMP DEFAULT NOW(),
                    data_quality_score DECIMAL(3,2),
                    UNIQUE(instrument_id, vendor_name)
                )
            """)

            # Clean up before test
            await conn.execute("TRUNCATE TABLE test_vendor_metadata, test_instrument_xrefs, test_instruments RESTART IDENTITY CASCADE")
        
        yield test_environment
        
        # Clean up after test
        async with test_environment.get_connection() as conn:
            await conn.execute("TRUNCATE TABLE test_vendor_metadata, test_instrument_xrefs, test_instruments RESTART IDENTITY CASCADE")

    @pytest.fixture
    async def real_vendor_daos(self, test_environment: Environment) -> Dict[str, object]:
        """Real vendor DAOs with actual API connections"""
        # These use real API keys from environment variables
        return {
            'polygon': PolygonInstrumentsDAO(test_environment),
            'eodhd': EodhdInstrumentsDAO(test_environment),
            'tiingo': TiingoInstrumentsDAO(test_environment)
        }

    @pytest.fixture
    async def unified_populator(
        self, 
        clean_database: Environment,
        real_vendor_daos: Dict[str, object]
    ) -> UnifiedInstrumentPopulator:
        """Real unified populator with actual vendor integrations"""
        return UnifiedInstrumentPopulator(
            environment=clean_database,
            vendor_daos=real_vendor_daos
        )

    # Test real vendor data merging with actual API calls

    @pytest.mark.asyncio
    async def test_real_vendor_data_merging_apple(self, unified_populator: UnifiedInstrumentPopulator):
        """Test merging real vendor data for AAPL from actual APIs"""
        # Use real symbol that exists across all vendor APIs
        symbol = "AAPL"
        
        # Fetch real data from all vendor APIs
        vendor_results = await unified_populator.fetch_symbol_from_all_vendors(symbol)
        
        # Verify real API responses
        assert len(vendor_results) > 0, "No vendor data returned - check API keys and connectivity"
        
        # Verify data from multiple real vendors
        vendor_names = {result['vendor'] for result in vendor_results}
        assert len(vendor_names) >= 2, f"Expected data from multiple vendors, got: {vendor_names}"
        
        # Test real data merging logic
        merged_instrument = await unified_populator.merge_vendor_data(symbol, vendor_results)
        
        # Verify real data merging results
        assert merged_instrument['symbol'] == symbol
        assert merged_instrument['name'] is not None
        assert len(merged_instrument['name'].strip()) > 0
        assert merged_instrument['exchange'] in ['NASDAQ', 'XNAS', 'NasdaqNM']  # Real exchange codes
        assert merged_instrument['currency'] == 'USD'
        
        # Verify metadata includes real vendor sources
        assert 'vendor_sources' in merged_instrument
        assert len(merged_instrument['vendor_sources']) >= 2

    @pytest.mark.asyncio
    async def test_real_vendor_data_consistency_validation(self, unified_populator: UnifiedInstrumentPopulator):
        """Test data consistency validation with real vendor data"""
        # Test multiple real symbols
        test_symbols = ["MSFT", "GOOGL", "AMZN"]
        
        inconsistencies = []
        
        for symbol in test_symbols:
            # Fetch real vendor data
            vendor_results = await unified_populator.fetch_symbol_from_all_vendors(symbol)
            
            if len(vendor_results) < 2:
                continue  # Skip if not available from multiple vendors
            
            # Check real data consistency
            consistency_report = await unified_populator.validate_data_consistency(symbol, vendor_results)
            
            # Verify consistency validation
            assert 'symbol' in consistency_report
            assert 'consistency_score' in consistency_report
            assert 'issues' in consistency_report
            
            # Document any real inconsistencies found
            if consistency_report['consistency_score'] < 0.8:
                inconsistencies.append({
                    'symbol': symbol,
                    'score': consistency_report['consistency_score'],
                    'issues': consistency_report['issues']
                })
        
        # Real vendor data may have inconsistencies - document them
        if inconsistencies:
            print(f"Real vendor data inconsistencies found: {inconsistencies}")
            # This is valuable information about actual data quality issues

    # Test real database integration with constraints

    @pytest.mark.asyncio
    async def test_unified_population_with_real_database_constraints(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test unified population with real database constraints and transactions"""
        # Test with real symbol
        symbol = "TSLA"
        
        # Execute real population process
        population_result = await unified_populator.populate_symbol(symbol)
        
        # Verify real database state
        assert population_result['success'] is True
        assert population_result['instrument_id'] is not None
        assert population_result['instrument_id'] > 0
        
        # Verify real database record exists
        instruments_dao = InstrumentsDAO(unified_populator.environment)
        created_instrument = await instruments_dao.get_instrument(population_result['instrument_id'])
        
        assert created_instrument is not None
        assert created_instrument['symbol'] == symbol
        assert created_instrument['name'] is not None
        assert len(created_instrument['name'].strip()) > 0
        
        # Verify real cross-references were created
        xrefs_dao = InstrumentXrefsDAO(unified_populator.environment)
        xrefs = await xrefs_dao.list_xrefs_for_instrument(population_result['instrument_id'])
        
        assert len(xrefs) > 0, "No cross-references created"
        
        # Verify vendor metadata was stored
        vendor_metadata = await unified_populator.get_vendor_metadata(population_result['instrument_id'])
        assert len(vendor_metadata) > 0, "No vendor metadata stored"

    @pytest.mark.asyncio
    async def test_duplicate_symbol_handling_real_constraints(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test duplicate symbol handling with real database constraints"""
        symbol = "NFLX"
        
        # First population should succeed
        first_result = await unified_populator.populate_symbol(symbol)
        assert first_result['success'] is True
        first_instrument_id = first_result['instrument_id']
        
        # Second population of same symbol should handle duplicate gracefully
        second_result = await unified_populator.populate_symbol(symbol)
        
        # Should either:
        # 1. Return existing instrument (update case)
        # 2. Handle duplicate constraint gracefully
        if second_result['success']:
            # Update case - same instrument ID returned
            assert second_result['instrument_id'] == first_instrument_id
        else:
            # Duplicate handling case
            assert 'duplicate' in second_result['error_message'].lower() or \
                   'already exists' in second_result['error_message'].lower()

    # Test real referential integrity

    @pytest.mark.asyncio
    async def test_referential_integrity_with_real_foreign_keys(
        self, 
        unified_populator: UnifiedInstrumentPopulator,
        clean_database: Environment
    ):
        """Test referential integrity with real foreign key constraints"""
        symbol = "META"
        
        # Populate instrument with real data
        population_result = await unified_populator.populate_symbol(symbol)
        assert population_result['success'] is True
        instrument_id = population_result['instrument_id']
        
        # Verify referential integrity by attempting to delete instrument
        # This should fail if cross-references exist (due to foreign key constraints)
        async with clean_database.get_connection() as conn:
            # Check if cross-references exist
            xrefs_count = await conn.fetchval(
                "SELECT COUNT(*) FROM test_instrument_xrefs WHERE instrument_id = $1",
                instrument_id
            )
            
            if xrefs_count > 0:
                # Should fail due to foreign key constraint
                with pytest.raises(Exception):  # Foreign key violation expected
                    await conn.execute(
                        "DELETE FROM test_instruments WHERE id = $1",
                        instrument_id
                    )
            
            # Cleanup properly by deleting cross-references first
            await conn.execute(
                "DELETE FROM test_instrument_xrefs WHERE instrument_id = $1",
                instrument_id
            )
            
            # Now instrument deletion should succeed
            await conn.execute(
                "DELETE FROM test_instruments WHERE id = $1",
                instrument_id
            )

    # Test real performance and scalability

    @pytest.mark.asyncio
    async def test_batch_population_performance_real_apis(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test batch population performance with real API calls"""
        import time
        
        # Use real symbols from major indices
        test_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA"]
        
        # Measure real API and database performance
        start_time = time.time()
        results = await unified_populator.populate_symbols_batch(test_symbols)
        end_time = time.time()
        
        # Verify performance and results
        successful_populations = [r for r in results if r['success']]
        failed_populations = [r for r in results if not r['success']]
        
        # Performance assertions
        assert end_time - start_time < 120.0, "Batch population took too long"  # 2 minutes max
        assert len(successful_populations) >= len(test_symbols) * 0.7, "Too many failures"  # 70% success rate minimum
        
        # Document any real API failures
        if failed_populations:
            print(f"Real API failures during batch population: {failed_populations}")

    # Test real data quality validation

    @pytest.mark.asyncio
    async def test_data_quality_validation_real_metrics(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test data quality validation with real vendor data"""
        # Populate several real instruments
        test_symbols = ["AAPL", "MSFT", "GOOGL"]
        
        for symbol in test_symbols:
            await unified_populator.populate_symbol(symbol)
        
        # Generate real data quality report
        quality_report = await unified_populator.generate_data_quality_report()
        
        # Verify real quality metrics
        assert 'total_instruments' in quality_report
        assert quality_report['total_instruments'] >= len(test_symbols)
        
        assert 'vendor_coverage' in quality_report
        assert 'data_completeness' in quality_report
        assert 'consistency_metrics' in quality_report
        
        # Verify vendor coverage metrics
        vendor_coverage = quality_report['vendor_coverage']
        assert isinstance(vendor_coverage, dict)
        
        for vendor in ['polygon', 'eodhd', 'tiingo']:
            if vendor in vendor_coverage:
                assert isinstance(vendor_coverage[vendor], (int, float))
                assert vendor_coverage[vendor] >= 0

    # Test real error handling and resilience

    @pytest.mark.asyncio
    async def test_vendor_api_failure_resilience(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test resilience to real vendor API failures"""
        # Test with invalid symbol that should cause API errors
        invalid_symbol = "INVALID_SYMBOL_12345"
        
        # Should handle real API errors gracefully
        result = await unified_populator.populate_symbol(invalid_symbol)
        
        # Verify graceful error handling
        assert result['success'] is False
        assert 'error_message' in result
        assert len(result['error_message']) > 0
        
        # Should not crash the system
        assert result is not None

    @pytest.mark.asyncio
    async def test_partial_vendor_data_handling(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test handling when only some vendors have data for a symbol"""
        # Use a symbol that might only be available from some vendors
        # (e.g., a newer listing or less common instrument)
        symbol = "COIN"  # Coinbase - might not be in all vendor APIs
        
        # Attempt population
        result = await unified_populator.populate_symbol(symbol)
        
        # Should succeed even with partial vendor data
        if result['success']:
            # Verify instrument was created with available data
            instruments_dao = InstrumentsDAO(unified_populator.environment)
            instrument = await instruments_dao.get_instrument(result['instrument_id'])
            
            assert instrument is not None
            assert instrument['symbol'] == symbol
            
            # May have fewer vendor sources, but should still work
            vendor_metadata = await unified_populator.get_vendor_metadata(result['instrument_id'])
            # Should have at least one vendor source
            assert len(vendor_metadata) >= 1
        else:
            # If no vendors have this symbol, failure is acceptable
            assert 'no vendor data' in result['error_message'].lower() or \
                   'not found' in result['error_message'].lower()

    # Test real data update and synchronization

    @pytest.mark.asyncio
    async def test_data_synchronization_real_updates(
        self, 
        unified_populator: UnifiedInstrumentPopulator
    ):
        """Test data synchronization with real vendor data updates"""
        symbol = "AAPL"
        
        # Initial population
        initial_result = await unified_populator.populate_symbol(symbol)
        assert initial_result['success'] is True
        instrument_id = initial_result['instrument_id']
        
        # Get initial metadata
        initial_metadata = await unified_populator.get_vendor_metadata(instrument_id)
        initial_update_time = datetime.now()
        
        # Wait a moment to ensure timestamp difference
        await asyncio.sleep(2)
        
        # Re-populate (should update existing data)
        update_result = await unified_populator.populate_symbol(symbol, force_update=True)
        
        # Verify update handling
        if update_result['success']:
            # Should return same instrument ID
            assert update_result['instrument_id'] == instrument_id
            
            # Metadata should be updated
            updated_metadata = await unified_populator.get_vendor_metadata(instrument_id)
            
            # Should have same or more vendor sources
            assert len(updated_metadata) >= len(initial_metadata)
            
            # At least one vendor should have newer timestamp
            any_updated = any(
                meta['last_updated'] > initial_update_time 
                for meta in updated_metadata
            )
            assert any_updated, "No metadata was updated during synchronization"