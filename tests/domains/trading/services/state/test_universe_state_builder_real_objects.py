"""
Real Objects Test for UniverseStateIntervalBuilder

This replaces mock-heavy testing with real database and market data integration.
Tests use actual database connections, real market data, and end-to-end universe state building.

BEFORE: Mock objects masked database integration and data processing issues  
AFTER: Real objects reveal actual data quality issues, constraint violations, and performance problems
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, List

from shared.utils.environment import Environment, EnvironmentType
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.market_data.managers.unified_market_data_manager import UnifiedMarketDataManager
from domains.instruments.repositories.instruments_dao import InstrumentsDAO


class TestUniverseStateIntervalBuilderRealObjects:
    """Real database and market data integration tests for UniverseStateIntervalBuilder"""

    @pytest.fixture(scope="session")
    async def test_environment(self) -> Environment:
        """Real test environment with actual database connection"""
        return Environment(
            env_type=EnvironmentType.TEST,
            db_url="postgresql://test:test@localhost/test_universe_state_db"
        )

    @pytest.fixture
    async def clean_database(self, test_environment: Environment) -> AsyncGenerator[Environment, None]:
        """Clean database with real universe state schema"""
        # Set up real database schema for universe state testing
        async with test_environment.get_connection() as conn:
            # Create real instruments table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_instruments (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL UNIQUE,
                    name VARCHAR(255) NOT NULL,
                    exchange VARCHAR(50) NOT NULL,
                    type VARCHAR(50) NOT NULL DEFAULT 'stock',
                    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
                    sector VARCHAR(100),
                    industry VARCHAR(100),
                    market_cap BIGINT,
                    is_active BOOLEAN DEFAULT TRUE,
                    list_date DATE,
                    delist_date DATE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Create real universe state intervals table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_universe_state_intervals (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    universe_id VARCHAR(100) NOT NULL,
                    interval_start TIMESTAMP NOT NULL,
                    interval_end TIMESTAMP NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    open_price DECIMAL(15,6),
                    high_price DECIMAL(15,6),
                    low_price DECIMAL(15,6),
                    close_price DECIMAL(15,6),
                    volume BIGINT,
                    vwap DECIMAL(15,6),
                    market_cap BIGINT,
                    sector VARCHAR(100),
                    technical_indicators JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(symbol, universe_id, interval_start, timeframe)
                )
            """)

            # Create real universe membership table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_universe_membership (
                    id SERIAL PRIMARY KEY,
                    universe_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE,
                    rank_value INTEGER,
                    weight DECIMAL(8,6),
                    reason VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(universe_id, symbol, start_date)
                )
            """)

            # Clean up before test
            await conn.execute("""
                TRUNCATE TABLE test_universe_membership, test_universe_state_intervals, test_instruments 
                RESTART IDENTITY CASCADE
            """)

            # Insert real test instruments
            await conn.executemany("""
                INSERT INTO test_instruments (symbol, name, exchange, sector, market_cap, is_active)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, [
                ("AAPL", "Apple Inc.", "NASDAQ", "Technology", 2800000000000, True),
                ("GOOGL", "Alphabet Inc.", "NASDAQ", "Technology", 1600000000000, True),
                ("MSFT", "Microsoft Corp.", "NASDAQ", "Technology", 2400000000000, True),
                ("TSLA", "Tesla Inc.", "NASDAQ", "Consumer Discretionary", 800000000000, True),
                ("AMZN", "Amazon.com Inc.", "NASDAQ", "Consumer Discretionary", 1400000000000, True),
                ("NVDA", "NVIDIA Corporation", "NASDAQ", "Technology", 1000000000000, True),
                ("META", "Meta Platforms Inc.", "NASDAQ", "Technology", 800000000000, True),
                ("NFLX", "Netflix Inc.", "NASDAQ", "Communication Services", 200000000000, True),
                ("SMALL_CAP", "Small Cap Corp.", "NYSE", "Technology", 50000000, True),  # Below min market cap
            ])
        
        yield test_environment
        
        # Clean up after test
        async with test_environment.get_connection() as conn:
            await conn.execute("""
                TRUNCATE TABLE test_universe_membership, test_universe_state_intervals, test_instruments 
                RESTART IDENTITY CASCADE
            """)

    @pytest.fixture
    async def real_market_data_manager(self, test_environment: Environment) -> UnifiedMarketDataManager:
        """Real market data manager with test data"""
        manager = UnifiedMarketDataManager(
            environment=test_environment,
            data_path="/tmp/test_market_data"
        )
        await manager.initialize()
        return manager

    @pytest.fixture
    async def real_universe_state_manager(
        self, 
        clean_database: Environment,
        real_market_data_manager: UnifiedMarketDataManager
    ) -> UniverseStateManager:
        """Real universe state manager with database integration"""
        return UniverseStateManager(
            environment=clean_database,
            market_data_manager=real_market_data_manager
        )

    @pytest.fixture
    async def universe_builder(
        self,
        clean_database: Environment,
        real_universe_state_manager: UniverseStateManager
    ) -> UniverseStateIntervalBuilder:
        """Real universe state builder with actual dependencies"""
        return UniverseStateIntervalBuilder(
            env=clean_database,
            base_duration='5m',
            target_durations='5m,15m,60m',
            universe_state_manager=real_universe_state_manager
        )

    # Test real initialization with database constraints

    @pytest.mark.asyncio
    async def test_initialization_with_real_environment(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test initialization with real database environment"""
        # Verify real environment connection
        assert universe_builder.env == clean_database
        assert universe_builder.base_duration == '5m'
        assert universe_builder.target_durations == ['5m', '15m', '60m']
        
        # Verify real database connectivity
        async with universe_builder.env.get_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1

        # Verify real configuration values
        assert universe_builder.min_market_cap == 100_000_000  # 100M minimum
        assert universe_builder.universe_state_manager is not None

    # Test real universe filtering with database queries

    @pytest.mark.asyncio
    async def test_filter_universe_by_market_cap_real_data(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test universe filtering with real database market cap constraints"""
        # Get real universe data from database
        async with clean_database.get_connection() as conn:
            universe_data = await conn.fetch("""
                SELECT symbol, name, exchange, sector, market_cap, is_active
                FROM test_instruments
                WHERE is_active = TRUE
                ORDER BY market_cap DESC
            """)
        
        # Convert to DataFrame for processing
        universe_df = pd.DataFrame([dict(row) for row in universe_data])
        
        # Apply real market cap filtering
        filtered_universe = universe_builder.filter_universe_by_market_cap(universe_df)
        
        # Verify real filtering results
        assert len(filtered_universe) > 0
        assert all(row['market_cap'] >= universe_builder.min_market_cap for _, row in filtered_universe.iterrows())
        
        # Verify small cap stock is filtered out
        small_cap_symbols = filtered_universe[filtered_universe['market_cap'] < 100_000_000]['symbol'].tolist()
        assert len(small_cap_symbols) == 0
        
        # Verify major stocks remain
        major_symbols = filtered_universe['symbol'].tolist()
        assert 'AAPL' in major_symbols
        assert 'GOOGL' in major_symbols
        assert 'MSFT' in major_symbols

    @pytest.mark.asyncio
    async def test_filter_universe_by_sector_real_constraints(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test sector filtering with real database constraints"""
        # Get real sector data
        async with clean_database.get_connection() as conn:
            universe_data = await conn.fetch("""
                SELECT symbol, name, sector, market_cap
                FROM test_instruments
                WHERE is_active = TRUE AND market_cap >= $1
            """, universe_builder.min_market_cap)
        
        universe_df = pd.DataFrame([dict(row) for row in universe_data])
        
        # Test technology sector filtering
        tech_filter = {'sector': 'Technology'}
        tech_universe = universe_builder.filter_universe_by_criteria(universe_df, tech_filter)
        
        # Verify real sector filtering
        assert len(tech_universe) > 0
        assert all(row['sector'] == 'Technology' for _, row in tech_universe.iterrows())
        
        # Verify expected tech stocks
        tech_symbols = tech_universe['symbol'].tolist()
        assert 'AAPL' in tech_symbols
        assert 'GOOGL' in tech_symbols
        assert 'MSFT' in tech_symbols
        assert 'NVDA' in tech_symbols

    # Test real universe state building with market data

    @pytest.mark.asyncio
    async def test_build_universe_state_with_real_market_data(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test universe state building with real market data integration"""
        # Define real time interval
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=1)
        
        # Use real symbols from database
        test_symbols = ['AAPL', 'GOOGL', 'MSFT']
        
        # Build real universe state
        universe_states = await universe_builder.build_universe_state_intervals(
            symbols=test_symbols,
            start_time=start_time,
            end_time=end_time,
            universe_id="test_universe_001"
        )
        
        # Verify real universe state results
        assert len(universe_states) > 0
        
        for state in universe_states:
            # Verify real data structure
            assert 'symbol' in state
            assert 'universe_id' in state
            assert 'interval_start' in state
            assert 'interval_end' in state
            assert 'timeframe' in state
            
            # Verify real symbol mapping
            assert state['symbol'] in test_symbols
            assert state['universe_id'] == "test_universe_001"
            
            # Verify real time constraints
            assert start_time <= state['interval_start'] <= end_time
            assert start_time <= state['interval_end'] <= end_time

    @pytest.mark.asyncio
    async def test_multi_timeframe_universe_state_building(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test multi-timeframe universe state building with real aggregation"""
        # Define real time range
        end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(hours=3)
        
        test_symbols = ['AAPL', 'MSFT']
        
        # Build universe states for all configured timeframes
        all_states = {}
        
        for timeframe in universe_builder.target_durations:
            states = await universe_builder.build_universe_state_intervals(
                symbols=test_symbols,
                start_time=start_time,
                end_time=end_time,
                universe_id="test_multi_timeframe",
                timeframe=timeframe
            )
            all_states[timeframe] = states
        
        # Verify multi-timeframe results
        assert len(all_states) == 3  # 5m, 15m, 60m
        
        # Verify timeframe-specific characteristics
        for timeframe, states in all_states.items():
            assert len(states) > 0
            
            # All states should have correct timeframe
            for state in states:
                assert state['timeframe'] == timeframe
            
            # Higher timeframes should have fewer intervals
            if timeframe == '60m':
                assert len(states) <= len(all_states.get('15m', []))
            if timeframe == '15m':
                assert len(states) <= len(all_states.get('5m', []))

    # Test real database persistence

    @pytest.mark.asyncio
    async def test_persist_universe_state_real_database(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test universe state persistence to real database"""
        # Create real universe state data
        test_state = {
            'symbol': 'AAPL',
            'universe_id': 'test_persist_001',
            'interval_start': datetime.now() - timedelta(minutes=5),
            'interval_end': datetime.now(),
            'timeframe': '5m',
            'open_price': 150.00,
            'high_price': 151.50,
            'low_price': 149.75,
            'close_price': 151.25,
            'volume': 1000000,
            'vwap': 150.75,
            'market_cap': 2800000000000,
            'sector': 'Technology',
            'technical_indicators': {
                'rsi': 65.5,
                'sma_20': 150.25,
                'volume_sma': 950000
            }
        }
        
        # Persist to real database
        await universe_builder.persist_universe_state(test_state)
        
        # Verify real database persistence
        async with clean_database.get_connection() as conn:
            persisted_state = await conn.fetchrow("""
                SELECT * FROM test_universe_state_intervals
                WHERE symbol = $1 AND universe_id = $2
            """, test_state['symbol'], test_state['universe_id'])
        
        # Verify real database record
        assert persisted_state is not None
        assert persisted_state['symbol'] == test_state['symbol']
        assert persisted_state['universe_id'] == test_state['universe_id']
        assert persisted_state['timeframe'] == test_state['timeframe']
        assert float(persisted_state['open_price']) == test_state['open_price']
        assert float(persisted_state['close_price']) == test_state['close_price']
        assert persisted_state['volume'] == test_state['volume']
        
        # Verify JSON data persistence
        assert persisted_state['technical_indicators'] is not None
        assert persisted_state['technical_indicators']['rsi'] == 65.5

    @pytest.mark.asyncio
    async def test_universe_state_constraints_real_database(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test database constraints for universe state data"""
        # Create initial state
        initial_state = {
            'symbol': 'GOOGL',
            'universe_id': 'test_constraints_001',
            'interval_start': datetime.now() - timedelta(minutes=5),
            'interval_end': datetime.now(),
            'timeframe': '5m',
            'open_price': 2500.00,
            'close_price': 2505.00,
            'volume': 500000
        }
        
        # Persist initial state
        await universe_builder.persist_universe_state(initial_state)
        
        # Attempt to create duplicate (should fail due to unique constraint)
        duplicate_state = initial_state.copy()
        
        # Real database unique constraint should prevent duplicate
        with pytest.raises(Exception):  # Unique constraint violation expected
            await universe_builder.persist_universe_state(duplicate_state)

    # Test real performance and scalability

    @pytest.mark.asyncio
    async def test_large_universe_processing_performance(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test performance with large universe processing"""
        import time
        
        # Create larger test universe
        large_symbol_set = [
            'AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX'
        ]
        
        # Define real time range
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=1)
        
        # Measure real processing performance
        start_time_perf = time.time()
        universe_states = await universe_builder.build_universe_state_intervals(
            symbols=large_symbol_set,
            start_time=start_time,
            end_time=end_time,
            universe_id="test_performance_001"
        )
        end_time_perf = time.time()
        
        # Verify performance characteristics
        processing_time = end_time_perf - start_time_perf
        assert processing_time < 60.0, f"Processing took too long: {processing_time}s"  # Should complete within 1 minute
        
        # Verify results were generated
        assert len(universe_states) > 0
        assert len(universe_states) >= len(large_symbol_set)  # At least one state per symbol

    # Test real error handling and edge cases

    @pytest.mark.asyncio
    async def test_invalid_symbol_handling_real_constraints(
        self, 
        universe_builder: UniverseStateIntervalBuilder
    ):
        """Test handling of invalid symbols with real constraints"""
        # Use symbols not in database
        invalid_symbols = ['INVALID123', 'NOTREAL456']
        
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=30)
        
        # Should handle invalid symbols gracefully
        universe_states = await universe_builder.build_universe_state_intervals(
            symbols=invalid_symbols,
            start_time=start_time,
            end_time=end_time,
            universe_id="test_invalid_symbols"
        )
        
        # Should return empty results or handle gracefully
        # (Exact behavior depends on implementation - should not crash)
        assert isinstance(universe_states, list)

    @pytest.mark.asyncio
    async def test_time_range_validation_real_constraints(
        self, 
        universe_builder: UniverseStateIntervalBuilder
    ):
        """Test time range validation with real constraints"""
        # Test invalid time range (end before start)
        start_time = datetime.now()
        end_time = start_time - timedelta(hours=1)  # Invalid: end before start
        
        test_symbols = ['AAPL']
        
        # Should handle invalid time range appropriately
        with pytest.raises(ValueError):
            await universe_builder.build_universe_state_intervals(
                symbols=test_symbols,
                start_time=start_time,
                end_time=end_time,
                universe_id="test_invalid_time_range"
            )

    # Test real data quality validation

    @pytest.mark.asyncio
    async def test_universe_state_data_quality_validation(
        self, 
        universe_builder: UniverseStateIntervalBuilder,
        clean_database: Environment
    ):
        """Test data quality validation for universe states"""
        # Build universe state with real data
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=30)
        
        test_symbols = ['AAPL', 'GOOGL']
        
        universe_states = await universe_builder.build_universe_state_intervals(
            symbols=test_symbols,
            start_time=start_time,
            end_time=end_time,
            universe_id="test_data_quality"
        )
        
        # Validate real data quality
        for state in universe_states:
            # Price validation
            if state.get('open_price') is not None:
                assert state['open_price'] > 0, f"Invalid open price: {state['open_price']}"
            if state.get('close_price') is not None:
                assert state['close_price'] > 0, f"Invalid close price: {state['close_price']}"
            
            # Volume validation
            if state.get('volume') is not None:
                assert state['volume'] >= 0, f"Invalid volume: {state['volume']}"
            
            # Time validation
            assert state['interval_start'] <= state['interval_end'], "Invalid time range"
            
            # Symbol validation
            assert len(state['symbol']) > 0, "Empty symbol"
            assert state['symbol'] in test_symbols, f"Unexpected symbol: {state['symbol']}"