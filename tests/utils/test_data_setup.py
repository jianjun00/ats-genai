#!/usr/bin/env python3
"""
Shared Test Data Setup Utility

This utility provides common test data setup functions for integration tests
that require real database objects and test data. Following CLAUDE.md principles:
- Use real objects only (no mocks)
- Reusable test data setup patterns
- Environment-aware configuration

Usage:
    from tests.utils.test_data_setup import TestDataSetup
    
    setup = TestDataSetup(environment=environment, db_connection=conn)
    await setup.create_test_symbol_data(symbol='TSLA', instrument_id=999999)
    await setup.create_test_universe(universe_id=1, name='test_universe')
"""

import asyncio
from typing import Optional, List
from datetime import datetime
import asyncpg

from core.shared.data_handling.utils.environment import Environment


class TestDataSetup:
    """
    Shared utility for setting up test data in real database environments.
    
    This class encapsulates common test data setup patterns used across
    integration tests, promoting reusability and consistency.
    """
    
    def __init__(self, environment: Environment, db_connection: asyncpg.Connection):
        """
        Initialize test data setup utility.
        
        Args:
            environment: Real Environment instance (not mocked)
            db_connection: Active database connection
        """
        self.environment = environment
        self.conn = db_connection
        
        # Cache table names for efficiency
        self._table_names = {
            'vendors': environment.get_table_name('vendors'),
            'instruments': environment.get_table_name('instrument'),
            'xrefs': environment.get_table_name('instrument_xrefs'),
            'universe': environment.get_table_name('universe'),
            'universe_membership': environment.get_table_name('universe_membership'),
            'universe_state_interval': environment.get_table_name('universe_state_interval'),
        }
    
    async def create_ticker_vendor(self) -> None:
        """
        Create the standard 'ticker' vendor entry.
        Uses ON CONFLICT DO NOTHING for idempotency.
        """
        await self.conn.execute(f"""
            INSERT INTO {self._table_names['vendors']} (name) 
            VALUES ('ticker') 
            ON CONFLICT (name) DO NOTHING
        """)
    
    async def create_test_symbol_data(
        self, 
        symbol: str, 
        instrument_id: int = 999999
    ) -> int:
        """
        Create complete test data for a symbol including instrument and xref.
        
        Args:
            symbol: Symbol to create (e.g., 'TSLA', 'AAPL')
            instrument_id: Instrument ID to use (default: 999999)
            
        Returns:
            The instrument_id that was created
        """
        # Ensure ticker vendor exists
        await self.create_ticker_vendor()
        
        # Create instrument
        await self.conn.execute(f"""
            INSERT INTO {self._table_names['instruments']} (id, symbol) 
            VALUES ($1, $2) 
            ON CONFLICT (symbol) DO NOTHING
        """, instrument_id, symbol)
        
        # Create instrument cross-reference
        await self.conn.execute(f"""
            INSERT INTO {self._table_names['xrefs']} (instrument_id, symbol, vendor_id) 
            VALUES ($1, $2, (SELECT id FROM {self._table_names['vendors']} WHERE name = 'ticker'))
            ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING
        """, instrument_id, symbol)
        
        return instrument_id
    
    async def create_test_universe(
        self, 
        universe_id: int = 1, 
        name: str = 'test_universe'
    ) -> int:
        """
        Create a test universe.
        
        Args:
            universe_id: Universe ID to create
            name: Universe name
            
        Returns:
            The universe_id that was created
        """
        await self.conn.execute(f"""
            INSERT INTO {self._table_names['universe']} (id, name) 
            VALUES ($1, $2) 
            ON CONFLICT (id) DO NOTHING
        """, universe_id, name)
        
        return universe_id
    
    async def add_symbol_to_universe(
        self, 
        universe_id: int, 
        instrument_id: int
    ) -> None:
        """
        Add an instrument to a universe.
        
        Args:
            universe_id: Universe to add to
            instrument_id: Instrument to add
        """
        await self.conn.execute(f"""
            INSERT INTO {self._table_names['universe_membership']} (universe_id, instrument_id) 
            VALUES ($1, $2) 
            ON CONFLICT (universe_id, instrument_id, entered_at) DO NOTHING
        """, universe_id, instrument_id)
    
    async def create_complete_test_setup(
        self,
        symbols: List[str] = None,
        universe_id: int = 1,
        universe_name: str = 'test_universe',
        base_instrument_id: int = 999999
    ) -> dict:
        """
        Create a complete test setup with symbols, universe, and memberships.
        
        Args:
            symbols: List of symbols to create (default: ['TSLA'])
            universe_id: Universe ID to create
            universe_name: Universe name
            base_instrument_id: Starting instrument ID (increments for each symbol)
            
        Returns:
            Dict with created data details:
            {
                'universe_id': int,
                'symbols': {symbol: instrument_id, ...},
                'total_instruments': int
            }
        """
        if symbols is None:
            symbols = ['TSLA']
        
        # Create universe
        await self.create_test_universe(universe_id, universe_name)
        
        # Create symbols and add to universe
        symbol_to_instrument = {}
        for i, symbol in enumerate(symbols):
            instrument_id = base_instrument_id + i
            await self.create_test_symbol_data(symbol, instrument_id)
            await self.add_symbol_to_universe(universe_id, instrument_id)
            symbol_to_instrument[symbol] = instrument_id
        
        return {
            'universe_id': universe_id,
            'symbols': symbol_to_instrument,
            'total_instruments': len(symbols)
        }
    
    async def clean_test_data(self, symbols: List[str] = None) -> None:
        """
        Clean up test data for specified symbols.
        
        Args:
            symbols: List of symbols to clean up (default: ['TSLA'])
        """
        if symbols is None:
            symbols = ['TSLA']
        
        for symbol in symbols:
            # Delete in reverse dependency order
            await self.conn.execute(f"""
                DELETE FROM {self._table_names['universe_membership']} 
                WHERE instrument_id IN (
                    SELECT id FROM {self._table_names['instruments']} WHERE symbol = $1
                )
            """, symbol)
            
            await self.conn.execute(f"""
                DELETE FROM {self._table_names['xrefs']} 
                WHERE symbol = $1
            """, symbol)
            
            await self.conn.execute(f"""
                DELETE FROM {self._table_names['instruments']} 
                WHERE symbol = $1
            """, symbol)
    
    async def verify_test_data(self, symbols: List[str] = None) -> dict:
        """
        Verify that test data was created correctly.
        
        Args:
            symbols: List of symbols to verify (default: ['TSLA'])
            
        Returns:
            Dict with verification results:
            {
                'vendors_count': int,
                'instruments_count': int,
                'xrefs_count': int,
                'universe_memberships_count': int
            }
        """
        if symbols is None:
            symbols = ['TSLA']
        
        # Count vendors
        vendors_result = await self.conn.fetchrow(f"""
            SELECT COUNT(*) as count FROM {self._table_names['vendors']} WHERE name = 'ticker'
        """)
        
        # Count instruments for symbols
        instruments_result = await self.conn.fetchrow(f"""
            SELECT COUNT(*) as count FROM {self._table_names['instruments']} 
            WHERE symbol = ANY($1)
        """, symbols)
        
        # Count xrefs for symbols
        xrefs_result = await self.conn.fetchrow(f"""
            SELECT COUNT(*) as count FROM {self._table_names['xrefs']} 
            WHERE symbol = ANY($1)
        """, symbols)
        
        # Count universe memberships for symbols
        memberships_result = await self.conn.fetchrow(f"""
            SELECT COUNT(*) as count FROM {self._table_names['universe_membership']} 
            WHERE instrument_id IN (
                SELECT id FROM {self._table_names['instruments']} WHERE symbol = ANY($1)
            )
        """, symbols)
        
        return {
            'vendors_count': vendors_result['count'],
            'instruments_count': instruments_result['count'],
            'xrefs_count': xrefs_result['count'],
            'universe_memberships_count': memberships_result['count']
        }


# Convenience functions for common patterns
async def setup_single_symbol_test(
    environment: Environment,
    db_connection: asyncpg.Connection,
    symbol: str = 'TSLA',
    instrument_id: int = 999999,
    universe_id: int = 1
) -> dict:
    """
    Convenience function to set up a single symbol test environment.
    
    Returns:
        Dict with setup details including universe_id, instrument_id, symbol
    """
    setup = TestDataSetup(environment, db_connection)
    result = await setup.create_complete_test_setup(
        symbols=[symbol],
        universe_id=universe_id,
        base_instrument_id=instrument_id
    )
    
    return {
        'universe_id': result['universe_id'],
        'instrument_id': result['symbols'][symbol],
        'symbol': symbol,
        'setup_result': result
    }


async def setup_multi_symbol_test(
    environment: Environment,
    db_connection: asyncpg.Connection,
    symbols: List[str] = None,
    universe_id: int = 1,
    base_instrument_id: int = 999999
) -> dict:
    """
    Convenience function to set up a multi-symbol test environment.
    
    Returns:
        Dict with setup details including universe_id, symbols mapping, total_instruments
    """
    if symbols is None:
        symbols = ['AAPL', 'TSLA']
    
    setup = TestDataSetup(environment, db_connection)
    result = await setup.create_complete_test_setup(
        symbols=symbols,
        universe_id=universe_id,
        base_instrument_id=base_instrument_id
    )
    
    return result