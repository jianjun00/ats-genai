"""
Core DAO tests using real database objects and fail-fast validation.

This replaces test_dao_base.py with real database integration testing.
All mocks and synthetic connections are eliminated for authentic testing.
"""

import pytest
from datetime import date, datetime

from domains.trading.repositories.universe_dao import UniverseDAO
from domains.trading.repositories.universe_membership_dao import UniverseMembershipDAO
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from shared.utils.environment import Environment, EnvironmentType


@pytest.fixture
async def test_environment():
    """Real test environment with actual database connection."""
    return Environment(
        env_type=EnvironmentType.DEV,
        db_url="postgresql://postgres:dev_password@localhost:5432/dev_db"
    )


@pytest.fixture
async def universe_dao(test_environment):
    """Real UniverseDAO instance."""
    return UniverseDAO(test_environment)


@pytest.fixture
async def universe_membership_dao(test_environment):
    """Real UniverseMembershipDAO instance."""
    return UniverseMembershipDAO(test_environment)


@pytest.fixture
async def instruments_dao(test_environment):
    """Real InstrumentsDAO for test instrument creation."""
    return InstrumentsDAO(test_environment)


@pytest.fixture
async def test_universe(universe_dao):
    """Create a real test universe for membership tests."""
    universe_name = f"TEST_UNIVERSE_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    universe_id = await universe_dao.create_universe(
        name=universe_name,
        description="Test universe for DAO integration testing"
    )
    yield universe_id
    
    # Cleanup - remove test universe
    # Note: This should cascade delete memberships
    await universe_dao.delete_universe(universe_id)


@pytest.fixture
async def test_instrument(instruments_dao):
    """Create a real test instrument for membership tests."""
    symbol = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    instrument_id = await instruments_dao.create_instrument(
        symbol=symbol,
        name=f"Test Instrument {symbol}",
        exchange="TEST_EXCHANGE"
    )
    yield {'id': instrument_id, 'symbol': symbol}
    
    # Cleanup - remove test instrument
    await instruments_dao.delete_instrument(instrument_id)


class TestUniverseDAORealObjects:
    """Real database integration tests for UniverseDAO."""

    async def test_create_universe_success(self, universe_dao):
        """Test successful universe creation with real database."""
        universe_name = f"CREATE_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        universe_id = await universe_dao.create_universe(
            name=universe_name,
            description="Integration test universe"
        )
        
        # Verify universe was actually created
        assert universe_id > 0
        created_universe = await universe_dao.get_universe(universe_id)
        assert created_universe is not None
        assert created_universe['name'] == universe_name
        assert created_universe['description'] == "Integration test universe"
        
        # Cleanup
        await universe_dao.delete_universe(universe_id)

    async def test_get_universe_by_name_found(self, universe_dao, test_universe):
        """Test retrieving existing universe by name."""
        # Get the test universe details first
        universe_details = await universe_dao.get_universe(test_universe)
        universe_name = universe_details['name']
        
        # Test get_universe_by_name
        result = await universe_dao.get_universe_by_name(universe_name)
        
        assert result is not None
        assert result['id'] == test_universe
        assert result['name'] == universe_name

    async def test_get_universe_by_name_not_found(self, universe_dao):
        """Test retrieving non-existent universe returns None."""
        nonexistent_name = f"NONEXISTENT_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        result = await universe_dao.get_universe_by_name(nonexistent_name)
        
        assert result is None

    async def test_list_universes_includes_created(self, universe_dao, test_universe):
        """Test that list_universes includes our test universe."""
        universes = await universe_dao.list_universes()
        
        universe_ids = [u['id'] for u in universes]
        assert test_universe in universe_ids

    async def test_update_universe_success(self, universe_dao, test_universe):
        """Test successful universe update with real database."""
        new_name = f"UPDATED_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        new_description = "Updated description"
        
        updated = await universe_dao.update_universe(
            test_universe,
            name=new_name,
            description=new_description
        )
        
        assert updated is True
        
        # Verify update was persisted
        updated_universe = await universe_dao.get_universe(test_universe)
        assert updated_universe['name'] == new_name
        assert updated_universe['description'] == new_description

    async def test_update_universe_no_fields(self, universe_dao, test_universe):
        """Test update with no fields returns False."""
        updated = await universe_dao.update_universe(test_universe)
        
        assert updated is False

    async def test_universe_name_uniqueness_constraint(self, universe_dao, test_universe):
        """Test that universe names must be unique (database constraint)."""
        universe_details = await universe_dao.get_universe(test_universe)
        existing_name = universe_details['name']
        
        # Attempting to create universe with same name should fail
        # This tests actual database constraint validation
        with pytest.raises(Exception):  # Database constraint violation
            await universe_dao.create_universe(
                name=existing_name,
                description="Duplicate name test"
            )


class TestUniverseMembershipDAORealObjects:
    """Real database integration tests for UniverseMembershipDAO."""

    async def test_add_membership_success(self, universe_membership_dao, test_universe, test_instrument):
        """Test successful membership addition with real database."""
        success = await universe_membership_dao.add_membership(
            universe_id=test_universe,
            instrument_id=test_instrument['id']
        )
        
        assert success is True
        
        # Verify membership was created
        memberships = await universe_membership_dao.get_memberships_by_universe(test_universe)
        instrument_ids = [m.get('instrument_id') for m in memberships]
        assert test_instrument['id'] in instrument_ids

    async def test_add_membership_full_success(self, universe_membership_dao, test_universe, test_instrument):
        """Test full membership addition with start/end dates."""
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        await universe_membership_dao.add_membership_full(
            universe_id=test_universe,
            symbol=test_instrument['symbol'],
            start_at=start_date,
            end_at=end_date
        )
        
        # Verify membership with dates
        memberships = await universe_membership_dao.get_memberships_by_universe(test_universe)
        # Find our membership
        our_membership = next(
            (m for m in memberships if m.get('symbol') == test_instrument['symbol']),
            None
        )
        assert our_membership is not None
        assert our_membership.get('start_at') == start_date
        assert our_membership.get('end_at') == end_date

    async def test_get_active_memberships_by_date(self, universe_membership_dao, test_universe, test_instrument):
        """Test getting active memberships for specific date."""
        # Add membership with date range
        start_date = date(2025, 6, 1)
        end_date = date(2025, 8, 31)
        
        await universe_membership_dao.add_membership_full(
            universe_id=test_universe,
            symbol=test_instrument['symbol'],
            start_at=start_date,
            end_at=end_date
        )
        
        # Test active on date within range
        active_date = date(2025, 7, 15)
        active_memberships = await universe_membership_dao.get_active_memberships(
            test_universe, active_date
        )
        
        symbols = [m.get('symbol') for m in active_memberships]
        assert test_instrument['symbol'] in symbols
        
        # Test not active on date outside range
        inactive_date = date(2025, 9, 15)
        inactive_memberships = await universe_membership_dao.get_active_memberships(
            test_universe, inactive_date
        )
        
        inactive_symbols = [m.get('symbol') for m in inactive_memberships]
        assert test_instrument['symbol'] not in inactive_symbols

    async def test_remove_membership_success(self, universe_membership_dao, test_universe, test_instrument):
        """Test successful membership removal."""
        # First add membership
        await universe_membership_dao.add_membership(
            universe_id=test_universe,
            instrument_id=test_instrument['id']
        )
        
        # Verify it exists
        memberships_before = await universe_membership_dao.get_memberships_by_universe(test_universe)
        symbols_before = [m.get('symbol') for m in memberships_before]
        assert test_instrument['symbol'] in symbols_before
        
        # Remove membership
        removed = await universe_membership_dao.remove_membership(
            universe_id=test_universe,
            symbol=test_instrument['symbol'],
            start_at=None
        )
        
        assert removed is True
        
        # Verify it's removed
        memberships_after = await universe_membership_dao.get_memberships_by_universe(test_universe)
        symbols_after = [m.get('symbol') for m in memberships_after]
        assert test_instrument['symbol'] not in symbols_after

    async def test_update_membership_end_date(self, universe_membership_dao, test_universe, test_instrument):
        """Test updating membership end date."""
        # Add membership without end date
        await universe_membership_dao.add_membership_full(
            universe_id=test_universe,
            symbol=test_instrument['symbol'],
            start_at=date(2025, 1, 1),
            end_at=None
        )
        
        # Update end date
        new_end_date = date(2025, 6, 30)
        await universe_membership_dao.update_membership_end(
            universe_id=test_universe,
            instrument_id=test_instrument['id'],
            end_at=new_end_date
        )
        
        # Verify end date was updated
        memberships = await universe_membership_dao.get_memberships_by_universe(test_universe)
        our_membership = next(
            (m for m in memberships if m.get('symbol') == test_instrument['symbol']),
            None
        )
        assert our_membership is not None
        assert our_membership.get('end_at') == new_end_date

    async def test_get_memberships_by_instrument(self, universe_membership_dao, test_universe, test_instrument):
        """Test getting all universe memberships for an instrument."""
        # Add membership
        await universe_membership_dao.add_membership(
            universe_id=test_universe,
            instrument_id=test_instrument['id']
        )
        
        # Get memberships by instrument
        memberships = await universe_membership_dao.get_memberships_by_instrument(
            test_instrument['id']
        )
        
        universe_ids = [m.get('universe_id') for m in memberships]
        assert test_universe in universe_ids

    async def test_duplicate_membership_constraint(self, universe_membership_dao, test_universe, test_instrument):
        """Test that duplicate memberships are handled by database constraints."""
        # Add first membership
        await universe_membership_dao.add_membership(
            universe_id=test_universe,
            instrument_id=test_instrument['id']
        )
        
        # Attempting to add duplicate should fail with database constraint
        # This tests actual database constraint validation
        with pytest.raises(Exception):  # Database constraint violation
            await universe_membership_dao.add_membership(
                universe_id=test_universe,
                instrument_id=test_instrument['id']
            )


class TestDAODatabaseConstraintValidation:
    """Test that DAOs properly validate database constraints."""

    async def test_foreign_key_constraint_universe_membership(self, universe_membership_dao, test_instrument):
        """Test foreign key constraint for nonexistent universe."""
        nonexistent_universe_id = 999999
        
        # Should fail with foreign key constraint violation
        with pytest.raises(Exception):  # Foreign key constraint
            await universe_membership_dao.add_membership(
                universe_id=nonexistent_universe_id,
                instrument_id=test_instrument['id']
            )

    async def test_foreign_key_constraint_instrument_membership(self, universe_membership_dao, test_universe):
        """Test foreign key constraint for nonexistent instrument."""
        nonexistent_instrument_id = 999999
        
        # Should fail with foreign key constraint violation
        with pytest.raises(Exception):  # Foreign key constraint
            await universe_membership_dao.add_membership(
                universe_id=test_universe,
                instrument_id=nonexistent_instrument_id
            )

    async def test_null_constraint_universe_name(self, universe_dao):
        """Test null constraint on required universe name."""
        # Should fail with null constraint violation
        with pytest.raises(Exception):  # Null constraint violation
            await universe_dao.create_universe(
                name=None,
                description="Test description"
            )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])