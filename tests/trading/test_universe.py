import pytest
from datetime import date
from trading.universe import Universe


def test_universe_init():
    """Test Universe initialization."""
    # Test basic initialization
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    assert universe.current_date == date(2023, 1, 1)
    assert universe.instrument_ids == [1, 2, 3]
    
    # Test with empty instrument list
    universe_empty = Universe(current_date=date(2023, 1, 1), instrument_ids=[])
    assert universe_empty.instrument_ids == []
    
    # Test with set (should convert to list and remove duplicates)
    universe_set = Universe(current_date=date(2023, 1, 1), instrument_ids={1, 2, 3, 2, 1})
    assert len(universe_set.instrument_ids) == 3
    assert all(id_ in universe_set.instrument_ids for id_ in [1, 2, 3])
    
    # Test with duplicates in list (should remove duplicates while preserving order)
    universe_dups = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3, 2, 1, 4])
    assert universe_dups.instrument_ids == [1, 2, 3, 4]


def test_advance_to():
    """Test advancing universe to new date."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    # Advance date only
    new_date = date(2023, 1, 2)
    universe.advanceTo(new_date)
    assert universe.current_date == new_date
    assert universe.instrument_ids == [1, 2, 3]  # Should remain unchanged
    
    # Advance date and instruments
    newer_date = date(2023, 1, 3)
    new_instruments = [4, 5, 6]
    universe.advanceTo(newer_date, new_instruments)
    assert universe.current_date == newer_date
    assert universe.instrument_ids == new_instruments
    
    # Advance with duplicate instruments (should remove duplicates)
    universe.advanceTo(date(2023, 1, 4), [1, 2, 2, 3, 1])
    assert universe.instrument_ids == [1, 2, 3]


def test_update_date():
    """Test updating universe date (backward compatibility)."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    # Update date only
    new_date = date(2023, 1, 2)
    universe.update_date(new_date)
    assert universe.current_date == new_date
    assert universe.instrument_ids == [1, 2, 3]  # Should remain unchanged
    
    # Update date and instruments
    newer_date = date(2023, 1, 3)
    new_instruments = [4, 5, 6]
    universe.update_date(newer_date, new_instruments)
    assert universe.current_date == newer_date
    assert universe.instrument_ids == new_instruments
    
    # Update with duplicate instruments (should remove duplicates)
    universe.update_date(date(2023, 1, 4), [1, 2, 2, 3, 1])
    assert universe.instrument_ids == [1, 2, 3]


def test_add_instrument():
    """Test adding instruments to universe."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2])
    
    # Add new instrument
    universe.add_instrument(3)
    assert 3 in universe.instrument_ids
    assert universe.instrument_ids == [1, 2, 3]
    
    # Add existing instrument (should not duplicate)
    universe.add_instrument(2)
    assert universe.instrument_ids == [1, 2, 3]  # No change


def test_remove_instrument():
    """Test removing instruments from universe."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    # Remove existing instrument
    universe.remove_instrument(2)
    assert 2 not in universe.instrument_ids
    assert universe.instrument_ids == [1, 3]
    
    # Remove non-existing instrument (should not error)
    universe.remove_instrument(99)
    assert universe.instrument_ids == [1, 3]  # No change


def test_has_instrument():
    """Test checking if instrument is in universe."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    assert universe.has_instrument(1) is True
    assert universe.has_instrument(2) is True
    assert universe.has_instrument(99) is False


def test_get_instrument_count():
    """Test getting instrument count."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    assert universe.get_instrument_count() == 3
    
    universe_empty = Universe(current_date=date(2023, 1, 1), instrument_ids=[])
    assert universe_empty.get_instrument_count() == 0


def test_copy():
    """Test copying universe."""
    original = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    copy = original.copy()
    
    # Should be equal but not the same object
    assert copy.current_date == original.current_date
    assert copy.instrument_ids == original.instrument_ids
    assert copy is not original
    assert copy.instrument_ids is not original.instrument_ids
    
    # Modifying copy should not affect original
    copy.add_instrument(4)
    assert 4 not in original.instrument_ids
    assert 4 in copy.instrument_ids


def test_len():
    """Test __len__ method."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    assert len(universe) == 3
    
    universe_empty = Universe(current_date=date(2023, 1, 1), instrument_ids=[])
    assert len(universe_empty) == 0


def test_contains():
    """Test __contains__ method."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    assert 1 in universe
    assert 2 in universe
    assert 99 not in universe


def test_iter():
    """Test __iter__ method."""
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=[1, 2, 3])
    
    instruments = list(universe)
    assert instruments == [1, 2, 3]
    
    # Test with for loop
    collected = []
    for instrument_id in universe:
        collected.append(instrument_id)
    assert collected == [1, 2, 3]


def test_universe_edge_cases():
    """Test edge cases and error conditions."""
    # Test with None instrument_ids (should handle gracefully)
    universe = Universe(current_date=date(2023, 1, 1), instrument_ids=None)
    assert universe.instrument_ids == []
    
    # Test with single instrument
    universe_single = Universe(current_date=date(2023, 1, 1), instrument_ids=[42])
    assert len(universe_single) == 1
    assert 42 in universe_single
    
    # Test operations on empty universe
    universe_empty = Universe(current_date=date(2023, 1, 1), instrument_ids=[])
    universe_empty.remove_instrument(1)  # Should not error
    assert len(universe_empty) == 0
    
    universe_empty.add_instrument(1)
    assert len(universe_empty) == 1
    assert 1 in universe_empty
