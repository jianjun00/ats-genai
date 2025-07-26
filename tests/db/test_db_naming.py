import pytest
from db.test_db_manager import TestDatabaseManager

@pytest.mark.asyncio
async def test_unit_db_name_format_and_length():
    test_name = 'a' * 80  # purposely long
    dbm = TestDatabaseManager('unit', test_name=test_name)
    env = dbm.env
    prefix = env.get_table_name('sample').replace('sample', '').rstrip('_')
    static_part = f"{prefix}_db_"
    full_db_name = f"{prefix}{dbm.test_db_suffix}"
    # Should not exceed 63 chars
    assert len(full_db_name) <= 63
    # Should start with prefix and _db_
    assert full_db_name.startswith(static_part)
    # Only allowed chars
    assert all(c.isalnum() or c == '_' for c in full_db_name)
    # Should include at least part of test_name
    assert 'a' in full_db_name
