import pytest

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_unit_db_name_format_and_length(unit_test_db):
    # unit_test_db is a test DB URL, e.g. postgresql://.../test_db_...
    db_url = unit_test_db
    db_name = db_url.rsplit('/', 1)[-1]
    # Should not exceed 63 chars
    assert len(db_name) <= 63
    # Should start with 'test_db_'
    assert db_name.startswith('test_db_')
    # Only allowed chars
    assert all(c.isalnum() or c == '_' for c in db_name)
    # Should contain some unique hash or truncated name
    # (Can't guarantee 'a' is in db_name, so just check it's not generic)
    assert db_name != 'test_db_'
