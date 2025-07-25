import pytest
import asyncio
from datetime import date, timedelta
import asyncpg
from config.environment import get_environment, set_environment, EnvironmentType
from src.secmaster.secmaster import SecMaster
from db.test_intg_db_base import AsyncPGTestDBBase
from intg_tests.secmaster.fixtures_spy_membership import spy_membership_fixture

set_environment(EnvironmentType.INTEGRATION)
env = get_environment()

class TestIntegrationSecMaster(AsyncPGTestDBBase):
    @pytest.mark.asyncio
    async def test_spy_membership_and_caches(self, spy_membership_fixture):
        # Pick a date with known membership (should be in DB from fixtures/migrations)
        test_date = date(2025, 1, 5)
        secmaster = SecMaster(env=env, as_of_date=test_date)
        # Test get_spy_membership returns non-empty membership
        membership = await secmaster.get_spy_membership()
        assert isinstance(membership, list)
        assert len(membership) > 0, "SPY membership should not be empty for test date"
        symbol = membership[0]
        # Test get_last_close_price, get_market_cap, get_average_dollar_volume
        close = await secmaster.get_last_close_price(symbol)
        assert close is None or isinstance(close, (float, int)), f"Close price for {symbol} should be numeric or None"
        mc = await secmaster.get_market_cap(symbol)
        assert mc is None or isinstance(mc, (float, int)), f"Market cap for {symbol} should be numeric or None"
        adv = await secmaster.get_average_dollar_volume(symbol, window=5)
        assert adv is None or isinstance(adv, (float, int)), f"ADV for {symbol} should be numeric or None"
        # Test advance logic (should update as_of_date and caches)
        new_date = test_date + timedelta(days=5)
        new_membership = await secmaster.advance(new_date)
        assert isinstance(new_membership, list)
        assert len(new_membership) > 0, "SPY membership after advance should not be empty"
        assert secmaster.as_of_date == new_date
        # Test get_spy_membership_over_dates
        dates = [test_date, new_date]
        over_dates = await secmaster.get_spy_membership_over_dates(dates)
        assert isinstance(over_dates, dict)
        assert all(isinstance(v, set) for v in over_dates.values())
        assert all(len(v) > 0 for v in over_dates.values())
