import os
import tempfile
from datetime import datetime, timedelta
from config.environment import Environment
from app.runner import Runner
from state.universe_state_builder import UniverseStateBuilder
import logging

class DummyUniverse:
    instrument_ids = ['AAPL', 'TSLA']
    # Add any other fields required by UniverseStateBuilder

class DummyStateManager:
    pass

import pytest
@pytest.mark.asyncio
async def test_runner_with_universe_state_builder(tmp_path, caplog):
    # Minimal environment config with runner callback
    config_path = tmp_path / "test.conf"
    with open(config_path, "w") as f:
        f.write("""
[runner]
callbacks=state.universe_state_builder.UniverseStateBuilder
""")
    import os
    from config.environment import EnvironmentType, set_environment, get_environment
    os.environ["ENVIRONMENT"] = "test"
    set_environment(EnvironmentType.TEST)
    env = get_environment()

    # Setup logger to capture output
    caplog.set_level(logging.INFO)

    # Instantiate UniverseStateBuilder (callback)
    universe = DummyUniverse()
    state_manager = DummyStateManager()
    usb = UniverseStateBuilder(env=env)

    # Patch env.get_base_duration to return a 1-day duration
    class DummyDuration:
        def is_daily_or_longer(self):
            return True
        def get_duration_minutes(self):
            return None
        duration_type = type('dt', (), {'name': 'DAILY'})
    env.get_base_duration = lambda: DummyDuration()

    # Patch UniverseManager and UniverseDB to avoid real DB access
    from unittest.mock import AsyncMock, MagicMock, patch

    class DummyUniverseManager:
        def __init__(self, env):
            self.env = env
            self.instrument_ids = ['AAPL', 'TSLA']
        async def update_for_sod(self, *args, **kwargs):
            return None
        async def update_for_eod(self, *args, **kwargs):
            return None
        async def get_members(self, universe_id, as_of_date):
            return ['AAPL', 'TSLA']

    class DummyUniverseDB:
        async def get_universe_members(self, universe_id, as_of):
            return ['AAPL', 'TSLA']

    # Patch MarketDataManager to avoid real DB access
    class DummyMarketDataManager:
        async def update_for_sod(self, *args, **kwargs):
            return None
        async def update_for_eod(self, *args, **kwargs):
            return None
        def get_ohlc_batch(self, instrument_ids, current_time, base_end_time):
            # Return dummy OHLC data for all instrument_ids
            return {iid: {'open': 100, 'high': 110, 'low': 90, 'close': 105, 'volume': 1000} for iid in instrument_ids}

    # Patch Runner to use DummyUniverseManager and DummyMarketDataManager
    class TestRunner(Runner):
        def _init_callbacks(self):
            return [usb]
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.universe_manager = DummyUniverseManager(self.env)
            self.universe_manager.universe_db = DummyUniverseDB()
            self.market_data_manager = DummyMarketDataManager()

    # Run for two days
    runner = TestRunner(start_date="2025-07-23", end_date="2025-07-24", environment=env, universe_id=1)
    await runner.run()

    # Check that callback methods were called
    logs = caplog.text
    assert "handleStartOfDay" in logs
    assert "handleEndOfDay" in logs
    assert "handleInterval" in logs
