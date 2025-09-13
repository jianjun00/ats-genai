import os
import tempfile
from datetime import datetime, timedelta
from core.platform.config.environment import Environment
from services.core.app.runner import Runner
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from shared.utils.environment import Environment
from app.runner import Runner
import logging

class DummyUniverse:
    instrument_ids = ['AAPL', 'TSLA']
    # Add any other fields required by UniverseStateIntervalBuilder

class DummyStateManager:
    pass

import pytest

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_runner_with_universe_state_builder(tmp_path, caplog, unit_test_db):
    # Minimal environment config with runner callback
    config_path = tmp_path / "test.conf"
    with open(config_path, "w") as f:
        f.write("""
[runner]
callbacks=state.universe_state_builder.UniverseStateIntervalBuilder
""")
    import os
    os.environ["ENVIRONMENT"] = "test"
    # set_environment(EnvironmentType.TEST)
    env = Environment(env_type="test", db_url=unit_test_db)

    # Setup logger to capture output
    caplog.set_level(logging.INFO)

    # Instantiate UniverseStateIntervalBuilder (callback)
    universe = DummyUniverse()
    state_manager = DummyStateManager()
    from state.runner_callback import RunnerCallback
    class DummyUniverseStateIntervalBuilder(RunnerCallback):
        def __init__(self):
            self.env = env
            self.base_duration = '1d'
            self.target_durations = '1d'
            self.logger = logging.getLogger(__name__)
        def handleStartOfDay(self, runner, current_time):
            self.logger.info(f"DummyUniverseStateIntervalBuilder.handleStartOfDay called at {current_time}")
        def handleEndOfDay(self, runner, current_time):
            self.logger.info(f"DummyUniverseStateIntervalBuilder.handleEndOfDay called at {current_time}")
        def handleInterval(self, runner, current_time):
            self.logger.info(f"DummyUniverseStateIntervalBuilder.handleInterval called at {current_time}")
    callback_class = DummyUniverseStateIntervalBuilder

    # Patch env.get_base_duration to return a 1-day duration
    class DummyDuration:
        def is_daily_or_longer(self):
            return True
        def get_duration_minutes(self):
            return None
        duration_type = type('dt', (), {'name': 'DAILY'})
    env.get_base_duration = lambda: DummyDuration()

    # Patch UniverseManager and UniverseDB to avoid real DB access

    class DummyUniverseManager:
        def __init__(self, env, universe_id):
            self.env = env
            self.universe_id = universe_id
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
        def __init__(self, start_date, end_date, environment, universe_id):
            super().__init__(
                start_date=start_date,
                end_date=end_date,
                environment=environment,
                universe_id=universe_id,
                callbacks=[callback_class],
                base_duration='1d',
                universe_manager=DummyUniverseManager(environment, universe_id),
                market_data_manager=DummyMarketDataManager()
            )
            self.universe_manager.universe_db = DummyUniverseDB()

    # Run for two days
    runner = TestRunner(start_date="2025-07-23", end_date="2025-07-24", environment=env, universe_id=1)
    await runner.run()

    # Check that callback methods were called
    logs = caplog.text
    assert "handleStartOfDay" in logs
    assert "handleEndOfDay" in logs
    assert "handleInterval" in logs
