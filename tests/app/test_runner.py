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

def test_runner_with_universe_state_builder(tmp_path, caplog):
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

    # Patch Runner._init_callbacks to use our instance
    class TestRunner(Runner):
        def _init_callbacks(self):
            return [usb]

    # Run for two days
    runner = TestRunner(start_date="2025-07-23", end_date="2025-07-24", environment=env, universe_id=1)
    runner.run()

    # Check that callback methods were called
    logs = caplog.text
    assert "handleStartOfDay" in logs
    assert "handleEndOfDay" in logs
    assert "handleInterval" in logs
