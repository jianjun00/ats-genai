from src.app.runner import Runner
from config.environment import Environment, EnvironmentType
import pytest

import pytest
from config.environment import Environment, EnvironmentType
from src.app.runner import Runner

@pytest.mark.usefixtures('unit_test_db')
def test_print_iter_events(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    runner = Runner(
        start_date='2024-01-01',
        end_date='2024-01-03',
        environment=env,
        universe_id=1,
        callbacks=[],
        base_duration='1d'
    )
    events = list(runner.iter_events())
    for dt, etype in events:
        print(f"iter_events: {etype} at {dt}")
    interval_events = [e for e in events if e[1] == 'interval']
    assert interval_events, 'No interval events emitted!'
