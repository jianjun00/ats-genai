from services.core.app.runner import Runner
from core.platform.config.environment import Environment, EnvironmentType
import pytest

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

# --- NEW TEST: Ensure iter_events only yields trading days ---
@pytest.mark.usefixtures('unit_test_db')
def test_iter_events_trading_days_only(unit_test_db):
    """
    Verifies that Runner.iter_events only yields events on exchange trading days (skips weekends/holidays).
    """
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    # 2024-02-02 is Friday, 2024-02-03 is Saturday, 2024-02-04 is Sunday, 2024-02-05 is Monday
    runner = Runner(
        start_date='2024-02-02',
        end_date='2024-02-05',
        environment=env,
        universe_id=1,
        callbacks=[],
        base_duration='1d'
    )
    events = list(runner.iter_events())
    interval_dates = [dt.date() for dt, etype in events if etype == 'interval']
    # Should not include Saturday or Sunday
    assert all(d.weekday() < 5 for d in interval_dates), f"Non-trading day found in interval events: {interval_dates}"
    # Should include Friday and Monday
    assert any(d.weekday() == 4 for d in interval_dates), "Missing Friday event in interval events"
    assert any(d.weekday() == 0 for d in interval_dates), "Missing Monday event in interval events"
    # Should not include 2024-02-03 (Sunday) or 2024-02-04 (Saturday)
    assert not any(d == __import__('datetime').date(2024,2,3) or d == __import__('datetime').date(2024,2,4) for d in interval_dates), f"Weekend day found in interval events: {interval_dates}"
