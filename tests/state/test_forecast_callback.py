import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import asyncio

import torch
import pytest


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_forecast_callback_augments_universe_state(tmp_path):
    # Ensure PYTHONPATH=src for imports when running in subprocess/pytests
    repo_root = Path(__file__).resolve().parents[2]
    src_dir = repo_root / "src"
    if "PYTHONPATH" in os.environ and os.environ["PYTHONPATH"]:
        os.environ["PYTHONPATH"] = f"{src_dir}:{os.environ['PYTHONPATH']}"
    else:
        os.environ["PYTHONPATH"] = str(src_dir)
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    # Local imports after path setup
    from state.forecast_callback import MultiInstrumentTransformer, ForecastCallback
    from state.universe_state_builder import UniverseStateIntervalBuilder

    # --- Create a tiny model checkpoint ---
    num_instruments = 2
    num_features = 4
    model = MultiInstrumentTransformer(num_instruments=num_instruments, num_features=num_features)
    ckpt_path = tmp_path / "model_checkpoint.pt"
    torch.save(model.state_dict(), ckpt_path)

    # --- Stubs for runner and dependencies ---
    class FakeUniverseManager:
        def __init__(self, instrument_ids):
            self.instrument_ids = instrument_ids

    class FakeMarketDataManager:
        async def get_ohlc_batch(self, instrument_ids, start_time, end_time):
            # Return deterministic OHLC
            return {i: {"open": 10.0 + i, "high": 11.0 + i, "low": 9.0 + i, "close": 10.5 + i, "volume": 1000.0 + i} for i in instrument_ids}

    class FakeUniverseStateManager:
        def __init__(self):
            self.captured = None
        async def addUniverseState(self, duration_to_state, current_time):
            self.captured = (duration_to_state, current_time)

    class FakeEnv:
        def get_indicator_config(self):
            # Avoid real indicator building complexity by returning an empty IndicatorConfig
            from domains.trading.services.indicator_config import IndicatorConfig
            return IndicatorConfig.empty_config()
        def get_table_name(self, base: str) -> str:
            # Minimal stub to satisfy DailyMarketCapDAO
            return base
        def get_database_url(self) -> str:
            # Not used (DAO is stubbed later), but required at init
            return "postgresql://user:pass@localhost/db"

    # Instantiate builder with simple durations
    env = FakeEnv()
    builder = UniverseStateIntervalBuilder(env=env, base_duration="5m", target_durations="5m", forecast_callback=ForecastCallback(str(ckpt_path)))

    # Patch out indicator builder with a stub that returns empty dict
    class StubIndicatorBuilder:
        def build_indicator_intervals(self, *args, **kwargs):
            return {}
    builder.indicator_builder = StubIndicatorBuilder()

    # Patch market cap DAO to avoid DB
    class StubMarketCapDAO:
        async def list_market_caps_for_date(self, day):
            return [{"instrument_id": 1, "market_cap": 1_000_000}, {"instrument_id": 2, "market_cap": 2_000_000}]
    builder.market_cap_dao = StubMarketCapDAO()

    runner = type("Runner", (), {})()
    runner.universe_id = 42
    runner.universe_manager = FakeUniverseManager([1, 2])
    runner.market_data_manager = FakeMarketDataManager()
    runner.universe_state_manager = FakeUniverseStateManager()

    now = datetime(2025, 1, 1, 10, 0, 0)

    # Run
    await builder.handleInterval(runner, now)

    # Validate captured state contains forecasts
    assert runner.universe_state_manager.captured is not None, "Universe state was not saved"
    (duration_to_state, ts) = runner.universe_state_manager.captured
    assert ts == now
    assert len(duration_to_state) == 1
    usi = list(duration_to_state.values())[0]

    # Check that instrument_forecast_intervals exist and have one forecast per instrument
    assert hasattr(usi, 'instrument_forecast_intervals')
    ffi = usi.instrument_forecast_intervals
    assert set(ffi.keys()) == {1, 2}
    for inst_id, finterval in ffi.items():
        assert finterval.instrument_id == inst_id
        assert finterval.start_date_time == now
        assert finterval.end_date_time == usi.end_date_time
        assert isinstance(finterval.forecasts, list) and len(finterval.forecasts) == 1
        # forecast should be finite float
        assert isinstance(finterval.forecasts[0], float)
