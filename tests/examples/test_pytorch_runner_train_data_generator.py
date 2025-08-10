import pytest
import torch
from config.environment import Environment, EnvironmentType
from examples.pytorch_runner_train_data_generator import generate_train_data, LAG_STEPS, LEAD_STEPS, FEATURE_COLS, TARGET_COL

import pytest
from src.db.test_db_manager import unit_test_db

from tests.fixtures.setup_test_universe_data import setup_test_universe_data

@pytest.mark.asyncio
async def test_generate_train_data_shapes(unit_test_db, setup_test_universe_data):
    import os
    from config.environment import Environment, EnvironmentType
    from examples.pytorch_runner_train_data_generator import generate_train_data, LAG_STEPS, LEAD_STEPS, FEATURE_COLS, TARGET_COL
    import torch
    # Use a very short date range for a fast test
    start_date = "2024-01-01"
    end_date = "2024-02-15"
    universe_id = 1
    env = Environment(None, EnvironmentType.TEST, db_url=unit_test_db)
    output_path = "test_train_data.pt"
    # Debug log for environment and table names
    print(f"[DEBUG][test_generate_train_data_shapes] env.env_type: {getattr(env, 'env_type', None)}")
    print(f"[DEBUG][test_generate_train_data_shapes] env.db_url: {getattr(env, 'db_url', None)}")
    if hasattr(env, 'get_table_name'):
        print(f"[DEBUG][test_generate_train_data_shapes] test_daily_prices_polygon: {env.get_table_name('daily_prices_polygon')}")
        print(f"[DEBUG][test_generate_train_data_shapes] test_daily_prices_tiingo: {env.get_table_name('daily_prices_tiingo')}")
    else:
        print(f"[DEBUG][test_generate_train_data_shapes] env.get_table_name not available")
    import os
    print(f"[DEBUG][test_generate_train_data_shapes] CWD before call: {os.getcwd()}")
    print(f"[DEBUG][test_generate_train_data_shapes] output_path: {output_path} (abs: {os.path.abspath(output_path)})")
    print(f"[DEBUG][test_generate_train_data_shapes] File exists before call: {os.path.exists(output_path)}")
    X, y, mask = await generate_train_data(
        start_date=start_date,
        end_date=end_date,
        environment=env,
        universe_id=universe_id,
        symbols=None,
        vendor='polygon',
        output_path=output_path,
    )
    print(f"[DEBUG][test_generate_train_data_shapes] File exists after call: {os.path.exists(output_path)}")
    print(f"[DEBUG][test_generate_train_data_shapes] X shape: {getattr(X, 'shape', None)}, y shape: {getattr(y, 'shape', None)}, mask shape: {getattr(mask, 'shape', None)}")
    # Print out contents of saved train data before deletion
    if os.path.exists(output_path):
        saved = torch.load(output_path)
        print("[TRAIN_DATA] Saved keys:", list(saved.keys()))
        print("[TRAIN_DATA] X.shape:", saved['X'].shape)
        print("[TRAIN_DATA] y.shape:", saved['y'].shape)
        print("[TRAIN_DATA] mask.shape:", saved['mask'].shape)
        # Print small samples
        try:
            print("[TRAIN_DATA] X[0, -1, 0, :]:", saved['X'][0, -1, 0, :])
            print("[TRAIN_DATA] y[0, 0, 0, 0]:", saved['y'][0, 0, 0, 0])
            print("[TRAIN_DATA] mask[0, 0, 0, 0]:", saved['mask'][0, 0, 0, 0])
        except Exception as e:
            print("[TRAIN_DATA] Sample print failed:", e)
    assert os.path.exists(output_path)
    os.remove(output_path)

    # X: [batch, lag_steps, num_instruments, features]
    # y: [batch, lead_steps, num_instruments, 1]
    # mask: same as y
    assert isinstance(X, torch.Tensor)
    assert isinstance(y, torch.Tensor)
    assert isinstance(mask, torch.Tensor)
    assert X.shape[1] == LAG_STEPS
    assert y.shape[1] == LEAD_STEPS
    assert X.shape[2] == y.shape[2]  # num_instruments
    assert X.shape[3] == len(FEATURE_COLS)
    assert y.shape[3] == 1
    assert mask.shape == y.shape
    # Optionally check that mask is 1 where y is not nan
    assert torch.all((mask == 1) | (mask == 0))
    # Assert there are no NaNs in feature tensor X
    assert not torch.isnan(X).any(), "X contains NaNs; lag data likely missing before current date"
    # Optionally print stats
    print(f"X shape: {X.shape}, y shape: {y.shape}, mask shape: {mask.shape}")
    print(f"X sample: {X[0,0,0,:]}")
    print(f"y sample: {y[0,0,0,0]}")
