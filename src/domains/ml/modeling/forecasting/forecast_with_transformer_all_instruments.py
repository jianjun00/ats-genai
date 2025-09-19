"""
Example: Multi-instrument Time Series Forecasting with Transformer (darts)
- Loops over all instruments in UniverseStateManager
- Uses lag/lead windowing for features/labels
- Trains a Darts TransformerModel per instrument
"""
from darts import TimeSeries
from darts.models import TransformerModel
from domains.trading.services.state.universe_state_manager import UniverseStateManager

# --- Config ---
LAG_DAYS = 30      # Number of past days to use as features
LEAD_DAYS = 7      # Number of future days to forecast
TRAIN_RATIO = 0.8
TARGET_COL = 'close'

# --- Data Preparation ---
# CRITICAL: Create UniverseStateManager with proper run_context to avoid constraint violations
from domains.trading.services.state.run_aware_universe_state_manager import create_run_aware_universe_state_manager
from core.infrastructure.run_context import RunContext
import uuid

# Create proper run_context with unique run_id to prevent constraint violations
run_context = RunContext(run_id=f"forecast_all_{uuid.uuid4().hex[:8]}")
manager = create_run_aware_universe_state_manager(env=None, run_context=run_context)

# Get all instrument IDs from universe state (from cache or latest state)
def get_all_instrument_ids(manager):
    # Try cache, else load latest universe state
    for df in manager._cache.values():
        return df['instrument_id'].unique().tolist()
    latest_ts = manager.get_latest_timestamp()
    if latest_ts:
        df = manager.load_universe_state(timestamp=latest_ts)
        return df['instrument_id'].unique().tolist()
    raise ValueError("No universe state data available")

instrument_ids = get_all_instrument_ids(manager)

results = {}
for inst_id in instrument_ids:
    print(f"\n=== Instrument {inst_id} ===")
    try:
        df = manager._get_instrument_history(inst_id)
        df = df.sort_values('date').reset_index(drop=True)
        if len(df) < (LAG_DAYS + LEAD_DAYS + 1):
            print(f"Not enough data for instrument {inst_id}, skipping.")
            continue
        series = TimeSeries.from_dataframe(df, 'date', TARGET_COL)
        covariate_cols = ['open', 'high', 'low', 'etop', 'ebot', 'pldot']
        covariates = TimeSeries.from_dataframe(df, 'date', covariate_cols) if all(col in df.columns for col in covariate_cols) else None
        train, val = series.split_before(int(TRAIN_RATIO * len(series)))
        if covariates is not None:
            cov_train, cov_val = covariates.split_before(int(TRAIN_RATIO * len(series)))
        else:
            cov_train = cov_val = None
        model = TransformerModel(
            input_chunk_length=LAG_DAYS,
            output_chunk_length=LEAD_DAYS,
            d_model=64,
            nhead=4,
            num_encoder_layers=2,
            num_decoder_layers=2,
            dropout=0.1,
            batch_size=32,
            n_epochs=30,
            random_state=42,
        )
        model.fit(
            train,
            past_covariates=cov_train if covariates is not None else None,
            verbose=True,
        )
        forecast = model.predict(
            n=LEAD_DAYS,
            past_covariates=cov_val if covariates is not None else None,
        )
        print(f"Forecast for next {LEAD_DAYS} days for instrument {inst_id}:")
        print(forecast.pd_dataframe())
        results[inst_id] = forecast
    except Exception as e:
        print(f"Error processing instrument {inst_id}: {e}")
