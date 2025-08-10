"""
Example: Time Series Forecasting with Transformer (darts)
- Predicts next n days price movement using past lag_days price/indicator features.
- Uses UniverseStateManager APIs for feature/label extraction.
"""
import pandas as pd
from darts import TimeSeries
from darts.models import TransformerModel
from state.universe_state_manager import UniverseStateManager
from datetime import datetime, timedelta

# --- Config ---
INSTRUMENT_ID = 1  # Example instrument
LAG_DAYS = 30      # Number of past days to use as features
LEAD_DAYS = 7      # Number of future days to forecast
TRAIN_RATIO = 0.8
TARGET_COL = 'close'  # or 'high', 'low', etc.

# --- Data Preparation ---
manager = UniverseStateManager()

# Get full instrument history DataFrame
df = manager._get_instrument_history(INSTRUMENT_ID)
df = df.sort_values('date').reset_index(drop=True)

# Create Darts TimeSeries object for the target column
series = TimeSeries.from_dataframe(df, 'date', TARGET_COL)

# Optional: add covariates (e.g., open, high, low, etop, ebot, pldot)
covariate_cols = ['open', 'high', 'low', 'etop', 'ebot', 'pldot']
covariates = TimeSeries.from_dataframe(df, 'date', covariate_cols) if all(col in df.columns for col in covariate_cols) else None

# --- Train/Val Split ---
train, val = series.split_before(int(TRAIN_RATIO * len(series)))
if covariates is not None:
    cov_train, cov_val = covariates.split_before(int(TRAIN_RATIO * len(series)))
else:
    cov_train = cov_val = None

# --- Model Setup ---
model = TransformerModel(
    input_chunk_length=LAG_DAYS,
    output_chunk_length=LEAD_DAYS,
    d_model=64,
    nhead=4,
    num_encoder_layers=2,
    num_decoder_layers=2,
    dropout=0.1,
    batch_size=32,
    n_epochs=50,
    random_state=42,
)

# --- Training ---
model.fit(
    train,
    past_covariates=cov_train if covariates is not None else None,
    verbose=True,
)

# --- Forecasting ---
forecast = model.predict(
    n=LEAD_DAYS,
    past_covariates=cov_val if covariates is not None else None,
)

print(f"Forecast for next {LEAD_DAYS} days:")
print(forecast.pd_dataframe())
