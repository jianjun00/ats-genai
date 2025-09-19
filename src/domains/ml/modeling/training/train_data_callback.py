import torch
import numpy as np
import pandas as pd
from domains.trading.services.state.runner_callback import RunnerCallback

class TrainDataCallback(RunnerCallback):
    def __init__(self, lag_steps, lead_steps, feature_cols, target_col, output_path):
        self.lag_steps = lag_steps
        self.lead_steps = lead_steps
        self.feature_cols = feature_cols
        self.target_col = target_col
        self.output_path = output_path
        self._prefetched_ohlc = None  # dict[date]->dict[instrument_id]->ohlc dict
        self._id_to_symbol = None
        self._reset()

    def set_prefetched_ohlc(self, prefetched_ohlc, id_to_symbol=None):
        """Inject pre-fetched OHLC data to avoid async calls during intervals.
        prefetched_ohlc: dict[datetime.date] -> dict[instrument_id] -> {open,high,low,close,volume,...}
        id_to_symbol: optional dict[int]->str for symbol labeling
        """
        self._prefetched_ohlc = prefetched_ohlc
        self._id_to_symbol = id_to_symbol or {}

    def _reset(self):
        self._dates = []
        self._date_to_idx = {}
        self._arr = None
        self._target_arr = None
        self._X_list = []
        self._y_list = []
        self._mask_list = []
        self._df_rows = []

    def handleStart(self, runner, current_time):
        self._reset()
        um = runner.get_universe_manager()
        ids = getattr(um, 'instrument_ids', []) if um is not None else []
        print(f"[DEBUG][handleStart] instrument_ids={ids}")
        # Dates will be collected as we go

    def handleInterval(self, runner, current_time):
        usm = runner.get_universe_state_manager()
        um = runner.get_universe_manager()
        instrument_ids = getattr(um, 'instrument_ids', []) if um is not None else []
        print(f"[DEBUG][handleInterval] called for date={current_time.date()}, instrument_ids={instrument_ids}")
        if not instrument_ids:
            return
        cur_date = current_time.date()
        for instrument_id in instrument_ids:
            # Prefer prefetched OHLC if available
            data = None
            if isinstance(self._prefetched_ohlc, dict):
                data = self._prefetched_ohlc.get(cur_date, {}).get(instrument_id)
            if data is not None:
                symbol = self._id_to_symbol.get(instrument_id)
                row = {
                    'date': cur_date,
                    'symbol': symbol if symbol else str(instrument_id),
                }
                for col in self.feature_cols:
                    row[col] = data.get(col, np.nan)
                self._df_rows.append(row)
                continue
            # Fallback to universe_state_manager lag prices
            symbol = usm.instrument_id_to_symbol(instrument_id) if hasattr(usm, 'instrument_id_to_symbol') else None
            print(f"[DEBUG][handleInterval] Attempting instrument_id={instrument_id}, symbol={symbol}, date={cur_date}")
            lag_df = usm.get_lag_prices(instrument_id, cur_date, self.lag_steps)
            row = {
                'date': cur_date,
                'symbol': symbol if symbol is not None else str(instrument_id),
            }
            if lag_df.empty:
                print(f"[DEBUG][handleInterval] No lag data: instrument_id={instrument_id}, symbol={symbol}, date={cur_date}; skipping row to avoid NaNs")
                continue
            # Only include the row if all required feature columns are present and non-null
            values = {}
            valid = True
            for col in self.feature_cols:
                if col in lag_df.columns and pd.notna(lag_df.iloc[-1][col]):
                    values[col] = lag_df.iloc[-1][col]
                else:
                    valid = False
                    break
            if not valid:
                print(f"[DEBUG][handleInterval] Missing feature columns for instrument_id={instrument_id} on {cur_date}; skipping row")
                continue
            row.update(values)
            self._df_rows.append(row)

    def handleEnd(self, runner, current_time):
        import pandas as pd
        import os
        print(f"[DEBUG][handleEnd] CWD: {os.getcwd()}")
        print(f"[DEBUG][handleEnd] Intended output_path: {self.output_path} (abs: {os.path.abspath(self.output_path)})")
        if not self._df_rows:
            print("[TrainDataCallback] No data collected.")
            print(f"[DEBUG][handleEnd] No data collected, output file will NOT be written: {self.output_path}")
            return
        df = pd.DataFrame(self._df_rows)
        df = df.sort_values(['symbol', 'date'])
        # Ensure 'close' exists (derive from close_price if needed)
        if 'close' not in df.columns and 'close_price' in df.columns:
            df['close'] = df['close_price']
        # Compute target as future close price shifted by -lead_steps per symbol
        df[self.target_col] = df.groupby('symbol')['close'].shift(-self.lead_steps)
        symbols = df['symbol'].unique()
        dates = sorted(df['date'].unique())
        symbol_to_idx = {s: i for i, s in enumerate(symbols)}
        date_to_idx = {d: i for i, d in enumerate(dates)}
        num_dates = len(dates)
        num_symbols = len(symbols)
        num_features = len(self.feature_cols)
        arr = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
        for _, row in df.iterrows():
            d_idx = date_to_idx[row['date']]
            s_idx = symbol_to_idx[row['symbol']]
            arr[d_idx, s_idx, :] = [row.get(col, np.nan) for col in self.feature_cols]
        target_arr = np.full((num_dates, num_symbols, 1), np.nan, dtype=np.float32)
        for _, row in df.iterrows():
            d_idx = date_to_idx[row['date']]
            s_idx = symbol_to_idx[row['symbol']]
            target_arr[d_idx, s_idx, 0] = row.get(self.target_col, np.nan)
        # Rolling window batching
        X_list, y_list, mask_list = [], [], []
        window_count = num_dates - self.lag_steps - self.lead_steps + 1
        for start in range(window_count if window_count is not None else 0):
            x = arr[start:start+self.lag_steps]
            y = target_arr[start+self.lag_steps:start+self.lag_steps+self.lead_steps]
            mask = ~np.isnan(y)
            X_list.append(x)
            y_list.append(y)
            mask_list.append(mask)
        if window_count is None or window_count <= 0 or len(X_list) == 0:
            # Not enough data to form standard windows; create a single padded window so batch>=1
            X = np.full((1, self.lag_steps, num_symbols, num_features), np.nan, dtype=np.float32)
            y = np.full((1, self.lead_steps, num_symbols, 1), np.nan, dtype=np.float32)
            # Fill the last min(num_dates, lag_steps) positions with available data
            lag_fill = min(num_dates, self.lag_steps)
            if lag_fill > 0:
                X[0, self.lag_steps - lag_fill:self.lag_steps, :, :] = arr[num_dates - lag_fill:num_dates, :, :]
            # Targets remain NaN if not enough future data; mask zeros
            mask = np.zeros_like(y, dtype=np.float32)
            X = torch.tensor(X, dtype=torch.float32)
            y = torch.tensor(y, dtype=torch.float32)
            mask = torch.tensor(mask, dtype=torch.float32)
        else:
            X = torch.tensor(np.stack(X_list), dtype=torch.float32)
            y = torch.tensor(np.stack(y_list), dtype=torch.float32)
            mask = torch.tensor(np.stack(mask_list), dtype=torch.float32)
        torch.save({'X': X, 'y': y, 'mask': mask}, self.output_path)
        print(f"[TrainDataCallback] Saved train data to {self.output_path}")
        print(f"[DEBUG][handleEnd] File exists after save: {os.path.exists(self.output_path)} (abs: {os.path.abspath(self.output_path)})")
