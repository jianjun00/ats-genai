#!/usr/bin/env python3

#!/usr/bin/env python3
"""Comprehensive Training Data Generation for ALL Available Instruments"""

import os
import asyncio
import logging
import json
import time
import decimal
from datetime import date, datetime
from pathlib import Path
import pandas as pd
import numpy as np
import asyncpg
from tqdm import tqdm
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Comprehensive10KTrainingGenerator:
def __init__(self):
self.db_url = f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
# Use broader date range to capture all available data
self.training_start = date(2015, 1, 1)  # Expanded range
self.training_end = date(2024, 12, 31)  # Include recent data
self.min_days_required = 50  # Lower threshold to include more instruments

def _decimal_to_float(self, value):
"""Convert decimal.Decimal to float safely"""
if isinstance(value, decimal.Decimal):
return float(value)
elif value is None:
return 0.0
else:
return float(value)

async def run_comprehensive_training(self):
logger.info("🚀 Starting COMPREHENSIVE Training Data Generation for ALL Instruments")
logger.info(f"📅 Date Range: {self.training_start} to {self.training_end}")
logger.info(f"🎯 Minimum Days Required: {self.min_days_required}")

start_time = time.time()

# Get ALL eligible instruments (not just 2020-2023)
all_instruments = await self.get_all_eligible_instruments()
logger.info(f"📊 Found {len(all_instruments)} eligible instruments across ALL time periods")

if len(all_instruments) == 0:
logger.error("❌ No instruments found with sufficient data")
return {}

# Process ALL instruments in batches
all_training_data = []
batch_size = 10

for i in range(0, len(all_instruments), batch_size):
batch = all_instruments[i:i+batch_size]
logger.info(f"📦 Processing batch {i//batch_size + 1}/{(len(all_instruments)-1)//batch_size + 1}: {len(batch)} instruments")

batch_data = await self.generate_training_data_batch(batch)
all_training_data.extend(batch_data)

logger.info(f"✅ Batch completed. Total examples so far: {len(all_training_data):,}")

# Memory management
if len(all_training_data) >= 100000:  # Cap at 100k examples
logger.info("🔄 Reached 100k examples limit for memory management")
break

logger.info(f"✅ Generated {len(all_training_data):,} COMPREHENSIVE training examples")

if len(all_training_data) < 100:
logger.error("❌ Insufficient training data")
return {}

# Train comprehensive model
model_results = await self.train_comprehensive_model(all_training_data)

# Save comprehensive results
training_time = time.time() - start_time

comprehensive_report = {
'training_type': 'comprehensive_all_instruments',
'training_completed': True,
'total_instruments_processed': len(all_instruments),
'total_training_examples': len(all_training_data),
'training_time_hours': training_time / 3600,
'date_range': {
'start': self.training_start.isoformat(),
'end': self.training_end.isoformat()
},
'model_performance': model_results,
'timestamp': datetime.utcnow().isoformat(),
'data_distribution': await self.analyze_data_distribution(all_training_data),
'coverage_analysis': await self.get_coverage_analysis()
}

# Save comprehensive report
Path('/app/models/production').mkdir(parents=True, exist_ok=True)
with open('/app/models/production/comprehensive_10k_training_report.json', 'w') as f:
json.dump(comprehensive_report, f, indent=2)

logger.info("✅ COMPREHENSIVE training completed successfully!")
return comprehensive_report

async def get_all_eligible_instruments(self):
"""Get ALL instruments with any significant price data"""
conn = await asyncpg.connect(self.db_url)
try:
# Comprehensive query for ALL instruments with data
query = """
SELECT 
i.id as instrument_id,
i.symbol, 
COUNT(dp.date) as price_count,
MIN(dp.date) as first_date,
MAX(dp.date) as last_date,
EXTRACT(DAYS FROM (MAX(dp.date) - MIN(dp.date))) as date_span_days
FROM dev_instruments i
JOIN dev_daily_prices dp ON i.id = dp.instrument_id
WHERE dp.date >= $1 AND dp.date <= $2
GROUP BY i.id, i.symbol
HAVING COUNT(dp.date) >= $3
ORDER BY COUNT(dp.date) DESC
"""

rows = await conn.fetch(query, self.training_start, self.training_end, self.min_days_required)
instruments = [dict(row) for row in rows]

# Log detailed statistics
if instruments:
total_prices = sum(inst['price_count'] for inst in instruments)
avg_prices = total_prices / len(instruments)
logger.info(f"📈 Instrument Statistics:")
logger.info(f"   Total Instruments: {len(instruments)}")
logger.info(f"   Total Price Records: {total_prices:,}")
logger.info(f"   Average Records per Instrument: {avg_prices:.1f}")
logger.info(f"   Top 10 Symbols: {[inst['symbol'] for inst in instruments[:10]]}")

return instruments

finally:
await conn.close()

async def generate_training_data_batch(self, instruments_batch):
"""Generate training data for a batch of instruments"""
batch_examples = []
conn = await asyncpg.connect(self.db_url)

try:
for inst in instruments_batch:
try:
# Get ALL available price data for this instrument
prices = await conn.fetch("""
SELECT date, open_price, high_price, low_price, close, volume
FROM dev_daily_prices
WHERE instrument_id = $1 
AND date >= $2 AND date <= $3
ORDER BY date
""", inst['instrument_id'], self.training_start, self.training_end)

if len(prices) < self.min_days_required:
continue

# Convert to DataFrame with decimal handling
price_data = []
for price_row in prices:
price_data.append({
'date': price_row['date'],
'open_price': self._decimal_to_float(price_row['open_price']),
'high_price': self._decimal_to_float(price_row['high_price']),
'low_price': self._decimal_to_float(price_row['low_price']),
'close_price': self._decimal_to_float(price_row['close']),
'volume': self._decimal_to_float(price_row['volume'])
})

df = pd.DataFrame(price_data)

# Generate comprehensive features
features = self._generate_comprehensive_features(df)

# Generate multiple prediction targets
targets = self._generate_prediction_targets(df)

# Create training examples
for j in range(len(features)):
if j < len(targets) and len(features[j]) > 0:
batch_examples.append({
'symbol': inst['symbol'],
'instrument_id': inst['instrument_id'],
'date': df.iloc[j]['date'].date() if j < len(df) else None,
'features': features[j],
'targets': targets[j]
})

except Exception as e:
logger.warning(f"Error processing {inst['symbol']}: {e}")
continue

return batch_examples

finally:
await conn.close()

def _generate_comprehensive_features(self, df):
"""Generate comprehensive technical features"""
try:
# Basic price features
df['returns'] = df['close_price'].pct_change()
df['high_low_ratio'] = (df['high_price'] - df['low_price']) / df['close_price']
df['open_close_ratio'] = (df['close_price'] - df['open_price']) / df['open_price']

# Multiple timeframe moving averages
for window in [5, 10, 20, 50]:
df[f'sma_{window}'] = df['close_price'].rolling(window).mean()
df[f'price_vs_sma_{window}'] = df['close_price'] / df[f'sma_{window}'] - 1

# Volatility measures
for window in [5, 10, 20]:
df[f'volatility_{window}'] = df['returns'].rolling(window).std()

# Volume features
df['volume_sma_10'] = df['volume'].rolling(10).mean()
df['volume_ratio'] = df['volume'] / df['volume_sma_10']

# Momentum indicators
df['rsi_14'] = self._calculate_rsi(df['close_price'], 14)
df['momentum_10'] = df['close_price'] / df['close_price'].shift(10) - 1

# Select feature columns
feature_cols = [
'returns', 'high_low_ratio', 'open_close_ratio',
'price_vs_sma_5', 'price_vs_sma_10', 'price_vs_sma_20', 'price_vs_sma_50',
'volatility_5', 'volatility_10', 'volatility_20',
'volume_ratio', 'rsi_14', 'momentum_10'
]

features = []
for i in range(len(df)):
row_features = []
for col in feature_cols:
val = df.iloc[i][col] if col in df.columns else 0
if pd.isna(val) or np.isinf(val):
val = 0.0
# Clip extreme values
val = np.clip(float(val), -10.0, 10.0)
row_features.append(val)
features.append(row_features)

return features

except Exception as e:
logger.warning(f"Feature generation error: {e}")
return []

def _calculate_rsi(self, prices, window=14):
"""Calculate RSI indicator"""
delta = prices.diff()
gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
rs = gain / loss
rsi = 100 - (100 / (1 + rs))
return rsi.fillna(50)  # Neutral RSI for NaN values

def _generate_prediction_targets(self, df):
"""Generate multiple prediction targets"""
targets = []

for i in range(len(df)):
if i >= len(df) - 5:  # Skip last few rows
targets.append({
'next_day_return': 0.0,
'next_5day_return': 0.0,
'volatility_next_5days': 0.0
})
continue

current_price = df.iloc[i]['close_price']

# Next day return
next_day_return = (df.iloc[i+1]['close_price'] - current_price) / current_price

# Next 5-day return
next_5day_return = (df.iloc[i+5]['close_price'] - current_price) / current_price

# Volatility over next 5 days
next_5day_returns = []
for j in range(1, 6):
if i+j < len(df):
daily_return = (df.iloc[i+j]['close_price'] - df.iloc[i+j-1]['close_price']) / df.iloc[i+j-1]['close_price']
next_5day_returns.append(daily_return)

volatility_next_5days = np.std(next_5day_returns) if next_5day_returns else 0.0

targets.append({
'next_day_return': float(next_day_return),
'next_5day_return': float(next_5day_return),
'volatility_next_5days': float(volatility_next_5days)
})

return targets

async def train_comprehensive_model(self, training_data):
"""Train comprehensive ensemble model"""
logger.info(f"🧠 Training COMPREHENSIVE model on {len(training_data)} examples")

# Prepare multi-target data
X = []
y_next_day = []
y_5day = []
y_volatility = []

for example in training_data:
if len(example['features']) == 13:  # Ensure consistent feature count
X.append(example['features'])
y_next_day.append(example['targets']['next_day_return'])
y_5day.append(example['targets']['next_5day_return'])
y_volatility.append(example['targets']['volatility_next_5days'])

X = np.array(X)
y_next_day = np.array(y_next_day)
y_5day = np.array(y_5day)
y_volatility = np.array(y_volatility)

logger.info(f"📊 Training data shape: X={X.shape}")

# Train ensemble models
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import xgboost as xgb

# Split data
X_train, X_test, y1_train, y1_test, y2_train, y2_test, y3_train, y3_test = train_test_split(
X, y_next_day, y_5day, y_volatility, test_size=0.2, random_state=42
)

# Train models for each target
models = {}
results = {}

# Next day return model
rf_next_day = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
rf_next_day.fit(X_train, y1_train)
models['next_day_model'] = rf_next_day

pred_next_day = rf_next_day.predict(X_test)
results['next_day_mse'] = float(mean_squared_error(y1_test, pred_next_day))
results['next_day_r2'] = float(r2_score(y1_test, pred_next_day))

# 5-day return model  
xgb_5day = xgb.XGBRegressor(n_estimators=100, random_state=42)
xgb_5day.fit(X_train, y2_train)
models['five_day_model'] = xgb_5day

pred_5day = xgb_5day.predict(X_test)
results['five_day_mse'] = float(mean_squared_error(y2_test, pred_5day))
results['five_day_r2'] = float(r2_score(y2_test, pred_5day))

# Volatility model
rf_volatility = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
rf_volatility.fit(X_train, y3_train)
models['volatility_model'] = rf_volatility

pred_volatility = rf_volatility.predict(X_test)
results['volatility_mse'] = float(mean_squared_error(y3_test, pred_volatility))
results['volatility_r2'] = float(r2_score(y3_test, pred_volatility))

# Save ensemble model
ensemble_model = {
'next_day_model': models['next_day_model'],
'five_day_model': models['five_day_model'],
'volatility_model': models['volatility_model'],
'feature_names': [
'returns', 'high_low_ratio', 'open_close_ratio',
'price_vs_sma_5', 'price_vs_sma_10', 'price_vs_sma_20', 'price_vs_sma_50',
'volatility_5', 'volatility_10', 'volatility_20',
'volume_ratio', 'rsi_14', 'momentum_10'
]
}

Path('/app/models/production').mkdir(parents=True, exist_ok=True)
with open('/app/models/production/comprehensive_10k_ensemble_model.pkl', 'wb') as f:
pickle.dump(ensemble_model, f)

results.update({
'model_type': 'comprehensive_ensemble',
'training_samples': len(X_train),
'test_samples': len(X_test),
'feature_count': X.shape[1],
'overall_performance': (results['next_day_r2'] + results['five_day_r2'] + results['volatility_r2']) / 3
})

logger.info(f"✅ Comprehensive model training completed!")
logger.info(f"📈 Next Day R²: {results['next_day_r2']:.3f}")
logger.info(f"📈 5-Day R²: {results['five_day_r2']:.3f}")
logger.info(f"📈 Volatility R²: {results['volatility_r2']:.3f}")
logger.info(f"🎯 Overall Performance: {results['overall_performance']:.3f}")

return results

async def analyze_data_distribution(self, training_data):
"""Analyze the distribution of training data"""
symbols = [ex['symbol'] for ex in training_data]
symbol_counts = {}
for symbol in symbols:
symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

return {
'unique_symbols': len(set(symbols)),
'total_examples': len(training_data),
'avg_examples_per_symbol': len(training_data) / len(set(symbols)),
'top_10_symbols_by_examples': sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:10]
}

async def get_coverage_analysis(self):
"""Get database coverage analysis"""
conn = await asyncpg.connect(self.db_url)
try:
coverage = await conn.fetchrow("""
SELECT 
COUNT(DISTINCT i.id) as total_instruments,
COUNT(DISTINCT dp.instrument_id) as instruments_with_data,
MIN(dp.date) as earliest_date,
MAX(dp.date) as latest_date,
COUNT(*) as total_price_records
FROM dev_instruments i
LEFT JOIN dev_daily_prices dp ON i.id = dp.instrument_id
""")

return dict(coverage)
finally:
await conn.close()

async def main():
trainer = Comprehensive10KTrainingGenerator()
results = await trainer.run_comprehensive_training()

if results.get('training_completed'):
print("🎉 COMPREHENSIVE 10K TRAINING DATA GENERATION COMPLETED!")
print("=" * 80)
print(f"📊 Total Examples: {results['total_training_examples']:,}")
print(f"🎯 Instruments Processed: {results['total_instruments_processed']:,}")
print(f"⏱️  Training Time: {results['training_time_hours']:.1f} hours")
print(f"🚀 Overall Performance: {results['model_performance']['overall_performance']:.3f}")
print(f"📅 Date Range: {results['date_range']['start']} to {results['date_range']['end']}")
print("=" * 80)
else:
print("❌ Comprehensive training failed")
return 1

return 0

if __name__ == "__main__":
import sys
sys.exit(asyncio.run(main()))
