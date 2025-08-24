#!/usr/bin/env python3

#!/usr/bin/env python3
"""
REAL Production Model Training with Actual Database Data

This trains an actual ML model using real data from the database,
not mock/simulated data.
"""

import os
import sys
import uuid
import asyncio
import logging
import pickle
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import asyncpg
from tqdm import tqdm
import traceback

# Configure logging
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealProductionModelTrainer:
"""REAL production model trainer using actual database data"""

def __init__(self):
self.training_start = date(2020, 1, 1)
self.training_end = date(2023, 12, 31)
self.batch_size = 50  # Process in smaller batches for memory management
self.target_instruments = 500  # Realistic target for actual training

# Database connection
self.db_url = f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"

async def train_real_model(self) -> Dict:
"""Train actual model with real data"""

logger.info("🚀 Starting REAL Production Model Training")
logger.info(f"📅 Training Period: {self.training_start} to {self.training_end}")
logger.info(f"🎯 Target Instruments: {self.target_instruments}")

start_time = time.time()
training_id = str(uuid.uuid4())

try:
# Step 1: Get real data statistics
data_stats = await self._get_real_data_stats()
logger.info(f"📊 Database Stats: {data_stats}")

# Step 2: Get eligible instruments with real data filtering
eligible_instruments = await self._get_real_eligible_instruments()
logger.info(f"✅ Found {len(eligible_instruments)} eligible instruments")

if len(eligible_instruments) < 10:
logger.error("❌ Insufficient instruments for training")
return {}

# Step 3: Generate real training features
training_data = await self._generate_real_training_data(eligible_instruments[:self.target_instruments])

if len(training_data) < 1000:
logger.error(f"❌ Insufficient training data: {len(training_data)} examples")
return {}

logger.info(f"✅ Generated {len(training_data):,} real training examples")

# Step 4: Train actual ML model
model_results = await self._train_real_ml_model(training_data)

# Step 5: Validate model with real data
validation_results = await self._validate_real_model(training_data, model_results)

# Step 6: Save real model artifacts
model_path = await self._save_real_model_artifacts(model_results, training_id, validation_results)

training_time = time.time() - start_time

# Step 7: Generate real training report
training_report = {
'training_id': training_id,
'training_timestamp': datetime.utcnow().isoformat(),
'training_period': {
'start': self.training_start.isoformat(),
'end': self.training_end.isoformat()
},
'data_summary': {
'database_instruments': data_stats['total_instruments'],
'eligible_instruments': len(eligible_instruments),
'trained_instruments': min(len(eligible_instruments), self.target_instruments),
'total_training_examples': len(training_data),
'avg_examples_per_symbol': len(training_data) / min(len(eligible_instruments), self.target_instruments)
},
'model_results': model_results,
'validation_results': validation_results,
'performance_metrics': {
'training_time_hours': training_time / 3600,
'examples_per_minute': len(training_data) / (training_time / 60),
'memory_usage_peak_gb': self._get_memory_usage()
},
'model_artifacts': {
'model_path': str(model_path),
'model_size_mb': model_path.stat().st_size / (1024*1024) if model_path.exists() else 0
}
}

# Save training report
report_path = Path("models/production") / f"real_training_report_{training_id}.json"
report_path.parent.mkdir(parents=True, exist_ok=True)
with open(report_path, 'w') as f:
json.dump(training_report, f, indent=2)

logger.info("✅ REAL Production model training completed!")
logger.info(f"📊 Training Examples: {len(training_data):,}")
logger.info(f"⏱️  Training Time: {training_time/3600:.1f} hours")
logger.info(f"🎯 Model Accuracy: {validation_results.get('accuracy', 0):.3f}")
logger.info(f"💾 Model Saved: {model_path}")

return training_report

except Exception as e:
logger.error(f"❌ REAL training failed: {e}")
traceback.print_exc()
return {}

async def _get_real_data_stats(self) -> Dict:
"""Get actual database statistics"""

conn = await asyncpg.connect(self.db_url)
try:
# Real instrument count
instrument_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")

# Real price data count for training period
price_count = await conn.fetchval("""
SELECT COUNT(*) FROM dev_daily_prices 
WHERE date >= $1 AND date <= $2
""", self.training_start, self.training_end)

# Real data coverage
symbols_with_data = await conn.fetchval("""
SELECT COUNT(DISTINCT symbol) FROM dev_daily_prices 
WHERE date >= $1 AND date <= $2
""", self.training_start, self.training_end)

return {
'total_instruments': instrument_count,
'price_records': price_count,
'symbols_with_data': symbols_with_data,
'data_coverage_pct': (symbols_with_data / instrument_count * 100) if instrument_count > 0 else 0
}

finally:
await conn.close()

async def _get_real_eligible_instruments(self) -> List[str]:
"""Get instruments with sufficient real data"""

conn = await asyncpg.connect(self.db_url)
try:
# Get symbols with sufficient price data
query = """
SELECT i.symbol, COUNT(dp.date) as price_count
FROM dev_instruments i
JOIN dev_daily_prices dp ON i.symbol = dp.symbol
WHERE dp.date >= $1 AND dp.date <= $2
GROUP BY i.symbol
HAVING COUNT(dp.date) >= 200  # At least 200 trading days
ORDER BY COUNT(dp.date) DESC
LIMIT 1000
"""

rows = await conn.fetch(query, self.training_start, self.training_end)
symbols = [row['symbol'] for row in rows]

logger.info(f"📈 Found {len(symbols)} symbols with sufficient data")
if symbols:
logger.info(f"📋 Top symbols: {symbols[:10]}")

return symbols

finally:
await conn.close()

async def _generate_real_training_data(self, symbols: List[str]) -> List[Dict]:
"""Generate training data from real price data"""

logger.info(f"🏗️  Generating training data for {len(symbols)} symbols...")

training_examples = []
conn = await asyncpg.connect(self.db_url)

try:
for i, symbol in enumerate(tqdm(symbols, desc="Processing symbols")):
try:
# Get real price data for this symbol
price_data = await conn.fetch("""
SELECT date, open_price, high_price, low_price, close_price, volume
FROM dev_daily_prices
WHERE symbol = $1 AND date >= $2 AND date <= $3
ORDER BY date
""", symbol, self.training_start, self.training_end)

if len(price_data) < 50:  # Skip if insufficient data
continue

# Convert to DataFrame for feature engineering
df = pd.DataFrame([dict(row) for row in price_data])
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date')

# Generate features
features = self._generate_features(df)

# Generate labels (simplified support/resistance detection)
labels = self._generate_labels(df)

# Create training examples
for j in range(len(features)):
if j < len(labels):
training_examples.append({
'symbol': symbol,
'date': df.iloc[j]['date'].date(),
'features': features[j],
'labels': labels[j]
})

if i % 10 == 0:
logger.info(f"Processed {i+1}/{len(symbols)} symbols, {len(training_examples)} examples so far")

# Memory management - limit total examples
if len(training_examples) >= 50000:
logger.info("Reached example limit, stopping data generation")
break

except Exception as e:
logger.warning(f"Failed to process {symbol}: {e}")
continue

return training_examples

finally:
await conn.close()

def _generate_features(self, df: pd.DataFrame) -> List[List[float]]:
"""Generate technical features from price data"""

# Basic price features
df['returns'] = df['close_price'].pct_change()
df['high_low_ratio'] = df['high_price'] / df['low_price'] - 1
df['open_close_ratio'] = df['close_price'] / df['open_price'] - 1

# Moving averages
df['sma_5'] = df['close_price'].rolling(5).mean()
df['sma_10'] = df['close_price'].rolling(10).mean()
df['sma_20'] = df['close_price'].rolling(20).mean()

# Price position features
df['price_vs_sma5'] = df['close_price'] / df['sma_5'] - 1
df['price_vs_sma10'] = df['close_price'] / df['sma_10'] - 1
df['price_vs_sma20'] = df['close_price'] / df['sma_20'] - 1

# Volatility
df['volatility_5'] = df['returns'].rolling(5).std()
df['volatility_20'] = df['returns'].rolling(20).std()

# Volume features
df['volume_sma_5'] = df['volume'].rolling(5).mean()
df['volume_ratio'] = df['volume'] / df['volume_sma_5']

# Select feature columns
feature_cols = [
'returns', 'high_low_ratio', 'open_close_ratio',
'price_vs_sma5', 'price_vs_sma10', 'price_vs_sma20',
'volatility_5', 'volatility_20', 'volume_ratio'
]

features = []
for i in range(len(df)):
row_features = []
for col in feature_cols:
val = df.iloc[i][col]
if pd.isna(val):
val = 0.0
row_features.append(float(val))
features.append(row_features)

return features

def _generate_labels(self, df: pd.DataFrame) -> List[Dict]:
"""Generate support/resistance labels"""

labels = []

for i in range(len(df)):
if i < 5 or i >= len(df) - 5:  # Skip edges
labels.append({'support_level': 0.0, 'resistance_level': 0.0, 'trend': 0})
continue

# Simple support/resistance detection
window = df.iloc[i-5:i+6]
current_price = df.iloc[i]['close_price']

# Support: lowest point in window
support_level = window['low_price'].min()
support_strength = (current_price - support_level) / current_price

# Resistance: highest point in window  
resistance_level = window['high_price'].max()
resistance_strength = (resistance_level - current_price) / current_price

# Trend detection
trend = 1 if df.iloc[i]['close_price'] > df.iloc[i-5]['close_price'] else -1

labels.append({
'support_level': float(support_strength),
'resistance_level': float(resistance_strength), 
'trend': trend
})

return labels

async def _train_real_ml_model(self, training_data: List[Dict]) -> Dict:
"""Train actual ML model"""

logger.info(f"🧠 Training ML model on {len(training_data)} real examples...")

# Prepare training data
X = []
y_support = []
y_resistance = []

for example in training_data:
if len(example['features']) == 9:  # Ensure consistent feature count
X.append(example['features'])
y_support.append(example['labels']['support_level'])
y_resistance.append(example['labels']['resistance_level'])

X = np.array(X)
y_support = np.array(y_support)
y_resistance = np.array(y_resistance)

logger.info(f"📊 Training data shape: X={X.shape}, y_support={y_support.shape}")

# Train models
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Split data
X_train, X_test, y_sup_train, y_sup_test, y_res_train, y_res_test = train_test_split(
X, y_support, y_resistance, test_size=0.2, random_state=42
)

# Train support level model
support_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
support_model.fit(X_train, y_sup_train)

# Train resistance level model
resistance_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
resistance_model.fit(X_train, y_res_train)

# Evaluate models
sup_pred = support_model.predict(X_test)
res_pred = resistance_model.predict(X_test)

support_mae = mean_absolute_error(y_sup_test, sup_pred)
resistance_mae = mean_absolute_error(y_res_test, res_pred)
support_r2 = r2_score(y_sup_test, sup_pred)
resistance_r2 = r2_score(y_res_test, res_pred)

results = {
'support_model': support_model,
'resistance_model': resistance_model,
'support_mae': support_mae,
'resistance_mae': resistance_mae,
'support_r2': support_r2,
'resistance_r2': resistance_r2,
'training_samples': len(X_train),
'test_samples': len(X_test)
}

logger.info(f"✅ Model training completed!")
logger.info(f"📈 Support MAE: {support_mae:.4f}, R²: {support_r2:.4f}")
logger.info(f"📈 Resistance MAE: {resistance_mae:.4f}, R²: {resistance_r2:.4f}")

return results

async def _validate_real_model(self, training_data: List[Dict], model_results: Dict) -> Dict:
"""Validate model with real metrics"""

# Calculate validation metrics
validation_results = {
'support_mae': model_results['support_mae'],
'resistance_mae': model_results['resistance_mae'],
'support_r2': model_results['support_r2'],
'resistance_r2': model_results['resistance_r2'],
'overall_accuracy': (model_results['support_r2'] + model_results['resistance_r2']) / 2,
'training_samples': model_results['training_samples'],
'test_samples': model_results['test_samples'],
'model_complexity': 'random_forest_ensemble'
}

return validation_results

async def _save_real_model_artifacts(self, model_results: Dict, training_id: str, validation_results: Dict) -> Path:
"""Save real trained models"""

model_dir = Path("models/production")
model_dir.mkdir(parents=True, exist_ok=True)

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
model_path = model_dir / f"real_sr_model_{timestamp}_{training_id[:8]}.pkl"

# Save both models
model_artifact = {
'support_model': model_results['support_model'],
'resistance_model': model_results['resistance_model'],
'training_id': training_id,
'validation_results': validation_results,
'feature_names': [
'returns', 'high_low_ratio', 'open_close_ratio',
'price_vs_sma5', 'price_vs_sma10', 'price_vs_sma20',
'volatility_5', 'volatility_20', 'volume_ratio'
],
'model_type': 'support_resistance_random_forest',
'created_at': datetime.utcnow().isoformat()
}

with open(model_path, 'wb') as f:
pickle.dump(model_artifact, f)

logger.info(f"💾 Real model saved: {model_path}")

return model_path

def _get_memory_usage(self) -> float:
"""Get current memory usage in GB"""
import psutil
process = psutil.Process()
return process.memory_info().rss / (1024**3)

async def main():
"""Main training function"""

print("🚀 " + "="*70)
print("   REAL PRODUCTION MODEL TRAINING: 2020-2023")
print("="*72)
print()

trainer = RealProductionModelTrainer()

try:
results = await trainer.train_real_model()

if results:
print("\n✅ REAL TRAINING COMPLETED SUCCESSFULLY!")
print("="*72)
print(f"🆔 Training ID: {results['training_id']}")
print(f"📊 Real Examples: {results['data_summary']['total_training_examples']:,}")
print(f"🎯 Instruments: {results['data_summary']['trained_instruments']:,}")
print(f"⏱️  Training Time: {results['performance_metrics']['training_time_hours']:.1f} hours")
print(f"🎯 Overall Accuracy: {results['validation_results']['overall_accuracy']:.3f}")
print(f"💾 Model Size: {results['model_artifacts']['model_size_mb']:.1f} MB")
print()
print("🎉 REAL MODEL READY FOR PRODUCTION!")

else:
print("\n❌ REAL TRAINING FAILED!")
return 1

return 0

except Exception as e:
logger.error(f"❌ Training error: {e}")
traceback.print_exc()
return 1

if __name__ == "__main__":
import sys
sys.exit(asyncio.run(main()))
